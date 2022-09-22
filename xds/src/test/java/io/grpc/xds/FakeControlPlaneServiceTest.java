/*
 * Copyright 2022 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.grpc.xds;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.UInt32Value;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.HealthStatus;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.core.v3.TrafficDirection;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.envoyproxy.envoy.config.listener.v3.ApiListener;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.FilterChainMatch;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.NonForwardingAction;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.testing.protobuf.ControlData;
import io.grpc.testing.protobuf.ExtraResourceRequest;
import io.grpc.testing.protobuf.ResourceType;
import io.grpc.testing.protobuf.UpdateControlDataRequest;
import io.grpc.testing.protobuf.XdsConfig;
import io.grpc.testing.protobuf.XdsResourceType;
import io.grpc.testing.protobuf.XdsTestConfigServiceGrpc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * Tests the fake control plane service itself (The one designed for misbehaving control planes).
 * {@link XdsTestControlPlaneExternalService}.
 */
@RunWith(JUnit4.class)
public class FakeControlPlaneServiceTest {

  private static final Logger logger =
      Logger.getLogger(FakeControlPlaneServiceTest.class.getName());
  private static final String SERVER_NAME = "test-xds";
  private static final String SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT =
      "grpc/server?udpa.resource.listening_address=";
  private static final String RDS_NAME = "route-config.googleapis.com";
  private static final String CLUSTER_NAME = "cluster0";
  private static final String EDS_NAME = "eds-service-0";
  private static final String HTTP_CONNECTION_MANAGER_TYPE_URL =
      "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3"
          + ".HttpConnectionManager";
  private static final String LOCAL_HOST = "localhost";

  private XdsTestControlPlaneExternalService controlPlaneService =
      new XdsTestControlPlaneExternalService();
  private XdsTestControlPlaneExternalWrapperService wrapperService =
      new XdsTestControlPlaneExternalWrapperService(controlPlaneService);

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private Server server = grpcCleanup.register(InProcessServerBuilder
      .forName(SERVER_NAME)
      .directExecutor()
      .addService(controlPlaneService)
      .addService(wrapperService)
      .build());
  private Channel channel = grpcCleanup.register(InProcessChannelBuilder.forName(SERVER_NAME)
      .directExecutor().build());
  protected XdsTestConfigServiceGrpc.XdsTestConfigServiceBlockingStub controlStub =
       XdsTestConfigServiceGrpc.newBlockingStub(channel);
  protected AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub discoveryStub =
      AggregatedDiscoveryServiceGrpc.newStub(channel);
  protected StreamObserver<DiscoveryRequest> discoveryWriter;

  // Last used resources
  protected Map<String, Message> ldsMap = new HashMap<>();
  protected Map<String, Message> rdsMap = new HashMap<>();
  protected Map<String, Message> cdsMap = new HashMap<>();
  protected Map<String, Message> edsMap = new HashMap<>();

  // Tracking changes
  protected Queue<DiscoveryResponse> responses = new LinkedList<>();
  protected Queue<XdsClient.LdsUpdate> ldsUpdateQueue = new LinkedList<>();
  protected Queue<XdsClient.RdsUpdate> rdsUpdateQueue = new LinkedList<>();
  protected Queue<XdsClient.CdsUpdate> cdsUpdateQueue = new LinkedList<>();
  protected Queue<XdsClient.EdsUpdate> edsUpdateQueue = new LinkedList<>();
  protected Map<ResourceType, Queue<Status>> resourceErrorMap = new HashMap<>();
  protected Map<ResourceType, Queue<String>> missingResourceMap = new HashMap<>();
  private LdsWatcher ldsWatcher = new LdsWatcher();
  private RdsWatcher rdsWatcher = new RdsWatcher();
  private CdsWatcher cdsWatcher = new CdsWatcher();
  private EdsWatcher edsWatcher = new EdsWatcher();

  /**
   * Start control plane server and get control plane port.
   */
  @Before
  public void setUp() throws Exception {
//    clearXdsClient();
    server.start();
    discoveryWriter =
        discoveryStub.withWaitForReady().streamAggregatedResources(getDiscoveryReader());
  }

  private void clearXdsClient() {
    ldsUpdateQueue.clear();
    rdsUpdateQueue.clear();
    cdsUpdateQueue.clear();
    edsUpdateQueue.clear();
    this.missingResourceMap.clear();
    this.resourceErrorMap.clear();

    wrapperService.clear(null);
  }

  @Test
  public void VerifySimpleCase() throws Exception {
    String tcpListenerName = SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT;
    String serverHostName = "test-server";
    setToDefaultConfig(tcpListenerName, serverHostName);

    // Register interest and check that we are getting what we expect
    watchResource(ResourceType.LDS, serverHostName);
    watchResource(ResourceType.RDS, RDS_NAME);
    watchResource(ResourceType.CDS, CLUSTER_NAME);
    watchResource(ResourceType.EDS, EDS_NAME);
    waitForResults();
    Assert.assertEquals(4, responses.size());
    // TODO verify things came as expected

    // TODO update config and make sure updates are propagated
    // TODO try each type of control data and make sure
    //    updates are propagated correctly
    //    new discovery is handled correctly
  }

  private void waitForResults() {
    controlPlaneService.waitForSyncContextToDrain();

  }

  private void watchResource(ResourceType type, String... resources) {
    DiscoveryRequest request = buildDiscoveryRequest(type, Arrays.asList(resources));
    discoveryWriter.onNext(request);
  }

  private DiscoveryRequest buildDiscoveryRequest(ResourceType cpType, Collection<String> resources) {
    AbstractXdsClient.ResourceType type = AbstractXdsClient.ResourceType.valueOf(cpType.name());
    DiscoveryRequest.Builder builder =
        DiscoveryRequest.newBuilder()
            .setVersionInfo("")
//            .setNode(bootstrapNode.toEnvoyProtoNodeV2())
            .addAllResourceNames(resources)
            .setTypeUrl(type.typeUrl())
            .setResponseNonce("");
    return builder.build();
  }

  private static Listener clientListener(String name) {
    HttpFilter httpFilter = HttpFilter.newBuilder()
        .setName("terminal-filter")
        .setTypedConfig(Any.pack(Router.newBuilder().build()))
        .setIsOptional(true)
        .build();
    ApiListener apiListener = ApiListener.newBuilder().setApiListener(Any.pack(
        HttpConnectionManager.newBuilder()
            .setRds(
                Rds.newBuilder()
                    .setRouteConfigName(RDS_NAME)
                    .setConfigSource(
                        ConfigSource.newBuilder()
                            .setAds(AggregatedConfigSource.getDefaultInstance())))
            .addAllHttpFilters(Collections.singletonList(httpFilter))
            .build(),
        HTTP_CONNECTION_MANAGER_TYPE_URL)).build();
    return Listener.newBuilder()
        .setName(name)
        .setApiListener(apiListener).build();
  }

  private static Listener serverListener(String name) {
    HttpFilter routerFilter = HttpFilter.newBuilder()
        .setName("terminal-filter")
        .setTypedConfig(
            Any.pack(Router.newBuilder().build()))
        .setIsOptional(true)
        .build();
    VirtualHost virtualHost = VirtualHost.newBuilder()
        .setName("virtual-host-0")
        .addDomains("*")
        .addRoutes(
            Route.newBuilder()
                .setMatch(
                    RouteMatch.newBuilder().setPrefix("/").build())
                .setNonForwardingAction(NonForwardingAction.newBuilder().build())
                .build()).build();
    RouteConfiguration routeConfig = RouteConfiguration.newBuilder()
        .addVirtualHosts(virtualHost)
        .build();
    Filter filter = Filter.newBuilder()
        .setName("network-filter-0")
        .setTypedConfig(
            Any.pack(
                HttpConnectionManager.newBuilder()
                    .setRouteConfig(routeConfig)
                    .addAllHttpFilters(Collections.singletonList(routerFilter))
                    .build())).build();
    FilterChainMatch filterChainMatch = FilterChainMatch.newBuilder()
        .setSourceType(FilterChainMatch.ConnectionSourceType.ANY)
        .build();
    FilterChain filterChain = FilterChain.newBuilder()
        .setName("filter-chain-0")
        .setFilterChainMatch(filterChainMatch)
        .addFilters(filter)
        .build();
    return Listener.newBuilder()
        .setName(name)
        .setTrafficDirection(TrafficDirection.INBOUND)
        .addFilterChains(filterChain)
        .build();
  }

  // Sets instance values and call setXdsConfigRpc
  private void setToDefaultConfig(String tcpListenerName, String serverHostName) {
    ldsMap = ImmutableMap.of(
        tcpListenerName, serverListener(tcpListenerName),
        serverHostName, clientListener(serverHostName),
        "dummy1", ControlData.newBuilder().build(),
        "dummy2", ControlData.newBuilder().build()
    );

    // Build RDS map of real value, plus 2 dummy entries
    rdsMap.clear();
    rdsMap.put(RDS_NAME, rds(serverHostName));
    for (int i = 1; i <= 3; i++) {
      rdsMap.put(RDS_NAME + i, rds(serverHostName));
    }

    // Build CDS map of real value, plus 2 empty entries
    cdsMap.clear();
    cdsMap.put(CLUSTER_NAME, cds());
    for (int i = 1; i <= 3; i++) {
      String name = CLUSTER_NAME + i;
      Cluster cluster = Cluster.newBuilder()
          .setName(CLUSTER_NAME)
          .setType(Cluster.DiscoveryType.EDS)
          .setLbPolicy(Cluster.LbPolicy.ROUND_ROBIN)
          .build();
      cdsMap.put(name, cluster);
    }

    // Build EDS map of real value, plus 2 empty entries
    edsMap.clear();
    edsMap.put(EDS_NAME, eds(serverHostName, 1));
    for (int i = 1; i <= 3; i++) {
      String endpointName = EDS_NAME + i;
      edsMap.put(endpointName,
          ClusterLoadAssignment.newBuilder().setClusterName(endpointName).build());
    }
    controlStub.setXdsConfigRpc(genXdsConfig(XdsResourceType.LDS));
    controlStub.setXdsConfigRpc(genXdsConfig(XdsResourceType.RDS));
    controlStub.setXdsConfigRpc(genXdsConfig(XdsResourceType.CDS));
    controlStub.setXdsConfigRpc(genXdsConfig(XdsResourceType.EDS));

    controlStub.setExtraResources(ExtraResourceRequest.getDefaultInstance());
    controlStub.updateControlData(UpdateControlDataRequest.getDefaultInstance());
  }

  private StreamObserver<DiscoveryResponse> getDiscoveryReader() {
    StreamObserver<DiscoveryResponse> responseReader =
        new StreamObserver<DiscoveryResponse>() {
          @Override
          public void onNext(final DiscoveryResponse response) {
            AbstractXdsClient.ResourceType type = AbstractXdsClient.ResourceType.fromTypeUrl(response.getTypeUrl());
            responses.add(response);
          }

          @Override
          public void onError(final Throwable t) {
            System.out.println(t);
            // TODO
          }

          @Override
          public void onCompleted() {
            // TODO cleanup?
          }
        };
    return responseReader;
  }

  private XdsConfig genXdsConfig(XdsResourceType type) {
    XdsConfig.Builder xdsConfigBuilder = XdsConfig.newBuilder().setType(type);

    Map<String, Message> messageMap;
    switch (type) {
      case LDS: messageMap = ldsMap; break;
      case RDS: messageMap = rdsMap; break;
      case CDS: messageMap = cdsMap; break;
      case EDS: messageMap = edsMap; break;
      default: throw new IllegalArgumentException();
    }
    for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
      xdsConfigBuilder.addConfigurationBuilder()
          .setName(entry.getKey())
          .setConfiguration(Any.pack(entry.getValue()));
    }

    return xdsConfigBuilder.build();
  }

  private static RouteConfiguration rds(String authority) {
    VirtualHost virtualHost = VirtualHost.newBuilder()
        .addDomains(authority)
        .addRoutes(
            Route.newBuilder()
                .setMatch(
                    RouteMatch.newBuilder().setPrefix("/").build())
                .setRoute(
                    RouteAction.newBuilder().setCluster(CLUSTER_NAME).build()).build()).build();
    return RouteConfiguration.newBuilder().setName(RDS_NAME).addVirtualHosts(virtualHost).build();
  }

  private static Cluster cds() {
    return Cluster.newBuilder()
        .setName(CLUSTER_NAME)
        .setType(Cluster.DiscoveryType.EDS)
        .setEdsClusterConfig(
            Cluster.EdsClusterConfig.newBuilder()
                .setServiceName(EDS_NAME)
                .setEdsConfig(
                    ConfigSource.newBuilder()
                        .setAds(AggregatedConfigSource.newBuilder().build())
                        .build())
                .build())
        .setLbPolicy(Cluster.LbPolicy.ROUND_ROBIN)
        .build();
  }

  private static ClusterLoadAssignment eds(String hostName, int port) {
    Address address = Address.newBuilder()
        .setSocketAddress(
            SocketAddress.newBuilder().setAddress(hostName).setPortValue(port).build()).build();
    LocalityLbEndpoints endpoints = LocalityLbEndpoints.newBuilder()
        .setLoadBalancingWeight(UInt32Value.of(10))
        .setPriority(0)
        .addLbEndpoints(
            LbEndpoint.newBuilder()
                .setEndpoint(
                    Endpoint.newBuilder().setAddress(address).build())
                .setHealthStatus(HealthStatus.HEALTHY)
                .build()).build();
    return ClusterLoadAssignment.newBuilder()
        .setClusterName(EDS_NAME)
        .addEndpoints(endpoints)
        .build();
  }

  private class LdsWatcher implements XdsClient.LdsResourceWatcher {
    @Override
    public void onChanged(XdsClient.LdsUpdate update) {
      ldsUpdateQueue.add(update);
    }

    @Override
    public void onError(Status error) {
      resourceErrorMap.computeIfAbsent(ResourceType.LDS, k -> new LinkedList<>())
          .add(error);
    }

    @Override
    public void onResourceDoesNotExist(String resourceName) {
      Queue<String> missingResQueue = missingResourceMap.computeIfAbsent(ResourceType.LDS, k -> new LinkedList<>());
      missingResQueue.add(resourceName);
    }
  }
  private class RdsWatcher implements XdsClient.RdsResourceWatcher {
    @Override
    public void onChanged(XdsClient.RdsUpdate update) {
      rdsUpdateQueue.add(update);
    }

    @Override
    public void onError(Status error) {
      resourceErrorMap.computeIfAbsent(ResourceType.RDS, k -> new LinkedList<>())
          .add(error);
    }

    @Override
    public void onResourceDoesNotExist(String resourceName) {
      missingResourceMap.computeIfAbsent(ResourceType.RDS, k -> new LinkedList<>())
          .add(resourceName);
    }
  }
  private class CdsWatcher implements XdsClient.CdsResourceWatcher {
    @Override
    public void onChanged(XdsClient.CdsUpdate update) {
      cdsUpdateQueue.add(update);
    }

    @Override
    public void onError(Status error) {
      resourceErrorMap.computeIfAbsent(ResourceType.CDS, k -> new LinkedList<>())
          .add(error);
    }

    @Override
    public void onResourceDoesNotExist(String resourceName) {
      missingResourceMap.computeIfAbsent(ResourceType.CDS, k -> new LinkedList<>())
          .add(resourceName);
    }
  }
  private class EdsWatcher implements XdsClient.EdsResourceWatcher {
    @Override
    public void onChanged(XdsClient.EdsUpdate update) {
      edsUpdateQueue.add(update);
    }

    @Override
    public void onError(Status error) {
      resourceErrorMap.computeIfAbsent(ResourceType.EDS, k -> new LinkedList<>())
          .add(error);
    }

    @Override
    public void onResourceDoesNotExist(String resourceName) {
      missingResourceMap.computeIfAbsent(ResourceType.EDS,k ->  new LinkedList<>())
          .add(resourceName);
    }
  }
}
