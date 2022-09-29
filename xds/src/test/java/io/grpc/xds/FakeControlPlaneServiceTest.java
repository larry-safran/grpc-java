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

import static io.grpc.testing.protobuf.XdsResourceType.CDS;
import static io.grpc.testing.protobuf.XdsResourceType.EDS;
import static io.grpc.testing.protobuf.XdsResourceType.LDS;
import static io.grpc.testing.protobuf.XdsResourceType.RDS;
import static io.grpc.testing.protobuf.XdsResourceType.UNRECOGNIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
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
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.testing.protobuf.AberrationType;
import io.grpc.testing.protobuf.ControlData;
import io.grpc.testing.protobuf.ExtraResourceRequest;
import io.grpc.testing.protobuf.ResourceType;
import io.grpc.testing.protobuf.TriggerTime;
import io.grpc.testing.protobuf.UpdateControlDataRequest;
import io.grpc.testing.protobuf.XdsConfig;
import io.grpc.testing.protobuf.XdsResourceType;
import io.grpc.testing.protobuf.XdsTestConfigServiceGrpc;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
  public static final UpdateControlDataRequest DEFAULT_UPDATE_CD =
      UpdateControlDataRequest.newBuilder().getDefaultInstanceForType();

  private XdsTestControlPlaneExternalService controlPlaneService =
      new XdsTestControlPlaneExternalService();
  private XdsTestControlPlaneExternalWrapperService wrapperService =
      new XdsTestControlPlaneExternalWrapperService(controlPlaneService);

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private Server server =
      grpcCleanup.register(
          InProcessServerBuilder.forName(SERVER_NAME)
              .directExecutor()
              .addService(controlPlaneService)
              .addService(wrapperService)
              .build());
  private Channel channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(SERVER_NAME).directExecutor().build());
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
  protected Map<String, List<DiscoveryResponse>> responses = new HashMap<>();
  protected Map<XdsResourceType, List<Status>> resourceErrorMap = new HashMap<>();

  /** Start control plane server and get control plane port. */
  @Before
  public void setUp() throws Exception {
    //    clearXdsClient();
    server.start();
    StreamObserver<DiscoveryResponse> discoveryReader = getDiscoveryReader();
    discoveryWriter = discoveryStub.withWaitForReady().streamAggregatedResources(discoveryReader);
  }

  @After
  public void tearDown() {
    if (discoveryWriter != null) {
      StreamObserver<DiscoveryRequest> oldWriter = discoveryWriter;
      discoveryWriter = null;
      oldWriter.onCompleted();
    }
  }

  private void clearXdsClients() {
    responses.clear();
    resourceErrorMap.clear();

    wrapperService.clear(null); // clears the server side
  }

  @Test
  public void verifySimpleCase() throws Exception {
    String tcpListenerName = SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT;
    String serverHostName = "test-server";
    setToDefaultConfig(tcpListenerName, serverHostName);

    // Register interest and check that we are getting what we expect
    watchResource(ResourceType.LDS, serverHostName);
    watchResource(ResourceType.RDS, RDS_NAME);
    watchResource(ResourceType.CDS, CLUSTER_NAME);
    watchResource(ResourceType.EDS, EDS_NAME);
    //    waitForResults();
    // verify things came as expected
    // Verify the right number of responses came and they had the correct resource names
    assertEquals(4, responses.size());
    Assert.assertTrue(resourceErrorMap.isEmpty());

    Map<XdsResourceType, String> thingsToCheck =
        ImmutableMap.of(
            LDS, serverHostName,
            RDS, RDS_NAME,
            CDS, CLUSTER_NAME,
            EDS, EDS_NAME);
    checkSingleNamesPerType(thingsToCheck);

    responses.clear();
    watchResource(ResourceType.LDS, serverHostName, "dummy1");
    asserEqualsUnordered(Arrays.asList("dummy1", serverHostName), getResourceNames(LDS));

    // update config and make sure updates are propagated
    responses.clear();
    ldsMap.remove("dummy1");
    controlStub.setXdsConfigRpc(genXdsConfig(LDS));
    checkSingleNamesPerType(ImmutableMap.of(LDS, serverHostName));
    Assert.assertTrue(resourceErrorMap.isEmpty());

    // try each type of control data and make sure
    //    updates are propagated correctly
    responses.clear();
    sendExtraConfiguration("dummy1", LDS);
    sendCdUpdateForAllResourceTypes(AberrationType.SEND_EXTRA, TriggerTime.BEFORE_LDS);
    Assert.assertEquals(Arrays.asList(serverHostName, "dummy1"), getResourceNames(LDS));

    responses.clear();
    sendCdUpdateForAllResourceTypes(AberrationType.SEND_EMPTY, TriggerTime.BEFORE_CDS);
    Assert.assertEquals(Arrays.asList(serverHostName), getResourceNames(LDS));
    Assert.assertEquals(1L, responses.get(CDS.name()).size());
    assertTrue(responses.get(CDS.name()).get(0).getResourcesList().isEmpty());

    responses.clear();
    sendCdUpdateForAllResourceTypes(AberrationType.SEND_REDUNDANT, TriggerTime.BEFORE_LDS);
    Assert.assertEquals(Arrays.asList(serverHostName, serverHostName), getResourceNames(LDS));

    responses.clear();
    int targetCode = 11;
    sendStatusCodeUpdate(targetCode, TriggerTime.BEFORE_CDS);
    Assert.assertEquals(Arrays.asList(serverHostName), getResourceNames(LDS));
    Assert.assertEquals(1, resourceErrorMap.get(RDS).size());
    Assert.assertEquals(targetCode, resourceErrorMap.get(RDS).get(0).getCode().value());
    Assert.assertNotNull(getResourceNames(RDS));
    Assert.assertNull("Should not have gotten CDS because of the  error. ",
        getResourceNames(CDS));
    // Verify request of CDS gets error but no resources
    watchResource(ResourceType.CDS, CLUSTER_NAME);
    Assert.assertNull("Should not have gotten CDS because of the  error. ",
        getResourceNames(CDS));

    //    new discovery is handled correctly
    clearXdsClients();
    String[] ldsNames = new String[] {"dummy1", serverHostName};
    Arrays.sort(ldsNames); // For assert comparisons
    watchResource(ResourceType.LDS, ldsNames);
    // Should get a response with no resources
    List<DiscoveryResponse> ldsList = responses.get(LDS.name());
    Assert.assertNotNull(ldsList);
    Assert.assertEquals(1, ldsList.size());
    Assert.assertEquals(0, ldsList.get(0).getResourcesCount());

    setToDefaultConfig(tcpListenerName, serverHostName);
    Assert.assertEquals(Arrays.asList(ldsNames), getResourceNames(LDS));
  }

  private void sendExtraConfiguration(String resName, XdsResourceType type) {
    XdsConfig ldsConfig = generateXdsConfig(resName, type);
    ExtraResourceRequest request =
        ExtraResourceRequest.newBuilder().addConfigurations(ldsConfig).build();
    controlStub.setExtraResources(request);
  }

  private XdsConfig generateXdsConfig(String resName, XdsResourceType type) {
    Message.Builder builder;
    switch (type) {
      case LDS:
        builder = Listener.newBuilder().setName(resName);
        break;
      case RDS:
        builder = RouteConfiguration.newBuilder().setName(resName);
        break;
      case CDS:
        builder = Cluster.newBuilder().setName(resName);
        break;
      case EDS:
        builder = ClusterLoadAssignment.newBuilder().setClusterName(resName);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized type: " + type.name());
    }

    XdsConfig.Resource resource =
        XdsConfig.Resource.newBuilder().setConfiguration(Any.pack(builder.build())).build();
    return XdsConfig.newBuilder().setType(type).addConfiguration(resource).build();
  }

  private void asserEqualsUnordered(List<String> first, List<String> second) {
    first.sort(String::compareTo);
    second.sort(String::compareTo);
    Assert.assertEquals(first, second);
  }

  private void sendCdUpdateForAllResourceTypes(AberrationType type, TriggerTime time) {
    ControlData controlData =
        ControlData.newBuilder()
            .addAllAffectedTypesValue(Arrays.asList(0, 1, 2, 3))
            .setAberrationType(type)
            .setTriggerAberration(time)
            .build();
    UpdateControlDataRequest update =
        UpdateControlDataRequest.newBuilder().setControlData(controlData).build();
    controlStub.updateControlData(update);
  }

  private void sendStatusCodeUpdate(int code, TriggerTime time) {
    ControlData controlData =
        ControlData.newBuilder()
            .addAllAffectedTypesValue(Arrays.asList(0, 1, 2, 3))
            .setAberrationType(AberrationType.STATUS_CODE)
            .setStatusCode(code)
            .setTriggerAberration(time)
            .build();
    UpdateControlDataRequest update =
        UpdateControlDataRequest.newBuilder().setControlData(controlData).build();
    controlStub.updateControlData(update);
  }

  private void checkSingleNamesPerType(Map<XdsResourceType, String> typedResNames) {
    for (Map.Entry<XdsResourceType, String> entry : typedResNames.entrySet()) {
      assertEquals(Arrays.asList(entry.getValue()), getResourceNames(entry.getKey()));
      checkRsponseIsExpected(entry.getKey(), entry.getValue());
    }
  }

  private static Message unpackSafely(Any resource, Class<? extends Message> clazz) {
    try {
      return resource.unpack(clazz);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private void checkRsponseIsExpected(XdsResourceType type, String name) {
    Message fromServer = controlPlaneService.getResource(type, name);
    Message received = getReceivedResource(type.name(), name);
    String fromString = (fromServer == null) ? null : fromServer.toString();
    String receivedString = (received == null) ? null : received.toString();

    assertEquals(fromString, receivedString);
  }

  private boolean namesMatch(Message msg, String name, Method getName) {
    if (msg == null) {
      return name == null;
    }
    if (name == null) {
      return false;
    }

    try {
      return name.equals(getName.invoke(msg));
    } catch (IllegalAccessException | InvocationTargetException e) {
      fail("Cannot get name from message: " + e.getMessage());
      return false;
    }
  }

  private Message getReceivedResource(String type, String name) {
    Class<? extends Message> clazz = getMessageClassForXdsType(type);
    try {
      Method getName = getGetNameMethod(clazz);
      Message resource =
          responses.get(type).stream()
              .map(DiscoveryResponse::getResourcesList)
              .flatMap(List::stream)
              .map(r -> unpackSafely(r, clazz))
              .filter(r -> namesMatch(r, name, getName))
              .findFirst()
              .orElse(null);

      return resource;
    } catch (NoSuchMethodException e) {
      fail("Could not find getName method for " + type);
      return null; //
    }
  }

  private List<String> getResourceNames(XdsResourceType type) {
    List<DiscoveryResponse> responseList = this.responses.get(type.name());
    if (responseList == null) {
      return null;
    }

    List<String> resourceNames = new ArrayList<>();
    try {
      Class<? extends Message> clazz = getMessageClassForXdsType(type.name());
      Method getName = getGetNameMethod(clazz);

      for (DiscoveryResponse discoveryResponse : responseList) {
        for (Any resource : discoveryResponse.getResourcesList()) {
          Message message = resource.unpack(clazz);
          resourceNames.add((String) getName.invoke(message));
        }
      }
    } catch (Exception e) {
      // should never happen
      fail("Error retrieving resource names: " + e.getMessage());
    }

    return resourceNames;
  }

  private Method getGetNameMethod(Class<? extends Message> clazz) throws NoSuchMethodException {
    String methodName = "getName";
    if (clazz == ClusterLoadAssignment.class) {
      methodName = "getClusterName";
    }
    return clazz.getMethod(methodName);
  }

  private static Class<? extends Message> getMessageClassForXdsType(String type) {
    Class<? extends Message> clazz;
    switch (type) {
      case "LDS":
        clazz = Listener.class;
        break;
      case "RDS":
        clazz = RouteConfiguration.class;
        break;
      case "CDS":
        clazz = Cluster.class;
        break;
      case "EDS":
        clazz = ClusterLoadAssignment.class;
        break;
      default:
        throw new IllegalArgumentException("invalid type: " + type);
    }
    return clazz;
  }

  private void watchResource(ResourceType type, String... resources) {
    DiscoveryRequest request = buildDiscoveryRequest(type, Arrays.asList(resources));
    if (discoveryWriter == null) {
      discoveryWriter =
          discoveryStub.withWaitForReady().streamAggregatedResources(getDiscoveryReader());
    }
    discoveryWriter.onNext(request);
  }

  private DiscoveryRequest buildDiscoveryRequest(
      ResourceType cpType, Collection<String> resources) {
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
    HttpFilter httpFilter =
        HttpFilter.newBuilder()
            .setName("terminal-filter")
            .setTypedConfig(Any.pack(Router.newBuilder().build()))
            .setIsOptional(true)
            .build();
    ApiListener apiListener =
        ApiListener.newBuilder()
            .setApiListener(
                Any.pack(
                    HttpConnectionManager.newBuilder()
                        .setRds(
                            Rds.newBuilder()
                                .setRouteConfigName(RDS_NAME)
                                .setConfigSource(
                                    ConfigSource.newBuilder()
                                        .setAds(AggregatedConfigSource.getDefaultInstance())))
                        .addAllHttpFilters(Collections.singletonList(httpFilter))
                        .build(),
                    HTTP_CONNECTION_MANAGER_TYPE_URL))
            .build();
    return Listener.newBuilder().setName(name).setApiListener(apiListener).build();
  }

  private static Listener serverListener(String name) {
    HttpFilter routerFilter =
        HttpFilter.newBuilder()
            .setName("terminal-filter")
            .setTypedConfig(Any.pack(Router.newBuilder().build()))
            .setIsOptional(true)
            .build();
    VirtualHost virtualHost =
        VirtualHost.newBuilder()
            .setName("virtual-host-0")
            .addDomains("*")
            .addRoutes(
                Route.newBuilder()
                    .setMatch(RouteMatch.newBuilder().setPrefix("/").build())
                    .setNonForwardingAction(NonForwardingAction.newBuilder().build())
                    .build())
            .build();
    RouteConfiguration routeConfig =
        RouteConfiguration.newBuilder().addVirtualHosts(virtualHost).build();
    Filter filter =
        Filter.newBuilder()
            .setName("network-filter-0")
            .setTypedConfig(
                Any.pack(
                    HttpConnectionManager.newBuilder()
                        .setRouteConfig(routeConfig)
                        .addAllHttpFilters(Collections.singletonList(routerFilter))
                        .build()))
            .build();
    FilterChainMatch filterChainMatch =
        FilterChainMatch.newBuilder()
            .setSourceType(FilterChainMatch.ConnectionSourceType.ANY)
            .build();
    FilterChain filterChain =
        FilterChain.newBuilder()
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
    ldsMap =
        new HashMap<>(
            ImmutableMap.of(
                tcpListenerName,
                serverListener(tcpListenerName),
                serverHostName,
                clientListener(serverHostName),
                "dummy1",
                Listener.newBuilder().setName("dummy1").build(),
                "dummy2",
                Listener.newBuilder().setName("dummy2").build()));

    // Build RDS map of real value, plus 2 dummy entries
    rdsMap.clear();
    rdsMap.put(RDS_NAME, rds(RDS_NAME, serverHostName));
    for (int i = 1; i <= 3; i++) {
      String name = RDS_NAME + i;
      rdsMap.put(name, RouteConfiguration.newBuilder().setName(name).build());
    }

    // Build CDS map of real value, plus 2 empty entries
    cdsMap.clear();
    cdsMap.put(CLUSTER_NAME, cds());
    for (int i = 1; i <= 3; i++) {
      String name = CLUSTER_NAME + i;
      Cluster cluster =
          Cluster.newBuilder()
              .setName(name)
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
      edsMap.put(
          endpointName, ClusterLoadAssignment.newBuilder().setClusterName(endpointName).build());
    }

    for (XdsResourceType type : XdsResourceType.values()) {
      if (type != UNRECOGNIZED) {
        controlStub.setXdsConfigRpc(genXdsConfig(type));
      }
    }
    controlStub.setExtraResources(ExtraResourceRequest.getDefaultInstance());
    controlStub.updateControlData(UpdateControlDataRequest.getDefaultInstance());
  }

  private StreamObserver<DiscoveryResponse> getDiscoveryReader() {
    StreamObserver<DiscoveryResponse> responseReader =
        new StreamObserver<DiscoveryResponse>() {
          @Override
          public void onNext(final DiscoveryResponse response) {
            AbstractXdsClient.ResourceType type =
                AbstractXdsClient.ResourceType.fromTypeUrl(response.getTypeUrl());
            responses.computeIfAbsent(type.name(), l -> new ArrayList<>()).add(response);
          }

          @Override
          public void onError(final Throwable t) {
            if (t instanceof StatusRuntimeException) {
              addStatusToErrors((StatusRuntimeException) t);
            }

            onCompleted();
          }

          @Override
          public void onCompleted() {
            logger.log(Level.FINE, "Completed was called: " + discoveryWriter);
            if (discoveryWriter == null) {
              return;
            }
            StreamObserver<DiscoveryRequest> oldWriter = discoveryWriter;
            discoveryWriter = null;
            oldWriter.onCompleted();
          }
        };
    return responseReader;
  }

  private void addStatusToErrors(StatusRuntimeException t) {
    Status status = t.getStatus();
    if (status == null || status.getDescription() == null) {
      fail("Malformed error received");
    }
    int lastSpace = status.getDescription().lastIndexOf(' ');
    String rt = status.getDescription().substring(lastSpace + 1);
    XdsResourceType type = XdsTestControlPlaneExternalService.convertStringToType(rt);
    List<Status> statuses = resourceErrorMap.computeIfAbsent(type, l -> new ArrayList<>());
    statuses.add(status);
  }

  private XdsConfig genXdsConfig(XdsResourceType type) {
    if (type == UNRECOGNIZED) {
      return null;
    }
    XdsConfig.Builder xdsConfigBuilder = XdsConfig.newBuilder().setType(type);

    Map<String, Message> messageMap;
    switch (type) {
      case LDS:
        messageMap = ldsMap;
        break;
      case RDS:
        messageMap = rdsMap;
        break;
      case CDS:
        messageMap = cdsMap;
        break;
      case EDS:
        messageMap = edsMap;
        break;
      default:
        throw new IllegalArgumentException();
    }
    for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
      xdsConfigBuilder
          .addConfigurationBuilder()
          .setName(entry.getKey())
          .setConfiguration(Any.pack(entry.getValue()));
    }

    return xdsConfigBuilder.build();
  }

  private static RouteConfiguration rds(String name, String authority) {
    VirtualHost virtualHost =
        VirtualHost.newBuilder()
            .addDomains(authority)
            .addRoutes(
                Route.newBuilder()
                    .setMatch(RouteMatch.newBuilder().setPrefix("/").build())
                    .setRoute(RouteAction.newBuilder().setCluster(CLUSTER_NAME).build())
                    .build())
            .build();
    return RouteConfiguration.newBuilder().setName(name).addVirtualHosts(virtualHost).build();
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
    Address address =
        Address.newBuilder()
            .setSocketAddress(
                SocketAddress.newBuilder().setAddress(hostName).setPortValue(port).build())
            .build();
    LocalityLbEndpoints endpoints =
        LocalityLbEndpoints.newBuilder()
            .setLoadBalancingWeight(UInt32Value.of(10))
            .setPriority(0)
            .addLbEndpoints(
                LbEndpoint.newBuilder()
                    .setEndpoint(Endpoint.newBuilder().setAddress(address).build())
                    .setHealthStatus(HealthStatus.HEALTHY)
                    .build())
            .build();
    return ClusterLoadAssignment.newBuilder()
        .setClusterName(EDS_NAME)
        .addEndpoints(endpoints)
        .build();
  }
}
