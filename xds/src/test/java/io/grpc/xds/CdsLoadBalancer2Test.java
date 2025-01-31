/*
 * Copyright 2020 The gRPC Authors
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

import static com.google.common.truth.Truth.assertThat;
import static io.grpc.util.GracefulSwitchLoadBalancerAccessor.getChildConfig;
import static io.grpc.util.GracefulSwitchLoadBalancerAccessor.getChildProvider;
import static io.grpc.xds.XdsLbPolicies.CLUSTER_IMPL_POLICY_NAME;
import static io.grpc.xds.XdsLbPolicies.CLUSTER_RESOLVER_POLICY_NAME;
import static io.grpc.xds.XdsLbPolicies.PRIORITY_POLICY_NAME;
import static io.grpc.xds.XdsTestUtils.RDS_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.grpc.Attributes;
import io.grpc.ChannelLogger;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InternalLogId;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.Helper;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolver;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import io.grpc.internal.ExponentialBackoffPolicy;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ObjectPool;
import io.grpc.util.OutlierDetectionLoadBalancer;
import io.grpc.util.OutlierDetectionLoadBalancer.OutlierDetectionLoadBalancerConfig;
import io.grpc.xds.CdsLoadBalancer2.ClusterResolverConfig;
import io.grpc.xds.CdsLoadBalancer2.ClusterResolverConfig.DiscoveryMechanism;
import io.grpc.xds.CdsLoadBalancerProvider.CdsConfig;
import io.grpc.xds.ClusterImplLoadBalancerProvider.ClusterImplConfig;
import io.grpc.xds.EnvoyServerProtoData.OutlierDetection;
import io.grpc.xds.EnvoyServerProtoData.SuccessRateEjection;
import io.grpc.xds.EnvoyServerProtoData.UpstreamTlsContext;
import io.grpc.xds.LeastRequestLoadBalancer.LeastRequestConfig;
import io.grpc.xds.PriorityLoadBalancerProvider.PriorityLbConfig;
import io.grpc.xds.RingHashLoadBalancer.RingHashConfig;
import io.grpc.xds.XdsClusterResource.CdsUpdate;
import io.grpc.xds.XdsEndpointResource.EdsUpdate;
import io.grpc.xds.client.Bootstrapper.BootstrapInfo;
import io.grpc.xds.client.Bootstrapper.ServerInfo;
import io.grpc.xds.client.EnvoyProtoData;
import io.grpc.xds.client.Locality;
import io.grpc.xds.client.XdsClient;
import io.grpc.xds.client.XdsLogger;
import io.grpc.xds.client.XdsLogger.XdsLogLevel;
import io.grpc.xds.client.XdsResourceType;
import io.grpc.xds.internal.security.CommonTlsContextTestsUtil;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.matchers.NotNull;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Tests for {@link CdsLoadBalancer2}.
 */
@RunWith(JUnit4.class)
public class CdsLoadBalancer2Test {
  private static final XdsLogger logger = XdsLogger.withLogId(
      InternalLogId.allocate("CdsLoadBalancer2Test", null));

  @Rule public final MockitoRule mocks = MockitoJUnit.rule();

  private static final String CLUSTER = "cluster-foo.googleapis.com";
  private static final String EDS_SERVICE_NAME = "backend-service-1.googleapis.com";
  private static final String DNS_HOST_NAME = "backend-service-dns.googleapis.com:443";
  private static final ServerInfo LRS_SERVER_INFO =
      ServerInfo.create("lrs.googleapis.com", InsecureChannelCredentials.create());
  private static final String SERVER_URI = "trafficdirector.googleapis.com";
  private static final String NODE_ID =
      "projects/42/networks/default/nodes/5c85b298-6f5b-4722-b74a-f7d1f0ccf5ad";
  private static final EnvoyProtoData.Node BOOTSTRAP_NODE =
      EnvoyProtoData.Node.newBuilder().setId(NODE_ID).build();
  private static final BootstrapInfo BOOTSTRAP_INFO = BootstrapInfo.builder()
      .servers(ImmutableList.of(
          ServerInfo.create(SERVER_URI, InsecureChannelCredentials.create())))
      .node(BOOTSTRAP_NODE)
      .build();
  private final UpstreamTlsContext upstreamTlsContext =
      CommonTlsContextTestsUtil.buildUpstreamTlsContext("google_cloud_private_spiffe", true);
  private final OutlierDetection outlierDetection = OutlierDetection.create(
      null, null, null, null, SuccessRateEjection.create(null, null, null, null), null);


  private static final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new RuntimeException(e);
          //throw new AssertionError(e);
        }
      });
  private final LoadBalancerRegistry lbRegistry = new LoadBalancerRegistry();
  private final List<FakeLoadBalancer> childBalancers = new ArrayList<>();
  private final FakeXdsClient xdsClient = new FakeXdsClient();
  private final ObjectPool<XdsClient> xdsClientPool = new ObjectPool<XdsClient>() {
    @Override
    public XdsClient getObject() {
      xdsClientRefs++;
      return xdsClient;
    }

    @Override
    public XdsClient returnObject(Object object) {
      xdsClientRefs--;
      return null;
    }
  };

  @Mock
  private Helper helper;
  @Captor
  private ArgumentCaptor<SubchannelPicker> pickerCaptor;

  private int xdsClientRefs;
  private CdsLoadBalancer2  loadBalancer;
  private TestXdsConfigWatcher configWatcher = new TestXdsConfigWatcher();
  private XdsConfig lastXdsConfig;

  @Before
  public void setUp() throws XdsResourceType.ResourceInvalidException, IOException {
    when(helper.getSynchronizationContext()).thenReturn(syncContext);
    when(helper.getNameResolverRegistry()).thenReturn(NameResolverRegistry.getDefaultRegistry());
    NameResolver.Args args = NameResolver.Args.newBuilder()
        .setDefaultPort(8080)
        .setProxyDetector(GrpcUtil.NOOP_PROXY_DETECTOR)
        .setSynchronizationContext(syncContext)
        .setServiceConfigParser(mock(NameResolver.ServiceConfigParser.class))
        .setChannelLogger(mock(ChannelLogger.class))
        .build();
    when(helper.getNameResolverArgs()).thenReturn(args);

    lbRegistry.register(new FakeLoadBalancerProvider(CLUSTER_RESOLVER_POLICY_NAME));
    lbRegistry.register(new FakeLoadBalancerProvider(CLUSTER_IMPL_POLICY_NAME));
    lbRegistry.register(new FakeLoadBalancerProvider(PRIORITY_POLICY_NAME));

    lbRegistry.register(new FakeLoadBalancerProvider("round_robin"));
    lbRegistry.register(new FakeLoadBalancerProvider("outlier_detection_experimental"));
    lbRegistry.register(
        new FakeLoadBalancerProvider("ring_hash_experimental", new RingHashLoadBalancerProvider()));
    lbRegistry.register(new FakeLoadBalancerProvider("least_request_experimental",
        new LeastRequestLoadBalancerProvider()));


    loadBalancer =
        new CdsLoadBalancer2(helper, lbRegistry, new ExponentialBackoffPolicy.Provider());

    lastXdsConfig = getDefaultXdsConfig();

    // Setup default configuration for the CdsLoadBalancer2
    XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
        CLUSTER, EDS_SERVICE_NAME, null, null, null, null)
        .roundRobinLbPolicy().build();

    xdsClient.deliverCdsUpdate(CLUSTER, cdsUpdate);
    xdsClient.createAndDeliverEdsUpdate(EDS_SERVICE_NAME);

//    loadBalancer.acceptResolvedAddresses(
//        ResolvedAddresses.newBuilder()
//            .setAddresses(Collections.<EquivalentAddressGroup>emptyList())
//            .setAttributes(
//                // Other attributes not used by cluster_resolver LB are omitted.
//                Attributes.newBuilder()
//                    .set(XdsAttributes.XDS_CONFIG, lastXdsConfig)
//                    .build())
//            .setLoadBalancingPolicyConfig(new CdsConfig(CLUSTER))
//            .build());
  }

  static XdsConfig getDefaultXdsConfig()
      throws XdsResourceType.ResourceInvalidException {
    XdsConfig.XdsConfigBuilder builder = new XdsConfig.XdsConfigBuilder();

    XdsListenerResource.LdsUpdate ldsUpdate = buildDefaultLdsUpdate();

    XdsRouteConfigureResource.RdsUpdate rdsUpdate = buildDefaultRdsUpdate();

    // Take advantage of knowing that there is only 1 virtual host in the route configuration
    assertThat(rdsUpdate.virtualHosts).hasSize(1);
    VirtualHost virtualHost = rdsUpdate.virtualHosts.get(0);

    // Need to create endpoints to create locality endpoints map to create edsUpdate
    Map<Locality, Endpoints.LocalityLbEndpoints> lbEndpointsMap =
        XdsTestUtils.createMinimalLbEndpointsMap(EDS_SERVICE_NAME);

    ImmutableMap.Builder<String, Object> configBuilder = ImmutableMap.builder();
    configBuilder.put("name", CLUSTER);

    // Need to create EdsUpdate to create CdsUpdate to create XdsClusterConfig for builder
    EdsUpdate edsUpdate = new EdsUpdate(
        EDS_SERVICE_NAME, lbEndpointsMap, Collections.emptyList());
    XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
            CLUSTER, EDS_SERVICE_NAME, null, null, null, null)
        .roundRobinLbPolicy().build();
    XdsConfig.XdsClusterConfig clusterConfig = new XdsConfig.XdsClusterConfig(
        CLUSTER, cdsUpdate, StatusOr.fromValue(edsUpdate));

    builder
        .setListener(ldsUpdate)
        .setRoute(rdsUpdate)
        .setVirtualHost(virtualHost)
        .addCluster(CLUSTER, StatusOr.fromValue(clusterConfig));

    return builder.build();
  }

  private static XdsRouteConfigureResource.RdsUpdate buildDefaultRdsUpdate()  {
    RouteConfiguration routeConfiguration =
        XdsTestUtils.buildRouteConfiguration(EDS_SERVICE_NAME, RDS_NAME, CLUSTER);
    XdsResourceType.Args args = new XdsResourceType.Args(null, "0", "0", null, null, null);
    XdsRouteConfigureResource.RdsUpdate rdsUpdate;
    try {
      rdsUpdate = XdsRouteConfigureResource.getInstance().doParse(args, routeConfiguration);
    } catch (XdsResourceType.ResourceInvalidException e) {
      throw new RuntimeException(e);
    }
    return rdsUpdate;
  }

  private static XdsListenerResource.LdsUpdate buildDefaultLdsUpdate() {
    Filter.NamedFilterConfig routerFilterConfig = new Filter.NamedFilterConfig(
        EDS_SERVICE_NAME, RouterFilter.ROUTER_CONFIG);

    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forRdsName(
        0L, RDS_NAME, Collections.singletonList(routerFilterConfig));
    XdsListenerResource.LdsUpdate ldsUpdate =
        XdsListenerResource.LdsUpdate.forApiListener(httpConnectionManager, "");
    return ldsUpdate;
  }

  @After
  public void tearDown() {
    loadBalancer.shutdown();
    configWatcher.cleanup();

    assertThat(xdsClient.watchers).isEmpty();
    assertThat(xdsClientRefs).isEqualTo(0);
    assertThat(childBalancers).isEmpty();

  }

  @Test
  //TODO: Code looks broken creating a second LB instead of updating the existing one or shutting it down
  public void discoverTopLevelEdsCluster() {
    configWatcher.watchCluster(CLUSTER);
    CdsUpdate update =
        CdsUpdate.forEds(CLUSTER, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L, upstreamTlsContext,
                outlierDetection)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(childBalancers).hasSize(1);
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);

    validateClusterImplConfig(getClusterImplConfig(childBalancers, CLUSTER), CLUSTER, EDS_SERVICE_NAME,
        null, LRS_SERVER_INFO, 100L, upstreamTlsContext, outlierDetection);

    PriorityLbConfig.PriorityChildConfig priorityChildConfig =
        getPriorityChildConfig(childBalancers, CLUSTER);
    assertThat(getChildProvider(priorityChildConfig.childConfig)
        .getPolicyName()).isEqualTo("round_robin");
  }

  private static ClusterImplConfig getClusterImplConfig(List<FakeLoadBalancer> childBalancers,
                                                        String cluster) {
    PriorityLbConfig.PriorityChildConfig priorityChildConfig =
        getPriorityChildConfig(childBalancers, cluster);
    assertNotNull("No cluster " + cluster + " in childBalancers", priorityChildConfig);
    Object clusterImplConfig = getChildConfig(priorityChildConfig.childConfig);
    if (clusterImplConfig instanceof ClusterImplConfig) {
      return (ClusterImplConfig) clusterImplConfig;
    }
    if (clusterImplConfig instanceof OutlierDetectionLoadBalancerConfig) {
      clusterImplConfig = getChildConfig(((OutlierDetectionLoadBalancerConfig) clusterImplConfig).childConfig);
    }
    
    assertThat(clusterImplConfig).isInstanceOf(ClusterImplConfig.class);
    return (ClusterImplConfig) clusterImplConfig;
  }

  private static PriorityLbConfig.PriorityChildConfig getPriorityChildConfig(List<FakeLoadBalancer> childBalancers, String cluster) {
    for (FakeLoadBalancer fakeLB : childBalancers) {
      if (fakeLB.config instanceof PriorityLbConfig) {
        Map<String, PriorityLbConfig.PriorityChildConfig> childConfigs =
            ((PriorityLbConfig) fakeLB.config).childConfigs;
        // keys have [xxx] appended to the cluster name
        for (String key : childConfigs.keySet()) {
          int indexOf = key.indexOf('[');
          if (indexOf != -1 && key.substring(0, indexOf).equals(cluster)) {
            return childConfigs.get(key);
          }
        }
      }
    }
    return null;
  }

  private FakeLoadBalancer getFakeLoadBalancer(List<FakeLoadBalancer> childBalancers, String cluster) {
    for (FakeLoadBalancer fakeLB : childBalancers) {
      if (fakeLB.config instanceof PriorityLbConfig) {
        Map<String, PriorityLbConfig.PriorityChildConfig> childConfigs =
            ((PriorityLbConfig) fakeLB.config).childConfigs;
        // keys have [xxx] appended to the cluster name
        for (String key : childConfigs.keySet()) {
          int indexOf = key.indexOf('[');
          if (indexOf != -1 && key.substring(0, indexOf).equals(cluster)) {
            return fakeLB;
          }
        }
      }
    }
    return null;
  }

  @Test
  // TODO Fix whatever needs it
  public void discoverTopLevelLogicalDnsCluster() {
    CdsUpdate update =
        CdsUpdate.forLogicalDns(CLUSTER, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, upstreamTlsContext)
            .leastRequestLbPolicy(3).build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(childBalancers).hasSize(1);
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    assertThat(childBalancer.name).isEqualTo(CLUSTER_RESOLVER_POLICY_NAME);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    assertThat(childLbConfig.discoveryMechanisms).hasSize(1);
    DiscoveryMechanism instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, CLUSTER, null,
        DNS_HOST_NAME, LRS_SERVER_INFO, 100L, upstreamTlsContext, null);
    assertThat(
        getChildProvider(childLbConfig.lbConfig).getPolicyName())
        .isEqualTo("least_request_experimental");
    LeastRequestConfig lrConfig = (LeastRequestConfig)
        getChildConfig(childLbConfig.lbConfig);
    assertThat(lrConfig.choiceCount).isEqualTo(3);
  }

  @Test
  // TODO why isn't the NODE_ID part of the error message?
  public void nonAggregateCluster_resourceNotExist_returnErrorPicker() {
    xdsClient.deliverResourceNotExist(CLUSTER);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
    assertThat(childBalancers).isEmpty();
  }

  @Test
  // TODO: Update to use DependencyManager
  public void nonAggregateCluster_resourceUpdate() {
    CdsUpdate update =
        CdsUpdate.forEds(CLUSTER, null, null, 100L, upstreamTlsContext, outlierDetection)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(childBalancers).hasSize(1);
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    DiscoveryMechanism instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, CLUSTER, null, null, null,
        100L, upstreamTlsContext, outlierDetection);

    update = CdsUpdate.forEds(CLUSTER, EDS_SERVICE_NAME, LRS_SERVER_INFO, 200L, null,
        outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    childLbConfig = (ClusterResolverConfig) childBalancer.config;
    instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, CLUSTER, EDS_SERVICE_NAME,
        null, LRS_SERVER_INFO, 200L, null, outlierDetection);
  }

  @Test
  // TODO: Switch to looking for expected structure from DependencyManager
  public void nonAggregateCluster_resourceRevoked() {
    CdsUpdate update =
        CdsUpdate.forLogicalDns(CLUSTER, DNS_HOST_NAME, null, 100L, upstreamTlsContext)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(childBalancers).hasSize(1);
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    DiscoveryMechanism instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, CLUSTER, null,
        DNS_HOST_NAME, null, 100L, upstreamTlsContext, null);

    xdsClient.deliverResourceNotExist(CLUSTER);
    assertThat(childBalancer.shutdown).isTrue();
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    assertPicker(pickerCaptor.getValue(), unavailable, null);
    assertThat(childBalancer.shutdown).isTrue();
    assertThat(childBalancers).isEmpty();
  }

  @Test
  // @TODO: Fix this test
  public void discoverAggregateCluster() {
    String cluster1 = "cluster-01.googleapis.com";
    String cluster2 = "cluster-02.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1 (aggr.), cluster2 (logical DNS)]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Arrays.asList(cluster1, cluster2))
            .ringHashLbPolicy(100L, 1000L).build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2);
    assertThat(childBalancers).isEmpty();
    String cluster3 = "cluster-03.googleapis.com";
    String cluster4 = "cluster-04.googleapis.com";
    // cluster1 (aggr.) -> [cluster3 (EDS), cluster4 (EDS)]
    CdsUpdate update1 =
        CdsUpdate.forAggregate(cluster1, Arrays.asList(cluster3, cluster4))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update1);
    assertThat(xdsClient.watchers.keySet()).containsExactly(
        CLUSTER, cluster1, cluster2, cluster3, cluster4);
    assertThat(childBalancers).isEmpty();
    CdsUpdate update3 = CdsUpdate.forEds(cluster3, EDS_SERVICE_NAME, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster3, update3);
    assertThat(childBalancers).isEmpty();
    CdsUpdate update2 =
        CdsUpdate.forLogicalDns(cluster2, DNS_HOST_NAME, null, 100L, null)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);
    assertThat(childBalancers).isEmpty();
    CdsUpdate update4 =
        CdsUpdate.forEds(cluster4, null, LRS_SERVER_INFO, 300L, null, outlierDetection)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster4, update4);
    assertThat(childBalancers).hasSize(1);  // all non-aggregate clusters discovered
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    assertThat(childBalancer.name).isEqualTo(CLUSTER_RESOLVER_POLICY_NAME);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    assertThat(childLbConfig.discoveryMechanisms).hasSize(3);
    // Clusters on higher level has higher priority: [cluster2, cluster3, cluster4]
    validateDiscoveryMechanism(childLbConfig.discoveryMechanisms.get(0), cluster2,
        null, DNS_HOST_NAME, null, 100L, null, null);
    validateDiscoveryMechanism(childLbConfig.discoveryMechanisms.get(1), cluster3,
        EDS_SERVICE_NAME, null, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection);
    validateDiscoveryMechanism(childLbConfig.discoveryMechanisms.get(2), cluster4,
        null, null, LRS_SERVER_INFO, 300L, null, outlierDetection);
    assertThat(getChildProvider(childLbConfig.lbConfig).getPolicyName())
        .isEqualTo("ring_hash_experimental");  // dominated by top-level cluster's config
    RingHashConfig ringHashConfig = (RingHashConfig) getChildConfig(childLbConfig.lbConfig);
    assertThat(ringHashConfig.minRingSize).isEqualTo(100L);
    assertThat(ringHashConfig.maxRingSize).isEqualTo(1000L);
  }

  @Test
  public void aggregateCluster_noNonAggregateClusterExits_returnErrorPicker() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1 (EDS)]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);
    xdsClient.deliverResourceNotExist(cluster1);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
    assertThat(childBalancers).isEmpty();
  }

  @Test
  public void aggregateCluster_descendantClustersRevoked() throws IOException {
    String cluster1 = "cluster-01.googleapis.com";
    String cluster2 = "cluster-02.googleapis.com";

    Closeable cluster1Watcher = configWatcher.watchCluster(cluster1);
    Closeable cluster2Watcher = configWatcher.watchCluster(cluster2);

    // CLUSTER (aggr.) -> [cluster1 (EDS), cluster2 (logical DNS)]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Arrays.asList(cluster1, cluster2))
            .roundRobinLbPolicy().build();

    xdsClient.deliverCdsUpdate(CLUSTER, update);
    CdsUpdate update1 = CdsUpdate.forEds(cluster1, EDS_SERVICE_NAME, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update1);
    CdsUpdate update2 =
        CdsUpdate.forLogicalDns(cluster2, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, null)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);
    xdsClient.createAndDeliverEdsUpdate(update1.edsServiceName());

    validateClusterImplConfig(getClusterImplConfig(childBalancers, cluster1), cluster1,
        EDS_SERVICE_NAME, null, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection);
    validateClusterImplConfig(getClusterImplConfig(childBalancers, cluster2), cluster2,
        null, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, null,
        null);

    // Revoke cluster1, should still be able to proceed with cluster2.
    xdsClient.deliverResourceNotExist(cluster1);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2);
    validateClusterImplConfig(getClusterImplConfig(childBalancers, CLUSTER),
        cluster2, null, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, null, null);
    verify(helper, never()).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), any(SubchannelPicker.class));

    // All revoked.
    xdsClient.deliverResourceNotExist(cluster2);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);

    String cluster = cluster1;
    FakeLoadBalancer childBalancer = null;
    for (FakeLoadBalancer fakeLB : childBalancers) {
      if (!(fakeLB.config instanceof PriorityLbConfig)) {
        continue;
      }
      Map<String, PriorityLbConfig.PriorityChildConfig> childConfigs =
          ((PriorityLbConfig) fakeLB.config).childConfigs;
      if (childConfigs.containsKey(cluster)) {
        childBalancer = fakeLB;
        break;
      }
    }

    assertNotNull("No balancer named " + cluster + "exists", childBalancer);
    assertThat(childBalancer.shutdown).isTrue();
    assertThat(childBalancers).isEmpty();

    cluster1Watcher.close();
    cluster2Watcher.close();
  }

  @Test
  public void aggregateCluster_rootClusterRevoked() {
    String cluster1 = "cluster-01.googleapis.com";
    String cluster2 = "cluster-02.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1 (EDS), cluster2 (logical DNS)]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Arrays.asList(cluster1, cluster2))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2);
    CdsUpdate update1 = CdsUpdate.forEds(cluster1, EDS_SERVICE_NAME, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update1);
    CdsUpdate update2 =
        CdsUpdate.forLogicalDns(cluster2, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, null)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);

    // TODO: fix the check
    assertThat("I am").isEqualTo("not done");

    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    assertThat(childLbConfig.discoveryMechanisms).hasSize(2);
    validateDiscoveryMechanism(childLbConfig.discoveryMechanisms.get(0), cluster1,
        EDS_SERVICE_NAME, null, LRS_SERVER_INFO, 200L,
        upstreamTlsContext, outlierDetection);
    validateDiscoveryMechanism(childLbConfig.discoveryMechanisms.get(1), cluster2,
        null, DNS_HOST_NAME, LRS_SERVER_INFO, 100L, null,
        null);

    xdsClient.deliverResourceNotExist(CLUSTER);
    assertThat(xdsClient.watchers.keySet())
        .containsExactly(CLUSTER);  // subscription to all descendant clusters cancelled
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
    assertThat(childBalancer.shutdown).isTrue();
    assertThat(childBalancers).isEmpty();
  }

  @Test
  public void aggregateCluster_intermediateClusterChanges() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);

    // CLUSTER (aggr.) -> [cluster2 (aggr.)]
    String cluster2 = "cluster-02.googleapis.com";
    update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster2))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster2);

    // cluster2 (aggr.) -> [cluster3 (EDS)]
    String cluster3 = "cluster-03.googleapis.com";
    CdsUpdate update2 =
        CdsUpdate.forAggregate(cluster2, Collections.singletonList(cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster2, cluster3);
    CdsUpdate update3 = CdsUpdate.forEds(cluster3, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster3, update3);

    // TODO: fix the check
    assertThat("I am").isEqualTo("not done");

    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    assertThat(childLbConfig.discoveryMechanisms).hasSize(1);
    DiscoveryMechanism instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, cluster3, EDS_SERVICE_NAME,
        null, LRS_SERVER_INFO, 100L, upstreamTlsContext, outlierDetection);

    // cluster2 revoked
    xdsClient.deliverResourceNotExist(cluster2);
    assertThat(xdsClient.watchers.keySet())
        .containsExactly(CLUSTER, cluster2);  // cancelled subscription to cluster3
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: found 0 leaf (logical DNS or EDS) clusters for root cluster " + CLUSTER
            + " xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
    assertThat(childBalancer.shutdown).isTrue();
    assertThat(childBalancers).isEmpty();
  }

  @Test
  @Ignore // TODO: handle loop detection
  public void aggregateCluster_withLoops() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);

    // CLUSTER (aggr.) -> [cluster2 (aggr.)]
    String cluster2 = "cluster-02.googleapis.com";
    update =
        CdsUpdate.forAggregate(cluster1, Collections.singletonList(cluster2))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2);

    // cluster2 (aggr.) -> [cluster3 (EDS), cluster1 (parent), cluster2 (self), cluster3 (dup)]
    String cluster3 = "cluster-03.googleapis.com";
    CdsUpdate update2 =
        CdsUpdate.forAggregate(cluster2, Arrays.asList(cluster3, cluster1, cluster2, cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2, cluster3);

    reset(helper);
    CdsUpdate update3 = CdsUpdate.forEds(cluster3, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster3, update3);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: circular aggregate clusters directly under cluster-02.googleapis.com for root"
            + " cluster cluster-foo.googleapis.com, named [cluster-01.googleapis.com,"
            + " cluster-02.googleapis.com], xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
  }

  @Test
  // TODO: Currently errors with no leafs under CLUSTER, so doesn't actually check what we want
  public void aggregateCluster_withLoops_afterEds() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);

    // CLUSTER (aggr.) -> [cluster2 (aggr.)]
    String cluster2 = "cluster-02.googleapis.com";
    update =
        CdsUpdate.forAggregate(cluster1, Collections.singletonList(cluster2))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2);

    String cluster3 = "cluster-03.googleapis.com";
    CdsUpdate update2 =
        CdsUpdate.forAggregate(cluster2, Arrays.asList(cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);
    CdsUpdate update3 = CdsUpdate.forEds(cluster3, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster3, update3);

    // cluster2 (aggr.) -> [cluster3 (EDS)]
    CdsUpdate update2a =
        CdsUpdate.forAggregate(cluster2, Arrays.asList(cluster3, cluster1, cluster2, cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2a);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1, cluster2, cluster3);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status unavailable = Status.UNAVAILABLE.withDescription(
        "CDS error: circular aggregate clusters directly under cluster-02.googleapis.com for root"
            + " cluster cluster-foo.googleapis.com, named [cluster-01.googleapis.com,"
            + " cluster-02.googleapis.com], xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), unavailable, null);
  }

  @Test
  public void aggregateCluster_duplicateChildren() {
    String cluster1 = "cluster-01.googleapis.com";
    String cluster2 = "cluster-02.googleapis.com";
    String cluster3 = "cluster-03.googleapis.com";
    String cluster4 = "cluster-04.googleapis.com";

    // CLUSTER (aggr.) -> [cluster1]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);

    // cluster1 (aggr) -> [cluster3 (EDS), cluster2 (aggr), cluster4 (aggr)]
    CdsUpdate update1 =
        CdsUpdate.forAggregate(cluster1, Arrays.asList(cluster3, cluster2, cluster4, cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update1);
    assertThat(xdsClient.watchers.keySet()).containsExactly(
        cluster3, cluster4, cluster2, cluster1, CLUSTER);
    xdsClient.watchers.values().forEach(list -> assertThat(list.size()).isEqualTo(1));

    // cluster2 (agg) -> [cluster3 (EDS), cluster4 {agg}] with dups
    CdsUpdate update2 =
        CdsUpdate.forAggregate(cluster2, Arrays.asList(cluster3, cluster4, cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster2, update2);

    // Define EDS cluster
    CdsUpdate update3 = CdsUpdate.forEds(cluster3, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster3, update3);

    // cluster4 (agg) -> [cluster3 (EDS)] with dups (3 copies)
    CdsUpdate update4 =
        CdsUpdate.forAggregate(cluster4, Arrays.asList(cluster3, cluster3, cluster3))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster4, update4);
    xdsClient.watchers.values().forEach(list -> assertThat(list.size()).isEqualTo(1));

    // TODO: fix the check
    assertThat("I am").isEqualTo("not done");
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    ClusterResolverConfig childLbConfig = (ClusterResolverConfig) childBalancer.config;
    assertThat(childLbConfig.discoveryMechanisms).hasSize(1);
    DiscoveryMechanism instance = Iterables.getOnlyElement(childLbConfig.discoveryMechanisms);
    validateDiscoveryMechanism(instance, cluster3, EDS_SERVICE_NAME,
        null, LRS_SERVER_INFO, 100L, upstreamTlsContext, outlierDetection);
  }

  @Test
  public void aggregateCluster_discoveryErrorBeforeChildLbCreated_returnErrorPicker() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    assertThat(xdsClient.watchers.keySet()).containsExactly(CLUSTER, cluster1);
    Status error = Status.RESOURCE_EXHAUSTED.withDescription("OOM");
    xdsClient.deliverError(error);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    Status expectedError = Status.UNAVAILABLE.withDescription(
        "Unable to load CDS cluster-foo.googleapis.com. xDS server returned: "
        + "RESOURCE_EXHAUSTED: OOM xDS node ID: " + NODE_ID);
    assertPicker(pickerCaptor.getValue(), expectedError, null);
    assertThat(childBalancers).isEmpty();
  }

  @Test
  @Ignore
  // TODO: Needs to be reworked as XdsDependencyManager grabs CDS errors and they show in XdsConfig
  public void aggregateCluster_discoveryErrorAfterChildLbCreated_propagateToChildLb() {
    String cluster1 = "cluster-01.googleapis.com";
    // CLUSTER (aggr.) -> [cluster1 (logical DNS)]
    CdsUpdate update =
        CdsUpdate.forAggregate(CLUSTER, Collections.singletonList(cluster1))
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    CdsUpdate update1 =
        CdsUpdate.forLogicalDns(cluster1, DNS_HOST_NAME, LRS_SERVER_INFO, 200L, null)
            .roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(cluster1, update1);
    FakeLoadBalancer childLb = getFakeLoadBalancer(childBalancers, CLUSTER);

    Status error = Status.RESOURCE_EXHAUSTED.withDescription("OOM");
    xdsClient.deliverError(error);
    assertThat(childLb.upstreamError.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(childLb.upstreamError.getDescription()).contains("RESOURCE_EXHAUSTED: OOM");
    assertThat(childLb.shutdown).isFalse();  // child LB may choose to keep working
  }

  @Test
  // TODO anaylyze why we are getting CONNECTING instead of TF
  public void handleNameResolutionErrorFromUpstream_beforeChildLbCreated_returnErrorPicker() {
    Status upstreamError = Status.UNAVAILABLE.withDescription(
        "unreachable xDS node ID: " + NODE_ID);
    loadBalancer.handleNameResolutionError(upstreamError);
    verify(helper).updateBalancingState(
        eq(ConnectivityState.TRANSIENT_FAILURE), pickerCaptor.capture());
    assertPicker(pickerCaptor.getValue(), upstreamError, null);
  }

  @Test
  // TODO: same error as above
  public void handleNameResolutionErrorFromUpstream_afterChildLbCreated_fallThrough() {
    CdsUpdate update = CdsUpdate.forEds(CLUSTER, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L,
        upstreamTlsContext, outlierDetection).roundRobinLbPolicy().build();
    xdsClient.deliverCdsUpdate(CLUSTER, update);
    FakeLoadBalancer childBalancer = Iterables.getOnlyElement(childBalancers);
    assertThat(childBalancer.shutdown).isFalse();
    loadBalancer.handleNameResolutionError(Status.UNAVAILABLE.withDescription("unreachable"));
    assertThat(childBalancer.upstreamError.getCode()).isEqualTo(Code.UNAVAILABLE);
    assertThat(childBalancer.upstreamError.getDescription()).isEqualTo("unreachable");
    verify(helper, never()).updateBalancingState(
        any(ConnectivityState.class), any(SubchannelPicker.class));
  }

  @Test
  // TODO: figure out what is going on
  public void unknownLbProvider() {
    try {
      xdsClient.deliverCdsUpdate(CLUSTER,
          CdsUpdate.forEds(CLUSTER, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L, upstreamTlsContext,
                  outlierDetection)
              .lbPolicyConfig(ImmutableMap.of("unknownLb", ImmutableMap.of("foo", "bar"))).build());
    } catch (Exception e) {
      assertThat(e).hasMessageThat().contains("unknownLb");
      return;
    }
    fail("Expected the unknown LB to cause an exception");
  }

  @Test
  // TODO Fix whatever needs it
  public void invalidLbConfig() {
    try {
      xdsClient.deliverCdsUpdate(CLUSTER,
          CdsUpdate.forEds(CLUSTER, EDS_SERVICE_NAME, LRS_SERVER_INFO, 100L, upstreamTlsContext,
                  outlierDetection).lbPolicyConfig(
                  ImmutableMap.of("ring_hash_experimental", ImmutableMap.of("minRingSize", "-1")))
              .build());
    } catch (Exception e) {
      assertThat(e).hasMessageThat().contains("Unable to parse");
      return;
    }
    fail("Expected the invalid config to cause an exception");
  }

  private static void assertPicker(SubchannelPicker picker, Status expectedStatus,
      @Nullable Subchannel expectedSubchannel)  {
    PickResult result = picker.pickSubchannel(mock(PickSubchannelArgs.class));
    Status actualStatus = result.getStatus();
    assertThat(actualStatus.getCode()).isEqualTo(expectedStatus.getCode());
    assertThat(actualStatus.getDescription()).isEqualTo(expectedStatus.getDescription());
    if (actualStatus.isOk()) {
      assertThat(result.getSubchannel()).isSameInstanceAs(expectedSubchannel);
    }
  }

  private static void validateDiscoveryMechanism(
      DiscoveryMechanism instance, String name,
      @Nullable String edsServiceName, @Nullable String dnsHostName,
      @Nullable ServerInfo lrsServerInfo, @Nullable Long maxConcurrentRequests,
      @Nullable UpstreamTlsContext tlsContext, @Nullable OutlierDetection outlierDetection) {
    assertThat(instance.cluster).isEqualTo(name);
    assertThat(instance.edsServiceName).isEqualTo(edsServiceName);
//    assertThat(instance.dnsHostName).isEqualTo(dnsHostName);
    assertThat(instance.lrsServerInfo).isEqualTo(lrsServerInfo);
    assertThat(instance.maxConcurrentRequests).isEqualTo(maxConcurrentRequests);
    assertThat(instance.tlsContext).isEqualTo(tlsContext);
//    assertThat(instance.outlierDetection).isEqualTo(outlierDetection);
  }

  private static void validateClusterImplConfig(
      ClusterImplConfig instance, String name,
      @Nullable String edsServiceName, @Nullable String dnsHostName,
      @Nullable ServerInfo lrsServerInfo, @Nullable Long maxConcurrentRequests,
      @Nullable UpstreamTlsContext tlsContext, @Nullable OutlierDetection outlierDetection) {
    assertThat(instance.cluster).isEqualTo(name);
    assertThat(instance.edsServiceName).isEqualTo(edsServiceName);
//    assertThat(instance.dnsHostName).isEqualTo(dnsHostName);
    assertThat(instance.lrsServerInfo).isEqualTo(lrsServerInfo);
    assertThat(instance.maxConcurrentRequests).isEqualTo(maxConcurrentRequests);
    assertThat(instance.tlsContext).isEqualTo(tlsContext);
//    assertThat(instance.outlierDetection).isEqualTo(outlierDetection);
  }

  private final class FakeLoadBalancerProvider extends LoadBalancerProvider {
    private final String policyName;
    private final LoadBalancerProvider configParsingDelegate;

    FakeLoadBalancerProvider(String policyName) {
      this(policyName, null);
    }

    FakeLoadBalancerProvider(String policyName, LoadBalancerProvider configParsingDelegate) {
      this.policyName = policyName;
      this.configParsingDelegate = configParsingDelegate;
    }

    @Override
    public LoadBalancer newLoadBalancer(Helper helper) {
      FakeLoadBalancer balancer = new FakeLoadBalancer(policyName);
      childBalancers.add(balancer);
      return balancer;
    }

    @Override
    public boolean isAvailable() {
      return true;
    }

    @Override
    public int getPriority() {
      return 0;  // doesn't matter
    }

    @Override
    public String getPolicyName() {
      return policyName;
    }

    @Override
    public NameResolver.ConfigOrError parseLoadBalancingPolicyConfig(
        Map<String, ?> rawLoadBalancingPolicyConfig) {
      if (configParsingDelegate != null) {
        return configParsingDelegate.parseLoadBalancingPolicyConfig(rawLoadBalancingPolicyConfig);
      }
      return super.parseLoadBalancingPolicyConfig(rawLoadBalancingPolicyConfig);
    }
  }

  private final class FakeLoadBalancer extends LoadBalancer {
    private final String name;
    private Object config;
    private Status upstreamError;
    private boolean shutdown;

    FakeLoadBalancer(String name) {
      this.name = name;
    }

    @Override
    public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
      config = resolvedAddresses.getLoadBalancingPolicyConfig();
    }

    @Override
    public void handleNameResolutionError(Status error) {
      upstreamError = error;
    }

    @Override
    public void shutdown() {
      shutdown = true;
      childBalancers.remove(this);
    }
  }

  private static final class FakeXdsClient extends XdsClient {
    // watchers needs to support any non-cyclic shaped graphs
    private final Map<String, List<ResourceWatcher<CdsUpdate>>> watchers = new HashMap<>();
    private final Map<String, List<ResourceWatcher<EdsUpdate>>> edsWatchers = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ResourceUpdate> void watchXdsResource(XdsResourceType<T> type,
        String resourceName,
        ResourceWatcher<T> watcher, Executor syncContext) {
      switch (type.typeName()) {
        case "CDS":
          watchers.computeIfAbsent(resourceName, k -> new ArrayList<>())
              .add((ResourceWatcher<CdsUpdate>) watcher);
          break;
        case "LDS":
          syncContext.execute(() -> watcher.onChanged((T) buildDefaultLdsUpdate()));
          break;
        case "RDS":
          syncContext.execute(() -> watcher.onChanged((T) buildDefaultRdsUpdate()));
          break;
        case "EDS":
          edsWatchers.computeIfAbsent(resourceName, k -> new ArrayList<>())
              .add((ResourceWatcher<EdsUpdate>) watcher);
          break;
        default:
          throw new AssertionError("Unsupported resource type: " + type.typeName());
      }
    }

    @Override
    public <T extends ResourceUpdate> void cancelXdsResourceWatch(XdsResourceType<T> type,
                                                                  String resourceName,
                                                                  ResourceWatcher<T> watcher) {
      switch (type.typeName()) {
        case "CDS":
          assertThat(watchers).containsKey(resourceName);
          List<ResourceWatcher<CdsUpdate>> watcherList = watchers.get(resourceName);
          assertThat(watcherList.remove(watcher)).isTrue();
          if (watcherList.isEmpty()) {
            watchers.remove(resourceName);
          }
          break;
          case "EDS":
            assertThat(edsWatchers).containsKey(resourceName);
            List<ResourceWatcher<EdsUpdate>> edsWatcherList = edsWatchers.get(resourceName);
            assertThat(edsWatcherList.remove(watcher)).isTrue();
            if (edsWatcherList.isEmpty()) {
              edsWatchers.remove(resourceName);
            }
            break;
        default:
          // ignore for other types
      }
    }

    @Override
    public BootstrapInfo getBootstrapInfo() {
      return BOOTSTRAP_INFO;
    }

    private void deliverCdsUpdate(String clusterName, CdsUpdate update) {
      if (!watchers.containsKey(clusterName)) {
        return;
      }
      List<ResourceWatcher<CdsUpdate>> resourceWatchers =
          ImmutableList.copyOf(watchers.get(clusterName));
      syncContext.execute(() -> resourceWatchers.forEach(w -> w.onChanged(update)));
    }

    private void createAndDeliverEdsUpdate(String edsName) {
      if (edsWatchers == null || !edsWatchers.containsKey(edsName)) {
        return;
      }

      List<ResourceWatcher<EdsUpdate>> resourceWatchers =
          ImmutableList.copyOf(edsWatchers.get(edsName));
      EdsUpdate edsUpdate = new EdsUpdate(edsName,
          XdsTestUtils.createMinimalLbEndpointsMap("host"), Collections.emptyList());
      syncContext.execute(() -> resourceWatchers.forEach(w -> w.onChanged(edsUpdate)));
    }

    private void deliverResourceNotExist(String clusterName)  {
      if (watchers.containsKey(clusterName)) {
        syncContext.execute(() -> {
          ImmutableList.copyOf(watchers.get(clusterName))
              .forEach(w -> w.onResourceDoesNotExist(clusterName));
        });
      }
    }

    private void deliverError(Status error) {
      syncContext.execute(() -> {
        watchers.values().stream()
            .flatMap(List::stream)
            .forEach(w -> w.onError(error));
      });
    }
  }

  private class TestXdsConfigWatcher implements XdsDependencyManager.XdsConfigWatcher {
    XdsDependencyManager dependencyManager;
    List<java.io.Closeable> clusterWatchers = new ArrayList<>();

    public TestXdsConfigWatcher() {
      dependencyManager = new XdsDependencyManager(xdsClient, this, syncContext, EDS_SERVICE_NAME, "" );
    }

    public Closeable watchCluster(String clusterName) {
      Closeable watcher = dependencyManager.subscribeToCluster(clusterName);
      clusterWatchers.add(watcher);
      return watcher;
    }

    public void cleanup() {
      for (Closeable w : clusterWatchers) {
        try {
          w.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      clusterWatchers.clear();
    }

    @Override
    public void onUpdate(XdsConfig xdsConfig) {
      if (loadBalancer == null) { // shouldn't happen outside of tests
        return;
      }

      // Build ResolvedAddresses from the config

      ResolvedAddresses.Builder raBuilder = ResolvedAddresses.newBuilder()
          .setLoadBalancingPolicyConfig(buildLbConfig(xdsConfig))
          .setAttributes(Attributes.newBuilder()
              .set(XdsAttributes.XDS_CONFIG, xdsConfig)
              .build())
          .setAddresses(buildEags(xdsConfig));

      // call loadBalancer.acceptResolvedAddresses() to update the config
      Status status = loadBalancer.acceptResolvedAddresses(raBuilder.build());
      if (!status.isOk()) {
        logger.log(XdsLogLevel.DEBUG, "acceptResolvedAddresses failed with %s", status);
      }
    }

    private List<EquivalentAddressGroup> buildEags(XdsConfig xdsConfig) {
      List<EquivalentAddressGroup> eags = new ArrayList<>();
      if (xdsConfig.getVirtualHost() == null || xdsConfig.getVirtualHost().routes() == null) {
        return eags;
      }

      for (VirtualHost.Route route : xdsConfig.getVirtualHost().routes()) {
        StatusOr<XdsConfig.XdsClusterConfig> configStatusOr =
            xdsConfig.getClusters().get(route.routeAction().cluster());
        if (configStatusOr == null || !configStatusOr.hasValue()) {
          continue;
        }
        XdsConfig.XdsClusterConfig clusterConfig = configStatusOr.getValue();
        eags.addAll(buildEagsForCluster(clusterConfig, xdsConfig));
        buildEagsForCluster(clusterConfig, xdsConfig);
      }
      return eags;
    }

    private List<EquivalentAddressGroup> buildEagsForCluster(
        XdsConfig.XdsClusterConfig clusterConfig, XdsConfig xdsConfig) {
      CdsUpdate clusterResource = clusterConfig.getClusterResource();
      switch (clusterResource.clusterType()) {
        case EDS:
          if (clusterConfig.getEndpoint().getValue() == null) {
            return Collections.emptyList();
          }
          return clusterConfig.getEndpoint().getValue().localityLbEndpointsMap.values().stream()
              .flatMap(localityLbEndpoints -> localityLbEndpoints.endpoints().stream())
              .map(Endpoints.LbEndpoint::eag)
              .collect(Collectors.toList());
        case LOGICAL_DNS:
          // TODO get the addresses from the DNS name
          return Collections.emptyList();
        case AGGREGATE:
          List<EquivalentAddressGroup> eags = new ArrayList<>();
          ImmutableMap<String, StatusOr<XdsConfig.XdsClusterConfig>> xdsConfigClusters =
              xdsConfig.getClusters();
          for (String childName : clusterResource.prioritizedClusterNames()) {
            StatusOr<XdsConfig.XdsClusterConfig> xdsClusterConfigStatusOr =
                xdsConfigClusters.get(childName);
            if (xdsClusterConfigStatusOr == null || !xdsClusterConfigStatusOr.hasValue()) {
              continue;
            }
            XdsConfig.XdsClusterConfig childClusterConfig = xdsClusterConfigStatusOr.getValue();
            if (childClusterConfig != null) {
              List<EquivalentAddressGroup> equivalentAddressGroups =
                  buildEagsForCluster(childClusterConfig, xdsConfig);
              eags.addAll(equivalentAddressGroups);
            }
          }
          return eags;
        default:
          throw new IllegalArgumentException("Unrecognized type: " + clusterResource.clusterType());
      }
    }

    private Object buildLbConfig(XdsConfig xdsConfig) {
      // TODO build it for real
      return new CdsConfig(CLUSTER);
    }

    @Override
    public void onError(String resourceContext, Status status) {

    }

    @Override
    public void onResourceDoesNotExist(String resourceContext) {

    }
  }
}
