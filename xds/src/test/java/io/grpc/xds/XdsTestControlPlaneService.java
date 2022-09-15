/*
 * Copyright 2021 The gRPC Authors
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
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.SynchronizationContext;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

class XdsTestControlPlaneService extends
    AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase {
  private static final Logger logger = Logger.getLogger(XdsTestControlPlaneService.class.getName());

  protected final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new AssertionError(e);
        }
      });

  static final String ADS_TYPE_URL_LDS =
      "type.googleapis.com/envoy.config.listener.v3.Listener";
  static final String ADS_TYPE_URL_RDS =
      "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
  static final String ADS_TYPE_URL_CDS =
      "type.googleapis.com/envoy.config.cluster.v3.Cluster";
  static final String ADS_TYPE_URL_EDS =
      "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

  public ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, Set<String>>>
  getSubscribers() {
    return ImmutableMap.copyOf(subscribers);
  }

  public ImmutableMap<String, AtomicInteger> getXdsVersions() {
    return ImmutableMap.copyOf(xdsVersions);
  }

  public ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, AtomicInteger>>
  getXdsNonces() {
    return ImmutableMap.copyOf(xdsNonces);
  }

  public ImmutableMap<String, HashMap<String, Message>> getXdsResources() {
    return ImmutableMap.copyOf(xdsResources);
  }

  private final Map<String, HashMap<String, Message>> xdsResources = new HashMap<>();
  private ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, Set<String>>> subscribers
      = ImmutableMap.of(
          ADS_TYPE_URL_LDS, new HashMap<>(),
          ADS_TYPE_URL_RDS, new HashMap<>(),
          ADS_TYPE_URL_CDS, new HashMap<>(),
          ADS_TYPE_URL_EDS, new HashMap<>()
          );
  private final ImmutableMap<String, AtomicInteger> xdsVersions = ImmutableMap.of(
      ADS_TYPE_URL_LDS, new AtomicInteger(1),
      ADS_TYPE_URL_RDS, new AtomicInteger(1),
      ADS_TYPE_URL_CDS, new AtomicInteger(1),
      ADS_TYPE_URL_EDS, new AtomicInteger(1)
  );
  private final ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, AtomicInteger>>
      xdsNonces = ImmutableMap.of(
      ADS_TYPE_URL_LDS, new HashMap<>(),
      ADS_TYPE_URL_RDS, new HashMap<>(),
      ADS_TYPE_URL_CDS, new HashMap<>(),
      ADS_TYPE_URL_EDS, new HashMap<>()
  );

  protected String getVersionForType(String resourceType) {
    return xdsVersions.get(resourceType).toString();
  }


  // treat all the resource types as state-of-the-world, send back all resources of a particular
  // type when any of them change.
  public <T extends Message> void setXdsConfig(final String type, final Map<String, T> resources) {
    logger.log(Level.FINE, "setting config {0} {1}", new Object[]{type, resources});
    syncContext.execute(new Runnable() {
      @Override
      public void run() {
        HashMap<String, Message> copyResources =  new HashMap<>(resources);
        xdsResources.put(type, copyResources);
        String newVersionInfo = String.valueOf(xdsVersions.get(type).getAndDecrement());

        notifySubscribers(newVersionInfo, type);
      }
    });
  }

  //must run in syncContext
  protected void notifySubscribers(String newVersionInfo, String type) {
    for (Map.Entry<StreamObserver<DiscoveryResponse>, Set<String>> entry :
        subscribers.get(type).entrySet()) {
      DiscoveryResponse response = generateResponse(type, newVersionInfo,
          String.valueOf(xdsNonces.get(type).get(entry.getKey()).incrementAndGet()),
          entry.getValue());
      entry.getKey().onNext(response);
    }
  }

  //must run in syncContext
  private DiscoveryResponse generateResponse(String resourceType, String version, String nonce,
                                             Set<String> resourceNames) {
    DiscoveryResponse.Builder responseBuilder =
        generateResponseBuilder(resourceType, version, nonce, resourceNames);
    return responseBuilder.build();
  }

  protected DiscoveryResponse.Builder generateResponseBuilder(String resourceType, String version, String nonce, Set<String> resourceNames) {
    DiscoveryResponse.Builder responseBuilder = DiscoveryResponse.newBuilder()
        .setTypeUrl(resourceType)
        .setVersionInfo(version)
        .setNonce(nonce);
    for (String resourceName: resourceNames) {
      if (xdsResources.containsKey(resourceType)
          && xdsResources.get(resourceType).containsKey(resourceName)) {
        responseBuilder.addResources(Any.pack(xdsResources.get(resourceType).get(resourceName),
            resourceType));
      }
    }
    return responseBuilder;
  }

  @Override
  public StreamObserver<DiscoveryRequest> streamAggregatedResources(
      final StreamObserver<DiscoveryResponse> responseObserver) {
    final StreamObserver<DiscoveryRequest> requestObserver =
        new StreamObserver<DiscoveryRequest>() {
          @Override
          public void onNext(final DiscoveryRequest value) {
            syncContext.execute(new Runnable() {
              @Override
              public void run() {
                logger.log(Level.FINEST, "control plane received request {0}", value);
                if (value.hasErrorDetail()) {
                  logger.log(Level.FINE, "control plane received nack resource {0}, error {1}",
                      new Object[]{value.getResourceNamesList(), value.getErrorDetail()});
                  return;
                }
                String resourceType = value.getTypeUrl();
                if (!value.getResponseNonce().isEmpty()
                    && !String.valueOf(xdsNonces.get(resourceType)).equals(value.getResponseNonce())) {
                  logger.log(Level.FINE, "Resource nonce does not match, ignore.");
                  return;
                }
                Set<String> requestedResourceNames = new HashSet<>(value.getResourceNamesList());
                if (subscribers.get(resourceType).containsKey(responseObserver)
                    && subscribers.get(resourceType).get(responseObserver)
                    .equals(requestedResourceNames)) {
                  logger.log(Level.FINEST, "control plane received ack for resource: {0}",
                      value.getResourceNamesList());
                  return;
                }
                if (!xdsNonces.get(resourceType).containsKey(responseObserver)) {
                  xdsNonces.get(resourceType).put(responseObserver, new AtomicInteger(0));
                }
                sendResponse(resourceType, requestedResourceNames, responseObserver);
                subscribers.get(resourceType).put(responseObserver, requestedResourceNames);
              }
            });
          }

          @Override
          public void onError(Throwable t) {
            logger.log(Level.FINE, "Control plane error: {0} ", t);
            onCompleted();
          }

          @Override
          public void onCompleted() {
            responseObserver.onCompleted();
            for (String type : subscribers.keySet()) {
              subscribers.get(type).remove(responseObserver);
              xdsNonces.get(type).remove(responseObserver);
            }
          }
        };
    return requestObserver;
  }

  protected void sendResponse(String resourceType, Set<String> requestedResourceNames, StreamObserver<DiscoveryResponse> responseObserver) {
    DiscoveryResponse response = generateResponse(resourceType,
        String.valueOf(xdsVersions.get(resourceType)),
        String.valueOf(xdsNonces.get(resourceType).get(responseObserver)),
        requestedResourceNames);
    responseObserver.onNext(response);
  }

}
