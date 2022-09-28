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

import static io.grpc.testing.protobuf.AberrationType.SEND_REDUNDANT;
import static io.grpc.testing.protobuf.AberrationType.STATUS_CODE;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.protobuf.AckResponse;
import io.grpc.testing.protobuf.ControlData;
import io.grpc.testing.protobuf.ExtraResourceRequest;
import io.grpc.testing.protobuf.TriggerTime;
import io.grpc.testing.protobuf.UpdateControlDataRequest;
import io.grpc.testing.protobuf.XdsConfig;
import io.grpc.testing.protobuf.XdsResourceType;
import io.grpc.testing.protobuf.XdsTestConfigServiceGrpc;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Add interface for {@link XdsTestConfigServiceGrpc} to the functionality of {@link
 * XdsTestControlPlaneService} and support returning "unexpected" content to support tests as
 * defined in go/grpc-xds-control-plane-tests. Specifically changing the data by resource type to
 * return: - Status code as specified - Extra data - Redundant data - Missing data
 *
 * <p>We are specifying a single value representing the time a modification begin. It will apply to
 * all types after the specified time with resource types having the order: LDS, RDS, CDS, EDS Note
 * that "after EDS" means that all resource types will be returned normally and then the aberation
 * will be applied if applicable
 */
class XdsTestControlPlaneExternalService extends XdsTestControlPlaneService {
  private static final Logger logger =
      Logger.getLogger(XdsTestControlPlaneExternalService.class.getName());
  public static final AckResponse OK_RESPONSE = AckResponse.newBuilder().setStatus(0).build();

  private ControlData controlData;
  private final Map<String, Map<String, Message>> extraXdsResources = new HashMap<>();
  private final String[] allResourceTypes =
      new String[] {ADS_TYPE_URL_LDS, ADS_TYPE_URL_CDS, ADS_TYPE_URL_RDS, ADS_TYPE_URL_EDS};

  public void setXdsConfigRpc(XdsConfig value, StreamObserver<AckResponse> responseObserver) {
    syncContext.execute(
        new Runnable() {
          @Override
          public void run() {
            logger.log(Level.FINEST, "control plane received request to set config {0}", value);
            try {
              setXdsConfigFromMsg(
                  convertTypeToString(value.getType()), value.getConfigurationList());
              responseObserver.onNext(OK_RESPONSE);
              responseObserver.onCompleted();
            } catch (Exception e) {
              logger.log(Level.SEVERE, "Problem updating extra resources", e);
              responseObserver.onError(e);
            }
          }
        });
  }

  public void updateControlData(
      UpdateControlDataRequest value, StreamObserver<AckResponse> responseObserver) {
    syncContext.execute(
        new Runnable() {
          @Override
          public void run() {
            logger.log(
                Level.FINEST, "control plane received request to update control data {0}", value);

            try {
              boolean skipNotification = false;
              if (value.hasControlData()) {
                controlData = value.getControlData();
              } else {
                if (controlData == null) { // Is a noop, so don't need to send udpates
                  skipNotification = true;
                }
                controlData = null;
              }
              // Need to send update to clients for all types based on the new control data
              if (!skipNotification) {
                for (String type : allResourceTypes) {
                  notifySubscribers(getVersionForType(type), type);
                }
              }

              responseObserver.onNext(OK_RESPONSE);
              responseObserver.onCompleted();
            } catch (Exception e) {
              logger.log(Level.SEVERE, "Problem updating control data", e);
              responseObserver.onError(e);
            }
          }
        });
  }

  public void setExtraResources(
      ExtraResourceRequest value, StreamObserver<AckResponse> responseObserver) {
    syncContext.execute(
        new Runnable() {
          @Override
          public void run() {
            logger.log(
                Level.FINEST, "control plane received request to set extra config {0}", value);

            try {
              for (XdsConfig cur : value.getConfigurationsList()) {
                Class<? extends Message> clazz =
                    convertStringToClass(convertTypeToString(cur.getType()));
                Map<String, Message> resourceMap = new HashMap<>();

                for (XdsConfig.Resource curRes : cur.getConfigurationList()) {
                  resourceMap.put(curRes.getName(), curRes.getConfiguration().unpack(clazz));
                }

                extraXdsResources.put(convertTypeToString(cur.getType()), resourceMap);
              }
              responseObserver.onNext(OK_RESPONSE);
              responseObserver.onCompleted();
            } catch (Exception e) {
              logger.log(Level.SEVERE, "Problem updating extra resources", e);
              responseObserver.onError(e);
            }
          }
        });
  }

  public static XdsResourceType convertStringToType(String type) {
    switch (type) {
      case ADS_TYPE_URL_LDS:
        return XdsResourceType.LDS;
      case ADS_TYPE_URL_CDS:
        return XdsResourceType.CDS;
      case ADS_TYPE_URL_RDS:
        return XdsResourceType.RDS;
      case ADS_TYPE_URL_EDS:
        return XdsResourceType.EDS;
      default:
        return XdsResourceType.UNRECOGNIZED;
    }
  }

  @Override
  protected void sendResponse(
      String resourceType,
      Set<String> requestedResourceNames,
      StreamObserver<DiscoveryResponse> client) {

    String versionInfo = getVersionForType(resourceType);
    boolean typeIsStatusCode =
        controlData != null && controlData.getAberrationType() == STATUS_CODE;
    boolean isAffected = isTypeAffected(resourceType);
    boolean isAberrationAfter = aberrationIsAfter(resourceType);
    HashMap<StreamObserver<DiscoveryResponse>, AtomicInteger> nonces =
        getXdsNonces().get(resourceType);

    sendResponseInternal(
        versionInfo,
        resourceType,
        typeIsStatusCode,
        isAffected,
        isAberrationAfter,
        nonces,
        client,
        requestedResourceNames);
  }

  // Note:  this must run in a syncContext.
  // Overrides the generic parent implementation to generate malformed responses.
  @Override
  protected void notifySubscribers(String newVersionInfo, String type) {
    ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, Set<String>>> subscribers =
        getSubscribers();
    ImmutableMap<String, HashMap<StreamObserver<DiscoveryResponse>, AtomicInteger>> xdsNonces =
        getXdsNonces();
    boolean typeIsStatusCode =
        controlData != null && controlData.getAberrationType() == STATUS_CODE;
    boolean isAffected = isTypeAffected(type);
    boolean isAberrationAfter = aberrationIsAfter(type);

    for (Map.Entry<StreamObserver<DiscoveryResponse>, Set<String>> entry :
        Objects.requireNonNull(subscribers.get(type)).entrySet()) {
      StreamObserver<DiscoveryResponse> client = entry.getKey();
      Set<String> resourceNames = entry.getValue();

      sendResponseInternal(
          newVersionInfo,
          type,
          typeIsStatusCode,
          isAffected,
          isAberrationAfter,
          xdsNonces.get(type),
          client,
          resourceNames);
    }
  }

  // Take incoming requests to update the XDS configuration, build the expected structure and
  // let our parent process the update.
  private void setXdsConfigFromMsg(String type, List<XdsConfig.Resource> resources)
      throws InvalidProtocolBufferException {
    logger.log(
        Level.FINE, "received request to set config {0} {1}", new Object[] {type, resources});
    Map<String, Message> resourceMap = new HashMap<>();
    Class<? extends Message> clazz = convertStringToClass(type);
    if (resources != null) {
      for (XdsConfig.Resource curRes : resources) {
        Message typedClass = curRes.getConfiguration().unpack(clazz);
        resourceMap.put(curRes.getName(), typedClass);
      }
    }
    setXdsConfig(type, resourceMap); // delegate work to super class
  }

  private void sendResponseInternal(
      String newVersionInfo,
      String type,
      boolean typeIsStatusCode,
      boolean isAffected,
      boolean isAberrationAfter,
      HashMap<StreamObserver<DiscoveryResponse>, AtomicInteger> xdsNonces,
      StreamObserver<DiscoveryResponse> client,
      Set<String> resourceNames) {

    if (!isAberrationAfter && typeIsStatusCode && isAffected && controlData != null) {
      callOnErrorWithStatusCode(controlData.getStatusCode(), type, client);
      return;
    }

    // Send an update to client
    DiscoveryResponse.Builder responseBuilder =
        genResponseBuilderAndIncrementNonce(
            type, isAffected, isAberrationAfter, newVersionInfo, client, resourceNames, xdsNonces);
    client.onNext(responseBuilder.build());

    if (controlData != null
        && controlData.getAberrationType() == SEND_REDUNDANT
        && !isAberrationAfter
        && isAffected) {
      String newNonce = String.valueOf(xdsNonces.get(client).incrementAndGet());
      responseBuilder.setNonce(newNonce);
      client.onNext(responseBuilder.build());
    }

    if (controlData != null
        && typeIsStatusCode
        && isAberrationImmediatelyAfter(type)
        && isAffected) {
      callOnErrorWithStatusCode(controlData.getStatusCode(), type, client);
    }
  }

  private void callOnErrorWithStatusCode(
      int statusCode, String resourceType, StreamObserver<DiscoveryResponse> client) {
    StatusRuntimeException t =
        Status.fromCodeValue(statusCode)
            .withDescription(
                String.format("Generated status code %d for type %s", statusCode, resourceType))
            .asRuntimeException();
    client.onError(t);
    removeClient(client);
  }

  private DiscoveryResponse.Builder genResponseBuilderAndIncrementNonce(
      String resourceType,
      boolean isAffected,
      boolean isAberrationAfter,
      String version,
      StreamObserver<DiscoveryResponse> client,
      Set<String> resourceNames,
      Map<StreamObserver<DiscoveryResponse>, AtomicInteger> clientToNonceMap) {

    String newNonce = String.valueOf(clientToNonceMap.get(client).incrementAndGet());
    if (controlData == null || !isAffected || isAberrationAfter) {
      return super.generateResponseBuilder(resourceType, version, newNonce, resourceNames);
    }

    DiscoveryResponse.Builder responseBuilder =
        DiscoveryResponse.newBuilder()
            .setTypeUrl(resourceType)
            .setVersionInfo(version)
            .setNonce(newNonce);

    switch (controlData.getAberrationType()) {
      case SEND_EMPTY:
        break;
      case SEND_EXTRA:
        buildStandardResponses(resourceType, resourceNames, responseBuilder);
        addExtraResponses(resourceType, responseBuilder);
        break;
      case MISSING_RESOURCES:
        buildMissingResponses(resourceType, resourceNames, responseBuilder);
        break;
      default:
        return super.generateResponseBuilder(resourceType, version, newNonce, resourceNames);
    }

    return responseBuilder;
  }

  private boolean isAberrationImmediatelyAfter(String type) {
    if (controlData == null
        || controlData.getTriggerAberration().equals(TriggerTime.UNRECOGNIZED)) {
      return false;
    }

    switch (type) {
      case ADS_TYPE_URL_LDS:
        return controlData.getTriggerAberration().equals(TriggerTime.BEFORE_CDS);
      case ADS_TYPE_URL_CDS:
        return controlData.getTriggerAberration().equals(TriggerTime.BEFORE_RDS);
      case ADS_TYPE_URL_RDS:
        return controlData.getTriggerAberration().equals(TriggerTime.BEFORE_ENDPOINTS);
      case ADS_TYPE_URL_EDS:
        return controlData.getTriggerAberration().equals(TriggerTime.AFTER_ENDPOINTS);
      default:
        return false;
    }
  }

  private boolean isTypeAffected(String resourceType) {
    if (controlData == null) {
      return false;
    }
    XdsResourceType resType = convertStringToType(resourceType);
    return controlData.getAffectedTypesList().isEmpty()
        || controlData.getAffectedTypesList().contains(resType);
  }

  private void addExtraResponses(String resourceType, DiscoveryResponse.Builder responseBuilder) {
    Map<String, Message> resources = extraXdsResources.get(resourceType);
    if (resources == null) {
      return;
    }

    for (Map.Entry<String, Message> entry : resources.entrySet()) {
      responseBuilder.addResources(Any.pack(entry.getValue(), resourceType));
    }
  }

  private void buildMissingResponses(
      String resourceType, Set<String> resourceNames, DiscoveryResponse.Builder responseBuilder) {
    Set<String> trimmedNames = new HashSet<>(resourceNames);

    if (isTypeAffected(resourceType)) {
      List<String> namesToSkip = controlData.getNamesToSkipList();
      trimmedNames.removeAll(namesToSkip);
    }
    buildStandardResponses(resourceType, trimmedNames, responseBuilder);
  }

  private void buildStandardResponses(
      String resourceType, Set<String> resourceNames, DiscoveryResponse.Builder responseBuilder) {
    ImmutableMap<String, HashMap<String, Message>> xdsResources = getXdsResources();
    if (!xdsResources.containsKey(resourceType)) {
      return;
    }

    HashMap<String, Message> typeToResourceMap = xdsResources.get(resourceType);
    if (typeToResourceMap == null) {
      return;
    }

    for (String resourceName : resourceNames) {
      if (typeToResourceMap.containsKey(resourceName)) {
        responseBuilder.addResources(Any.pack(typeToResourceMap.get(resourceName), resourceType));
      }
    }
  }

  private boolean aberrationIsAfter(String resourceType) {
    if (controlData == null) {
      return true;
    }

    int triggerAberrationTime = controlData.getTriggerAberrationValue();
    switch (resourceType) {
      case ADS_TYPE_URL_LDS:
        return triggerAberrationTime > TriggerTime.BEFORE_LDS_VALUE;
      case ADS_TYPE_URL_CDS:
        return triggerAberrationTime > TriggerTime.BEFORE_CDS_VALUE;
      case ADS_TYPE_URL_RDS:
        return triggerAberrationTime > TriggerTime.BEFORE_RDS_VALUE;
      case ADS_TYPE_URL_EDS:
        return triggerAberrationTime > TriggerTime.BEFORE_ENDPOINTS_VALUE;
      default:
        return true;
    }
  }

  private String convertTypeToString(XdsResourceType type) {
    switch (type) {
      case LDS:
        return ADS_TYPE_URL_LDS;
      case CDS:
        return ADS_TYPE_URL_CDS;
      case RDS:
        return ADS_TYPE_URL_RDS;
      case EDS:
        return ADS_TYPE_URL_EDS;
      default:
        return "";
    }
  }

  private Class<? extends Message> convertStringToClass(String type) {
    switch (type) {
      case ADS_TYPE_URL_LDS:
        return Listener.class;
      case ADS_TYPE_URL_CDS:
        return Cluster.class;
      case ADS_TYPE_URL_RDS:
        return RouteConfiguration.class;
      case ADS_TYPE_URL_EDS:
        return ClusterLoadAssignment.class;
      default:
        return null;
    }
  }

  public void clear() {
    syncContext.execute(
        new Runnable() {
          @Override
          public void run() {
            controlData = null;
            extraXdsResources.clear();
            clearSubscribersAndResources(); // Parent class owns those data structures
          }
        });
  }

  public Message getResource(XdsResourceType type, String resourceName) {
    HashMap<String, Message> resourceMap = getXdsResources().get(convertTypeToString(type));
    return (resourceMap != null) ? resourceMap.get(resourceName) : null;
  }
}
