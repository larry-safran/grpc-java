package io.grpc.xds;

import com.google.protobuf.Empty;
import io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.protobuf.AckResponse;
import io.grpc.testing.protobuf.ExtraResourceRequest;
import io.grpc.testing.protobuf.UpdateControlDataRequest;
import io.grpc.testing.protobuf.XdsConfig;
import io.grpc.testing.protobuf.XdsTestConfigServiceGrpc;
import java.util.logging.Logger;

/**
 * Wrapper class for {@link XdsTestControlPlaneExternalService} to allow it to serve
 * two services: {@link XdsTestConfigServiceGrpc} and {@link AggregatedDiscoveryServiceGrpc}
 */
class XdsTestControlPlaneExternalWrapperService extends
    XdsTestConfigServiceGrpc.XdsTestConfigServiceImplBase {

  private static final Logger logger =
      Logger.getLogger(XdsTestControlPlaneExternalWrapperService.class.getName());

  private XdsTestControlPlaneExternalService xdsService;

  XdsTestControlPlaneExternalWrapperService(XdsTestControlPlaneExternalService svc) {
    xdsService = svc;
  }

  @Override
  public void setXdsConfigRpc(XdsConfig request, StreamObserver<AckResponse> responseObserver) {
    logger.finest("Set xds config: " + request);
    xdsService.setXdsConfigRpc(request,responseObserver);
  }

  @Override
  public void updateControlData(UpdateControlDataRequest request, StreamObserver<AckResponse> responseObserver) {
    logger.finest("Updating control data: " + request);
    xdsService.updateControlData(request, responseObserver);
  }

  @Override
  public void setExtraResources(ExtraResourceRequest request, StreamObserver<AckResponse> responseObserver) {
    logger.finest("Set extra resources: " + request);
    xdsService.setExtraResources(request, responseObserver);
  }

  public void clear(Empty empty) {
    xdsService.clear();
  }
}
