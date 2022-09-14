package io.grpc.testing.protobuf;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * A service for configuring a fake xds server.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler",
    comments = "Source: io/grpc/testing/protobuf/xds_test_config_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class XdsTestConfigServiceGrpc {

  private XdsTestConfigServiceGrpc() {}

  public static final String SERVICE_NAME = "grpc.testing.XdsTestConfigService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.grpc.testing.protobuf.XdsConfig,
      io.grpc.testing.protobuf.AckResponse> getSetXdsConfigRpcMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetXdsConfigRpc",
      requestType = io.grpc.testing.protobuf.XdsConfig.class,
      responseType = io.grpc.testing.protobuf.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.testing.protobuf.XdsConfig,
      io.grpc.testing.protobuf.AckResponse> getSetXdsConfigRpcMethod() {
    io.grpc.MethodDescriptor<io.grpc.testing.protobuf.XdsConfig, io.grpc.testing.protobuf.AckResponse> getSetXdsConfigRpcMethod;
    if ((getSetXdsConfigRpcMethod = XdsTestConfigServiceGrpc.getSetXdsConfigRpcMethod) == null) {
      synchronized (XdsTestConfigServiceGrpc.class) {
        if ((getSetXdsConfigRpcMethod = XdsTestConfigServiceGrpc.getSetXdsConfigRpcMethod) == null) {
          XdsTestConfigServiceGrpc.getSetXdsConfigRpcMethod = getSetXdsConfigRpcMethod =
              io.grpc.MethodDescriptor.<io.grpc.testing.protobuf.XdsConfig, io.grpc.testing.protobuf.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetXdsConfigRpc"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.XdsConfig.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new XdsTestConfigServiceMethodDescriptorSupplier("SetXdsConfigRpc"))
              .build();
        }
      }
    }
    return getSetXdsConfigRpcMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.grpc.testing.protobuf.UpdateControlDataRequest,
      io.grpc.testing.protobuf.AckResponse> getUpdateControlDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateControlData",
      requestType = io.grpc.testing.protobuf.UpdateControlDataRequest.class,
      responseType = io.grpc.testing.protobuf.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.testing.protobuf.UpdateControlDataRequest,
      io.grpc.testing.protobuf.AckResponse> getUpdateControlDataMethod() {
    io.grpc.MethodDescriptor<io.grpc.testing.protobuf.UpdateControlDataRequest, io.grpc.testing.protobuf.AckResponse> getUpdateControlDataMethod;
    if ((getUpdateControlDataMethod = XdsTestConfigServiceGrpc.getUpdateControlDataMethod) == null) {
      synchronized (XdsTestConfigServiceGrpc.class) {
        if ((getUpdateControlDataMethod = XdsTestConfigServiceGrpc.getUpdateControlDataMethod) == null) {
          XdsTestConfigServiceGrpc.getUpdateControlDataMethod = getUpdateControlDataMethod =
              io.grpc.MethodDescriptor.<io.grpc.testing.protobuf.UpdateControlDataRequest, io.grpc.testing.protobuf.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateControlData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.UpdateControlDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new XdsTestConfigServiceMethodDescriptorSupplier("UpdateControlData"))
              .build();
        }
      }
    }
    return getUpdateControlDataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.grpc.testing.protobuf.ExtraResourceRequest,
      io.grpc.testing.protobuf.AckResponse> getSetExtraResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetExtraResources",
      requestType = io.grpc.testing.protobuf.ExtraResourceRequest.class,
      responseType = io.grpc.testing.protobuf.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.testing.protobuf.ExtraResourceRequest,
      io.grpc.testing.protobuf.AckResponse> getSetExtraResourcesMethod() {
    io.grpc.MethodDescriptor<io.grpc.testing.protobuf.ExtraResourceRequest, io.grpc.testing.protobuf.AckResponse> getSetExtraResourcesMethod;
    if ((getSetExtraResourcesMethod = XdsTestConfigServiceGrpc.getSetExtraResourcesMethod) == null) {
      synchronized (XdsTestConfigServiceGrpc.class) {
        if ((getSetExtraResourcesMethod = XdsTestConfigServiceGrpc.getSetExtraResourcesMethod) == null) {
          XdsTestConfigServiceGrpc.getSetExtraResourcesMethod = getSetExtraResourcesMethod =
              io.grpc.MethodDescriptor.<io.grpc.testing.protobuf.ExtraResourceRequest, io.grpc.testing.protobuf.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetExtraResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.ExtraResourceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.testing.protobuf.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new XdsTestConfigServiceMethodDescriptorSupplier("SetExtraResources"))
              .build();
        }
      }
    }
    return getSetExtraResourcesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static XdsTestConfigServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceStub>() {
        @java.lang.Override
        public XdsTestConfigServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new XdsTestConfigServiceStub(channel, callOptions);
        }
      };
    return XdsTestConfigServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static XdsTestConfigServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceBlockingStub>() {
        @java.lang.Override
        public XdsTestConfigServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new XdsTestConfigServiceBlockingStub(channel, callOptions);
        }
      };
    return XdsTestConfigServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static XdsTestConfigServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<XdsTestConfigServiceFutureStub>() {
        @java.lang.Override
        public XdsTestConfigServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new XdsTestConfigServiceFutureStub(channel, callOptions);
        }
      };
    return XdsTestConfigServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service for configuring a fake xds server.
   * </pre>
   */
  public static abstract class XdsTestConfigServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * unary RPC to set configuration served by the fake xds server.
     * </pre>
     */
    public void setXdsConfigRpc(io.grpc.testing.protobuf.XdsConfig request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetXdsConfigRpcMethod(), responseObserver);
    }

    /**
     * <pre>
     * Unary RPC to set control data for the fake control plane.
     * </pre>
     */
    public void updateControlData(io.grpc.testing.protobuf.UpdateControlDataRequest request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateControlDataMethod(), responseObserver);
    }

    /**
     * <pre>
     * Unary RPC to set extra resources to return in addition to the regular ones.
     * </pre>
     */
    public void setExtraResources(io.grpc.testing.protobuf.ExtraResourceRequest request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetExtraResourcesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSetXdsConfigRpcMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.grpc.testing.protobuf.XdsConfig,
                io.grpc.testing.protobuf.AckResponse>(
                  this, METHODID_SET_XDS_CONFIG_RPC)))
          .addMethod(
            getUpdateControlDataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.grpc.testing.protobuf.UpdateControlDataRequest,
                io.grpc.testing.protobuf.AckResponse>(
                  this, METHODID_UPDATE_CONTROL_DATA)))
          .addMethod(
            getSetExtraResourcesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.grpc.testing.protobuf.ExtraResourceRequest,
                io.grpc.testing.protobuf.AckResponse>(
                  this, METHODID_SET_EXTRA_RESOURCES)))
          .build();
    }
  }

  /**
   * <pre>
   * A service for configuring a fake xds server.
   * </pre>
   */
  public static final class XdsTestConfigServiceStub extends io.grpc.stub.AbstractAsyncStub<XdsTestConfigServiceStub> {
    private XdsTestConfigServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected XdsTestConfigServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new XdsTestConfigServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * unary RPC to set configuration served by the fake xds server.
     * </pre>
     */
    public void setXdsConfigRpc(io.grpc.testing.protobuf.XdsConfig request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetXdsConfigRpcMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Unary RPC to set control data for the fake control plane.
     * </pre>
     */
    public void updateControlData(io.grpc.testing.protobuf.UpdateControlDataRequest request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateControlDataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Unary RPC to set extra resources to return in addition to the regular ones.
     * </pre>
     */
    public void setExtraResources(io.grpc.testing.protobuf.ExtraResourceRequest request,
        io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetExtraResourcesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A service for configuring a fake xds server.
   * </pre>
   */
  public static final class XdsTestConfigServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<XdsTestConfigServiceBlockingStub> {
    private XdsTestConfigServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected XdsTestConfigServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new XdsTestConfigServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * unary RPC to set configuration served by the fake xds server.
     * </pre>
     */
    public io.grpc.testing.protobuf.AckResponse setXdsConfigRpc(io.grpc.testing.protobuf.XdsConfig request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetXdsConfigRpcMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Unary RPC to set control data for the fake control plane.
     * </pre>
     */
    public io.grpc.testing.protobuf.AckResponse updateControlData(io.grpc.testing.protobuf.UpdateControlDataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateControlDataMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Unary RPC to set extra resources to return in addition to the regular ones.
     * </pre>
     */
    public io.grpc.testing.protobuf.AckResponse setExtraResources(io.grpc.testing.protobuf.ExtraResourceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetExtraResourcesMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A service for configuring a fake xds server.
   * </pre>
   */
  public static final class XdsTestConfigServiceFutureStub extends io.grpc.stub.AbstractFutureStub<XdsTestConfigServiceFutureStub> {
    private XdsTestConfigServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected XdsTestConfigServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new XdsTestConfigServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * unary RPC to set configuration served by the fake xds server.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.testing.protobuf.AckResponse> setXdsConfigRpc(
        io.grpc.testing.protobuf.XdsConfig request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetXdsConfigRpcMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Unary RPC to set control data for the fake control plane.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.testing.protobuf.AckResponse> updateControlData(
        io.grpc.testing.protobuf.UpdateControlDataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateControlDataMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Unary RPC to set extra resources to return in addition to the regular ones.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.testing.protobuf.AckResponse> setExtraResources(
        io.grpc.testing.protobuf.ExtraResourceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetExtraResourcesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SET_XDS_CONFIG_RPC = 0;
  private static final int METHODID_UPDATE_CONTROL_DATA = 1;
  private static final int METHODID_SET_EXTRA_RESOURCES = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final XdsTestConfigServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(XdsTestConfigServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SET_XDS_CONFIG_RPC:
          serviceImpl.setXdsConfigRpc((io.grpc.testing.protobuf.XdsConfig) request,
              (io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CONTROL_DATA:
          serviceImpl.updateControlData((io.grpc.testing.protobuf.UpdateControlDataRequest) request,
              (io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse>) responseObserver);
          break;
        case METHODID_SET_EXTRA_RESOURCES:
          serviceImpl.setExtraResources((io.grpc.testing.protobuf.ExtraResourceRequest) request,
              (io.grpc.stub.StreamObserver<io.grpc.testing.protobuf.AckResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class XdsTestConfigServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    XdsTestConfigServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.grpc.testing.protobuf.XdsTestConfigServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("XdsTestConfigService");
    }
  }

  private static final class XdsTestConfigServiceFileDescriptorSupplier
      extends XdsTestConfigServiceBaseDescriptorSupplier {
    XdsTestConfigServiceFileDescriptorSupplier() {}
  }

  private static final class XdsTestConfigServiceMethodDescriptorSupplier
      extends XdsTestConfigServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    XdsTestConfigServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (XdsTestConfigServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new XdsTestConfigServiceFileDescriptorSupplier())
              .addMethod(getSetXdsConfigRpcMethod())
              .addMethod(getUpdateControlDataMethod())
              .addMethod(getSetExtraResourcesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
