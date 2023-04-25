package io.grpc.stub;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.BlockingBiDiStream.ActivityDescr;
import io.grpc.stub.ServerCalls.BidiStreamingMethod;
import io.grpc.stub.ServerCalls.NoopStreamObserver;
import io.grpc.stub.ServerCallsTest.IntegerMarshaller;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class BlockingBidiStreamTest {

  private static final MethodDescriptor<Integer, Integer> BIDI_STREAMING_METHOD =
      MethodDescriptor.<Integer, Integer>newBuilder()
          .setType(MethodType.BIDI_STREAMING)
          .setFullMethodName("some/method")
          .setRequestMarshaller(new IntegerMarshaller())
          .setResponseMarshaller(new IntegerMarshaller())
          .build();

  private Server server;

  private ManagedChannel channel;

  private IntegerTestMethod<Integer, Integer> testMethod;
  private BlockingBiDiStream biDiStream;
  @Mock
  private ManagedChannel mockChannel;

  @Captor
  private ArgumentCaptor<MethodDescriptor<?, ?>> methodDescriptorCaptor;
  @Captor
  private ArgumentCaptor<CallOptions> callOptionsCaptor;
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    testMethod = new IntegerTestMethod<>();

    ServerServiceDefinition service = ServerServiceDefinition.builder(
            new ServiceDescriptor("some", BIDI_STREAMING_METHOD))
        .addMethod(BIDI_STREAMING_METHOD, ServerCalls.asyncBidiStreamingCall(testMethod))
        .build();
    long tag = System.nanoTime();

    server = InProcessServerBuilder.forName("go-with-the-flow" + tag).directExecutor()
        .addService(service).build().start();

    channel = InProcessChannelBuilder.forName("go-with-the-flow" + tag).directExecutor().build();
    biDiStream = ClientCalls.blockingBidiStreamingCall(channel,  BIDI_STREAMING_METHOD,
        CallOptions.DEFAULT);
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.shutdownNow();
    }
    if (channel != null) {
      channel.shutdownNow();
    }
  }


  @Test
  public void sanityTest() throws Exception{
    Integer req = 2;

    //  verify activity ready
    assertTrue(biDiStream.isEitherReadOrWriteReady());
    assertTrue(biDiStream.isWriteReady());
    // Have server send a value

    // Do a writeAndOrRead
    ActivityDescr response = biDiStream.blockingWriteOrRead(req);
    assertTrue(response.isReadDone());
    assertTrue(response.isWriteDone());
    assertNotNull(response.getResponse());

    // mark complete
    biDiStream.writesComplete();
    // verify activity !ready and !writeable
    assertFalse(biDiStream.isEitherReadOrWriteReady());
    assertFalse(biDiStream.isWriteReady());
  }

  @Test
  public void testReadSuccess_withoutBlocking() throws InterruptedException {
    // Have server push a value
    long start = System.nanoTime();
    assertNotNull(biDiStream.blockingRead(1, TimeUnit.SECONDS));
  }

  @Test
  public void testReadSuccess_withBlocking() {

  }

  @Test
  public void testCancel() {
    // read terminated
    // write terminated
    // new read not allowed
    // new write not allowed
  }

  @Test
  public void testIsActivityReady_true() {
    // write only ready
    // read only ready
    // both ready
  }

  @Test
  public void testIsActivityReady_false() {
    // Nothing ready
    // Add a read element
  }

  @Test
  public void testWriteSuccess_withBlocking() {

  }

  @Test
  public void testReadNonblocking_whenWriteBlocked() {
    // One value waiting
    // Two values waiting
  }

  @Test
  public void testReadsAndWritesInterleaved_withBlocking() {

  }

  @Test
  public void testUnaryOverBidi_withBlocking() {

  }

  @Test
  public void testUnaryOverBidi_withoutBlocking() {

  }

  @Test
  public void testMarkCompleted() {

  }

  @Test
  public void testClose_withException() {

  }

  private static class IntegerTestMethod<ReqT, RespT>
      implements BidiStreamingMethod<Integer, Integer> {
    int iteration;

    @Override
    public StreamObserver<Integer> invoke(StreamObserver<Integer> responseObserver) {
      final ServerCallStreamObserver<Integer> serverCallObserver =
          (ServerCallStreamObserver<Integer>) responseObserver;

      serverCallObserver.setOnReadyHandler(new Runnable() {
        @Override
        public void run() {
          while (serverCallObserver.isReady()) {
            serverCallObserver.onNext(iteration);
          }
          iteration++;
        }
      });

      return new NoopStreamObserver<Integer>() {
        @Override
        public void onCompleted() {
          serverCallObserver.onCompleted();
        }
      };
    }
  }

}