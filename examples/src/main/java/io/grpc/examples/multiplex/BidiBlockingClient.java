/*
 * Copyright 2023 The gRPC Authors
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

package io.grpc.examples.multiplex;

import com.google.common.collect.ImmutableList;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.examples.echo.EchoGrpc.EchoBlockingStub;
import io.grpc.examples.echo.EchoGrpc;
import io.grpc.examples.echo.EchoRequest;
import io.grpc.examples.echo.EchoResponse;
import io.grpc.stub.BlockingBiDiStream;
import io.grpc.stub.BlockingBiDiStream.ActivityDescr;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


/**
 * A class that tries multiple ways to do blocking bidi streaming
 * communication with an echo server
 */
public class BidiBlockingClient {
  private static final Logger logger = Logger.getLogger(BidiBlockingClient.class.getName());

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting. The second argument is the target server.
   * You can see the multiplexing in the server logs.
   */
  public static void main(String[] args) throws Exception {
    String user = "world";
    // Access a service running on the local machine on port 50051
    String target = "localhost:50051";
    // Allow passing in the user and target strings as command line arguments
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [name [target]]");
        System.err.println("");
        System.err.println("  name    The name you wish to be greeted by. Defaults to " + user);
        System.err.println("  target  The server to connect to. Defaults to " + target);
        System.exit(1);
      }
      user = args[0];
    }
    if (args.length > 1) {
      target = args[1];
    }

    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
        .build();
    EchoBlockingStub blockingStub = EchoGrpc.newBlockingStub(channel);
    List<String> echoInput = ImmutableList.of("some", "thing", "wicked", "this", "way", "comes");
    try {
      List<String> simpleWrite = useSimpleWrite(blockingStub, echoInput);
      List<String> blockUntilSomethingReady =
          doCommunication("blockUntilSomethingReady", blockingStub, echoInput);
      List<String> writeOrRead = doCommunication("writeOrRead", blockingStub, echoInput);
      List<String> writeUnlessBlockedAndReadIsReady =
          doCommunication("writeUnlessBlockedAndReadIsReady", blockingStub, echoInput);

      System.out.println("The echo requests and results were:");
      System.out.println("Input      : " + echoInput);
      System.out.println("simpleWrite             : " + simpleWrite);
      System.out.println("blockUntilSomethingReady: " + blockUntilSomethingReady);
      System.out.println("writeOrRead             : " + writeOrRead);
      System.out.println("writeUnlessBlockedAndReadIsReady: " + writeUnlessBlockedAndReadIsReady);

    } finally {
      // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
      // resources the channel should be shut down when it will no longer be used. If it may be used
      // again leave it running.
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static List<String> doCommunication(String method,
      EchoBlockingStub blockingStub, List<String> echoInput) throws InterruptedException {
    BlockingBiDiStream<EchoRequest, EchoResponse> stream = blockingStub.bidirectionalStreamingEcho();

    List<String> readValues = new ArrayList<String>();
    Queue<String> queue = new ArrayDeque<>(echoInput);

    while ((stream.getClosedStatus() == null)
        && (!queue.isEmpty() || readValues.size() < echoInput.size())) {
      switch (method) {
        case "writeUnlessBlockedAndReadIsReady":
          writeUnlessBlockedAndReadIsReady(stream, readValues, queue);
          break;
        case "writeOrRead":
          writeOrRead(stream, readValues, queue);
          break;
        case "blockUntilSomethingReady":
          blockUntilSomethingReady(stream, readValues, queue);
          break;
        default:
          System.out.println("Method not recognized " + method);

      }
    }

    return readValues;
  }

  private static void writeUnlessBlockedAndReadIsReady(
      BlockingBiDiStream<EchoRequest, EchoResponse> stream,
      List<String> readValues, Queue<String> queue) throws InterruptedException {

    if (!queue.isEmpty()) {
      String curValue = queue.peek();
      EchoRequest req = EchoRequest.newBuilder().setMessage(curValue).build();
      if (stream.writeUnlessBlockedAndReadIsReady(req, 10, TimeUnit.SECONDS)) {
        queue.poll();
        if (queue.isEmpty()) {
          stream.sendCloseWrite();
        }
      } else {
        EchoResponse response = stream.read(10, TimeUnit.SECONDS);
        if (response != null) {
          readValues.add(response.getMessage());
        }
      }
    } else {
      EchoResponse response = stream.read(10, TimeUnit.MINUTES);
      if (response != null) {
        readValues.add(response.getMessage());
      }
    }
  }

  private static void writeOrRead(BlockingBiDiStream<EchoRequest, EchoResponse> stream,
      List<String> readValues, Queue<String> queue)
      throws InterruptedException {
      if (!queue.isEmpty()) {
        String curValue = queue.peek();
        EchoRequest req = EchoRequest.newBuilder().setMessage(curValue).build();
        ActivityDescr<EchoResponse> response =
            stream.writeOrRead(req, 10, TimeUnit.MINUTES);
        if (response.isReadDone()) {
          readValues.add(response.getResponse().getMessage());
        }
        if (response.isWriteDone()) {
          queue.poll();
          if (queue.isEmpty()) {
            stream.sendCloseWrite();
          }
        }
      } else {
        EchoResponse respValue = stream.read(10, TimeUnit.MINUTES);
        if (respValue != null) {
          readValues.add(respValue.getMessage());
        }
      }
  }

  private static void blockUntilSomethingReady(
      BlockingBiDiStream<EchoRequest, EchoResponse> stream,
      List<String> readValues, Queue<String> queue) throws InterruptedException {

    stream.blockUntilSomethingReady(10, TimeUnit.SECONDS);

    if (stream.isWriteReady() && !queue.isEmpty()) {
      String curValue = queue.peek();
      EchoRequest req = EchoRequest.newBuilder().setMessage(curValue).build();
      if (stream.write(req)) {
        queue.poll();
        if (queue.isEmpty()) {
          stream.sendCloseWrite();
        }
      }
    } else if (stream.isReadReady()) {
      EchoResponse response = stream.read(1, TimeUnit.SECONDS);
      if (response != null) {
        readValues.add(response.getMessage());
      }
    }
  }

  private static List<String> useSimpleWrite(EchoBlockingStub blockingStub, List<String> echoInput)
      throws InterruptedException {
    List<String> readValues = new ArrayList<String>();
    BlockingBiDiStream<EchoRequest, EchoResponse> stream = blockingStub.bidirectionalStreamingEcho();

    for (String curValue : echoInput) {
      boolean successfulWrite = false;
      EchoRequest req = EchoRequest.newBuilder().setMessage(curValue).build();
      while (stream.isWriteLegal() && !successfulWrite) {
        successfulWrite = stream.write(req, 10, TimeUnit.SECONDS);
        if (!stream.isWriteReady()) {
          while (stream.isReadReady()) {
            EchoResponse readValue = stream.read(0, TimeUnit.MILLISECONDS);
            if (readValue != null) {
              readValues.add(readValue.getMessage());
            }
          }
        }
      }
    }
    stream.sendCloseWrite();

    while (readValues.size() < echoInput.size() && stream.getClosedStatus() == null) {
      EchoResponse readValue = stream.read();
      if (readValue != null) {
        readValues.add(readValue.getMessage());
      }
    }
    return readValues;
  }

  private void readWhatIsAvailable(List<String> readValues) throws InterruptedException {
  }

}
