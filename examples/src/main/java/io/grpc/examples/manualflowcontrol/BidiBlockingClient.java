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

package io.grpc.examples.manualflowcontrol;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.examples.manualflowcontrol.StreamingGreeterGrpc.StreamingGreeterBlockingStub;
import io.grpc.stub.BlockingBiDiStream;
import io.grpc.stub.BlockingBiDiStream.ActivityDescr;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


/**
 * A class that tries multiple ways to do blocking bidi streaming
 * communication with an echo server
 */
public class BidiBlockingClient {

  private static final Logger logger = Logger.getLogger(BidiBlockingClient.class.getName());
  private static String lastLogMsg = "";
  private static int consecutiveCount = 0;

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting. The second argument is the target server. You can see the multiplexing in the server
   * logs.
   */
  public static void main(String[] args) throws Exception {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tH:%1$tM:%1$tS %5$s%6$s%n");

    // Access a service running on the local machine on port 50051
    String target = "localhost:50051";
    // Allow passing in the user and target strings as command line arguments
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [target]");
        System.err.println("");
        System.err.println("  target  The server to connect to. Defaults to " + target);
        System.exit(1);
      }
      target = args[0];
    }

    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
        .build();
    StreamingGreeterBlockingStub blockingStub = StreamingGreeterGrpc.newBlockingStub(channel);
    List<String> echoInput = names();
        // ImmutableList.of("some", "thing", "wicked", "this", "way", "comes");
    try {
      List<String> simpleWrite = useSimpleWrite(blockingStub, echoInput);
      List<String> blockUntilSomethingReady =
          doCommunication("blockUntilSomethingReady", blockingStub, echoInput);
      List<String> writeOrRead = doCommunication("writeOrRead", blockingStub, echoInput);
      List<String> writeUnlessBlockedAndReadIsReady =
          doCommunication("writeUnlessBlockedAndReadIsReady", blockingStub, echoInput);

      System.out.println("The echo requests and results were:");
      // System.out.println("Input                   : " + echoInput);
      // System.out.println("simpleWrite             : " + simpleWrite);
      // System.out.println("blockUntilSomethingReady: " + blockUntilSomethingReady);
      // System.out.println("writeOrRead             : " + writeOrRead);
      // System.out.println("writeUnlessBlockedAndReadIsReady: " + writeUnlessBlockedAndReadIsReady);
      System.out.println("Input                   : " + echoInput.size());
      System.out.println("simpleWrite             : " + simpleWrite.size());
      System.out.println("blockUntilSomethingReady: " + blockUntilSomethingReady.size());
      System.out.println("writeOrRead             : " + writeOrRead.size());
      System.out.println("writeUnlessBlockedAndReadIsReady: " + writeUnlessBlockedAndReadIsReady.size());

    } finally {
      // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
      // resources the channel should be shut down when it will no longer be used. If it may be used
      // again leave it running.
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Common logic for looping over the values to write and only moving on from the current
   * one when a write has been successful.
   * Looping terminates when there is an error causing the stream to close or when all
   * values have
   * @param method
   * @param stub
   * @param strings
   * @return List of strings read from server
   * @throws InterruptedException
   */
  private static List<String> doCommunication(String method,
      StreamingGreeterBlockingStub stub, List<String> strings) throws InterruptedException {
    BlockingBiDiStream<HelloRequest, HelloReply> stream = stub.sayHelloStreaming();

    List<String> readValues = new ArrayList<>();
    Iterator<String> iterator = strings.iterator();
    boolean lastWriteOk = true;
    String lastValue = null;
    boolean writesCompleted = false;

    logMethodStart(method);

    while ((stream.getClosedStatus() == null)
        && (!writesCompleted || readValues.size() < strings.size())) {
      if (lastWriteOk && iterator.hasNext()) {
        lastValue = iterator.next();
      }
      int readCount = readValues.size();
      switch (method) {
        case "writeUnlessBlockedAndReadIsReady":
          lastWriteOk = writeUnlessBlockedAndReadIsReady(stream, readValues, lastValue);
          break;
        case "writeOrRead":
          lastWriteOk = writeOrRead(stream, readValues, lastValue);
          break;
        case "blockUntilSomethingReady":
          lastWriteOk = blockUntilSomethingReady(stream, readValues, lastValue);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized method " + method );
      }

      logAction(readValues, lastWriteOk, lastValue, readCount);

      if (lastWriteOk && !iterator.hasNext()) { // last value was successfully sent
        lastValue = null;
        writesCompleted = true;
      }

    }
    if (consecutiveCount > 0) {
      logger.info("Repeated " + consecutiveCount + " times");
      consecutiveCount = 0;
    }


    return readValues;
  }

  private static void logMethodStart(String method) {
    logger.info("--------------------- Starting to process using method:  " + method);
    lastLogMsg = "";
    consecutiveCount = 0;
  }

  private static void logAction(List<String> readValues, boolean lastWriteOk, String lastValue,
      int readCount) {
    String writeResult =
        (lastValue != null) ? (lastWriteOk ? "successful" : "not done") : "skipped";
    String msg = String.format("The write was %s.  There were %d values read",
        writeResult, readValues.size() - readCount);

    if (msg.equals(lastLogMsg)) {
      consecutiveCount++;
    } else {
      if (consecutiveCount > 0) {
        logger.info("Repeated " + consecutiveCount + " times");
      }
      consecutiveCount = 0;
      lastLogMsg = msg;
      logger.info(msg);
    }
  }

  /**
   * writeUnlessBlockedAndReadIsReady
   * returns true if write was successful or valueToWrite was null
   **/
  private static boolean writeUnlessBlockedAndReadIsReady(
      BlockingBiDiStream<HelloRequest, HelloReply> stream,
      List<String> readValues, String valueToWrite) throws InterruptedException {

    // Check for whether stream is still usable
    if (valueToWrite != null) {
      HelloRequest req = HelloRequest.newBuilder().setName(valueToWrite).build();
      if (stream.writeUnlessBlockedAndReadIsReady(req, 10, TimeUnit.SECONDS)) {
        return true;
      }

      HelloReply response = stream.read(1, TimeUnit.SECONDS);
      if (response != null) {
        readValues.add(response.getMessage());
      }
      return false;
    } else {
      HelloReply response = stream.read(10, TimeUnit.MINUTES);
      if (response != null) {
        readValues.add(response.getMessage());
      }
      return true;
    }
  }






  private static boolean writeOrRead(BlockingBiDiStream<HelloRequest, HelloReply> stream,
      List<String> readValues, String valueToWrite)
      throws InterruptedException {
    if (valueToWrite != null) {
      HelloRequest req = HelloRequest.newBuilder().setName(valueToWrite).build();
      ActivityDescr<HelloReply> response =
          stream.writeOrRead(req, 10, TimeUnit.MINUTES);
      if (response.isReadDone()) {
        readValues.add(response.getResponse().getMessage());
      }
      return response.isWriteDone();
    } else {
      HelloReply respValue = stream.read(10, TimeUnit.MINUTES);
      if (respValue != null) {
        readValues.add(respValue.getMessage());
      }
      return true;
    }
  }








  private static boolean blockUntilSomethingReady(
      BlockingBiDiStream<HelloRequest, HelloReply> stream,
      List<String> readValues, String curValue) throws InterruptedException {

    if (curValue == null) {
      HelloReply response = stream.read(1, TimeUnit.SECONDS);
      if (response != null) {
        readValues.add(response.getMessage());
      }
      return true;
    }

    boolean writeIsReady = stream.blockUntilSomethingReady(10, TimeUnit.SECONDS);

    if (writeIsReady) {
      HelloRequest req = HelloRequest.newBuilder().setName(curValue).build();
      return stream.write(req);
    }

    HelloReply response = stream.read();
    if (response != null) {
      readValues.add(response.getMessage());
    }

    return true;
  }


  /**
   *  Try to write all values (with breaks for reads as needed to manage flow control), and then
   *  read the rest.
   */
  private static List<String> useSimpleWrite(StreamingGreeterBlockingStub blockingStub,
      List<String> valuesToWrite) throws InterruptedException {
    logMethodStart("Simple Write");

    List<String> readValues = new ArrayList<>();
    BlockingBiDiStream<HelloRequest, HelloReply> stream =
        blockingStub.sayHelloStreaming();

    for (String curValue : valuesToWrite) {
      boolean successfulWrite = false;
      HelloRequest req = HelloRequest.newBuilder().setName(curValue).build();
      while (stream.isWriteLegal() && !successfulWrite) {
        successfulWrite = stream.write(req, 1, TimeUnit.SECONDS);
        if (stream.isWriteReady()) {
          continue;
        }
        // Since write is now blocked, try to do reads
        while (stream.isReadReady()) {
          HelloReply readValue = stream.read(100, TimeUnit.MILLISECONDS);
          if (readValue != null) {
            logger.info("Read a value");
            readValues.add(readValue.getMessage());
          }
        }
      }
      if (!successfulWrite && stream.getClosedStatus() != null) {
        throw new IllegalStateException("Writing hasn't completed and stream has been closed",
            stream.getClosedStatus().asRuntimeException());
      }
    }

    // If state is still good after writes, let server know that we are done writing
    if (stream.getClosedStatus() == null || stream.getClosedStatus().isOk()) {
      stream.sendCloseWrite();
      logger.info("Completed writes");
    }

    // Read any remaining values
    while (readValues.size() < valuesToWrite.size() && stream.getClosedStatus() == null) {
      HelloReply readValue = stream.read(1, TimeUnit.SECONDS);
      if (readValue != null) {
        readValues.add(readValue.getMessage());
      } else {
        logger.info("Skipped reading");
      }
    }

    if (stream.getClosedStatus() != null && !stream.getClosedStatus().isOk()) {
      throw stream.getClosedStatus().asRuntimeException();
    }
    return readValues;
  }

  private static List<String> names() {
    return Arrays.asList(
        "Sophia",
        "Jackson",
        "Emma",
        "Aiden",
        "Olivia",
        "Lucas",
        "Ava",
        "Liam",
        "Mia",
        "Noah",
        "Isabella",
        "Ethan",
        "Riley",
        "Mason",
        "Aria",
        "Caden",
        "Zoe",
        "Oliver",
        "Charlotte",
        "Elijah",
        "Lily",
        "Grayson",
        "Layla",
        "Jacob",
        "Amelia",
        "Michael",
        "Emily",
        "Benjamin",
        "Madelyn",
        "Carter",
        "Aubrey",
        "James",
        "Adalyn",
        "Jayden",
        "Madison",
        "Logan",
        "Chloe",
        "Alexander",
        "Harper",
        "Caleb",
        "Abigail",
        "Ryan",
        "Aaliyah",
        "Luke",
        "Avery",
        "Daniel",
        "Evelyn",
        "Jack",
        "Kaylee",
        "William",
        "Ella",
        "Owen",
        "Ellie",
        "Gabriel",
        "Scarlett",
        "Matthew",
        "Arianna",
        "Connor",
        "Hailey",
        "Jayce",
        "Nora",
        "Isaac",
        "Addison",
        "Sebastian",
        "Brooklyn",
        "Henry",
        "Hannah",
        "Muhammad",
        "Mila",
        "Cameron",
        "Leah",
        "Wyatt",
        "Elizabeth",
        "Dylan",
        "Sarah",
        "Nathan",
        "Eliana",
        "Nicholas",
        "Mackenzie",
        "Julian",
        "Peyton",
        "Eli",
        "Maria",
        "Levi",
        "Grace",
        "Isaiah",
        "Adeline",
        "Landon",
        "Elena",
        "David",
        "Anna",
        "Christian",
        "Victoria",
        "Andrew",
        "Camilla",
        "Brayden",
        "Lillian",
        "John",
        "Natalie",
        "Lincoln"
    );
  }
}
