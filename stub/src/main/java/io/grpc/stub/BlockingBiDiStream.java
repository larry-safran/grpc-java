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

package io.grpc.stub;

import com.google.common.base.Preconditions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.ClientCalls.StartableListener;
import io.grpc.stub.ClientCalls.ThreadlessExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a bidirectional streaming call from a client.  Allows in a blocking manner, sending
 * over the stream and receiving from the stream.  Also supports terminating the call. Handles
 *
 * @param <ReqT> Type of the Request Message
 * @param <RespT> Type of the Response Message
 */
public final class BlockingBiDiStream<ReqT, RespT> {

  private static final Logger logger = Logger.getLogger(BlockingBiDiStream.class.getName());

  private final BlockingQueue<RespT> buffer;
  private final StartableListener<RespT> listener;
  private final ClientCall<ReqT, RespT> call;

  private final ThreadlessExecutor executor;

  private volatile boolean writeClosed;
  private Status closedStatus;

  /**
   * Indicates which actions have been performed and maybe contains a value sent by the server.
   *
   * @param <RespT> is the response type expected from the server
   */
  public static final class ActivityDescr<RespT> {

    private RespT response = null;
    private boolean writeDone;
    private boolean readDone;

    /**
     * If a read was successfully done then this will return the value that was read.
     *
     * @return value read from server or null if no value was read or stream has been closed
     */
    public RespT getResponse() {
      return response;
    }

    /**
     * Was the value requested to be written passed to the stream to send to the server. If this
     * returns false then the write was skipped and can be tried again later.
     * <br><br>
     * <p>
     * <b>NOTE</b> that this does not indicate
     * whether the value has been received, only that it was passed to the stream for sending.
     * </p>
     *
     * @return True if the req argument was sent to the channel
     */
    public boolean isWriteDone() {
      return writeDone;
    }

    /**
     * Was a value read.
     *
     * @return True if a value was retrieved from the server
     */
    public boolean isReadDone() {
      return readDone;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ActivityDescr)) {
        return false;
      }
      ActivityDescr<?> o = (ActivityDescr<?>) obj;
      return this.response == o.response
          && this.readDone == o.readDone
          && this.writeDone == o.writeDone;
    }

    @Override
    public int hashCode() {
      // Not likely to be used anywhere, but need something
      return response.hashCode() + (readDone ? 1 << 10 : 0) + (writeDone ? 1 << 20 : 0);
    }
  }

  BlockingBiDiStream(ClientCall<ReqT, RespT> call, ThreadlessExecutor executor) {
    this.call = call;
    this.executor = executor;
    buffer = new ArrayBlockingQueue<>(1);
    listener = new QueuingListener(buffer, call);
  }

  /**
   * Check for whether some action is ready.
   *
   * @return True if legal to write and writeOrRead can run without blocking
   */
  public boolean isEitherReadOrWriteReady() throws InterruptedException {
    return (isWriteLegal() && isWriteReady()) || isReadReady();
  }

  /**
   * Check whether there are any values waiting to be read.
   *
   * @return true if read will not block
   */
  public boolean isReadReady() throws InterruptedException {
    executor.drain();

    return !buffer.isEmpty();
  }

  /**
   * Check that write hasn't been marked complete and stream is ready to receive a write (so will
   * not block).
   *
   * @return true if legal to write and write will not block
   */
  public boolean isWriteReady() throws InterruptedException {
    executor.drain();

    return isWriteLegal() && call.isReady();
  }

  /**
   * Block until read or write is ready or timeout is exceeded or stream is closed.
   *
   * @return true if something is ready, false if stream was closed
   */
  public boolean blockUntilSomethingReady(long timeout, TimeUnit unit) throws InterruptedException {
    if (closedStatus != null) {
      return false;
    }

    long start = System.nanoTime();
    long duration = unit.toNanos(timeout);

    while (!isReadReady() && !isWriteReady()) {
      if (waitAndDrainExecutorOrTimeout(start, duration)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Write a value unless write would block and a read is ready.  The write will be skipped if:
   * <ul>
   *   <li>Write would block and a read would not block</li>
   *   <li>Called after calling {@link #sendCloseWrite} or {@linkplain #cancel}</li>
   *   <li>Called after close is received from the server (either onComplete or onError)</li>
   * </ul>
   *
   * @param request value to send to server
   * @param timeout how long to wait before giving up.  Values &lt;= 0 are no wait
   * @param unit a TimeUnit determining how to interpret the timeout parameter
   * @return True if the request was passed to the stream.  False if skipped
   */
  public boolean writeUnlessBlockedAndReadIsReady(ReqT request, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (!isWriteLegal() || (!isWriteReady() && isReadReady())) {
      return false;
    }

    long start = System.nanoTime();
    long duration = unit.toNanos(timeout);

    while (isWriteLegal() && !isWriteReady() && !isReadReady()) {
      // wait for something to be ready or timeout
      if (waitAndDrainExecutorOrTimeout(start, duration)) {
        return false;
      }
    }

    if (isWriteReady()) {
      return write(request, duration - (System.nanoTime() - start), TimeUnit.NANOSECONDS);
    } else {
      return false;
    }
  }

  /**
   * Write a value or read a value.  If the write will not block, immediately do the write. If write
   * was not done and there is a value to read, get that value and return it. If neither write nor
   * read were ready, park until one or the other is ready or timeout triggers.
   * <br>
   * This method is a no-op when called after {@link #sendCloseWrite}, {@linkplain #cancel} or after
   * close is received from the server (either onComplete or onError).
   * <br><br>
   * <b>NOTE:</b> that this method will consider the write action to be complete as soon as it
   * passes the request to the grpc stream layer.  It will not wait for the message to be sent on
   * the wire.
   *
   * @param request value to send to server
   * @param timeout how long to wait before giving up.  Values &lt;= 0 are no wait
   * @param unit a TimeUnit determining how to interpret the timeout parameter
   * @return Object identifying which actions were done and maybe value received from server
   */
  public ActivityDescr<RespT> writeOrRead(ReqT request, long timeout, TimeUnit unit)
      throws InterruptedException {

    if (!isWriteLegal()) {
      return new ActivityDescr<>();
    }

    ActivityDescr<RespT> retVal = new ActivityDescr<>();

    long start = System.nanoTime();
    long duration = unit.toNanos(timeout);

    while (!retVal.writeDone && !retVal.readDone && !writeClosed && closedStatus == null) {
      // Try to write
      if (call.isReady()) {
        call.sendMessage(request);
        retVal.writeDone = true;
        executor.drain();
        return retVal;
      }

      // Try to read
      executor.drain();
      retVal.response = buffer.poll();
      if (retVal.response != null) {
        retVal.readDone = true;
        call.request(1);
      }

      // If we did something then we are ready to return
      if (retVal.isReadDone() || retVal.isWriteDone()) {
        break;
      }

      // Wait for something to become available, timeout or stream to close
      if (waitAndDrainExecutorOrTimeout(start, duration)) {
        break;
      }
    }

    return retVal;
  }

  /**
   * Wait if necessary for a value to be available from the server. If there is an available value
   * return it immediately, if the stream is closed return a null. Otherwise, wait for a value to be
   * available or the stream to be closed
   *
   * @return value from server or null if stream has been closed
   */
  public RespT read() throws InterruptedException {
    return read(0, TimeUnit.NANOSECONDS);
  }

  /**
   * Wait with timeout, if necessary, for a value to be available from the server. If there is an
   * available value, return it immediately.  If the stream is closed return a null. Otherwise, wait
   * for a value to be available, the stream to be closed or the timeout to expire.
   *
   * @param timeout how long to wait before giving up.  Values &lt;= 0 are no wait
   * @param unit a TimeUnit determining how to interpret the timeout parameter
   * @return value from server or null (if stream has been closed or timeout occurs)
   */
  public RespT read(long timeout, TimeUnit unit) throws InterruptedException {
    long start = System.nanoTime();

    executor.drain();

    if (buffer.isEmpty() && closedStatus != null) {
      return null;
    }

    long duration = unit.toNanos(timeout);
    RespT bufferedValue;
    while ((bufferedValue = buffer.poll()) == null && closedStatus == null) {
      if (waitAndDrainExecutorOrTimeout(start, duration)) {
        break;
      }
    }

    if (logger.isLoggable(Level.FINER)) {
      String conditional = (duration > 0) ? " with timeout " : " ";
      logger.finer("Client Blocking read" + conditional + "had value:  " + bufferedValue);
    }

    if (bufferedValue != null) {
      call.request(1);
    }

    return bufferedValue;
  }

  /**
   * Send a value to the stream for sending to server, wait if necessary for the grpc stream to be
   * ready.
   * <br>
   * If write is not legal at the time of call, immediately returns false
   * <br><br>
   * <b>NOTE:  </b>This method will return as soon as it passes the request to the grpc stream
   * layer. It will not block while the message is being sent on the wire and returning true does
   * not guarantee that the server gets the message.
   * <p>
   * <b>WARNING:  </b>Doing only writes without reads can lead to deadlocks as a result of flow
   * control.
   * </p>
   *
   * @param request Message to send to the server
   * @return true if the request is sent to stream, false if skipped
   */
  public boolean write(ReqT request) throws InterruptedException {
    return write(request, 0, TimeUnit.NANOSECONDS);
  }

  /**
   * Send a value to the stream for sending to server, wait if necessary for the grpc stream to be
   * ready up to specified timeout.
   * <br>
   * If write is not legal at the time of call, immediately returns false
   * <br><br>
   * <p>
   * <b>NOTE:  </b>This method will return as soon as it passes the request to the grpc stream
   * layer. It will not block while the message is being sent on the wire and returning true does
   * not guarantee that the server gets the message.
   * </p>
   * <p>
   * <b>WARNING:  </b>Doing only writes without reads can lead to deadlocks as a result of flow
   * control.
   * </p>
   *
   * @param request Message to send to the server
   * @param timeout How long to wait before giving up.  Values &lt;= 0 are no wait
   * @param unit A TimeUnit determining how to interpret the timeout parameter
   * @return true if the request is sent to stream, false if skipped
   */
  public boolean write(ReqT request, long timeout, TimeUnit unit)
      throws InterruptedException {
    executor.drain();

    if (!isWriteLegal()) {
      return false;
    }

    boolean writeDone = false;

    long start = System.nanoTime();
    long duration = unit.toNanos(timeout);

    while (!writeDone && !writeClosed) {
      if (call.isReady()) {
        call.sendMessage(request);
        writeDone = true;
      } else {
        if (waitAndDrainExecutorOrTimeout(start, duration)) {
          break;
        }
      }
    }

    return writeDone;
  }

  private boolean isWriteLegal() {
    return !writeClosed && closedStatus == null;
  }

  /**
   * calls this.executor's waitAndDrain or waitAndDrainWithTimeout.
   *
   * @return true if there was a timeout
   */
  private boolean waitAndDrainExecutorOrTimeout(long start, long duration)
      throws InterruptedException {
    if (duration > 0) {
      long soFar = System.nanoTime() - start;
      if (soFar >= duration) {
        return true;
      }
      // Let threadless executor do stuff until there is something for us to check
      executor.waitAndDrainWithTimeout(duration - soFar);
    } else {
      executor.drain();
    }

    return false;
  }

  /**
   * Cancel stream and stop any further writes.  Note that some reads that are in flight may still
   * happen after the cancel.
   *
   * @param message if not {@code null}, will appear as the description of the CANCELLED status
   * @param cause if not {@code null}, will appear as the cause of the CANCELLED status
   */
  public void cancel(String message, Throwable cause) {
    writeClosed = true;
    if (message == null) {
      message = "User requested a cancel";
    }
    call.cancel(message, cause);
    executor.add(NoOpRunnable.INSTANCE);
  }

  /**
   * Indicate that no more writes will be done and the stream will be closed from the client side.
   * <br>
   * See {@link ClientCall#halfClose()}
   */
  public void sendCloseWrite() {
    if (!writeClosed && closedStatus == null) {
      call.halfClose();
    }
    writeClosed = true;
    executor.add(NoOpRunnable.INSTANCE);
  }

  /**
   * Status that server sent when closing channel from its side.
   *
   * @return null if stream not closed by server, otherwise Status sent by server
   */
  public Status getClosedStatus() {
    drainQuietly();
    return closedStatus;
  }

  StartableListener<RespT> getListener() {
    return listener;
  }

  private void drainQuietly() {
    try {
      executor.drain();
    } catch (InterruptedException e) {
      logger.warning("Draining interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  /**
   * wake up write if it is blocked.
   */
  void handleReady() {
    executor.add(NoOpRunnable.INSTANCE);
  }

  final class QueuingListener extends StartableListener<RespT> {

    private final ClientCall<ReqT, RespT> call;
    private boolean done = false;
    private final BlockingQueue<RespT> buffer;


    QueuingListener(BlockingQueue<RespT> bufferArg, ClientCall<ReqT, RespT> callArg) {
      this.call = callArg;
      this.buffer = bufferArg;
    }

    @Override
    public void onHeaders(Metadata headers) {
    }

    @Override
    public void onMessage(RespT value) {
      Preconditions.checkState(!done, "ClientCall already closed");
      buffer.add(value);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      Preconditions.checkState(!done, "ClientCall already closed");
      closedStatus = status;
      done = true;
      sendCloseWrite(); // We are not allowed to send anything to server after close
    }

    @Override
    void onStart() {
      call.request(1);
    }
  }

  private static class NoOpRunnable implements Runnable {

    static NoOpRunnable INSTANCE = new NoOpRunnable();

    @Override
    public void run() {
      // do nothing
    }
  }
}
