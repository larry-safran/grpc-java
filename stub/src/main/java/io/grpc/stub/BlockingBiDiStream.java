package io.grpc.stub;

import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls.QueuingListener;
import io.grpc.stub.ClientCalls.StartableListener;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Represents a bidirectional streaming call from a client.  Allows in a blocking manner, sending
 * over the stream and receiving from the stream.  Also supports terminating the call.
 * Handles
 * @param <ReqT> Type of the Request Message
 * @param <RespT> Type of the Response Message
 */
public final class BlockingBiDiStream<ReqT,RespT> {
  private final BlockingQueue<Object> buffer;
  private final StartableListener<RespT> listener;
  private final ClientCall<ReqT,RespT> call;

  private boolean writeClosed;
  private Status closedStatus;

  private StatusRuntimeException closedException;

  private Waiter waiter = new Waiter();

  private static final class Waiter {
    // Threads waiting for read or write to be ready
    Queue<Thread> waitingThreads = new ConcurrentLinkedDeque<>();

    // Threads waiting for write to be ready
    Queue<Thread> waitingWriteOnlyThreads =  new ConcurrentLinkedDeque<>();

    synchronized void park(boolean writeOnly) {
      Queue<Thread> queue = (writeOnly ? waitingWriteOnlyThreads : waitingThreads);
      queue.add(Thread.currentThread());
      LockSupport.park();
    }

    synchronized void unpark(boolean read) {
      Thread cur;
      if (!read) {
        while ( (cur =waitingWriteOnlyThreads.poll()) != null) {
          LockSupport.unpark(cur);
        }
      }

      while ( (cur =waitingThreads.poll()) != null) {
        LockSupport.unpark(cur);
      }
    }
  }

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
     * If a read was successfully done then this will return the value that was read
     * @return value read from server or null if no value was read or stream has been closed
     */
    public RespT getResponse() {
      return response;
    }

    /**
     * Was the value requested to be written sent to the server.
     * If this returns false then the write was skipped and can be tried again later
     * <br>
     * Note that this does not indicate
     * whether the value has been received, only that it was passed to the stream for sending.
     * @return True if the req argument was sent to the channel
     */
    public boolean isWriteDone() {
      return writeDone;
    }

    /**
     * Was a value read
     * @return True if a value was retrieved from the server
     */
    public boolean isReadDone() {
      return readDone;
    }
  }

  BlockingBiDiStream(ClientCall<ReqT, RespT> call) {
    this.call = call;
    buffer = new ArrayBlockingQueue<Object>(3) {
      @Override
      public boolean add(Object o) {
        boolean didAdd = super.add(o);
        if (didAdd) {
          waiter.unpark(true);
        }
        return didAdd;
      }
    };
    listener = new QueuingListener(buffer, call);
  }

  /**
   * Check for whether some action is ready.
   * @return True if blockingWriteOrRead can run without blocking
   */
  public boolean isEitherReadOrWriteReady(){
    return isWriteReady() || isReadReady();
  }

  /**
   * Check whether there are any values waiting to be read
   * @return true if read will not block
   */
  public boolean isReadReady(){
    return !buffer.isEmpty();
  }

  /**
   * Check that write hasn't been marked complete and stream is ready to receive a write so will
   * not block.
   * @return true if legal to write and write will not block
   */
  public boolean isWriteReady() {
    return !writeClosed && call.isReady();
  }

  /**
   * Wait if necessary for a value to be available from the server.
   * If there is an available value return it immediately, if the stream is closed return a null.
   * Otherwise, wait for a value to be available or the stream to be closed
   * Params:
   * @return value from server or null if stream has been closed
   * @throws InterruptedException
   */
  public RespT blockingRead() throws InterruptedException {
    Object take;
    synchronized (this) {
      if (closedStatus != null) {
        return null;
      }

      take = buffer.take();
    }

    return processQueuedValue(take);
  }

  /**
   * Wait with timeout if necessary for a value to be available from the server.
   * If there is an available value return it immediately, if the stream is closed return a null.
   * Otherwise, wait for a value to be available or the stream to be closed
   * Params:
   * @param timeout – how long to wait before giving up, in units of unit
   * @param unit – a TimeUnit determining how to interpret the timeout parameter
   * @return value from server or null if stream has been closed or timeout occurs
   * @throws InterruptedException
   */
  public RespT blockingRead(long timeout, TimeUnit unit) throws InterruptedException{
    Object take;
    synchronized (this) {
      if (closedStatus != null) {
        return null;
      }

      take = buffer.poll(timeout, unit);
    }

    return processQueuedValue(take);
  }

  /**
   * Send a value to the stream for sending to server
   * Wait if necessary for the grpc stream to be ready
   * Note that it will return as soon as it passes the request to the grpc stream layer.  It will
   * not block while the message is being sent on the wire.
   * @param request Message to send to the server
   * @return true if value sent to stream, false if skipped
   * @throws InterruptedException
   */
  public boolean blockingWrite(ReqT request) throws InterruptedException {
    if (writeClosed) {
      return false;
    }

    boolean writeDone = false;

    while (!writeDone && !writeClosed) {
      if (call.isReady()) {
        call.sendMessage(request);
        writeDone = true;
      } else {
        // Block until the stream is ready
        waiter.park(true);
      }
    }

    return writeDone;
  }

  /**
   * Write a value and/or read a value.  If the write will not block, immediately do the write.
   * Regardless of whether write was done, if there is a value to read, get that value and
   * return it.
   * If neither write nor read were ready, park until one or the other is ready.
   * <br>
   * Note that it will consider the write action complete as soon as it passes the request to the
   * grpc stream layer.  It will not wait while the message is being sent on the wire.
   * <br>
   * This method cannot be called after {@link #writesComplete} or {@link #cancel}
   * @param request value to send to server
   * @return Object identifying which actions were done and maybe value received from server
   * @throws InterruptedException
   */
  public ActivityDescr<RespT> blockingWriteOrRead(ReqT request) throws InterruptedException {

    if (writeClosed) {
      return null;
    }

    ActivityDescr retVal = new ActivityDescr();

    while (!retVal.writeDone && !retVal.readDone && !writeClosed) {
      // Try to write
      if (call.isReady()) {
        call.sendMessage(request);
        retVal.writeDone = true;
      }

      // Try to read
      if (!buffer.isEmpty()) {
        retVal.response = processQueuedValue(buffer.take());
        retVal.readDone = true;
      }

      if (retVal.isReadDone() || retVal.isWriteDone()) {
        break;
      }

      // Block until one or the other is ready
      waiter.park(false);
    }

    return retVal;
  }

  /**
   * Do exactly one write and then one read
   *
   * This is designed for utilizing a bidi stream for unary requests in a non-multithreading
   * environment.
   * @param request Value to write
   * @return First value read
   * @throws InterruptedException
   */
  public RespT blockingReqResp(ReqT request) throws InterruptedException {
    synchronized (this) {
      if (writeClosed) {
        return null;
      }
      call.sendMessage(request);
    }
    return blockingRead();
  }

  /**
   * Cancel stream and stop any further writes.  Note that some reads that are in flight may still
   * happen after the cancel.
   * @param message
   * @param cause
   */
  public void cancel(String message, Throwable cause) {
    writeClosed = true;
    if (message == null) {
      message = "User requested a cancel";
    }
    call.cancel(message, cause);
    waiter.unpark(false);
  }

  /**
   * Indicate that no more writes will be done and the stream can be closed from the client side
   */
  public void writesComplete() {
    writeClosed = true;
    call.halfClose();
  }

  /**
   * Status that server sent when closing channel from its side
   * @return null if stream not closed by server, otherwise status sent by server
   */
  public Status getClosedStatus() {
    return closedStatus;
  }

  /**
   * If server closed the stream from its side with a non-OK status that included an exception
   * will be the exception that was sent
   * @return null if no exception from server, exception sent if there was one
   */
  public StatusRuntimeException getClosedException() {
    return closedException;
  }

  StartableListener<RespT> getListener() {
    return listener;
  }

  /**
   * Since the queue puts a ClientCall object when closed with Status.OK and a
   * StatusRuntimeException when closed with any other status, check for and handle those cases.
   * The other case is that there was a response value which we cast and return.
   * @param bufferVal value pulled from queue
   * @return null if stream was closed by server, otherwise the parameter itself
   */
  private RespT processQueuedValue(Object bufferVal) {

    if (bufferVal instanceof StatusRuntimeException) {
      synchronized (this) {
        closedException = (StatusRuntimeException) bufferVal;
        closedStatus = ((StatusRuntimeException) bufferVal).getStatus();
        writeClosed = true;
      }
      return null;
    }

    if (bufferVal instanceof ClientCall) {
      synchronized (this) {
        closedStatus = Status.OK;
      }
      return null;
    }

    return (RespT) bufferVal;
  }

  /**
   * wake up write if it is blocked
   */
  void handleReady() {
    waiter.unpark(false);
  }
}
