/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A Future on a filesystem sync call.  It given to a client or 'Handler' for it to wait on till
 * the sync completes.
 *
 * <p>Handlers coming in call append, append, append, and then do a flush/sync of
 * the edits they have appended the WAL before returning. Since sync takes a while to
 * complete, we give the Handlers back this sync future to wait on until the
 * actual HDFS sync completes. Meantime this sync future goes across the ringbuffer and into a
 * sync runner thread; when it completes, it finishes up the future, the handler get or failed
 * check completes and the Handler can then progress.
 * <p>
 * This is just a partial implementation of Future; we just implement get and
 * failure.  Unimplemented methods throw {@link UnsupportedOperationException}.
 * <p>
 * There is not a one-to-one correlation between dfs sync invocations and
 * instances of this class. A single dfs sync call may complete and mark many
 * SyncFutures as done; i.e. we batch up sync calls rather than do a dfs sync
 * call every time a Handler asks for it.
 * <p>
 * SyncFutures are immutable but recycled. Call {@link #reset(long)} before use even if it
 * the first time, start the sync, then park the 'hitched' thread on a call to
 * {@link #get()}
 */
@InterfaceAudience.Private
class SyncFuture {
  private static final long NOT_DONE = 0;

  /**
   * The sequence at which we were added to the ring buffer.
   */
  private long ringBufferSequence;

  /**
   * The sequence that was set in here when we were marked done. Should be equal
   * or > ringBufferSequence.  Put this data member into the NOT_DONE state while this
   * class is in use.  But for the first position on construction, let it be -1 so we can
   * immediately call {@link #reset(long)} below and it will work.
   */
  private long doneSequence = -1;

  /**
   * If error, the associated throwable. Set when the future is 'done'.
   */
  private Throwable throwable = null;

  private Thread t;
  /**
   * The last completed sequence Id. 
   */
  private long lastCompletedSequence = -1;

  /**
   * Call this method to clear old usage and get it ready for new deploy. Call
   * this method even if it is being used for the first time.
   * 
   * @param sequence
   * @return this
   */
  synchronized SyncFuture reset(final long sequence) {
    if (t != null && t != Thread.currentThread()) throw new IllegalStateException();
    t = Thread.currentThread();
    if (!isDone()) throw new IllegalStateException("" + sequence + " " + Thread.currentThread());
    this.lastCompletedSequence = this.doneSequence;
    this.doneSequence = NOT_DONE;
    this.ringBufferSequence = sequence;
    
    return this;
  }

  @Override
  public synchronized String toString() {
    return "done=" + isDone() + ", ringBufferSequence=" + this.ringBufferSequence;
  }

  synchronized long getRingBufferSequence() {
    return this.ringBufferSequence;
  }

  /**
   * @param sequence Sync sequence at which this future 'completed'.
   * @param t Can be null.  Set if we are 'completing' on error (and this 't' is the error).
   * @return True if we successfully marked this outstanding future as completed/done.
   * Returns false if this future is already 'done' when this method called.
   */
  synchronized boolean done(final long sequence, final long offeredSequence, final Throwable t) {
    // Earlier we would throw IllegalStateException whether the passed
    // sequence < this.ringBufferSequence. This is no longer true because if the current
    // SyncRunner is stuck, the SwitchMonitor would complete the SyncFuture. And, the SyncFuture
    // could be put again into the RingBuffer by the RegionServer handler. If the old SyncRunner
    // now becomes live, it would try to release this SF, but its sequence Id < ringBufferSequence.

    // More interestingly, before coming here, the old SyncRunner could also update its sequence
    // Id equal to current highestSyncedSequenceId, and then would try to mark this SynFuture
    // as done. We can't allow that as it wouldn't be true, i.e., we could be acking for writes
    // that never got sync'ed.

    // We now rather check the last completed sequence Id with the offeredSequence value of the
    // SyncRunner. In case the offeredSequence < lastCompletedSequence, it means the SyncFuture
    // is already passed the state which SyncRunner is trying to complete. In case switching is not
    // enabled, this should never be true, and we throw an IllegalStateException in the
    // FSHLog#releaseSyncFuture method.

    // The below condition means that SR trying to complete a SF which is already completed.
    // it is like completing a old SF (but it is again cocked because of a new request from RBH
    // and would be handled by another done request from a separate SR. Ignore its error too.
    if (offeredSequence < this.lastCompletedSequence) return false;
 
    if (isDone()) return false;
    // coming here means it has the right to process it. Any 't' is valid.
    this.throwable = t;

   // Mark done.
    this.doneSequence = sequence;
    // Wake up waiting threads.
    notify();
    return true;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException();
  }

  public synchronized long get() throws InterruptedException, ExecutionException {
    while (!isDone()) {
      wait(1000);
    }
    if (this.throwable != null) throw new ExecutionException(this.throwable);
    return this.doneSequence;
  }

  public Long get(long timeout, TimeUnit unit)
  throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException();
  }

  public boolean isCancelled() {
    throw new UnsupportedOperationException();
  }

  synchronized boolean isDone() {
    return this.doneSequence != NOT_DONE;
  }

  synchronized boolean isThrowable() {
    return isDone() && getThrowable() != null;
  }

  synchronized Throwable getThrowable() {
    return this.throwable;
  }

  public long getLastCompletedRingBufferSequence() {
    return lastCompletedSequence;
  }

  /**
   * Mark the current SyncFuture as completed and failed.
   * @param sequence
   * @param t
   */
  public synchronized void doneAndMarkAsFailed(long sequence, Throwable t) {
    this.doneSequence = sequence;
    this.throwable = t;
  }
}
