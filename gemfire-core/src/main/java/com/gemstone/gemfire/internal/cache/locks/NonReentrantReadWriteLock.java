/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.gemstone.gemfire.internal.cache.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Lightweight reader-writer lock implementation (like
 * <code>ReadWriteLock</code>) that avoids the overhead of reentrancy and
 * tracking of thread owner completely. It also honours {@link CancelCriterion}
 * instead of throwing {@link InterruptedException} by making use of the
 * customized {@link QueuedSynchronizer} for the thread wait queue instead of
 * JDK5's <code>AbstractQueuedSynchronizer</code>. It also uses the spin feature
 * of {@link QueuedSynchronizer} by default even when infinite wait has been
 * provided. Consequently it may be harder to debug any deadlocks when using
 * these locks so it is designed to use in controlled situations for short
 * durations and when performance is of utmost consideration.
 * 
 * @author swale
 * @since 7.0
 */
public class NonReentrantReadWriteLock extends AtomicInteger implements
    ReadWriteLockObject {

  private static final long serialVersionUID = 4534366298749944510L;

  protected static final int READ_BITS = Integer.SIZE - 2;

  protected static final int WRITE_MASK = 1 << READ_BITS;

  protected static final int READ_MASK = WRITE_MASK - 1;

  /**
   * The {@link QueuedSynchronizer} to use for waiter threads.
   */
  private final QueuedSynchronizer sync;

  /**
   * The {@link CancelCriterion} to use for terminating waits on this lock. If
   * null then {@link InternalDistributedSystem#getCancelCriterion()} is used.
   */
  private transient final CancelCriterion stopper;

  // exported methods

  public NonReentrantReadWriteLock() {
    this(null, null);
  }

  public NonReentrantReadWriteLock(CancelCriterion stopper) {
    this(null, stopper);
  }

  public NonReentrantReadWriteLock(InternalDistributedSystem sys,
      CancelCriterion stopper) {
    this.sync = new QueuedSynchronizer();
    if (stopper == null) {
      this.stopper = sys != null ? sys.getCancelCriterion() : null;
    }
    else {
      this.stopper = stopper;
    }
  }

  /**
   * An attempt to acquire reader lock should succeed when there are no writer
   * locks already taken (by calling {@link #attemptWriteLock}). The owner
   * argument is ignored since this is a non-reentrant lock and the behaviour is
   * identical to {@link #attemptReadLock(long)}.
   * 
   * @param msecs
   *          The timeout to wait for lock acquisition before failing. A value
   *          equal to zero means not to wait at all, while a value less than
   *          zero means to wait indefinitely.
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public final boolean attemptReadLock(long msecs, Object owner) {
    return attemptReadLock(msecs);
  }

  /**
   * An attempt to acquire reader lock should succeed when there are no writer
   * locks already taken (by calling {@link #attemptWriteLock}).
   * 
   * @param msecs
   *          The timeout to wait for lock acquisition before failing. A value
   *          equal to zero means not to wait at all, while a value less than
   *          zero means to wait indefinitely.
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public final boolean attemptReadLock(long msecs) {
    // first try to acquire the lock without any blocking
    if (tryAcquireShared()) {
      return true;
    }
    if (msecs == 0) {
      // return the result immediately for zero timeout
      return false;
    }
    if (msecs < 0) {
      msecs = getMaxMillis();
    }

    // we do this in units of "ack-wait-threshold" to allow writing log
    // messages in case lock acquire has not succeeded so far; also check for
    // CancelCriterion

    boolean result = false;
    final int waitThreshold = getWaitThreshold();
    long timeoutMillis;
    LogWriterI18n logger = null;
    if (waitThreshold > 0) {
      timeoutMillis = TimeUnit.SECONDS.toMillis(waitThreshold);
    }
    else {
      timeoutMillis = msecs;
    }
    while (msecs > timeoutMillis) {
      if (this.sync.tryAcquireSharedNanos(0, null, this,
          TimeUnit.MILLISECONDS.toNanos(timeoutMillis), null, this.stopper)) {
        result = true;
        break;
      }

      if (logger == null) {
        logger = NonReentrantLock.getDistributedSystem(true).getLogWriterI18n();
      }
      getCancelCriterion().checkCancelInProgress(null);
      msecs -= timeoutMillis;
      if (logger.warningEnabled()) {
        logger.warning(LocalizedStrings.LocalLock_Waiting, new Object[] {
            "NonReetrantReadWriteLock", Double.toString(
                (double)timeoutMillis / 1000.0), "READ",
            "this object", toString(), msecs });
      }
      // increase the timeout for logging by a factor for next iteration
      if (waitThreshold > 0) {
        timeoutMillis <<= 1;
      }
    }
    if (!result) {
      result = this.sync.tryAcquireSharedNanos(0, null, this,
          TimeUnit.MILLISECONDS.toNanos(msecs), null, this.stopper);
    }
    return result;
  }

  /**
   * An attempt to acquire write lock should succeed when there are no read
   * locks already taken ({@link #attemptReadLock(long)}). The owner argument is
   * ignored since this is a non-reentrant lock and the behaviour is identical
   * to {@link #attemptWriteLock(long)}.
   * 
   * @param msecs
   *          The timeout to wait for lock acquisition before failing. A value
   *          equal to zero means not to wait at all, while a value less than
   *          zero means to wait indefinitely.
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public final boolean attemptWriteLock(long msecs, Object owner) {
    return attemptWriteLock(msecs);
  }

  /**
   * An attempt to acquire write lock should succeed when there are no read
   * locks already taken ({@link #attemptReadLock(long)}).
   * 
   * @param msecs
   *          The timeout to wait for lock acquisition before failing. A value
   *          equal to zero means not to wait at all, while a value less than
   *          zero means to wait indefinitely.
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public final boolean attemptWriteLock(long msecs) {
    // first try to acquire the lock without any blocking
    if (tryAcquire()) {
      return true;
    }
    if (msecs == 0) {
      // return the result immediately for zero timeout
      return false;
    }
    if (msecs < 0) {
      msecs = getMaxMillis();
    }

    // we do this in units of "ack-wait-threshold" to allow writing log
    // messages in case lock acquire has not succeeded so far; also check for
    // CancelCriterion

    boolean result = false;
    final int waitThreshold = getWaitThreshold();
    long timeoutMillis;
    LogWriterI18n logger = null;
    if (waitThreshold > 0) {
      timeoutMillis = TimeUnit.SECONDS.toMillis(waitThreshold);
    }
    else {
      timeoutMillis = msecs;
    }
    while (msecs > timeoutMillis) {
      if (this.sync.tryAcquireNanos(0, null, this,
          TimeUnit.MILLISECONDS.toNanos(timeoutMillis), null, this.stopper)) {
        result = true;
        break;
      }

      if (logger == null) {
        logger = NonReentrantLock.getDistributedSystem(true).getLogWriterI18n();
      }
      getCancelCriterion().checkCancelInProgress(null);
      msecs -= timeoutMillis;
      if (logger.warningEnabled()) {
        logger.warning(LocalizedStrings.LocalLock_Waiting, new Object[] {
            "NonReentrantReadWriteLock", Double.toString(
                (double)timeoutMillis / 1000.0),
            "WRITE", "this object", toString(), msecs });
      }
      // increase the timeout for logging by a factor for next iteration
      if (waitThreshold > 0) {
        timeoutMillis <<= 1;
      }
    }
    if (!result) {
      result = this.sync.tryAcquireNanos(0, null, this,
          TimeUnit.MILLISECONDS.toNanos(msecs), null, this.stopper);
    }
    return result;
  }

  /**
   * Releases in shared mode. Implemented by unblocking one or more threads if
   * {@link #tryReleaseShared(int, Object, Object)} returns true.
   * 
   * @throws IllegalMonitorStateException
   *           If releasing would place this synchronizer in an illegal state or
   *           lock is not held by the calling thread. This exception must be
   *           thrown in a consistent fashion for synchronization to work
   *           correctly.
   */
  public final void releaseReadLock() {
    tryReleaseShared();
    this.sync.signalSharedWaiters();
  }

  /**
   * Releases in exclusive mode. Implemented by unblocking one or more threads.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state or
   *           lock is not held by the calling thread. This exception must be
   *           thrown in a consistent fashion for synchronization to work
   *           correctly.
   */
  public final void releaseWriteLock(Object owner) {
    releaseWriteLock();
  }

  /**
   * Releases in exclusive mode. Implemented by unblocking one or more threads.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state or
   *           lock is not held by the calling thread. This exception must be
   *           thrown in a consistent fashion for synchronization to work
   *           correctly.
   */
  public final void releaseWriteLock() {
    tryRelease();
    this.sync.signalWaiters();
  }

  public final int numReaders() {
    return getState() & READ_MASK;
  }

  public final boolean isWriteLocked() {
    return getState() == WRITE_MASK;
  }

  /**
   * Return the {@link CancelCriterion} for this lock.
   */
  public final CancelCriterion getCancelCriterion() {
    return this.stopper != null ? this.stopper : NonReentrantLock
        .getDistributedSystem(true).getCancelCriterion();
  }

  // helper methods

  /**
   * Return the wait time after to try for the lock in a loop after which a
   * warning is logged.
   */
  protected final int getWaitThreshold() {
    return NonReentrantLock.getWaitThreshold();
  }

  protected final long getMaxMillis() {
    return (Long.MAX_VALUE / 1000) - 1; // division by 1000 for nanos
  }

  /**
   * Returns a string identifying this synchronizer's state. The state, in
   * brackets, includes the String {@code "State="} followed by the current
   * value of {@link #getState}.
   * 
   * @return a string identifying the state of this synchronizer
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName())
        .append('@').append(Integer.toHexString(System.identityHashCode(this)))
        .append(',');
    return this.sync.toObjectString(sb).append("(lockState=0x")
        .append(Integer.toHexString(getState())).append(')').toString();
  }

  // TryLock implementation below

  /**
   * Returns the current value of synchronization state. This operation has
   * memory semantics of a <tt>volatile</tt> read.
   * 
   * @return current state value
   */
  public final int getState() {
    return super.get();
  }

  /**
   * Attempts to acquire in exclusive mode. This method should query if the
   * state of the object permits it to be acquired in the exclusive mode, and if
   * so to acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * <p>
   * All arguments to this method are ignored and are only there to satisfy the
   * {@link TryLockObject} interface.
   * 
   * @return 0 if the acquire succeeded and -1 if it failed
   */
  public final int tryAcquire(int arg, Object ownerId, Object context) {
    return tryAcquire() ? 0 : -1;
  }

  /**
   * Attempts to acquire in exclusive mode. This method should query if the
   * state of the object permits it to be acquired in the exclusive mode, and if
   * so to acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * @return true if the acquire succeeded and false otherwise
   */
  private final boolean tryAcquire() {
    if (compareAndSet(0, WRITE_MASK)) {
      this.sync.setOwnerThread();
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Attempts to set the state to reflect a release in exclusive mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * All arguments to this method are ignored and are only there to satisfy the
   * {@link TryLockObject} interface.
   * 
   * @return {@code true} if this object is now in a fully released state, so
   *         that any waiting threads may attempt to acquire; and {@code false}
   *         otherwise.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   */
  public final boolean tryRelease(int arg, Object ownerId, Object context) {
    tryRelease();
    return true;
  }

  /**
   * Attempts to set the state to reflect a release in exclusive mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   */
  private final void tryRelease() {
    if (compareAndSet(WRITE_MASK, 0)) {
      this.sync.clearOwnerThread();
    }
    else {
      // if system is going down then this can happen in some rare cases
      getCancelCriterion().checkCancelInProgress(null);
      throw new IllegalMonitorStateException("write lock not held in release");
    }
  }

  /**
   * Attempts to acquire in shared mode. This method should query if the state
   * of the object permits it to be acquired in the shared mode, and if so to
   * acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * <p>
   * All arguments to this method are ignored and are only there to satisfy the
   * {@link TryLockObject} interface.
   * 
   * @return 0 if the acquire succeeded and -1 if it failed
   */
  public final int tryAcquireShared(int arg, Object ownerId, Object context) {
    return tryAcquireShared() ? 0 : -1;
  }

  /**
   * Attempts to acquire in shared mode. This method should query if the state
   * of the object permits it to be acquired in the shared mode, and if so to
   * acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * @return true if the acquire succeeded and false otherwise
   */
  private final boolean tryAcquireShared() {
    for (;;) {
      final int currentState = getState();

      if ((currentState & WRITE_MASK) == 0) {
        if (currentState >= READ_MASK || currentState < 0) {
          throw new IllegalMonitorStateException(
              "Maximum read lock count exceeded " + READ_MASK + " = "
                  + currentState);
        }
        if (compareAndSet(currentState, currentState + 1)) {
          return true;
        }
      }
      else {
        return false;
      }
    }
  }

  /**
   * Attempts to set the state to reflect a release in shared mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * All arguments to this method are ignored and are only there to satisfy the
   * {@link TryLockObject} interface.
   * 
   * @return {@code true} if this object is now in a fully released state, so
   *         that any waiting threads may attempt to acquire; and {@code false}
   *         otherwise.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   */
  public final boolean tryReleaseShared(int arg, Object ownerId, Object context) {
    tryReleaseShared();
    return true;
  }

  /**
   * Attempts to set the state to reflect a release in shared mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   */
  private final void tryReleaseShared() {
    for (;;) {
      final int currentState = getState();
      if (currentState <= 0) {
        // if system is going down then this can happen in some rare cases
        getCancelCriterion().checkCancelInProgress(null);
        throw new IllegalMonitorStateException(
            "read lock count is <= zero in release = " + currentState);
      }
      if (compareAndSet(currentState, currentState - 1)) {
        return;
      }
    }
  }
}
