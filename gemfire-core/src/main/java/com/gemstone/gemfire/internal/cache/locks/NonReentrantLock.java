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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Lightweight lock implementation that avoids the overhead of reentrancy and
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
public final class NonReentrantLock extends AtomicInteger implements
    TryLockObject, Lock {

  private static final long serialVersionUID = 3635531474259856124L;

  /**
   * Caches this VM's DistributedSystem. Deliberately not volatile since we are
   * okay with threads seeing a little old value and then setting this again.
   */
  private static InternalDistributedSystem DSYS;

  /**
   * Caches this VM's {@link DistributionConfig#getAckWaitThreshold()} used for
   * logging warnings when waiting for lock acquisition.Deliberately not
   * volatile since we are okay with threads seeing a little old value and then
   * setting this again.
   */
  private static int WAIT_THRESHOLD = DistributionConfig
      .DEFAULT_ACK_WAIT_THRESHOLD;

  /**
   * The {@link QueuedSynchronizer} to use for waiter threads.
   */
  private final QueuedSynchronizer sync;

  /**
   * The {@link CancelCriterion} to use for terminating waits on this lock. If
   * null then {@link InternalDistributedSystem#getCancelCriterion()} is used.
   */
  private transient final CancelCriterion stopper;

  /**
   * If true then use system "waitThreshold" to log warnings after that period
   * else no logging.
   */
  private final boolean useWaitThreshold;

  // exported methods

  public NonReentrantLock(boolean useWaitThreshold) {
    this(useWaitThreshold, null, null);
  }

  public NonReentrantLock(boolean useWaitThreshold, CancelCriterion stopper) {
    this(useWaitThreshold, null, stopper);
  }

  public NonReentrantLock(boolean useWaitThreshold,
      InternalDistributedSystem sys, CancelCriterion stopper) {
    this.sync = new QueuedSynchronizer();
    if (stopper == null) {
      this.stopper = sys != null ? sys.getCancelCriterion() : null;
    }
    else {
      this.stopper = stopper;
    }
    this.useWaitThreshold = useWaitThreshold;
  }

  /**
   * An attempt to acquire lock should succeed when the lock has not been
   * already taken, either by this thread or another.
   * 
   * @param msecs
   *          The timeout passed to the {@link LockingPolicy#getTimeout} method.
   *          A value equal to zero means not to wait at all, while a value less
   *          than zero means to wait indefinitely.
   * 
   * @return - true if lock acquired successfully, false otherwise
   */
  public final boolean attemptLock(long msecs) {
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
    return tryAcquire(msecs);
  }

  private final boolean tryAcquire(long msecs) {
    // we do this in units of "ack-wait-threshold" to allow writing log
    // messages in case lock acquire has not succeeded so far; also check for
    // CancelCriterion

    boolean result = false;
    long timeoutMillis;
    LogWriterI18n logger = null;
    int waitThreshold = 0;
    if (this.useWaitThreshold && (waitThreshold = getWaitThreshold()) > 0) {
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

      if (!this.useWaitThreshold) {
        continue;
      }
      if (logger == null) {
        logger = getDistributedSystem(true).getLogWriterI18n();
      }
      getCancelCriterion().checkCancelInProgress(null);
      msecs -= timeoutMillis;
      if (logger.warningEnabled()) {
        logger.warning(LocalizedStrings.LocalLock_Waiting, new Object[] {
            "NonReentrantLock",
            Double.toString((double)timeoutMillis / 1000.0), "WRITE",
            "this object", toString(), msecs });
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

  private final long getMaxMillis() {
    return (Long.MAX_VALUE / 1000) - 1; // division by 1000 for nanos
  }

  /**
   * Releases the lock previously acquired by a call to
   * {@link #attemptLock(long)}. Implemented by unblocking one or more threads.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   */
  public final void releaseLock() {
    tryRelease();
    this.sync.signalWaiters();
  }

  // Lock interface methods

  /**
   * @see Lock#lock()
   */
  public final void lock() {
    for (;;) {
      // first try to acquire the lock without any blocking
      if (tryAcquire() || tryAcquire(getMaxMillis())) {
        break;
      }
    }
  }

  /**
   * @see Lock#lockInterruptibly()
   */
  public final void lockInterruptibly() throws InterruptedException {
    for (;;) {
      if (tryAcquire()) {
        return;
      }
      if (!tryAcquire(getMaxMillis()) && Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new InterruptedException();
      }
    }
  }

  /**
   * @see Lock#unlock()
   */
  public final void unlock() {
    tryRelease();
    this.sync.signalWaiters();
  }

  /**
   * @see Lock#tryLock()
   */
  public final boolean tryLock() {
    return tryAcquire();
  }

  /**
   * @see Lock#tryLock(long, TimeUnit)
   */
  public final boolean tryLock(final long time, final TimeUnit unit) {
    // first try to acquire the lock without any blocking
    if (tryAcquire()) {
      return true;
    }
    return tryAcquire(unit.toMillis(time));
  }

  public final boolean isLocked() {
    return getState() == 1;
  }

  /**
   * Returns an estimate of the number of threads waiting to acquire this lock.
   * The value is only an estimate because the number of threads may change
   * dynamically while this method traverses internal data structures. This
   * method is designed for use in monitoring of the system state, not for
   * synchronization control.
   * 
   * @return the estimated number of threads waiting for this lock
   */
  public final int getQueueLength() {
    return this.sync.getQueueLength();
  }

  /**
   * Return the {@link CancelCriterion} for this lock.
   */
  public final CancelCriterion getCancelCriterion() {
    return this.stopper != null ? this.stopper : getDistributedSystem(true)
        .getCancelCriterion();
  }

  // helper methods

  /**
   * Return the wait time after to try for the lock in a loop after which a
   * warning is logged.
   */
  protected static final int getWaitThreshold() {
    InternalDistributedSystem sys = DSYS;
    if (sys != null) {
      return WAIT_THRESHOLD;
    }
    else {
      getDistributedSystem(false);
      return WAIT_THRESHOLD;
    }
  }

  /**
   * Return the {@link InternalDistributedSystem} of this VM.
   */
  protected static final InternalDistributedSystem getDistributedSystem(
      boolean throwDisconnectException) {
    InternalDistributedSystem sys = DSYS;
    if (sys == null || !sys.isConnected()) {
      sys = InternalDistributedSystem.getConnectedInstance();
      // still not connected?
      if (sys == null) {
        if (throwDisconnectException) {
          throw InternalDistributedSystem.newDisconnectedException(null);
        }
        else {
          return null;
        }
      }
      final int timeoutSecs = sys.getConfig().getAckWaitThreshold();
      if (timeoutSecs > 0) {
        WAIT_THRESHOLD = timeoutSecs;
      }
      else {
        WAIT_THRESHOLD = -1;
      }
      DSYS = sys;
    }
    return sys;
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
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append('@').append(Integer.toHexString(hashCode())).append(',');
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
    if (compareAndSet(0, 1)) {
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
    if (compareAndSet(1, 0)) {
      this.sync.clearOwnerThread();
    }
    else {
      // if system is going down then this can happen in some rare cases
      getCancelCriterion().checkCancelInProgress(null);
      throw new IllegalMonitorStateException("lock not held in release");
    }
  }

  // unsupported methods from TryLockObject below

  public int tryAcquireShared(int arg, Object ownerId, Object context) {
    throw new UnsupportedOperationException();
  }

  public boolean tryReleaseShared(int arg, Object ownerId, Object context) {
    throw new UnsupportedOperationException();
  }

  // unsupported methods from Lock below

  /**
   * @see java.util.concurrent.locks.Lock#newCondition()
   */
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
