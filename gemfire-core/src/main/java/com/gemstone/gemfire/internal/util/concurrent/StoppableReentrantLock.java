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

package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;

/**
 * Reentrant locks that respond to cancellations.
 * 
 * This class is functionally equivalent to {@link Lock}; however, it does not
 * implement the interface, in an attempt to encourage GemFire API writers to
 * refer to this "stoppable" version instead.
 * 
 * @author jpenney
 */
public class StoppableReentrantLock {

  /**
   * the underlying lock
   */
  private final ReentrantLock lock;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

  /**
   * the cancellation criterion
   */
  private final CancelCriterion stopper;

  /**
   * Create a new instance with the given cancellation criterion
   * 
   * @param stopper
   *          the cancellation criterion
   */
  public StoppableReentrantLock(CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    this.lock = new ReentrantLock();
    this.stopper = stopper;
  }

  /**
   * Create a new instance with given fairness and cancellation criterion
   * 
   * @param fair
   *          whether to be fair
   * @param stopper
   *          the cancellation criterion
   */
  public StoppableReentrantLock(boolean fair, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    this.stopper = stopper;
    this.lock = new ReentrantLock(fair);
  }

  /**
   * @see Lock#lock()
   */
  public void lock() {
    for (;;) {
      boolean interrupted = Thread.interrupted();
      try {
        lockInterruptibly();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    } // for
  }

  /**
   * @see Lock#lockInterruptibly()
   * @throws InterruptedException
   */
  public void lockInterruptibly() throws InterruptedException {
    for (;;) {
      stopper.checkCancelInProgress(null);
      if (lock.tryLock(RETRY_TIME, TimeUnit.MILLISECONDS)) break;
    }
  }

  /**
   * @see Lock#tryLock()
   * @return true if the lock is acquired
   */
  public boolean tryLock() {
    stopper.checkCancelInProgress(null);
    return lock.tryLock();
  }

  /**
   * @see Lock#tryLock(long, TimeUnit)
   * 
   * @param time
   *          the maximum time to wait for the lock
   * @param unit
   *          the time unit of the {@code time} argument
   * @return {@code true} if the lock was acquired and {@code false} if the
   *         waiting time elapsed before the lock was acquired
   * @throws InterruptedException
   */
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return lock.tryLock(time, unit);
  }

  /**
   * @see Lock#unlock()
   */
  public void unlock() {
    lock.unlock();
  }

  /**
   * @see Lock#newCondition()
   * @return the new stoppable condition
   */
  public StoppableCondition newCondition() {
    return new StoppableCondition(lock.newCondition(), stopper);
  }

  /**
   * @see ReentrantLock#isHeldByCurrentThread()
   * @return true if it is held
   */
  public boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }
}
