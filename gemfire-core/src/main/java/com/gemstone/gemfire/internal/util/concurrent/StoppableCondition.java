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

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;

/**
 * This class is functionally equivalent to {@link Condition}; however, it does
 * not implement the interface, in an attempt to encourage GemFire API writers
 * to refer to this "stoppable" version instead.
 * <p>
 * It is implemented as a strict "cover" for a genuine {@link Condition}.
 * 
 * @author jpenney
 */
public class StoppableCondition implements /* Condition, */ java.io.Serializable {

  private static final long serialVersionUID = -7091681525970431937L;

  /** The underlying condition **/
  private final Condition condition;

  /** The cancellation object */
  private final CancelCriterion stopper;

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  private static final long RETRY_TIME = 15 * 1000; // milliseconds

  /**
   * Create a new StoppableCondition based on given condition and cancellation
   * criterion
   * 
   * @param c
   *          the underlying condition
   **/
  StoppableCondition(Condition c, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    this.condition = c;
    this.stopper = stopper;
  }

  /**
   * @see Condition#awaitUninterruptibly()
   */
  public void awaitUninterruptibly() {
    for (;;) {
      boolean interrupted = Thread.interrupted();
      try {
        await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * @see Condition#await()
   */
  public void await() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    for (;;) {
      stopper.checkCancelInProgress(null);
      if (await(RETRY_TIME, TimeUnit.MILLISECONDS)) break;
    }
  }

  /**
   * @see Condition#await(long, TimeUnit)
   */
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return condition.await(time, unit);
  }

  /**
   * @see Condition#awaitNanos(long)
   */
  public long awaitNanos(long nanosTimeout) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return condition.awaitNanos(nanosTimeout);
  }

  /**
   * @see Condition#awaitUntil(Date)
   */
  public boolean awaitUntil(Date deadline) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return condition.awaitUntil(deadline);
  }

  /**
   * @see Condition#signal()
   */
  public synchronized void signal() {
    condition.signal();
  }

  /**
   * @see Condition#signalAll()
   */
  public synchronized void signalAll() {
    condition.signalAll();
  }
}
