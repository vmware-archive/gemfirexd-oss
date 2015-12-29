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
package com.gemstone.gemfire.internal.concurrent;

import java.util.concurrent.Semaphore;

/**
 * These methods are the same ones on
 * the JDK 5 version java.util.concurrent.Semaphore.
 * <p>Note that methods that take TimeUnit parameters
 * have been changed to end with <code>Ms</code> and to
 * always use milliseconds.
 * @see CFactory
 * @author darrel
 * @deprecated use Semaphore
 */
public interface S {
  /**
   * Acquires a permit from this semaphore, blocking until one is
   * available, or the thread is {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires a permit, if one is available and returns immediately,
   * reducing the number of available permits by one.
   *
   * <p>If no permit is available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * one of two things happens:
   * <ul>
   * <li>Some other thread invokes the {@link #release()} method for this
   * semaphore and the current thread is next to be assigned a permit; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts}
   * the current thread.
   * </ul>
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting
   * for a permit,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public void acquire() throws InterruptedException;

  /**
   * Acquires a permit from this semaphore, blocking until one is
   * available.
   *
   * <p>Acquires a permit, if one is available and returns immediately,
   * reducing the number of available permits by one.
   *
   * <p>If no permit is available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * some other thread invokes the {@link #release()} method for this
   * semaphore and the current thread is next to be assigned a permit.
   *
   * <p>If the current thread is {@linkplain Thread#interrupt interrupted}
   * while waiting for a permit then it will continue to wait, but the
   * time at which the thread is assigned a permit may change compared to
   * the time it would have received the permit had no interruption
   * occurred.  When the thread does return from this method its interrupt
   * status will be set.
   */
  public void acquireUninterruptibly();

  /**
   * Acquires a permit from this semaphore, only if one is available at the
   * time of invocation.
   *
   * <p>Acquires a permit, if one is available and returns immediately,
   * with the value {@code true},
   * reducing the number of available permits by one.
   *
   * <p>If no permit is available then this method will return
   * immediately with the value {@code false}.
   *
   * <p>Even when this semaphore has been set to use a
   * fair ordering policy, a call to {@code tryAcquire()} <em>will</em>
   * immediately acquire a permit if one is available, whether or not
   * other threads are currently waiting.
   * This &quot;barging&quot; behavior can be useful in certain
   * circumstances, even though it breaks fairness. If you want to honor
   * the fairness setting, then use
   * {@link #tryAcquireMs(long) tryAcquireMs(0) }
   * which is almost equivalent (it also detects interruption).
   *
   * @return {@code true} if a permit was acquired and {@code false}
   *         otherwise
   */
  public boolean tryAcquire();

  /**
   * Acquires a permit from this semaphore, if one becomes available
   * within the given waiting time and the current thread has not
   * been {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires a permit, if one is available and returns immediately,
   * with the value {@code true},
   * reducing the number of available permits by one.
   *
   * <p>If no permit is available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * one of three things happens:
   * <ul>
   * <li>Some other thread invokes the {@link #release()} method for this
   * semaphore and the current thread is next to be assigned a permit; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts}
   * the current thread; or
   * <li>The specified waiting time elapses.
   * </ul>
   *
   * <p>If a permit is acquired then the value {@code true} is returned.
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting
   * to acquire a permit,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   *
   * <p>If the specified waiting time elapses then the value {@code false}
   * is returned.  If the time is less than or equal to zero, the method
   * will not wait at all.
   *
   * @param timeout the maximum time to wait for a permit in milliseconds
   * @return {@code true} if a permit was acquired and {@code false}
   *         if the waiting time elapsed before a permit was acquired
   * @throws InterruptedException if the current thread is interrupted
   */
  public boolean tryAcquireMs(long timeout)
    throws InterruptedException;

  /**
   * Releases a permit, returning it to the semaphore.
   *
   * <p>Releases a permit, increasing the number of available permits by
   * one.  If any threads are trying to acquire a permit, then one is
   * selected and given the permit that was just released.  That thread
   * is (re)enabled for thread scheduling purposes.
   *
   * <p>There is no requirement that a thread that releases a permit must
   * have acquired that permit by calling {@link #acquire()}.
   * Correct usage of a semaphore is established by programming convention
   * in the application.
   */
  public void release();

  /**
   * Acquires the given number of permits from this semaphore,
   * blocking until all are available,
   * or the thread is {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires the given number of permits, if they are available,
   * and returns immediately, reducing the number of available permits
   * by the given amount.
   *
   * <p>If insufficient permits are available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * one of two things happens:
   * <ul>
   * <li>Some other thread invokes one of the {@link #release() release}
   * methods for this semaphore, the current thread is next to be assigned
   * permits and the number of available permits satisfies this request; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts}
   * the current thread.
   * </ul>
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting
   * for a permit,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   * Any permits that were to be assigned to this thread are instead
   * assigned to other threads trying to acquire permits, as if
   * permits had been made available by a call to {@link #release()}.
   *
   * @param permits the number of permits to acquire
   * @throws InterruptedException if the current thread is interrupted
   * @throws IllegalArgumentException if {@code permits} is negative
   */
  public void acquire(int permits) throws InterruptedException;

  /**
   * Acquires the given number of permits from this semaphore,
   * blocking until all are available.
   *
   * <p>Acquires the given number of permits, if they are available,
   * and returns immediately, reducing the number of available permits
   * by the given amount.
   *
   * <p>If insufficient permits are available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * some other thread invokes one of the {@link #release() release}
   * methods for this semaphore, the current thread is next to be assigned
   * permits and the number of available permits satisfies this request.
   *
   * <p>If the current thread is {@linkplain Thread#interrupt interrupted}
   * while waiting for permits then it will continue to wait and its
   * position in the queue is not affected.  When the thread does return
   * from this method its interrupt status will be set.
   *
   * @param permits the number of permits to acquire
   * @throws IllegalArgumentException if {@code permits} is negative
   *
   */
  public void acquireUninterruptibly(int permits);

  /**
   * Acquires the given number of permits from this semaphore, only
   * if all are available at the time of invocation.
   *
   * <p>Acquires the given number of permits, if they are available, and
   * returns immediately, with the value {@code true},
   * reducing the number of available permits by the given amount.
   *
   * <p>If insufficient permits are available then this method will return
   * immediately with the value {@code false} and the number of available
   * permits is unchanged.
   *
   * <p>Even when this semaphore has been set to use a fair ordering
   * policy, a call to {@code tryAcquire} <em>will</em>
   * immediately acquire a permit if one is available, whether or
   * not other threads are currently waiting.  This
   * &quot;barging&quot; behavior can be useful in certain
   * circumstances, even though it breaks fairness. If you want to
   * honor the fairness setting, then use {@link #tryAcquireMs(int,
   * long) tryAcquire(permits, 0) }
   * which is almost equivalent (it also detects interruption).
   *
   * @param permits the number of permits to acquire
   * @return {@code true} if the permits were acquired and
   *         {@code false} otherwise
   * @throws IllegalArgumentException if {@code permits} is negative
   */
  public boolean tryAcquire(int permits);

  /**
   * Acquires the given number of permits from this semaphore, if all
   * become available within the given waiting time and the current
   * thread has not been {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires the given number of permits, if they are available and
   * returns immediately, with the value {@code true},
   * reducing the number of available permits by the given amount.
   *
   * <p>If insufficient permits are available then
   * the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until one of three things happens:
   * <ul>
   * <li>Some other thread invokes one of the {@link #release() release}
   * methods for this semaphore, the current thread is next to be assigned
   * permits and the number of available permits satisfies this request; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts}
   * the current thread; or
   * <li>The specified waiting time elapses.
   * </ul>
   *
   * <p>If the permits are acquired then the value {@code true} is returned.
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while waiting
   * to acquire the permits,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   * Any permits that were to be assigned to this thread, are instead
   * assigned to other threads trying to acquire permits, as if
   * the permits had been made available by a call to {@link #release()}.
   *
   * <p>If the specified waiting time elapses then the value {@code false}
   * is returned.  If the time is less than or equal to zero, the method
   * will not wait at all.  Any permits that were to be assigned to this
   * thread, are instead assigned to other threads trying to acquire
   * permits, as if the permits had been made available by a call to
   * {@link #release()}.
   *
   * @param permits the number of permits to acquire
   * @param timeout the maximum time to wait for the permits in milliseconds
   * @return {@code true} if all permits were acquired and {@code false}
   *         if the waiting time elapsed before all permits were acquired
   * @throws InterruptedException if the current thread is interrupted
   * @throws IllegalArgumentException if {@code permits} is negative
   */
  public boolean tryAcquireMs(int permits, long timeout)
    throws InterruptedException;

  /**
   * Releases the given number of permits, returning them to the semaphore.
   *
   * <p>Releases the given number of permits, increasing the number of
   * available permits by that amount.
   * If any threads are trying to acquire permits, then one
   * is selected and given the permits that were just released.
   * If the number of available permits satisfies that thread's request
   * then that thread is (re)enabled for thread scheduling purposes;
   * otherwise the thread will wait until sufficient permits are available.
   * If there are still permits available
   * after this thread's request has been satisfied, then those permits
   * are assigned in turn to other threads trying to acquire permits.
   *
   * <p>There is no requirement that a thread that releases a permit must
   * have acquired that permit by calling {@link Semaphore#acquire(int) acquire}.
   * Correct usage of a semaphore is established by programming convention
   * in the application.
   *
   * @param permits the number of permits to release
   * @throws IllegalArgumentException if {@code permits} is negative
   */
  public void release(int permits);

  /**
   * Returns the current number of permits available in this semaphore.
   *
   * <p>This method is typically used for debugging and testing purposes.
   *
   * @return the number of permits available in this semaphore
   */
  public int availablePermits();

  /**
   * Acquires and returns all permits that are immediately available.
   *
   * @return the number of permits acquired
   */
  public int drainPermits();

  /**
   * Returns {@code true} if this semaphore has fairness set true.
   *
   * @return {@code true} if this semaphore has fairness set true
   */
  public boolean isFair();

  /**
   * Queries whether any threads are waiting to acquire. Note that
   * because cancellations may occur at any time, a {@code true}
   * return does not guarantee that any other thread will ever
   * acquire.  This method is designed primarily for use in
   * monitoring of the system state.
   *
   * @return {@code true} if there may be other threads waiting to
   *         acquire the lock
   */
  public boolean hasQueuedThreads();

  /**
   * Returns an estimate of the number of threads waiting to acquire.
   * The value is only an estimate because the number of threads may
   * change dynamically while this method traverses internal data
   * structures.  This method is designed for use in monitoring of the
   * system state, not for synchronization control.
   *
   * @return the estimated number of threads waiting for this lock
   */
  public int getQueueLength();
//   /**
//    * Shrinks the number of available permits by the indicated reduction.
//    * This method can be useful in subclasses that use semaphores to
//    * track resources that become unavailable.
//    * This method differs from acquire in that it does not block
//    * waiting for permits to become available.
//    * @param reduction the number of permits to remove
//    * @throws IllegalArgumentException if {@code reduction} is negative
//    */
//   public void reducePermits(int reduction);
}
