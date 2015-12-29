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

/**
 * These methods are the same ones on
 * the JDK 5 version java.util.concurrent.locks.Lock
 * @see CFactory
 * @author darrel
 * @deprecated use Lock
 */
public interface L {
  /**
   * Acquires the lock.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   */
  void lock();

  /**
   * Acquires the lock unless the current thread is
   * {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires the lock if it is available and returns immediately.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until
   * one of two things happens:
   *
   * <ul>
   * <li>The lock is acquired by the current thread; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
   * current thread, and interruption of lock acquisition is supported.
   * </ul>
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while acquiring the
   * lock, and interruption of lock acquisition is supported,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   *
   * @throws InterruptedException if the current thread is
   *         interrupted while acquiring the lock (and interruption
   *         of lock acquisition is supported).
   */
  void lockInterruptibly() throws InterruptedException;

  /**
   * Acquires the lock only if it is free at the time of invocation.
   *
   * <p>Acquires the lock if it is available and returns immediately
   * with the value {@code true}.
   * If the lock is not available then this method will return
   * immediately with the value {@code false}.
   *
   * @return {@code true} if the lock was acquired and
   *         {@code false} otherwise
   */
  boolean tryLock();

  /**
   * Acquires the lock if it is free within the given waiting time and the
   * current thread has not been {@linkplain Thread#interrupt interrupted}.
   *
   * <p>If the lock is available this method returns immediately
   * with the value {@code true}.
   * If the lock is not available then
   * the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until one of three things happens:
   * <ul>
   * <li>The lock is acquired by the current thread; or
   * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
   * current thread, and interruption of lock acquisition is supported; or
   * <li>The specified waiting time elapses
   * </ul>
   *
   * <p>If the lock is acquired then the value {@code true} is returned.
   *
   * <p>If the current thread:
   * <ul>
   * <li>has its interrupted status set on entry to this method; or
   * <li>is {@linkplain Thread#interrupt interrupted} while acquiring
   * the lock, and interruption of lock acquisition is supported,
   * </ul>
   * then {@link InterruptedException} is thrown and the current thread's
   * interrupted status is cleared.
   *
   * <p>If the specified waiting time elapses then the value {@code false}
   * is returned.
   * If the time is
   * less than or equal to zero, the method will not wait at all.
   *
   * @param msTime the maximum time to wait for the lock in milliseconds
   * @return {@code true} if the lock was acquired and {@code false}
   *         if the waiting time elapsed before the lock was acquired
   *
   * @throws InterruptedException if the current thread is interrupted
   *         while acquiring the lock (and interruption of lock
   *         acquisition is supported)
   */
  boolean tryLock(long msTime) throws InterruptedException;

  /**
   * Releases the lock.
   *
   */
  void unlock();

  /**
   * Returns a new {@link C} instance that is bound to this
   * {@code L} instance.
   *
   * <p>Before waiting on the condition the lock must be held by the
   * current thread.
   * A call to {@link C#await()} will atomically release the lock
   * before waiting and re-acquire the lock before the wait returns.
   *
   * @return A new {@link C} instance for this {@code L} instance
   * @throws UnsupportedOperationException if this {@code L}
   *         implementation does not support conditions
   */
  C getNewCondition();
  
  /**
   * These methods are the same ones on
   * the JDK 5 version java.util.concurrent.locks.Condition
   * @see CFactory
   * @author darrel
   */
  public interface C {
    /**
     * Causes the current thread to wait until it is signalled or
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>The lock associated with this {@code Condition} is atomically
     * released and the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until <em>one</em> of four things happens:
     * <ul>
     * <li>Some other thread invokes the {@link #signal} method for this
     * {@code Condition} and the current thread happens to be chosen as the
     * thread to be awakened; or
     * <li>Some other thread invokes the {@link #signalAll} method for this
     * {@code Condition}; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread, and interruption of thread suspension is supported; or
     * <li>A &quot;<em>spurious wakeup</em>&quot; occurs.
     * </ul>
     * <p>In all cases, before this method can return the current thread must
     * re-acquire the lock associated with this condition. When the
     * thread returns it is <em>guaranteed</em> to hold this lock.
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * and interruption of thread suspension is supported,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared. It is not specified, in the first
     * case, whether or not the test for interruption occurs before the lock
     * is released.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         (and interruption of thread suspension is supported)
     */
    void await() throws InterruptedException;

    /**
     * Causes the current thread to wait until it is signalled.
     *
     * <p>The lock associated with this condition is atomically
     * released and the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until <em>one</em> of three things happens:
     * <ul>
     * <li>Some other thread invokes the {@link #signal} method for this
     * {@code C} and the current thread happens to be chosen as the
     * thread to be awakened; or
     * <li>Some other thread invokes the {@link #signalAll} method for this
     * {@code C}; or
     * <li>A &quot;<em>spurious wakeup</em>&quot; occurs.
     * </ul>
     *
     * <p>In all cases, before this method can return the current thread must
     * re-acquire the lock associated with this condition. When the
     * thread returns it is <em>guaranteed</em> to hold this lock.
     *
     * <p>If the current thread's interrupted status is set when it enters
     * this method, or it is {@linkplain Thread#interrupt interrupted}
     * while waiting, it will continue to wait until signalled. When it finally
     * returns from this method its interrupted status will still
     * be set.
     */
    void awaitUninterruptibly();

    /**
     * Causes the current thread to wait until it is signalled or interrupted,
     * or the specified waiting time elapses.
     *
     * @param msTime the maximum time to wait in milliseconds
     * @return {@code false} if the waiting time detectably elapsed
     *         before return from the method, else {@code true}
     * @throws InterruptedException if the current thread is interrupted
     *         (and interruption of thread suspension is supported)
     */
    boolean await(long msTime) throws InterruptedException;

    /**
     * Causes the current thread to wait until it is signalled or interrupted,
     * or the specified deadline elapses.
     *
     * <p>The lock associated with this condition is atomically
     * released and the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until <em>one</em> of five things happens:
     * <ul>
     * <li>Some other thread invokes the {@link #signal} method for this
     * {@code C} and the current thread happens to be chosen as the
     * thread to be awakened; or
     * <li>Some other thread invokes the {@link #signalAll} method for this
     * {@code C}; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread, and interruption of thread suspension is supported; or
     * <li>The specified deadline elapses; or
     * <li>A &quot;<em>spurious wakeup</em>&quot; occurs.
     * </ul>
     *
     * <p>In all cases, before this method can return the current thread must
     * re-acquire the lock associated with this condition. When the
     * thread returns it is <em>guaranteed</em> to hold this lock.
     *
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * and interruption of thread suspension is supported,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared. It is not specified, in the first
     * case, whether or not the test for interruption occurs before the lock
     * is released.
     *
     *
     * <p>The return value indicates whether the deadline has elapsed,
     * which can be used as follows:
     * <pre>
     * synchronized boolean aMethod(Date deadline) {
     *   boolean stillWaiting = true;
     *   while (!conditionBeingWaitedFor) {
     *     if (stillWaiting)
     *         stillWaiting = theCondition.awaitUntil(deadline);
     *      else
     *        return false;
     *   }
     *   // ...
     * }
     * </pre>
     *
     *
     * @param deadline the absolute time to wait until
     * @return {@code false} if the deadline has elapsed upon return, else
     *         {@code true}
     * @throws InterruptedException if the current thread is interrupted
     *         (and interruption of thread suspension is supported)
     */
    boolean awaitUntil(java.util.Date deadline) throws InterruptedException;

    /**
     * Wakes up one waiting thread.
     *
     * <p>If any threads are waiting on this condition then one
     * is selected for waking up. That thread must then re-acquire the
     * lock before returning from {@code await}.
     */
    void signal();

    /**
     * Wakes up all waiting threads.
     *
     * <p>If any threads are waiting on this condition then they are
     * all woken up. Each thread must re-acquire the lock before it can
     * return from {@code await}.
     */
    void signalAll();
  }
}
