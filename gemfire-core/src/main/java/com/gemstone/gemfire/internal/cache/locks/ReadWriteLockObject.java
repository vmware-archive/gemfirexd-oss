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

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.GemFireCache;

/**
 * Interface for reader-writer locks providing for arbitrary owners (unlike
 * always current thread for JDK's implementations) for checking during lock
 * release or for reentrancy. Some implementations like non-reentrant locks may
 * choose to ignore the owner completely, or provide it only for the write lock
 * for example. Other implementations may allow for null owner that will
 * completely bypass owner checking. This also uses {@link GemFireCache}'s
 * {@link CancelCriterion} for cancellation instead of throwing
 * {@link InterruptedException}s.
 * 
 * @see java.util.concurrent.locks.ReadWriteLock
 * 
 * @author swale
 * @since 7.0
 */
public interface ReadWriteLockObject extends TryLockObject {

  /**
   * An attempt to acquire reader lock should succeed when there are no writer
   * locks already taken (by calling {@link #attemptWriteLock}).
   * <p>
   * The method has best-effort semantics: The msecs bound cannot be guaranteed
   * to be a precise upper bound on wait time in Java. Implementations generally
   * can only attempt to return as soon as possible after the specified bound.
   * Also, timers in Java do not stop during garbage collection, so timeouts can
   * occur just because a GC intervened. So, msecs arguments should be used in a
   * coarse-grained manner. Further, implementations cannot always guarantee
   * that this method will return at all without blocking indefinitely when used
   * in unintended ways. For example, deadlocks may be encountered when called
   * in an unintended context.
   * <p>
   * 
   * @param msecs
   *          the number of milleseconds to wait. A value equal to zero means
   *          not to wait at all, while a value less than zero means to wait
   *          indefinitely. However, this may still require access to a
   *          synchronization lock, which can impose unbounded delay if there is
   *          a lot of contention among threads.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored; the owner is only used when acquiring read lock while
   *          still holding the write lock (for atomic lock downgrade, for
   *          example)
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public boolean attemptReadLock(long msecs, Object owner);

  /**
   * An attempt to acquire write lock should succeed when there are no read
   * locks already taken ({@link #attemptReadLock}).
   * <p>
   * The method has best-effort semantics: The msecs bound cannot be guaranteed
   * to be a precise upper bound on wait time in Java. Implementations generally
   * can only attempt to return as soon as possible after the specified bound.
   * Also, timers in Java do not stop during garbage collection, so timeouts can
   * occur just because a GC intervened. So, msecs arguments should be used in a
   * coarse-grained manner. Further, implementations cannot always guarantee
   * that this method will return at all without blocking indefinitely when used
   * in unintended ways. For example, deadlocks may be encountered when called
   * in an unintended context.
   * <p>
   * 
   * @param msecs
   *          The number of milleseconds to wait. A value equal to zero means
   *          not to wait at all, while a value less than zero means to wait
   *          indefinitely. However, this may still require access to a
   *          synchronization lock, which can impose unbounded delay if there is
   *          a lot of contention among threads.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored and {@link #releaseWriteLock(Object)} should also be
   *          passed null
   * 
   * @return true if lock acquired successfully, false if the attempt timed out
   */
  public boolean attemptWriteLock(long msecs, Object owner);

  /**
   * Releases in shared mode. Implemented by unblocking one or more threads if
   * {@link #tryReleaseShared} returns true.
   * <p>
   * Because release does not raise exceptions, it can be used in `finally'
   * clauses without requiring extra embedded try/catch blocks. But keep in mind
   * that as with any java method, implementations may still throw unchecked
   * exceptions such as Error or {@link IllegalMonitorStateException} or
   * NullPointerException when faced with uncontinuable errors. However, these
   * should normally only be caught by higher-level error handlers.
   * <p>
   * 
   * @throws IllegalMonitorStateException
   *           If releasing would place this synchronizer in an illegal state or
   *           lock is not held by the given owner. This exception must be
   *           thrown in a consistent fashion for synchronization to work
   *           correctly.
   */
  public void releaseReadLock() throws IllegalMonitorStateException;

  /**
   * Releases in exclusive mode. Implemented by unblocking one or more threads.
   * <p>
   * Because release does not raise exceptions, it can be used in `finally'
   * clauses without requiring extra embedded try/catch blocks. But keep in mind
   * that as with any java method, implementations may still throw unchecked
   * exceptions such as Error or {@link IllegalMonitorStateException} or
   * NullPointerException when faced with uncontinuable errors. However, these
   * should normally only be caught by higher-level error handlers.
   * 
   * @param owner
   *          the owner for this lock provided in {@link #attemptWriteLock}
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state or
   *           lock is not held by the given owner. This exception must be
   *           thrown in a consistent fashion for synchronization to work
   *           correctly.
   */
  public void releaseWriteLock(Object owner)
      throws IllegalMonitorStateException;

  /**
   * Current number of readers for this lock object. Useful only for debugging
   * purposes.
   */
  public int numReaders();

  /**
   * Return true if this object has been locked for write. Useful only for
   * debugging purposes.
   */
  public boolean isWriteLocked();
}
