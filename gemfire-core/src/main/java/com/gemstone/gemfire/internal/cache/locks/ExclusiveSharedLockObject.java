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

/**
 * Lock interface for an object that provides for different modes of operation
 * allowing for shared or exclusive or intermediate semantics. This also allows
 * the owner to be an arbitrary object rather than the current Thread.
 * Implementations may choose to provide reentrancy for the owner though this
 * interface does not require it.
 * 
 * a) exclusive (EX): no other entity can own this lock; if reentrancy has been
 * provided by an implementation then the same owner will be able to lock the
 * object in exclusive mode again
 * 
 * b) exclusive-shared (EX_SH): allows other entities requesting shared locks to
 * go through but no other exclusive or exclusive-shared locks; if reentrancy
 * has been provided by an implementation then the same owner will be able to
 * lock the object in exclusive-shared mode again
 * 
 * c) shared (SH): allows multiple entities to have shared locks on the object
 * concurrently but no EX mode locks and exactly one EX_SH mode lock
 * 
 * d) read-only (READ_ONLY): allows multiple entities to read the object
 * concurrently in either SH or READ_ONLY modes but no EX mode or EX_SH mode
 * locks
 * 
 * @author swale
 * @since 7.0
 */
public interface ExclusiveSharedLockObject {

  /**
   * Return the current owner, if any, of this lock.
   * 
   * @param context
   *          context object passed in to the {@link #attemptLock} and
   *          {@link #releaseLock} methods
   */
  Object getOwnerId(Object context);

  /**
   * Attempt to lock the object in given {@link LockMode} subject to given
   * timeout as determined from {@link LockingPolicy#getTimeout}.
   * <p>
   * An attempt to acquire an SH lock should return immediately(successfully)
   * irrespective of the number of SH locks and upto a single EX_SH lock already
   * taken. It should wait only when there are already max number of allowed SH
   * locks (implementation dependent) taken or an EX lock has been taken.
   * <p>
   * An attempt to acquire an READ_ONLY lock should return
   * immediately(successfully) irrespective of the number of SH/READ_ONLY locks
   * already taken. It should wait only when there are already max number of
   * allowed SH locks (implementation dependent) taken or one of EX/EX_SH lock
   * has been taken.
   * <p>
   * An attempt to acquire EX lock should succeed when there are no other SH,
   * EX_SH or EX locks already taken for this object. Else it will wait
   * honouring the provided timeout as determined from
   * {@link LockingPolicy#getTimeout}.
   * <p>
   * An attempt to acquire EX_SH lock should succeed when there are no other
   * EX_SH or EX locks already taken for this object. Else it will wait
   * honouring the provided timeout as determined from
   * {@link LockingPolicy#getTimeout}.
   * <p>
   * The method has best-effort semantics: The timeout bound cannot be
   * guaranteed to be a precise upper bound on wait time in Java.
   * Implementations generally can only attempt to return as soon as possible
   * after the specified bound. Also, timers in Java do not stop during garbage
   * collection, so timeouts can occur just because a GC intervened. So, timeout
   * should be used in a coarse-grained manner. Further, implementations cannot
   * always guarantee that this method will return at all without blocking
   * indefinitely when used in unintended ways. For example, deadlocks may be
   * encountered when called in an unintended context.
   * <p>
   * 
   * @param mode
   *          the {@link LockMode} to be used; one of {@link LockMode#SH},
   *          {@link LockMode#READ_ONLY}, {@link LockMode#EX} or
   *          {@link LockMode#EX_SH}
   * @param flags
   *          any additional locking flags to be passed
   * @param lockPolicy
   *          The {@link LockingPolicy} that is used to determine number of
   *          milliseconds to wait by invoking the
   *          {@link LockingPolicy#getTimeout} method. A value equal to zero
   *          means not to wait at all, while a value less than zero means to
   *          wait indefinitely. However, this may still require access to a
   *          synchronization lock, which can impose unbounded delay if there is
   *          a lot of contention among threads.
   * @param msecs
   *          the timeout passed to the {@link LockingPolicy#getTimeout} method
   * @param owner
   *          The owner of this {@link LockMode#EX} or {@link LockMode#EX_SH}
   *          lock which can be used to check for re-entrancy by
   *          implementations. This can be an arbitrary object and not limited
   *          to the current Thread. Implementations may choose to ignore this
   *          parameter completely but even those that honour it, this will be
   *          ignored for {@link LockMode#SH} locks.
   * @param context
   *          When required an arbitrary object to specify the context can be
   *          passed here.
   * @return true if acquired
   */
  boolean attemptLock(LockMode mode, int flags, LockingPolicy lockPolicy,
      long msecs, Object owner, Object context);

  /**
   * Release the lock for the object as acquired by a previous call to
   * {@link #attemptLock(LockMode, int, LockingPolicy, long, Object, Object)}.
   * <p>
   * Because release does not raise exceptions, it can be used in `finally'
   * clauses without requiring extra embedded try/catch blocks. But keep in mind
   * that as with any java method, implementations may still throw unchecked
   * exceptions such as Error or NullPointerException or
   * IllegalMonitorStateException when faced with uncontinuable errors. However,
   * these should normally only be caught by higher-level error handlers.
   * 
   * @param mode
   *          the {@link LockMode} to be used; one of {@link LockMode#SH},
   *          {@link LockMode#READ_ONLY}, {@link LockMode#EX} or
   *          {@link LockMode#EX_SH}
   * @param releaseAll
   *          if set to true then all shared locks on the object (possibly
   *          acquired even by other entities) will be released, else only a
   *          single shared count is decremented
   * @param owner
   *          The owner of this {@link LockMode#EX} or {@link LockMode#EX_SH}
   *          lock which can be used to check for re-entrancy by
   *          implementations. This can be an arbitrary object and not limited
   *          to the current Thread. Implementations may choose to ignore this
   *          parameter completely but even those that honour it, this will be
   *          ignored for {@link LockMode#SH} locks.
   * @param context
   *          When required an arbitrary object to specify the context can be
   *          passed here.
   */
  void releaseLock(LockMode mode, boolean releaseAll, Object owner,
      Object context);

  /** Return the number of active {@link LockMode#SH} locks on this object. */
  int numSharedLocks();

  /**
   * Return the number of active {@link LockMode#READ_ONLY} locks on this
   * object.
   */
  int numReadOnlyLocks();

  /**
   * Return true if the given owner holds an {@link LockMode#EX} lock. This is
   * mostly for diagnostic purposes since implementations may choose to not
   * implement this method.
   */
  boolean hasExclusiveLock(Object owner, Object context);

  /**
   * Return true if the given owner holds an {@link LockMode#EX_SH} lock. This
   * is mostly for diagnostic purposes since implementations may choose to not
   * implement this method.
   */
  boolean hasExclusiveSharedLock(Object ownerId, Object context);

  /**
   * Returns the current value of synchronization state. This operation has
   * memory semantics of a <tt>volatile</tt> read.
   * 
   * Use the static methods in {@link ExclusiveSharedSynchronizer} like
   * {@link ExclusiveSharedSynchronizer#getLockModeFromState(int)}
   * {@link ExclusiveSharedSynchronizer#isExclusive(int)} to check for various
   * lock settings on this object.
   * 
   * @return current synchronization state value
   */
  int getState();

  /**
   * returns true if the entry has been locked for any operation (read,
   * read-only, write, exclusive)
   */
  boolean hasAnyLock();
}
