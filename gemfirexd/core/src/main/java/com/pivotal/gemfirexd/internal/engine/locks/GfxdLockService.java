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

package com.pivotal.gemfirexd.internal.engine.locks;

import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.LeaseExpiredException;
import com.gemstone.gemfire.distributed.LockNotHeldException;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * Interface class providing for reader/writer locking semantics for
 * {@link GfxdLockable}s. Also provides for accounting of the locks taken so far
 * indexed by their {@link GfxdLockable#getName()}s so that those can be dumped
 * in case of lock timeouts etc.
 * 
 * @author swale
 */
public interface GfxdLockService {

  /**
   * Denotes the state of the read lock for current thread i.e. one of
   * {@link #UNKNOWN} meaning the state is not known, {@link #HELD} meaning the
   * current thread holds the lock for sure and {@link #NOT_HELD} meaning the
   * current thread does not hold the lock for sure.
   */
  public static enum ReadLockState {
    UNKNOWN, HELD, NOT_HELD
  }

  /**
   * <p>
   * Attempts to acquire a read lock on object <code>name</code>. Returns
   * <code>true</code> as soon as the lock is acquired. If a write lock on the
   * same object is currently held by another thread in this or any other
   * process in the distributed system (by a call to
   * {@link #writeLock(Object, long, long, boolean)}), this method keeps trying
   * to acquire the lock for up to <code>waitTimeMillis</code> before giving up
   * and returning <code>false</code>. If the lock is acquired, it is held until
   * <code>readUnlock(Object)</code> is invoked.
   * </p>
   * 
   * <p>
   * Obtaining a read lock is a local operation only and guaranteed to involve
   * no messaging.
   * 
   * @param name
   *          The name of the lock to acquire in this service. This object must
   *          conform to the general contract of <code>equals(Object)</code> and
   *          <code>hashCode()</code> as described in
   *          {@link java.lang.Object#hashCode()}.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored; the owner is only used when acquiring read lock while
   *          still holding the write lock (for atomic lock downgrade, for
   *          example); in most cases one can safely use
   *          {@link #newCurrentOwner()} if there are no special needs
   * @param waitTimeMillis
   *          The number of milliseconds to try to acquire the lock before
   *          giving up and returning false. A value of -1 causes this method to
   *          block until the lock is acquired. A value of 0 causes this method
   *          to return false without waiting for the lock if the lock is held
   *          by another member or thread.
   * 
   * @return true if the lock was acquired, false if the timeout
   *         <code>waitTimeMillis</code> passed without acquiring the lock.
   */
  public boolean readLock(Object name, Object owner, long waitTimeMillis);

  /**
   * Attempts to acquire a read lock on given {@link GfxdLockable}. This method
   * has the same semantics as {@link #readLock(Object, long)} with the
   * optimization that the {@link GfxdReadWriteLock} as obtained using
   * {@link GfxdLockable#getReadWriteLock()} is used if available instead of
   * looking up from the global map.
   * 
   * @param lockable
   *          The {@link GfxdLockable} to acquire in this service. This object
   *          must conform to the general contract of
   *          <code>equals(Object)</code> and <code>hashCode()</code> as
   *          described in {@link java.lang.Object#hashCode()}.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored; the owner is only used when acquiring read lock while
   *          still holding the write lock (for atomic lock downgrade, for
   *          example); in most cases one can safely use
   *          {@link #newCurrentOwner()} if there are no special needs
   * @param waitTimeMillis
   *          The number of milliseconds to try to acquire the lock before
   *          giving up and returning false. A value of -1 causes this method to
   *          block until the lock is acquired. A value of 0 causes this method
   *          to return false without waiting for the lock if the lock is held
   *          by another member or thread.
   * 
   * @return true if the lock was acquired, false if the timeout
   *         <code>waitTimeMillis</code> passed without acquiring the lock.
   * 
   * @see #readLock(Object, long)
   */
  public boolean readLock(GfxdLockable lockable, Object owner,
      long waitTimeMillis);

  /**
   * Release the read lock previously granted for the given <code>name</code>.
   * 
   * @param name
   *          the object to unlock in this service
   * 
   * @throws LockNotHeldException
   *           if the current thread is not the owner of this lock
   */
  public void readUnlock(Object name);

  /**
   * Release the read lock previously granted for the given {@link GfxdLockable}
   * . This method has the same semantics as {@link #readUnlock(Object)} with
   * the optimization that the {@link GfxdReadWriteLock} as obtained using
   * {@link GfxdLockable#getReadWriteLock()} is used if available instead of
   * looking up from the global map.
   * 
   * @param lockable
   *          the {@link GfxdLockable} to unlock in this service
   * 
   * @throws LockNotHeldException
   *           if the current thread is not the owner of this lock
   * 
   * @see #readUnlock(Object)
   */
  public void readUnlock(GfxdLockable lockable);

  /**
   * Returns the current execution context as the owner. Usually it will be
   * either the current thread or current thread combined with VM ID (depending
   * on whether the lock service is local or distributed respectively).
   */
  public Object newCurrentOwner();

  /**
   * Returns a {@link ReadLockState} denoting the state of the given lock for
   * the given owner.
   * 
   * @param name
   *          object to be checked for read lock
   * 
   * @throws LockServiceDestroyedException
   *           if the service has been destroyed
   */
  public ReadLockState hasReadLock(Object name);

  /**
   * <p>
   * Attempts to acquire a write lock on object <code>name</code>. Returns
   * <code>true</code> as soon as the lock is acquired. If a read or write lock
   * on the same object is currently held by another thread, this method keeps
   * trying to acquire the lock for up to <code>waitTimeMillis</code> before
   * giving up and returning <code>false</code>. If the lock is acquired, it is
   * held until <code>writeUnlock(Object, boolean)</code> is invoked, or until
   * <code>leaseTimeMillis</code> milliseconds have passed since the lock was
   * granted - whichever comes first.
   * </p>
   * 
   * @param name
   *          the name of the lock to acquire in this service. This object must
   *          conform to the general contract of <code>equals(Object)</code> and
   *          <code>hashCode()</code> as described in
   *          {@link java.lang.Object#hashCode()}.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored and {@link #writeUnlock} should also be passed null; in
   *          most cases one can safely use {@link #newCurrentOwner()} if there
   *          are no special needs
   * @param waitTimeMillis
   *          the number of milliseconds to try to acquire the lock before
   *          giving up and returning false. A value of -1 causes this method to
   *          block until the lock is acquired. A value of 0 causes this method
   *          to return false without waiting for the lock if the lock is held
   *          by another member or thread.
   * @param leaseTimeMillis
   *          the number of milliseconds to hold the lock after granting it,
   *          before automatically releasing it if it hasn't already been
   *          released by invoking {@link #writeUnlock(Object, boolean)}. If
   *          <code>leaseTimeMillis</code> is -1, hold the lock until explicitly
   *          unlocked.
   * 
   * @return true if the lock was acquired, false if the timeout
   *         <code>waitTimeMillis</code> passed without acquiring the lock.
   * 
   * @throws LockServiceDestroyedException
   *           if this lock service has been destroyed
   */
  public boolean writeLock(Object name, Object owner, long waitTimeMillis,
      long leaseTimeMillis);

  /**
   * Attempts to acquire a write lock on given {@link GfxdLockable} object. This
   * method has the same semantics as {@link #writeLock(Object, Object, long)}
   * with the optimization that when possible the {@link GfxdReadWriteLock} as
   * obtained using {@link GfxdLockable#getReadWriteLock()} is used if available
   * instead of looking up from the global map.
   * 
   * @param name
   *          The {@link GfxdLockable} to acquire in this service. This object
   *          must conform to the general contract of
   *          <code>equals(Object)</code> and <code>hashCode()</code> as
   *          described in {@link java.lang.Object#hashCode()}.
   * @param owner
   *          the owner for this lock; can be null in which case ownership is
   *          ignored and {@link #localWriteUnlock} should also be passed null;
   *          in most cases one can safely use {@link #newCurrentOwner()} if
   *          there are no special needs
   * @param waitTimeMillis
   *          The number of milliseconds to try to acquire the lock before
   *          giving up and returning false. A value of -1 causes this method to
   *          block until the lock is acquired. A value of 0 causes this method
   *          to return false without waiting for the lock if the lock is held
   *          by another member or thread.
   * @param leaseTimeMillis
   *          the number of milliseconds to hold the lock after granting it,
   *          before automatically releasing it if it hasn't already been
   *          released by invoking {@link #writeUnlock(Object, boolean)}. If
   *          <code>leaseTimeMillis</code> is -1, hold the lock until explicitly
   *          unlocked.
   * 
   * @return true if the lock was acquired, false if the timeout
   *         <code>waitTimeMillis</code> passed without acquiring the lock.
   * 
   * @throws LockServiceDestroyedException
   *           if this lock service has been destroyed
   */
  public boolean writeLock(GfxdLockable lockable, Object owner,
      long waitTimeMillis, long leaseTimeMillis);

  /**
   * Release the write lock previously granted for the given <code>name</code>.
   * This should have been granted by a previous call to {@link #writeLock} on
   * this node.
   * 
   * @param name
   *          the object to unlock in this service.
   * @param owner
   *          the owner for this lock provided in {@link #writeLock}
   * 
   * @throws LockNotHeldException
   *           if the current thread is not the owner of this lock
   * 
   * @throws LeaseExpiredException
   *           if the current thread was the owner of this lock, but it's lease
   *           has expired.
   * 
   * @throws LockServiceDestroyedException
   *           if the service has been destroyed
   */
  public void writeUnlock(Object name, Object owner);

  /**
   * Release the write lock previously granted for the given
   * <code>GfxdLockable</code> object. This should have been granted by a
   * previous call to {@link #writeLock} on this node. This method has the same
   * semantics as {@link #writeUnlock(Object, boolean)} with the optimization
   * that when possible the {@link GfxdReadWriteLock} as obtained using
   * {@link GfxdLockable#getReadWriteLock()} is used if available instead of
   * looking up from the global map.
   * 
   * @param lockable
   *          the {@link GfxdLockable} to unlock in this service
   * @param owner
   *          the owner for this lock provided in {@link #localWriteLock}
   * 
   * @throws LockNotHeldException
   *           if the current thread is not the owner of this lock and
   *           checkOwner parameter is true
   * 
   * @throws LockServiceDestroyedException
   *           if the service has been destroyed
   */
  public void writeUnlock(GfxdLockable lockable, Object owner);

  /**
   * <p>
   * Returns true if the given owner holds a local write lock on the given
   * object.
   * </p>
   * 
   * @param name
   *          object to be checked for write lock
   * @param owner
   *          the owner for this lock provided when acquiring write lock
   * 
   * @throws LockServiceDestroyedException
   *           if the service has been destroyed
   */
  public boolean hasWriteLock(Object name, Object owner);

  /**
   * <p>
   * Returns the current local write lock owner for given object, or null if no
   * write is currently held.
   * </p>
   * 
   * @param name
   *          object to be checked for write lock
   * 
   * @throws LockServiceDestroyedException
   *           if the service has been destroyed
   */
  public Object getWriteLockOwner(Object name);

  /**
   * Get an {@link GfxdLockService} for lock read/write locks.
   */
  public GfxdLockService getLocalLockService();

  /**
   * Free internal resources associated with the given <code>name</code>. This
   * may reduce this VM's memory use, but may also prohibit performance
   * optimizations if <code>name</code> is subsequently locked in this VM.
   * 
   * @throws LockServiceDestroyedException
   *           if this service has been destroyed
   */
  public void freeResources(Object name);

  /**
   * Generate TimeoutException for given lock object name or
   * {@link GfxdLockable} with requested lock owner.
   */
  public TimeoutException getLockTimeoutRuntimeException(Object lockObject,
      Object owner, boolean dumpAllLocks);

  /**
   * Generate a {@link StandardException} for lock timeout for given lock object
   * name or {@link GfxdLockable} with requested lock owner.
   */
  public StandardException getLockTimeoutException(Object lockObject,
      Object owner, boolean dumpAllLocks);
}
