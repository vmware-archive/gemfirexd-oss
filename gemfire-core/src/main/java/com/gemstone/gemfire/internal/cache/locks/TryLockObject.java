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
 * Encapsulates immediate lock acquisition without any timeout. Implementations
 * will typically use an integer volatile state that is manipulated atomically
 * using JDK5's CAS primitives.
 * 
 * @author swale
 * @since 7.0
 * @see java.util.concurrent.locks.AbstractQueuedSynchronizer
 */
public interface TryLockObject {

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
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the acquire argument. This value is always the one passed to an
   *          acquire method, or is the value saved on entry to a condition
   *          wait. The value is otherwise uninterpreted and can represent
   *          anything you like.
   * @param ownerId
   *          The owner which will be used to check re-entrancy instead of
   *          current Thread.
   * @param context
   *          Any context object can be provided here (that can be passed to
   *          methods like
   *          {@link ExclusiveSharedSynchronizer#getOwnerId(Object)} by
   *          implementations).
   * 
   * @return a negative value on failure; zero if acquisition in exclusive mode
   *         succeeded but no subsequent exclusive-mode acquire can succeed; and
   *         a positive value if acquisition in exclusive mode succeeded and
   *         subsequent exclusive-mode acquires might also succeed, in which
   *         case a subsequent waiting thread must check availability. Upon
   *         success, this object has been acquired.
   * 
   * @throws IllegalMonitorStateException
   *           if acquiring would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if exclusive mode is not supported
   */
  public int tryAcquire(int arg, Object ownerId, Object context);

  /**
   * Attempts to set the state to reflect a release in exclusive mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the release argument. This value is always the one passed to a
   *          release method, or the current state value upon entry to a
   *          condition wait. The value is otherwise uninterpreted and can
   *          represent anything you like.
   * @param ownerId
   *          The owner which will be used to check re-entrancy instead of
   *          current Thread.
   * @param context
   *          Any context object can be provided here (that can be passed to
   *          methods like
   *          {@link ExclusiveSharedSynchronizer#getOwnerId(Object)} by
   *          implementations).
   * 
   * @return {@code true} if this object is now in a fully released state, so
   *         that any waiting threads may attempt to acquire; and {@code false}
   *         otherwise.
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if exclusive mode is not supported
   */
  public boolean tryRelease(int arg, Object ownerId, Object context);

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
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the acquire argument. This value is always the one passed to an
   *          acquire method, or is the value saved on entry to a condition
   *          wait. The value is otherwise uninterpreted and can represent
   *          anything you like.
   * @param ownerId
   *          The owner which will be used to check re-entrancy instead of
   *          current Thread.
   * @param context
   *          Any context object can be provided here (that can be passed to
   *          methods like
   *          {@link ExclusiveSharedSynchronizer#getOwnerId(Object)} by
   *          implementations).
   * 
   * @return a negative value on failure; zero if acquisition in shared mode
   *         succeeded but no subsequent shared-mode acquire can succeed; and a
   *         positive value if acquisition in shared mode succeeded and
   *         subsequent shared-mode acquires might also succeed, in which case a
   *         subsequent waiting thread must check availability. (Support for
   *         three different return values enables this method to be used in
   *         contexts where acquires only sometimes act exclusively.) Upon
   *         success, this object has been acquired.
   * 
   * @throws IllegalMonitorStateException
   *           if acquiring would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if shared mode is not supported
   */
  public int tryAcquireShared(int arg, Object ownerId, Object context);

  /**
   * Attempts to set the state to reflect a release in shared mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the release argument. This value is always the one passed to a
   *          release method, or the current state value upon entry to a
   *          condition wait. The value is otherwise uninterpreted and can
   *          represent anything you like.
   * @param ownerId
   *          The owner which will be used to check re-entrancy instead of
   *          current Thread.
   * @param context
   *          Any context object can be provided here (that can be passed to
   *          methods like
   *          {@link ExclusiveSharedSynchronizer#getOwnerId(Object)} by
   *          implementations).
   * 
   * @return {@code true} if this release of shared mode may permit a waiting
   *         acquire (shared or exclusive) to succeed; and {@code false}
   *         otherwise
   * 
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if shared mode is not supported
   */
  public boolean tryReleaseShared(int arg, Object ownerId, Object context);

  /**
   * Returns the current value of synchronization state. This operation has
   * memory semantics of a <tt>volatile</tt> read.
   * 
   * @return current state value
   */
  public int getState();
}
