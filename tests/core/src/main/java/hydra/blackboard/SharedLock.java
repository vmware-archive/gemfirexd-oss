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

package hydra.blackboard;

/**
 * A nonreentrant lock useful for synchronizing across hydra client VMs.
 * For example, it can be used to atomically read and update the shared map
 * or to serialize arbitrary sections of test code.  Can be used from both
 * logical hydra threads (across task boundaries) and from non-hydra threads
 * such as product-spawned listener threads.
 */
public interface SharedLock {

  /**
   * Locks the lock on behalf of this thread, waiting forever.  Use with care
   * to avoid deadlock and excessive overhead.  The time spent holding a lock
   * should be minimal to avoid long-lived RMI calls.
   *
   * @throws HydraRuntimeException if the lock is already locked by this
   *                               logical thread.
   */
  public void lock();

  /**
   * Unlocks the lock on behalf of this thread.
   *
   * @throws HydraRuntimeException if the lock is already unlocked or is
   *                               owned by another logical thread.
   */
  public void unlock();
  
  /**
   * Get a condition object associated with this lock, which can be used to
   * signal from one VM to another.
   * 
   * The time spent waiting on a condition should be minimal to avoid long-lived
   * RMI calls.
   */
  public SharedCondition getCondition(String name);
}
