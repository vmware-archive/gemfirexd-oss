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
 * A condition object useful for synchronizing between Hydra VMs. One VM can
 * wait on the condition, and another VM can signal the condition. You must
 * aquire the lock associated with this condition before calling await or signal
 * 
 * @author dsmith
 * 
 */
public interface SharedCondition {
  /**
   * Wait for another thread to signal this condition.
   * 
   * Note that this wait is subject to spurious wakups, so the caller
   * should wait in a loop with some sort of shared flag (perhaps
   * by using a hydra blackboard).
   * 
   * @throws InterruptedException
   *                 if the waiting thread on the master was interrupted.
   */
  public void await() throws InterruptedException;

  /**
   * Wake up one of the waiting threads
   */
  public void signal();

  /**
   * Wake up all of the waiting threads
   */
  public void signalAll();

}
