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

package hydra;

import java.util.*;

/*
 *  This is the same as its superclass except that it operates on a client
 *  vm managed dynamically through the {@link ClientVmMgr} API.
 */
public class DynamicConcurrentTaskScheduler
extends SimpleConcurrentTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   *  @param vm the client vm record to schedule
   */
  public DynamicConcurrentTaskScheduler( String name,
                                         Vector tasks,
                                         String purpose,
                                         ClientVmRecord vm,
                                         boolean disconnect ) {
    super( name, tasks, purpose );
    this.vms = new HashMap();
    this.vms.put( vm.getVmid(), vm );
    this.disconnect = disconnect;
  }

  /**
   *  Overrides {@link BaseTaskScheduler#executeTasks(boolean,long)} to allow
   *  current tasks to complete and to skip executing tasks if the test is
   *  failing.
   */
  public boolean executeTasks( boolean haltIfBadResult,
                               long maxResultWaitSec ) {

    initExecutionParameters( haltIfBadResult, maxResultWaitSec );

    if ( executedNormally() ) {
      waitForClientsToFinish();   // wait for clients to complete current tasks

      if ( executedNormally() ) {
        initClientRuntime();      // initialize client runtime state
        if ( this.tasks != null ) {
          super.executeTasks();   // execute supplied tasks, if any
        }
      }
    }
    return schedulerResult();
  }
}
