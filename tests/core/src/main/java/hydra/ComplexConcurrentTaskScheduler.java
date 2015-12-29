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

/** 
 *  This class acts as the task scheduler for scheduling TASKs onto clients
 *  concurrently.  It is invoked when both {@link Prms#serialExecution} and
 *  {@link Prms#roundRobin} are false.
 */
public class ComplexConcurrentTaskScheduler extends ComplexTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging.
   *  @param tasks the tasks to be scheduled.
   *  @param totalTaskTimeSec total amount of time to execute tasks with
   *         this scheduler (overrides the task control parameter
   *         <code>maxTimesToRun</code>).
   */
  public ComplexConcurrentTaskScheduler( String name,
                                         Vector tasks,
                                         String purpose,
                                         long totalTaskTimeSec ) {
    super( name, tasks, purpose, totalTaskTimeSec );
  }

  /**
   *  Executes the tasks on the existing clients.
   *  @return true if there were no fatal task errors or hangs
   */
  protected boolean executeTasks() {

    // start the clock
    initializeTaskTimes();

    int count; // number of clients scheduled this round
    long now;  // current time
    do {
      // schedule as many clients as possible
      count = 0;
      now = System.currentTimeMillis();
      for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        if ( ! vm.isDynamic() ) {
          for (Iterator j = vm.getClients().values().iterator(); j.hasNext();) {
            ClientRecord client = (ClientRecord) j.next();
            if ( schedule( client, now, this ) ) {
              ++count;
            }
          }
        }
      }
    } while ( ! terminateScheduling( count, now ) );

    log.info(this.name + " terminating scheduling because " + this.termination);

    if ( executedNormally() ) {
      // wait for clients to complete current tasks
      waitForClientsToFinish();
    }

    // wait for dynamic schedulers to terminate
    waitForDynamicThreadsToFinish();

    // return termination status
    return schedulerResult();
  }
}
