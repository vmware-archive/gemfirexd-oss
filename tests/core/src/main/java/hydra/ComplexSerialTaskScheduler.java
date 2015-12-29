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
 *  sequentially in a random fashion.  It is invoked when {@link
 *  Prms#serialExecution} is true but {@link Prms#roundRobin} is false.
 */
public class ComplexSerialTaskScheduler extends ComplexTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging.
   *  @param tasks the tasks to be scheduled.
   *  @param totalTaskTimeSec total amount of time to execute tasks with
   *         this scheduler (overrides the task control parameter
   *         <code>maxTimesToRun</code>).
   */
  public ComplexSerialTaskScheduler( String name,
                                     Vector tasks,
                                     String purpose,
                                     long totalTaskTimeSec ) {
    super( name, tasks, purpose, totalTaskTimeSec );
  }

  /**
   *  Executes the tasks using the existing clients.
   *  @return true if there were no fatal task errors or hangs
   */
  protected boolean executeTasks() {

    // start the clock
    initializeTaskTimes();

    int count; // number of clients eligible for scheduling this round
    long now;  // current time
    do {
      // find the eligble clients
      Vector clients = new Vector();
      now = System.currentTimeMillis();
      for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        if ( ! vm.isDynamic() ) {
          for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
            ClientRecord client = (ClientRecord) j.next();
            if ( hasTask( client, now ) ) {
              clients.add( client );
            }
          }
        }
      }
      count = clients.size();
      if ( count != 0 ) {
        // select a random eligible client
        int i = TestConfig.tab().getRandGen().nextInt( 0, count - 1 );
        ClientRecord client = (ClientRecord) clients.get( i );

        // schedule the client and wait for it to finish
        now = System.currentTimeMillis();
        if ( schedule( client, now, this ) ) {
          now = waitForClientToFinish( client );
        }
      }
    } while ( ! terminateScheduling( count, now ) );

    log.info(this.name + " terminating scheduling because " + this.termination);

    // wait for dynamic schedulers to terminate
    waitForDynamicThreadsToFinish();

    return schedulerResult();
  }
}
