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
 *  This class acts as the task scheduler for scheduling TASKs onto clients in
 *  a round robin fashion (with respect to the logical hydra client thread
 *  IDs).  A client that is ineligible when it comes up for scheduling loses
 *  its turn.  Tasks are always executed sequentially.  It is invoked when
 *  {@link Prms#roundRobin} is true.
 */
public class ComplexRoundRobinTaskScheduler extends ComplexTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of executing
   *  the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging.
   *  @param tasks the tasks to be scheduled.
   *  @param totalTaskTimeSec total amount of time to execute tasks with
   *         this scheduler (overrides the task control parameter
   *         <code>maxTimesToRun</code>).
   */
  public ComplexRoundRobinTaskScheduler( String name,
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

    // get an array of all clients, sorted by logical thread id
    SortedSet tmp = new TreeSet();
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      tmp.addAll( vm.getClients().values() );
    }
    Object[] clients = tmp.toArray();
    if ( log.fineEnabled() ) {
      log.fine( "Sorted array of clients: " + clients );
    }

    // start the clock
    initializeTaskTimes();

    int index = 0;     // index into the set of all clients
    int count = 0;     // number of clients scheduled this round
    int lastCount = 1; // number of clients scheduled last round
    long now = 0;      // current time
    do {
      // select the next client in round-robin order
      ClientRecord client = (ClientRecord) clients[ index ];
      index = (index + 1) % clients.length;

      // schedule the client and wait for it to finish
      if ( ! client.vm().isDynamic() ) {
        now = System.currentTimeMillis();
        if ( schedule( client, now, this ) ) {
          ++count;
          now = waitForClientToFinish( client );
        }
      }

      // update the scheduling counts
      if ( index == 0 ) {  // see if we have completed a round
        lastCount = count; // save the count from the last round
        count = 0;         // reset the count for the next round
      }

    } while ( ! terminateScheduling( lastCount, now ) );

    log.info(this.name + " terminating scheduling because " + this.termination);

    // wait for dynamic schedulers to terminate
    waitForDynamicThreadsToFinish();

    return schedulerResult();
  }
}
