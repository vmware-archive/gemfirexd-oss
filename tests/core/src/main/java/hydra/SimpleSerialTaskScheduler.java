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
 *  This class acts as a serial task scheduler for INITTASKs.  It is invoked
 *  when {@link Prms#doInitTasksSequentially} is true.  It operates like {@link
 *  SimpleConcurrentTaskScheduler} except that task(i) is run on one thread at a *  time.  The order in which clients run task(i) is unspecified.
 */
public class SimpleSerialTaskScheduler extends SimpleTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   */
  public SimpleSerialTaskScheduler( String name,
                                    Vector tasks,
                                    String purpose ) {
    super( name, tasks, purpose );
  }

  /**
   *  Executes the tasks using the existing clients.
   *  @return true if there were no fatal task errors or hangs
   */
  protected boolean executeTasks() {
    for ( int i = 0; i < this.tasks.size(); i++ ) {
      TestTask task = (TestTask) this.tasks.get( i );
      if ( task.batch() ) {
        scheduleInBatches( task );
      } else {
        schedule( task );
      }
      if (!executedNormallyOrNoHaltOrder()) {
        break;
      }
    }
    return schedulerResult();
  }

  /**
   *  Schedules the same batched task repeatedly on each client in turn until
   *  it is time to stop scheduling the task.  Waits for each client to finish
   *  all of its batches before scheduling the next client.
   */
  private void scheduleInBatches( TestTask task ) {
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        int count = 0;
        long now = 0;
        do {
          if ( schedule( client, task, now ) ) {
            ++count;
            now = waitForClientToFinish( client );
          }
        } while ( ! terminateScheduling( client, task, count, now ) );
      }
    }
  }

  /**
   *  Schedules the same task on each client in turn.  Waits for each client to
   *  finish its task before scheduling the next client.
   */
  private void schedule( TestTask task ) {
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        long now = System.currentTimeMillis();
        if ( schedule( client, task, now ) ) {
          waitForClientToFinish( client );
        }
        if ( ! executedNormally() ) {
          break;
        }
      }
    }
  }
}
