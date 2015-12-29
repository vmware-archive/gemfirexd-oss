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
 *  This class acts as a concurrent task scheduler for INITTASKs and CLOSETASKs.
 *  It is invoked on INITTASKs when {@link Prms#doInitTasksSequentially} is
 *  false, and is always used for CLOSETASKs.
 *  <p>
 *  Tasks are run in the order given in the test configuration.  All client
 *  threads designated to run a task run it concurrently, but task(i+1) is only
 *  scheduled after all clients running task(i) have completed.
 *  <p>Sample timeline with 3 clients running 3 tasks...
 *  <blockquote>
 *  <pre>
 *  c1:  t1---------------t2------     t3------    |
 *  c2:  t1---------                   t3----------|
 *  c3:  t1----           t2-----------t3---       |
 *  </pre>
 *  </blockquote>
 *  <p>
 *  Each task is executed only in client threads belonging to the threadgroups
 *  specified for that task by logical name in the test configuration file, or,
 *  if none are specified, in the client threads in each and every threadgroup
 *  defined in the file.  See {@link hydra.HydraThreadGroup} and
 *  <code>hydra_grammar.txt</code> for how to specify threadgroups.
 */
public class SimpleConcurrentTaskScheduler extends SimpleTaskScheduler {

  /**
   * Creates a task scheduler by the given name for the purpose of
   * executing the given set of tasks.
   * @param name a descriptive name of the tasks to use for logging
   * @param tasks the tasks to be scheduled
   */
  public SimpleConcurrentTaskScheduler( String name,
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
      } else if (task.sequential()) {
        scheduleSequentially(task);
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
   *  Repeatedly schedules the same batched task on clients until it is time to
   *  stop scheduling the task.  Waits for the clients to finish each batch.
   */
  private void scheduleInBatches( TestTask task ) {
    long now = 0;
    int count;
    Vector clients;
    do {
      clients = new Vector();
      count = 0;
      now = System.currentTimeMillis();
      for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
          ClientRecord client = (ClientRecord) j.next();
          if ( schedule( client, task, now ) ) {
            clients.add( client );
            ++count;
          }
        }
      }
      now = waitForClientsToFinish( clients );

    } while( ! terminateScheduling( clients, task, count, now ) );

    log.info(this.name + " terminating scheduling because " + this.termination);
  }

  /**
   *  Schedules the task on clients and waits for them to finish.
   */
  private void schedule( TestTask task ) {
    Vector clients = new Vector();
    long now = System.currentTimeMillis();
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        if ( schedule( client, task, now ) ) {
          clients.add( client );
        }
      }
    }
    waitForClientsToFinish( clients );
  }

  /**
   *  Schedules the same task on each client in turn.  Waits for each client to
   *  finish its task before scheduling the next client.
   */
  private void scheduleSequentially(TestTask task) {
    for (Iterator i = this.vms.values().iterator(); i.hasNext();) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for (Iterator j = vm.getClients().values().iterator(); j.hasNext();) {
        ClientRecord client = (ClientRecord)j.next();
        long now = System.currentTimeMillis();
        if (schedule(client, task, now)) {
          waitForClientToFinish(client);
        }
        if (!executedNormally()) {
          break;
        }
      }
    }
  }
}
