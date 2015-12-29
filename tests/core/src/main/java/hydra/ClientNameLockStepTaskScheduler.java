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
 * This class acts as the task scheduler for tasks that run at the start
 * and end of a test, in single-threaded VMs started just to run the tasks,
 * that is, tasks of type STARTTASK and ENDTASK.
 * <p> 
 * Tasks are run in the order given in the test configuration.  All client
 * threads designated to run a task run it concurrently, but task(i+1) is only
 * scheduled after all clients running task(i) have completed.
 * <p>Sample timeline with 3 clients running 3 tasks...
 * <blockquote>
 * <pre>
 * c1:  t1---------------t2------     t3------    |
 * c2:  t1---------                   t3----------|
 * c3:  t1----           t2-----------t3---       |
 * </pre>
 * </blockquote>
 * <p>
 * Each task is executed only in VMs with the logical client names specified
 * for that task in the test configuration file, or, if none are specified,
 * in the VM for each and every logical client name defined in the file.  See
 * {@link hydra.ClientPrms#names} for how to specify clients.
 */
public class ClientNameLockStepTaskScheduler extends ClientNameTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of executing
   *  the given set of start or end tasks in lock step.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   */
  public ClientNameLockStepTaskScheduler(String name, Vector tasks,
                                         String purpose) {
    super( name, tasks, purpose );
  }

  /**
   * Executes the tasks using the existing clients.
   * @return true if there were no fatal task errors or hangs
   */
  protected boolean executeTasks() {
    for (int i = 0; i < this.tasks.size(); i++) {
      TestTask task = (TestTask) this.tasks.get(i);
      schedule(task);
      if (!executedNormally()) {
        break;
      }
    }
    return schedulerResult();
  }

  /**
   *  Schedules the task on clients and waits for them to finish.
   */
  private void schedule(TestTask task) {
    Vector clients = new Vector();
    long now = System.currentTimeMillis();
    for (Iterator i = this.vms.values().iterator(); i.hasNext();) {
      ClientVmRecord vm = (ClientVmRecord)i.next();
      for (Iterator j = vm.getClients().values().iterator(); j.hasNext();) {
        ClientRecord client = (ClientRecord)j.next();
        if (schedule(client, task, now)) {
          clients.add(client);
        }
      }
    }
    waitForClientsToFinish(clients);
  }

  /**
   *  Schedules the client with the task, if it is in the proper threadgroup
   *  and has not thrown a stop scheduling order for it.
   */
  private boolean schedule(ClientRecord client, TestTask task, long now) {
    if (task.getClientNames().contains(client.vm().getClientName())) {
      client.setBusy(true);
      client.setTask(task);
      client.setStartTime(now);

      synchronized(task) {
        task.incrementNumTimesInUse();
      }
      assignTask(client, task);
      return true;
    } else {
      return false;
    }
  }
}
