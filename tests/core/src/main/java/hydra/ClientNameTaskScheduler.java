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
*  This class acts as the task scheduler for tasks that run at the start
*  and end of a test, in single-threaded VMs started just to run the tasks,
*  that is, tasks of type STARTTASK and ENDTASK.
*  <p>
*  The set of client threads runs tasks concurrently, but each client thread
*  executes each task in sequence.  One client thread can get ahead of the
*  other in its sequence.
*  <p>Sample timeline with 3 clients running 3 tasks...
*  <blockquote>
*  <pre>
*  c1:  t1---------------t2------t3------|
*  c2:  t1----t2------t3---|
*  c3:  t1---------t2---------t3----------|
*  </pre>
*  </blockquote>
*  <p>
*  Each task is executed only in VMs with the logical client names specified
*  for that task in the test configuration file, or, if none are specified,
*  in the VM for each and every logical client name defined in the file.  See
*  {@link hydra.ClientPrms#names} for how to specify clients.
*/

public class ClientNameTaskScheduler extends BaseTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of executing
   *  the given set of start or end tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   */
  public ClientNameTaskScheduler( String name, Vector tasks, String purpose ) {
    super( name, tasks, purpose );
  }

  /**
   *  Asks this scheduler to create one single-threaded client vm for each
   *  client description required by the tasks, rather than using existing
   *  clients.
   *  @throws HydraTimeoutException if the clients do not start within
   *                                {@link Prms#maxClientStartupWaitSec}
   */
  public void createClients( Map cds, Map tgs ) {

    // get a client description for each client name used in a task
    Map tmpcds = new HashMap();
    for ( Iterator i = this.tasks.iterator(); i.hasNext(); ) {
      TestTask tt = (TestTask) i.next();
      Vector clientNames = tt.getClientNames();
      for ( Iterator j = clientNames.iterator(); j.hasNext(); ) {
        String clientName = (String) j.next();
        tmpcds.put( clientName, cds.get( clientName ) );
      }
    }

    // make modified client descriptions that are single-threaded
    Map newcds = new HashMap();
    for ( Iterator i = tmpcds.values().iterator(); i.hasNext(); ) {
      ClientDescription tmpcd = (ClientDescription) i.next();
      ClientDescription newcd = tmpcd.copy();
      newcd.setVmQuantity( 1 );
      newcd.setVmThreads( 1 );
      newcds.put( newcd.getName(), newcd );
    }

    super.createClients( newcds, tgs );
  }

  /**
   *  Executes the tasks using the existing clients.
   *  @return true if there were no fatal task errors or hangs
   */
  protected boolean executeTasks() {

    int count = 0;;
    long now = 0;
    do {
      // schedule as many clients as possible
      count = 0;
      now = System.currentTimeMillis();
      for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
          ClientRecord client = (ClientRecord) j.next();
          if ( schedule( client, now ) ) {
            ++count;
          }
        }
      }
    } while ( ! terminateScheduling( count, now ) );

    return schedulerResult();
  }

  /**
   *  Schedules the client with the next task for which it has the proper
   *  client name.
   */
  private boolean schedule( ClientRecord client, long now ) {

    synchronized( client ) {
      if ( ! client.isBusy() ) {
        if ( client.getTaskIndex() == this.tasks.size() ) {
          return false; // client has completed all of its tasks
        }
        // find the next task appropriate to the client
        int i;
        for ( i = client.getTaskIndex() + 1; i < this.tasks.size(); i++ ) {
          TestTask task = (TestTask) this.tasks.get( i );
          if ( task.getClientNames().contains( client.vm().getClientName() ) ) {
            client.setBusy( true );
            client.setTask( task );
            client.setStartTime( now );

            synchronized( task ) {
              task.incrementNumTimesInUse();
            }
            assignTask( client, task );
            return true;
          }
        }
        // the client has no more tasks to do if it reaches here
        client.setTaskIndex( this.tasks.size() );
        return false;
      } else {
        // else client is still executing the last assigned task
        return false;
      }
    }
  }

  /**
   *  True if it is time to terminate scheduling.
   */
  private boolean terminateScheduling( int count, long now ) {
    if ( ! exceededMaxResultWaitSec( now ) ) {
      if ( ! hadFatalTaskErrorAndHaltOrder() && ! hadStatMonitorError() ) {
        if ( ! executedAllTasks() ) {
          if ( count == 0 ) {
            printSchedulingReport( now );
            pauseScheduling();
          }
          return false;
        }
      }
    }
    return true;
  }

  /**
   *  True if all clients have completed their tasks.
   */
  private boolean executedAllTasks() {
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        if ( client.getTaskIndex() != this.tasks.size() ) {
          return false;
        }
      }
    }
    return true;
  }
}
