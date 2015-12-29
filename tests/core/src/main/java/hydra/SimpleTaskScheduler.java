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
 *  This class acts as the superclass for all simple task schedulers.  These
 *  schedulers work with static and dynamic INITTASKs and CLOSETASKs.
 */
public abstract class SimpleTaskScheduler extends BaseTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   */                                
  public SimpleTaskScheduler( String name,
                              Vector tasks,
                              String purpose ) {
    super( name, tasks, purpose );
  }

  /**
   *  Schedules the client with the task, if it is in the proper threadgroup
   *  and has not thrown a stop scheduling order for it.
   */
  protected boolean schedule( ClientRecord client,
                              TestTask task,
                              long now ) {
    if ( task.usesThreadGroup( client.getThreadGroupName() )
    && ! task.receivedStopSchedulingTaskOnClientOrder( client ) ) {
      ClientVmRecord vm = client.vm();
      synchronized( vm ) {
        if ( vm.isLive() ) {
          synchronized( client ) {
            client.setBusy( true );
            client.setTask( task );
            client.setStartTime( now );

            synchronized( task ) {
              task.incrementNumTimesInUse();
            }
            assignTask( client, task );
          }
        }
      }
      return true;

    } else {
      return false;
    }
  }

  /**
   *  True if it is time to stop scheduling batched clients.
   */
  protected boolean terminateScheduling( Vector clients, TestTask task,
                                         int count, long now ) {
    return receivedStopSchedulingTaskOnClientOrder( task, clients )
        || terminateScheduling( count, now );
  }

  /**
   *  True if it is time to stop scheduling a batched client.
   */
  protected boolean terminateScheduling( ClientRecord client, TestTask task,
                                         int count, long now ) {
    return receivedStopSchedulingTaskOnClientOrder( task, client )
        || terminateScheduling( count, now );
  }

  /**
   *  True if it is time to stop scheduling clients.
   */
  private boolean terminateScheduling( int count, long now ) {
    if ( ! hadFatalTaskErrorAndHaltOrder() ) {
      if ( ! hadStatMonitorError() ) {
        if ( ! hadDynamicActionError() ) {
          if ( ! hadDynamicActionHang() ) {
            if ( ! exceededMaxResultWaitSec( now ) ) {
              if ( count == 0 ) {
                printSchedulingReport( now );
                pauseScheduling();
              }
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  /**
   *  True if the clients have all thrown a stop scheduling order on the task.
   */
  private boolean receivedStopSchedulingTaskOnClientOrder( TestTask task,
                                                           Vector clients ) {
    for ( Iterator i = clients.iterator(); i.hasNext(); ) {
      ClientRecord client = (ClientRecord) i.next();
      if ( ! task.receivedStopSchedulingTaskOnClientOrder( client ) ) {
        return false;
      }
    }
    return true;
  }

  /**
   *  True if the client has thrown a stop scheduling order on the task.
   */
  private boolean receivedStopSchedulingTaskOnClientOrder( TestTask task,
                                                         ClientRecord client ) {
    return task.receivedStopSchedulingTaskOnClientOrder( client );
  }
}
