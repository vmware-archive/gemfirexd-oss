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
 *  This class acts as the superclass for all complex task schedulers.  These
 *  schedulers work with TASKs, which have a greater number of attributes than
 *  other types of hydra tasks.
 *  <p>
 *  TASKs that do not specify thread groups are assigned to the default group.
 */
public abstract class ComplexTaskScheduler extends BaseTaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of
   *  executing the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   *  @param totalTaskTimeSec total amount of time to execute tasks with
   *         this scheduler (overrides the task control parameter
   *         <code>maxTimesToRun</code>).
   */
  public ComplexTaskScheduler( String name, Vector tasks, String purpose,
                               long totalTaskTimeSec ) {
    super( name, tasks, purpose );

    this.totalTaskTimeSec = totalTaskTimeSec;
    this.totalTaskTimeMs = totalTaskTimeSec * 1000;

    this.status = new int[ tasks.size() ];
    this.receivedStopSchedulingOrder = false;
  }
  /** initializes the task start and end times */
  protected void initializeTaskTimes() { 
    this.birthTime = System.currentTimeMillis();
    for ( Iterator i = this.tasks.iterator(); i.hasNext(); ) {
      TestTask task = (TestTask) i.next();
      task.setStartTimes( this.birthTime );
      task.setEndTimes( this.birthTime );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TASK SCHEDULING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Schedules the client with the task, after rechecking under synchronization
   *  that the client is still available for scheduling.  Returns true if the
   *  client was actually scheduled.
   */
  protected boolean schedule( ClientRecord client, long now,
                              ComplexTaskScheduler ts) {
    ClientVmRecord vm = client.vm();
    synchronized( vm ) {
      if ( ! vm.isDynamic() ) {
        synchronized( client ) {
          if ( ! ts.receivedStopSchedulingOrder() && ! client.isBusy() &&
               ! ( client.hadError() && this.haltIfBadResult ) ) {
            TestTask task = selectTask( client, now );
            if ( task != null ) {
              client.setBusy( true );
              client.setTask( task );
              client.setStartTime( now );

              synchronized( task ) {
                task.incrementNumTimesInUse();
                task.updateStartTimes( now );
              }
              assignTask( client, task );
              return true;
            } // else there is no suitable task for this client
          } // else client is already busy
        }
      } // else client vm is not available
    }
    return false;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TASK SELECTION
  //////////////////////////////////////////////////////////////////////////////

  /** returns whether the client has an eligible task */
  protected boolean hasTask( ClientRecord client, long now ) {
    filterTasks( client, now );
    for ( int i = 0; i < status.length; i++ )
      if ( status[i] == OK )
        return true;
    return false;
  }
  /** selects the best eligible task for the client to run */
  protected TestTask selectTask( ClientRecord client, long now ) {
    if ( hasTask( client, now ) ) {
      if ( client.getThreadGroup().usesCustomWeights().booleanValue() )
        return selectTaskByWeight();
      else
        return selectTaskRandom();
    }
    return null;
  }
  /** filters tasks that are ineligible to run on this client */
  private void filterTasks( ClientRecord client, long now ) {
    for ( int i = 0; i < this.tasks.size(); i++ ) {
      TestTask task = (TestTask) this.tasks.elementAt(i);
      filterTask( i, task, client, now );
    }
  }
  /** filters tasks that cannot be assigned */
  private void filterTask(int i, TestTask task, ClientRecord client, long now) {
    if ( task.getNumTimesRun()+task.getNumTimesInUse()<task.getMaxTimesToRun() )
      if ( task.getNumTimesInUse() < task.getMaxThreads() )
        if ( task.usesThreadGroup( client.getThreadGroupName() ) )
          if ( ! task.receivedStopSchedulingTaskOnClientOrder( client ) )
            if ( task.satisfiesStartInterval( now ) )
              if ( task.satisfiesEndInterval( now ) )
                status[i] = OK;
              else status[i] = END_INTERVAL;
            else status[i] = START_INTERVAL;
          else status[i] = STOP_SCHEDULING_ORDER;
        else status[i] = THREAD_GROUP;
      else status[i] = MAX_THREADS;
    else status[i] = MAX_TIMES_TO_RUN;

    printFilterStatus();
  }
  /** return the eligible task with the minimum usage:weight ratio */
  private TestTask selectTaskByWeight() {
    int minindex = -1;
    double minratio = -1;
    for ( int i = 0; i < status.length; i++ ) {
      if ( status[i] == OK ) {
        TestTask task = (TestTask) this.tasks.elementAt(i);
        if ( minindex == -1 ) { // this is the first one
          minratio = computeRatio( task );
          minindex = i;
        } else {
          double ratio = computeRatio( task );
          if ( ratio < minratio ) {
            minratio = ratio;
            minindex = i;
          }
        }
      }
    }
    if ( minindex == -1 )
      throw new HydraInternalException( "Unexpected failure to find task" );
    else {
      TestTask task = (TestTask) this.tasks.elementAt( minindex );
      if ( log.finestEnabled() ) {
        log.finest( "highest priority " + task.toShortString() );
      }
      return task;
    }
    // @todo lises pick random task from tasks with min weight
  }
  /** returns a random eligible task (used when all tasks have equal weights) */
  private TestTask selectTaskRandom() {
    // get the number of eligible tasks
    int numEligible = 0;
    for ( int i = 0; i < status.length; i++ )
      if ( status[i] == OK )
        ++numEligible;

    printFilterStatus();
    // choose a random eligible task
    int whichEligible = TestConfig.tab().getRandGen().nextInt(0, numEligible-1);
    if ( log.finestEnabled() ) {
      log.finest( "NUMELIGIBLE: " + numEligible +
                 " WHICHELIGIBLE: " + whichEligible );
    }
    int count = 0;
    for ( int i = 0; i < status.length; i++ ) {
      if ( status[i] == OK ) {
        if ( log.finestEnabled() ) {
          log.finest( "COUNT: " + count );
        }
        if ( count == whichEligible )
          return (TestTask) this.tasks.elementAt( i );
        else
          ++count;
      }
    }
    throw new HydraInternalException( "Unexpected failure to find task" );
  }
  /** computes the usage:weight ratio for the task */
  private double computeRatio( TestTask task ) {
    double ratio = (double)( 1 + task.getNumTimesInUse()
                               + task.getNumTimesRun() ) / task.getWeight();
    if ( log.finestEnabled() ) {
      log.finest( "RATIO " + ratio + " for " + task.toShortString() );
    }
    return ratio;
  }
  /** print the status of the task assignment filter */
  private void printFilterStatus() {
    if ( log.finerEnabled() ) {
      StringBuffer buf = new StringBuffer(10);
      buf.append( "STATUS: " );
      for ( int i = 0; i < status.length; i++ ) {
        buf.append( status[i] ).append( " " );
      }
      log.finer( buf.toString() );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TERMINATION
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  True if it is time to stop scheduling clients.  Else if the scheduler is
   *  idle, prints a report then pauses to avoid running hot.
   */
  protected boolean terminateScheduling( int count, long now ) {
    boolean terminate = terminate( now );
    if ( ! terminate && count == 0 ) {
      printSchedulingReport( now );
      pauseScheduling();
    }
    return terminate;
  }

  /**
   *  Blocks until all dynamic scheduler threads have completed.  If they have
   *  not, prints a report then pauses to avoid running hot
   */
  protected void waitForDynamicThreadsToFinish() {
    ClientMgr.refuseDynamicActions(); // stop new dynamic threads from starting
    while ( true ) {
      long now = System.currentTimeMillis();
      if ( ClientMgr.hasDynamicActions() ) {
        printBlockingReport( now );
        pauseBlocking();
      } else {
        break;
      }
    }
  }

  /**
   *  True if it is time to terminate.
   */
  private boolean terminate( long now ) {
    return
           hadFatalTaskErrorAndHaltOrder()  // had task error
        || hadStatMonitorError()            // had error in statistics monitor
        || hadDynamicActionError()          // had dynamic action error
        || hadDynamicActionHang()           // had dynamic action hang
        || exceededMaxResultWaitSec( now )  // client timed out

        || exceededTotalTaskTimeSec( now )  // no more time to work
        || completedAllTasks()              // no more work to do
        || clientVmsPermanentlyStopped()    // no more clients to do work
    ;
  }

  /**
   *  True if any clients are still busy and have run out of time.
   */
  protected boolean exceededMaxResultWaitSec( long now ) {
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      synchronized( vm ) {
        if ( ! vm.isDynamic() ) {
          Collection clients = vm.getClients().values();
          if ( super.exceededMaxResultWaitSec( clients, now ) ) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   *  True if the test has exceeded totalTaskTimeSec.
   */
  private boolean exceededTotalTaskTimeSec( long now ) {
    if ( now - this.birthTime <= this.totalTaskTimeMs ) {
      return false;
    }
    setTermination( "they exceeded the total task time sec: "
                  + this.totalTaskTimeSec );
    return true;
  }

  /**
   *  True if all tasks have been completed due to imposed limits and/or
   *  scheduling orders.
   */
  private boolean completedAllTasks() {
    if ( this.receivedStopSchedulingOrder ) {
      setTermination( "a client threw a StopSchedulingOrder" );
      return true;
    }
    int limits = 0;
    int orders = 0;
    for ( int i = 0; i < status.length; i++ ) {
      TestTask task = (TestTask) this.tasks.get(i);
      if ( status[i] == MAX_TIMES_TO_RUN ) {
        ++limits;
      } else if ( task.receivedAllStopSchedulingTaskOnClientOrders() ) {
        ++orders;
      } else {
        return false;
      }
    }
    if ( limits == this.tasks.size() ) {
      setTermination( "all tasks exceeded max times to run" );
    } else if ( orders == this.tasks.size() ) {
      setTermination( "all tasks were thrown StopSchedulingTaskOnClientOrders "
                    + "from all of their eligible clients" );
    } else {
      setTermination( "all tasks either exceeded max times to run or " +
                      "were thrown StopSchedulingTaskOnClientOrders "
                    + "from all of their eligible clients" );
    }
    return true;
  }

  /**
   *  True if all vms have been dynamically stopped with no hope of being
   *  restarted.
   */
  private boolean clientVmsPermanentlyStopped() {
    if ( ClientMgr.clientVmsPermanentlyStopped() ) {
      setTermination( "all client VMs have been dynamically stopped and there "
                    + "is no hope of restarting them" );
      return true;
    }
    return false;
  }

  /**
   *  Registers the fact that a StopSchedulingOrder has been thrown.
   */
  protected void receiveStopSchedulingOrder() {
    this.receivedStopSchedulingOrder = true;
  }

  /**
   *  Returns true if a StopSchedulingOrder has been thrown.
   */
  protected boolean receivedStopSchedulingOrder() {
    return this.receivedStopSchedulingOrder;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME DATA
  //////////////////////////////////////////////////////////////////////////////

  /** status of each task computed per scheduling loop iteration */
  private int[] status;

  /** stop scheduling order */
  private volatile boolean receivedStopSchedulingOrder;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTANT DATA
  //////////////////////////////////////////////////////////////////////////////

  /** time (in seconds) tasks are allowed to execute */
  private long totalTaskTimeSec;
  /** time (in milliseconds) tasks are allowed to execute */
  private long totalTaskTimeMs;

  /** time (in milliseconds) when task execution began */
  private long birthTime;

  /** task filters */
  private static final int OK                     = 0;
  private static final int MAX_TIMES_TO_RUN       = 1;
  private static final int MAX_THREADS            = 2;
  private static final int START_INTERVAL         = 3;
  private static final int END_INTERVAL           = 4;
  private static final int THREAD_GROUP           = 5;
  private static final int STOP_SCHEDULING_ORDER  = 6;
}
