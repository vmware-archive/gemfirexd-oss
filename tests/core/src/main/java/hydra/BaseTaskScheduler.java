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

import com.gemstone.gemfire.LogWriter;

import java.rmi.*;
import java.util.*;

/** 
 *  This class is the abstract superclass for hydra task schedulers.  It
 *  contains a basic implementation of the {@link TaskScheduler} interface.
 */

public abstract class BaseTaskScheduler implements TaskScheduler {

  /**
   *  Creates a task scheduler by the given name for the purpose of executing
   *  the given set of tasks.
   *  @param name a descriptive name of the tasks to use for logging
   *  @param tasks the tasks to be scheduled
   */
  public BaseTaskScheduler( String name, Vector tasks, String purpose ) {
    this.name = name;
    this.tasks = tasks;
    this.purpose = purpose;
    this.tsid = nextTsid();
    this.log = Log.getLogWriter();

    addTaskScheduler( this.tsid, this );
  }

  /**
   *  Asks this scheduler to create vm and client records based on the client
   *  descriptions and map clients to threadgroups.
   *  @param cds the ClientDescription describes the client vms and threads
   *  @param tgs the thread groups to map clients to.
   */
  public void createClients( Map cds, Map tgs ) {
    log.info( "Creating the client vms for " + name );
    this.vms = ClientMgr.createClients( cds.values(), tgs );
    log.info( "Created the client vms for " + name + ": " + this.vms.values() );
  }

  /**
   *  Asks this scheduler to start clients previously created.
   *  @throws HydraInternalException if there are no clients, or any are running
   *  @throws HydraTimeoutException if the clients do not start within
   *                                {@link Prms#maxClientStartupWaitSec}
   */
  public void startClients() {
    if ( this.vms == null ) {
      throw new HydraInternalException( "no client vms found" );
    }
    log.info( "Starting the client vms for " + name + ": "
             + this.vms.values() );
    try {
      ClientMgr.startClients( this.purpose, this.vms );
      ClientMgr.waitForClientsToRegister( this.vms );
    } catch( HydraTimeoutException e ) {
      ResultLogger.reportHang( "Timeout starting clients for " + this.name, e );
      throw e;
    }
    log.info( "Started the client vms for " + name + ": " + this.vms.values() );
  }

  /**
   *  Asks this scheduler to start clients previously created, regardless of
   *  whether there have been fatal task errors or hangs in earlier phases of
   *  the test.
   *  @throws HydraTimeoutException if the clients do not start within
   *                                {@link Prms#maxClientStartupWaitSec}
   */
  public void startClientsForced() {

    // clear global error state
    HadFatalTaskError = false;
    ExceededMaxResultWaitSec = false;
    HadStatMonitorError = false;
    HadDynamicActionError = false;
    HadDynamicActionHang = false;

    startClients();
  }

  /**
   *  Asks this scheduler to stop the clients, rather than leaving them running
   *  for another scheduler to use later.
   *  @throws HydraTimeoutException if the clients do not stop within
   *                                {@link Prms#maxClientShutdownWaitSec}
   */
  public void stopClients() {
    if ( this.vms == null ) {
      throw new HydraInternalException( "no client vms found" );
    }
    try {
      if ( ExceededMaxResultWaitSec || HadDynamicActionHang ) {
        String s = "Possible hung clients for " + this.name + ". "
                + "Leaving the client vms running so they can be investigated.";
        log.severe( s );
        
        ClientMgr.runClientShutdownHooks( this.vms );

      } else if ( HadFatalTaskError || HadDynamicActionError || HadStatMonitorError) {
        MasterController.dumpStacksOnError(2);
        log.info( "Stopping the client vms for " + name + ": "
                 + this.vms.values() );
        ClientMgr.stopClients( this.vms, false, true );
        ClientMgr.waitForClientsToDie( this.vms );
        log.info( "Stopped the client vms for " + name + ": "
                 + this.vms.values() );

      } else {
        log.info( "Disconnecting and stopping the client vms for " + name + ": "
                 + this.vms.values() );
        ClientMgr.stopClients( this.vms, this.disconnect, false );
        ClientMgr.waitForClientsToDie( this.vms );
        log.info( "Disconnected and stopped the client vms for " + name + ": "
                 + this.vms.values() );
      }
    } catch( HydraTimeoutException e ) {
      ResultLogger.reportHang( "Timeout stopping clients for " + this.name, e );
      throw e;
    } finally {
      this.vms = null;
    }
  }

  /**
   *  Executes the tasks for this scheduler using existing clients.
   *  @param haltIfBadResult whether to terminate on a fatal task error
   *  @param maxResultWaitSec the maximum amount of time to wait for any one
   *                          task to complete
   *  @return true if there were no fatal task errors or hangs
   *  @throws HydraInternalException if there are no tasks or no clients
   */
  public boolean executeTasks( boolean haltIfBadResult,
                               long maxResultWaitSec ) {
    if ( this.vms == null ) {
      this.vms = ClientMgr.getClientVms();
    }
    initExecutionParameters( haltIfBadResult, maxResultWaitSec );
    initClientRuntime();
    return executeTasks();
  }

  /**
   *  Initializes task execution fields.
   */
  protected void initExecutionParameters( boolean haltIfBadResult,
                                          long maxResultWaitSec ) {
    this.haltIfBadResult = haltIfBadResult;
    this.maxResultWaitSec = maxResultWaitSec;
    this.maxResultWaitMs  = maxResultWaitSec * 1000;
  }

  /**
   *  Initializes client runtime state prior to task execution.  Also logs the
   *  number of logical clients and tasks.
   */
  protected void initClientRuntime() {
    int count = 0;
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        client.initRuntime();
        ++count;
      }
    }
    if ( this.tasks != null ) {
      log.info( "Executing " + this.tasks.size() + " " + this.name
              + " on " + count + " clients");
    }
  }

  /**
   *  Executes the tasks using the existing clients.
   *  @return true if there were no fatal task errors or hangs
   */
  protected abstract boolean executeTasks();

  //////////////////////////////////////////////////////////////////////////////
  //    ACCESSORS FOR SCHEDULERS, VMS, CLIENTS, TASKS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns the task scheduler with the given id.
   */
  private static TaskScheduler getTaskScheduler( int tsid ) {
    synchronized( TaskSchedulers ) {
      TaskScheduler ts = (TaskScheduler) TaskSchedulers.get(new Integer(tsid));
      if ( ts == null ) {          
        throw new HydraInternalException( "No scheduler " + tsid );
      }
      return ts;
    }
  } 
  
  /**
   *  Returns the task scheduler id.
   */
  protected int getTsid() {
    return this.tsid;
  }

  /**
   *  Adds the task scheduler with the given id.
   */
  private static void addTaskScheduler( int tsid, TaskScheduler ts ) {
    synchronized( TaskSchedulers ) {
      Integer i = new Integer( tsid );
      if ( TaskSchedulers.get( i ) != null ) {
        throw new HydraInternalException( "Duplicate tsid: " + tsid );
      }
      TaskSchedulers.put( i, ts );
    }
  } 
  
  /**
   *  Returns the client vm records.
   */
  protected Map getVms() { 
    return this.vms;
  }

  /**
   *  Returns the client vm record with the given id.
   */
  protected ClientVmRecord getVm( Integer vmid ) { 
    ClientVmRecord vm = (ClientVmRecord) this.vms.get( vmid );
    if ( vm == null ) {
      throw new HydraInternalException( "No client vm_" + vmid
                + " in " + this.name + " scheduler " + this.tsid );
    }
    return vm;
  }

  //////////////////////////////////////////////////////////////////////////////
  //    SCHEDULING SUPPORT METHODS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Asks the client to execute the task.  Issues the actual remote call.
   */
  protected void assignTask( ClientRecord client, TestTask task )  { 
    log.info( "Assigning " + task.toShortString() + " to " + client );
    RemoteTestModuleIF rem = client.getTestModule();
    try {
      rem.executeTask( this.tsid, task.getTaskType(), task.getTaskIndex() );
    } catch( RemoteException e ) {
      String reason = client + " unable to execute: " + task;
      throw new HydraRuntimeException( reason, e );
    }
  }

  /**
   *  Waits for clients in all vms to finish their tasks or encounter a failure.
   */
  protected long waitForClientsToFinish() {
    long now;
    boolean finished;
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
    }
    lastwaitingreporttime = 0;
    do {
      finished = true;
      now = System.currentTimeMillis();
      for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
          ClientRecord client = (ClientRecord) j.next();
          finished = finished && isFinished( client, now );
        }
      }
    } while ( ! finished );
    return now;
  }

  /**
   *  Waits for clients in a vm to finish their tasks or encounter a failure.
   */
  protected long waitForClientsToFinish( Vector clients ) {
    long now;
    boolean finished;
    lastwaitingreporttime = 0;
    do {
      finished = true;
      now = System.currentTimeMillis();
      for ( Iterator i = clients.iterator(); i.hasNext(); ) {
        ClientRecord client = (ClientRecord) i.next();
        finished = finished && isFinished( client, now );
      }
    } while ( ! finished );
    return now;
  }

  /**
   *  Waits for a client to finish its task or encounter a failure.
   */
  protected long waitForClientToFinish( ClientRecord client ) {
    long now;
    boolean finished;
    lastwaitingreporttime = 0;
    do {
      now = System.currentTimeMillis();
      finished = isFinished( client, now );
    } while ( ! finished );
    return now;
  }

  /**
   *  Returns true if a client has finished its task or encountered a failure.
   *  If not, prints a report then pauses to avoid running hot.
   */
  protected boolean isFinished( ClientRecord client, long now ) {
    if ( ! client.isBusy() || hadFatalTaskErrorAndHaltOrder() ||
         hadStatMonitorError() || hadDynamicActionError() ||
         hadDynamicActionHang() ||
         exceededMaxResultWaitSec( client, now ) ) {
      return true;
    } else {
      printWaitingReport( now );
      pauseWaiting();
      return false;
    }
  }

  /**
   *  Sets the global task error flag and local termination condition.
   */
  private void setHadFatalTaskError() {
    synchronized( TerminationLock ) {
      if ( HadFatalTaskError ) {
        return; // already detected a fatal task error
      } else {
        HadFatalTaskError = true;
      }
    }
    setTermination( "a client had a fatal task error" );
  }

  /**
   *  Returns true if any client has encountered a fatal task error and the
   *  test is configured to halt.
   */
  protected boolean hadFatalTaskErrorAndHaltOrder() {
    if ( this.haltIfBadResult && HadFatalTaskError ) {
      return true;
    }
    return false;
  }

  /**
   *  Sets the global statistics monitor error flag and local termination
   *  condition.  This method allows the statistics monitors to halt the test.
   */
  protected static void setStatMonitorError( Throwable t ) {
    HadStatMonitorError = true;
    String msg = null;
    if ( t.getMessage() == null ) {
      msg = "Statistics monitoring error: " + t.getClass().getName();
    } else {
      msg = "Statistics monitoring error: " + t.getMessage();
    }
    ResultLogger.reportErr( msg, t );
  }

  /**
   *  Returns true if a monitored condition was violated or there was an error
   *  in the statistics monitor.
   */
  protected boolean hadStatMonitorError() {
    if ( HadStatMonitorError ) {
      setTermination( "a monitored condition was violated or the statistics monitor had a fatal error" );
      return true;
    }
    return false;
  }

  /**
   *  Triggers the test to halt due to an error during a dynamic action.
   */
  protected static void processDynamicActionError( Throwable t ) {
    if ( t instanceof HydraTimeoutException ) {
      setDynamicActionHang( t );

    } else if ( t instanceof DynamicActionException ) {
      setDynamicActionError( t );

    } else {
      DynamicActionException e = null;
      if ( t.getMessage() == null ) {
        e = new DynamicActionException( "got a " + t.getClass().getName(), t );
      } else {
        e = new DynamicActionException( t.getMessage(), t );
      }
      setDynamicActionError( e );
    }
  }

  /**
   *  Sets the global dynamic action error flag and local termination condition.
   *  This method allows dynamic scheduler threads to halt the test.
   */
  protected static void setDynamicActionError( Throwable t ) {
    HadDynamicActionError = true;
    String msg = null;
    if ( t.getMessage() == null ) {
      msg = "Problem during dynamic action: " + t.getClass().getName();
    } else {
      msg = "Problem during dynamic action: " + t.getMessage();
    }
    ResultLogger.reportErr( msg, t );
  }

  /**
   *  Sets the global dynamic action hang flag and local termination condition.
   *  This method allows dynamic scheduler threads to halt the test.
   */
  protected static void setDynamicActionHang( Throwable t ) {
    HadDynamicActionHang = true;
    String msg = null;
    if ( t.getMessage() == null ) {
      msg = "Timeout during dynamic action: " + t.getClass().getName();
    } else {
      msg = "Timeout during dynamic action: " + t.getMessage();
    }
    ResultLogger.reportHang( msg, t );
  }

  /**
   *  Returns true if there has been an error during a dynamic action.
   */
  protected boolean hadDynamicActionError() {
    if ( HadDynamicActionError ) {
      setTermination( "a dynamic action had a fatal error" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true if there has been a hang during a dynamic action.
   */
  protected boolean hadDynamicActionHang() {
    if ( HadDynamicActionHang ) {
      setTermination( "a dynamic action timed out" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true if any clients are still busy and have run out of time.
   */
  protected boolean exceededMaxResultWaitSec( long now ) {
    if ( ExceededMaxResultWaitSec ) {
      return true;
    }
    for ( Iterator i = this.vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      Collection clients = vm.getClients().values();
      if ( exceededMaxResultWaitSec( clients, now ) ) {
        return true;
      }
    }
    return false;
  }

  /**
   *  Returns true if any clients are still busy and have run out of time.
   */
  protected boolean exceededMaxResultWaitSec( Collection clients, long now ) {
    boolean hung = false;
    if ( ExceededMaxResultWaitSec ) {
      return true;
    } else {
      for ( Iterator i = clients.iterator(); i.hasNext(); ) {
        ClientRecord client = (ClientRecord) i.next();
        if ( exceededMaxResultWaitSec( client, now ) ) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   *  Returns true if the client is still busy and has run out of time.  If so,
   *  it also sets up termination, logs the hang, and prints process stacks.
   */
  protected boolean exceededMaxResultWaitSec( ClientRecord client, long now ) {

    if ( client.isBusy() &&
       ( now - client.getStartTime() > this.maxResultWaitMs )  ) {

      synchronized( client ) {
        if ( client.isBusy() ) {
          setExceededMaxResultWaitSec( client );
          return true;
        }
      }
    }
    return false;
  }

  /**
   *  Sets the global task hang flag and local termination condition.
   *  Also logs the hang since a task result is not forthcoming and
   *  dumps stack on the offending client.
   */
  private boolean setExceededMaxResultWaitSec( ClientRecord client ) {
    synchronized( TerminationLock ) {
      if ( ExceededMaxResultWaitSec ) {
        return true; // already detected a hang
      } else {
        ExceededMaxResultWaitSec = true;
      }
    }

    // set the termination cause
    String msg = "a client exceeded max result wait sec: "
               + this.maxResultWaitSec;
    setTermination( msg );

    // log the hang
    TestTask task = client.getTask();
    task.logHangResult( client, msg );

    // dump stacks
    ClientMgr.printProcessStacks( client.vm() );

    return true;
  }

  /**
   *  Sets the reason for terminating the scheduler.
   */
  protected synchronized void setTermination( String reason ) {
    if ( this.termination == null ) {
      this.termination = reason;
    } else {
      this.termination += " then " + reason;
    }
  }

  /**
   *  Returns true if the scheduler executed normally up to this point or
   *  should continue in spite of fatal task error.
   */
  protected boolean executedNormallyOrNoHaltOrder() {
    return ! ( hadFatalTaskErrorAndHaltOrder() || ExceededMaxResultWaitSec ||
               HadStatMonitorError ||
               HadDynamicActionError || HadDynamicActionHang );
  }

  /**
   *  Returns true if the scheduler executed normally up to this point.
   */
  protected boolean executedNormally() {
    return ! ( HadFatalTaskError || ExceededMaxResultWaitSec ||
               HadStatMonitorError ||
               HadDynamicActionError || HadDynamicActionHang );
  }

  /**
   *  Returns the pass/fail outcome of running the scheduler and prints the
   *  reason the scheduler terminated.
   */
  protected boolean schedulerResult() {
    boolean passed = executedNormally();
    if ( passed ) {
      if ( this.termination == null ) {
        log.info( this.name
                + " terminating normally after tasks completed" );
      } else {
        log.info( this.name
                + " terminating normally after " + this.termination );
      }
    } else {
      log.severe( this.name
                + " terminating abnormally after " + this.termination );
    }
    return passed;
  }

  //////////////////////////////////////////////////////////////////////////////
  //    RESULT REPORTING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Called remotely by client threads to pass back task results.
   */ 
  protected static void reportResult( int tsid, int vmid, int tid,
                                      TestTaskResult result ) {
    try {
      // look up the client record
      ClientVmRecord vm = ClientMgr.getClientVm( vmid );
      ClientRecord client = vm.getClient( tid );

      // look up the task scheduler
      BaseTaskScheduler ts =
          (BaseTaskScheduler) BaseTaskScheduler.getTaskScheduler( tsid );

      // dump stacks outside synchronization if needed
      if (result.getResult() instanceof HydraTimeoutException) {
        MasterController.dumpStacks(2);
      }

      // process the result
      synchronized( client ) {
        TestTask task = client.getTask();
        if ( client.isBusy() ) {
  
          Object o = result.getResult();      
  
          // process scheduling orders
          if ( o instanceof StopSchedulingOrder &&
              task.acceptsStopSchedulingOrder() ) {
            ComplexTaskScheduler cts = (ComplexTaskScheduler) ts;
            cts.receiveStopSchedulingOrder();

          } else if ( o instanceof StopSchedulingTaskOnClientOrder &&
              task.acceptsStopSchedulingTaskOnClientOrder() ) {
            task.receiveStopSchedulingTaskOnClientOrder( client ); 
          }

          // process errors
          if ( result.getErrorStatus() ) {
            ts.setHadFatalTaskError();
            client.setError();
          }

          // reset state
          synchronized( task ) {
            task.addElapsedTime( result.getElapsedTime() );
            task.updateEndTimes( System.currentTimeMillis() );
            task.decrementNumTimesInUse();
            task.incrementNumTimesRun();
          }
          client.setBusy( false );

          // log the result
          if (o instanceof HydraTimeoutException) {
            task.logHangResult( client, result );
          } else {
            task.logTaskResult( client, result );
          }

        } else {
          // @todo lises treat this as an internal fatal error
          String s = client + " already reported the result for " + task;
          Log.getLogWriter().severe( s );
        }
      }
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      processDynamicActionError( t );
    }
  }

  /**
   *  Called by dynamic stopper threads to fake task results for the busy
   *  clients in a killed vm so the scheduler will not wait for them to finish.
   */
  protected static void fakeKillResults( ClientVmRecord vm ) {
    // find the scheduler id of the complex task scheduler
    // since it's the only one that can have a vm killed
    int tsid = -1;
    synchronized( TaskSchedulers ) {
      for ( Iterator i = TaskSchedulers.values().iterator(); i.hasNext(); ) {
        TaskScheduler ts = (TaskScheduler) i.next();
        if ( ts instanceof ComplexTaskScheduler ) {
          ComplexTaskScheduler cts = (ComplexTaskScheduler)ts;
          if ( cts.getVms().values().contains( vm ) ) {
            tsid = cts.getTsid();
            break;
          } else {
            throw new HydraInternalException( cts + " does not contain " + vm );
          }
        }
      }
    }
    if ( tsid < 0 ) {
      throw new HydraInternalException( "No ComplexTaskScheduler found" );
    }

    // report the fake results
    String msg = "Intentionally killed";
    for ( Iterator i = vm.getClients().values().iterator(); i.hasNext(); ) {
      ClientRecord client = (ClientRecord) i.next();
      if ( client.isBusy() ) {
        long elapsedTime = System.currentTimeMillis() - client.getStartTime();
        TestTaskResult result =
          new TestTaskResult( new MethExecutorResult( msg ), elapsedTime );
        reportResult( tsid, vm.getVmid().intValue(), client.getTid(), result );
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //    PAUSING AND PRINTING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Periodically logs that scheduler is waiting for idle eligible clients.
   *  Waits at least one round before reporting, then reports no more frequently
   *  than every 15 seconds.
   */
  protected void printSchedulingReport( long now ) {
    if ( lastschedulingreporttime == 0 ) {
      lastschedulingreporttime = now;
    } else if ( now - lastschedulingreporttime > 15000 ) {
      log.info( SCHEDULING_MSG );
      lastschedulingreporttime = now;
    }
  }

  private static final String SCHEDULING_MSG =
        "Scheduler waiting for idle clients with eligible tasks...";

  /**
   *  Used to pause an active scheduler when it has had an unfruitful pass
   *  through the live client list.
   */
  protected void pauseScheduling() {
    MasterController.sleepForMs( 200 );
  }

  /**
   *  Periodically logs that scheduler is waiting for busy clients to finish.
   *  Waits at least one round before reporting, then reports no more frequently
   *  than every 15 seconds.
   */
  protected void printWaitingReport( long now ) {
    if ( lastwaitingreporttime == 0 ) {
      maxwaitingreporttime = now + this.maxResultWaitMs;
      lastwaitingreporttime = now;
    } else if ( now - lastwaitingreporttime > 15000 ) {
      long remaining = ( maxwaitingreporttime - now ) / 1000;
      String msg = "Waiting " + remaining + " seconds for clients to finish "
                 + "current tasks...";
      log.info( msg );
      lastwaitingreporttime = now;
    }
  }

  /**
   *  Used to pause a terminating scheduler when it is waiting for currently
   *  busy clients to complete their tasks.
   */
  protected void pauseWaiting() {
    MasterController.sleepForMs( 2000 );
  }

  /**
   *  Periodically logs that scheduler is waiting for dynamic actions to finish.
   *  Waits at least one round before reporting, then reports no more frequently
   *  than every 15 seconds.
   */
  protected void printBlockingReport( long now ) {
    if ( lastblockingreporttime == 0 ) {
      lastblockingreporttime = now;
    } else if ( now - lastblockingreporttime > 15000 ) {
      log.info( BLOCKING_MSG );
      lastblockingreporttime = now;
    }
  }

  private static final String BLOCKING_MSG =
      "Waiting for dynamic client VM actions to complete...";

  /**
   *  Used to pause a terminating scheduler when it is waiting for dynamic
   *  schedulers to complete.
   */
  protected void pauseBlocking() {
    MasterController.sleepForMs( 2000 );
  }

  /**
   *  Asks this scheduler to log the result of executing its tasks, including
   *  the number of times the task was assigned and a summary of response times.
   */
  public void printReport() { 
    StringBuffer buf = new StringBuffer( 500 );
    buf.append("TASK REPORT.....");
    if ( this.tasks == null ) {
      buf.append( "no " + this.name + " found" );
    } else {
      for ( Iterator i = this.tasks.iterator(); i.hasNext(); ) {
        TestTask task = (TestTask) i.next();
        buf.append( "\n\n" ).append( task.toString() ).append( "\n" );
        buf.append("Number of times assigned: ").append(task.getNumTimesRun());
        String stats = task.getElapsedTimes().printStatsAsTimes();
        if ( stats != null ) {
          buf.append( "\n" ).append( stats );
        }
      }
    }
    log.info( buf.toString() );
  }

  //////////////////////////////////////////////////////////////////////////////
  //    DATA
  //////////////////////////////////////////////////////////////////////////////

  /** The name of this scheduler */
  protected String name;

  /** The client vm records for this scheduler */
  protected Map vms;

  /** The tasks for this scheduler */
  protected Vector tasks;

  /** The purpose of this scheduler */
  protected String purpose;

  /** Whether to halt when a (non-hang) fatal task error occurs. */
  protected boolean haltIfBadResult;

  /** Whether to disconnect from gemfire on normal exit */
  protected boolean disconnect = true;

  /** The maximum time to wait for a client to complete a task */
  private long maxResultWaitSec;
  private long maxResultWaitMs;

  /** The unique id for this task scheduler */
  private int tsid = -1;
  // provides unique logical id for each task scheduler
  private static int nextTsid = 0;
  private static synchronized int nextTsid() {
    return ++nextTsid;
  }
                
  private long lastschedulingreporttime = 0;
  private long lastwaitingreporttime = 0;
  private long maxwaitingreporttime = 0;
  private long lastblockingreporttime = 0;

  /** reason this scheduler is terminating */
  protected String termination;

  protected LogWriter log;

  //////////////////////////////////////////////////////////////////////////////
  //    GLOBAL STATE
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The global map of task schedulers used in the test, mapped by logical id.
   *  Provides static access to client vm records and tasks required to process
   *  queries and task results from RMI threads.
   */
  private static Map TaskSchedulers = new HashMap();

  /**
   * Global test error state is used to coordinate abnormal termination of
   * multiple, possibly concurrent, schedulers.  To prevent losing state, it
   * must not be initialized in scheduler instances.
   */

  /** Lock for synchronizing updates to global error state. */
  private static Object TerminationLock = new Object();

  /** True if a client encountered a fatal task error. */
  private static volatile boolean HadFatalTaskError = false;

  /** True if a client failed to complete a task in the expected time. */
  private static volatile boolean ExceededMaxResultWaitSec = false;

  /** True if a client encountered a statistics monitor error. */
  private static volatile boolean HadStatMonitorError = false;

  /** True if a client encountered a dynamic action error. */
  private static volatile boolean HadDynamicActionError = false;

  /** True if a client encountered a dynamic action hang. */
  private static volatile boolean HadDynamicActionHang = false;
}
