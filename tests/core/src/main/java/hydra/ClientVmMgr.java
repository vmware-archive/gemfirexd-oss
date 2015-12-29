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
import java.rmi.RemoteException;
import java.util.*;

/**
 *  <p>
 *  This class provides a public API that hydra clients can use to dynamically
 *  stop or start client VMs during a test run.  The API can only be used from
 *  hydra tasks of type "TASK".
 *  <p>
 *  Hydra clients can stop any client VM, including their own.  They can only
 *  start client VMs other than themselves, for obvious reasons.  A VM can only
 *  be the target of a dynamic action by one client at a time.  A VM cannot be
 *  the target of a dynamic action while it is itself (briefly) engaged as a
 *  source initiating an action on another VM.  However, once a dynamic action
 *  has been initiated, the source of the action becomes available as a target
 *  while the action is taking place (unless it is also the target).
 *  <p>
 *  Stop and start methods are available in synchronous and asynchronous
 *  versions.  Asynchronous methods return as soon as the stop or start is
 *  initiated, and allow clients to stop themselves and stop or start others
 *  while they are doing real work.  Synchronous methods do not return until the
 *  requested stop or start has fully completed.  They should be used with
 *  caution for reasons described later.
 *  <p>
 *  A {@link ClientVmInfo} object can be passed in to some methods to describe
 *  the target VM using various combinations of parameters.  If more than one
 *  VM matches the target info, the implementation chooses one to be acted upon.
 *  The methods throw {@link ClientVmNotFoundException} if no VMs matching the
 *  target info are available as a target, either because they are already
 *  the targets of dynamic actions or are initiating dynamic actions themselves.
 *  <p>
 *  How a VM is stopped is controlled by the <b>stop mode</b>, which is one of
 *  {@link #MEAN_KILL}, {@link #NICE_KILL}, {@link #MEAN_EXIT}, {@link
 *  #NICE_EXIT}, or {@link #NICE_DISCONNECT}.  When a VM is actually stopped
 *  depends on 1) the stop
 *  mode and 2) whether it contains clients that have been configured for
 *  CLOSETASKs that have the <code>runMode</code> set to <code>always</code> or
 *  <code>dynamic</code>.  To prevent a CLOSETASK from running on dynamic exits,
 *  use run mode <code>once</code>, which is the default setting.
 *  <p>
 *  How and when a VM is restarted is controlled by the <b>start mode</b>, which
 *  is one of {@link #IMMEDIATE}, {@link #ON_DEMAND}, {@link #NEVER}, or
 *  after a specified number of milliseconds.  When the VM is actually scheduled
 *  to run TASKs again depends on 1) the start mode and 2) whether it contains
 *  clients that have been configured for INITTASKs that have the
 *  <code>run mode</code> set to <code>always</code> or <code>dynamic</code>.
 *  To prevent an INITTASK from running on dynamic starts, use run mode
 *  <code>once</code>, which is the default setting.
 *  <p>
 *  Synchronous methods using an exit stop mode can experience a large delay if
 *  clients in the target VM must complete their current TASKs.  The delay is
 *  even greater if they must complete some number of CLOSETASKs on shutdown or
 *  INITTASKs on startup.  Also, synchronization can cause deadlock, and it is
 *  up to the test developer to avoid it.  For example, if two VMs attempt to
 *  stop each other at the same time, they will block if circumstances require
 *  them both to complete their current task.
 *  <p>
 *  When a VM is dynamically started, it creates a new log file using the same
 *  logical VM ID but a new PID.  Client logs contain informational messages
 *  about the requested action.  The action itself takes place in the master,
 *  using threads named for the source client and the particular action, e.g.,
 *  Dynamic Client VM Stopper.
 */
public class ClientVmMgr {

  ///////////////////////   Sync Modes   ///////////////////////

  protected static final int  SYNC = 0;
  private static final String Sync = "synchronous";

  protected static final int ASYNC = 1;
  private static final String Async = "asynchronous";

  /**
   *  Convert a sync mode into a string.
   */
  private static String toSyncModeString( int syncMode ) {
    switch( syncMode ) {
      case  SYNC: return Sync;
      case ASYNC: return Async;
      default: throw new HydraInternalException( "Unhandled sync mode" );
    }
  }

  ///////////////////////   Stop Modes   ///////////////////////

  /**
   *  Use as a stop mode to issue kill -TERM on the VM.
   *  The VM is killed without regard to whether its clients are still
   *  running tasks or are configured to run CLOSETASKs.
   */
  public static final int NICE_KILL = -20;
  public static final String NiceKill = "nice_kill";

  /**
   *  Use as a stop mode to issue kill -9 on the VM.
   *  The VM is killed without regard to whether its clients are still
   *  running tasks or are configured to run CLOSETASKs.
   */
  public static final int MEAN_KILL = -21;
  public static final String MeanKill = "mean_kill";

  /**
   *  Use as a stop mode to issue a distributed system disconnect and normal
   *  <code>System.exit(0)</code> shutdown on the VM.
   *  The clients in the VM are first allowed to complete their
   *  current tasks and any CLOSETASKs that have been configured for them.
   */
  public static final int NICE_EXIT = -22;
  public static final String NiceExit = "nice_exit";

  /**
   *  Use as a stop mode to issue a normal <code>System.exit(0)</code>
   *  shutdown on the VM, without a prior distributed system disconnect.
   *  The clients in the VM are first allowed to complete their
   *  current tasks and any CLOSETASKs that have been configured for them.
   */
  public static final int MEAN_EXIT = -23;
  public static final String MeanExit = "mean_exit";

  /**
   *  Use as a stop mode to issue a distributed system disconnect on the VM.
   *  The clients in the VM are first allowed to complete their
   *  current tasks and any CLOSETASKs that have been configured for them.
   */
  public static final int NICE_DISCONNECT = -24;
  public static final String NiceDisconnect = "nice_disconnect";

  /**
   *  The default stop mode, {@link #MEAN_KILL}.
   */
  public static final int DEFAULT_STOP_MODE = MEAN_KILL;

  /**
   *  Convert a stop mode into a string.
   */
  public static String toStopModeString( int stopMode ) {
    switch( stopMode ) {
      case NICE_KILL: return NiceKill;
      case MEAN_KILL: return MeanKill;
      case NICE_EXIT: return NiceExit;
      case MEAN_EXIT: return MeanExit;
      case NICE_DISCONNECT: return NiceDisconnect;
      default:
        throw new HydraInternalException( "Unhandled stop mode" );
    }
  }

  /**
   *  Convert a string to a stop mode.
   */
  public static int toStopMode( String stopMode ) {
    if ( stopMode.equalsIgnoreCase( NiceExit ) ) {
      return NICE_EXIT;
    } else if ( stopMode.equalsIgnoreCase( MeanExit ) ) {
      return MEAN_EXIT;
    } else if ( stopMode.equalsIgnoreCase( NiceKill ) ) {
      return NICE_KILL;
    } else if ( stopMode.equalsIgnoreCase( MeanKill ) ) {
      return MEAN_KILL;
    } else if ( stopMode.equalsIgnoreCase( NiceDisconnect ) ) {
      return NICE_DISCONNECT;
    } else {
      throw new HydraConfigException( "Illegal stop mode: " + stopMode );
    }
  }

  //////////////////////   Start Modes   ///////////////////////

  /**
   *  Use as a start mode when immediate restart is desired.
   */
  public static final int IMMEDIATE = -30;
  public static final String Immediate = "immediate";

  /**
   *  Use as a start mode when restart should wait for invocation of a start
   *  method in this interface.
   */
  public static final int ON_DEMAND = -31;
  public static final String OnDemand = "on_demand";

  /**
   *  Use as a start mode when restart must never occur.
   */
  public static final int NEVER = -32;
  public static final String Never = "never";

  /**
   *  The default start mode, {@link #ON_DEMAND}.
   */
  public static final int DEFAULT_START_MODE = ON_DEMAND;

  /**
   *  Convert a start mode into a string.
   */
  private static String toStartModeString( int startMode ) {
    if ( startMode > 0 ) {
      return "after " + startMode + " ms";
    }
    switch( startMode ) {
      case IMMEDIATE: return Immediate;
      case ON_DEMAND: return OnDemand;
      case NEVER:     return Never;
      default:
        throw new HydraInternalException( "Unhandled start mode" );
    }
  }

  /**
   *  Convert a string to a start mode.
   */
  public static int toStartMode( String startMode ) {
    if ( startMode.equalsIgnoreCase( Immediate ) ) {
      return IMMEDIATE;
    } else if ( startMode.equalsIgnoreCase( OnDemand ) ) {
      return ON_DEMAND;
    } else if ( startMode.equalsIgnoreCase( Never ) ) {
      return NEVER;
    } else {
      try {
        int delay = Integer.parseInt( startMode );
        if ( delay > 0 ) {
          return delay;
        } else {
          throw new HydraConfigException( "Illegal start mode: " + startMode );
        }
      } catch( NumberFormatException e ) {
        throw new HydraConfigException( "Illegal start mode: " + startMode );
      }
    }
  }

  //////////////////////   Stop Methods   //////////////////////

  /**
   *  IMPLEMENTATION NOTES
   *  <p>
   *  A client action produces three RMI calls.  The first does the reservation
   *  and collects the target ClientVmInfo and any exceptions related to
   *  matching or test termination.  The second initiates the actual stop or
   *  start.  The third is a notification to synchronous clients that the action
   *  is complete (asynchronous clients need no notification).
   *  <p>
   *  The reason for this is to support asynchronous methods that allow the
   *  client to be doing real work when it is stopped.  Using a single RMI call
   *  from client to master forces the client side to do a join in order to get
   *  hold of ClientVmNotFoundException.  Using a single RMI call to initiate
   *  the action keeps the RMI connection open for a potentially long time.
   *  <p>
   *  Threads are used on both the client and master sides.  A thread is needed
   *  client-side to allow asynchronous clients to be doing real work when the
   *  action takes place.  Without the thread, the master can go forward with
   *  the action before the RMI call that initiates it returns to the client.
   *  A thread is needed master-side to implement asynchronous methods.
   */

  /**
   *  Stops this client VM using {@link #DEFAULT_STOP_MODE}, and
   *  {@link #DEFAULT_START_MODE}.  This method is synchronous, blocking the
   *  calling client until its VM has stopped.  Note that this will cause
   *  deadlock if the stop mode is {@link #NICE_EXIT} or {@link #MEAN_EXIT}.
   *
   *  @param reason The reason for stopping the VM.
   *  @return A {@link ClientVmInfo} describing this VM.
   *  @throws ClientVmNotFoundException
   *          if this VM is unavailable as a target.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stop( String reason )
  throws ClientVmNotFoundException {
    return stop( reason, SYNC, DEFAULT_STOP_MODE, DEFAULT_START_MODE,
                 new ClientVmInfo() );
  }

  /**
   *  Stops this client VM using {@link #DEFAULT_STOP_MODE}, and
   *  {@link #DEFAULT_START_MODE}.  This method is asynchronous, allowing the
   *  client to continue doing real work while its VM is stopped.
   *
   *  @param reason The reason for stopping the VM.
   *  @return A {@link ClientVmInfo} describing this VM.
   *  @throws ClientVmNotFoundException
   *          if this VM is unavailable as a target.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stopAsync( String reason )
  throws ClientVmNotFoundException {
    return stop( reason, ASYNC, DEFAULT_STOP_MODE, DEFAULT_START_MODE,
                 new ClientVmInfo() );
  }

  /**
   *  Stops this client VM using the specified stop and start modes.  This
   *  method is synchronous, blocking the calling client until its VM has
   *  stopped.  Note that this will cause deadlock if the stop mode is {@link
   *  #NICE_EXIT} or {@link #MEAN_EXIT}.
   *
   *  @param reason    The reason for stopping the VM.
   *  @param stopMode  One of {@link #NICE_KILL}, {@link #MEAN_KILL},
   *                   {@link #NICE_EXIT}, {@link #MEAN_EXIT}, or
   *                   {@link #NICE_DISCONNECT}.
   *  @param startMode One of {@link #IMMEDIATE}, {@link #ON_DEMAND},
   *                   {@link #NEVER}, or a positive delay in milliseconds.
   *  @return A {@link ClientVmInfo} describing this VM.
   *  @throws ClientVmNotFoundException
   *          if this VM is unavailable as a target.
   *  @throws IllegalArgumentException if a mode is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stop( String reason,
                                   int stopMode, int startMode )
  throws ClientVmNotFoundException {
    return stop( reason, SYNC, stopMode, startMode, new ClientVmInfo() );
  }

  /**
   *  Stops this client VM using the specified stop and start modes.  This
   *  method is asynchronous, allowing the client to continue doing real work
   *  while its VM is stopped.
   *
   *  @param reason    The reason for stopping the VM.
   *  @param stopMode  One of {@link #NICE_KILL}, {@link #MEAN_KILL},
   *                   {@link #NICE_EXIT}, {@link #MEAN_EXIT}, or
   *                   {@link #NICE_DISCONNECT}.
   *  @param startMode One of {@link #IMMEDIATE}, {@link #ON_DEMAND},
   *                   {@link #NEVER}, or a positive delay in milliseconds.
   *  @return A {@link ClientVmInfo} describing this VM.
   *  @throws ClientVmNotFoundException
   *          if this VM is unavailable as a target.
   *  @throws IllegalArgumentException if a mode is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stopAsync( String reason,
                                        int stopMode, int startMode )
  throws ClientVmNotFoundException {
    return stop( reason, ASYNC, stopMode, startMode, new ClientVmInfo() );
  }

  /**
   *  Stops a client VM described by the given {@link ClientVmInfo} using
   *  {@link #DEFAULT_STOP_MODE} and {@link #DEFAULT_START_MODE}.  This method
   *  is synchronous.  It returns when the target VM has fully stopped, but does
   *  not wait for it to restart.  Note that this method can cause a significant
   *  wait or even deadlock if used inappropriately.
   *
   *  @param reason The reason for stopping the VM.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if this VM is unavailable as a target.
   *  @throws IllegalArgumentException if target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stop( String reason, ClientVmInfo target )
  throws ClientVmNotFoundException {
    return stop( reason, SYNC, DEFAULT_STOP_MODE, DEFAULT_START_MODE, target );
  }

  /**
   *  Stops a client VM described by the given {@link ClientVmInfo} using
   *  {@link #DEFAULT_STOP_MODE} and {@link #DEFAULT_START_MODE}.  This method
   *  is asynchronous, allowing the client to continue doing real work while the
   *  target VM is stopped.
   *
   *  @param reason The reason for stopping the VM.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws IllegalArgumentException if target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stopAsync( String reason, ClientVmInfo target )
  throws ClientVmNotFoundException {
    return stop( reason, ASYNC, DEFAULT_STOP_MODE, DEFAULT_START_MODE,
                 target );
  }

  /**
   *  Stops a client VM described by the given {@link ClientVmInfo} using the
   *  specified stop and start modes.  This method is synchronous.  It returns
   *  when the target VM has fully stopped, but does not wait for it to restart.
   *  Note that this method can cause a significant wait or even deadlock if
   *  used inappropriately.
   *
   *  @param reason The reason for stopping the VM.
   *  @param stopMode  One of {@link #NICE_KILL}, {@link #MEAN_KILL},
   *                   {@link #NICE_EXIT}, {@link #MEAN_EXIT}, or
   *                   {@link #NICE_DISCONNECT}.
   *  @param startMode One of {@link #IMMEDIATE}, {@link #ON_DEMAND},
   *                   {@link #NEVER}, or a positive delay in milliseconds.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws IllegalArgumentException if mode or target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stop( String reason,
                                   int stopMode, int startMode,
                                   ClientVmInfo target )
  throws ClientVmNotFoundException {
    return stop( reason, SYNC, stopMode, startMode, target );
  }

  /**
   *  Stops a client VM described by the given {@link ClientVmInfo} using the
   *  specified stop and start modes.  This method is asynchronous, allowing the
   *  client to continue doing real work while the target VM is stopped.
   *
   *  @param reason The reason for stopping the VM.
   *  @param stopMode  One of {@link #NICE_KILL}, {@link #MEAN_KILL},
   *                   {@link #NICE_EXIT}, {@link #MEAN_EXIT}, or
   *                   {@link #NICE_DISCONNECT}.
   *  @param startMode One of {@link #IMMEDIATE}, {@link #ON_DEMAND},
   *                   {@link #NEVER}, or a positive delay in milliseconds.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws IllegalArgumentException if mode or target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo stopAsync( String reason,
                                        int stopMode, int startMode,
                                        ClientVmInfo target )
  throws ClientVmNotFoundException {
    return stop( reason, ASYNC, stopMode, startMode, target );
  }

  /////////////////////   Start Methods   //////////////////////

  /**
   *  Starts a client VM that was stopped using start mode {@link #ON_DEMAND}.
   *  The specific VM chosen is up to the implementation.
   *  This method is synchronous, blocking the calling client until the target
   *  VM has fully started and run any INITTASKs configured for it.  Note that
   *  this could entail a significant wait.
   *
   *  @param reason The reason for starting the VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo start( String reason )
  throws ClientVmNotFoundException {
    ClientVmInfo target = new ClientVmInfo( null, null, null );
    return start( reason, SYNC, target );
  }

  /**
   *  Starts a client VM that was stopped using start mode {@link #ON_DEMAND}.
   *  The specific VM chosen is up to the implementation.
   *  This method is asynchronous, allowing the client to continue doing real
   *  work while the target VM is started.
   *
   *  @param reason The reason for starting the VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo startAsync( String reason )
  throws ClientVmNotFoundException {
    ClientVmInfo target = new ClientVmInfo( null, null, null );
    return start( reason, ASYNC, target );
  }

  /**
   *  Starts a client VM that was stopped using start mode {@link #ON_DEMAND},
   *  and is described by the given {@link ClientVmInfo}.
   *  This method is synchronous, blocking the calling client until the target
   *  VM has fully started and run any INITTASKs configured for it.  Note that
   *  this could entail a significant wait.
   *
   *  @param reason The reason for starting the VM.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws IllegalArgumentException if the target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo start( String reason, ClientVmInfo target )
  throws ClientVmNotFoundException {
    return start( reason, SYNC, target );
  }

  /**
   *  Starts a client VM that was stopped using start mode {@link #ON_DEMAND},
   *  and is described by the given {@link ClientVmInfo}.
   *  This method is asynchronous, allowing the client to continue doing real
   *  work while the target VM is started.
   *
   *  @param reason The reason for starting the VM.
   *  @param target Information used to select the target client VM.
   *  @return A {@link ClientVmInfo} describing the selected VM.
   *  @throws ClientVmNotFoundException
   *          if no matching VMs are available as targets.
   *  @throws IllegalArgumentException if the target info is invalid.
   *  @throws DynamicActionException if the test is terminating abnormally and
   *          refuses to allow any further dynamic actions.
   */
  public static ClientVmInfo startAsync( String reason, ClientVmInfo target )
  throws ClientVmNotFoundException {
    return start( reason, ASYNC, target );
  }

  ////////////////////   Helper Methods   //////////////////////

  /**
   * Returns the PID for the client VM with the given VM ID.  This is -1 if
   * the VM is currently started or stopped or is being started or stopped.
   */
  public static int getPid(int vmid) {
    try {
      return RemoteTestModule.Master.getPid(vmid);
    } catch( RemoteException e ) {
      throw new HydraRuntimeException("Problem with remote operation", e);
    }
  }

  /**
   * Returns the VM IDs (as Integers) for all client VMs, regardless of whether
   * the VMs are currently started or stopped.
   */
  public static Vector getClientVmids() {
    try {
      return RemoteTestModule.Master.getClientVmids();
    } catch( RemoteException e ) {
      throw new HydraRuntimeException("Problem with remote operation", e);
    }
  }

  /**
   * Returns the VM IDs (as Integers) for all client VMs except this one,
   * regardless of whether the VMs are currently started or stopped.
   */
  public static Vector getOtherClientVmids() {
    int myVmid = RemoteTestModule.getMyVmid();
    try {
      return RemoteTestModule.Master.getOtherClientVmids(myVmid);
    } catch( RemoteException e ) {
      throw new HydraRuntimeException("Problem with remote operation", e);
    }
  }

  ////////////////////   Generic Methods   /////////////////////

  /**
   *  Generic stop.
   */
  private static ClientVmInfo stop( String reason, int syncMode,
                                    int stopMode, int startMode,
                                    ClientVmInfo target )
  throws ClientVmNotFoundException {
    validate( syncMode, stopMode, startMode );
    return reserveAndStop( reason, syncMode, stopMode, startMode,
                                 target );
  }

  /**
   *  Generic start.
   */
  private static ClientVmInfo start( String reason, int syncMode,
                                     ClientVmInfo target )
  throws ClientVmNotFoundException {
    validate( syncMode, DEFAULT_STOP_MODE, DEFAULT_START_MODE );
    return reserveAndStart( reason, syncMode, target );
  }

  /**
   *  Reserves a client VM matching the specified target info and stops it.
   */
  private static ClientVmInfo reserveAndStop( final String reason,
                                              final int syncMode,
                                              final int stopMode,
                                              final int startMode,
                                              ClientVmInfo target )
  throws ClientVmNotFoundException {
    log().info( "Reserving " + target + " for dynamic stop" );
    final String srcName = Thread.currentThread().getName();
    final int srcVmid = RemoteTestModule.MyVmid;
    final ClientVmInfo match = reserveToStopRemote( srcName, srcVmid,
                                                    syncMode, stopMode,
                                                    target );
    String act = "dynamic stop (" + toSyncModeString( syncMode ) + ":"
               + toStopModeString( stopMode ) + ":"
               + toStartModeString( startMode ) + ") " + match
               + " because " + reason;
    log().info( "Reserved " + match + " for " + act );

    final int actionId = DynamicActionUtil.nextActionId();
    Runnable action = new Runnable() {
      public void run() {
        stopRemote( srcName, srcVmid, actionId,
                    reason, syncMode, stopMode, startMode, match );
      }
    };
    boolean synchronous = (syncMode == SYNC);
    DynamicActionUtil.runActionThread(actionId, act, action, srcName,
                                      synchronous);
    return match;
  }

  /**
   *  Reserves a client VM matching the specified target info and starts it.
   */
  private static ClientVmInfo reserveAndStart( final String reason,
                                               final int syncMode,
                                               ClientVmInfo target )
  throws ClientVmNotFoundException {
    log().info( "Reserving " + target + " for dynamic start" );
    final String srcName = Thread.currentThread().getName();
    final int srcVmid = RemoteTestModule.MyVmid;
    final ClientVmInfo match = reserveToStartRemote( srcName, srcVmid, target );
    String act = "dynamic start (" + toSyncModeString( syncMode ) + ") "
               + match + " because " + reason;
    log().info( "Reserved " + match + " for " + act );

    final int actionId = DynamicActionUtil.nextActionId();
    Runnable action = new Runnable() {
      public void run() {
        startRemote( srcName, srcVmid, actionId,
                     reason, syncMode, match );
      }
    };
    boolean synchronous = (syncMode == SYNC);
    DynamicActionUtil.runActionThread(actionId, act, action, srcName,
                                      synchronous);
    return match;
  }

  /////////////////////   Remote Methods   /////////////////////

  /**
   *  Reserves a client VM matching the specified target info for future stop.
   */
  private static ClientVmInfo reserveToStopRemote( String srcName,
                                                   int srcVmid,
                                                   int syncMode, int stopMode,
                                                   ClientVmInfo target )
  throws ClientVmNotFoundException {
    ClientVmInfo match = null;
    try {
      match = RemoteTestModule.Master.reserveClientVmForStop( srcName, srcVmid,
                                                              syncMode,
                                                              stopMode,
                                                              target );
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Problem with remote operation", e );
    }
    return match;
  }

  /**
   *  Reserves a client VM matching the specified target info for future start.
   */
  private static ClientVmInfo reserveToStartRemote( String srcName,
                                                    int srcVmid,
                                                    ClientVmInfo target )
  throws ClientVmNotFoundException {
    ClientVmInfo match = null;
    try {
      match = RemoteTestModule.Master.reserveClientVmForStart( srcName, srcVmid,
                                                               target );
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Problem with remote operation", e );
    }
    return match;
  }

  /**
   *  Stops a client VM previously reserved.
   */
  protected static void stopRemote( String srcName, int srcVmid,
                                  int actionId,
                                  String reason, int syncMode,
                                  int stopMode, int startMode,
                                  ClientVmInfo match ) {
    try {
      RemoteTestModule.Master.stopClientVm
            (
              srcName, srcVmid, actionId,
              reason, syncMode, stopMode, startMode, match
            );
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Problem with remote operation", e );
    }
  }

  /**
   *  Starts a client VM previously reserved.
   */
  protected static void startRemote( String srcName, int srcVmid,
                                   int actionId,
                                   String reason, int syncMode,
                                   ClientVmInfo match ) {
    try {
      RemoteTestModule.Master.startClientVm
            (
              srcName, srcVmid, actionId,
              reason, syncMode, match
            );
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Problem with remote operation", e );
    }
  }

  ////////////////////   Utility Methods   /////////////////////

  /**
   *  Ensures API is used only from tasks of type TASK and validates arguments.
   */
  private static void validate( int syncMode, int stopMode, int startMode ) {
    if (RemoteTestModule.getCurrentThread() == null) {
      String s = "The ClientVmMgr API can only be used from hydra threads -- "
               + "use HydraSubthread when creating threads that use the API";
      throw new UnsupportedOperationException(s);
    }
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    if ( task.getTaskType() != TestTask.TASK ) {
      String s = "The ClientVmMgr API can only be used from tasks of type TASK";
      throw new UnsupportedOperationException( s );
    }
    switch( syncMode ) {
      case SYNC:
      case ASYNC:
        break;
      default:
        throw new HydraInternalException( "Invalid sync mode: "
                                        + toSyncModeString( syncMode ) );
    }
    switch( stopMode ) {
      case NICE_KILL:
      case MEAN_KILL:
      case NICE_EXIT:
      case MEAN_EXIT:
      case NICE_DISCONNECT:
        break;
      default:
        throw new IllegalArgumentException( "Invalid stop mode: "
                                          + toStopModeString( stopMode ) );
    }
    if ( startMode <= 0 ) {
      switch( startMode ) {
        case IMMEDIATE:
        case ON_DEMAND:
        case NEVER:
          break;
        default:
          throw new IllegalArgumentException( "Invalid start mode: "
                                            + toStartModeString( startMode ) );
      }
    }
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
