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

import hydra.HostHelper.OSType;

import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import com.gemstone.gemfire.LogWriter;

/** 
 *  Creates, starts, and stops client vms, among other things.
 */

public class ClientMgr {

  //////////////////////////////////////////////////////////////////////////////
  //                             CLIENT STATE                                 //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *   The current master list of created client vm records (vmid,vm).
   */
  private static Map<Integer,ClientVmRecord> ClientVms;

  /**
   * String representation of client mapping, made available to hydra clients.
   */
  private static List<List<String>> ClientMapping;

  /**
   *  Returns client vm records with the given id from the master list.
   */
  protected static ClientVmRecord getClientVm( int vmid ) {
    return (ClientVmRecord) ClientVms.get( new Integer( vmid ) );
  }

  /**
   *  Returns the master list of client vm records (vmid,vm).
   */
  public static Map getClientVms() {
    return ClientVms;
  }

  /**
   *  Returns the client mapping.
   */
  public static List<List<String>> getClientMapping() {
    return ClientMapping;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                           CLIENT CREATION                                //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates client vm records from a set of client descriptions and
   *  threadgroups.
   *  @return map of client vm records, indexed by vmid
   */
  protected static Map createClients( Collection cds, Map tgs ) {
    // create client vm records for each description
    log().info( "Creating vm and client records" );
    ClientVms = new HashMap();
    int count = 0;
    for ( Iterator i = cds.iterator(); i.hasNext(); ) {
      // create the appropriate number of vm records for this description
      ClientDescription cd = (ClientDescription) i.next();

      // look up info from client description
      String host = cd.getVmDescription().getHostDescription().getHostName();
      int numVms = cd.getVmQuantity();
      int numThreads = cd.getVmThreads();

      // create the specified number of vm records of this type
      for ( int j = 0; j < numVms; j++ ) {

        // set the id for the vm and the base id for threads in this vm
        int vmid = nextVmid();
        int basetid = nextBaseThreadId( numThreads );

        // create the client vm record and its client records
        ClientVmRecord vm = new ClientVmRecord( vmid, basetid, cd );
        for ( int tid = basetid; tid < basetid + numThreads; tid++ ) {
          ClientRecord cr = new ClientRecord( tid, vm );
          vm.addClient( cr );
          ++count;
        }
        ClientVms.put( new Integer( vmid ), vm );
        log().info( "Created vm and client records: " + vm.toClientString() );
      }
    }
    log().info( "Created " + ClientVms.size() + " total client VMs with "
               + count + " total client threads" );

    // map the clients to threadgroups
    if (tgs == null) {
      ClientMapping = null;
    } else {
      ClientMapper mapper = new ClientMapper( tgs );
      mapper.mapClients( ClientVms );
      ClientMapping = mapper.getMappingAsList(ClientVms);
      log().info(mapper.getMappingAsString(ClientVms));
    }
    return ClientVms;
  }

  // permanent unique logical id for each client VM
  private static int nextVmid = -1;
  private static int nextVmid() {
    return ++nextVmid;
  }

  // permanent base for unique logical id for each client thread
  private static int nextBaseThreadId = 0;
  private static int nextBaseThreadId( int batchsize ) {
    int base = nextBaseThreadId;
    nextBaseThreadId += batchsize;
    return base;
  }

  /**
   *  Fires up the given client vms for the stated purpose.
   */
  protected static void startClients( String purpose, Map vms ) {
    // @todo lises put a timeout on this step
    log().info( "Starting " + vms.size() + " client VMs" );
    for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      startClient( vm, purpose );
    }
    log().info( "Started " + vms.size() + " client VMs" );
  }

  /**
   *  Fires up the client vm based on its record and description.
   */
  private static void startClient( ClientVmRecord vm, String purpose ) {
    log().info( "Starting " + vm.toInfoString() );
    synchronized( vm ) {
      int state = vm.getState();
      if ( state != ClientVmRecord.POTENTIAL ) {
        throw new HydraInternalException( "wrong vm state: " + state );
      }
      String masterHost = HostHelper.getCanonicalHostName();
      int masterPid = ProcessMgr.getProcessId();
      int pid = Java.javaRemoteTestModule( masterHost, masterPid, vm, purpose );
      vm.setPid( pid );
      vm.setState( ClientVmRecord.PENDING );
      vm.bumpVersion();
      vm.bumpJavaHome();
    }
    log().info( "Started " + vm.toInfoString() );
  }

  /**
   *  Waits <code>Prms.maxClientStartupWaitSec</code> for the given client vms
   *  to register all their client threads with master.
   */
  protected static void waitForClientsToRegister( Map vms ) {
    int waitSec = tab().intAt( Prms.maxClientStartupWaitSec );
    log().info( "Waiting " + waitSec + " seconds for " + vms.size()
              + " client vms to register." );
    long timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      if ( ! waitForPendingClient( vm, timeoutMs ) ) {
        String s = "Failed to register client vms within "
                 + waitSec + " seconds, such as " + vm;
        throw new HydraTimeoutException( s );
      }
    }
    log().info( "Within " + waitSec + " seconds, all " + vms.size()
              + " client vms have registered." );
  }

  /**
   *  Waits until the specified timeout for a client vm to start and register
   *  all its client threads with master.  Returns true if it does so in time.
   */
  private static boolean waitForPendingClient( ClientVmRecord vm,
                                               long timeoutMs ) {
    while ( System.currentTimeMillis() < timeoutMs ) {
      synchronized( vm ) {
        if ( vm.getState() == ClientVmRecord.LIVE ) {
          return true;
        }
      }
      MasterController.sleepForMs(250);
    }
    return false;
  }

  /**
   * Used by hydra clients to get the VM ID corresponding to a host and PID.
   * There are no guarantees that the VM ID is valid for dynamic actions.
   */
  protected static Integer getVmid(String host, int pid) {
    boolean fine = Log.getLogWriter().fineEnabled();
    for (Iterator i = ClientVms.values().iterator(); i.hasNext();) {
      ClientVmRecord vm = (ClientVmRecord)i.next();
      synchronized (vm) {
        if (fine) {
          Log.getLogWriter().fine("Comparing " + host + ":" + pid
                                 + " to " + vm.getHost() + ":" + vm.getPid());
        }
        if (vm.getPid() == pid && HostHelper.compareHosts(host, vm.getHost())) {
          if (fine) {
            Log.getLogWriter().fine("Matched " + host + ":" + pid
                                   + " to " + vm.getHost() + ":" + vm.getPid());
          }
          return vm.getVmid();
        }
      }
    }
    if (fine) {
      Log.getLogWriter().fine("No match found for " + host + ":" + pid);
    }
    return null;
  }


  /**
   * Used by hydra clients to get the VM ID corresponding to a PID.
   * There are no guarantees that the VM ID is valid for dynamic actions.
   */
  protected static Integer getVmid(int pid) {
    for (Iterator i = ClientVms.values().iterator(); i.hasNext();) {
      ClientVmRecord vm = (ClientVmRecord)i.next();
      synchronized (vm) {
        if (vm.getPid() == pid) {
          return vm.getVmid();
        }
      }
    }
    return null;
  }
  
  /**
   *  Used by new client processes to get an accurate PID (needed for Linux)
   *  before they start their client threads.  This method must block until the
   *  master has recorded the PID for the newly started vm.
   */
  protected static int getPid( int vmid ) {
    ClientVmRecord vm = (ClientVmRecord) ClientVms.get( new Integer( vmid ) );
    synchronized( vm ) {
      return vm.getPid();
    }
  }

  /** 
   *  Used by new client threads to tell master they are ready and to get their
   *  threadgroup information.  The last thread to show up triggers the vm to
   *  go live.
   */
  public static HydraThreadGroupInfo registerClient( int vmid, int tid,
                                                     RemoteTestModuleIF mod ) {
    ClientVmRecord vm = (ClientVmRecord) ClientVms.get( new Integer( vmid ) );
    ClientRecord cr = vm.getClient( tid );
    synchronized( cr ) {
      cr.setTestModule( mod );
    }
    synchronized( vm ) {
      vm.registerClient();
      if ( vm.fullyRegistered() ) {
        vm.setState( ClientVmRecord.LIVE );
      }
    }
    return new HydraThreadGroupInfo( cr.getThreadGroupName(),
                                     cr.getThreadGroupId() );
  }

  /** 
   * Used by a dynamic action thread to tell master a JVM has finished
   * disconnecting.
   */
  public static void registerClientDisconnect(int vmid) {
    ClientVmRecord vm = getClientVm(vmid);
    synchronized(vm) {
      vm.registerDisconnect();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                          CLIENT DESTRUCTION                              //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Tells the given client vms to exit.  Optionally asks clients to disconnect
   *  from GemFire before exiting.
   *  <p>
   *  Disconnect should only be used if all client threads are finished doing
   *  work, to prevent spurious error reporting.
   */
  protected static void stopClients( Map vms, boolean closeConnection,
					      boolean runShutdownHook) {
    // @todo lises put a timeout on this step
    log().info( "Issuing stop request to " + vms.size() + " client VMs" );
    for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      stopClient( vm, closeConnection, runShutdownHook);
    }
    log().info( "Issued stop request to " + vms.size() + " client VMs" );
  }
  
  /**
   *  Tells the given client vm to exit.  Optionally asks it to disconnect
   *  from GemFire before exiting.
   */
  private static void stopClient( ClientVmRecord vm,
                                  boolean closeConnection,
				  boolean runShutdownHook) {
    log().info( "Issuing stop request to " + vm.toInfoString() );
    synchronized( vm ) {
      if ( vm.getPid() == ClientVmRecord.NO_PID ) {
        log().info( vm.toInfoString() + " is already stopped" );
        return;
      }
      ClientRecord cr = vm.getRepresentativeClient();
      try {
        cr.getTestModule().shutDownVM( closeConnection, runShutdownHook );
      } catch( UnmarshalException ignore ) {
        // vm is shutting down, don't expect clean reply
      } catch( RemoteException e ) {
        String reason = "Unable to reach client " + cr + " in " + vm;
        throw new HydraRuntimeException( reason, e );
      }
    }
    log().info( "Issued stop request to " + vm.toInfoString() );
  }

  /**
   * Tells the given client vm to disconnect from the distributed system.
   */
  private static void disconnectClient(ClientVmRecord vm) {
    log().info("Issuing disconnect request to " + vm.toInfoString());
    synchronized(vm) {
      if (vm.getState() == ClientVmRecord.DEAD) {
        log().info(vm.toInfoString() + " is dead and cannot disconnect");
        return;
      }
      if (vm.getState() == ClientVmRecord.DISCONNECTED) {
        log().info(vm.toInfoString() + " is already disconnected");
        return;
      }
      ClientRecord cr = vm.getRepresentativeClient();
      try {
        cr.getTestModule().disconnectVM();
      } catch (RemoteException e) {
        String reason = "Unable to reach client " + cr + " in " + vm;
        throw new HydraRuntimeException(reason, e);
      }
    }
    log().info("Issued disconnect request to " + vm.toInfoString());
  }

  /** run client shutdown hooks without shutting down the clients */
  protected static void runClientShutdownHooks(Map vms) {
    if (tab().get(Prms.clientShutdownHook) != null) {
      log().info("Printing process stacks in " + vms.size() + " client VMs prior to running client shutdown hooks");
      MasterController.dumpStacks(2);
      log().info("Running client shutdown hooks in " + vms.size() + " client VMs");
      int waitSec = tab().intAt(Prms.maxClientShutdownWaitSec);
      Vector ops = new Vector();
      Vector records = new Vector();
      for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
        final ClientVmRecord vm = (ClientVmRecord)i.next();
        Runnable op = new Runnable() {
          public void run() {
            runClientShutdownHook(vm);
          }
        };
        ops.add(op);
        records.add("");
      }
      ResourceOperation.doOps(ops, "runShutdownHook", "client", records,
                              waitSec*1000, true /* concurrent */, 0 /* delayMs */,
                              true /* verbose */);
      log().info("Ran client shutdown hooks in " + vms.size() + " client VMs");
    }
  }

  private static void runClientShutdownHook( ClientVmRecord vm ) {
    log().info( "Running client shutdown hook in " + vm.toInfoString() );
    ClientRecord cr = null;
    synchronized( vm ) {
      if ( vm.getPid() == ClientVmRecord.NO_PID ) {
        log().info( vm.toInfoString() + " is stopped: can't run client shutdown hook" );
        return;
      }
      cr = vm.getRepresentativeClient();
    }
    // must run hook outside vm synchronization since it is a fully synchronous
    // RMI call that could hang, deadlocking master attempt to do stack dumps,
    // but all is well since the cr test module is never unset and we already
    // handle an intervening shutdown
    try {
      cr.getTestModule().runShutdownHook( );
    } catch( UnmarshalException ignore ) {
      // vm is shutting down, don't expect clean reply
    } catch( RemoteException e ) {
      String reason = "Unable to reach client " + cr + " in " + vm;
      log().severe(reason, e);
    }
    log().info( "Ran client shutdown hook in " + vm.toInfoString() );
  }

  /**
   *  Waits <code>Prms.maxClientShutdownWaitSec</code> for the given client vms
   *  to come down.
   */
  protected static void waitForClientsToDie( Map vms ) {
    int waitSec = tab().intAt( Prms.maxClientShutdownWaitSec );
    log().info( "Waiting " + waitSec + " seconds for " + vms.size()
              + " client VMs to stop." );
    long timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      if ( ! waitForClientToDie( vm, timeoutMs ) ) {
        String s = "Failed to stop client vms within " + waitSec
                 + " seconds, starting with " + vm;
        throw new HydraTimeoutException( s );
      }
    }
  }

  private static boolean waitForClientToDie( ClientVmRecord vm,
                                             long timeoutMs ) {
    int pid = vm.getPid();
    if ( pid == ClientVmRecord.NO_PID ) { // killed off previously
      return true;
    }
    String host = vm.getHost();
    do {
      if ( ! ProcessMgr.processExists( host, pid ) ) {
        synchronized( vm ) {
          HostDescription hd = vm.getClientDescription().getVmDescription()
                                 .getHostDescription();
          Nuker.getInstance().removePID(hd, pid);
          vm.setState( ClientVmRecord.DEAD );
          vm.setPid( ClientVmRecord.NO_PID );
        }
        return true;
      }
      MasterController.sleepForMs(250);
    } while ( System.currentTimeMillis() < timeoutMs );
    return false;
  }

  /**
   * Waits <code>Prms.maxClientShutdownWaitSec</code> for the given client vm
   * to disconnect.
   */
  protected static void waitForClientToDisconnect(ClientVmRecord vm) {
    int waitSec = tab().intAt(Prms.maxClientShutdownWaitSec);
    log().info("Waiting " + waitSec + " seconds for client VM " + vm
              + " to disconnect." );
    long timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    if (!waitForClientToDisconnect(vm, timeoutMs)) {
      String s = "Failed to disconnect client vm within " + waitSec
               + " seconds: " + vm;
      throw new HydraTimeoutException(s);
    }
  }

  private static boolean waitForClientToDisconnect(ClientVmRecord vm,
                                                   long timeoutMs) {
    int pid = vm.getPid();
    if (pid == ClientVmRecord.NO_PID) { // killed off previously
      return true;
    }
    do {
      if (vm.registeredDisconnect()) {
        Log.getLogWriter().info(vm + " has registered disconnect");
        return true;
      }
      MasterController.sleepForMs(250);
    } while (System.currentTimeMillis() < timeoutMs);
    return false;
  }

  /**
   *  Prints process stacks for all live client vms.  Throw in the master also.
   */
  public static void printProcessStacks() {
    // master
    ProcessMgr.printProcessStacks( HostHelper.getLocalHost(),
                                   ProcessMgr.getProcessId() );
    // clients
    if (ClientVms == null) {
      return;
    }
    for ( Iterator i = ClientVms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      synchronized( vm ) {
        if ( vm.getState() == ClientVmRecord.LIVE ) {
          printProcessStacks( vm );
        }
      }
    }
  }

  /**
   *  Prints process stacks for a specific client vm.
   */
  protected static void printProcessStacks( ClientVmRecord vm ) {
    ProcessMgr.printProcessStacks( vm.getHost(), vm.getPid() );
  }

  //////////////////////////////////////////////////////////////////////////////
  //                         DYNAMIC START AND STOP                           //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Reserves a client VM matching the specified target info for future stop.
   *  Returns null if no match is found.
   *  @param srcName Thread name for the invoking client.
   *  @param srcVmid Logical VM id for the invoking client.
   *  @param syncMode Synchronization mode for the dynamic stop.
   *  @param stopMode Stop mode for the dynamic stop.
   *  @param target Information used to select the target client VM.
   *  @return Fully specified information for the selected VM.
   *  @throws ClientVmNotFoundException if no match could be made.
   *  @throws IllegalArgumentException if the target info is inconsistent
   *                                 with any known VM or forces deadlock.
   *  @throws DynamicActionException if the test is terminating and refuses to
   *                                 allow any further dynamic actions.
   */
  protected static ClientVmInfo reserveClientVmForStop( String srcName,
                                                        int srcVmid,
                                                        int syncMode,
                                                        int stopMode,
                                                        ClientVmInfo target )
  throws ClientVmNotFoundException {
    String name = srcName + " Dynamic Client VM Stopper";
    Thread.currentThread().setName( name );

    // find the source
    ClientVmRecord srcVm =
        (ClientVmRecord) ClientVms.get( new Integer( srcVmid ) );
    if ( srcVm == null ) {
      throw new HydraInternalException( "Missing source vm_" + srcVmid );
    }

    // find the matches
    Set matches = matchClientVm( target );
    if ( matches == null ) {
      throw new IllegalArgumentException( "No such VM exists: " + target );
    }

    // avoid obvious deadlock
    preventDeadlock( srcVm, syncMode, stopMode, target, matches );

    // reserve the vm
    return reserveClientVm( ClientVmRecord.LIVE, srcVm, target, matches );
  }

  /**
   *  Reserves a client VM matching the specified target info for future start.
   *  Returns null if no match is found.
   *  @param srcName Thread name for the invoking client.
   *  @param srcVmid Logical VM id for the invoking client.
   *  @param target Information used to select the target client VM.
   *  @return Fully specified information for the selected VM.
   *  @throws ClientVmNotFoundException if no match could be made.
   *  @throws IllegalArgumentException if the target info is inconsistent
   *                                 with any known VM.
   *  @throws DynamicActionException if the test is terminating and refuses to
   *                                 allow any further dynamic actions.
   */
  protected static ClientVmInfo reserveClientVmForStart( String srcName,
                                                         int srcVmid,
                                                         ClientVmInfo target )
  throws ClientVmNotFoundException {
    String name = srcName + " Dynamic Client VM Starter";
    Thread.currentThread().setName( name );

    // find the source
    ClientVmRecord srcVm =
        (ClientVmRecord) ClientVms.get( new Integer( srcVmid ) );
    if ( srcVm == null ) {
      throw new HydraInternalException( "Missing source vm_" + srcVmid );
    }

    // find the matches
    Set matches = matchClientVm( target );
    if ( matches == null ) {
      throw new IllegalArgumentException( "No such VM exists: " + target );
    }

    // reserve the vm
    try {
      return reserveClientVm( ClientVmRecord.POTENTIAL, srcVm, target, matches );
    } catch (ClientVmNotFoundException e) {
      return reserveClientVm( ClientVmRecord.DISCONNECTED, srcVm, target, matches );
    }
  }

  /**
   *  Tries to find and reserve a suitable VM for future dynamic action.  If it
   *  finds one, it also protects the source from being targeted by clients in
   *  its own or other vms until it has completed the reservation and invoked
   *  the dynamic action rmi call, to prevent zombie reservations.
   */
  private static ClientVmInfo reserveClientVm( int state,
                                               ClientVmRecord srcVm,
                                               ClientVmInfo target,
                                               Set matches )
  throws ClientVmNotFoundException {

    // try to hit one of the targets, being careful to avoid deadlock
    for ( Iterator i = matches.iterator(); i.hasNext(); ) {
      ClientVmRecord trgVm = (ClientVmRecord) i.next();
      synchronized( DynamicActionLock ) {
        if ( srcVm == trgVm ) {
          synchronized( trgVm ) {
            ClientVmInfo info = reserveClientVm( state, srcVm, trgVm );
            if ( info != null ) return info;
          }
        } else if ( srcVm.getVmid().intValue() < trgVm.getVmid().intValue() ) {
          synchronized( srcVm ) {
            synchronized( trgVm ) {
              ClientVmInfo info = reserveClientVm( state, srcVm, trgVm );
              if ( info != null ) return info;
            }
          }
        } else if ( srcVm.getVmid().intValue() > trgVm.getVmid().intValue() ) {
          synchronized( trgVm ) {
            synchronized( srcVm ) {
              ClientVmInfo info = reserveClientVm( state, srcVm, trgVm );
              if ( info != null ) return info;
            }
          }
        }
      }
    }
    String s = "All VMs matching " + target + " are unavailable to " + srcVm;
    throw new ClientVmNotFoundException( s );
  }

  /**
   *  Prevents the most obvious deadlock case: a vm cannot exit itself
   *  synchronously since that will deadlock waiting for the invoking client
   *  thread to finish its current task, which is this one.
   */
  private static void preventDeadlock( ClientVmRecord srcVm,
                                       int syncMode, int stopMode,
                                       ClientVmInfo target, Set matches ) {

    if ( syncMode == ClientVmMgr.SYNC && ( stopMode == ClientVmMgr.MEAN_EXIT ||
           stopMode == ClientVmMgr.NICE_EXIT ) ) {

      // doing a synchronous exit, so throw source out if present
      for ( Iterator i = matches.iterator(); i.hasNext(); ) {
        ClientVmRecord trgVm = (ClientVmRecord) i.next();
        if ( srcVm.getVmid().equals( trgVm.getVmid() ) ) {
          i.remove(); // throw source out of the mix
        }
      }
      if ( matches.size() == 0 ) { // deadlock was guaranteed, so complain
        String s = srcVm + " cannot stop itself synchronously using an \"exit\""
                 + " mode without deadlocking.  Use an asynchronous method or"
                 + " a \"kill\" mode.";
        throw new IllegalArgumentException( s );
      }
    }
  }

  /**
   *  Returns the set of all vms matching the target info.
   */
  private static Set matchClientVm( ClientVmInfo target ) {
    Set matches = null;
    for ( Iterator i = ClientVms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      if ( vm.matches( target ) ) {
        if ( matches == null ) {
          matches = new HashSet();
        }
        matches.add( vm );
      }
    }
    return matches;
  }

  /**
   *  Returns the info of the target, if it is available, after protecting the
   *  source and reserving the target.
   */
  private static ClientVmInfo reserveClientVm( int state, ClientVmRecord srcVm,
                                                          ClientVmRecord trgVm )
  throws ClientVmNotFoundException {
    if ( srcVm.reserved() ) { // somebody put out a hit on the hitter
      String s = srcVm.getInfo() + " is the target of a dynamic action";
      throw new ClientVmNotFoundException( s );
    }
    if ( trgVm.getState() == state && ! trgVm.reserved() && ! trgVm.acting() ) {

      synchronized( DynamicActionLock ) {
        if ( RefuseDynamicActions ) {
          String s = "Dynamic action refused.  Test is terminating in failure.";
          throw new DynamicActionException( s );
        }
        ++DynamicActionCount;           // protect the master
      }

      if ( srcVm != trgVm ) {
        srcVm.incDynamicActionCount();  // protect the source
      }
      trgVm.reserve();                  // reserve the target
      return trgVm.getInfo();           // return the info
    }
    return null;
  }

  /**
   *  Stops the target client VM.  Exceptions and hangs are handled here in
   *  case the source invoked asynchronously or has in turn become a target.
   *  <p>
   *  Creates and runs a thread to carry out the action asynchronously on behalf
   *  of the master.  If the action is synchronous, it uses a remote signal to
   *  notify the source when it is complete.  All exceptions are handled in the
   *  thread.  None are thrown back to the client.  Clients are left to time out
   *  on their own in the case of a hang.
   */
  protected static void stopClientVm( final String srcName,
            final int srcVmid, final int srcActionId,
            final String reason, final int syncMode,
            final int stopMode, final int startMode,
            final ClientVmInfo target ) {
    Runnable action = new Runnable() {
      public void run() {
        _stopClientVm( srcName, srcVmid, srcActionId,
                       reason, syncMode, stopMode, startMode, target );
      }
    };
    String name = srcName + " Dynamic Client VM Stopper";
    Thread t = new Thread( action, name );
    t.start();
  }

  /**
   *  Starts the target client VM.  Exceptions and hangs are handled here in
   *  case the source invoked asynchronously or has in turn become a target.
   *  <p>
   *  Creates and runs a thread to carry out the action asynchronously on behalf
   *  of the master.  If the action is synchronous, it uses a remote signal to
   *  notify the source when it is complete.  All exceptions are handled in the
   *  thread.  None are thrown back to the client.  Clients are left to time out
   *  on their own in the case of a hang.
   */
  protected static void startClientVm( final String srcName,
            final int srcVmid, final int srcActionId,
            final String reason, final int syncMode,
            final ClientVmInfo target ) {
    Runnable action = new Runnable() {
      public void run() {
        _startClientVm( srcName, srcVmid, srcActionId,
                        reason, syncMode, target );
      }
    };
    String name = srcName + " Dynamic Client VM Starter";
    Thread t = new Thread( action, name );
    t.start();
  }

  /**
   *  Stops the target client VM.  Throws no exceptions.
   */
 protected static void _stopClientVm( String srcName, int srcVmid,
         int srcActionId,
         String reason, int syncMode, int stopMode, int startMode,
         ClientVmInfo target ) {
    try {
      ClientVmRecord srcVm =
          (ClientVmRecord) ClientVms.get( new Integer( srcVmid ) );
      ClientVmRecord trgVm =
          (ClientVmRecord) ClientVms.get( target.getVmid() );

      synchronized( srcVm ) { // open the source to attack
        srcVm.decDynamicActionCount();
      }
      stopClientVm( srcName, srcVmid, srcVm, srcActionId,
                    reason, syncMode, stopMode, startMode, target, trgVm );
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      BaseTaskScheduler.processDynamicActionError( t );
    } finally {
      synchronized( DynamicActionLock ) {
        --DynamicActionCount;
      }
    }
  }

  /**
   *  Starts the target client VM.  Throws no exceptions.
   */
  protected static void _startClientVm( String srcName, int srcVmid,
          int srcActionId, String reason, int syncMode, ClientVmInfo target ) {
    try {
      ClientVmRecord srcVm =
          (ClientVmRecord) ClientVms.get( new Integer( srcVmid ) );
      ClientVmRecord trgVm =
          (ClientVmRecord) ClientVms.get( target.getVmid() );

      synchronized( srcVm ) { // free the source to be attacked by others
        srcVm.decDynamicActionCount();
      }
      startClientVm( srcName, srcVmid, srcVm, srcActionId,
                     reason, syncMode, target, trgVm );
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      BaseTaskScheduler.processDynamicActionError( t );
    } finally {
      synchronized( DynamicActionLock ) {
        --DynamicActionCount;
      }
    }
  }

  /**
   *  Actually stops the client VM specified by target.
   */
  private static void stopClientVm( String srcName, int srcVmid,
          ClientVmRecord srcVm, int srcActionId,
          String reason, int syncMode, int stopMode, int startMode,
          ClientVmInfo target, ClientVmRecord trgVm ) {

    if (stopMode == ClientVmMgr.NICE_DISCONNECT) {
      log().info( "Disconnecting " + target + " because " + reason );
    } else {
      log().info( "Stopping " + target + " because " + reason );
    }

    int srcPid = srcVm.getPid(); // store for later error checking

    // stop or disconnect the client
    switch( stopMode ) {
      case ClientVmMgr.NICE_KILL:
      case ClientVmMgr.MEAN_KILL:
        killClientVm( trgVm, stopMode );
        break;
      case ClientVmMgr.NICE_EXIT:
      case ClientVmMgr.MEAN_EXIT:
        exitClientVm( trgVm, stopMode );
        break;
      case ClientVmMgr.NICE_DISCONNECT:
        disconnectClientVm( trgVm, stopMode );
        break;
      default:
        throw new HydraInternalException( "Illegal stop mode: " + stopMode );
    }

    // set the target vm state and do the logging
    String s = "Stopped " + target + " because " + reason + ". ";
    switch( startMode ) {
      case ClientVmMgr.NEVER:
        if (stopMode == ClientVmMgr.NICE_DISCONNECT) {
          log().info( s + "Never reconnecting " + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.resetForDisconnect();
          }
        } else {
          log().info( s + "Never restarting " + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.reset();
            trgVm.setState( ClientVmRecord.DEAD );
          }
        }
        break;
      case ClientVmMgr.ON_DEMAND:
        if (stopMode == ClientVmMgr.NICE_DISCONNECT) {
          log().info( s + "Reconnecting " + trgVm + " on demand." );
          synchronized( trgVm ) {
            trgVm.resetForDisconnect();
          }
        } else {
          log().info( s + "Restarting " + trgVm + " on demand." );
          synchronized( trgVm ) {
            trgVm.reset();
          }
        }
        break;
      case ClientVmMgr.IMMEDIATE:
        if (stopMode == ClientVmMgr.NICE_DISCONNECT) {
          log().info( s + "Immediately reconnecting " + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.resetForDisconnect();
            trgVm.reserve();
          }
        } else {
          log().info( s + "Immediately restarting " + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.reset();
            trgVm.reserve();
          }
        }
        break;
      default:
        if (stopMode == ClientVmMgr.NICE_DISCONNECT) {
          log().info( s + "Sleeping " + startMode + " ms before reconnecting "
                    + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.resetForDisconnect();
            trgVm.reserve();
          }
        } else {
          log().info( s + "Sleeping " + startMode + " ms before restarting "
                    + trgVm + "." );
          synchronized( trgVm ) {
            trgVm.reset();
            trgVm.reserve();
          }
        }
        break;
    }

    // notify the client that the stop or disconnect is complete
    if ( syncMode == ClientVmMgr.SYNC && ! srcVm.equals( trgVm ) ) {
      srcVm.notifyDynamicActionComplete( srcActionId, srcPid );
    }

    // restart or reconnect the target
    switch( startMode ) {
      case ClientVmMgr.NEVER:
        break;
      case ClientVmMgr.ON_DEMAND:
        break;
      case ClientVmMgr.IMMEDIATE:
        startClientVm( srcName, srcVmid, srcVm, srcActionId,
                       reason, syncMode, target, trgVm );
        break;
      default:
        MasterController.sleepForMs( startMode );
        startClientVm( srcName, srcVmid, srcVm, srcActionId,
                       reason, syncMode, target, trgVm );
        break;
    }
  }

  /**
   *  Actually starts the client VM specified by target.
   */
  private static void startClientVm( String srcName, int srcVmid,
          ClientVmRecord srcVm, int srcActionId,
          String reason, int syncMode,
          ClientVmInfo target, ClientVmRecord trgVm ) {

    boolean disconnected = trgVm.isDisconnected();
    if (disconnected) {
      log().info("Reconnecting " + target + " because " + reason);
    } else {
      log().info("Starting " + target + " because " + reason);
    }

    int srcPid = srcVm.getPid(); // store for later error checking

    // start the vm
    boolean passed = doDynamicInitTasks(trgVm);
    if ( ! passed ) {
      throw new DynamicActionException( "dynamic start failed" );
    }

    // set the vm state and do the logging
    synchronized( trgVm ) {
      trgVm.unreserve();
    }
    if (disconnected) {
      log().info( "Reconnected " + target + " because " + reason );
    } else {
      log().info( "Started " + target + " because " + reason );
    }

    // notify the client that the start is complete
    if ( syncMode == ClientVmMgr.SYNC ) {
      srcVm.notifyDynamicActionComplete( srcActionId, srcPid );
    }
  }

  /**
   *  Does kill type stops.  Fakes their task results.
   */
  private static void killClientVm( ClientVmRecord vm, int stopMode ) {
    switch( stopMode ) {
      case ClientVmMgr.NICE_KILL:
        ProcessMgr.shutdownProcess( vm.getHost(), vm.getPid() );
        break;
      case ClientVmMgr.MEAN_KILL:
        ProcessMgr.killProcess( vm.getHost(), vm.getPid() );
        break;
    }
    int waitSec = tab().intAt( Prms.maxClientShutdownWaitSec );
    long timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    log().info("Waiting " + waitSec + " seconds for " + vm + " to die");
    if (!waitForClientToDie(vm, timeoutMs)) {
      String s = "Failed to stop " + vm + " within " + waitSec + " seconds";
      throw new HydraTimeoutException(s);
    }
    log().info("Client " + vm + " has died");
    BaseTaskScheduler.fakeKillResults( vm );
  }

  /**
   *  Does exit type stops.  Runs them through closetasks.
   */
  private static void exitClientVm( ClientVmRecord vm, int stopMode ) {
    boolean passed = false;
    switch( stopMode ) {
      case ClientVmMgr.NICE_EXIT:
        passed = doDynamicCloseTasks( vm, true, true );
        break;
      case ClientVmMgr.MEAN_EXIT:
        passed = doDynamicCloseTasks( vm, false, true );
        break;
    }
    if ( ! passed ) {
      throw new DynamicActionException( "dynamic stop failed" );
    }
  }

  /**
   * Does disconnect type stops. Runs them through closetasks.
   */
  private static void disconnectClientVm(ClientVmRecord vm, int stopMode) {
    boolean passed = false;
    switch (stopMode) {
      case ClientVmMgr.NICE_DISCONNECT:
        passed = doDynamicCloseTasks(vm, true, false);
        break;
    }
    if ( ! passed ) {
      throw new DynamicActionException( "dynamic disconnect failed" );
    }
  }

  /**
   * Does kill type stops for unit test vms followed by immediate restart.
   * Returns the new vm record.  Does not bother to synchronize since this
   * is taken care of in the unit test controller (see dunit.VM).
   */
  protected static ClientVmRecord bounceUnitTestVm(int pid) 
  throws RemoteException {
    try {
      return _bounceUnitTestVm(pid);
    }
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch (Throwable t) {
      throw new RemoteException("While bouncing unit test VM", t);
    }
  }

  private static ClientVmRecord _bounceUnitTestVm(int pid) {
    Integer vmid = getVmid(pid);
    if (vmid == null) {
      String s = "No VM found with PID " + pid;
      throw new HydraInternalException(s);
    }
    ClientVmRecord vm = getClientVm(vmid.intValue());
    log().info("Bouncing " + vm.toInfoString());
    ProcessMgr.killProcess(vm.getHost(), vm.getPid());
    int waitSec = tab().intAt(Prms.maxClientShutdownWaitSec);
    long timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    log().info("Waiting " + waitSec + " seconds for " + vm + " to die");
    if (!waitForClientToDie(vm, timeoutMs)) {
      String s = "Failed to stop " + vm + " within " + waitSec + " seconds";
      throw new HydraTimeoutException(s);
    }
    vm.reset();
    log().info("Client " + vm + " has died...restarting");

    startClient(vm, null);
    waitSec = tab().intAt(Prms.maxClientStartupWaitSec);
    timeoutMs = System.currentTimeMillis() + waitSec * 1000;
    waitForPendingClient(vm, timeoutMs);
    log().info("Client " + vm + " has registered and is ready for use");
    log().info("Bounced " + vm.toInfoString());
    return vm;
  }

  /**
   *  Runs inittasks for dynamic starts and reconnects.
   */
  private static boolean doDynamicInitTasks(ClientVmRecord vm) {
    Vector tasks = TestConfig.getInstance().getDynamicInitTasksClone();
    TaskScheduler ts = null;
    boolean halt = TestConfig.tab().booleanAt( Prms.haltIfBadResult );
    if ( TestConfig.tab().booleanAt( Prms.doInitTasksSequentially ) ) {
      ts = new DynamicSerialTaskScheduler( "DynamicInitTasks",
                                            tasks, null, vm, true );
    } else {
      ts = new DynamicConcurrentTaskScheduler( "DynamicInitTasks",
                                                tasks, null, vm, true );
    }
    if (vm.isDisconnected()) {
      synchronized(vm) {
        vm.setState(ClientVmRecord.LIVE);
      }
    } else {
      ts.startClients();
    }
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      tab().longAt( Prms.maxResultWaitSec ) );
    ts.printReport();
    return passed;
  }

  /**
   *  Runs closetasks for dynamic stops.
   */
  private static boolean doDynamicCloseTasks( ClientVmRecord vm,
                                              boolean disconnect, boolean stop ) {
    Vector tasks = TestConfig.getInstance().getDynamicCloseTasksClone();
    TaskScheduler ts =
      new DynamicConcurrentTaskScheduler( "DynamicCloseTasks",
                                           tasks, null, vm, disconnect );
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      tab().longAt( Prms.maxResultWaitSec ) );
    if (stop) {
      ts.stopClients();
    } else {
      disconnectClient(vm);
      waitForClientToDisconnect(vm);
    }
    ts.printReport();
    return passed;
  }

  /**
   * Returns the VM IDs (as Integers) for all client VMs, regardless of whether
   * the VMs are currently started or stopped.
   */
  protected static Vector getClientVmids() {
    Vector vmids = new Vector();
    for ( Iterator i = ClientVms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      vmids.add(vm.getVmid());
    }
    return vmids;
  }

  /**
   * Returns the VM IDs (as Integers) for all client VMs except the given one,
   * regardless of whether the VMs are currently started or stopped.
   */
  protected static Vector getOtherClientVmids(int vmid) {
    Vector vmids = new Vector();
    for ( Iterator i = ClientVms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      if (vm.getVmid().intValue() != vmid) {
        vmids.add(vm.getVmid());
      }
    }
    return vmids;
  }

  protected static boolean clientVmsPermanentlyStopped() {
    synchronized( DynamicActionLock ) {
      if ( hasDynamicActions() ) {
        return false; // have ongoing dynamic actions
      }
      for ( Iterator i = ClientVms.values().iterator(); i.hasNext(); ) {
        ClientVmRecord vm = (ClientVmRecord) i.next();
        synchronized( vm ) {
          if ( vm.getPid() != ClientVmRecord.NO_PID ) {
            return false; // have an existing client vm
          }
        }
      }
      return true; // no hope of having client vms in future
    }
  }

  /**
   *  Tells the test to refuse any further dynamic actions.  Invoked when the
   *  test is terminating task execution.
   */
  protected static void refuseDynamicActions() {
    synchronized( DynamicActionLock ) {
      RefuseDynamicActions = true;
    }
  }

  /**
   *  Returns true if there are uninterruptible dynamic actions.
   */
  protected static boolean hasDynamicActions() {
    synchronized( DynamicActionLock ) {
      return DynamicActionCount != 0;
    }
  }

  private static Object DynamicActionLock = new Object();
  private static boolean RefuseDynamicActions = false;
  private static int DynamicActionCount = 0;

  //////////////////////////////////////////////////////////////////////////////
  //                         DYNAMIC REBOOT                                   //
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Reserves the host for future reboot.
   * @param srcName Thread name for the invoking client.
   * @param srcVmid Logical VM id for the invoking client.
   * @param target The host to reboot.
   * @param liveOnly Whether to return only processes that were live at reboot.
   * @return Info about processes running on the host.
   * @throws RebootHostNotFoundException if the host is not available.
   * @throws IllegalArgumentException if the host is not used in the test.
   * @throws DynamicActionException if the test is terminating and refuses to
   *                                allow any further dynamic actions.
   */
  protected static RebootInfo reserveForReboot(String srcName, int srcVmid,
                                               String target, boolean liveOnly)
  throws RebootHostNotFoundException {
    String name = srcName + " Dynamic Rebooter";
    Thread.currentThread().setName(name);
    RebootInfo result = null;

    // find the host of the source
    ClientVmRecord srcVm =
        (ClientVmRecord)ClientVms.get(Integer.valueOf(srcVmid));
    if (srcVm == null) {
      throw new HydraInternalException("Missing source vm_" + srcVmid);
    }
    String srcHost = srcVm.getHost();

    if (HostHelper.isLocalHost(target)) {
      String s = "Master controller is running on " + target;
      throw new RebootHostNotFoundException(s);
    }
    if (srcHost.equals(target)) {
      String s = "Hydra client " + srcName + " cannot reboot its own host: "
               + target;
      throw new RebootHostNotFoundException(s);
    }

    // verify that the target is a virtual machine running on unix
    String baseHost = RebootPrms.getBaseHost(target);
    HostDescription hd = TestConfig.getInstance()
                                .getAnyPhysicalHostDescription(baseHost);
    if (hd.getOSType() != OSType.unix) {
      String s = "Host " + target + " is running on an unsupported "
               + "operating system: " + hd.getOSType();
      throw new RebootHostNotFoundException(s);
    }

    if (!RebootUtil.isAlive(target)) {
      String s = "Host " + target + " is either not a virtual machine "
               + "or is not running on " + RebootPrms.getBaseHost(target);
      throw new RebootHostNotFoundException(s);
    }

    // find all of the processes on the target host and reserve them in
    // a predefined order
    List<ClientInfo> reservedClients = new ArrayList();
    List<HadoopInfo> reservedHadoops = new ArrayList();
    synchronized (DynamicActionLock) {
      RebootInfo rebootInfo = (RebootInfo)RebootBlackboard.getInstance()
                                         .getSharedMap().get(target);
      if (rebootInfo != null) {
        String s = "RebootInfo for " + target + " has not been cleared";
        throw new RebootHostNotFoundException(s);
      }

      SortedSet<Integer> vmids = new TreeSet();
      vmids.addAll(ClientVms.keySet());
      for (Integer vmid : vmids) {
        ClientVmRecord vm = ClientVms.get(vmid);
        if (vm.getHost().equals(target)) {
          if (vm.acting()) {
            String s = "At least one hydra client on " + target
                     + " is already the source of a dynamic action: " + vm;
            throw new RebootHostNotFoundException(s);
          }
          if (vm.reserved()) {
            String s = "At least one hydra client on " + target
                     + " is already the target of a dynamic action: " + vm;
            throw new RebootHostNotFoundException(s);
          }
          if (liveOnly && vm.isLive()) {
            log().info("Reserving live hydra client: " + vm.toStateString());
            ClientInfo clientInfo = new ClientInfo(vm.getVmid(),
                                                   vm.getClientDescription());
            reservedClients.add(clientInfo);
          } else if (!vm.isDead()) {
            log().info("Reserving temporarily stopped hydra client: "
                      + vm.toStateString());
            ClientInfo clientInfo = new ClientInfo(vm.getVmid(),
                                                   vm.getClientDescription());
            reservedClients.add(clientInfo);
          } else {
            log().info("Not reserving permanently dead hydra client: "
                      + vm.toStateString());
          }
        }
      }
      // reserve the affected vms in the vmid order in which they were added
      for (ClientInfo reservedClient : reservedClients) {
        ClientVmRecord vm = ClientVms.get(reservedClient.getVmid());
        vm.reserve();
        log().info("Reserved " + vm.toStateString() + " on " + target
                  + " for reboot");
      }

      // gather up hadoop processes
      reservedHadoops = HadoopMgr.getHadoopInfos(target, liveOnly);

      if (reservedClients.size() + reservedHadoops.size() == 0) {
        if (liveOnly) {
          String s = "No live hydra-managed processes found on " + target;
          throw new RebootHostNotFoundException(s);
        } else {
          String s = "Only permanently dead hydra-managed processes found on "
                   + target;
          throw new RebootHostNotFoundException(s);
        }
      }

      srcVm.incDynamicActionCount();  // protect the source
      ++DynamicActionCount;           // protect the master
      log().info("Reserved hydra client processes: " + reservedClients);
      log().info("Reserved hydra hadoop processes: " + reservedHadoops);

      result = new RebootInfo(target, reservedClients, reservedHadoops);
      RebootBlackboard.getInstance().getSharedMap().put(target, result);
      log().info("Put " + target + " with " + result + " on reboot blackboard");
    }

    return result;
  }

  /**
   * Reboots the target host. Exceptions and hangs are handled here even though
   * the method is only invoked in synchronous mode from the client.
   * <p>
   * Creates and runs a thread to carry out the action asynchronously on behalf
   * of the master. Since the action is synchronous, it uses a remote signal to
   * notify the source when it is complete. All exceptions are handled in the
   * thread. None are thrown back to the client. Clients are left to time out
   * on their own in the case of a hang.
   */
  protected static void reboot(final String srcName, final int srcVmid,
      final int srcActionId, final String reason, final RebootInfo target) {
    Runnable action = new Runnable() {
      public void run() {
        _reboot(srcName, srcVmid, srcActionId, reason, target);
      }
    };
    String name = srcName + " Dynamic Rebooter";
    Thread t = new Thread(action, name);
    t.start();
  }

  /**
   * Reboots the target host. Throws no exceptions.
   */
  protected static void _reboot(String srcName, int srcVmid, int srcActionId,
                                String reason, RebootInfo target) {
    try {
      ClientVmRecord srcVm = (ClientVmRecord)ClientVms.get(srcVmid);
      synchronized (srcVm) { // free the source to be attacked by others
        srcVm.decDynamicActionCount();
      }
      reboot(srcVm, srcActionId, reason, target);
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch (Throwable t) {
      BaseTaskScheduler.processDynamicActionError(t);
    } finally {
      synchronized (DynamicActionLock) {
        --DynamicActionCount;
      }
    }
  }

  private static void reboot(ClientVmRecord srcVm, int srcActionId,
        String reason, RebootInfo target) throws RemoteException {
    String host = target.getHost();
    log().info("Rebooting " + host + " because " + reason);

    int srcPid = srcVm.getPid(); // store for later error checking

    reboot(target);

    String s = "Rebooted " + host + " because " + reason + ". ";
    log().info( s + "Restarting" + target.toProcessList() + " on demand.");
    for (ClientInfo client : target.getClients()) {
      ClientVmInfo info = client.getClientVmInfo();
      ClientVmRecord trgVm = ClientVms.get(info.getVmid());
      synchronized (trgVm) {
        trgVm.reset();
      }
    }

    // notify the client that the reboot is complete
    log().info("Notifying " + srcVm + " with PID " + srcPid
              + " that reboot is complete");
    srcVm.notifyDynamicActionComplete(srcActionId, srcPid);
  }

  private static void reboot(RebootInfo target) throws RemoteException {
    String host = target.getHost();

    // get the hostagent kill command in nukerun for later use
    String nukeCommand = HostAgentMgr.getNukePIDCommand(host);

    // reboot the host
    RebootUtil.reboot(host);

    // restore the hostagent and wipe old one from nukerun script
    Nuker.getInstance().removePIDNoDumps(nukeCommand);
    HostAgentMgr.restartHostAgent(host);

    // handle hydra clients
    for (ClientInfo client : target.getClients()) {
      ClientVmInfo info = client.getClientVmInfo();
      ClientVmRecord vm = ClientVms.get(info.getVmid());
      synchronized (vm) {
        int pid = vm.getPid();
        if (pid != ClientVmRecord.NO_PID) {
          HostDescription hd = vm.getClientDescription().getVmDescription()
                                 .getHostDescription();
          Nuker.getInstance().removePID(hd, pid);
        } // else was killed off previously
        BaseTaskScheduler.fakeKillResults(vm);
        vm.reset();
      }
    }

    // handle hadoop processes
    for (HadoopInfo hadoop : target.getHadoops()) {
      HadoopMgr.removeHadoop(hadoop);
    }

    log().info("Host " + host + " has rebooted");
  }

  //////////////////////////////////////////////////////////////////////////////
  //                             MISC SUPPORT                                 //
  //////////////////////////////////////////////////////////////////////////////

  private static ConfigHashtable tab() {
    return TestConfig.tab();
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
