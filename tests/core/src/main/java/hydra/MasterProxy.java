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

import hydra.HadoopDescription.NodeDescription;
import java.rmi.*;
import java.rmi.server.*;
import java.util.*;

/**
*
* Implements the remote interface for client access to the master.
*
*/

public class MasterProxy extends UnicastRemoteObject implements MasterProxyIF
{
  public MasterProxy() throws RemoteException {
    super();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TEST CONFIGURATION
  //////////////////////////////////////////////////////////////////////////////

  public TestConfig getTestConfig() throws RemoteException {
    return TestConfig.getInstanceFromMaster();
  }

  public TestConfig getHostAgentTestConfig() throws RemoteException {
    return TestConfig.getHostAgentInstanceFromMaster();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    LOCATOR AGENTS                                                    ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by the locatoragent to register itself with the master.
   */
  public synchronized void registerGemFireLocatorAgent( GemFireLocatorAgentRecord lar )
  throws RemoteException {
    GemFireLocatorAgentMgr.registerGemFireLocatorAgent( lar );
  }

  public synchronized void recordLocator(DistributedSystemHelper.Endpoint endpoint)
  throws RemoteException {
    endpoint.record();
  }

  /**
   *  Start workaround for Bug 30341.
   */
  public void lockToSynchronizeJavaGroupsForBug30341()
  throws RemoteException {
    boolean gotLock = false;
    while ( ! gotLock ) {
      synchronized( MasterProxy.class ) {
      if ( TheLock == 0 ) {
          TheLock = 1;
          gotLock = true;
            Log.getLogWriter().info( "Locked the javagroups lock" );
        }
      }
      MasterController.sleepForMs( 250 );
    }
  }
  public void unlockToSynchronizeJavaGroupsForBug30341()
  throws RemoteException {
    MasterController.sleepForMs( 5000 );
    Log.getLogWriter().info( "Unlocking the javagroups lock" );
    TheLock = 0;
  }
  private static int TheLock = 0;
  /**
   *  End workaround for Bug 30341.
   */

  //////////////////////////////////////////////////////////////////////////////
  ////    DERBY SERVER                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * For internal hydra use only.
   * <p>
   * Called remotely by hydra clients to get the derby server endpoint.
   */
  public DerbyServerHelper.Endpoint getDerbyServerEndpoint()
  throws RemoteException {
    return DerbyServerMgr.getEndpoint();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    HOST AGENTS                                                       ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by hostagents to register themselves with the master.
   */
  public synchronized void registerHostAgent( HostAgentRecord har )
  throws RemoteException {
    HostAgentMgr.registerHostAgent( har );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by hostagents to report a statistics monitor error.
   */
  public synchronized void reportStatMonitorError( Throwable t )
  throws RemoteException {
    BaseTaskScheduler.setStatMonitorError( t );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called by ProcessMgr to get a hostagent for a host.
   */
  public synchronized HostAgentIF getHostAgent(String host)
  throws RemoteException {
    return HostAgentMgr.getRemoteHostAgentForClient(host);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CLIENTS                                                           ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Called remotely by client processes to get VM ID corresponding to host
   * and PID.
   * @return the VM ID if it exists else null.
   */
  public Integer getVmid(String host, int pid)
  throws RemoteException {
    return ClientMgr.getVmid(host, pid);
  }

  /**
   * Called remotely by client processes to get the client mapping as a list
   * of lists, where each sublist is the threadgroup plus the thread name.
   */
  public List<List<String>> getClientMapping()
  throws RemoteException {
    return ClientMgr.getClientMapping();
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by client processes to get accurate PID (for Linux).
   */
  public int getPid( int vmid )
  throws RemoteException {
    return ClientMgr.getPid( vmid );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by client threads to register themselves with the master.
   */
  public HydraThreadGroupInfo registerClient( int vmid, int tid,
                                              RemoteTestModuleIF mod )
  throws RemoteException {
    return ClientMgr.registerClient( vmid, tid, mod );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by client threads to register disconnect with the master.
   */
  public void registerClientDisconnect(int vmid)
  throws RemoteException {
    ClientMgr.registerClientDisconnect(vmid);
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Called remotely by the unit test controller.
   */
  public Map getClientVms()
  throws RemoteException {
    return ClientMgr.getClientVms();
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Used by client threads to report task results to master.
   */
  public void reportResult( int tsid, int vmid, int tid,
                            TestTaskResult result )
  throws RemoteException {
    BaseTaskScheduler.reportResult( tsid, vmid, tid, result );
  }

  public void printClientProcessStacks()
  throws RemoteException {
    ClientMgr.printProcessStacks();
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Used by client threads to get the global next from a {@link hydra.RobinG}.
   */
  public Object getGlobalNext( Long key )
  throws RemoteException {
    RobinG r = (RobinG) TestConfig.tab().get( key );
    if ( r == null ) {
      throw new HydraConfigException( "No RobinG at this key" );
    }
    return r.next( key );
  }

  /**
   * For internal hydra use only.
   * <p>
   * Used by client threads to reserve a port with the master, who adds it to
   * the list of used ports for the host.
   * @return whether the port was successfully reserved
   */
  public boolean reservePort(String host, int port)
  throws RemoteException {
    return PortHelper.reservePort(host, port);
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Reserves a client VM matching the specified info for future stop.
   *  Returns null if no match is found.
   *  @param srcName Thread name for the invoking client.
   *  @param srcVmid Logical VM id for the invoking client.
   *  @param syncMode Synchronization mode for the dynamic stop.
   *  @param stopMode Stop mode for the dynamic stop.
   *  @param target Information used to select the target client VM.
   *  @return Fully specified information for the selected VM.
   *  @throws ClientVmNotFoundException if a reservation could not be made.
   *  @throws IllegalArgumentException if the target info is invalid.
   */
  public ClientVmInfo reserveClientVmForStop( String srcName, int srcVmid,
                                              int syncMode, int stopMode,
                                              ClientVmInfo target )
  throws RemoteException, ClientVmNotFoundException {
    return ClientMgr.reserveClientVmForStop( srcName, srcVmid,
                                             syncMode, stopMode,
                                             target );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Reserves a client VM matching the specified info for future start.
   *  Returns null if no match is found.
   *  @param srcName Thread name for the invoking client.
   *  @param srcVmid Logical VM id for the invoking client.
   *  @param target Information used to select the target client VM.
   *  @return Fully specified information for the selected VM.
   *  @throws ClientVmNotFoundException if a reservation could not be made.
   *  @throws IllegalArgumentException if the target info is invalid.
   */
  public ClientVmInfo reserveClientVmForStart( String srcName, int srcVmid,
                                               ClientVmInfo target )
  throws RemoteException, ClientVmNotFoundException {
    return ClientMgr.reserveClientVmForStart( srcName, srcVmid, target );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Stops the target VM.
   */
  public void stopClientVm( String srcName, int srcVmid, int actionId,
                            String reason, int syncMode,
                            int stopMode, int startMode,
                            ClientVmInfo target )
  throws RemoteException {
    ClientMgr.stopClientVm( srcName, srcVmid, actionId,
                            reason, syncMode, stopMode, startMode, target );
  }

  /**
   *  For internal hydra use only.
   *  <p>
   *  Starts the target VM.
   */
  public void startClientVm( String srcName, int srcVmid, int actionId,
                             String reason, int syncMode,
                             ClientVmInfo target )
  throws RemoteException {
    ClientMgr.startClientVm( srcName, srcVmid, actionId,
                             reason, syncMode, target );
  }

  /**
   * For internal hydra use only.
   * <p>
   * Bounces the VM with the given PID.
   */
  public ClientVmRecord bounceUnitTestVm(int pid)
  throws RemoteException {
    return ClientMgr.bounceUnitTestVm(pid);
  }

  /**
   * For internal hydra use only.
   * <p>
   * Returns the VM IDs (as Integers) for all client VMs, regardless of whether
   * the VMs are currently started or stopped.
   */
  public Vector getClientVmids()
  throws RemoteException {
    return ClientMgr.getClientVmids();
  }

  /**
   * For internal hydra use only.
   * <p>
   * Returns the VM IDs (as Integers) for all client VMs except the given one,
   * regardless of whether the VMs are currently started or stopped.
   */
  public Vector getOtherClientVmids(int vmid)
  throws RemoteException {
    return ClientMgr.getOtherClientVmids(vmid);
  }

  /**
   * For internal hydra use only.
   * <p>
   * Reserves a host for future reboot.
   * @param srcName Thread name for the invoking client.
   * @param srcVmid Logical VM id for the invoking client.
   * @param target The target host.
   * @param liveOnly Whether to return only processes that were live at the
   *                 time of the reboot.
   * @return Information about the host and the processes running on it.
   * @throws RebootHostNotFoundException if a reservation could not be made.
   * @throws IllegalArgumentException if the target is invalid.
   */
  public RebootInfo reserveForReboot(String srcName, int srcVmid, String target,
    boolean liveOnly) throws RemoteException, RebootHostNotFoundException {
    return ClientMgr.reserveForReboot(srcName, srcVmid, target, liveOnly);
  }

  /**
   * For internal hydra use only.
   * <p>
   * Reboots the target.
   */
  public void reboot(String srcName, int srcVmid, int actionId,
                     String reason, RebootInfo target)
  throws RemoteException {
    ClientMgr.reboot(srcName, srcVmid, actionId, reason, target);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    NETWORK                                                           ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * For internal hydra use only.
   */
  public void configureNetworkConnection(String source, String target,
                                         int op, int way)
  throws RemoteException {
    NetworkHelper.configure(source, target, op, way);
  }

  /**
   * For internal hydra use only.
   */
  public Map getNetworkConnectionState()
  throws RemoteException {
    return NetworkHelper.getState();
  }

  /**
   * For internal hydra use only.
   */
  public void showNetworkConnectionState()
  throws RemoteException {
    NetworkHelper.showState();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    JMX AGENTS                                                        ////
  //////////////////////////////////////////////////////////////////////////////

  public synchronized void recordAgent(AgentHelper.Endpoint endpoint)
  throws RemoteException {
    endpoint.record();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    NUKING                                                            ////
  //////////////////////////////////////////////////////////////////////////////

   public void recordPID(HostDescription hd, int pid )
   throws RemoteException {
     Nuker.getInstance().recordPID(hd, pid );
   }
   public void recordPIDNoDumps(HostDescription hd, int pid)
   throws RemoteException {
     Nuker.getInstance().recordPIDNoDumps(hd, pid);
   }
   public void removePID(HostDescription hd, int pid )
   throws RemoteException {
     Nuker.getInstance().removePID(hd, pid);
   }
   public void removePIDNoDumps(HostDescription hd, int pid)
   throws RemoteException {
     Nuker.getInstance().removePIDNoDumps(hd, pid);
   }

   public void recordHadoop(HadoopDescription hdd, NodeDescription nd,
                            int pid, boolean secure)
   throws RemoteException {
     HadoopMgr.recordHadoop(hdd, nd, pid, secure);
   }
   public void removeHadoop(HadoopDescription hdd, NodeDescription nd,
                            int pid, boolean secure)
   throws RemoteException {
     HadoopMgr.removeHadoop(hdd, nd, pid, secure);
   }

   public void recordHDFSPIDNoDumps(HostDescription hd, int pid, boolean secure)
   throws RemoteException {
     Nuker.getInstance().recordHDFSPIDNoDumps(hd, pid, secure);
   }
   public void removeHDFSPIDNoDumps(HostDescription hd, int pid, boolean secure)
   throws RemoteException {
     Nuker.getInstance().removeHDFSPIDNoDumps(hd, pid, secure);
   }

   public void recordRootCommand(HostDescription hd, String cmd, String dir)
   throws RemoteException {
     Nuker.getInstance().recordRootCommand(hd, cmd, dir);
   }

  //////////////////////////////////////////////////////////////////////////////
  ////    DIR MOVING                                                        ////
  //////////////////////////////////////////////////////////////////////////////

   public void recordDir(HostDescription hd, String name, String dir)
   throws RemoteException {
     Nuker.getInstance().recordDir(hd, name, dir);
   }

  //////////////////////////////////////////////////////////////////////////////
  ////    METHOD INVOCATION                                                 ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Invokes the specified class method.  Intended for use by client threads.
   *  The invocation is synchronized across clients, so this is suitable for
   *  operations such as lazily creating shared blackboards. 
   */
  public void invoke( String classname, String methodname,
                      Class[] types, Object[] args )
  throws RemoteException {
    MasterController.invoke( classname, methodname, types, args );
  }
}
