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
import java.util.*;

public interface MasterProxyIF extends Remote {

   //// test configuration

   public TestConfig getTestConfig() throws RemoteException;
   public TestConfig getHostAgentTestConfig() throws RemoteException;

   //// gemfirelocatoragent management

   /**
    *  For internal hydra use only.
    */
   public void registerGemFireLocatorAgent( GemFireLocatorAgentRecord lar )
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
  public void recordLocator(DistributedSystemHelper.Endpoint endpoint)
  throws RemoteException;

   /**
    *  Workaround for bug 30341.
    */
   public void lockToSynchronizeJavaGroupsForBug30341()
   throws RemoteException;
   public void unlockToSynchronizeJavaGroupsForBug30341()
   throws RemoteException;

   //// derby server management

   /**
    * For internal hydra use only.
    */
   public DerbyServerHelper.Endpoint getDerbyServerEndpoint()
   throws RemoteException;

   //// hostagent management

   /**
    *  For internal hydra use only.
    */
   public void registerHostAgent( HostAgentRecord har )
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public HostAgentIF getHostAgent(String host)
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public void reportStatMonitorError( Throwable t )
   throws RemoteException;

   //// client management

  /**
   * Called remotely by client processes to get the VM ID corresponding to host
   * and PID.
   * <p>
   * If host is an IPv6 host name, hydra attempts to match it to the IPv4 host
   * name known by hydra.  This only succeeds if the IPv6 host name is the
   * same as the IPv4 host name with the suffix "6".  If you are running on an
   * IPv6 host for which this is not the case, ask I.S. to configure the host
   * so that it is.
   *
   * @return the VM ID if it exists else null.
   */
  public Integer getVmid(String host, int pid)
  throws RemoteException;

  /**
   * Called remotely by client processes to get the client mapping as a list
   * of lists, where each sublist is the threadgroup plus the thread name.
   */
  public List<List<String>> getClientMapping()
  throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public int getPid( int vmid )
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public HydraThreadGroupInfo registerClient( int vmid, int tid,
                                               RemoteTestModuleIF mod )
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public void registerClientDisconnect(int vmid)
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public Map getClientVms()
   throws RemoteException;

   public void printClientProcessStacks()
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public Object getGlobalNext( Long key )
   throws RemoteException;

   /**
    * For internal hydra use only.
    */
   public boolean reservePort(String host, int port)
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public ClientVmInfo reserveClientVmForStop( String srcName, int srcVmid,
                                               int syncMode, int stopMode,
                                               ClientVmInfo target )
   throws RemoteException, ClientVmNotFoundException;

   /**
    *  For internal hydra use only.
    */
   public ClientVmInfo reserveClientVmForStart( String srcName, int srcVmid,
                                                ClientVmInfo target )
   throws RemoteException, ClientVmNotFoundException;

   /**
    *  For internal hydra use only.
    */
   public void stopClientVm( String srcName, int srcVmid, int actionId,
                             String reason, int syncMode,
                             int stopMode, int startMode,
                             ClientVmInfo target )
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public void startClientVm( String srcName, int srcVmid, int actionId,
                              String reason, int syncMode,
                              ClientVmInfo target )
   throws RemoteException;

   /**
    * For internal hydra use only.
    */
   public ClientVmRecord bounceUnitTestVm(int pid)
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public Vector getClientVmids()
   throws RemoteException;

   /**
    *  For internal hydra use only.
    */
   public Vector getOtherClientVmids(int vmid)
   throws RemoteException;

  /**
   * For internal hydra use only.
   */
  public RebootInfo reserveForReboot(String srcName, int srcVmid, String target,
                                     boolean liveOnly)
  throws RemoteException, RebootHostNotFoundException;

  /**
   * For internal hydra use only.
   */
  public void reboot(String srcName, int srcVmid, int actionId,
                     String reason, RebootInfo target)
  throws RemoteException;

  /**
   * For internal hydra use only.
   */
  public void configureNetworkConnection(String source, String target,
                                         int op, int way)
  throws RemoteException;

  /**
   * For internal hydra use only.
   */
  public Map getNetworkConnectionState()
  throws RemoteException;

  /**
   * For internal hydra use only.
   */
  public void showNetworkConnectionState()
  throws RemoteException;

  //// jmx agents

  public void recordAgent(AgentHelper.Endpoint endpoint)
  throws RemoteException;

   //// nuke script management

   public void recordPID(HostDescription hd, int pid )
   throws RemoteException;
   public void recordPIDNoDumps(HostDescription hd, int pid)
   throws RemoteException;
   public void removePID(HostDescription hd, int pid )
   throws RemoteException;
   public void removePIDNoDumps(HostDescription hd, int pid)
   throws RemoteException;

   public void recordHadoop(HadoopDescription hdd, NodeDescription nd, int pid, boolean secure)
   throws RemoteException;
   public void removeHadoop(HadoopDescription hdd, NodeDescription nd, int pid, boolean secure)
   throws RemoteException;

   public void recordHDFSPIDNoDumps(HostDescription hd, int pid, boolean secure)
   throws RemoteException;
   public void removeHDFSPIDNoDumps(HostDescription hd, int pid, boolean secure)
   throws RemoteException;

   //// move script management
   public void recordDir(HostDescription hd, String name, String dir)
   throws RemoteException;

   public void recordRootCommand(HostDescription hd, String cmd, String dir)
   throws RemoteException;

   //// results

   /**
    *  For internal hydra use only.
    */
   public void reportResult( int tsid, int vmid, int tid,
                             TestTaskResult result )
   throws RemoteException;

   //// method invocation

   public void invoke( String classname, String methodname, Class[] types, Object[] args )
   throws RemoteException;
}
