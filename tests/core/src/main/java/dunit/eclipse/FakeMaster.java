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
package dunit.eclipse;

import hydra.ClientVmInfo;
import hydra.ClientVmNotFoundException;
import hydra.ClientVmRecord;
import hydra.GemFireLocatorAgentRecord;
import hydra.HadoopDescription;
import hydra.HadoopDescription.NodeDescription;
import hydra.HostAgentIF;
import hydra.HostAgentRecord;
import hydra.HostDescription;
import hydra.HydraThreadGroupInfo;
import hydra.MasterProxyIF;
import hydra.RebootHostNotFoundException;
import hydra.RebootInfo;
import hydra.RemoteTestModuleIF;
import hydra.TestTaskResult;
import hydra.TestConfig;
import hydra.AgentHelper.Endpoint;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class FakeMaster implements MasterProxyIF {

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#bounceUnitTestVm(String,int)
   */
  public ClientVmRecord bounceUnitTestVm(int pid) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#configureNetworkConnection(java.lang.String, java.lang.String, int, int)
   */
  public void configureNetworkConnection(String source, String target,
      int op, int way) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getClientVmids()
   */
  public Vector getClientVmids() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getClientVms()
   */
  public Map getClientVms() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getClientMapping()
   */
  public List<List<String>> getClientMapping() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getGlobalNext(java.lang.Long)
   */
  public Object getGlobalNext(Long key) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reservePort(java.lang.String,int)
   */
  public boolean reservePort(String host, int port) throws RemoteException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getJMXAgentRmiPort(java.lang.String)
   */
  public int getJMXAgentRmiPort(String name) throws RemoteException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getMulticastPort()
   */
  public int getMulticastPort() throws RemoteException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getNetworkConnectionState()
   */
  public Map getNetworkConnectionState() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getOtherClientVmids(int)
   */
  public Vector getOtherClientVmids(int vmid) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reserveForReboot(String,int,String,boolean)
   */
  public RebootInfo reserveForReboot(String srcName, int srcVmid, String target,
                                     boolean liveOnly)
  throws RemoteException, RebootHostNotFoundException {
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reboot(String,int,int,String,RebootInfo)
   */
  public void reboot(String srcName, int srcVmid, int actionId,
                     String reason, RebootInfo target)
  throws RemoteException {
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getPid(int)
   */
  public int getPid(int vmid) throws RemoteException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getTestConfig()
   */
  public TestConfig getTestConfig() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getHostAgentTestConfig()
   */
  public TestConfig getHostAgentTestConfig() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getVmid(String,int)
   */
  public Integer getVmid(String host, int pid) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#invoke(java.lang.String, java.lang.String, java.lang.Class[], java.lang.Object[])
   */
  public void invoke(String classname, String methodname, Class[] types,
      Object[] args) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#lockToSynchronizeJavaGroupsForBug30341()
   */
  public void lockToSynchronizeJavaGroupsForBug30341() throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#printClientProcessStacks()
   */
  public void printClientProcessStacks() throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordDir(hydra.HostDescription, java.lang.String, java.lang.String)
   */
  public void recordDir(HostDescription hd, String name, String dir)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordPID(hydra.HostDescription, int)
   */
  public void recordPID(HostDescription hd, int pid)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#registerClient(int, int, hydra.RemoteTestModuleIF)
   */
  public HydraThreadGroupInfo registerClient(int vmid, int tid,
      RemoteTestModuleIF mod) throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#registerClientDisconnect(int)
   */
  public void registerClientDisconnect(int vmid)
  throws RemoteException {
    // TODO Auto-generated method stub
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#registerGemFireLocatorAgent(hydra.GemFireLocatorAgentRecord)
   */
  public void registerGemFireLocatorAgent(GemFireLocatorAgentRecord lar)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#registerHostAgent(hydra.HostAgentRecord)
   */
  public void registerHostAgent(HostAgentRecord har) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getHostAgent(String)
   */
  public HostAgentIF getHostAgent(String host) throws RemoteException {
    // TODO Auto-generated method stub
    return null; 
  }


  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#removeDir(hydra.HostDescription, java.lang.String)
   */
  public void removeDir(HostDescription hd, String dir)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#removePID(hydra.HostDescription, int)
   */
  public void removePID(HostDescription hd, int pid)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordHadoop(hydra.HadoopDescription, NodeDescription, int, boolean)
   */
   public void recordHadoop(HadoopDescription hdd, NodeDescription nd, int pid, boolean secure)
   throws RemoteException {
   }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#removeHadoop(hydra.HadoopDescription, NodeDescription, int, boolean)
   */
   public void removeHadoop(HadoopDescription hdd, NodeDescription nd, int pid, boolean secure)
   throws RemoteException {
   }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reportResult(int, int, int, hydra.TestTaskResult)
   */
  public void reportResult(int tsid, int vmid, int tid, TestTaskResult result)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reportStatMonitorError(java.lang.Throwable)
   */
  public void reportStatMonitorError(Throwable t) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reserveClientVmForStart(java.lang.String, int, hydra.ClientVmInfo)
   */
  public ClientVmInfo reserveClientVmForStart(String srcName, int srcVmid,
      ClientVmInfo target) throws RemoteException, ClientVmNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#reserveClientVmForStop(java.lang.String, int, int, int, hydra.ClientVmInfo)
   */
  public ClientVmInfo reserveClientVmForStop(String srcName, int srcVmid,
      int syncMode, int stopMode, ClientVmInfo target)
      throws RemoteException, ClientVmNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#showNetworkConnectionState()
   */
  public void showNetworkConnectionState() throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#startClientVm(java.lang.String, int, int, java.lang.String, int, hydra.ClientVmInfo)
   */
  public void startClientVm(String srcName, int srcVmid, int actionId,
      String reason, int syncMode, ClientVmInfo target)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#stopClientVm(java.lang.String, int, int, java.lang.String, int, int, int, hydra.ClientVmInfo)
   */
  public void stopClientVm(String srcName, int srcVmid, int actionId,
      String reason, int syncMode, int stopMode, int startMode,
      ClientVmInfo target) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#unlockToSynchronizeJavaGroupsForBug30341()
   */
  public void unlockToSynchronizeJavaGroupsForBug30341()
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  public void recordAgent(Endpoint endpoint) throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  public void recordLocator(hydra.DistributedSystemHelper.Endpoint endpoint)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#getDerbyServerEndpoint()
   */
  public hydra.DerbyServerHelper.Endpoint getDerbyServerEndpoint()
      throws RemoteException {
    return null;
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordPIDNoDumps(hydra.HostDescription, java.lang.String, int)
   */
  public void recordPIDNoDumps(HostDescription hd, int pid)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordHDFSPIDNoDumps(hydra.HostDescription, java.lang.String, int, boolean)
   */
  public void recordHDFSPIDNoDumps(HostDescription hd, int pid, boolean secure)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#removePIDNoDumps(hydra.HostDescription, java.lang.String, int)
   */
  public void removePIDNoDumps(hydra.HostDescription hd, int pid)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }
  
  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#removeHDFSPIDNoDumps(hydra.HostDescription, java.lang.String, int, boolean)
   */
  public void removeHDFSPIDNoDumps(hydra.HostDescription hd, int pid, boolean secure)
      throws RemoteException {
    // TODO Auto-generated method stub
    
  }
  
  
  /* (non-Javadoc)
   * @see hydra.MasterProxyIF#recordRootCommand(hydra.HostDescription, java.lang.String, java.lang.String)
   */
   public void recordRootCommand(HostDescription hd, String cmd, String dir)
       throws RemoteException {
    // TODO Auto-generated method stub

  }
  
  
}
