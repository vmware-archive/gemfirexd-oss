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
package com.gemstone.gemfire.admin.jmx.internal;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.Agent;
import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.admin.jmx.AgentFactory;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * This is a DUnit test to verify binding of two different JMX Agents to two
 * Network Interfaces on the same machine while using the same RMI port for 
 * both JMX Agents.
 * 
 * <pre>
 * 1. Get available NICs. Invoke this test only if there are at least two NICs 
 *    available.
 * 2. Start agent1 & agent2 in (VM#2) & (VM#3) respectively from controller VM. 
 * 3. Use (VM#2) & (VM#3) for JMX Clients of agent1 & agent2 respectively. 
 * 4. From these JMX client VMs, check if agent can be accessed using Agent's 
 *    ObjectName 
 * 5. From these JMX client VMs,  invoke connectToSystem on agent VM. Get 
 *    reference to distributed system MBean & check if it's non null.
 * 6. Stop agent1 & agent2 from controller.
 * </pre>
 * 
 * @author abhishek
 */
//Related to enhancement mentioned in #42566
public class RMIBindSamePortDifferentNICDUnitTest extends DistributedTestCase {

  private static final long serialVersionUID = 1L;

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();
  
//  private static final String JMX_URL_FORMAT = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxconnector";
  
  private static Agent agent;
  
  private static int AGENT_VM1 = 2;
  
  
  private static int AGENT_VM2 = 3;
  
  private static int[] AGENT_VMS = new int[] {AGENT_VM1, AGENT_VM2};

  public RMIBindSamePortDifferentNICDUnitTest(String name) {
    super(name);
  }
  
  public void testNICs() {
    List<InetAddress> nicIPs = getNetworkInterfacesIPs();
    
    if (nicIPs.size() < 2) {
      logWriter.info("This test needs 2 reachable network interfaces. " +
      		"SKIPPING this test further as no. of available NICs are :: " + 
      		nicIPs.size());
      return;
    }

    int reachableIPs = 0;
    for (InetAddress ip : nicIPs) {
      try {
        if (ip.isReachable(1000)) {
          reachableIPs++;
        }
      } catch (IOException ioe) {
        continue;
      }
    }

    logWriter.info("No. of interfaces: (Reachable/Total) :: (" + 
                                 reachableIPs + "/" + nicIPs.size() + ")");
    if (reachableIPs < 2) {
      logWriter.info("This test needs 2 reachable network interfaces. " +
      		"SKIPPING this test further as all are not reachable.");
      return;
    }
    
    logWriter.info("Available NIC IPs :: "+nicIPs);
    
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    
    InetAddress ip = null;
    Host host = Host.getHost(0);
    
    logWriter.fine("Starting agents .... ");
    for (int i = 0; i < AGENT_VMS.length; i++) {
      ip = nicIPs.get(i);
      VM vm = host.getVM(AGENT_VMS[i]);
      
      vm.invoke(RMIBindSamePortDifferentNICDUnitTest.class, 
                "startAgent", 
                new Object[] {ip, port, vm.getPid()});
    }
    
    pause(5 * 1000);
    
    for (int i = 0; i < AGENT_VMS.length; i++) {
      VM vm = host.getVM(AGENT_VMS[i]);
      ip = nicIPs.get(i);
      
      vm.invoke(RMIBindSamePortDifferentNICDUnitTest.class, 
                "connectJMXClient");
    }
    
    pause(1 * 1000);
    
    logWriter.fine("Stopping agents .... ");
    for (int i = 0; i < AGENT_VMS.length; i++) {
      VM vm = host.getVM(AGENT_VMS[i]);
      
      vm.invoke(RMIBindSamePortDifferentNICDUnitTest.class, "stopAgent");
    }
  }
  
  private static String getThisTestName() {
    return RMIBindSamePortDifferentNICDUnitTest.class.getSimpleName();
  } 
  
  public static void startAgent(InetAddress rmiBindAddress, int port, int pid) throws AdminException {
    AgentConfig agentConfig = AgentFactory.defineAgent();
    agentConfig.setRmiBindAddress(rmiBindAddress.getHostAddress());
    agentConfig.setRmiPort(port);
    agentConfig.setHttpEnabled(false);
    agentConfig.setMcastPort(AvailablePortHelper.getRandomAvailableTCPPort());
    agentConfig.setLogFile(getThisTestName()+"_"+pid+"_jmxagent.log");
    agentConfig.setLogLevel(getDUnitLogLevel());
    
    // set state save file name 
    agentConfig.setStateSaveFile(getThisTestName()+"_"+pid+"_agent.ser");
    
    agent = AgentFactory.getAgent(agentConfig);
    agent.start();
    
    logWriter.info("Started agent started at :: "+rmiBindAddress+":"+port);
  }
  
  public static void stopAgent() throws AdminException {
    if (agent != null && agent.getMBeanServer() != null) {
      agent.stop();
    }
    DistributionManager.isDedicatedAdminVM = false;
    logWriter.info("Agent stopped.");
  }
  
  public static void connectJMXClient() throws Exception {
    if (agent != null) {
      MBeanServer mbsc = agent.getMBeanServer();
      assertNotNull("Couldn't get MBeanServerConnection from the Agent: "+agent, mbsc);
      
      
      String agentMBeanName = "GemFire:type=Agent";
      ObjectName agentObjName = new ObjectName(agentMBeanName);
      
      assertTrue("Couldn't find Agent MBean for Agent :: "+agent, mbsc.isRegistered(agentObjName));
      logWriter.fine("Sucessfully connected to Agent :: "+agent);
      
      Object[] params    = new Object[0];
      String[] signature = new String[0];
      
      ObjectName adminDSObjName = (ObjectName) mbsc.invoke(agentObjName, "connectToSystem", params, signature);
      
      assertTrue("Couldn't invoke connectToSystem on Agent : "+agent, adminDSObjName != null);
      logWriter.info("Sucessfully invoked connectToSystem on Agent :: "+agent);
    }
  }
  
  private static List<InetAddress> getNetworkInterfacesIPs() {
    List<InetAddress> nicIPs  = new ArrayList<InetAddress>(); 
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface nic = networkInterfaces.nextElement();
        
        Enumeration<InetAddress> interfaceIPs = nic.getInetAddresses();
        while (interfaceIPs.hasMoreElements()) {
          InetAddress interfaceIP = (InetAddress) interfaceIPs.nextElement();
          //Considering Inet4Address only
          if (interfaceIP instanceof Inet4Address) {
            nicIPs.add(interfaceIP);
          }
        }
      }
    } catch (SocketException e) {
      logWriter.info("Couldn't retrieve info about available NetworkInterfaces", e);
    }
    
    return nicIPs;
  }
  
  private void cleanUp() {
    Host host = Host.getHost(0);
    for (int i = 0; i < AGENT_VMS.length; i++) {
      VM vm = host.getVM(AGENT_VMS[i]);
      
      try {
        vm.invoke(RMIBindSamePortDifferentNICDUnitTest.class, "stopAgent");
      } catch (Exception e) {
        logWriter.info("Exception while calling stopAgent in (VM#"+AGENT_VMS[i]+")", e);
      }
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }
  
  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
      disconnectAllFromDS();
    } finally {
      cleanUp();
    }
  }
}
