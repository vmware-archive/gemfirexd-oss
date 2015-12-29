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
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.internal.AgentImpl;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.*;
import hydra.*;

import java.io.File;
import java.util.*;

import javax.management.ObjectName;

/**
 * A helper class that contains methods for performing various
 * JMX-related operations.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class JMXHelper {
  public static Agent startAgent(AdminDUnitTestCase test, int pid)
    throws Exception {
    DistributionConfig dsConfig = test.getDistributionConfig();
    AgentConfig config = AgentFactory.defineAgent();
    //set auto-connect to false. See #40390
    config.setAutoConnect(false);
    return startAgent(test, config, dsConfig, pid);
  }

  public static Agent startAgent(AdminDUnitTestCase test, AgentConfig config,
                               int pid)
    throws Exception {
    DistributionConfig dsConfig = test.getDistributionConfig();
    return startAgent(test, config, dsConfig, pid);
  }

  private static Agent doAgent(AgentConfig config) throws AdminException {
    System.out.println("AgentConfig.LOG_FILE_NAME=" +
        config.getLogFile());
    System.out.println("AgentConfig.LOG_LEVEL_NAME=" +
        config.getLogLevel()); 

    Agent agent = null;
    try {
      agent = AgentFactory.getAgent(config);
      agent.start();
    }
    catch (Throwable e) {
      e.printStackTrace();
    }
    return agent;
  } 
  
  /**
   * Creates a new JMX agent in this VM
   */
  public static Agent startAgent(AdminDUnitTestCase test, AgentConfig config,
                               DistributionConfig dsConfig, int pid)
    throws Exception {
    DistributedTestCase.disconnectFromDS();
    DistributionManager.isDedicatedAdminVM = true;
    boolean done = false;
    try {
    // Distributed system
    config.setMcastPort(dsConfig.getMcastPort());
    config.setMcastAddress(dsConfig.getMcastAddress().getHostName());
    config.setLocators(dsConfig.getLocators());
    config.setBindAddress(DistributedTestCase.getIPLiteral());

    // HttpAdaptor...
    int httpPort =
      AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    config.setHttpPort(httpPort);

    // don't need the RMI adaptor...
    config.setRmiEnabled(false);
    
    // set logging options for agent...
    String testClassName = test.getClass().getName();
    String testName = testClassName.substring(testClassName.lastIndexOf(".") + 1);
    config.setLogFile(testName+"_"+pid+"_jmxagent.log");
    config.setLogLevel(DistributedTestCase.getDUnitLogLevel());
    
    // set state save file name 
    config.setStateSaveFile(testName+"_"+pid+"_agent.ser");

    System.out.println("AgentConfig.LOG_FILE_NAME=" +
                       config.getLogFile());
    System.out.println("AgentConfig.LOG_LEVEL_NAME=" +
                       config.getLogLevel()); 
    
    Agent agent = AgentFactory.getAgent(config);
    agent.start();
    done = true;
    return agent;
    } finally {
      if (!done) {
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }
    
  /**
   * Stops the given JMX agent
   */
  public static void stopAgent(Agent agent) throws Exception {
    try {
    Log.getLogWriter().info("[JMXHelper] Stopping JMX Agent: " + agent);

    if (agent != null) {
      ObjectName agentName = new ObjectName("GemFire:type=Agent");
      agent.getMBeanServer().invoke(agentName, "stop",
                                    new Object[0], new String[0]);
    }

    String agentPropsFile = System.getProperty("gfAgentPropertyFile");
    System.setProperty("gfAgentPropertyFile", AgentConfig.DEFAULT_PROPERTY_FILE);
    if (agentPropsFile != null) {
      new File(agentPropsFile).delete();
    }

    Assert.assertTrue((agent != null && !agent.isConnected()));
    } finally {
      DistributionManager.isDedicatedAdminVM = false;
    }
  }

  /** The RMI agent running in this VM */
  protected static Agent rmiAgent;

  /**
   * Starts a new RMI JMX agent that runs in the given VM.
   *
   * @return The URL for accessing the newly-started RMI agent
   */
  public static String startRMIAgent(final dunit.DistributedTestCase test,
                                     VM vm)
    throws Exception {

    Integer pid = Integer.valueOf(vm.getPid());
    return (String) vm.invoke(JMXHelper.class, "startRMIAgent",
                              new Object[] { test, pid });
  }

  /**
   * Starts a new RMI JMX agent (that is not marked as "admin only")
   * that runs in the given VM.
   *
   * @return The URL for accessing the newly-started RMI agent
   */
  public static String startRMIAgentNoAdmin(final dunit.DistributedTestCase test,
                                     VM vm)
    throws Exception {

    Integer pid = Integer.valueOf(vm.getPid());
    return (String) vm.invoke(JMXHelper.class, "startRMIAgentNoAdmin",
                              new Object[] { test, pid });
  }

  /*
   * Starts a new  SSL-secured RMI JMX agent that runs in the given VM.
   *
   * @return The URL for accessing the newly-started RMI agent
   */
  public static String startRMISSLAgent(final dunit.DistributedTestCase test,
                                        VM vm)
    throws Exception {

    Integer pid = Integer.valueOf(vm.getPid());
    return (String) vm.invoke(JMXHelper.class, "startRMISSLAgent",
                              new Object[] { test, pid });
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @param test
   * @param pid
   * @return
   * @throws Exception
   */
  protected static String startRMIAgent(final AdminDUnitTestCase test, 
                                      int pid)
    throws Exception {

    DistributionConfig dsConfig = test.getDistributionConfig();
    DistributedTestCase.disconnectFromDS();
    DistributionManager.isDedicatedAdminVM = true;
    boolean done = false;
    try {
      String result = startRMIAgent(dsConfig, test, pid);
      done = true;
      return result;
    } finally {
      if (!done) {
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * 
   * Starts a JMX agent in this VM, but doesn't mark it as "admin only"
   */
  protected static String startRMIAgentNoAdmin(final CoLocatedAgentDUnitTest test, 
                                            int pid)
    throws Exception {

    DistributionConfig dsConfig = test.getDistributionConfig();

    String xmlFile = test.getEntityConfigXMLFile();
    if (xmlFile != null) {
      // Not sure why we have to do this, but it works
      DistributedTestCase.disconnectFromDS();
    }
    return startRMIAgent(dsConfig, test, pid, xmlFile);
  }


  /**
   * Starts a JMX Agent with the HTTP adapter enabled in this VM.
   */
  static String startRMIAgent(DistributionConfig dsConfig,
                              dunit.DistributedTestCase test, 
                              int pid) 
    throws Exception {

    return startRMIAgent(dsConfig, test, pid, null);
  }

  /**
   * Starts a JMX Agent with the HTTP adapter enabled in this VM.
   */
  static String startRMIAgent(DistributionConfig dsConfig,
                              dunit.DistributedTestCase test, 
                              int pid,
                              String xmlFile) 
    throws Exception {

    // Must use IPv4 hostname here for now, because RMI cannot handle IPv6 
    // literals and not all platforms/JRE combinations can correctly translate 
    // to a hostname given our current network setup
    String hostName = TestConfig.getInstance().getMasterDescription().
      getVmDescription().getHostDescription().getHostName();
    AgentConfig config = AgentFactory.defineAgent();
    
    //set auto-connect to false. See #40390
    config.setAutoConnect(false);
    
    if (xmlFile != null) {
      System.out.println("JMX XML config file: " + xmlFile);
      config.setEntityConfigXMLFile(xmlFile);
    }

    // Distributed system
    config.setMcastPort(dsConfig.getMcastPort());
    config.setMcastAddress(dsConfig.getMcastAddress().getHostName());
    config.setLocators(dsConfig.getLocators());
    config.setBindAddress(DistributedTestCase.getIPLiteral());

    // JMX connector creates its own registry.  
    int rmiRegistryPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    /*
     * Setting RMI Server Port. Default is 0 i.e. the port will get allocated 
     * dynamically. With using multiple dunitsites, there could be simultaneous 
     * requests to open RMI Server Socket using anonymous port 0, which might 
     * result in BindExcepption for port in use for port '0'.
     */
    int rmiServerPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    // JMX RMI adaptor...
    config.setRmiEnabled(true);
    config.setRmiRegistryEnabled(true);
    config.setRmiBindAddress(hostName);
    config.setRmiPort(rmiRegistryPort);
    config.setRmiServerPort(rmiServerPort);
    
    // don't need the HTTP adaptor...
    config.setHttpEnabled(false);

    // set logging options for agent...
    String testClassName = test.getClass().getName();
    String testName = testClassName.substring(testClassName.lastIndexOf(".") + 1);
    config.setLogFile(testName+"_"+pid+"_jmxagent.log");
    config.setLogLevel(DistributedTestCase.getDUnitLogLevel());
    
    // set state save file name 
    config.setStateSaveFile(testName+"_"+pid+"_agent.ser");

    System.out.println("AgentConfig.LOG_FILE_NAME=" +
                       config.getLogFile()); 
    System.out.println("AgentConfig.LOG_LEVEL_NAME=" +
                       config.getLogLevel());
    
    rmiAgent = AgentFactory.getAgent(config);
    rmiAgent.start();
    Assert.assertTrue(((AgentImpl) rmiAgent).getRMIAddress() != null);
    return ((AgentImpl) rmiAgent).getRMIAddress().toString();
  }

  /**
   * Connects to the JMX RMI agent in the given VM and stops it.
   */
  public static void stopRMIAgent(final dunit.DistributedTestCase test,
                                   VM vm)
    throws Exception {

    vm.invoke(new SerializableRunnable("Stop RMI Agent") {
        public void run() {
          try {
            stopAgent(rmiAgent);
            rmiAgent = null;

          } catch (Exception ex) {
            DistributedTestCase.fail("While stopping RMI agent", ex);
          }
        }
      });
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * 
   * @param test
   * @param pid
   * @return
   * @throws Exception
   */
  protected static String startRMISSLAgent(final AdminDUnitTestCase test, 
                                         final int pid)
    throws Exception {

    DistributionConfig dsConfig = test.getDistributionConfig();
    DistributedTestCase.disconnectFromDS();
    DistributionManager.isDedicatedAdminVM = true;
    boolean done = false;
    try {

    String hostName = DistributedTestCase.getIPLiteral(); 

    AgentConfig config = AgentFactory.defineAgent();
    
    //set auto-connect to false. See #40390
    config.setAutoConnect(false);

    // Distributed system
    config.setMcastPort(dsConfig.getMcastPort());
    config.setMcastAddress(dsConfig.getMcastAddress().getHostName());
    config.setLocators(dsConfig.getLocators());

    // JMX connector creates its own registry.  
    int rmiRegistryPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    /*
     * Setting RMI Server Port. Default is 0 i.e. the port will get allocated 
     * dynamically. With using multiple dunitsites, there could be simultaneous 
     * requests to open RMI Server Socket using anonymous port 0, which might 
     * result in BindExcepption for port in use for port '0'.
     */
    int rmiServerPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();

    // JMX RMI adaptor...
    config.setRmiEnabled(true);
    config.setRmiRegistryEnabled(false);
    config.setRmiBindAddress(hostName);
    config.setRmiPort(rmiRegistryPort);
    config.setRmiServerPort(rmiServerPort);

    // don't need the HTTP adaptor...
    config.setHttpEnabled(false);
    
    // JMX SSL props...
    config.setAgentSSLEnabled(true);

    // set the SSL System properties...
    String JTESTS = System.getProperty("JTESTS");
    String keyStore = 
        JTESTS + File.separator + "ssl" + File.separator + "trusted.keystore";
    
    System.setProperty("javax.net.ssl.keyStore", keyStore);
    System.setProperty("javax.net.ssl.keyStorePassword", "password");
    System.setProperty("javax.net.ssl.trustStore", keyStore);
    System.setProperty("javax.net.ssl.trustStorePassword", "password");
    
    // set logging options for agent...
    String testClassName = test.getClass().getName();
    String testName = testClassName.substring(testClassName.lastIndexOf(".") + 1);
    config.setLogFile(testName + "_" + pid + "_jmxagent.log");
    config.setLogLevel(DistributedTestCase.getDUnitLogLevel());
    
    // set state save file name 
    config.setStateSaveFile(testName+"_"+pid+"_agent.ser");

    System.out.println("AgentConfig.LOG_FILE_NAME=" +
                       config.getLogFile());
    System.out.println("AgentConfig.LOG_LEVEL_NAME=" +
                       config.getLogLevel()); 
    
    rmiAgent = AgentFactory.getAgent(config);
    rmiAgent.start();
    Assert.assertTrue(((AgentImpl) rmiAgent).getRMIAddress() != null);
    String result = ((AgentImpl) rmiAgent).getRMIAddress().toString();
    done = true;
    return result;
    } finally {
      if (!done) {
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }

}
