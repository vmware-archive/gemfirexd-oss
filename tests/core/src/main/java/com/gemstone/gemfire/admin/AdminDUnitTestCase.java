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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.jmx.*;
import com.gemstone.gemfire.admin.jmx.internal.*;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogFileParser;
import com.gemstone.gemfire.internal.LogWriterImpl;
import dunit.*;

import javax.management.*;
import javax.management.remote.*;
import java.io.*;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

/**
 * Abstract superclass of tests that exercise the functionality of the
 * GemFire external administration API.  It is a subclass of
 * <code>CacheTestCase</code> because it is easiest to test the
 * administration of cache-related components.
 *
 * <P>
 *
 * Admin tests that test JMX behavior should override methods like
 * {@link #isJMX} and {@link #isRMI}.
 *
 * @author David Whitlock
 * @since 3.5
 */
public abstract class AdminDUnitTestCase extends CacheTestCase {

  /** The index of the VM in which the JMX Agent is hosted */
  protected static final int AGENT_VM = 3;

  /** The <code>DistributedSystem</code> admin object created for this
   * test */ 
  protected transient AdminDistributedSystem tcSystem;

  protected transient final boolean debug = false;
  //protected transient final boolean debug = true;
  
  /** A shutdown hook that is used for debugging */
  protected static final Thread shutdownThread = new ShutdownThread();
    
  /** The JMX agent optionally started by this test */
  protected transient Agent agent = null;
  
  /** The URL on which the RMI agent runs */
  protected String urlString;
  
  public static final int NUM_RETRY = 10;
  
  public static final int RETRY_INTERVAL = 100;

  ////////  Constructors

  /**
   * Creates a new <code>AdminDUnitTestCase</code>
   */
  public AdminDUnitTestCase(String name) {
    super(name);
  }

  ////////  Test lifecycle methods

  public void reconnectToSystem() /* throws Exception */ {
    try {
      DistributionConfig config = super.getSystem().getConfig();
      assertFalse(config.getMcastPort() ==
                  DistributionConfig.DEFAULT_MCAST_PORT);

      // Disconnect from the distributed system in this VM
      disconnectAllFromDS();

      if(this.tcSystem instanceof JMXAdminDistributedSystem) {
        getLogWriter().info("[disconnectRMIConnection]");
       ((JMXAdminDistributedSystem)this.tcSystem).closeRmiConnection(); 
      }


      String locators = getLocators(config);
      //if (locators == null || !"".equals(locators)) {
      if (locators != null && locators.length() > 0) {
        // use locators...
        this.tcSystem = createDistributedSystem(
                                                DistributedSystemConfig.DEFAULT_MCAST_ADDRESS, 
                                                0, 
                                                locators, 
                                                DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
      } 
      else {
        // use mcast...
        String address = hydra.HostHelper.getHostAddress(
                           config.getMcastAddress());
        int port = config.getMcastPort();
        this.tcSystem = createDistributedSystem(
                                                address, 
                                                port, 
                                                DistributedSystemConfig.DEFAULT_LOCATORS, 
                                                DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
      }
    }
    catch (Exception e) {
      fail("unexpected reconnect failure", e);
    }
  }
  
  protected String getLocators(DistributionConfig config) {
    return config.getLocators();
  }
  
  private static boolean hookRegistered = false;
  
  /**
   * In addition to the setup done by the superclass, also creates a
   * {@link DistributedSystem} admin object.
   */
  public void setUp() throws Exception {
    getLogWriter().info("Entering AdminDUnitTestCase#setUp");
    boolean setUpFailed = true;
    try {
      if (isJMX()) {
        if (!hookRegistered) {
          Runtime.getRuntime().addShutdownHook(shutdownThread);
          hookRegistered = true;
        }
        startAgent();
      }

      DistributionManager.isDedicatedAdminVM = true;

      // When is this necessary?
//      DistributionConfig config = super.getSystem().getConfig();
//      assertFalse(config.getMcastPort() ==
//                  DistributionConfig.DEFAULT_MCAST_PORT);
//      // Disconnect from the distributed system in this VM
      disconnectAllFromDS();
      
      if((this.tcSystem != null) && (this.tcSystem.isConnected())) {
       disconnect(); 
      }

      super.setUp();
      reconnectToSystem();
      if (isJMX()) {
        assertAgent();
      }
      
      // Success!
      setUpFailed = false;
    }
    finally {
      if (setUpFailed) {
        try {
          disconnectAllFromDS();
        }
        finally {
          DistributionManager.isDedicatedAdminVM = false;
        }
      }
    }
  }
  
  /**
   * Destroys the {@link DistributedSystem} created in {@link #setUp}
   * and performs the tear down inherited from the superclass.
   */
  public void tearDown2() throws Exception {

    // Note we only get here when explicitly called from caseTearDown
    try {
      super.tearDown2(); // This only removes regions and cache.  Is it necessary?

      if (this.isJMX()) {
        getLogWriter().info("[tearDown]");
        disconnect();
        stopAgent();
        getLogWriter().info("[calling super.tearDown...]");
      }
  
      if (this.tcSystem != null) {
        this.tcSystem.disconnect();
        this.tcSystem = null;
      }

      if (this.isJMX()) {
        if (hookRegistered) {
          Runtime.getRuntime().removeShutdownHook(shutdownThread);
          hookRegistered = false;
        }
      }
    }
    finally {
      try {
        disconnectAllFromDS();
      }
      finally {
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }
  
  public synchronized AdminDistributedSystem getAdminDistributedSystem() throws Exception {
    if (this.tcSystem == null) {
      setUp();
    }
    return this.tcSystem;
  }

  /**
   * Returns DistributedMember instance if connected to the dist system.
   * @since 5.0
   */
  protected static DistributedMember getDistributedMember() {
    DistributedSystem system = InternalDistributedSystem.getAnyInstance();
    return system.getDistributedMember();
  }
    
  /**
   * Returns the distribution configuration for this test.  This is
   * needed primarily for starting the JMX agent.
   *
   * @since 4.0
   */
  public DistributionConfig getDistributionConfig() {
    return this.getSystem().getConfig();    
  }

  /**
   * Starts the JMX Agent
   */
  protected void startAgent() throws Exception {
    if (this.isRMI()) {
      if (this.isSSL()) {
        // /export/shared_build/users/klund/gemfire/tests/ssl/trusted.keystore
        String JTESTS = System.getProperty("JTESTS");
        String keyStore = 
          JTESTS + File.separator + "ssl" + File.separator +
          "trusted.keystore";
    
        System.setProperty("javax.net.ssl.keyStore", keyStore);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", keyStore);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        
        this.urlString =
          JMXHelper.startRMISSLAgent(this, Host.getHost(0).getVM(AGENT_VM));
        assertNotNull(urlString);

      } else {
        this.urlString =
          JMXHelper.startRMIAgent(this, Host.getHost(0).getVM(AGENT_VM));
        assertNotNull(urlString);
      }

    } else {
      int pid = hydra.ProcessMgr.getProcessId();
      this.agent = JMXHelper.startAgent(this, pid);
    }
  }
  
  protected void disconnect() {
    getLogWriter().info("[disconnect]");
    try {
      if (this.tcSystem != null) {
        this.tcSystem.disconnect();
        
        if(this.tcSystem instanceof JMXAdminDistributedSystem) {
		  getLogWriter().info("[disconnectRMIConnection]");
          JMXAdminDistributedSystem sys = (JMXAdminDistributedSystem)this.tcSystem;
          sys.closeRmiConnection();
        }
        
        this.tcSystem = null;
      }
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      getLogWriter().info("Failed to disconnect", t);
    }
  }
  
  protected void stopAgent() throws Exception {
    if (isRMI()) {
      JMXHelper.stopRMIAgent(this, Host.getHost(0).getVM(AGENT_VM));

    } else {
      getLogWriter().info("[stopAgent]");
      try {
        JMXHelper.stopAgent(this.agent);
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        getLogWriter().info("Failed to stop agent", t);
      }
    }
  }

  protected void assertAgent() throws Exception {
    if (this.isRMI()) {
      if (this.isSSL()) {
        assertNotNull(this.urlString);
        JMXConnector conn = createJMXConnector();
        MBeanServerConnection mbs = conn.getMBeanServerConnection();
        assertNotNull(mbs);
    
        ObjectName agent = new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
        assertTrue(mbs.isRegistered(agent));
        
        Boolean connected = (Boolean) 
          mbs.getAttribute(agent, "connected");
        assertTrue(connected.booleanValue());
        
        ObjectName system = (ObjectName) 
          mbs.invoke(agent, "manageDistributedSystem", 
                     new Object[0], new String[0]);
        assertTrue(mbs.isRegistered(system));
        
        conn.close();
        

      } else {
        assertNotNull(this.urlString);
        JMXServiceURL url = new JMXServiceURL(this.urlString);
        JMXConnector conn = JMXConnectorFactory.connect(url);
        MBeanServerConnection mbs = conn.getMBeanServerConnection();
        assertNotNull(mbs);
      
        ObjectName agent = new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
        assertTrue(mbs.isRegistered(agent));
        
        Boolean connected = (Boolean) 
          mbs.getAttribute(agent, "connected");
        assertTrue(connected.booleanValue());
        
        ObjectName system = (ObjectName) 
          mbs.invoke(agent, "manageDistributedSystem", 
                     new Object[0], new String[0]);
        assertTrue(mbs.isRegistered(system));
        
        conn.close();
      }
    } else {
      assertNotNull(this.agent);
      assertTrue(this.agent.isConnected());
      MBeanServer mbs = this.agent.getMBeanServer();
      assertNotNull(mbs);
      ObjectName sys = this.agent.manageDistributedSystem();
      assertNotNull(sys);
      assertTrue(mbs.isRegistered(sys));
    }
  }

  private JMXConnector createJMXConnector() throws Exception {
    JMXServiceURL url = new JMXServiceURL(this.urlString);
    RMIServerSocketFactory ssf = new MX4JServerSocketFactory(
        true, true, "any", "any", new LocalLogWriter(LogWriterImpl.FINE_LEVEL), new Properties());
    RMIClientSocketFactory csf = new SslRMIClientSocketFactory();
    
    Map env = new HashMap();
    env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);
    env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, csf);
      
    return JMXConnectorFactory.connect(url, env);
  }

  /**
   * Creates a new <code>DistributedSystem</code> with the given
   * configuration.  Note that the <code>DistributedSystem</code> will
   * log to DUnit's (Hydra's) log file.  The actual type of
   * <code>DistributedSystem</code> that is returned depends on
   * whether or not this test tests {@link #isJMX JMX} behavior.
   */
  protected AdminDistributedSystem
    createDistributedSystem(String mcastAddress,
                            int mcastPort,
                            String locators,
                            String remoteCommand)
    throws AdminException {

    String bindAddress = null;
    if (System.getProperty("gemfire.bind-address") != null) {
      bindAddress = System.getProperty("gemfire.bind-address"); // ipv6 testing
    }
    
    if (this.isJMX()) {
      if (this.isRMI()) {
        if (this.isSSL()) {
          MBeanServerConnection mbs;
          ObjectName objectName;
          try {
            JMXConnector conn = createJMXConnector();
            mbs = conn.getMBeanServerConnection();
            objectName = new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
            return new JMXAdminDistributedSystem(mcastAddress, mcastPort, locators,
                bindAddress,
                remoteCommand, mbs, conn,  objectName);
          } 
          catch (Exception ex) {
            String s = "While connecting to JMX RMI agent";
            throw new AdminException(s, ex);
          }


        } else {
          MBeanServerConnection mbs;
          ObjectName objectName;
          try {
            JMXServiceURL url = new JMXServiceURL(this.urlString);
            JMXConnector conn = JMXConnectorFactory.connect(url);
            mbs = conn.getMBeanServerConnection();
            objectName = new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
            return new JMXAdminDistributedSystem(mcastAddress, mcastPort, locators,
                bindAddress,
                remoteCommand, mbs, conn,  objectName);
            
          } catch (Exception ex) {
            String s = "While connecting to JMX RMI agent on " +
              this.urlString;
            throw new AdminException(s, ex);
          }
        }

      } else {
        assertNotNull(this.agent);
        return new JMXAdminDistributedSystem(mcastAddress, mcastPort, locators,
                                        bindAddress,
                                        remoteCommand,
                                        this.agent.getMBeanServer(), null,
                                        this.agent.getObjectName());
      }

    } else {
      DistributedSystemConfig config =
        AdminDistributedSystemFactory.defineDistributedSystem();
      config.setMcastAddress(mcastAddress);
      config.setMcastPort(mcastPort); 
      config.setLocators(locators);
      config.setRemoteCommand(remoteCommand);
      config.setBindAddress(bindAddress);
      config.setServerBindAddress(bindAddress);

      final AdminDistributedSystem system =
        AdminDistributedSystemFactory.getDistributedSystem(config);
      ((AdminDistributedSystemImpl) system).connect(DistributedTestCase.getLogWriter().convertToLogWriterI18n());
      this.tcSystem = system;

      // Wait for the DistributedSystem's connection to the distributed
      // system to be initialized.
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return system.isConnected();
        }
        public String description() {
          return "system never connected:" + system;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 20 * 1000, 200, true);
      return system;
    }
  }

  /**
   * Returns whether or not this test exercises JMX behavior.  Unless
   * overridden, this method returns <code>false</code>.
   */
  protected boolean isJMX() {
    return false;
  }

  /**
   * Returns whether or not this test exercises JMX RMI behavior.
   * Unless overridden, this method returns <code>false</code>.
   */
  protected boolean isRMI() {
    return false;
  }

  /**
   * Returns whether or not this test exercises JMX RMI SSL behavior.
   * Unless overridden, this method returns <code>false</code>.
   */
  protected boolean isSSL() {
    return false;
  }

  /**
   * Asserts that a log has at least the given number of entries.
   *
   * @since 4.0
   */
  protected static void assertLog(String log, int minEntries) 
    throws IOException {

    StringReader sr = new StringReader(log);
    BufferedReader br = new BufferedReader(sr);

    LogFileParser parser = new LogFileParser(null, br);
    int entries = 0;
    while (parser.hasMoreEntries()) {
      parser.getNextEntry();
      entries++;
    }

    if (entries < minEntries) {
      String s = "Log file " + log + " must contain at least " + minEntries +
        " entries.  It only contains " + entries + " entries.\n\n";
      s += log;
      fail(s);
    }
  }

  ////////////////////////  Inner Classes  ////////////////////////

  /**
   * A shutdown hook that is used for debugging
   */
  static class ShutdownThread extends Thread {
    public void run() {
      System.err.println("VM IS SHUTTING DOWN but hasn't done tearDown yet!!");
    }
  }
}
