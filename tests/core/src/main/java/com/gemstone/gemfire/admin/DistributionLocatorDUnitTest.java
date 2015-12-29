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

import com.gemstone.gemfire.admin.internal.ManagedEntityControllerFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePort;
import dunit.*;
import hydra.HostHelper;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * Tests the admin API's ability to configure, start, and stop
 * distribution locators.
 *
 * @author David Whitlock
 * @since 4.0
 */ 
public class DistributionLocatorDUnitTest
  extends DistributedTestCase {

  /**
   * Creates a new <code>DistributionLocatorDUnitTest</code>
   */
  public DistributionLocatorDUnitTest(String name) {
    super(name);
  }

  /**
   * Wait for a given number of milliseconds for a
   * <code>DistributionLocator</code> to start.
   *
   * @return Whether or not the locator started
   */
  private boolean waitToStart(DistributionLocator locator,
                              long duration) {

    long start = System.currentTimeMillis();
    while (!locator.isRunning() &&
           System.currentTimeMillis() - start < duration) {
      pause(500);
    }

    return locator.isRunning();
  }

  static private final String TIMEOUT_MS_NAME 
      = "DistributionLocatorDUnitTest.TIMEOUT_MS";
  static private final int TIMEOUT_MS_DEFAULT = 60000;
  static private final int TIMEOUT_MS 
      = Integer.getInteger(TIMEOUT_MS_NAME, TIMEOUT_MS_DEFAULT).intValue();
  

  ////////  Test Methods

  /**
   * Tests that we can create, configure, and start a distribution
   * locator and then stop it.
   */
  public void testStartAndStopLocator() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    if (HostHelper.isWindows()) {
      return;
    }

    disconnectAllFromDS();

    int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File dir = new File(this.getUniqueName() + "-locator");
    dir.mkdirs();
    assertTrue(dir.exists());

    DistributedSystemConfig adminConfig =
      AdminDistributedSystemFactory.defineDistributedSystem();
    adminConfig.setMcastPort(0);
    adminConfig.setLogFile(this.getUniqueName() + ".log");
    adminConfig.setLogLevel(getDUnitLogLevel());

    getLogWriter().info("adminConfig=" + adminConfig);
    DistributionLocatorConfig locatorConfig =
      adminConfig.createDistributionLocatorConfig();
    assertEquals(1, adminConfig.getDistributionLocatorConfigs().length); 

    locatorConfig.setPort(port);
    locatorConfig.setWorkingDirectory(dir.getAbsolutePath());
    locatorConfig.setHost(getServerHostName(Host.getHost(0)));

    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(adminConfig);
    DistributionLocator[] locators = admin.getDistributionLocators();
    assertEquals(1, locators.length);
    assertFalse(locators[0].isRunning());

    admin.start();

    locators = admin.getDistributionLocators();
    assertEquals(1, locators.length);

    DistributionLocator locator = locators[0];
    assertTrue(waitToStart(locator, TIMEOUT_MS));
    pause(5 * 100);

    locator.stop();
    deleteStateFile(port);

    assertTrue(locator.waitToStop(60 * 1000));
  }

  private void deleteStateFile(int port) {
    File stateFile = new File("locator"+port+"state.dat");
    if (stateFile.exists()) {
      stateFile.delete();
    }
  }

  /**
   * Tests that we can create, configure, and start a distribution
   * locator and that another VM can connect to it.
   */
  public void testOneDistributionLocator() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    if (HostHelper.isWindows()) {
      return;
    }

    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File dir = new File(this.getUniqueName() + "-locator");
    dir.mkdirs();
    assertTrue(dir.exists());

    DistributedSystemConfig adminConfig =
      AdminDistributedSystemFactory.defineDistributedSystem();
    adminConfig.setMcastPort(0);
    adminConfig.setLogFile(this.getUniqueName() + ".log");
    adminConfig.setLogLevel(getDUnitLogLevel());

    DistributionLocatorConfig locatorConfig =
      adminConfig.createDistributionLocatorConfig();
    locatorConfig.setHost(getServerHostName(host));
    locatorConfig.setPort(port);
    locatorConfig.setWorkingDirectory(dir.getAbsolutePath());
    locatorConfig.setBindAddress(getServerHostName(host));

    getLogWriter().info("locatorConfig=" + locatorConfig);
    getLogWriter().info("adminConfig=" + adminConfig);
    
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(adminConfig);
    admin.start();

    DistributionLocator locator = admin.getDistributionLocators()[0];
    assertTrue(waitToStart(locator, 10 * 1000));
    pause(5 * 1000);
    
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");

    // construct a locator string that includes a bind-address
    String loc = adjustLocators(adminConfig.getLocators(), host);
    props.setProperty("locators", loc);
    props.setProperty("bind-address", getServerHostName(Host.getHost(0)));
   
    vm.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          DistributedSystem.connect(props);
        }
      });

    admin.connect();
    boolean connected = admin.waitToBeConnected(TIMEOUT_MS);
    //if (!connected) {
    //  ClientMgr.printProcessStacks();
    //}
    assertTrue(connected);

    assertEquals(2, admin.getSystemMemberApplications().length);

    vm.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          DistributedSystem.connect(props).disconnect();
        }
      });

    admin.disconnect();
    for (DistributedSystem ds =
           InternalDistributedSystem.getAnyInstance(); ds != null;
         ds = InternalDistributedSystem.getAnyInstance()) {
      ds.disconnect();
    }

    locator.stop();
    deleteStateFile(port);
    assertTrue(locator.waitToStop(TIMEOUT_MS));
  }


  /**
   * Tests that we can create, configure, and start a distribution
   * locator for multicast and that another VM can connect to it.
   */
  public void testOneMcastDistributionLocator() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    if (HostHelper.isWindows()
        || Boolean.getBoolean(hydra.Prms.USE_IPV6_PROPERTY) ) {
      return;
    }

    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int mcastport =
      AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    File dir = new File(this.getUniqueName() + "-locator");
    dir.mkdirs();
    assertTrue(dir.exists());

    DistributedSystemConfig adminConfig =
      AdminDistributedSystemFactory.defineDistributedSystem();
    adminConfig.setMcastPort(mcastport);
    adminConfig.setLogFile(this.getUniqueName() + ".log");
    adminConfig.setLogLevel(getDUnitLogLevel());
    getLogWriter().info("adminConfig=" + adminConfig);

    DistributionLocatorConfig locatorConfig =
      adminConfig.createDistributionLocatorConfig();
    locatorConfig.setPort(port);
    locatorConfig.setWorkingDirectory(dir.getAbsolutePath());
    java.util.Properties dsProperties = new java.util.Properties();
    dsProperties.setProperty("mcast-port", String.valueOf(mcastport));
    locatorConfig.setDistributedSystemProperties(dsProperties);

   
    AdminDistributedSystem admin = null;
    DistributionLocator locator = null;
    final Properties props = new Properties();

    try {
      admin = AdminDistributedSystemFactory.getDistributedSystem(adminConfig);
      admin.start();
      
      // note that getLocators() is only correct after the admin ds has started
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastport));
      props.setProperty(DistributionConfig.LOCATORS_NAME, adminConfig.getLocators());

      locator = admin.getDistributionLocators()[0];
      assertTrue(waitToStart(locator, TIMEOUT_MS));
      pause(5 * 1000);
      
      vm.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          DistributedSystem.connect(props);
        }
      });

      admin.connect();
      assertTrue(admin.waitToBeConnected(TIMEOUT_MS));
      
      final AdminDistributedSystem wadmin = admin;
      
      waitForCriterion(new WaitCriterion() {
        public String description() {
          return "waiting for system member applications to become 2";
        }
        public boolean done() {
          try {
            return wadmin.getSystemMemberApplications().length == 2;
          } catch (AdminException e) {
            return true;
          }
        }},
        20000, 100, false);
      
      if (admin.getSystemMemberApplications().length != 2) {
        getLogWriter().info("dump of getSystemMemberApplications:");
        for (SystemMember sm: admin.getSystemMemberApplications()) {
          getLogWriter().info("  " + sm);
        }
      }
      assertEquals(2, admin.getSystemMemberApplications().length);
      
    }
    finally {
      vm.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          DistributedSystem.connect(props).disconnect();
        }
      });

      if (admin != null) {
        admin.disconnect();
      }
      for (DistributedSystem ds =
        InternalDistributedSystem.getAnyInstance(); ds != null;
        ds = InternalDistributedSystem.getAnyInstance()) {
        ds.disconnect();
      }
    }
    locator.stop();
    deleteStateFile(port);
    assertTrue(locator.waitToStop(TIMEOUT_MS));

  }

  /**
   * Tests that we can create, configure, and start a distribution
   * locator and that another VM can connect to it.
   */
  public void testDistributionLocatorUnderDS() throws Exception {
    if (HostHelper.isWindows()) {
      return;
    }

    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    Properties props = new Properties();
    props.put("start-locator", getServerHostName(host) + "["+port+"],peer=true,server=false");
    props.put("mcast-port", "0");
    props.put("bind-address", getServerHostName(Host.getHost(0)));
    DistributedSystem ds = null;
    try {
      ds = DistributedSystem.connect(props);
      Collection locs = new HashSet(Locator.getLocators());
      Assert.assertTrue(locs.size() == 1, "Incorrect number of locators found: " + locs.size());
      final String locatorString = getServerHostName(host) + "["+port+"]";
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Properties p = new Properties();
          p.put("locators", locatorString);
          p.put("mcast-port", "0");
          DistributedSystem ds1 = null;
          try {
            ds1 = DistributedSystem.connect(p);
          }
          catch (Exception e) {
            fail("exception trying to connect distributed system", e);
          }
          if (!ds1.isConnected()) {
            fail("distributed system not connected after connect()");
          }
          try {
            ds1.disconnect();
          }
          catch (Exception e) {
            fail("unable to disconnect distributed system", e);
          }
        }
      });
      ds.disconnect();
      ServerSocket sock = null;
      try {
        sock = new ServerSocket(port);
        sock.close();
      }
      catch (Exception e) {
        fail("DistributedSystem.disconnect did not stop the hosted locator");
      }
    }
    catch (Exception e) {
      if (ds != null && ds.isConnected()) {
        try {
          ds.disconnect();
        }
        catch (Exception e2) {
          getLogWriter().severe("Unable to disconnect distributed system", e2);
        }
      }
      fail("test failed", e);
    }
    finally {
      deleteStateFile(port);
    }
  }
  
  private String adjustLocators(String loc_orig, Host host) { 
    String loc = loc_orig;
    if ( loc.indexOf(':') < 0 ) {
      int idx = loc.indexOf('[');
      String addr = null;
      try {
        final String hostname = getServerHostName(host);
        addr = InetAddress.getByName(hostname).getHostAddress();
      }
      catch (Exception e) {
        fail("Unable to get host address", e);
      }
      loc = loc.substring(0, idx) + '@' + addr + loc.substring(idx);
    }
    return loc;
  }
}
