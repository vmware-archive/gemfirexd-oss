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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.internal.ManagedEntityConfigXmlGenerator;
import com.gemstone.gemfire.admin.internal.ManagedEntityConfigXmlParser;
import com.gemstone.gemfire.admin.internal.ManagedEntityControllerFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.StringIdImpl;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.ManagerLogWriter;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.org.jgroups.util.StringId;

import dunit.*;
import hydra.HostHelper;
import java.io.*;
import java.net.*;
import java.util.*;

import junit.framework.AssertionFailedError;

/**
 * Tests the functionality of the {@link AdminDistributedSystem}
 * administration API.
 *
 * @author David Whitlock
 * @author Kirk Lund
 */
public class DistributedSystemDUnitTest extends AdminDUnitTestCase {

  /**
   * Creates a new <code>DistributedSystemDUnitTest</code>
   */
  public DistributedSystemDUnitTest(String name) {
    super(name);
  }

  /**
   * Asserts that two <code>DistributedSystemConfig</code>s have
   * exactly the same configuration.
   *
   * @since 4.0
   */
  public static void assertEquals(DistributedSystemConfig config1,
                                  DistributedSystemConfig config2) {
    assertEquals(config1.getSystemId(), config2.getSystemId());
    assertEquals(config1.getSystemName(), config2.getSystemName());
    assertEquals(config1.getMcastAddress(), config2.getMcastAddress());
    assertEquals(config1.getMcastPort(), config2.getMcastPort());
    assertEquals(config1.getLocators(), config2.getLocators());
    assertEquals(config1.getRemoteCommand(),
                 config2.getRemoteCommand());
    assertEquals(config1.getDistributionLocatorConfigs(),
                 config2.getDistributionLocatorConfigs()); 
    assertEquals(config1.isSSLEnabled(), config2.isSSLEnabled());
    assertEquals(config1.getSSLProtocols(), config2.getSSLProtocols());
    assertEquals(config1.getSSLCiphers(), config2.getSSLCiphers());
    assertEquals(config1.isSSLAuthenticationRequired(),
                 config2.isSSLAuthenticationRequired()); 
    assertEquals(config1.getSSLProperties(),
                 config2.getSSLProperties());
  }

  /**
   * Asserts that two arrays of <code>DistributionLocator</code>s have
   * exactly the same configuration.
   *
   * @since 4.0
   */
  public static void assertEquals(DistributionLocatorConfig[] array1,
                                  DistributionLocatorConfig[] array2) {
    assertEquals(array1.length, array2.length);
    for (int i = 0; i < array1.length; i++) {
      assertEquals(array1[i], array2[i]);
    }
  }

  /**
   * Asserts that two <code>DistributionLocator</code>s have exactly
   * the same configuration.
   *
   * @since 4.0
   */
  public static void assertEquals(DistributionLocatorConfig l1,
                                  DistributionLocatorConfig l2) {
    assertEquals(l1.getHost(), l2.getHost());
    assertEquals(l1.getPort(), l2.getPort());
    assertEquals(l1.getBindAddress(), l2.getBindAddress());
    assertEquals(l1.getWorkingDirectory(), l2.getWorkingDirectory());
    assertEquals(l1.getProductDirectory(), l2.getProductDirectory());
    assertEquals(l1.getRemoteCommand(), l2.getRemoteCommand());
  }

  ////////  Test Methods

  public void testDefaultRemoteCommand() {
    assertNotNull(this.tcSystem);
    AdminDistributedSystem adminSystem = this.tcSystem;
    assertEquals(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND, adminSystem.getRemoteCommand());
    assertEquals("rsh -n {HOST} {CMD}", DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
  }
  
  public void testNullRemoteCommand() throws Exception {
    assertNotNull(this.tcSystem);
    final String command = null;
    AdminDistributedSystem adminSystem = this.tcSystem;
    adminSystem.setRemoteCommand(command);
    assertEquals(command, adminSystem.getRemoteCommand());
  }
  
  public void testEmptyRemoteCommand() throws Exception {
    assertNotNull(this.tcSystem);
    final String command = "";
    AdminDistributedSystem adminSystem = this.tcSystem;
    adminSystem.setRemoteCommand(command);
    assertEquals(command, adminSystem.getRemoteCommand());
  }
  
  /**
   * A simple methods that tests that we can set the remote command
   */
  public void testValidRemoteCommands() throws Exception {
    assertNotNull(this.tcSystem);
    validateRemoteCommandIsValid("rsh -n {host} {cmd}");
    validateRemoteCommandIsValid("rsh -l klund {host} {cmd}");
    validateRemoteCommandIsValid("rsh -n -d {host} {cmd}");
    validateRemoteCommandIsValid("rsh -PN {host} {cmd}");
    validateRemoteCommandIsValid("rsh -P0 {host} {cmd}");
    validateRemoteCommandIsValid("rsh -l klund -n -d {host} {cmd}");
    validateRemoteCommandIsValid("rsh -n -l klund -d {host} {cmd}");
    validateRemoteCommandIsValid("rsh -n -d -l klund {host} {cmd}");
    validateRemoteCommandIsValid("/usr/kerberos/bin/rsh -n {host} {cmd}");
    validateRemoteCommandIsValid("C:\\somepath\\rsh.exe -n {host} {cmd}");
    validateRemoteCommandIsValid("C:/somepath/rsh.exe -n {host} {cmd}");
    
    validateRemoteCommandIsValid("ssh klund@{host} {cmd}");
    validateRemoteCommandIsValid("ssh -l klund {host} {cmd}");
    validateRemoteCommandIsValid("ssh -1246AaCfgkMNnqsTtVvXxY klund@{host} {cmd}");
    validateRemoteCommandIsValid("ssh -b 127.0.0.1 klund@{host} {cmd}");
    validateRemoteCommandIsValid("ssh -b 0:0:0:0:0:0:0:1 {host} {cmd}");
    validateRemoteCommandIsValid("ssh -b ::1 klund@{host} {cmd}");
    validateRemoteCommandIsValid("ssh -D 127.0.0.1:9999 klund@{host} {cmd}");
  }
  private void validateRemoteCommandIsValid(String command) {
    AdminDistributedSystem adminSystem = this.tcSystem;
    adminSystem.setRemoteCommand(command);
    assertEquals(command, adminSystem.getRemoteCommand());
  }

  public void testInvalidRemoteCommands() throws Exception {
    assertNotNull(this.tcSystem);
    validateRemoteCommandIsInvalid("invalid");
    validateRemoteCommandIsInvalid("ssh d");
    validateRemoteCommandIsInvalid("ssh {host}");
    validateRemoteCommandIsInvalid("ssh {host} /bin/ls");
    validateRemoteCommandIsInvalid("\"C:/some path/rsh.exe\" -n {host} {cmd}");
    validateRemoteCommandIsInvalid("C:/some path/rsh.exe -n {host} {cmd}");
    validateRemoteCommandIsInvalid("rsh.bin -n {host} {cmd}");
    validateRemoteCommandIsInvalid("{host} {cmd}");
  }
  private void validateRemoteCommandIsInvalid(String command) {
    AdminDistributedSystem adminSystem = this.tcSystem;
    try {
      adminSystem.setRemoteCommand(command);
      fail("invalid remote command should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // passed
    } catch (InternalGemFireException expected) {
      Throwable cause = expected.getCause();
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      if (!(cause instanceof IllegalArgumentException)) {
        throw expected;
      }
    }
    assertFalse(command.equals(adminSystem.getRemoteCommand()));
  }
  
  protected int getSystemMemberApplicationCount() {
    int vmCount = 0;
    Host host = Host.getHost(0);
    for (int i = 0; i < host.getVMCount(); i++) {
      if (isJMX() && i == AGENT_VM) {
        // The Agent is not a member of the distributed system
        continue;
      }

      vmCount++;
    }

    if (this.isConnectedToDS()) {
      vmCount++;
    }

    return vmCount;
  }

  public void testGetSystemMemberApplications() throws Throwable {
    //disconnectAllFromDS();

    if (debug) getLogWriter().info("[finished setUp]");
    assertTrue(this.tcSystem.isConnected());
    if (!isJMX()) {
      // something about JMXRMI seems to be async so the following
      // ends up failing because if does not find any existing systems
      // in DistributedSystem.
      // So Darrel & Kirk decided to disable this part of the test if JMX
      DistributedSystemFactoryTest.checkEnableAdministrationOnly(true, true);
      DistributedSystemFactoryTest.checkEnableAdministrationOnly(false, true);
    }
    // make all VMs connect to ds...
    for (int i = 0; i < Host.getHostCount(); i++) {
      for (int j = 0; j < Host.getHost(i).getVMCount(); j++) {
        VM vm = Host.getHost(i).getVM(j);
//        final int h = i;
        final int v = j;
        vm.invoke(new SerializableRunnable("disconnect from distributed system") {
          public void run() {
            if (isJMX() && v == AGENT_VM) {
              // Don't connect in the Agent VM because it is already
              // connected
              return;
            }

            disconnectFromDS();
          }
        });
      }
    }
    for (int i = 0; i < Host.getHostCount(); i++) {
      for (int j = 0; j < Host.getHost(i).getVMCount(); j++) {
        VM vm = Host.getHost(i).getVM(j);
        final int h = i;
        final int v = j;
        vm.invoke(new SerializableRunnable("Connect to distributed system") {
          public void run() {
            if (isJMX() && v == AGENT_VM) {
              // Don't connect in the Agent VM because it is already
              // connected
              return;
            }

            Properties props = new Properties();
            String className = 
              DistributedSystemDUnitTest.this.getClass().getName();
            String name =
              className.substring(className.lastIndexOf(".")+1) +
              "_Host"+h+"_VM"+v;
            String roles = "VM"+v;
            props.setProperty(DistributionConfig.NAME_NAME, name);
            props.setProperty("roles", roles);
            getSystem(props); // this connects to ds?
            assertEquals(name, getSystem().getName());
            getCache();
          }
        });
      }
    }
    //Thread.sleep(2000);
    /* if (debug) */ getLogWriter().info("finished connecting VMs");
    
    assertFalse("We're connected to the DS?", this.isConnectedToDS());
    assertTrue(this.tcSystem.isConnected());

    int retries = 20;
    for (int attempts = 0; attempts <= retries; attempts++) {
      if (attempts > 0) {
        getLogWriter().info("attempt #" + attempts);
      }
      if (!this.tcSystem.isConnected() ||
          !this.tcSystem.isRunning()) {
        //if (debug)
        getLogWriter().info("attempt #" + attempts + ": !connected or !running");
        pause(2000);
        continue;
      }

      SystemMember[] members = this.tcSystem.getSystemMemberApplications();
      int count = getSystemMemberApplicationCount();
      if (count != members.length) {
        //if (debug)
        getLogWriter().info("attempt #" + attempts + ": appcount = " 
            + count + "; members = " + members.length);
        pause(2000);
        continue;
      }

      for (int i = 0; i < members.length; i++) {
        checkSystemMember(members[i]);
      }
      if (debug)
        getLogWriter().info("attempt #" + attempts + ": done");
    
      break; // completed test successfully
    }

    assertTrue(this.tcSystem.isConnected());
    assertTrue(this.tcSystem.isRunning());
    SystemMember[] apps = this.tcSystem.getSystemMemberApplications();
    if (getSystemMemberApplicationCount() != apps.length) {
      for (int i=0; i<apps.length; i++) {
        getLogWriter().info("app" + i + ": " + apps[i]);
      }
    }
    assertEquals(getSystemMemberApplicationCount(),
                 apps.length);

    subTestGetSystemMemberApplications();
                 
    if (debug) getLogWriter().info("[completed test]");
  }
  
  protected void subTestGetSystemMemberApplications() throws Exception {
    // override to extend testGetSystemMemberApplications
  }
  
  /**
   * Tests the lookupSystemMember operation.
   * @since 5.0
   */
  public void testLookupSystemMember() throws Throwable {
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        getSystem();
      }
    });
    DistributedMember distMember = 
        (DistributedMember) Host.getHost(0).getVM(0).invoke(
          DistributedSystemDUnitTest.class, "getDistributedMember", 
          new Object[] {});
        
    //Thread.sleep(2000);
    
    assertFalse("We're connected to the DS?", this.isConnectedToDS());

    int retries = 20;
    SystemMember member = null;
    for (int attempts = 0; attempts <= retries; attempts++) {
      if (attempts > 0) {
        getLogWriter().info("attempt #" + attempts);
      }
      if (!this.tcSystem.isConnected() ||
          !this.tcSystem.isRunning()) {
        if (debug)
          getLogWriter().info("attempt #" + attempts + ": !connected or !running");
        pause(2000);
        continue;
      }

      member = this.tcSystem.lookupSystemMember(distMember);
      if (member == null) {
        if (debug)
          getLogWriter().info("attempt #" + attempts + ": member is null");
        pause(2000);
        continue;
      }

      checkSystemMember(member);
      if (debug)
        getLogWriter().info("attempt #" + attempts + ": done");
    
      break; // completed test successfully
    }
    Assert.assertTrue(member != null, "Member " + distMember + " never appeared");

    assertTrue(this.tcSystem.isConnected());
    assertTrue(this.tcSystem.isRunning());
    assertNotNull(this.tcSystem.lookupSystemMember(distMember));
  }
  
  public void checkSystemMember(SystemMember mbr) throws Exception {
    assertEquals(SystemMemberType.APPLICATION, mbr.getType());
    assertNotNull(mbr.getId());
    assertNotNull(mbr.getName());
    assertNotNull(mbr.getHost());
    assertNotNull(mbr.getHostAddress());
    assertNotNull(mbr.getLog());
    assertNotNull(mbr.getVersion());

    try {
      ConfigurationParameter[] config = mbr.getConfiguration();
      assertTrue(config.length > 0);
    } catch (UnsupportedOperationException e) {
      getLogWriter().info("getConfiguration not supported in this test");
    }
    
    StatisticResource[] statResources = mbr.getStats();
    assertTrue(statResources.length > 0);
    
    for (int i = 0; i < statResources.length; i++) {
      StatisticResource statResource = statResources[i];
      assertNotNull(statResource.getName());
      assertNotNull(statResource.getDescription());
      assertNotNull(statResource.getType());
      assertNotNull(statResource.getOwner());
      assertNotNull(statResource.getStatistics());
      statResource.refresh();
    }
    
    //checkSystemMemberCache(mbr.getCache());
  }
  
  
  /**
   * Tests that alerts are delivered correctly. NOTE: Calls to setAlertLevel
   * results in a msg sent to other members to add/remove AlertListener. There
   * is no reply/response. Because it's asynchronous, this test requires
   * retries or sleeps to prevent false failures.
   * 13/12/2007 - added Japanese alerts, thus verifying UTF-8 support.
   */
  public void testAlerts() throws Exception {
    int[] arg = new int[] { 1, 2, 5, 10 };
    final StringId messageId = LocalizedStrings.TESTING_THIS_MESSAGE_HAS_0_MEMBERS;
    LogWriter logger = getLogWriter();
    try {
      List localeList = new ArrayList();
      localeList.add(Locale.US);
      localeList.add(Locale.JAPAN);
      for (int index = 0; index < localeList.size(); index++ ) {
        Locale testLocale = (Locale) localeList.get(index);
        logger.info("Testing Alerts with Locale = " + testLocale);
        StringIdImpl.setLocale(testLocale);
        for (int i = 0; i < arg.length; i++) {
          try {
            doTestAlerts(arg[i], messageId.toLocalizedString(1));
            break;
          }
          catch (AssertionFailedError e) {
            if (i == arg.length) {
              throw e; 
            }
          }
        }
      }
    } finally {
      //reset local to US English
      logger.finer("Test complete, Restoring locale to US English");
      StringIdImpl.setLocale(Locale.US);
    }
  }
  
  private void doTestAlerts(final int waitMultiplier, final String message) throws Exception {
    getLogWriter().info("[doTestAlerts] testing with waitMultiplier " + waitMultiplier);
    
    final String connectionName = "TestAlerts";

    if (!this.tcSystem.isConnected()) {
      reconnectToSystem();
    }
    
    // try SEVERE...

    this.tcSystem.setAlertLevel(AlertLevel.SEVERE);
    TestAlertListener listener = new TestAlertListener() {
        @Override
        public void alert2(Alert alert) {
          getLogWriter().info("DEBUG: alert=" + alert);
          assertEquals(AlertLevel.SEVERE, alert.getLevel());
          assertTrue(alert.getMessage(),
                     alert.getMessage().indexOf(message) != -1);
          assertEquals(connectionName, alert.getConnectionName());
          if (!isJMX()) {
            assertNotNull(alert.getSystemMember());
          }
          long now = System.currentTimeMillis();
          long then = alert.getDate().getTime();
          assertTrue(now + " - " + then + " >= 1000",
                     now - then < 1000);
        }
      };
      
    this.tcSystem.addAlertListener(listener);
    assertEquals(AlertLevel.SEVERE, this.tcSystem.getAlertLevel());

    // ASYNC MESSAGE!!
    // when vm(0) enters VIEW we send msg for interest in alerts to vm(0)
    VM vm = Host.getHost(0).getVM(0);
    vm.invoke(new SerializableRunnable("Reconnect to distributed system") {
        public void run() {
          disconnectFromDS();
          Properties props = new Properties();
          props.setProperty(DistributionConfig.NAME_NAME, connectionName);
          LogWriter logger = getSystem(props).getLogWriter();
          assertEquals(connectionName, getSystem().getName());
          assertEquals(connectionName,
            ((PureLogWriter) logger).getConnectionName());

          // Give other members of distributed system a chance to
          // process this new member
          pause(1000 * waitMultiplier);
           // ASYNC MESSAGE!!
          logger.severe(message);
        }
      });

    assertTrue(listener.waitForInvoked(1000 * waitMultiplier));
    
    String latest = this.tcSystem.getLatestAlert();
    assertNotNull(latest);
    assertTrue("Latest message was: \"" + latest + "\"",
               latest.indexOf(message) != -1);

    // try CONFIG which should have NO alert...
               
    vm.invoke(new SerializableRunnable("Send low-level alert") {
        public void run() {
          LogWriter logger = getSystem().getLogWriter();
          assertEquals(connectionName, getSystem().getName());
          // ASYNC MESSAGE !!
          logger.config("No alert should be issued");
        }
      });

    pause(1000 * waitMultiplier);
    assertFalse("Lowering of log level failed", listener.wasInvoked());

    latest = this.tcSystem.getLatestAlert();
    assertNotNull(latest);
    assertTrue("Latest message was: " + latest,
               latest.indexOf(message) != -1);

    // remove AlertListener as we'll create a new one
               
    this.tcSystem.removeAlertListener(listener);
    
    // verify that removed listener was removed
    vm.invoke(new SerializableRunnable("Send severe-level alert") {
        public void run() {
          LogWriter logger = getSystem().getLogWriter();
          assertEquals(connectionName, getSystem().getName());
          // ASYNC MESSAGE !!
          logger.severe(message);
        }
      });

    pause(1000 * waitMultiplier);
    assertFalse("Removal of AlertListener failed. See bug 34687.", 
        listener.wasInvoked());
    
    // try WARNING now...

    DistributedTestCase.getLogWriter().info("Changing alert level to " +
                             AlertLevel.WARNING);
    
    // ASYNC MESSAGE!!
    this.tcSystem.setAlertLevel(AlertLevel.WARNING);

    listener = new TestAlertListener() {
        @Override
        public void alert2(Alert alert) {
          assertTrue(AlertLevel.WARNING.getSeverity() <=
                     alert.getLevel().getSeverity()); 
          if (alert.getMessage().indexOf(message) == -1) {
            // We're going to get lots of messages
            return;
          }
          if (!isJMX()) {
            assertNotNull(alert.getSystemMember());
          }
          long now = System.currentTimeMillis();
          long then = alert.getDate().getTime();
          assertTrue(now + " - " + then + " >= 1000",
                     now - then < 1000);
        }
      };
      
    // ASYNC MESSAGE!!
    this.tcSystem.addAlertListener(listener);

    // adding alert listener sent asynchronous message so retry logging
    // to make sure it came in TODO: add test hook to verify remote
    // listener was added
    pause(1000 * waitMultiplier);

    vm.invoke(new SerializableRunnable("Send warning level alert") {
        public void run() {
          LogWriter logger = getSystem().getLogWriter();
          assertEquals(connectionName, getSystem().getName());
          // ASYNC MESSAGE!!
          logger.warning(message);
        }
      });

    assertTrue(listener.waitForInvoked(1000 * waitMultiplier));

    String latest2 = this.tcSystem.getLatestAlert();
    assertNotNull(latest2);
    assertTrue(!latest.equals(latest2));

    // remove AlertListener as we'll create a new one
               
    this.tcSystem.removeAlertListener(listener);
    
    // try ERROR now...

    DistributedTestCase.getLogWriter().info("Changing alert level to " +
                             AlertLevel.ERROR);
    
    // ASYNC MESSAGE!!
    this.tcSystem.setAlertLevel(AlertLevel.ERROR);

    listener = new TestAlertListener() {
        @Override
        public void alert2(Alert alert) {
          assertTrue(AlertLevel.ERROR.getSeverity() <=
                     alert.getLevel().getSeverity()); 
          if (alert.getMessage().indexOf(message) == -1) {
            // We're going to get lots of messages
            return;
          }
          if (!isJMX()) {
            assertNotNull(alert.getSystemMember());
          }
          long now = System.currentTimeMillis();
          long then = alert.getDate().getTime();
          assertTrue(now + " - " + then + " >= 1000",
                     now - then < 1000);
        }
      };
      
    this.tcSystem.addAlertListener(listener);
    
    pause(1000 * waitMultiplier);

    vm.invoke(new SerializableRunnable("Send error level alert") {
        public void run() {
          LogWriter logger = getSystem().getLogWriter();
          assertEquals(connectionName, getSystem().getName());
          int maxAttempts = 100;
          boolean found = false;
          for (int i=0; i<maxAttempts; i++) {
            found = ((ManagerLogWriter)logger).hasAlertListener();
            if (found) {
              break;
            }
            pause(100);
          }
          assertTrue("did not find an alert listener in 10 seconds, or was interrupted", found);
          // ASYNC MESSAGE!!
          logger.error(message);
        }
      });

    assertTrue(listener.waitForInvoked(1000 * waitMultiplier));

    String latest3 = this.tcSystem.getLatestAlert();
    assertNotNull(latest3);
    assertTrue(!latest.equals(latest3));
  }

  /**
   * Tests that {@link SystemMembershipEvent}s occur properly.  We
   * connect and disconnect from distributed system.  We can't really
   * test crashing.
   */
  public void testMembershipEvents() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // first disconnect the vms we will be using
    vm0.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });
    vm1.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });

    final String[] id = new String[1];

    TestSystemMembershipListener listener =
      new TestSystemMembershipListener() {
          @Override
          public void memberJoined2(SystemMembershipEvent event) {
            id[0] = event.getMemberId();
          }
        };
    if (!this.tcSystem.isConnected())
      reconnectToSystem();
    this.tcSystem.addMembershipListener(listener);

    vm0.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          getSystem();
        }
      });

    {
      int maxAttempts = 20;
      for (int i = 0; i <= maxAttempts; i++) {
        pause(50);
        try {
          assertTrue(listener.wasInvoked());
          break;
        }
        catch (junit.framework.AssertionFailedError e) {
          if (i >= maxAttempts) {
            vm0.invoke(new SerializableRunnable("Disconnect from DS") {
                public void run() {
                  disconnectFromDS();
                }
              });
            throw e;
          }
        }
      }
    }
    
    vm0.invoke(new SerializableRunnable("Check id") {
        public void run() {
          String myId = getSystem().getMemberId();
          assertEquals(id[0], myId);
        }
      });

    vm1.invoke(new SerializableRunnable("Connect to DS") {
        public void run() {
          getSystem();
        }
      });

    {
      int maxAttempts = 20;
      for (int i = 0; i <= maxAttempts; i++) {
        pause(50);
        try {
          assertTrue(listener.wasInvoked());
          break;
        }
        catch (junit.framework.AssertionFailedError e) {
          if (i >= maxAttempts) {
            throw e;
          }
        }
      }
    }
    
    vm1.invoke(new SerializableRunnable("Check id") {
        public void run() {
          String myId = getSystem().getMemberId();
          assertEquals(id[0], myId);
        }
      });

    this.tcSystem.removeMembershipListener(listener);
    
    // verify that listener was removed
    
    vm1.invoke(new SerializableRunnable("Reconnect to DS") {
        public void run() {
          disconnectFromDS();
          getSystem();
        }
      });

    pause(250);
    assertFalse("Removal of MembershipListener failed. See bug 34687.", 
                listener.wasInvoked());
    
    // ...
    
    listener =
      new TestSystemMembershipListener() {
          @Override
          public void memberLeft2(SystemMembershipEvent event) {
            id[0] = event.getMemberId();
          }
        };
    this.tcSystem.addMembershipListener(listener);

    vm0.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });

    {
      int maxAttempts = 20;
      for (int i = 0; i <= maxAttempts; i++) {
        pause(50);
        try {
          assertTrue(listener.wasInvoked());
          break;
        }
        catch (junit.framework.AssertionFailedError e) {
          if (i >= maxAttempts) {
            throw e;
          }
        }
      }
    }

    vm1.invoke(new SerializableRunnable("Disconnect from DS") {
        public void run() {
          disconnectFromDS();
        }
      });

    {
      int maxAttempts = 20;
      for (int i = 0; i <= maxAttempts; i++) {
        pause(50);
        try {
          assertTrue(listener.wasInvoked());
          break;
        }
        catch (junit.framework.AssertionFailedError e) {
          if (i >= maxAttempts) {
            throw e;
          }
        }
      }
    }
  }

  /**
   * Tests that attempting to modify a
   * <code>DistributedSystemConfig</code> after an
   * <code>AdminDistributedSystem</code> has been created throws an
   * exception. 
   *
   * @since 4.0
   */
  public void testReadOnlyDistributedSystemConfig() {
    if (isJMX()) {
      // Not interesting
      return;
    }

    if (!this.tcSystem.isConnected())
      reconnectToSystem();
    DistributedSystemConfig config = this.tcSystem.getConfig();
    try {
      config.setLocators("frank[12345]");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalStateException ex) {
      // pass...
    }

    try {
      config.setMcastPort(44444);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalStateException ex) {
      // pass...
    }

  }

  /**
   * Tests that cloning a <code>DistributedSystemConfig</code> returns
   * an equivalent object.
   */
  public void testCloneDistributedSystemConfig() throws Exception {
    if (isJMX()) {
      // Not so interesting
      return;
    }

    DistributedSystemConfig config = this.tcSystem.getConfig();
    DistributedSystemConfig config2 =
      (DistributedSystemConfig) config.clone();

    assertEquals(config, config2);
  }

  /**
   * Tests starting locators with the admin API and connecting to the
   * distributed system they form.
   *
   * @since 4.0
   */
  public void testManageDistributionLocators() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    if (HostHelper.isWindows()) {
      return;
    }

    VM vm = Host.getHost(0).getVM(0);
    String hostName = getServerHostName(Host.getHost(0));

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite(2);
    final int port1 = freeTCPPorts[0];
    final int port2 = freeTCPPorts[1];
    
    AdminDistributedSystem admin = this.tcSystem;
    admin.disconnect();
    disconnectFromDS();

    // We don't want the existing locator to be consulted
    DistributedSystemConfig config =
      AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastAddress(DistributedSystemConfig.DEFAULT_MCAST_ADDRESS);
    config.setMcastPort(0); 
    config.setLocators("");
    String bindAddress = DistributedTestCase.getIPLiteral();
    config.setBindAddress(bindAddress);
    boolean ipv6 = false;
    try {
      InetAddress addr = InetAddress.getByName(hostName);
      if (addr instanceof Inet6Address) {
        ipv6 = true;
      }
    }
    catch (UnknownHostException e) {
      getLogWriter().severe("exception looking up " + hostName, e);
    }
    if (ipv6) {
      config.setRemoteCommand("ssh -6");
    }
    else {
      config.setRemoteCommand(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
    }
    admin = AdminDistributedSystemFactory.getDistributedSystem(config);
    
    String locatorsString = hostName + "[" + port1 + "],"
      + hostName + "[" + port2 + "]";

    DistributionLocator locator1 = admin.addDistributionLocator();
    locator1.getConfig().setPort(port1);
    File dir1 = new File(this.getUniqueName() + "_locator1");
    dir1.mkdirs();
    locator1.getConfig().setHost(hostName);
    locator1.getConfig().setWorkingDirectory(dir1.getAbsolutePath());
    locator1.getConfig().getDistributedSystemProperties().setProperty("locators", locatorsString);
    locator1.getConfig().getDistributedSystemProperties().setProperty("bind-address", hostName);
    locator1.getConfig().getDistributedSystemProperties().setProperty("log-level", getDUnitLogLevel());
    getLogWriter().info("Starting Locator 1 on port " + port1);
    locator1.start();
    assertTrue("Locator1 did not start on " + locator1.getConfig().getHost(), locator1.waitToStart(60 * 1000));

    DistributionLocator locator2 = admin.addDistributionLocator();
    locator2.getConfig().setPort(port2);
    File dir2 = new File(this.getUniqueName() + "_locator2");
    dir2.mkdirs();
    locator2.getConfig().setHost(hostName);
    locator2.getConfig().setWorkingDirectory(dir2.getAbsolutePath());
    locator2.getConfig().getDistributedSystemProperties().setProperty("locators", locatorsString);
    locator2.getConfig().getDistributedSystemProperties().setProperty("bind-address", hostName);
    locator2.getConfig().getDistributedSystemProperties().setProperty("log-level", getDUnitLogLevel());
    getLogWriter().info("Starting Locator 2 on port " + port2);
    locator2.start();
    assertTrue("Locator2 did not start on " + locator2.getConfig().getHost(), locator2.waitToStart(120 * 1000));

    DistributionLocatorConfig config1 = locator1.getConfig();
    DistributionLocatorConfig config2 = locator2.getConfig();
    final String locatorString =
      config1.getHost() + "[" + config1.getPort() + "]," +
      config2.getHost() + "[" + config2.getPort() + "]";

    String vmName = getUniqueName() + "-vm";
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.NAME_NAME, vmName);
    props.setProperty("locators", locatorString);
    props.setProperty("mcast-port", "0");
    props.setProperty("log-file", vmName + ".log");
    props.setProperty("log-level", getDUnitLogLevel());

    vm.invoke(new SerializableRunnable("Connect to new DS") {
        public void run() {
          disconnectFromDS();
          com.gemstone.gemfire.distributed.DistributedSystem.connect(props);
        }
      }); 

    boolean wasDedicated = DistributionManager.isDedicatedAdminVM;
    DistributionManager.isDedicatedAdminVM = true;
    try {

    getLogWriter().info("Administering DS with locators " +
                        admin.getConfig().getLocators());
    admin.getConfig().setSystemName(this.getUniqueName() + "-admin");
    admin.getConfig().setSystemId(this.getUniqueName() + "-admin");
    admin.connect();
    assertTrue("Couldn't administer DS",
               admin.waitToBeConnected(60 * 1000));
               
    DistributionLocator[] locs = admin.getDistributionLocators();
    assertEquals(2, locs.length);

//    DistributionLocator loc1 = null;
//    DistributionLocator loc2 = null;
//    if (config1.getPort() == locs[0].getConfig().getPort())
//    }
    
    final DistributionLocator finalLoc1 = locator1;
    final DistributionLocator finalLoc2 = locator2;
    
    WaitCriterion wc1 = new WaitCriterion() {
      public boolean done() {
        return finalLoc1.isRunning();
      }
      public String description() {
        return "locator1 started";
      }
    };
    waitForCriterion(wc1, 2000, 10, true);
//    DM dm = ((AdminDistributedSystemImpl)admin).getDistributionManager();
//    assertTrue("allHostedLocators=" + dm.getAllHostedLocators() 
//        + " finalLoc1: host=" + finalLoc1.getConfig().getHost()
//        + " bind-address=" + finalLoc1.getConfig().getBindAddress()
//        + " port=" + finalLoc1.getConfig().getPort(), 
//        finalLoc1.isRunning());
    
    WaitCriterion wc2 = new WaitCriterion() {
      public boolean done() {
        return finalLoc2.isRunning();
      }
      public String description() {
        return "locator2 started";
      }
    };
    waitForCriterion(wc2, 2000, 10, true);
    
    SystemMember[] members = admin.getSystemMemberApplications();
    StringBuffer sb = new StringBuffer();
    if (members.length > 0) {
      sb.append(members.length);
      sb.append(" members: ");
      for (int i = 0; i < members.length; i++) {
        SystemMember member = members[i];
        sb.append(member.getName());
        sb.append(" (");
        sb.append(member.getId());
        sb.append(") ");
      }
    }

    if (members.length == 1) {
      assertEquals(vmName, members[0].getName());
    }

    vm.invoke(new SerializableRunnable("Disconnect from new DS") {
        public void run() {
          disconnectFromDS();
          com.gemstone.gemfire.distributed.DistributedSystem.connect(props).disconnect();
        }
      }); 

    assertLog(locator1.getLog(), 5);
    assertLog(locator2.getLog(), 5);

    locator1.stop();
    locator2.stop();
    assertTrue(locator1.waitToStop(60 * 1000));
    assertTrue(locator2.waitToStop(60 * 1000));

    assertFalse(locator1.isRunning());
    assertFalse(locator2.isRunning());
    
    admin.disconnect();
    
    // due to port reuse, we need to wait at the end of this test to avoid the
    // possibility of collision between this distributed system and the overall
    // dunit distributed system
    pause(5 * 1000);

    // Make sure to clean up before do this assertion.  Otherwise, the
    // test is put into a very bad state.
    assertEquals(sb.toString(), 1, members.length);
    }
    finally {
      DistributionManager.isDedicatedAdminVM = wasDedicated;
    }
    
    reconnectToSystem(); // put things back as they were
  }

  /**
   * Tests that an <code>AdminDistributedSystem</code> configured
   * using an XML file has the correct configuration.
   */
  public void testXmlConfig() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    final Random random = new Random();

    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem();
    AdminDistributedSystem adminSystem =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    config.setMcastPort(0);
    config.setLocators("");

    final String id = this.getUniqueName();
    config.setSystemId(id);
    
    final String remoteCommand = "ssh {HOST} {CMD}";
    config.setRemoteCommand(remoteCommand);

    final boolean sslEnabled = true;
    config.setSSLEnabled(sslEnabled);
    final String protocols = "FRED/JOE/BOB";
    config.setSSLProtocols(protocols);
    final String ciphers = "BOB/JOE/FRED";
    config.setSSLCiphers(ciphers);

    final boolean sslAuthenticationRequired = random.nextBoolean();
    config.setSSLAuthenticationRequired(sslAuthenticationRequired);

    final Properties sslProps = new Properties();
    sslProps.setProperty("ONE", "1");
    sslProps.setProperty("TWO", "2");
    config.setSSLProperties(sslProps);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = freeTCPPorts[0];
    final int port2 = freeTCPPorts[1];

    String locatorsString = getServerHostName(Host.getHost(0)) + "[" + port1 + "],"
      + getServerHostName(Host.getHost(0)) + "[" + port2 + "]";

    DistributionLocator locator1 = adminSystem.addDistributionLocator();
    locator1.getConfig().setPort(port1);
    locator1.getConfig().getDistributedSystemProperties().setProperty("locators", locatorsString);
    
    DistributionLocator locator2 = adminSystem.addDistributionLocator();
    locator2.getConfig().setPort(port2);
    locator2.getConfig().getDistributedSystemProperties().setProperty("locators", locatorsString);

//     GemFireManagerConfig manager1 =
//       system.addGemFireManager().getConfig();
//     final String host1 = "LUIS";
//     manager1.setHost(host1);

    CacheServerConfig server1 = adminSystem.addCacheServer().getConfig();
    final String classpath = "/usr/local/guillermo";
    server1.setClassPath(classpath);
    final String productDirectory = "c:\\GemFire\\stuff";
    server1.setProductDirectory(productDirectory);

    CacheServerConfig server2 = adminSystem.addCacheServer().getConfig();
    server2.setRemoteCommand(remoteCommand);
    final String workingDirectory = System.getProperty("user.dir");
    server2.setWorkingDirectory(workingDirectory);
    //server2.setHost(host1);

    // Generate the XML file
    File file = new File(this.getUniqueName() + ".xml");
    PrintWriter pw = new PrintWriter(new FileWriter(file), true);
    ManagedEntityConfigXmlGenerator.generate(adminSystem, pw);
    pw.flush();

    config = AdminDistributedSystemFactory.defineDistributedSystem();
    FileInputStream fis = new FileInputStream(file);
    ManagedEntityConfigXmlParser.parse(fis, config);

    assertEquals(id, config.getSystemId());
    assertEquals(remoteCommand, config.getRemoteCommand());

    assertEquals(sslEnabled, config.isSSLEnabled());
    assertEquals(protocols, config.getSSLProtocols());
    assertEquals(ciphers, config.getSSLCiphers());
    assertEquals(sslAuthenticationRequired,
                 config.isSSLAuthenticationRequired());
    assertEquals(sslProps, config.getSSLProperties());

    DistributionLocatorConfig[] locators =
      config.getDistributionLocatorConfigs(); 
    assertEquals(2, locators.length);
    boolean found1 = false;
    boolean found2 = false;

    for (int i = 0; i < locators.length; i++) {
      DistributionLocatorConfig locator = locators[i];
      if (locator.getPort() == port1) {
        found1= true;

      } else if (locator.getPort() == port2) {
        found2 = true;

      } else {
        fail("Locator with unknown port " + locator.getPort());
      }
    }

    assertTrue(found1);
    assertTrue(found2);

//     GemFireManagerConfig[] managers =
//       config.getGemFireManagerConfigs();
//     assertEquals(1, managers.length);

//     manager1 = managers[0];
//     assertEquals(host1, manager1.getHost());

    CacheServerConfig[] servers = config.getCacheServerConfigs();
    assertEquals(2, servers.length);

    for (int i = 0; i < servers.length; i++) {
      CacheServerConfig server = servers[i];
      if (classpath.equals(server.getClassPath())) {
        assertEquals(productDirectory, server.getProductDirectory());

      } else {
        assertEquals(remoteCommand, server.getRemoteCommand());
        assertEquals(workingDirectory, server.getWorkingDirectory());
        //assertEquals(host1, server.getHost());
      }
    }
  }

  /**
   * Tests that a distribution locator that was not created by the
   * admin API is visible using the admin API.  This is a test for the
   * fix for bug 31959.
   *
   * @since 4.0
   */
  public void testExistingLocator() throws Exception {
    AdminDistributedSystem system = this.tcSystem;

    DistributionLocator[] locators = system.getDistributionLocators();
    assertEquals(1, locators.length);

    // Note that we cannot test that locator is running because Hydra
    // does not use the normal mechanism for starting locators.
//     assertTrue(locators[0].isRunning());

    DistributionLocatorConfig config = locators[0].getConfig();

    assertTrue(config.getPort() > 0);
    
    InetAddress host2 = InetAddress.getByName(config.getHost());

    assertNotNull(config.getWorkingDirectory());

    new File(config.getProductDirectory());
  }

  /** test whether several VMs starting distributed systems concurrently find each
      other during startup. This test is disabled due to insoluable problems in
      the jgroups mcast discovery protocol.  See bug 30341 */
  public void xxtestManyNewMcastMembers() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int expectedCount = 4;
    final int cpuLoadTime = 10000;
    final long startTryingTime = System.currentTimeMillis() + 5000;
    
    SerializableRunnable consumeCPU = new SerializableRunnable("consumeCPU") { // TODO bug36296
      public void run() {
        waitForStartTime(startTryingTime);
        if (Thread.interrupted())
          return;
        long endTime = System.currentTimeMillis() + cpuLoadTime;
        while (System.currentTimeMillis() < endTime) {
          for (int i=0; i<100000; i++) {
            double u = 1.73741824 * 1.73741824;
            u = u * u * u / 3.1415926;
          }
          pause(100);
        } // while
      }
    };
    
    
    SerializableRunnable testMcast = new SerializableRunnable("manyNewMcastMembers") {
      public void run() {
        java.util.Properties env = new java.util.Properties();
        env.setProperty("mcast-port", ""+mcastPort);
        env.setProperty("locators", "");
        env.setProperty("log-level", getDUnitLogLevel());
        env.setProperty("disable-tcp", "true");
        env.setProperty("statistic-sampling-enabled","true");
        env.setProperty("statistic-archive-file","manyNewMcastmembers.gfs");
        com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager.DEBUG_JAVAGROUPS=true;
        try {
          waitForStartTime(startTryingTime);
          if (Thread.interrupted())
            return;
          DistributedSystem sys = DistributedSystem.connect(env);
          int numClients = ((InternalDistributedSystem)sys).getDistributionManager()
            .getNormalDistributionManagerIds().size();
          try {
            // need this in the log to be able to tell what VM caused the assertion failure
            if (numClients != expectedCount) {
              getLogWriter().error("Expected " + expectedCount + " members but found " + numClients);
            }
            sys.disconnect();
          }
          finally {
            assertTrue(numClients == expectedCount);
          }
        }
        finally {
          com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager.DEBUG_JAVAGROUPS=false;
        }
      }
    };
    AsyncInvocation asyncs[] = new AsyncInvocation[4];
    asyncs[0] = vm0.invokeAsync(testMcast);
    asyncs[1] = vm1.invokeAsync(testMcast);
    asyncs[2] = vm2.invokeAsync(testMcast);
    asyncs[3] = vm3.invokeAsync(testMcast);
    //asyncs[4] = vm0.invokeAsync(consumeCPU);  // starting 4 systems in hydra seems to cause enough CPU overhead
    
    long endWait = System.currentTimeMillis() + cpuLoadTime + 60000;
    while ( (System.currentTimeMillis() < endWait) && anyThreadsAlive(asyncs)) {
      pause(1000);
    }
    if (anyThreadsAlive(asyncs)) {
      throw new RuntimeException("testManyNewMcastMembers failed to wait long enough for async tasks to finish");
    }
    for (int i=0; i<asyncs.length; i++) {
      if (asyncs[i].getException() != null) {
        throw new RuntimeException("testManyNewMcastMembers failed", asyncs[i].getException());
      }
    }     
  }

  /** test whether several VMs starting distributed systems concurrently find each
      other during startup */
  public void xxxtestManyNewLocatorMembers() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    final int expectedCount = 4;
    final int cpuLoadTime = 10000;
    final long startTryingTime = System.currentTimeMillis() + 5000;
    
    SerializableRunnable consumeCPU = new SerializableRunnable("consumeCPU") { // TODO bug36296
      public void run() {
        waitForStartTime(startTryingTime);
        if (Thread.interrupted())
          return;
        long endTime = System.currentTimeMillis() + cpuLoadTime;
        while (System.currentTimeMillis() < endTime) {
          for (int i=0; i<100000; i++) {
            double u = 1.73741824 * 1.73741824;
            u = u * u * u / 3.1415926;
          }
          pause(100);
        }
      }
    };
    
    
    SerializableRunnable testLocators = new SerializableRunnable("manyNewMembers with locators") {
      public void run() {
        waitForStartTime(startTryingTime);
        if (Thread.interrupted())
          return;
        java.util.Properties env = new java.util.Properties();
        env.setProperty("log-level", getDUnitLogLevel());
        DistributedSystem sys = DistributedSystem.connect(env);
        int numClients = ((InternalDistributedSystem)sys).getDistributionManager()
          .getNormalDistributionManagerIds().size();
        try {
          sys.disconnect();
        }
        finally {
          assertTrue(numClients == expectedCount);
        }
      }
    };
    AsyncInvocation[] asyncs = new AsyncInvocation[4];
    asyncs[0] = vm0.invokeAsync(testLocators);
    asyncs[1] = vm1.invokeAsync(testLocators);
    asyncs[2] = vm2.invokeAsync(testLocators);
    asyncs[3] = vm3.invokeAsync(testLocators);
    //asyncs[4] = vm0.invokeAsync(consumeCPU);
    
    long endWait = System.currentTimeMillis() + cpuLoadTime + 60000;
    while ( (System.currentTimeMillis() < endWait) && anyThreadsAlive(asyncs)) {
      pause(1000);
    }
    if (anyThreadsAlive(asyncs)) {
      throw new RuntimeException("testManyNewLocatorMembers failed to wait long enough for async tasks to finish");
    }
    for (int i=0; i<asyncs.length; i++) {
      if (asyncs[i].getException() != null) {
        throw new RuntimeException("testManyNewLocatorMembers failed", asyncs[i].getException());
      }
    }     
  }

  /** return true if any of the threads in the given collection are alive.  It's okay
      to have null entries in the thread array */  
  private boolean anyThreadsAlive(Thread[] threads) {
    for (int i=0; i<threads.length; i++) {
      if (threads[i] != null && threads[i].isAlive()) {
        return true;
      }
    }
    return false;
  }
    
  protected void waitForStartTime(long startTime) {
    // if this assertion fails, we need to increase the startTime delay
    // to let everyone get to the starting gate
    long timeLeft = startTime - System.currentTimeMillis();
    assertTrue(timeLeft >= 0);
    for (;;) {
      timeLeft = startTime - System.currentTimeMillis();
      if (timeLeft <= 0) {
        break;
      }
      try {
        Thread.sleep(timeLeft);
      }
      catch (InterruptedException e) {
        fail("interrupted", e);
      }
    }
  }

  /////////////////////////  Inner Classes  /////////////////////////

  /**
   * An <code>AlertListener</code> used for testing.
   */
  public abstract static class TestAlertListener
    implements AlertListener {

    /** Was a callback event method invoked? */
    boolean invoked = false;
  
    protected Throwable callbackError = null;

    private final Object syncMe = new Object();
    
    /**
     * Returns wether or not one of this <code>CacheListener</code>
     * methods was invoked.  Before returning, the <code>invoked</code>
     * flag is cleared.
     */
    public boolean wasInvoked() {
      synchronized (this.syncMe) {
        checkForError();
        boolean value = this.invoked;
        this.invoked = false;
        return value;
      }
    }

    public boolean waitForInvoked(long timeout) throws InterruptedException {
      synchronized (this.syncMe) {
        StopWatch timer = new StopWatch(true);
        long timeLeft = timeout - timer.elapsedTimeMillis();
        while (!this.invoked) {
          if (timeLeft <= 0) {
            break;
          }
          this.syncMe.wait(timeLeft);
        }
        return wasInvoked();
      }
    }
    
    public final void alert(Alert alert) {
      synchronized (this.syncMe) {
        this.invoked = true;
        try {
          alert2(alert);
  
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable t) {
          this.callbackError = t;
        } finally {
          this.syncMe.notifyAll();
        }
      }
    }

    public abstract void alert2(Alert alert);

    private void checkForError() {
      synchronized (this.syncMe) {
        if (this.callbackError != null) {
          throw new Error("Exception occurred in callback", this.callbackError);
        }
      }
    }
  }

  /**
   * An <code>SystemMembershipListener</code> used for testing.
   */
  public abstract static class TestSystemMembershipListener
    implements SystemMembershipListener {

    /** Was a callback event method invoked? */
    boolean invoked = false;
  
    protected Throwable callbackError = null;

    /**
     * Returns wether or not one of this <code>CacheListener</code>
     * methods was invoked.  Before returning, the <code>invoked</code>
     * flag is cleared.
     */
    public boolean wasInvoked() {
      checkForError();
      boolean value = this.invoked;
      this.invoked = false;
      return value;
    }

    public final void memberJoined(SystemMembershipEvent event) {
      this.invoked = true;
      try {
        memberJoined2(event);

      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        this.callbackError = t;
      }
    }

    public void memberJoined2(SystemMembershipEvent event) {
      String s = "Unexpected join; member = " + event.getDistributedMember()
          + "; id = " + event.getMemberId();
      throw new UnsupportedOperationException(s);
    }

    public final void memberLeft(SystemMembershipEvent event) {
      this.invoked = true;
      try {
        memberLeft2(event);

      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        this.callbackError = t;
      }
    }

    public void memberLeft2(SystemMembershipEvent event) {
      String s = "Unexpected left: " + event;
      throw new UnsupportedOperationException(s);
    }

    public final void memberCrashed(SystemMembershipEvent event) {
      this.invoked = true;
      try {
        memberCrashed2(event);

      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        this.callbackError = t;
      }
    }

    public void memberCrashed2(SystemMembershipEvent event) {
      String s = "Unexpected crash: " + event;
      throw new UnsupportedOperationException(s);
    }

    public final void memberInfo(SystemMembershipEvent event) {
      this.invoked = true;
      try {
        memberInfo2(event);

      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        this.callbackError = t;
      }
    }

    public void memberInfo2(SystemMembershipEvent event) {
      String s = "Unexpected crash: " + event;
      throw new UnsupportedOperationException(s);
    }

    private void checkForError() {
      if (this.callbackError != null) {
        throw new Error("Exception occurred in callback", this.callbackError);
      }
    }
  }

}
