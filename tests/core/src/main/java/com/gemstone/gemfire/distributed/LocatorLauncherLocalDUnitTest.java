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
package com.gemstone.gemfire.distributed;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.process.FileAlreadyExistsException;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;

import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Tests usage of LocatorLauncher as a local API in existing JVM.
 *
 * @author Kirk Lund
 * @since 8.0
 */
@SuppressWarnings("serial")
public class LocatorLauncherLocalDUnitTest extends AbstractLocatorLauncherDUnitTestCase {

  public LocatorLauncherLocalDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void subSetUp1() throws Exception {
    disconnectAllFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
    subSetUp2();
  }

  @Override
  protected final void subTearDown1() throws Exception {    
    disconnectAllFromDS();
    System.clearProperty(ProcessType.TEST_PREFIX_PROPERTY);
    subTearDown2();
  }
  
  /**
   * To be overridden in subclass.
   */
  protected void subSetUp2() throws Exception {
  }
  
  /**
   * To be overridden in subclass.
   */
  protected void subTearDown2() throws Exception {    
  }
  
  protected Status getExpectedStopStatusForNotRunning() {
    return Status.NOT_RESPONDING;
  }
  
  public void testBuilderSetProperties() throws Throwable {
    // collect and throw the FIRST failure
    Throwable failure = null;

    this.launcher = new Builder()
        .setForce(true)
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .set(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "true")
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0")
        .build();

    assertNotNull(this.launcher);
    
    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());
      waitForLocatorToStart(this.launcher, true);
  
      final InternalLocator locator = this.launcher.getLocator();
      assertNotNull(locator);
  
      final DistributedSystem distributedSystem = locator.getDistributedSystem();
  
      assertNotNull(distributedSystem);
      assertEquals("true", distributedSystem.getProperties().getProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME));
      assertEquals("0", distributedSystem.getProperties().getProperty(DistributionConfig.MCAST_PORT_NAME));
      //assertEquals("0", distributedSystem.getProperties().getProperty(DistributionConfig.LOCATORS_NAME));
      assertEquals("config", distributedSystem.getProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME));
      assertEquals(getUniqueName(), distributedSystem.getProperties().getProperty(DistributionConfig.NAME_NAME));

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getLocator());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testBuilderSetProperties

  public void testGetAllHostedLocators() throws Exception {
    final InternalDistributedSystem system = getSystem();
    final String dunitLocator = system.getConfig().getLocators();
    assertNotNull(dunitLocator);
    assertFalse(dunitLocator.isEmpty());

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    
    for (int i = 0 ; i < 4; i++) {
      final int whichvm = i;
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          try {
            System.setProperty("gemfire.locators", dunitLocator);
            System.setProperty("gemfire.mcast-port", "0");
            
            final String name = getUniqueName() + "-" + whichvm;
            final File subdir = new File(name);
            subdir.mkdir();
            assertTrue(subdir.exists() && subdir.isDirectory());
            
            final Builder builder = new Builder()
                .setMemberName(name)
                .setPort(ports[whichvm])
                .setRedirectOutput(true)
                .setWorkingDirectory(name);
    
            launcher = builder.build();
            assertEquals(Status.ONLINE, launcher.start().getStatus());
            waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10, true);
          } finally {
            System.clearProperty("gemfire.locators");
            System.clearProperty("gemfire.mcast-port");
          }
        }
      });
    }
    
    final String host = SocketCreator.getLocalHost().getHostAddress();
    
    final Set<String> locators = new HashSet<String>();
    locators.add(host + "[" + dunitLocator.substring(dunitLocator.indexOf("[")+1, dunitLocator.indexOf("]")) + "]");
    for (int port : ports) {
      locators.add(host +"[" + port + "]");
    }

    // validation within non-locator
    final DistributionManager dm = (DistributionManager)system.getDistributionManager();
    
    final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
    assertEquals(5, locatorIds.size());
    
    final Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
    assertTrue(!hostedLocators.isEmpty());
    assertEquals(5, hostedLocators.size());
    
    for (InternalDistributedMember member : hostedLocators.keySet()) {
      assertEquals(1, hostedLocators.get(member).size());
      final String hostedLocator = hostedLocators.get(member).iterator().next();
      assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
    }

    // validate fix for #46324
    for (int whichvm = 0 ; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager)InternalDistributedSystem.getAnyInstance().getDistributionManager();
          final InternalDistributedMember self = dm.getDistributionManagerId();
          
          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertTrue(locatorIds.contains(self));
          
          final Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
          assertTrue("hit bug #46324: " + hostedLocators + " is missing " + InternalLocator.getLocatorStrings() + " for " + self, hostedLocators.containsKey(self));
        }
      });
    }
    
    // validation with locators
    for (int whichvm = 0 ; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager)InternalDistributedSystem.getAnyInstance().getDistributionManager();
          
          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertEquals(5, locatorIds.size());
          
          final Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
          assertTrue(!hostedLocators.isEmpty());
          assertEquals(5, hostedLocators.size());
          
          for (InternalDistributedMember member : hostedLocators.keySet()) {
            assertEquals(1, hostedLocators.get(member).size());
            final String hostedLocator = hostedLocators.get(member).iterator().next();
            assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
          }
        }
      });
    }
  } // testGetAllHostedLocators

  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertTrue(factory.isAttachAPIFound());

    invokeInEveryVM(new SerializableRunnable("isAttachAPIFound") {
      @Override
      public void run() {
        final ProcessControllerFactory factory = new ProcessControllerFactory();
        assertTrue(factory.isAttachAPIFound());
      }
    });
  } // testIsAttachAPIFound
  
  public void testStartCreatesPidFile() throws Throwable {
    // build and start the Locator locally
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    assertNotNull(this.launcher);

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      assertEquals(Status.ONLINE, this.launcher.status().getStatus());

      // validate the pid file and its contents
      this.pidFile = new File(builder.getWorkingDirectory(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      assertEquals(Status.ONLINE, this.launcher.status().getStatus());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartCreatesPidFile

  public void testStartDeletesStaleControlFiles() throws Throwable {
    // create existing control files
    this.stopRequestFile = new File(ProcessType.LOCATOR.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(ProcessType.LOCATOR.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(ProcessType.LOCATOR.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForLocatorToStart(this.launcher);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate stale control files were deleted
      assertFalse(stopRequestFile.exists());
      assertFalse(statusRequestFile.exists());
      assertFalse(statusFile.exists());
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartDeletesStaleControlFiles
  
  public void testStartOverwritesStalePidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
    assertFalse("Integer.MAX_VALUE shouldn't be the same as local pid " + Integer.MAX_VALUE, Integer.MAX_VALUE == ProcessUtils.identifyPid());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForLocatorToStart(this.launcher);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartOverwritesStalePidFile

  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
    assertTrue(getUniqueName() + " is broken if PID == Integer.MAX_VALUE", ProcessUtils.identifyPid() != Integer.MAX_VALUE);
    
    // create existing pid file
    this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse(realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);

    // build and start the locator
    final Builder builder = new Builder()
        .setForce(true)
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertTrue(builder.getForce());
    this.launcher = builder.build();
    assertTrue(this.launcher.isForcing());
    this.launcher.start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingForceOverwritesExistingPidFile

  public void testStartWithDefaultPortInUseFails() throws Throwable {
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.locatorPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    assertTrue(this.socket.isBound());
    assertFalse(this.socket.isClosed());
    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));

    assertNotNull(System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY));
    assertEquals(this.locatorPort, Integer.valueOf(System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY)).intValue());
    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    this.launcher = builder.build();
    
    assertEquals(this.locatorPort, this.launcher.getPort().intValue());
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;

    try {
      this.launcher.start();
     
      // why did it not fail like it's supposed to?
      final String property = System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
      assertNotNull(property);
      assertEquals(this.locatorPort, Integer.valueOf(property).intValue());
      assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
      assertEquals(this.locatorPort, this.launcher.getPort().intValue());
      assertEquals(this.locatorPort, this.socket.getLocalPort());
      assertTrue(this.socket.isBound());
      assertFalse(this.socket.isClosed());
      //assertEquals(0, this.launcher.getCache().getCacheServers().size());
      //fail("KIRK port is " + this.launcher.getCache().getCacheServers().get(0).getPort());
      
      fail("LocatorLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException text varies by platform
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      this.pidFile = new File (ProcessType.LOCATOR.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartWithDefaultPortInUseFails
  
  public void testStartWithExistingPidFileFails() throws Throwable {
    // create existing pid file
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());

    this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
    writePid(this.pidFile, realPid);
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;
    
    try {
      this.launcher.start();
      fail("LocatorLauncher start should have thrown RuntimeException caused by FileAlreadyExistsException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      assertTrue(expected.getMessage(), expected.getMessage().contains("A PID file already exists and a Locator may be running in"));
      assertEquals(RuntimeException.class, expected.getClass());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof FileAlreadyExistsException);
      assertTrue(cause.getMessage().contains("Pid file already exists: "));
      assertTrue(cause.getMessage().contains("vf.gf.locator.pid for process " + realPid));
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      delete(this.pidFile);
      final Status theStatus = status.getStatus();
      assertFalse(theStatus == Status.STARTING);
      assertFalse(theStatus == Status.ONLINE);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartWithExistingPidFileFails

  public void testStartUsingPort() throws Throwable {
    // collect and throw the FIRST failure
    Throwable failure = null;

    // generate one free port and then use it instead of default
    final int freeTCPPort = AvailablePortHelper.getRandomAvailableTCPPort();
    assertTrue(AvailablePort.isPortAvailable(freeTCPPort, AvailablePort.SOCKET));
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(freeTCPPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    this.launcher = builder.build();

    // wait for locator to start
    int pid = 0;
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(pidFile.exists());
      pid = readPid(pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify locator did not use default port
      assertTrue(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
      
      final LocatorState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"" + freeTCPPort + "\" instead of " + portString, String.valueOf(freeTCPPort), portString);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingPort
  
  public void testStartUsingPortInUseFails() throws Throwable {
    // generate one free port and then use it instead of default
    final int freeTCPPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        freeTCPPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(freeTCPPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    this.launcher = builder.build();
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;
    
    try {
      this.launcher.start();
      fail("LocatorLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException string varies by platform
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      this.pidFile = new File (ProcessType.LOCATOR.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingPortInUseFails
  
  public void testStatusUsingPid() throws Throwable {
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    LocatorLauncher pidLauncher = null;
    
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      final LocatorState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath(), actualStatus.getWorkingDirectory());
      //assertEquals(???, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    if (pidLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        getLogWriter().error(e);
        if (failure == null) {
          failure = e;
        }
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        getLogWriter().error(e);
        if (failure == null) {
          failure = e;
        }
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStatusUsingPid
  
  public void testStatusUsingWorkingDirectory() throws Throwable {
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    LocatorLauncher dirLauncher = null;
    
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      final LocatorState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath(), actualStatus.getWorkingDirectory());
      //assertEquals(???, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    if (dirLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        getLogWriter().error(e);
        if (failure == null) {
          failure = e;
        }
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        getLogWriter().error(e);
        if (failure == null) {
          failure = e;
        }
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStatusUsingWorkingDirectory
  
  public void testStopUsingPid() throws Throwable {
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    LocatorLauncher pidLauncher = null;
    
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());
      
      // stop the locator
      final LocatorState locatorState = pidLauncher.stop();
      assertNotNull(locatorState);
      assertEquals(Status.STOPPED, locatorState.getStatus());
    
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStopUsingPid
  
  public void testStopUsingWorkingDirectory() throws Throwable {
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    LocatorLauncher dirLauncher = null;
    
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
    
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());
      
      // stop the locator
      final LocatorState locatorState = dirLauncher.stop();
      assertNotNull(locatorState);
      assertEquals(Status.STOPPED, locatorState.getStatus());
    
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStopUsingWorkingDirectory
}
