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
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.BindException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.AbstractBridgeServer;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.process.FileAlreadyExistsException;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;

import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Tests usage of ServerLauncher as a local API in existing JVM.
 *
 * @author Kirk Lund
 * @author David Hoots
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.ServerState
 * @see com.gemstone.gemfire.internal.AvailablePortHelper
 * @since 8.0
 */
@SuppressWarnings("serial")
public class ServerLauncherLocalDUnitTest extends AbstractServerLauncherDUnitTestCase {

  public ServerLauncherLocalDUnitTest(String name) {
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
        .setDisableDefaultServer(true)
        .setForce(true)
        .setMemberName(getUniqueName())
        .set(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "true")
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0")
        .build();

    assertNotNull(this.launcher);
    
    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());
      waitForServerToStart(this.launcher);
  
      final Cache cache = this.launcher.getCache();
  
      assertNotNull(cache);
  
      final DistributedSystem distributedSystem = cache.getDistributedSystem();
  
      assertNotNull(distributedSystem);
      assertEquals("true", distributedSystem.getProperties().getProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME));
      assertEquals("config", distributedSystem.getProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME));
      assertEquals("0", distributedSystem.getProperties().getProperty(DistributionConfig.MCAST_PORT_NAME));
      assertEquals(getUniqueName(), distributedSystem.getProperties().getProperty(DistributionConfig.NAME_NAME));

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getCache());
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
  }
  
  public void testStartCreatesPidFile() throws Throwable {
    // build and start the Server locally
    final Builder builder = new Builder()
    .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    assertNotNull(this.launcher);

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      assertEquals(Status.ONLINE, this.launcher.status().getStatus());

      // validate the pid file and its contents
      this.pidFile = new File(builder.getWorkingDirectory(), ProcessType.SERVER.getPidFileName());
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
    this.stopRequestFile = new File(ProcessType.SERVER.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(ProcessType.SERVER.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(ProcessType.SERVER.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForServerToStart(this.launcher);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate stale control files were deleted
      assertFalse(this.stopRequestFile.exists());
      assertFalse(this.statusRequestFile.exists());
      assertFalse(this.statusFile.exists());
      
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
    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    assertFalse("Integer.MAX_VALUE shouldn't be the same as local pid " + Integer.MAX_VALUE, Integer.MAX_VALUE == ProcessUtils.identifyPid());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForServerToStart(this.launcher);
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

  /**
   * Confirms fix for #47778.
   */
  public void testStartUsingDisableDefaultServerLeavesPortFree() throws Throwable {
    // collect and throw the FIRST failure
    Throwable failure = null;

    // build and start the server
    assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    this.launcher = builder.build();

    // wait for server to start
    int pid = 0;
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForServerToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify server did not a port
      assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
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
  } // testStartUsingDisableDefaultServerLeavesPortFree

  /**
   * Confirms fix for #47778.
   */
  public void testStartUsingDisableDefaultServerSkipsPortCheck() throws Throwable {
    // collect and throw the FIRST failure
    Throwable failure = null;

    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.serverPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    this.launcher = builder.build();

    // wait for server to start
    int pid = 0;
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForServerToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
  
    // verify port is still in use
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingDisableDefaultServerSkipsPortCheck

  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
    assertTrue(getUniqueName() + " is broken if PID == Integer.MAX_VALUE", ProcessUtils.identifyPid() != Integer.MAX_VALUE);
    
    // create existing pid file
    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse(realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);

    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setForce(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertTrue(builder.getForce());
    this.launcher = builder.build();
    assertTrue(this.launcher.isForcing());
    this.launcher.start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForServerToStart(this.launcher);

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

  /**
   * Confirms part of fix for #47664
   */
  public void testStartUsingServerPortOverridesCacheXml() throws Throwable {
    // verifies part of the fix for #47664
    
    // generate two free ports
    final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
    assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
    
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer().setPort(freeTCPPorts[0]);
    
    this.cacheXmlFile = new File(getUniqueName() + ".xml");
    final boolean useSchema = false;
    final PrintWriter pw = new PrintWriter(new FileWriter(this.cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, CacheXml.VERSION_7_5);
    pw.close();
    
    System.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, this.cacheXmlFile.getCanonicalPath());
    
    // start server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(freeTCPPorts[1])
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    this.launcher.start();
  
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start up
    int pid = 0;
    try {
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

//      // TODO: is there a way to validate which cache.xml file was used?
//      final InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
//      final DistributionConfig config = ids.getConfig();
//      assertEquals(this.cacheXmlFile, config.getCacheXmlFile());
      
      // verify server used --server-port instead of default or port in cache.xml
      assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
      assertFalse(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      final int port = Integer.valueOf(portString);
      assertEquals("Port should be " + freeTCPPorts[1] + " instead of " + port, freeTCPPorts[1], port);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
      assertFalse("PID file still exists!", pidFile.exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    if (failure != null) {
      throw failure;
    }
  } // testStartUsingServerPortOverridesCacheXml
  
  /**
   * Confirms part of fix for #47664
   */
  public void testStartUsingServerPortUsedInsteadOfDefaultCacheXml() throws Throwable {
    // verifies part of the fix for #47664
    
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer();
    
    this.cacheXmlFile = new File(getUniqueName() + ".xml");
    final boolean useSchema = false;
    final PrintWriter pw = new PrintWriter(new FileWriter(this.cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, CacheXml.VERSION_7_5);
    pw.close();
    
    System.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, this.cacheXmlFile.getCanonicalPath());
      
    // start server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(this.serverPort)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    this.launcher.start();
  
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start up
    int pid = 0;
    try {
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify server used --server-port instead of default
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final int port = Integer.valueOf( this.launcher.status().getPort());
      assertEquals("Port should be " + this.serverPort + " instead of " + port, this.serverPort, port);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    // stop the server
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
  } // testStartWithServerPortUsedInsteadOfDefaultCacheXml

  public void testStartWithDefaultPortInUseFails() throws Throwable {
    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.serverPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);

    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;

    try {
      this.launcher.start();
     
      // why did it not fail like it's supposed to?
      final String property = System.getProperty(AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
      assertNotNull(property);
      assertEquals(this.serverPort, Integer.valueOf(property).intValue());
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      //assertEquals(0, this.launcher.getCache().getCacheServers().size());
      
      fail("Server port is " + this.launcher.getCache().getCacheServers().get(0).getPort());
      fail("ServerLauncher start should have thrown RuntimeException caused by BindException");
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
      this.pidFile = new File (ProcessType.SERVER.getPidFileName());
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
    ServerState status = null;
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

    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    writePid(this.pidFile, realPid);
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
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
      fail("ServerLauncher start should have thrown RuntimeException caused by FileAlreadyExistsException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      assertTrue(expected.getMessage().contains("A PID file already exists and a Server may be running in"));
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    ServerState status = null;
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
      assertTrue(cause.getMessage().contains("vf.gf.server.pid for process " + realPid));
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

  /**
   * Confirms fix for #47665.
   */
  public void testStartUsingServerPortInUseFails() throws Throwable {
    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    final int freeTCPPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        freeTCPPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    
    // build and start the server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(freeTCPPort)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    this.launcher = builder.build();
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;
    
    try {
      this.launcher.start();
      fail("ServerLauncher start should have thrown RuntimeException caused by BindException");
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
      this.pidFile = new File (ProcessType.SERVER.getPidFileName());
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
    ServerState status = null;
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
  } // testStartUsingServerPortInUseFails
  
  public void testStatusUsingPid() throws Throwable {
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    ServerLauncher pidLauncher = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      final ServerState actualStatus = pidLauncher.status();
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
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    // collect and throw the FIRST failure
    Throwable failure = null;
    ServerLauncher dirLauncher = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      final ServerState actualStatus = dirLauncher.status();
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
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    ServerLauncher pidLauncher = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());
      
      // stop the server
      final ServerState serverState = pidLauncher.stop();
      assertNotNull(serverState);
      assertEquals(Status.STOPPED, serverState.getStatus());
    
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
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    ServerLauncher dirLauncher = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
    
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());
      
      // stop the server
      final ServerState serverState = dirLauncher.stop();
      assertNotNull(serverState);
      assertEquals(Status.STOPPED, serverState.getStatus());
    
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
