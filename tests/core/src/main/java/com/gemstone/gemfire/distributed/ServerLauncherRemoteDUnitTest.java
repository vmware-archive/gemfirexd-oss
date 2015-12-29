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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import quickstart.ProcessWrapper;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.AbstractBridgeServer;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;

import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Tests launching ServerLauncher in forked processes.
 *
 * @author Kirk Lund
 * @author David Hoots
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.ServerState
 * @see com.gemstone.gemfire.internal.AvailablePortHelper
 * @since 7.5
 */
@SuppressWarnings("serial")
public class ServerLauncherRemoteDUnitTest extends AbstractServerLauncherDUnitTestCase {

  protected transient volatile Process process;
  protected transient volatile ProcessStreamReader processOutReader;
  protected transient volatile ProcessStreamReader processErrReader;
  
  protected transient volatile File workingDirectory = null;

  public ServerLauncherRemoteDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void subSetUp1() throws Exception {
    subSetUp2();
  }

  @Override
  protected final void subTearDown1() throws Exception {    
    if (this.process != null) {
      this.process.destroy();
      this.process = null;
    }
    if (this.processOutReader != null && this.processOutReader.isRunning()) {
      this.processOutReader.stop();
    }
    if (this.processErrReader != null && this.processErrReader.isRunning()) {
      this.processErrReader.stop();
    }
    
    if (this.workingDirectory != null) {
      try {
        //delete(this.workingDirectory); keep it around to help debug failures
      } finally {
        this.workingDirectory = null;
      }
    }
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
  
  public void broken_testRunningServerOutlivesForkingProcess() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // launch ServerLauncherForkingProcess which then launches server
    
//    final List<String> command = new ArrayList<String>();
//    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
//    command.add("-cp");
//    command.add(System.getProperty("java.class.path"));
//    command.add(ServerLauncherDUnitTest.class.getName().concat("$").concat(ServerLauncherForkingProcess.class.getSimpleName()));
//
//    process = new ProcessBuilder(command).directory(workingDirectory).start();
//    assertNotNull(process);
//    processOutReader = new ProcessStreamReader(process.getInputStream(), createListener("sysout", getUniqueName() + "#sysout")).start();
//    processErrReader = new ProcessStreamReader(process.getErrorStream(), createListener("syserr", getUniqueName() + "#syserr")).start();

    @SuppressWarnings("unused")
    File file = new File(this.workingDirectory, ServerLauncherForkingProcess.class.getSimpleName().concat(".log"));
    //-getLogWriter().info("KIRK: log file is " + file);
    
    final ProcessWrapper pw = new ProcessWrapper(ServerLauncherForkingProcess.class);
    pw.execute(null, this.workingDirectory).waitFor(true);
    //getLogWriter().info("[testRunningServerOutlivesForkingProcess] ServerLauncherForkingProcess output is:\n\n"+pw.getOutput());
    
//    // create waiting thread since waitFor does not have a timeout 
//    Thread waiting = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          assertEquals(0, process.waitFor());
//        } catch (InterruptedException e) {
//          getLogWriter().error("Interrupted while waiting for process", e);
//        }
//      }
//    });

    // collect and throw the FIRST failure
    Throwable failure = null;
    
//    // start waiting thread and join to it for timeout
//    try {
//      waiting.start();
//      waiting.join(TIMEOUT_MILLISECONDS);
//      assertFalse("ServerLauncherForkingProcess took too long and caused timeout", waiting.isAlive());
//      
//    } catch (Throwable e) {
//      getLogWriter().error(e);
//      if (failure == null) {
//        failure = e;
//      }
//    } finally {
//      if (waiting.isAlive()) {
//        waiting.interrupt();
//      }
//    }

    // wait for server to start
    int pid = 0;
    final String serverName = ServerLauncherForkingProcess.class.getSimpleName()+"_server";
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = serverName+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
      // validate the status
      final ServerState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.workingDirectory.getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(ServerLauncherForkingProcess.getJvmArguments(), actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.workingDirectory.getCanonicalPath() + File.separator + serverName + ".log", actualStatus.getLogFile());
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(),
          actualStatus.getHost());
      assertEquals(serverName, actualStatus.getMemberName());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
  
    if (failure != null) {
      throw failure;
    }
  } // broken_testRunningServerOutlivesForkingProcess

  public void testStartCreatesPidFile() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
    
      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
      // check the status
      final ServerState serverState = this.launcher.status();
      assertNotNull(serverState);
      assertEquals(Status.ONLINE, serverState.getStatus());

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
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
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // create existing control files
    this.stopRequestFile = new File(this.workingDirectory, ProcessType.SERVER.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(this.workingDirectory, ProcessType.SERVER.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(this.workingDirectory, ProcessType.SERVER.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate stale control files were deleted
//      assertFalse(this.stopRequestFile.exists());
//      assertFalse(this.statusRequestFile.exists());
//      assertFalse(this.statusFile.exists());
      waitForFileToDelete(this.stopRequestFile);
      waitForFileToDelete(this.statusRequestFile);
      waitForFileToDelete(this.statusFile);
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
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
    assertTrue(getUniqueName() + " is broken if PID == Integer.MAX_VALUE", ProcessUtils.identifyPid() != Integer.MAX_VALUE);
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // create existing pid file
    this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertFalse(pid == Integer.MAX_VALUE);

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
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
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

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
      waitForPidToStop(pid);
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

  public void testStartUsingDisableDefaultServerSkipsPortCheck() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    // make serverPort in use
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.serverPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
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
      waitForPidToStop(pid);
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
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // create existing pid file
    this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
    final int otherPid = getPid();
    assertTrue("Pid " + otherPid + " should be alive", isPidAlive(otherPid));
    writePid(this.pidFile, otherPid);

    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");
    command.add("--force");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));
      assertTrue(pid != otherPid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
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
   * Confirms fix for #47665.
   */
  public void testStartUsingServerPortInUseFails() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    // make serverPort in use
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.serverPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + this.serverPort);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start and fail
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      int code = this.process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    // if the following fails, then the SHORTER_TIMEOUT is too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper
    
    // validate that output contained BindException 
    assertTrue(outputContainedExpectedString.get());

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
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
  
  /**
   * Confirms part of fix for #47664
   */
  public void testStartUsingServerPortOverridesCacheXml() throws Throwable {
    // generate two free ports
    final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    
    // write out cache.xml with one port
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory());
    
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer().setPort(freeTCPPorts[0]);
    
    this.cacheXmlFile = new File(this.workingDirectory, getUniqueName()+".xml");
    final boolean useSchema = false;
    final PrintWriter pw = new PrintWriter(new FileWriter(this.cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, CacheXml.VERSION_7_5);
    pw.close();
    
    // launch server and specify a different port
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    jvmArguments.add("-Dgemfire."+DistributionConfig.CACHE_XML_FILE_NAME+"="+this.cacheXmlFile.getCanonicalPath());
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + freeTCPPorts[1]);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start up
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
  
      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      // verify server used --server-port instead of default or port in cache.xml
      assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
      assertFalse(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
      
      ServerState status = this.launcher.status();
      String portString = status.getPort();
      int port = Integer.valueOf(portString);
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
      waitForPidToStop(pid);
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
  } // testStartUsingServerPortOverridesCacheXml
  
  /**
   * Confirms part of fix for #47664
   */
  public void testStartUsingServerPortUsedInsteadOfDefaultCacheXml() throws Throwable {
    // write out cache.xml with one port
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer();
    
    this.cacheXmlFile = new File(this.workingDirectory, getUniqueName()+".xml");
    final boolean useSchema = false;
    final PrintWriter pw = new PrintWriter(new FileWriter(this.cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, CacheXml.VERSION_7_5);
    pw.close();
  
    // launch server and specify a different port
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    jvmArguments.add("-Dgemfire."+DistributionConfig.CACHE_XML_FILE_NAME+"="+this.cacheXmlFile.getCanonicalPath());
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + this.serverPort);

    final String expectedString = "java.net.BindException";
    final AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start up
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
  
      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      // verify server used --server-port instead of default or port in cache.xml
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      int port = Integer.valueOf(portString);
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
      waitForPidToStop(pid);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    if (failure != null) {
      throw failure;
    }
  } // testStartUsingServerPortUsedInsteadOfDefaultCacheXml

  public void testStartWithDefaultPortInUseFails() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();

    // make serverPort in use
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.serverPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // launch server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + AbstractBridgeServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start up
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      int code = this.process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    // if the following fails, then the SHORTER_TIMEOUT might be too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper
    
    // validate that output contained BindException 
    assertTrue(outputContainedExpectedString.get());

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
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
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // create existing pid file
    this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);
    
    // build and start the server
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart(dirLauncher, 10*1000, false);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
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

  public void testStatusUsingPid() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    ServerLauncher pidLauncher = null; 
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final ServerState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.workingDirectory.getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.workingDirectory.getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(),
          actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }          
      waitForPidToStop(pid);
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
  } // testStatusUsingPid
  
  public void testStatusUsingWorkingDirectory() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      assertNotNull(this.launcher);
      assertFalse(this.launcher.isRunning());

      // validate the status
      final ServerState actualStatus = this.launcher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.workingDirectory.getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.workingDirectory.getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(),
          actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStatusUsingWorkingDirectory
  
  public void testStatusWithEmptyPidFile() throws Exception {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
    assertTrue(this.pidFile + " already exists", this.pidFile.createNewFile());
    //writePid(pidFile, realPid);
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      dirLauncher.status();
      fail("Status should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      String expected = "Invalid pid 'null' found in " + this.pidFile.getCanonicalPath();
      assertEquals("Message was " + e.getMessage() + " instead of " + expected, expected, e.getMessage());
    }
  } // testStatusWithEmptyPidFile
  
  public void testStatusWithNoPidFile() throws Exception {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    ServerState serverState = dirLauncher.status();
    assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
  } // testStatusWithNoPidFile
  
  public void testStatusWithStalePidFile() throws Exception {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
    final int pid = 0;
    assertFalse(ProcessUtils.isProcessAlive(pid));
    writePid(this.pidFile, pid);
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(workingDirectory.getCanonicalPath())
        .build();
    try {
      dirLauncher.status();
      fail("Status should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      String expected = "Invalid pid '" + pid + "' found in " + this.pidFile.getCanonicalPath();
      assertEquals("Message was " + e.getMessage() + " instead of " + expected, expected, e.getMessage());
    }
  } // testStatusWithStalePidFile
  
  public void testStopUsingPid() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createLoggingListener("sysout", getUniqueName() + "#sysout")).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createLoggingListener("syserr", getUniqueName() + "#syserr")).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    ServerLauncher pidLauncher = null; 
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final ServerState status = pidLauncher.status();
      assertNotNull(status);
      assertEquals(Status.ONLINE, status.getStatus());
      assertEquals(pid, status.getPid().intValue());

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }          
      waitForPidToStop(pid);
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
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      // stop the server
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
      assertFalse("PID file still exists!", this.pidFile.exists());
      
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

  protected void waitForServerToStart() {
    // this.process
    waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        try {
          assertNotNull(process);
          try {
            final int value = process.exitValue();
            fail("Process has died with exit value " + value + " while waiting for it to start.");
          } catch (IllegalThreadStateException e) {
            // expected
          }
          final ServerState serverState = launcher.status();
          assertNotNull(serverState);
          return Status.ONLINE.equals(serverState.getStatus());
        }
        catch (RuntimeException e) {
          return false;
        }
      }

      @Override
      public String description() {
        return "waiting for local Server to start: " + launcher.status();
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
  }
  
  public static class ServerLauncherForkingProcess {

    public static List<String> getJvmArguments() {
      final List<String> jvmArguments = new ArrayList<String>();
      jvmArguments.add("-Dgemfire.log-level=config");
      jvmArguments.add("-Dgemfire.mcast-port=0");
      return jvmArguments;
    }
    
    public static void main(final String... args) throws IOException, PidUnavailableException {
      //-System.out.println("KIRK inside main");
      File file = new File(System.getProperty("user.dir"), ServerLauncherForkingProcess.class.getSimpleName().concat(".log"));
      file.createNewFile();
      LocalLogWriter logWriter = new LocalLogWriter(LogWriterImpl.ALL_LEVEL, new PrintStream(new FileOutputStream(file, true)));
      //LogWriter logWriter = new PureLogWriter(LogWriterImpl.ALL_LEVEL);
      logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main PID is " + getPid());

      try {
        // launch ServerLauncher
        final List<String> jvmArguments = getJvmArguments();
        assertTrue(jvmArguments.size() == 2);
        final List<String> command = new ArrayList<String>();
        command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
        for (String jvmArgument : jvmArguments) {
          command.add(jvmArgument);
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ServerLauncher.class.getName());
        command.add(ServerLauncher.Command.START.getName());
        command.add(ServerLauncherForkingProcess.class.getSimpleName()+"_server");
        command.add("--disable-default-server");
        command.add("--redirect-output");

        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main command: " + command);
        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main starting...");

        //-System.out.println("KIRK launching " + command);
        
        @SuppressWarnings("unused")
        Process forkedProcess = new ProcessBuilder(command).start();

//        processOutReader = new ProcessStreamReader(forkedProcess.getInputStream()).start();
//        processErrReader = new ProcessStreamReader(forkedProcess.getErrorStream()).start();

//        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main waiting for Server to start...");
//
//        File workingDir = new File(System.getProperty("user.dir"));
//        System.out.println("KIRK waiting for server to start in " + workingDir);
//        final ServerLauncher dirLauncher = new ServerLauncher.Builder()
//            .setWorkingDirectory(workingDir.getCanonicalPath())
//            .build();
//        waitForServerToStart(dirLauncher, true);

        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main exiting...");

        //-System.out.println("KIRK exiting");
        System.exit(0);
      }
      catch (Throwable t) {
        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main error: " + t, t);
        System.exit(-1);
      }
    }
  }
}
