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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;

import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Tests launching LocatorLauncher in forked processes.
 *
 * @author Kirk Lund
 * @since 8.0
 */
@SuppressWarnings("serial")
public class LocatorLauncherRemoteDUnitTest extends AbstractLocatorLauncherDUnitTestCase {

  protected transient volatile Process process;
  protected transient volatile ProcessStreamReader processOutReader;
  protected transient volatile ProcessStreamReader processErrReader;
  
  protected transient volatile File workingDirectory = null;

  public LocatorLauncherRemoteDUnitTest(String name) {
    super(name);
  }

  @Override
  protected void subSetUp1() throws Exception {
    subSetUp2();
  }

  @Override
  protected void subTearDown1() throws Exception {
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
        //delete(this.workingDirectory);
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
  
  public void testRunningLocatorOutlivesForkingProcess() throws InterruptedException, IOException {
    // TODO:KIRK: fix up this test
    
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // launch LocatorLauncherForkingProcess which then launches the GemFire Locator
    List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncherRemoteDUnitTest.class.getName().concat("$").concat(LocatorLauncherForkingProcess.class.getSimpleName()));
    command.add(String.valueOf(this.locatorPort));

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    Thread waiting = new Thread(new Runnable() {
      public void run() {
        try {
          assertEquals(0, process.waitFor());
        }
        catch (InterruptedException ignore) {
          getLogWriter().error("Interrupted while waiting for process!", ignore);
        }
      }
    });

    try {
      waiting.start();
      waiting.join(TIMEOUT_MILLISECONDS);
      assertFalse("Process took too long and timed out!", waiting.isAlive());
    }
    finally {
      if (waiting.isAlive()) {
        waiting.interrupt();
      }
    }

    LocatorLauncher locatorLauncher = new Builder().setWorkingDirectory(this.workingDirectory.getCanonicalPath()).build();

    assertEquals(Status.ONLINE, locatorLauncher.status().getStatus());
    assertEquals(Status.STOPPED, locatorLauncher.stop().getStatus());
  }

  public void testStartCreatesPidFile() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // build and start the locator
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);
    
      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
      // check the status
      final LocatorState locatorState = this.launcher.status();
      assertNotNull(locatorState);
      assertEquals(Status.ONLINE, locatorState.getStatus());

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    // stop the locator
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
    this.stopRequestFile = new File(this.workingDirectory, ProcessType.LOCATOR.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(this.workingDirectory, ProcessType.LOCATOR.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(this.workingDirectory, ProcessType.LOCATOR.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the locator
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
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

    // stop the locator
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
    this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the locator
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

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

    // stop the locator
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

  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    // create existing pid file
    this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
    final int otherPid = getPid();
    assertTrue("Pid " + otherPid + " should be alive", isPidAlive(otherPid));
    writePid(this.pidFile, otherPid);

    // build and start the locator
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");
    command.add("--force");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

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

    // stop the locator
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

  public void testStartUsingPortInUseFails() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.locatorPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--port=" + this.locatorPort);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start and fail
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      int code = process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
      
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
    LocatorState status = null;
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
  } // testStartUsingPortInUseFails

  public void testStartWithDefaultPortInUseFails() throws Throwable {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());
    
    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();

    this.socket = SocketCreator.getDefaultInstance().createServerSocket(
        this.locatorPort, 50, null, getLogWriter().convertToLogWriterI18n(), -1);
    
    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
    assertTrue(this.socket.isBound());
    assertFalse(this.socket.isClosed());
    
    // launch locator
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.locatorPort);
    jvmArguments.add("-D" + getUniqueName() + "=true");
    jvmArguments.add("-Dgemfire.log-level=config");
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    
    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).start();
    
    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start up
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      int code = process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
      
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
    LocatorState status = null;
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
    this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);
    
    // build and start the locator
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher, 10*1000, false);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    try {
      final LocatorState status = dirLauncher.stop();
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    LocatorLauncher pidLauncher = null; 
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
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
      final LocatorState actualStatus = pidLauncher.status();
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
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the locator
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(isPidAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.workingDirectory, logFileName).exists());

      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      // validate the status
      final LocatorState actualStatus = dirLauncher.status();
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
      assertEquals(SocketCreator.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the locator
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
  } // testStatusUsingWorkingDirectory
  
  public void testStatusWithEmptyPidFile() throws Exception {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
    assertTrue(this.pidFile + " already exists", this.pidFile.createNewFile());
    //writePid(pidFile, realPid);
    
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
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

    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    LocatorState locatorState = dirLauncher.status();
    assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
  } // testStatusWithNoPidFile
  
  public void testStatusWithStalePidFile() throws Exception {
    this.workingDirectory = new File(getUniqueName());
    this.workingDirectory.mkdir();
    assertTrue(this.workingDirectory.isDirectory() && this.workingDirectory.canWrite());

    this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
    final int pid = 0;
    assertFalse(ProcessUtils.isProcessAlive(pid));
    writePid(this.pidFile, pid);
    
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream(), createLoggingListener("sysout", getUniqueName() + "#sysout")).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream(), createLoggingListener("syserr", getUniqueName() + "#syserr")).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    LocatorLauncher pidLauncher = null; 
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
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
      final LocatorState status = pidLauncher.status();
      assertNotNull(status);
      assertEquals(Status.ONLINE, status.getStatus());
      assertEquals(pid, status.getPid().intValue());

    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the locator
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }          
      waitForPidToStop(pid);
      waitForFileToDelete(pidFile);
      
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
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.workingDirectory).start();
    this.processOutReader = new ProcessStreamReader(this.process.getInputStream()).start();
    this.processErrReader = new ProcessStreamReader(this.process.getErrorStream()).start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    // wait for locator to start
    int pid = 0;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.workingDirectory.getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.workingDirectory, ProcessType.LOCATOR.getPidFileName());
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
      // stop the locator
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
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

  public static class LocatorLauncherForkingProcess {

    public static void main(final String... args) throws FileNotFoundException {
      File file = new File(System.getProperty("user.dir"), LocatorLauncherForkingProcess.class.getSimpleName().concat(".log"));

      LocalLogWriter logWriter = new LocalLogWriter(LogWriterImpl.ALL_LEVEL, new PrintStream(new FileOutputStream(file, true)));

      try {
        final int port = Integer.parseInt(args[0]);

        // launch LocatorLauncher
        List<String> command = new ArrayList<String>();
        command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("-Dgemfire.mcast-port=0");
        command.add(LocatorLauncher.class.getName());
        command.add(LocatorLauncher.Command.START.getName());
        command.add(LocatorLauncherForkingProcess.class.getSimpleName() + "_Locator");
        command.add("--port=" + port);
        command.add("--redirect-output");
        
        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main command: " + command);
        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main starting...");
        
        Process forkedProcess = new ProcessBuilder(command).start();

        ProcessStreamReader processOutReader = new ProcessStreamReader(forkedProcess.getInputStream()).start();
        ProcessStreamReader processErrReader = new ProcessStreamReader(forkedProcess.getErrorStream()).start();

        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main waiting for locator to start...");

        waitForLocatorToStart(port, TIMEOUT_MILLISECONDS, 10, true);

        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main exiting...");

        System.exit(0);
      }
      catch (Throwable t) {
        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main error: " + t, t);
        System.exit(-1);
      }
    }
  }
}
