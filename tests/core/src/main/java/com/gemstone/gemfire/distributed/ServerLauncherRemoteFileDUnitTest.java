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
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.lang.AttachAPINotFoundException;

import dunit.SerializableRunnable;

/**
 * Subclass of ServerLauncherRemoteDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar.  As a result ServerLauncher
 * ends up using the FileProcessController implementation.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
@SuppressWarnings("serial")
public class ServerLauncherRemoteFileDUnitTest extends ServerLauncherRemoteDUnitTest {
  
  public ServerLauncherRemoteFileDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected void subSetUp2() throws Exception {
    disableAttachApi();
    invokeInEveryVM(new SerializableRunnable("disableAttachApi") {
      @Override
      public void run() {
        disableAttachApi();
      }
    });
  }
  
  @Override
  protected void subTearDown2() throws Exception {   
    enableAttachApi();
    invokeInEveryVM(new SerializableRunnable("enableAttachApi") {
      @Override
      public void run() {
        enableAttachApi();
      }
    });
  }
  
  @Override
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());

    invokeInEveryVM(new SerializableRunnable("isAttachAPIFound") {
      @Override
      public void run() {
        final ProcessControllerFactory factory = new ProcessControllerFactory();
        assertFalse(factory.isAttachAPIFound());
      }
    });
  }
  
  @Override
  public void testStatusUsingPid() throws Throwable {
    // FileProcessController cannot request status with PID
    
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

      // status with pid only should throw AttachAPINotFoundException
      try {
        pidLauncher.status();
        fail("FileProcessController should have thrown AttachAPINotFoundException");
      } catch (AttachAPINotFoundException e) {
        // passed
      }
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid, true);
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    } finally {
      new File(ProcessType.SERVER.getStatusRequestFileName()).delete();
    }
    
    if (failure != null) {
      throw failure;
    }
  }
  
  @Override
  public void testStopUsingPid() throws Throwable {
    // FileProcessController cannot request stop with PID
    
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

      // stop with pid only should throw AttachAPINotFoundException
      try {
        pidLauncher.stop();
        fail("FileProcessController should have thrown AttachAPINotFoundException");
      } catch (AttachAPINotFoundException e) {
        // passed
      }

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
      waitForFileToDelete(this.pidFile);
      
    } catch (Throwable e) {
      getLogWriter().error(e);
      if (failure == null) {
        failure = e;
      }
    } finally {
      new File(ProcessType.SERVER.getStopRequestFileName()).delete();
    }
    
    if (failure != null) {
      throw failure;
    }
  }
  
  private static void disableAttachApi() {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }

  private static void enableAttachApi() {
    System.clearProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API);
  }
}
