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
package com.gemstone.gemfire.internal.process;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import quickstart.ProcessWrapper;
import com.gemstone.gemfire.internal.process.LocalProcessControllerJUnitTest.Process;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Integration tests for LocalProcessController.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
@SuppressWarnings("serial")
public class LocalProcessControllerDUnitTest extends DistributedTestCase {

  private static final String TEST_CASE = "LocalProcessControllerDUnitTest";

  private static final String FAILSAFE_BINDING = TEST_CASE + ".FAILSAFE_BINDING";

  private int registryPort;
  
  public LocalProcessControllerDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(TEST_CASE).mkdir();
    final int[] tcpPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    this.registryPort = tcpPorts[0];
  }
  
  @Override
  public void tearDown2() throws Exception {
    invokeFailsafe();
  }

  public void testConnectPidMatchesDisconnect() throws Exception {
    final int pid = ProcessUtils.identifyPid();
    
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable("LocalProcessControllerDUnitTest#testConnectPidMatchesDisconnect") {
      @Override
      public void run() {
        LocalProcessController stopper = null;
        try {
          stopper = new LocalProcessController(pid);
          stopper.connect();
          assertTrue(stopper.checkPidMatches());
        } catch (ConnectionFailedException e) {
          throw new Error(e);
        } catch (IOException e) {
          throw new Error(e);
        } catch (IllegalStateException e) {
          throw new Error(e);
        } catch (PidUnavailableException e) {
          throw new Error(e);
        } finally {
          if (stopper != null) {
            stopper.disconnect();
          }
        }
      }
    });
  }
  
  public void testStartAndStopProcess() throws Exception {
    final String testName = "testStartAndStopProcess";
    
    execAndValidate(new String[] {
        ""+this.registryPort,
        testName
        },
        TEST_CASE + " process running");
    
    final File pidFile = new File(TEST_CASE + File.separator + testName + ".pid");
    final ObjectName objectName = ObjectName.getInstance(
        TEST_CASE + ":testName=" + testName);
    final String pidAttribute = "Pid";
    final String method = "stop";
    final LocalProcessController stopper = new LocalProcessController(pidFile);
    
    stopper.connect();
    assertTrue(stopper.checkPidMatches());
    stopper.disconnect();
    stopper.stop(objectName, pidAttribute, method, new String[] {"Process"}, new Object[]{Boolean.TRUE});
  }
  
  private void execAndValidate(String[] args, String regex) throws InterruptedException {
    final ProcessWrapper process = new ProcessWrapper(getClass(), args);
    process.execute(null);
    if (regex != null) {
      process.waitForOutputToMatch(regex);
    }
  }

  private void invokeFailsafe() {
    try {
      final Registry registry = LocateRegistry.getRegistry(this.registryPort);
      final FailSafeRemote failsafe = (FailSafeRemote) registry.lookup(FAILSAFE_BINDING);
      failsafe.kill();
    } catch (RemoteException ignore) {
      // process was probably stopped already
    } catch (NotBoundException ignore) {
      // process was probably stopped already
    }
  }

  public static void main(String[] args) throws Exception {
    // install fail safe
    if (args.length != 2) {
      throw new IllegalArgumentException("Requires two args: registry_port, test_name");
    }
    
    final int registryPort = Integer.valueOf(args[0]);
    final String testName = args[1];
    
    try {
      final Registry registry = LocateRegistry.createRegistry(registryPort);
      final FailSafe failsafe = new FailSafe();
      registry.bind(FAILSAFE_BINDING, failsafe);
    } catch (RemoteException ignore) {
      throw new InternalGemFireError(ignore);
    } catch (AlreadyBoundException ignore) {
      throw new InternalGemFireError(ignore);
    }

    // create pid file with LocalProcessLauncher
    final File pidFile = new File(TEST_CASE + File.separator + testName + ".pid");
    final LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    final int pid = launcher.getPid();
    
    // install simple MBean
    final Process process = new Process(pid, true);
    final ObjectName objectName = ObjectName.getInstance(
        TEST_CASE + ":testName=" + testName);

    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    server.registerMBean(process, objectName);
    
    // wait until the MBean says to stop
    process.waitUntilStopped();
  }
  
  public static interface FailSafeRemote extends Remote {
    public void kill() throws RemoteException;
  }

  public static class FailSafe extends UnicastRemoteObject implements FailSafeRemote {
    private static final long serialVersionUID = 4419392070588214807L;
    public FailSafe() throws RemoteException {
      super();
    }
    public void kill() throws RemoteException {
      System.exit(0);
    }
  }
}
