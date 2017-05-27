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

import java.io.*;
import java.net.InetAddress;
import java.util.*;

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.internal.DistributionLocatorImpl;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.SocketCreator;

import dunit.*;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * A test of admin distributed systems layered on a normal distributed system
 *
 * @author Bruce Schuchardt
 * @since 5.0
 */
public class AdminOnDsDUnitTest extends DistributedTestCase {

  ////////  Constructors

  public AdminOnDsDUnitTest(String name) {
    super(name);
  }

  public void tearDown2() throws Exception {
    super.tearDown2();    
  }

  /** test for regression of bug 34364,
      "Admin API collocation fails when bind-address is specified"
   */
  public void testCreateAdminOnNormalDs() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm = host.getVM(1);
    final String hostAddr = SocketCreator.getLocalHost().getHostAddress();
    final InetAddress hostInetAddr = InetAddress.getByName(hostAddr);
    final int port = 
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET, hostInetAddr);
    final String locators = hostAddr + "[" + port + "]";
    DistributedSystem myDS = null;
    AdminDistributedSystem adminDS = null;

    // start a locator with a bind-address
    final Properties p = new Properties();
    if (Boolean.getBoolean("java.net.preferIPv6Addresses")) {
      p.setProperty("bind-address", hostAddr);
    }
    p.setProperty("mcast-port", "0");
    p.setProperty("log-level", getDUnitLogLevel());
    p.setProperty("locators", locators);
    vm.invoke(new SerializableRunnable("Start locator on " + port) {
      public void run() {
        File logFile = new File(getUniqueName() + "-locator" + port
                                + ".log");
        try {
          // changed to create a DS so that locator is a member
          Locator.startLocatorAndDS(port, logFile, hostInetAddr, p);

        } catch (IOException ex) {
          fail("While starting locator on port " + port, ex);
        }
      }
    });

    try {
      try {
        // start a distributed system with the same bind address
        myDS = DistributedSystem.connect(p);
        DistributedSystemFactoryTest.checkEnableAdministrationOnly(true, true);
        DistributedSystemFactoryTest.checkEnableAdministrationOnly(false, true);
  
        DistributedSystemConfig config =  
          AdminDistributedSystemFactory.defineDistributedSystem(myDS, null); 
    
        // now create a layered admin distributed system.  If it works, bug
        // 34364 is still fixed
        adminDS =  
          AdminDistributedSystemFactory.getDistributedSystem(config); 
        adminDS.connect();
        adminDS.waitToBeConnected(5000);
        boolean connected = adminDS.isConnected();
        assertTrue(connected);
  
        // added new validation for locator isRunning() here and after stop
        DistributionLocator[] distLocs = adminDS.getDistributionLocators();
        assertEquals(1, distLocs.length);
        final DistributionLocator locator = distLocs[0];
        
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return locator.isRunning();
          }
          public String description() {
            return "locator started";
          }
        };
        waitForCriterion(wc, 5000, 10, true);
      }
      finally {
        SerializableRunnable stop = 
          new SerializableRunnable("Stop locators ") {
              public void run() {
                assertTrue(Locator.hasLocator());
                Locator.getLocator().stop();
                assertFalse(Locator.hasLocator());
              }
            };
        vm.invoke(stop);

        DistributionLocator[] distLocs = adminDS.getDistributionLocators();
        assertEquals(1, distLocs.length);
        final DistributionLocator locator = distLocs[0];

        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return !locator.isRunning();
          }
          public String description() {
            return "locator stopped";
          }
        };
        waitForCriterion(wc, 5000, 10, true);
      }
    } finally {
      if (adminDS != null) {
        adminDS.disconnect();
      }
      if (myDS != null) {
        myDS.disconnect();
      }
    }
  }
}
