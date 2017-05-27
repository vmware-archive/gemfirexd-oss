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
package com.gemstone.gemfire.admin.internal;
import com.gemstone.gemfire.admin.*;
//import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
//import com.gemstone.gemfire.distributed.internal.DistributionManager;
//import com.gemstone.gemfire.distributed.internal.DistributionChannel;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

//import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;
//import com.gemstone.gemfire.distributed.internal.membership.*;

import com.gemstone.gemfire.internal.AvailablePort;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

import java.net.InetAddress;

import java.net.Socket;
import java.util.*;
import junit.framework.*;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl}.
 *
 * @author    Kirk Lund
 * @created   August 30, 2004
 * @since     3.5
 */
public class BindDistributedSystemTest extends TestCase {

  private final static int RETRY_ATTEMPTS = 3;
  private final static int RETRY_SLEEP = 100;

  /**
   * Description of the Field
   */
  protected com.gemstone.gemfire.distributed.DistributedSystem system;

  /**
   * Creates a new <code>DistributedSystemTest</code>
   *
   * @param name  Description of the Parameter
   */
  public BindDistributedSystemTest(String name) {
    super(name);
  }

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   *
   * @exception Exception  Description of the Exception
   */
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   *
   * @exception Exception  Description of the Exception
   */
  public void tearDown() throws Exception {
    super.tearDown();
  }

//  public void testBindToAddressSuccessive() throws Exception {
//    testBindToAddressLocalHost();
//    testBindToAddressNull();
//    testBindToAddressLocalHost();
//  }
//
//  public void testBindToAddressNull() throws Exception {
//    DistributedSystemFactory.bindToAddress(null);
//     todo...
//  }
//
//  public void testBindToAddressEmpty() throws Exception {
//    DistributedSystemFactory.bindToAddress("");
//     todo...
//  }
//
  /**
   * A unit test for JUnit
   *
   * @exception Exception  Description of the Exception
   */
  public void testBindToAddressLoopback() throws Exception {
    String bindTo = "127.0.0.1";
    // make sure bindTo is the loopback... needs to be later in test...
    assertEquals(true, InetAddressUtil.isLoopback(bindTo));

    //System.setProperty("gemfire.jg-bind-address", bindTo);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.BIND_ADDRESS_NAME, bindTo);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,
        String.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)));
    this.system = com.gemstone.gemfire.distributed.DistributedSystem.connect(
        props);
        
    try {
      assertEquals(true, this.system.isConnected());
  
      // analyze the system's config...
      DistributionConfig distConfig =
          ((InternalDistributedSystem) this.system).getConfig();
  
      // create a loner distributed system and make sure it is connected...
      String locators = distConfig.getLocators();
      String mcastAddress = InetAddressUtil.toString(distConfig.getMcastAddress());
      int mcastPort = distConfig.getMcastPort();

      // Because of fix for bug 31409
      this.system.disconnect();

      checkAdminAPI(bindTo, locators, mcastAddress, mcastPort);
    }
    finally {
      System.out.println(">>DONE<<");
      if (this.system != null) {
        this.system.disconnect();
      }
      this.system = null;
    }
  }

  /**
   * Description of the Method
   *
   * @param bindTo         Description of the Parameter
   * @param locators       Description of the Parameter
   * @param mcastAddress   Description of the Parameter
   * @param mcastPort      Description of the Parameter
   * @exception Exception  Description of the Exception
   */
  protected void checkAdminAPI(String bindTo,
      String locators,
      String mcastAddress,
      int mcastPort) throws Exception {

    // build the config that defines the system...
    com.gemstone.gemfire.admin.DistributedSystemConfig config =
      AdminDistributedSystemFactory.defineDistributedSystem();
    config.setBindAddress(bindTo);
    config.setLocators(locators);
    config.setMcastAddress(mcastAddress);
    config.setMcastPort(mcastPort);
    config.setRemoteCommand(com.gemstone.gemfire.admin.DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
    assertNotNull(config);

    AdminDistributedSystem distSys =
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);

    // connect to the system...
    distSys.connect();
    try {
      checkSystemIsRunning(distSys);

//      DistributionManager dm =
//          ((AdminDistributedSystemImpl) distSys).getDistributionManager();
//      checkDistributionPorts(dm, bindTo);

      // check gemfire.jg-bind-address...
      if (true) {// kill this block if we stop using gemfire.jg-bind-address
        // validate the bindAddress using gemfire.jg-bind-address...
        String bindAddress = System.getProperty("gemfire.jg-bind-address");
        InetAddress address = InetAddress.getByName(bindAddress);
        assertNotNull(address);
        assertEquals(InetAddress.getByName(bindTo), address);
      }

    }
    finally {
      distSys.disconnect();
    }
  }

  /**
   * Description of the Method
   *
   * @param distSys        Description of the Parameter
   * @exception Exception  Description of the Exception
   */
  public void checkSystemIsRunning(final AdminDistributedSystem distSys) throws Exception {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return distSys.isConnected() || distSys.isRunning();
      }
      public String description() {
        return "distributed system is either connected or running: " + distSys;
      }
    };
    DistributedTestBase.waitForCriterion(ev, 120 * 1000, 200, true);
    assertTrue(distSys.isConnected());
    //assertTrue(distSys.isRunning());
  }

//  /**
//   * Description of the Method
//   *
//   * @param cdm            Description of the Parameter
//   * @param bindToAddress  Description of the Parameter
//   * @exception Exception  Description of the Exception
//   */
//  protected void checkDistributionPorts(DistributionManager dm,
//      String bindToAddress) throws Exception {
//    assertNotNull(dm);
//    DistributionChannel distChannel = dm.getDistributionChannel();
//
//    // now query the Channel...
//    DirectChannel dc = distChannel.getDirectChannel();
////    MembershipManager jc = distChannel.getMembershipManager();
//
//    // get Jgroups IpAddress and retrieve its ports...
//    DistributedMember ipAddress = dc.getLocalAddress();
//    InetAddress jgAddr = ipAddress.getIpAddress();
////    int mcastPort = ipAddress.getPort();
//    int directPort = ipAddress.getDirectChannelPort();
//    int[] portsToCheck = new int[]{directPort};
//
//    // check the loopback first...
//    assertEquals(InetAddress.getByName(bindToAddress), jgAddr);
//    checkInetAddress(jgAddr, portsToCheck, true);
//
//    // check all non-loopback (127.0.0.1) interfaces...
//    // note: if this fails then this machine probably lacks a NIC
//    InetAddress host = InetAddress.getLocalHost();
//    assertEquals(false, InetAddressUtil.isLoopback(host));
//    InetAddress[] hostAddrs = InetAddress.getAllByName(host.getHostName());
//    for (int i = 0; i < hostAddrs.length; i++) {
//      if (!InetAddressUtil.isLoopback(hostAddrs[i])) {
//        // make sure sure non-loopback is not listening to ports...
//        checkInetAddress(hostAddrs[i], portsToCheck, false);
//      }
//    }
//  }

  /**
   * Description of the Method
   *
   * @param addr           Description of the Parameter
   * @param ports          Description of the Parameter
   * @param isListening    Description of the Parameter
   * @exception Exception  Description of the Exception
   */
  public static void checkInetAddress(InetAddress addr,
      int[] ports,
      boolean isListening) throws Exception {
    for (int i = 0; i < ports.length; i++) {
      System.out.println("addr = " + addr);
      System.out.println("ports[i] = " + ports[i]);
      System.out.println("isListening = " + isListening);
      assertEquals(isListening, isPortListening(addr, ports[i]));
    }
  }

  /**
   * Gets the portListening attribute of the BindDistributedSystemTest class
   *
   * @param addr           Description of the Parameter
   * @param port           Description of the Parameter
   * @return               The portListening value
   * @exception Exception  Description of the Exception
   */
  public static boolean isPortListening(InetAddress addr, int port) throws Exception {
    // Try to create a Socket
    Socket client = null;
    try {
      client = new Socket(addr, port);
      return true;
    }
    catch (Exception e) {
      return false;
    }
    finally {
      if (client != null) {
        try {
          client.close();
        }
        catch (Exception e) {
          /*
           *  ignore
           */
        }
      }
    }
  }

}

