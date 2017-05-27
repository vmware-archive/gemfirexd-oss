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

//import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
//import com.gemstone.gemfire.internal.LocalLogWriter;
//import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import com.gemstone.org.jgroups.stack.IpAddress;
//import com.gemstone.gemfire.distributed.internal.DistributionChannel;
//import com.gemstone.org.jgroups.JChannel;
//import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import junit.framework.*;

//import java.net.InetAddress;
//import java.util.*;


/**
 * Tests {@link com.gemstone.gemfire.admin.AdminDistributedSystemFactory}.
 *
 * @author Kirk Lund
 * @since 3.5
 */
public class DistributedSystemFactoryTest extends TestCase {

  /**
   * Creates a new <code>DistributedSystemFactoryTest</code>
   */
  public DistributedSystemFactoryTest(String name) {
    super(name);
  }

  /**
   * Tests <code>defineDistributedSystem(String locators)</code>.
   *
   * @see AdminDistributedSystemFactory#defineDistributedSystem()
   */
  public void testDefineDistributedSystemLocators() {
    String locators = "merry[8002],happy[8002]";
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastPort(0);
    config.setLocators(locators);
    
    assertNotNull(config);
    assertEquals(locators, 
                 config.getLocators());
    assertEquals(0, 
                 config.getMcastPort());
    assertEquals(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND, 
                 config.getRemoteCommand());
  }

  public void testDefineDistributedSystemMcast() { 
    String mcastAddress = "214.0.0.240";
    int mcastPort = 10347;
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastAddress(mcastAddress); 
    config.setMcastPort(mcastPort);
        
    assertNotNull(config);
    assertEquals(mcastAddress, 
                 config.getMcastAddress());
    assertEquals(mcastPort, 
                 config.getMcastPort());
    assertEquals(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND, 
                 config.getRemoteCommand());
  }
  
  public void testDefineDistributedSystem() { 
    String locators = "";
    String mcastAddress = "215.0.0.230";
    int mcastPort = 10346;
    String remoteCommand = "myrsh -n {CMD} {HOST}";
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setLocators(locators); 
    config.setMcastAddress(mcastAddress); 
    config.setMcastPort(mcastPort); 
    config.setRemoteCommand(remoteCommand);
        
    assertNotNull(config);
    assertEquals(locators, 
                 config.getLocators());
    assertEquals(mcastAddress, 
                 config.getMcastAddress());
    assertEquals(mcastPort, 
                 config.getMcastPort());
    assertEquals(remoteCommand, 
                 config.getRemoteCommand());
  }

  // this configuration is legal in the Congo release
//  public void testDefineDistributedSystemIllegal() { 
//    String locators = "merry[8002],happy[8002]";
//    String mcastAddress = "215.0.0.230";
//    int mcastPort = 10346;
//    String remoteCommand = "myrsh -n {CMD} {HOST}";
//    
//    try {
//      DistributedSystemConfig config = 
//          AdminDistributedSystemFactory.defineDistributedSystem();
//      config.setMcastAddress(mcastAddress); 
//      config.setMcastPort(mcastPort); 
//      config.setRemoteCommand(remoteCommand);
//      config.setLocators(locators); 
//      config.validate();
//
//      fail("IllegalArgumentException should have been thrown");
//    }
//    catch (IllegalArgumentException e) {
//      // passed
//    }
//  }
  
  public void testDefineDistributedSystemIllegalPort() { 
    String mcastAddress = DistributedSystemConfig.DEFAULT_MCAST_ADDRESS;
    
    int mcastPort = DistributedSystemConfig.MAX_MCAST_PORT+1;
    try {
      DistributedSystemConfig config = 
          AdminDistributedSystemFactory.defineDistributedSystem();
      config.setMcastAddress(mcastAddress); 
      config.setMcastPort(mcastPort);
      config.validate();

      fail("mcastPort > MAX should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      // passed
    }
    
    mcastPort = DistributedSystemConfig.MIN_MCAST_PORT-1;
    try {
      DistributedSystemConfig config = 
          AdminDistributedSystemFactory.defineDistributedSystem();
      config.setMcastAddress(mcastAddress); 
      config.setMcastPort(mcastPort);
      config.validate();

      fail("mcastPort < MIN should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      // passed
    }
  }
  
  public void testGetDistributedSystem() throws Exception {
    String locators = "";
    
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    assertNotNull(config);

    config.setMcastPort(0);
    config.setLocators(locators); 
    
    AdminDistributedSystem distSys = 
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);
    distSys.disconnect();
  }

  public void testConnect() throws Exception {
    String locators = "";
    
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    assertNotNull(config);

    config.setMcastPort(0);
    config.setLocators(locators);
    config.setSystemName("testConnect");
    
    AdminDistributedSystem distSys = 
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);
    boolean origIsDedicatedAdminVM = DistributionManager.isDedicatedAdminVM;
    try {
      AdminDistributedSystemFactory.setEnableAdministrationOnly(true);
      assertTrue(DistributionManager.isDedicatedAdminVM);
      distSys.connect();
      distSys.waitToBeConnected(30000);
      checkEnableAdministrationOnly(true, true);
      checkEnableAdministrationOnly(false, true);
      
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
      assertEquals("testConnect", ids.getName());
      
      distSys.disconnect();
      checkEnableAdministrationOnly(false, false);
      checkEnableAdministrationOnly(true, false);
    } finally {
      DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
    }
  }

  public static void checkEnableAdministrationOnly(boolean v, boolean expectException) {
    boolean origIsDedicatedAdminVM = DistributionManager.isDedicatedAdminVM;
    if (expectException) {
      try {
        AdminDistributedSystemFactory.setEnableAdministrationOnly(v);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals(origIsDedicatedAdminVM, DistributionManager.isDedicatedAdminVM);
      } finally {
        DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
      }
    } else {
      try {
        AdminDistributedSystemFactory.setEnableAdministrationOnly(v);
        assertEquals(v, DistributionManager.isDedicatedAdminVM);
      } finally {
        DistributionManager.isDedicatedAdminVM = origIsDedicatedAdminVM;
      }
    }
  }
}
