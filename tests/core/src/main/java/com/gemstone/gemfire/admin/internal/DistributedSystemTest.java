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

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
//import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.admin.jmx.internal.AgentConfigImpl;
//import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import com.gemstone.org.jgroups.stack.IpAddress;
//import com.gemstone.gemfire.distributed.internal.DistributionChannel;
//import com.gemstone.org.jgroups.JChannel;
//import com.gemstone.gemfire.distributed.internal.direct.DirectChannel;

//import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.admin.*;

//import java.net.InetAddress;
import java.util.*;
import junit.framework.*;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.DistributedSystemImpl}.
 *
 * @author Kirk Lund
 * @since 3.5
 */
public class DistributedSystemTest extends DistributedSystemTestCase {

  /**
   * Creates a new <code>DistributedSystemTest</code>
   */
  public DistributedSystemTest(String name) {
    super(name);
  }

  public void testConnect() throws Exception {
    assertEquals(true, this.system.isConnected());
    com.gemstone.gemfire.distributed.internal.DistributionConfig distConfig = 
        ((InternalDistributedSystem)this.system).getConfig();
    
    // create a loner distributed system and make sure it is connected...
    String locators = distConfig.getLocators();
    String mcastAddress = InetAddressUtil.toString(distConfig.getMcastAddress());
    int mcastPort = distConfig.getMcastPort();
    
    // Because of fix for bug 31409
    this.system.disconnect();

    com.gemstone.gemfire.admin.DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem();
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
          
      boolean passed = false;
      int tries = 3;
      for (int i = 0; i < tries && !passed; i++) {
        try {          
          assertTrue(distSys.isConnected());
          //assertTrue(distSys.isRunning());
          passed = true;
        }
        catch (AssertionFailedError e) {
          // if last try...
          if (i < tries-1) {
            throw e;
          }
        }
      }

    }
    finally {
      distSys.disconnect();
      assertTrue(!distSys.isConnected());
      assertTrue(!distSys.isRunning());
    }
  }
  
  /**
   * Tests that each of the JMX agent properties that reside in the
   * agent properties file has a description.
   */
  public void testAgentProperties() {
    Properties props = (new AgentConfigImpl()).toProperties();
    for (Iterator iter = props.keySet().iterator();
         iter.hasNext(); ) {
      String prop = (String) iter.next();
      AgentConfigImpl.getPropertyDescription(prop);
    }
  }

  /**
   * Tests that only one connection to the distributed system is made
   * when working with the admin API.
   *
   * @since 4.0
   */
  public void testAdministerDS() throws Exception {
    DistributedSystem system = this.system;
    DistributedSystemConfig config =
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    DistributedSystem system2 =
      ((AdminDistributedSystemImpl) admin).getAdminAgent().getDSConnection();
    assertSame(system, system2);
    admin.disconnect();
  }
  
}

