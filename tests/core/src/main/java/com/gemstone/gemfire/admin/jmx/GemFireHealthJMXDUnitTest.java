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
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.admin.*;

import dunit.DistributedTestCase;
import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import com.gemstone.gemfire.admin.jmx.internal.MBeanUtil;

/**
 * Tests the functionality of the GemFire health monitoring via JMX
 * MBeans. 
 *
 * @author David Whitlock
 * @author Kirk Lund
 *
 * @since 3.5
 */
public class GemFireHealthJMXDUnitTest
  extends GemFireHealthDUnitTest {


  ////////  Constructors

  /**
   * Creates a new <code>GemFireHealthJMXDUnitTest</code>
   */
  public GemFireHealthJMXDUnitTest(String name) {
    super(name);
  }

  protected boolean isJMX() {
    return true;
  }
  
  /** Bug 31675 JMX Agent fails to re-register health mbeans */
  public void testBug31675() throws Exception {
    if (isSSL() || isRMI()) {
      DistributedTestCase.getLogWriter().info("Skipping testBug31675 in SSL and RMI enabled tests");
      return;
    }
    MBeanServerConnection mbs = this.agent.getMBeanServer();
    assertNotNull(mbs);
      
    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    ObjectName systemName = (ObjectName) 
      mbs.invoke(agentName, "manageDistributedSystem", 
                 new Object[0], new String[0]);
    assertNotNull(systemName);
    String systemId = (String) mbs.getAttribute(systemName, "id");
    systemId = MBeanUtil.makeCompliantMBeanNameProperty(systemId);
    assertNotNull(systemId);
      
    ObjectName healthName = (ObjectName) 
      mbs.invoke(systemName, "monitorGemFireHealth", 
                 new Object[0], new String[0]);
                          
    ObjectName systemHealthConfigName = new ObjectName(
                                                       "GemFire:type=DistributedSystemHealthConfig,id=" + systemId);
    ObjectName defaultHealthConfigName = new ObjectName(
                                                        "GemFire:type=GemFireHealthConfig,id=" + systemId + ",host=default");
          
    assertTrue(mbs.isRegistered(healthName));
    assertTrue(mbs.isRegistered(systemHealthConfigName));
    assertTrue(mbs.isRegistered(defaultHealthConfigName));
      
    { // make sure the health mbean does not say it is closed
      this.tcSystem.getGemFireHealth().getHealth();
    }

    mbs.unregisterMBean(healthName);
    mbs.unregisterMBean(systemHealthConfigName);
    mbs.unregisterMBean(defaultHealthConfigName);
  
    assertTrue(!mbs.isRegistered(healthName));
    assertTrue(!mbs.isRegistered(systemHealthConfigName));
    assertTrue(!mbs.isRegistered(defaultHealthConfigName));
      
    healthName = (ObjectName) 
      mbs.invoke(systemName, "monitorGemFireHealth", 
                 new Object[0], new String[0]);
                          
    assertTrue(mbs.isRegistered(healthName));
    assertTrue(mbs.isRegistered(systemHealthConfigName));
    assertTrue(mbs.isRegistered(defaultHealthConfigName));
    { // make sure the health mbean does not say it is closed
      this.tcSystem.getGemFireHealth().getHealth();
    }
  }
  
}

