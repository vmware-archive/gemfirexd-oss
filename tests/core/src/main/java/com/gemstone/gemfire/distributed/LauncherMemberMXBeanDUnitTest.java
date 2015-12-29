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

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.MemberMXBean;

/**
 * Tests querying of MemberMXBean which is used by MBeanProcessController to
 * control GemFire ControllableProcesses.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
@SuppressWarnings("serial")
public class LauncherMemberMXBeanDUnitTest extends AbstractLauncherDUnitTestCase {

  public LauncherMemberMXBeanDUnitTest(String name) {
    super(name);
  }

  @Override
  protected void subSetUp() throws Exception {
  }

  @Override
  protected void subTearDown() throws Exception {
  }

  public void testQueryForMemberMXBean() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    getSystem(props);
    getCache();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");
    
    final WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, null);
        return !mbeanNames.isEmpty();
      }
      public String description() {
        return "waiting for MemberMXBean to be registered";
      }
    };
    waitForCriterion(wc, 10*1000, 10, false);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, null);
    assertFalse(mbeanNames.isEmpty());
    assertEquals("mbeanNames=" + mbeanNames, 1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName,
      MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(ProcessUtils.identifyPid(), mbean.getProcessId());
    assertEquals(getUniqueName(), mbean.getName());
    assertEquals(getUniqueName(), mbean.getMember());
  }
  
  public void testQueryForMemberMXBeanWithProcessId() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    getSystem(props);
    getCache();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");
    final QueryExp constraint = Query.eq(Query.attr("ProcessId"),Query.value(ProcessUtils.identifyPid()));
    
    final WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
        return !mbeanNames.isEmpty();
      }
      public String description() {
        return "waiting for MemberMXBean to be registered";
      }
    };
    waitForCriterion(wc, 10*1000, 10, false);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
    assertFalse(mbeanNames.isEmpty());
    assertEquals(1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName,
      MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(ProcessUtils.identifyPid(), mbean.getProcessId());
    assertEquals(getUniqueName(), mbean.getName());
    assertEquals(getUniqueName(), mbean.getMember());
  }
  
  public void testQueryForMemberMXBeanWithMemberName() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    getSystem(props);
    getCache();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");
    final QueryExp constraint = Query.eq(Query.attr("Name"), Query.value(getUniqueName()));
    
    final WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
        return !mbeanNames.isEmpty();
      }
      public String description() {
        return "waiting for MemberMXBean to be registered";
      }
    };
    waitForCriterion(wc, 10*1000, 10, false);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
    assertFalse(mbeanNames.isEmpty());
    assertEquals(1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(getUniqueName(), mbean.getMember());
  }
}
