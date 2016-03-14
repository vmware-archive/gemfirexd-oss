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

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.*;

import junit.framework.TestCase;

/**
 * Unit tests for LocalProcessController.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public class LocalProcessControllerJUnitTest extends TestCase {

  private static final String TEST_CASE = "LocalProcessControllerJUnitTest";

  public LocalProcessControllerJUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testProcessMBean() throws Exception {
    final String testName = "testProcessMBean";
    final int pid = ProcessUtils.identifyPid();
    final Process process = new Process(pid, true);
    final ObjectName objectName = ObjectName.getInstance(
        getClass().getSimpleName() + ":testName=" + testName);
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final ObjectInstance instance = server.registerMBean(process, objectName);
    assertNotNull(instance);
    try {
      // validate basics of the ProcessMBean
      Set<ObjectName> mbeanNames = server.queryNames(objectName, null);
      assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
      assertEquals(1, mbeanNames.size());
      final ObjectName name = mbeanNames.iterator().next();
      
      final MBeanInfo info = server.getMBeanInfo(name);
      
      final MBeanOperationInfo[] operInfo = info.getOperations();
      assertEquals(1, operInfo.length);
      assertEquals("stop", operInfo[0].getName());
      
      final MBeanAttributeInfo[] attrInfo = info.getAttributes();
      assertEquals(2, attrInfo.length);
      // The order of these attributes is indeterminate
//      assertEquals("Pid", attrInfo[0].getName());
//      assertEquals("Process", attrInfo[1].getName());
      assertNotNull(server.getAttribute(name, "Pid"));
      assertNotNull(server.getAttribute(name, "Process"));
      
      assertEquals(pid, server.getAttribute(name, "Pid"));
      assertEquals(true, server.getAttribute(name, "Process"));

      // validate query using only Pid attribute
      QueryExp constraint = Query.eq(
          Query.attr("Pid"),
          Query.value(pid));
      mbeanNames = server.queryNames(objectName, constraint);
      assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
      
      // validate query with wrong Pid finds nothing
      constraint = Query.eq(
          Query.attr("Pid"),
          Query.value(pid+1));
      mbeanNames = server.queryNames(objectName, constraint);
      assertTrue("Found matching mbeans", mbeanNames.isEmpty());
      
      // validate query using both attributes
      constraint = Query.and(
          Query.eq(Query.attr("Process"),Query.value(true)),
          Query.eq(Query.attr("Pid"),Query.value(pid)));
      mbeanNames = server.queryNames(objectName, constraint);
      assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
      
      // validate query with wrong attribute finds nothing
      constraint = Query.and(
          Query.eq(Query.attr("Process"),Query.value(false)),
          Query.eq(Query.attr("Pid"),Query.value(pid)));
      mbeanNames = server.queryNames(objectName, constraint);
      assertTrue("Found matching mbeans", mbeanNames.isEmpty());
      
    } finally {
      try {
        server.unregisterMBean(objectName);
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  public interface ProcessMBean {
    int getPid();
    boolean isProcess();
    void stop();
  }

  public static class Process implements ProcessMBean {

    private final Object object = new Object();
    private final int pid;
    private final boolean process;
    private volatile boolean stop;

    public Process(int pid, boolean process) {
      this.pid = pid;
      this.process = process;
    }

    @Override
    public int getPid() {
      return this.pid;
    }

    public boolean isProcess() {
      return this.process;
    }

    @Override
    public void stop() {
      synchronized (this.object) {
        this.stop = true;
        this.object.notifyAll();
      }
    }

    public void waitUntilStopped() throws InterruptedException {
      synchronized (this.object) {
        System.out.println(TEST_CASE + " process running");
        while (!this.stop) {
          this.object.wait();
        }
        System.out.println(TEST_CASE + " process stopped");
      }
    }
  }
}
