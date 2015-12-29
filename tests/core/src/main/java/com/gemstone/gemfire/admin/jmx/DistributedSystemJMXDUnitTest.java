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

import dunit.*;
import java.util.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.*;
import javax.management.*;
import javax.management.modelmbean.*;

/**
 * Tests the functionality of the <code>DistributedSystem</code> admin
 * interface via JMX MBeans.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class DistributedSystemJMXDUnitTest
  extends DistributedSystemDUnitTest {

  ////////  Constructors

  /**
   * Creates a new <code>DistributedSystemJMXDUnitTest</code>
   */
  public DistributedSystemJMXDUnitTest(String name) {
    super(name);
  }

  protected boolean isJMX() {
    return true;
  }
  
  /** 
   * Perform additional testing under banner of testGetSystemMemberApplications
   * because setup for this is atrocious.
   */
  protected void subTestGetSystemMemberApplications() throws Exception {
    doTestSystemMemberCacheGetStatistics();
    doTestGetMBeanInfo();
    doTestMutationOfConfigurationParameter();
  }
  
  /** 
   * Test calls to getMBeanInfo for bug 35213.
   */
  protected void doTestGetMBeanInfo() throws Exception {
    getLogWriter().info("[doTestGetMBeanInfo]");
    SystemMember[] apps = this.tcSystem.getSystemMemberApplications();
    Collection members = arrayToCollection(apps);
    
    //getLogWriter().info("[doTestGetMBeanInfo] members: " + members);
    for (Iterator iter = members.iterator(); iter.hasNext(); ) {
      SystemMember member = (SystemMember) iter.next();
      JMXSystemMember jmxMember = (JMXSystemMember) member;
      MBeanInfo mbeanInfo = jmxMember.getMBeanInfo();
      assertNotNull(mbeanInfo);
      MBeanAttributeInfo[] attributes = mbeanInfo.getAttributes();
      assertNotNull(attributes);

      // test getters on every attribute      
      int nonNullAttributes = 0;
      for (int i = 0; i < attributes.length; i++) {
        // find out name of attribute and getAttribute from jmxMember
        ModelMBeanAttributeInfo attr = (ModelMBeanAttributeInfo) attributes[i];
        Descriptor desc = attr.getDescriptor();
        String attrName = (String) desc.getFieldValue("name");
        try {
        if (jmxMember.getAttribute(attrName) != null) {
          nonNullAttributes++;
        }
        }
        catch (javax.management.MBeanException e) {
          throw new Error("getAttribute " + attrName, e);
        }

        // make sure distributedMember and its roles deserialized fine
        if (attrName.equals("distributedMember")) {
          DistributedMember distributedMember = 
              (DistributedMember) jmxMember.getAttribute(attrName);
          //getLogWriter().info("[KIRK] DistributedMember " + distributedMember);
          assertNotNull(distributedMember);
          assertEquals(jmxMember.getDistributedMember(), distributedMember);
          Set roles = distributedMember.getRoles();
          //getLogWriter().info("[KIRK] Roles " + roles);
          assertNotNull(roles);
          assertEquals(1, roles.size());
          Role role = (Role) roles.toArray()[0];
          assertTrue(role.getName().indexOf("VM") == 0);
          String[] jmxMemberRoles = jmxMember.getRoles();
          assertEquals(jmxMemberRoles.length, roles.size());
          assertEquals(jmxMemberRoles[0], role.getName());
        }
      }
      // better have been several non-null attributes...
      assertTrue(nonNullAttributes > 0);
      
      MBeanConstructorInfo[] constructors = mbeanInfo.getConstructors();
      assertNotNull(constructors);
      for (int i = 0; i < constructors.length; i++) {
        MBeanParameterInfo[] signature = constructors[i].getSignature();
        assertNotNull(signature);
      }
      MBeanNotificationInfo[] notifications = mbeanInfo.getNotifications();
      assertNotNull(notifications);
      MBeanOperationInfo[] operations = mbeanInfo.getOperations();
      assertNotNull(operations);
      for (int i = 0; i < operations.length; i++) {
        //getLogWriter().info("[testGetMBeanInfo] operation " + operations[i]);
        MBeanParameterInfo[] signature = operations[i].getSignature();
        assertNotNull(signature);
      }
    }
  }

  /** 
   * Tests mutation of log-level on every system member. Additional validation
   * should be added when bug 35320 is fixed.
   */
  protected void doTestMutationOfConfigurationParameter() throws Exception {
    getLogWriter().info("[doTestMutationOfConfigurationParameter]");
    SystemMember[] apps = this.tcSystem.getSystemMemberApplications();
    Collection members = arrayToCollection(apps);
    
    //getLogWriter().info("[doTestMutationOfConfigurationParameter] members: " + members);
    for (Iterator iter = members.iterator(); iter.hasNext(); ) {
      SystemMember member = (SystemMember) iter.next();
      JMXSystemMember jmxMember = (JMXSystemMember) member;

      // identify jmxMember...      
      String memberName = (String) jmxMember.getAttribute("name");
      assertTrue(memberName.indexOf("VM") > -1);
      String lastCharOfMemberName = memberName.substring(memberName.length()-1);
      assertNotNull(lastCharOfMemberName);
      assertTrue(lastCharOfMemberName.length() > 0);
      int whichVM = Integer.parseInt(lastCharOfMemberName);
      VM vm = Host.getHost(0).getVM(whichVM);

      // make sure current log-level is not warning since we'll be changing it to warning
      final String oldLogLevel = (String) jmxMember.getAttribute("log-level");
      assertFalse("warning".equals(oldLogLevel));

      // modify log level
      //getLogWriter().info("[doTestMutationOfConfigurationParameter] oldLogLevel=" + oldLogLevel);
      jmxMember.setAttribute("log-level", "warning");
      try {
        
      // verify log level... can find VM# from suffix of name attribute
      assertEquals("warning", jmxMember.getAttribute("log-level"));
      
      // have that DUnit VM verify that the config changed for it
      vm.invoke(new SerializableRunnable("Validate log-level changed") {
        public void run() {
          // validate config level of LogWriter
          InternalDistributedSystem sys = getSystem();
          Config conf = sys.getConfig();
          Object value = conf.getAttributeObject("log-level");
          assertEquals("warning", value);
          
          // validate runtime level of LogWriter (requires bugfix 35320)
          LogWriter logWriter = sys.getLogWriter();
          assertTrue(logWriter.warningEnabled());
          assertFalse(logWriter.infoEnabled());
        }
      });
      
      }
      finally {
      // restore log level
      jmxMember.setAttribute("log-level", oldLogLevel);
      vm.invoke(new SerializableRunnable("Validate log-level restored") {
        public void run() {
          // validate config level of LogWriter
          InternalDistributedSystem sys = getSystem();
          Config conf = sys.getConfig();
          Object value = conf.getAttributeObject("log-level");
          assertEquals(oldLogLevel, value);
          
          // validate runtime level of LogWriter (requires bugfix 35320)
          LogWriter logWriter = sys.getLogWriter();
          assertTrue(logWriter.warningEnabled());
          assertTrue(logWriter.infoEnabled());
        }
      });
      }
    }
  }

  /** 
   * Test calls to SystemMemberCache.getStatistics for bug 35213.
   */
  protected void doTestSystemMemberCacheGetStatistics() throws Exception {
    getLogWriter().info("[doTestSystemMemberCacheGetStatistics]");
    SystemMember[] apps = this.tcSystem.getSystemMemberApplications();
    Collection members = arrayToCollection(apps);
    
    int cacheCount = 0;
    for (Iterator iter = members.iterator(); iter.hasNext(); ) {
      SystemMember member = (SystemMember) iter.next();
      JMXSystemMember jmxMember = (JMXSystemMember) member;
      
      // get SystemMemberCache
      JMXSystemMemberCache jmxCache = (JMXSystemMemberCache) jmxMember.getCache();
      if (jmxCache != null) {
        cacheCount++;
      
        // getStatistics
        Statistic[] stats = jmxCache.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.length > 0);
        for (int i = 0; i < stats.length; i++) {
          Statistic stat = stats[i];
          assertNotNull(stat);
          String name = stat.getName();
          assertNotNull(name);
          Number value = stat.getValue();
          assertNotNull(value);
          String units = stat.getUnits();
          assertNotNull(units);
//          boolean isCounter = stat.isCounter();
          String desc = stat.getDescription();
          assertNotNull(desc);
        }
      }
    }
    
    assertTrue(cacheCount > 0);
  }

  private Collection arrayToCollection(SystemMember[] members) {
    Collection collection = new ArrayList();
    collection.addAll(Arrays.asList(members));
    return collection;
  }
  
}

