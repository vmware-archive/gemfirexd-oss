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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.Assert;
//import java.util.Properties;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.*;

/**
 * An implementation of <code>SystemMember</code> that
 * communicates via JMX.
 *
 * @author Kirk Lund
 */
class JMXSystemMember extends JMXAdminImpl
  implements SystemMember {

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>JMXSystemMember</code> with the given MBean
   * server and object name.
   */
  JMXSystemMember(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  public AdminDistributedSystem getDistributedSystem() {
    throw new UnsupportedOperationException("not yet implemented");
  }
  
  // ---------------------- ATTRIBUTES -----------------------------
  
  public String getId() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "id");
    } catch (Exception ex) {
      String s = "While calling getId";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public String getName() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "name");
    } catch (Exception ex) {
      String s = "While getting the name";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public SystemMemberType getType() {
    return SystemMemberType.APPLICATION;
  }
  
  public String getHost() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "host");
    } catch (Exception ex) {
      String s = "While calling getHost";
      throw new InternalGemFireException(s, ex);
    }
  }

  public InetAddress getHostAddress() {
    try {
      return InetAddress.getByName(getHost());
    } catch (Exception ex) {
      String s = "While calling getHost";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public boolean isRunning() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "running")).booleanValue();

    } catch (Exception ex) {
      String s = "While calling getLog";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public String getVersion() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "version");
    } catch (Exception ex) {
      String s = "While calling getVersion";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public DistributedMember getDistributedMember() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (DistributedMember) this.mbs.getAttribute(this.objectName, "distributedMember");
    } catch (Exception ex) {
      String s = "While calling getDistributedMember";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  // ---------------------- OPERATIONS -----------------------------
  
  public String[] getRoles() {
    try {
      return (String[]) this.mbs.invoke(this.objectName, "getRoles",
                                        new Object[0], new String[0]);
    } catch (Exception ex) {
      String s = "While calling getRoles";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public String getLog() {
    try {
      return (String) this.mbs.invoke(this.objectName, "getLog",
                                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While calling getLog";
      throw new InternalGemFireException(s, ex);
    }
  }

  public java.util.Properties getLicense() {
    try {
      return (java.util.Properties) this.mbs.invoke(this.objectName, "getLicense",
                                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While calling getLicense";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public ConfigurationParameter[] getConfiguration() {
    throw new UnsupportedOperationException("not yet implemented");
  }
  
  public ConfigurationParameter[] setConfiguration(ConfigurationParameter[] parms) throws com.gemstone.gemfire.admin.AdminException {
    throw new UnsupportedOperationException("not yet implemented");
  }
  
  public void refreshConfig() throws com.gemstone.gemfire.admin.AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.invoke(this.objectName, "refreshConfig",
                                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While calling refreshConfig";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public StatisticResource[] getStats() throws AdminException {
    try {
      ObjectName[] names = (ObjectName[]) 
          this.mbs.invoke(this.objectName, "manageStats",
                          new Object[0], new String[0]);
                          
      StatisticResource[] stats = new StatisticResource[names.length]; 
      for (int i = 0; i < names.length; i++) {
        stats[i] = new JMXStatisticResource(this.mbs, names[i]);
      }
      return stats;
      
    } catch (Exception ex) {
      String s = "While calling manageStats";
      throw new InternalGemFireException(s, ex);
    }
  }

  public StatisticResource[] getStat(String statisticsTypeName) throws AdminException {
    try {
      String[] params = {"DistributionStats"};
      ObjectName[] names = (ObjectName[]) 
          this.mbs.invoke(this.objectName, "manageStat",
                                        params, new String[0]);
                          
      StatisticResource[] stat = new JMXStatisticResource[names.length];
      for (int i=0; i<names.length; i++) {
        stat[i] = new JMXStatisticResource(this.mbs, names[i]);
      }

      return stat;
    } catch (Exception ex) {
      String s = "While calling manageStats";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public boolean hasCache() throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "hasCache")).booleanValue();
    } catch (Exception ex) {
      String s = "While calling getVersion";
      throw new InternalGemFireException(s, ex);
    }
  }

  public SystemMemberCache getCache() throws AdminException {
    if (!hasCache()) {
      // Maintain SystemMember semantics
      return null;
    }

    try {
      Assert.assertTrue(this.objectName != null);
      ObjectName name = (ObjectName) 
        this.mbs.invoke(this.objectName, "manageCache",
                        new Object[0], new String[0]);
      Assert.assertTrue(name != null);
      return new JMXSystemMemberCache(this.mbs, name);
      
    } catch (Exception ex) {
      String s = "While calling getCache";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  // -------------------------------------------------------------------------
  //                         JMX Specific Operations
  // -------------------------------------------------------------------------
  
  public MBeanInfo getMBeanInfo() 
  throws javax.management.JMException, java.io.IOException {
    Assert.assertTrue(this.objectName != null);
      return this.mbs.getMBeanInfo(this.objectName);
  }
  
  public Object getAttribute(String attribute) 
  throws javax.management.JMException, java.io.IOException {
    Assert.assertTrue(this.objectName != null);
    return this.mbs.getAttribute(this.objectName, attribute);
  }
  
  public void setAttribute(String attribute, Object value) 
  throws javax.management.JMException, java.io.IOException {
    Attribute wrapper = new Attribute(attribute, value);
    Assert.assertTrue(this.objectName != null);
    this.mbs.setAttribute(this.objectName, wrapper);
  }
  
  public String toString() {
    return getName() + " " + getId();
  }
}

