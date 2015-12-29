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
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.Assert;

import java.util.*;
import javax.management.*;

/**
 * An implementation of the <code>SystemMemberCache</code> interface
 * that communicates via JMX.
 *
 * @author David Whitlock
 * @since 3.5
 */
class JMXSystemMemberCache extends JMXAdminImpl
  implements SystemMemberCache {

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>JMXSystemMemberCache</code> with the given
   * MBean server and object name.
   */
  JMXSystemMemberCache(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  public String getName() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "name");
    } catch (Exception ex) {
      String s = "While calling getName on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getId() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "id")).intValue();
    } catch (Exception ex) {
      String s = "While calling getId on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public boolean isClosed() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "closed")).booleanValue();
    } catch (Exception ex) {
      String s = "While calling isClosed on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getLockTimeout() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "lockTimeout")).intValue();
    } catch (Exception ex) {
      String s = "While calling getLockTimeout on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setLockTimeout(int seconds) 
    throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("lockTimeout", new Integer(seconds)));

    } catch (MBeanException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof AdminException) {
        throw (AdminException) cause;

      } else {
        String s = "While calling setLockTimeout on: "+objectName;
        throw new InternalGemFireException(s, ex);
      }

    } catch (Exception ex) {
      String s = "While calling setLockTimeout on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getLockLease() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "lockLease")).intValue();
    } catch (Exception ex) {
      String s = "While calling getLockLease on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setLockLease(int seconds) 
    throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("lockLease", new Integer(seconds)));
    } catch (MBeanException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof AdminException) {
        throw (AdminException) cause;

      } else {
        String s = "While calling setLockLease on: "+objectName;
        throw new InternalGemFireException(s, ex);
      }

    } catch (Exception ex) {
      String s = "While calling setLockLease on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getSearchTimeout() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "searchTimeout")).intValue();
    } catch (Exception ex) {
      String s = "While calling getSearchTimeout on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setSearchTimeout(int seconds) 
    throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("searchTimeout", new Integer(seconds)));

    } catch (MBeanException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof AdminException) {
        throw (AdminException) cause;

      } else {
        String s = "While calling setSearchTimeout on: "+objectName;
        throw new InternalGemFireException(s, ex);
      }

    } catch (Exception ex) {
      String s = "While calling setSearchTimeout on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getUpTime() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "upTime")).intValue();
    } catch (Exception ex) {
      String s = "While calling upTime on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public Set getRootRegionNames() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (Set) this.mbs.getAttribute(this.objectName, "rootRegionNames");
    } catch (Exception ex) {
      String s = "While calling rootRegionNames on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public Statistic[] getStatistics() {
    try {
      Statistic[] stats = (Statistic[]) 
        this.mbs.invoke(this.objectName, "getStatistics",
                        new Object[0], new String[0]);
      return stats;
      
    } catch (MBeanException ex) {
      String s = "While calling getStatistics on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling getStatistics on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public SystemMemberRegion getRegion(String path)
    throws AdminException {
    try {
      ObjectName name = (ObjectName) 
        this.mbs.invoke(this.objectName, "manageRegion",
                        new Object[] { path },
                        new String[] { String.class.getName() });
      return new JMXSystemMemberRegion(this.mbs, name);
      
    } catch (MBeanException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof AdminException) {
        if (cause.getMessage().indexOf("does not contain") != -1) {
          return null;
        }

      } else if (cause instanceof RegionNotFoundException) {
        return null;

      } else if (cause instanceof RegionDestroyedException) {
        return null;
      } 

      String s = "While calling manageRegion on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling manageRegion on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public SystemMemberRegion createRegion(String name,
                                           RegionAttributes attrs)
    throws AdminException {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  public SystemMemberRegion createVMRegion(String name,
                                         RegionAttributes attrs)
    throws AdminException {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void refresh() {
    try {
      this.mbs.invoke(this.objectName, "refresh",
                      new Object[0], new String[0]);
      
    } catch (Exception ex) {
      String s = "While calling refresh() on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public SystemMemberBridgeServer addBridgeServer()
    throws AdminException {

    try {
      ObjectName name = (ObjectName) 
        this.mbs.invoke(this.objectName, "manageBridgeServer",
                        new Object[0], new String[0]);
      Assert.assertTrue(name != null);
      return new JMXSystemMemberBridgeServer(this.mbs, name);
      
    } catch (MBeanException ex) {
      String s = "While calling manageBridgeServer on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling manageBridgeServer on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }
  public SystemMemberCacheServer addCacheServer()
    throws AdminException {

    try {
      ObjectName name = (ObjectName) 
        this.mbs.invoke(this.objectName, "manageCacheServer",
                        new Object[0], new String[0]);
      Assert.assertTrue(name != null);
      return new JMXSystemMemberBridgeServer(this.mbs, name);
      
    } catch (MBeanException ex) {
      String s = "While calling manageCacheServer on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling manageCacheServer on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public SystemMemberBridgeServer[] getBridgeServers()
    throws AdminException {

    try {
      ObjectName[] names = (ObjectName[]) 
        this.mbs.invoke(this.objectName, "manageBridgeServers",
                        new Object[0], new String[0]);
      SystemMemberBridgeServer[] bridges =
        new SystemMemberBridgeServer[names.length];
      for (int i = 0; i < names.length; i++) {
        Assert.assertTrue(names[i] != null);
        bridges[i] =
          new JMXSystemMemberBridgeServer(this.mbs, names[i]);
      }

      return bridges;
      
    } catch (MBeanException ex) {
      String s = "While calling manageBridgeServers on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling manageBridgeServers on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }
  public SystemMemberCacheServer[] getCacheServers()
    throws AdminException {

    try {
      ObjectName[] names = (ObjectName[]) 
        this.mbs.invoke(this.objectName, "manageCacheServers",
                        new Object[0], new String[0]);
      SystemMemberCacheServer[] bridges =
        new SystemMemberCacheServer[names.length];
      for (int i = 0; i < names.length; i++) {
        Assert.assertTrue(names[i] != null);
        bridges[i] =
          new JMXSystemMemberBridgeServer(this.mbs, names[i]);
      }

      return bridges;
      
    } catch (MBeanException ex) {
      String s = "While calling manageCacheServers on: "+objectName;
      throw new InternalGemFireException(s, ex);

    } catch (RuntimeException ex) {
      throw ex;

    } catch (Exception ex) {
      String s = "While calling manageCacheServers on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public boolean isServer() throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "server")).booleanValue();
    } catch (Exception ex) {
      String s = "While calling isServer on: "+objectName;
      throw new InternalGemFireException(s, ex);
    }
  }

}
