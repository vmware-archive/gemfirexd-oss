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
import com.gemstone.gemfire.internal.Assert;
//import java.util.Properties;
import javax.management.*;

/**
 * An implementation of <code>CacheVm</code> that communicates via
 * JMX.
 *
 * @author David Whitlock
 * @since 4.0
 */
class JMXCacheServer extends JMXSystemMember 
  implements CacheVm, CacheServer {

  /**
   * Creates a new <code>JMXCacheServer</code. with the given MBean
   * server and object name.
   */
  JMXCacheServer(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(objectName != null);
  }

  public SystemMemberType getType() {
    return SystemMemberType.CACHE_VM;
  }
  
  public CacheServerConfig getConfig() {
    Assert.assertTrue(this.objectName != null);
    return new JMXCacheServerConfig(this.mbs, this.objectName);
  }
  public CacheVmConfig getVmConfig() {
    Assert.assertTrue(this.objectName != null);
    return new JMXCacheServerConfig(this.mbs, this.objectName);
  }

  public void start() {     
    try {
      this.mbs.invoke(this.objectName, "start",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While starting";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void stop() {     
    try {
      this.mbs.invoke(this.objectName, "stop",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While stopping";
      throw new InternalGemFireException(s, ex);
    }
  }

  public boolean waitToStart(long timeout) {
    try {
      Boolean b = (Boolean)
        this.mbs.invoke(this.objectName, "waitToStart",
                        new Object[] { new Long(timeout) },
                        new String[] { long.class.getName() });
      return b.booleanValue();

    } catch (Exception ex) {
      String s = "While waiting to start";
      throw new InternalGemFireException(s, ex);
    }
  }

  public boolean waitToStop(long timeout) {
    try {
      Boolean b = (Boolean)
        this.mbs.invoke(this.objectName, "waitToStop",
                        new Object[] { new Long(timeout) },
                        new String[] { long.class.getName() });
      return b.booleanValue();

    } catch (Exception ex) {
      String s = "While waiting to stop";
      throw new InternalGemFireException(s, ex);
    }
  }

  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * A <code>CacheVmConfig</code> that lets JMX do its work.
   */
  static class JMXCacheServerConfig extends JMXManagedEntityConfig
    implements CacheVmConfig, CacheServerConfig {

    JMXCacheServerConfig(MBeanServerConnection mbs,
                         ObjectName objectName) {
      super(mbs, objectName);
      Assert.assertTrue(objectName != null);
    }

    public String getCacheXMLFile() {
      try {
        Assert.assertTrue(this.objectName != null);
        return (String) this.mbs.getAttribute(this.objectName, "cacheXMLFile");
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public void setCacheXMLFile(String cacheXml) {
      try {
        Assert.assertTrue(this.objectName != null);
        this.mbs.setAttribute(this.objectName,
                              new Attribute("cacheXMLFile", cacheXml));
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public String getClassPath() {
      try {
        Assert.assertTrue(this.objectName != null);
        return (String) this.mbs.getAttribute(this.objectName, "classPath");

      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public void setClassPath(String classpath) {
      try {
        Assert.assertTrue(this.objectName != null);
        this.mbs.setAttribute(this.objectName,
                              new Attribute("classPath", classpath));
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

  }

  /**
   * Not implemented yet.
   * 
   * @since 5.6
   */
  public boolean isPrimaryForDurableClient(String durableClientId)
  {
    throw new UnsupportedOperationException("Not yet implemented");
}
}
