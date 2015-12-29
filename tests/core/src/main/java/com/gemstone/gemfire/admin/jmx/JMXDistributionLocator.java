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

import javax.management.*;

/**
 * An implementation of <code>DistributionLocator</code> that
 * communicates via JMX.
 *
 * @author David Whitlock
 * @since 4.0
 */
class JMXDistributionLocator extends JMXAdminImpl
  implements DistributionLocator {

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>JMXDistributionLocator</code> with the given
   * MBean server and object name.
   */
  JMXDistributionLocator(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  /////////////////////  Instance Methods  /////////////////////

  public void start() throws AdminException {
    try {
      this.mbs.invoke(this.objectName, "start",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While starting";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void stop() throws AdminException {
    try {
      this.mbs.invoke(this.objectName, "stop",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While stopping";
      throw new InternalGemFireException(s, ex);
    }
  }

  public boolean waitToStart(long timeout)
    throws InterruptedException {
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

  public boolean waitToStop(long timeout)
    throws InterruptedException {
    try {
      Boolean b = (Boolean)
        this.mbs.invoke(this.objectName, "waitToStop",
                        new Object[] { new Long(timeout) },
                        new String[] { long.class.getName() });
      return b.booleanValue();

    } catch (Exception ex) {
      String s = "While waiting to start";
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

  public String getLog() throws AdminException {
    try {
      return (String) this.mbs.invoke(this.objectName, "getLog",
                                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While calling getLog";
      throw new InternalGemFireException(s, ex);
    }
  }

  public String getId() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "id");

    } catch (Exception ex) {
      String s = "While calling getLog";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public DistributionLocatorConfig getConfig() {
    Assert.assertTrue(this.objectName != null);
    return new JMXDistributionLocatorConfig(this.mbs, this.objectName);
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A <code>DistributionLocatorConfig</code> that lets JMX do its work.
   */
  static class JMXDistributionLocatorConfig extends JMXManagedEntityConfig
    implements DistributionLocatorConfig {

    JMXDistributionLocatorConfig(MBeanServerConnection mbs,
                                 ObjectName objectName) {
      super(mbs, objectName);
      Assert.assertTrue(objectName != null);
    }

    public int getPort() {
      try {
        Assert.assertTrue(this.objectName != null);
        return ((Integer) this.mbs.getAttribute(this.objectName, "port")).intValue();
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public void setPort(int port) {
      try {
        Assert.assertTrue(this.objectName != null);
        this.mbs.setAttribute(this.objectName,
                              new Attribute("port", new Integer(port)));
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public String getBindAddress() {
      try {
        Assert.assertTrue(this.objectName != null);
        return (String) this.mbs.getAttribute(this.objectName, "bindAddress");
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }

    public void setBindAddress(String bindAddress) {
      try {
        Assert.assertTrue(this.objectName != null);
        this.mbs.setAttribute(this.objectName,
                              new Attribute("bindAddress", bindAddress));
      } catch (Exception ex) {
        throw handleException(ex);
      }
    }
    
    public void setDistributedSystemProperties(java.util.Properties props) {
      try {
        Assert.assertTrue(this.objectName != null);
        this.mbs.setAttribute(this.objectName,
          new Attribute("gemfire-properties", props));
      }
      catch (Exception ex) {
        throw handleException(ex);
      }
    }
    
    public java.util.Properties getDistributedSystemProperties() {
      try {
        Assert.assertTrue(this.objectName != null);
        return (java.util.Properties)this.mbs.getAttribute(this.objectName,
          "gemfire-properties");
      }
      catch (Exception ex) {
        throw handleException(ex);
      }
    }

  }

}
