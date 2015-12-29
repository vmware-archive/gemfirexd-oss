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

import java.util.*;
import javax.management.*;

/**
 * An implementation of <code>GemFireHeealth</code> that
 * communicates via JMX.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
class JMXGemFireHealth extends JMXAdminImpl
  implements GemFireHealth {

  /** The <code>DistributedSystemHealthConfig</code> for this
   * <code>GemFireHealth</code>. */
  private JMXDistributedSystemHealthConfig dsHealthConfig = null;

  /** The default <code>GemFireHealthConfig</code> for this
   * <code>GemFireHealth</code>. */
  private JMXGemFireHealthConfig defaultHealthConfig = null;

  /** Maps the name of a host to its GemFireHealthConfig */
  private final Map hostHealthConfigs = new HashMap();

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>JMXGemFireHealth</code> with the given MBean
   * server and object name.
   */
  JMXGemFireHealth(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  /////////////////////  Instance Methods  /////////////////////

  public Health getHealth() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (Health) this.mbs.getAttribute(this.objectName, "health");

    } catch (Exception ex) {
      String s = "While getting the health";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void resetHealth() {
    try {
      this.mbs.invoke(this.objectName, "resetHealth",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While resetting the health";
      throw new InternalGemFireException(s, ex);
    }
  }

  public String getDiagnosis() {
    try {
      return (String) this.mbs.invoke(this.objectName, "getDiagnosis",
                                      new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While getting the diagnosis";
      throw new InternalGemFireException(s, ex);
    }
  }

  public DistributedSystemHealthConfig
    getDistributedSystemHealthConfig() {

    synchronized(this) {
      if (this.dsHealthConfig == null) {
        try {
          Hashtable props = new Hashtable();
          props.put("type", "DistributedSystemHealthConfig");
          props.put("id", this.objectName.getKeyProperty("id"));
          ObjectName dsHealthConfigName =
            new ObjectName("GemFire", props); 
          this.dsHealthConfig =
            new JMXDistributedSystemHealthConfig(this.mbs,
                                                 dsHealthConfigName);

        } catch (Exception ex) {
          String s = "While getting the DistributedSystemHealthConfig";
          throw new InternalGemFireException(s, ex);
        }
      }

      return this.dsHealthConfig;
    }
  }

  public void setDistributedSystemHealthConfig(DistributedSystemHealthConfig
                                               config) {
    try {
      this.mbs.invoke(this.dsHealthConfig.getObjectName(),
                      "applyChanges", new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While setting the DistributedSystemHealthConfig";
      throw new InternalGemFireException(s, ex);
    }
  }

  /**
   * Creates a new <code>JMXGemFireHealthConfig</code> for the host
   * with the given name.
   */
  private JMXGemFireHealthConfig createHealthConfig(String hostName) 
    throws Exception {

    ObjectName configName =
      (ObjectName) this.mbs.invoke(this.objectName,
                                   "manageGemFireHealthConfig",
                                   new Object[] { hostName },
                                   new String[] { String.class.getName() });

    return new JMXGemFireHealthConfig(this.mbs, configName);
  }

  public GemFireHealthConfig getDefaultGemFireHealthConfig() {
    synchronized(this) {
      if (this.defaultHealthConfig == null) {
        try {
          this.defaultHealthConfig = createHealthConfig(null);

        } catch (Exception ex) {
          String s = "While getting the DistributedSystemHealthConfig";
          throw new InternalGemFireException(s, ex);
        }
      }

      return this.defaultHealthConfig;
    }
  }

  public void setDefaultGemFireHealthConfig(GemFireHealthConfig config) {
    try {
      this.mbs.invoke(this.defaultHealthConfig.getObjectName(),
                      "applyChanges", new Object[0], new String[0]);

    } catch (Exception ex) {
      String s = "While setting the default GemFireHealthConfig";
      throw new InternalGemFireException(s, ex);
    }
  }

  public GemFireHealthConfig getGemFireHealthConfig(String hostName) {
    try {
      synchronized (this.hostHealthConfigs) {
        JMXGemFireHealthConfig config =
          (JMXGemFireHealthConfig) this.hostHealthConfigs.get(hostName);
        if (config == null) {
          config = createHealthConfig(hostName);
          this.hostHealthConfigs.put(hostName, config);
        }

        return config;
      }
    } catch (Exception ex) {
      String s = "While setting the GemFireHealthConfig for " + hostName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setGemFireHealthConfig(String hostName,
                                     GemFireHealthConfig config) {
    JMXGemFireHealthConfig jmxConfig =
      (JMXGemFireHealthConfig) config;
    try {
      this.mbs.invoke(jmxConfig.getObjectName(),
                      "applyChanges", new Object[0], new String[0]);


    } catch (Exception ex) {
      String s = "While setting the GemFireHealthConfig for " + hostName;
      throw new InternalGemFireException(s, ex);
    }
  }

  public void close() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public boolean isClosed() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

}
