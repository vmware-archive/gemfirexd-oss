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

package hydra;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DM;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Provides version-dependent support for {@link GemFireDescription}.
 */
public class GemFireVersionHelper {

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManager(Properties p, String val) {
    String key = DistributionConfig.JMX_MANAGER_NAME;
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManagerAccessFile(Properties p, String fn,
                                                HostDescription hd) {
    String key = DistributionConfig.JMX_MANAGER_ACCESS_FILE_NAME;
    if (fn == null) {
      p.setProperty(key, DistributionConfig.DEFAULT_JMX_MANAGER_ACCESS_FILE);
    } else {
      p.setProperty(key, EnvHelper.expandEnvVars(fn, hd));
    }
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManagerPasswordFile(Properties p, String fn,
                                                  HostDescription hd) {
    String key = DistributionConfig.JMX_MANAGER_PASSWORD_FILE_NAME;
    if (fn == null) {
      p.setProperty(key, DistributionConfig.DEFAULT_JMX_MANAGER_PASSWORD_FILE);
    } else {
      p.setProperty(key, EnvHelper.expandEnvVars(fn, hd));
    }
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManagerPorts(Properties p,
                        boolean generatePort, boolean generateHttpPort) {
    if (generatePort || generateHttpPort) {
      // only use an endpoint when needed to store a non-default port
      JMXManagerHelper.Endpoint endpoint =
                JMXManagerHelper.getEndpoint(generatePort, generateHttpPort);
      p.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME,
                    String.valueOf(endpoint.getPort()));
      p.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME,
                    String.valueOf(endpoint.getHttpPort()));
    } else {
      p.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME,
        String.valueOf(JMXManagerHelper.DEFAULT_JMX_MANAGER_PORT));
      p.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME,
        String.valueOf(JMXManagerHelper.DEFAULT_JMX_MANAGER_HTTP_PORT));
    }
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManagerSSL(Properties p, String val) {
    String key = DistributionConfig.JMX_MANAGER_SSL_NAME;
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setJMXManagerStart(Properties p, String val) {
    String key = DistributionConfig.JMX_MANAGER_START_NAME;
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link DistributedSystemHelper#getMembers}.
   */
  public static Set getMembers(DM dm) {
    return dm.getNormalDistributionManagerIds();
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setRemoteLocators(Properties p, List<String> vals) {
    String key = DistributionConfig.REMOTE_LOCATORS_NAME;
    List remoteEndpoints = DistributedSystemHelper.getContacts(vals);
    String val = DistributedSystemHelper.endpointsToString(remoteEndpoints);
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setEnforceUniqueHost(Properties p, Boolean val) {
    String key = DistributionConfig.ENFORCE_UNIQUE_HOST_NAME;
    p.setProperty(key, val.toString());
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setRedundancyZone(Properties p, String val) {
    String key = DistributionConfig.REDUNDANCY_ZONE_NAME;
    p.setProperty(key, val.toString());
  }

  /**
   * Version-dependent support for {@link GemFireDescription#configure}.
   * <p>
   * Returns the value of hydra.GemFirePrms-deltaPropagation at the given
   * index.
   */
  protected static Boolean getDeltaPropagation(ConfigHashtable tab, int i) {
    Long key = GemFirePrms.deltaPropagation;
    boolean defaultValue = DistributionConfig.DEFAULT_DELTA_PROPAGATION;
    boolean val = tab.booleanAtWild(key, i, Boolean.valueOf(defaultValue));
    return Boolean.valueOf(val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setDeltaPropagation(Properties p, Boolean val) {
    String key = DistributionConfig.DELTA_PROPAGATION_PROP_NAME;
    p.setProperty(key, val.toString());
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setDistributedSystemId(Properties p, Integer val) {
    String key = DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME;
    p.setProperty(key, val.toString());
  }

  /**
   * Version-dependent support for {@link GemFireDescription#configure}.
   * <p>
   * Returns the value of hydra.GemFirePrms-membershipPortRange at the given
   * index.
   */
  protected static String getMembershipPortRange(ConfigHashtable tab, int i) {
    Long key = GemFirePrms.membershipPortRange;
    String defaultValue =
           DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0] + "-" +
           DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1];
    return tab.stringAtWild(key, i, defaultValue);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setMembershipPortRange(Properties p, String val) {
    String key = DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME;
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription#configure}.
   * <p>
   * Returns the value of hydra.GemFirePrms-tcpPort at the given index.
   */
  protected static int getTcpPort(ConfigHashtable tab, int i) {
    Long key = GemFirePrms.tcpPort;
    int defaultValue = DistributionConfig.DEFAULT_TCP_PORT;
    return tab.intAtWild(key, i, defaultValue);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getDistributedSystemProperties}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setTcpPort(Properties p, int val) {
    String key = DistributionConfig.TCP_PORT_NAME;
    p.setProperty(key, String.valueOf(val));
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #getOffHeapMemorySize}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setOffHeapMemorySize(Properties p, String val) {
    String key = DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME;
    p.setProperty(key, val);
  }

  /**
   * Version-dependent support for {@link GemFireDescription
   * #disableAutoReconnect}.
   * <p>
   * Sets the property to the given value.
   */
  protected static void setDisableAutoReconnect(Properties p, Boolean val) {
    String key = DistributionConfig.DISABLE_AUTO_RECONNECT_NAME;
    if (val == null) {
      p.setProperty(key, ""+DistributionConfig.DEFAULT_DISABLE_AUTO_RECONNECT);
    } else {
      p.setProperty(key, val.toString());
    }
  }

}
