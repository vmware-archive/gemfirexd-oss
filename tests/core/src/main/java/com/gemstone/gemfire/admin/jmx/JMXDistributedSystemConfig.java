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
 * An implementation of <code>DistributedSystemConfig</code> that
 * communicates via JMX.  Note that some of the methods of this class
 * could access attributes and operations on the
 * <code>GemFireAgent</code> MBean.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class JMXDistributedSystemConfig extends JMXAdminImpl
  implements DistributedSystemConfig {

  /** The name of the Agent MBean */
  private final ObjectName agentName;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>JMXDistributedSystemConfig</code> with the
   * given server and object name.
   */
  JMXDistributedSystemConfig(MBeanServerConnection mbs,
                             ObjectName objectName,
                             ObjectName agentName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
    this.agentName = agentName;
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Handles the given exception and will throw the appropriate
   * wrapped runtime exception.
   */
  protected RuntimeException handleException(Exception ex) {
    for (Throwable thr = ex; thr != null; thr = thr.getCause()) {
      if (thr instanceof MBeanException) {
        continue;

      } else if (thr instanceof RuntimeException) {
        return (RuntimeException) thr;
      }
    }

    String s = "While invoking a JMX operation exception occurred with " +
    		       "message :" + ex.getMessage();
    return new InternalGemFireException(s, ex);
  }

  public String getEntityConfigXMLFile() {
    try {
      return (String) this.mbs.getAttribute(this.agentName,
                                            "entityConfigXMLFile");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setEntityConfigXMLFile(String xmlFile) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("entityConfigXMLFile", xmlFile));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getSystemId() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "systemId");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSystemId(String systemId) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("systemId", systemId));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getSystemName() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "systemName");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSystemName(String name) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("systemName", name));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getMcastAddress() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "mcastAddress");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean getDisableTcp() {
    try {
      return ((Boolean)this.mbs.getAttribute(this.agentName, "disableTcp")).booleanValue();
    }
    catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public void setDisableTcp(boolean flag) {
    try {
      this.mbs.setAttribute(this.agentName,
        new Attribute("disableTcp", flag? Boolean.TRUE : Boolean.FALSE));
    }
    catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public void setMcastAddress(String mcastAddress) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("mcastAddress", mcastAddress));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getMcastPort() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "mcastPort")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setMcastPort(int mcastPort) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("mcastPort", new Integer(mcastPort)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getAckWaitThreshold() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "ackWaitThreshold")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public void setAckWaitThreshold(int seconds) {
    try {
      this.mbs.setAttribute(this.agentName,
          new Attribute("ackWaitThreshold", new Integer(seconds)));
    }
    catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getAckSevereAlertThreshold() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "ackSevereAlertThreshold")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public void setAckSevereAlertThreshold(int seconds) {
    try {
      this.mbs.setAttribute(this.agentName,
          new Attribute("ackSevereAlertThreshold", new Integer(seconds)));
    }
    catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setEnableNetworkPartitionDetection(boolean newValue) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("enableNetworkPartitionDetection", new Boolean(newValue)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public boolean getEnableNetworkPartitionDetection() {
    try {
      return ((Boolean) this.mbs.getAttribute(this.agentName, "enableNetworkPartitionDetection")).booleanValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setDisableAutoReconnect(boolean newValue) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("disableAutoReconnect", new Boolean(newValue)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public boolean getDisableAutoReconnect() {
    try {
      return ((Boolean) this.mbs.getAttribute(this.agentName, "disableAutoReconnect")).booleanValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public int getMemberTimeout() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "memberTimeout")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMemberTimeout(int value) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("memberTimeout", new Integer(value)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getLocators() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "locators");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLocators(String locators) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("setLocators", locators));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getMembershipPortRange() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "membershipPortRange");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setMembershipPortRange(String membershipPortRange) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("membershipPortRange", 
                            membershipPortRange));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }  

  public String getBindAddress() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "bindAddress");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setBindAddress(String bindAddress) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("bindAddress", bindAddress));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getServerBindAddress() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "serverBindAddress");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setServerBindAddress(String bindAddress) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("serverBindAddress", bindAddress));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getRemoteCommand() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "remoteCommand");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setRemoteCommand(String command) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("remoteCommand", command));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public DistributionLocator[] getDistributionLocators() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void setDistributionLocators(DistributionLocator[] locators)
  {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public boolean isSSLEnabled() {
    try {
      return ((Boolean) this.mbs.getAttribute(this.agentName, "sslEnabled")).booleanValue();

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSSLEnabled(boolean enabled) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("sslEnabled", new Boolean(enabled)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getSSLProtocols() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "sslProtocols");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSSLProtocols(String protocols) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("sslProtocols", protocols));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getSSLCiphers() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "sslCiphers");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSSLCiphers(String ciphers) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("sslCiphers", ciphers));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean isSSLAuthenticationRequired() {
    try {
      return ((Boolean) this.mbs.getAttribute(this.agentName, "sslAuthenticationRequired")).booleanValue();

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSSLAuthenticationRequired(boolean authRequired) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("sslAuthenticationRequired", new Boolean(authRequired)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public Properties getSSLProperties() {
    try {
      return (Properties) this.mbs.getAttribute(this.agentName, "sslProperties");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setSSLProperties(Properties sslProperties) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void addSSLProperty(String key, String value) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void removeSSLProperty(String key) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public String getLogFile() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "logFile");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLogFile(String logFile) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("logFile", logFile));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getRefreshInterval() {
    try {
      Integer interval = (Integer) this.mbs.getAttribute(this.objectName, "refreshInterval");
      return interval.intValue();

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setRefreshInterval(int interval) {
    try {
      this.mbs.setAttribute(this.objectName,
                            new Attribute("refreshInterval", interval));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getLogLevel() {
    try {
      return (String) this.mbs.getAttribute(this.agentName, "logLevel");

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLogLevel(String logLevel) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("logLevel", logLevel));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getLogDiskSpaceLimit() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "logDiskSpaceLimit")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLogDiskSpaceLimit(int limit) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("logDiskSpaceLimit", new Integer(limit)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getLogFileSizeLimit() {
    try {
      return ((Integer) this.mbs.getAttribute(this.agentName, "logFileSizeLimit")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLogFileSizeLimit(int limit) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("logFileSizeLimit", new Integer(limit)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public CacheServerConfig[] getCacheServerConfigs() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public CacheServerConfig createCacheServerConfig() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void removeCacheServerConfig(CacheServerConfig config) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public CacheVmConfig[] getCacheVmConfigs() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public CacheVmConfig createCacheVmConfig() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void removeCacheVmConfig(CacheVmConfig config) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public DistributionLocatorConfig[] getDistributionLocatorConfigs() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public DistributionLocatorConfig createDistributionLocatorConfig() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void removeDistributionLocatorConfig(DistributionLocatorConfig config) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void addListener(DistributedSystemConfig.ConfigListener
                          listener) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void removeListener(DistributedSystemConfig.ConfigListener
                             listener) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void validate() {

  }

  public Object clone() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public int getTcpPort() {
    try {
      return ((Integer)this.mbs.getAttribute(this.agentName, "tcpPort")).intValue();

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setTcpPort(int port) {
    try {
      this.mbs.setAttribute(this.agentName,
                            new Attribute("tcpPort", 
                            port));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }  


}
