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
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.SystemMemberBridgeServer;
import com.gemstone.gemfire.admin.SystemMemberCacheServer;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.internal.Assert;

import javax.management.*;

/**
 * A implementation of the <code>SystemMemberBridgeServer</code>
 * interface that communicates via JMX.
 *
 * @author David Whitlock
 * @since 4.0
 */
class JMXSystemMemberBridgeServer extends JMXAdminImpl
  implements SystemMemberCacheServer, SystemMemberBridgeServer {

  /**
   * Creates a new <code>JMXSystemMemberRegion</code> with the given
   * MBean server and object name.
   */
  JMXSystemMemberBridgeServer(MBeanServerConnection mbs,
                              ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  //////////////////////  Instance Methods  /////////////////////

  /**
   * Handles an exception thrown when invoking MBean method by
   * unwrapping it appropriately.
   *
   * @return The appropriate exception to be thrown
   */
  private RuntimeException handleException(Exception ex) {
    String s = "While accessing cache server MBean";
    return new InternalGemFireException(s, ex);
  }
  private AdminException handleAdminException(Exception ex) {
    return new AdminException(ex);
  }

  public int getPort() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "port")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setPort(int port) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("port", new Integer(port)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }

  public void start() throws AdminException {
    try {
      this.mbs.invoke(this.objectName, "start",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean isRunning() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "running")).booleanValue();

    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void stop() throws AdminException {
    try {
      this.mbs.invoke(this.objectName, "stop",
                      new Object[0], new String[0]);

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }

  public void refresh() {
    try {
      this.mbs.invoke(this.objectName, "refresh",
                      new Object[0], new String[0]);

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
  public void setBindAddress(String address) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("bindAddress", address));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public String getHostnameForClients() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "hostnameForClients");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setHostnameForClients(String name) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("hostnameForClients", name));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public void setNotifyBySubscription(boolean b) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("notifyBySubscription", new Boolean(b)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public boolean getNotifyBySubscription() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean) this.mbs.getAttribute(this.objectName, "notifyBySubscription")).booleanValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setSocketBufferSize(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("socketBufferSize", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public int getSocketBufferSize() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "socketBufferSize")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMaximumTimeBetweenPings(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("maximumTimeBetweenPings", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public int getMaximumTimeBetweenPings() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "maximumTimeBetweenPings")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public int getMaxConnections() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "maxConnections")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMaxConnections(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("maxConnections", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public int getMaxThreads() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "maxThreads")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMaxThreads(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("maxThreads", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public int getMaximumMessageCount() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "maximumMessageCount")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMaximumMessageCount(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("maximumMessageCount", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public int getMessageTimeToLive() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Integer) this.mbs.getAttribute(this.objectName, "messageTimeToLive")).intValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  public void setMessageTimeToLive(int v) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("messageTimeToLive", new Integer(v)));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public void setGroups(String[] groups) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("groups", groups));

    } catch (Exception ex) {
      throw handleAdminException(ex);
    }
  }
  public String[] getGroups() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String[]) this.mbs.getAttribute(this.objectName, "groups");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public long getLoadPollInterval() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Long) this.mbs.getAttribute(this.objectName, "loadPollInterval"))
          .longValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }


  public void setLoadPollInterval(long loadPollInterval) throws AdminException {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, new Attribute("loadPollInterval", new Long(loadPollInterval)));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public String getLoadProbe() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "loadProbe");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setLoadProbe(ServerLoadProbe loadProbe) throws AdminException {
    throw new UnsupportedOperationException("Not implemented yet");
  }
  
  
}
