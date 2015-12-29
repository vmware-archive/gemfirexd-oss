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

package hydra.gemfirexd;

import com.pivotal.gemfirexd.NetworkInterface;
import com.pivotal.gemfirexd.NetworkInterface.ConnectionListener;
import com.pivotal.gemfirexd.Property;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.HydraConfigException;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.TestConfig;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a network server.
 */
public class NetworkServerDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this network server description */
  private String name;

  /** Remaining parameters, in alphabetical order */

  private String connectionListener;
  private Boolean logConnections;
  private Integer maxIdleTime;
  private Integer maxStartupWaitSec;
  private Integer maxThreads;
  private Integer numServers;
  private Boolean reusePorts;
  private Integer timeSlice;
  private Boolean trace;
  private transient String traceDirectory; // HYDRA path to derby.log

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public NetworkServerDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in alphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this network server description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this network server description.
   */
  private void setName(String str) {
    this.name = str;
  }

  public String getConnectionListener() {
    return this.connectionListener;
  }
  private void setConnectionListener(String str) {
    this.connectionListener = str;
  }

  public Boolean getLogConnections() {
    return this.logConnections;
  }
  private void setLogConnections(Boolean bool) {
    this.logConnections = bool;
  }

  public Integer getMaxIdleTime() {
    return this.maxIdleTime;
  }
  private void setMaxIdleTime(Integer i) {
    this.maxIdleTime = i;
  }

  public Integer getMaxStartupWaitSec() {
    return this.maxStartupWaitSec;
  }
  private void setMaxStartupWaitSec(Integer i) {
    this.maxStartupWaitSec = i;
  }

  public Integer getMaxThreads() {
    return this.maxThreads;
  }
  private void setMaxThreads(Integer i) {
    this.maxThreads = i;
  }

  public Integer getNumServers() {
    return this.numServers;
  }
  private void setNumServers(Integer i) {
    this.numServers = i;
  }

  public Boolean getReusePorts() {
    return this.reusePorts;
  }
  private void setReusePorts(Boolean bool) {
    this.reusePorts = bool;
  }

  public Integer getTimeSlice() {
    return this.timeSlice;
  }
  private void setTimeSlice(Integer i) {
    this.timeSlice = i;
  }

  public Boolean getTrace() {
    return this.trace;
  }
  private void setTrace(Boolean bool) {
    this.trace = bool;
  }

  /**
   * Client-side transient.  Returns absolute path to trace directory.
   * Lazily creates and registers it.  This directory contains derby logs.
   */
  private synchronized String getTraceDirectory() {
    if (this.traceDirectory == null) {
      FabricServerDescription fsd =
                  FabricServerHelper.TheFabricServerDescription;
      if (fsd == null) {
        String s = "Cannot start network server before fabric server";
        throw new HydraRuntimeException(s);
      }
      this.traceDirectory = fsd.getSysDir();
    }
    return this.traceDirectory;
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the network properties for this network server description.
   */
  protected Properties getNetworkProperties() {
    Properties p = new Properties();

    p.setProperty(Property.DRDA_PROP_LOGCONNECTIONS,
                  getLogConnections().toString());
    p.setProperty(Property.DRDA_PROP_THREAD_MAXIDLETIME,
                  getMaxIdleTime().toString());
    p.setProperty(Property.DRDA_PROP_MAXTHREADS,
                  getMaxThreads().toString());
    p.setProperty(Property.DRDA_PROP_TIMESLICE,
                  getTimeSlice().toString());
    p.setProperty(Property.DRDA_PROP_TRACE,
                  getTrace().toString());
    p.setProperty(Property.DRDA_PROP_TRACEDIRECTORY,
                  getTraceDirectory().toString());

    return p;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap<String,Object> toSortedMap() {
    SortedMap<String,Object> map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "connectionListener", getConnectionListener());
    map.put(header + "logConnections", getLogConnections());
    map.put(header + "maxIdleTime", getMaxIdleTime());
    map.put(header + "maxStartupWaitSec", getMaxStartupWaitSec());
    map.put(header + "maxThreads", getMaxThreads());
    map.put(header + "numServers", getNumServers());
    map.put(header + "reusePorts", getReusePorts());
    map.put(header + "timeSlice", getTimeSlice());
    map.put(header + "trace", getTrace());
    map.put(header + "traceDirectory", "autogenerated");
    return map;
  }

//------------------------------------------------------------------------------
// NetworkServer configuration
//------------------------------------------------------------------------------

  /**
   * Configures the network server using this description.
   */
  protected void configure(NetworkInterface ns) {
    ns.setConnectionListener(this.getConnectionListenerInstance());
  }

  /**
   * Returns the network server as a string.  For use only by {@link
   * NetworkServerHelper#networkServerToString(NetworkServer)}.
   */
  protected static synchronized String networkServerToString(
                                             NetworkInterface ni) {
    StringBuffer buf = new StringBuffer();
    //buf.append("\n  connectionListener: " + connectionListenerFor(ni.getConnectionListener()));
    buf.append("\n  currentProperties: " + ni.getCurrentProperties());
    //buf.append("\n  logConnections: " + ni.logConnections());
    buf.append("\n  maxThreads: " + ni.getMaxThreads());
    buf.append("\n  timeSlice: " + ni.getTimeSlice());
    //buf.append("\n  trace: " + ni.getTrace());
    //buf.append("\n  traceDirectory: " + ni.getTraceDirectory());
    return buf.toString();
  }

  /**
   * Returns the connection listener class name.
   */
  //private static String connectionListenerFor(ConnectionListener listener) {
  //  return listener == null ? null : listener.getClass().getName();
  //}
  // waiting for product support

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates network server descriptions from the test configuration parameters.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each network server name
    SortedMap<String,NetworkServerDescription> nsds = new TreeMap();
    Vector names = tab.vecAt(NetworkServerPrms.names, new HydraVector());
    for (int i = 0; i < names.size(); i++) {
      String name = (String)names.elementAt(i);
      if (nsds.containsKey(name)) {
        String s = BasePrms.nameForKey(NetworkServerPrms.names)
                 + " contains duplicate entries: " + names;
        throw new HydraConfigException(s);
      }
      NetworkServerDescription nsd = createNetworkServerDescription(name,
                                          config, sconfig, i);
      nsds.put(name, nsd);
    }
    return nsds;
  }

  /**
   * Creates the network server description using test configuration parameters
   * and product defaults.
   */
  private static NetworkServerDescription createNetworkServerDescription(
                 String name, TestConfig config, GfxdTestConfig sconfig,
                 int index) {

    ConfigHashtable tab = config.getParameters();

    NetworkServerDescription nsd = new NetworkServerDescription();
    nsd.setName(name);

    // connectionListener
    {
      Long key = NetworkServerPrms.connectionListener;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        nsd.setConnectionListener(getConnectionListener(key, str, config));
      }
    }
    // logConnections
    {
      Long key = NetworkServerPrms.logConnections;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.TRUE;
      }
      nsd.setLogConnections(bool);
    }
    // maxIdleTime
    {
      Long key = NetworkServerPrms.maxIdleTime;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 30000;
      }
      nsd.setMaxIdleTime(i);
    }
    // maxStartupWaitSec
    {
      Long key = NetworkServerPrms.maxStartupWaitSec;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 30;
      }
      nsd.setMaxStartupWaitSec(i);
    }
    // maxThreads
    {
      Long key = NetworkServerPrms.maxThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 0;
      }
      nsd.setMaxThreads(i);
    }
    // numServers
    {
      Long key = NetworkServerPrms.numServers;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 1;
      }
      nsd.setNumServers(i);
    }
    // reusePorts
    {
      Long key = NetworkServerPrms.reusePorts;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.TRUE;
      }
      nsd.setReusePorts(bool);
    }
    // timeSlice
    {
      Long key = NetworkServerPrms.timeSlice;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 0;
      }
      nsd.setTimeSlice(i);
    }
    // trace
    {
      Long key = NetworkServerPrms.trace;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      nsd.setTrace(bool);
    }
    // defer traceDirectory
    return nsd;
  }

//------------------------------------------------------------------------------
// Connection listener configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the connection listener classname from the given string, after
   * validating that it exists and implements ConnectionListener.
   *
   * @throws HydraConfigException if the value has an illegal value or type.
   */
  private static String getConnectionListener(Long key, String val,
                                              TestConfig config) {
    return getConnectionListenerInstance(val).getClass().getName();
  }

  /**
   * Returns an instance of the connection listener for this description.
   */
  private synchronized ConnectionListener getConnectionListenerInstance() {
    String s = this.getConnectionListener();
    return s == null ? null : getConnectionListenerInstance(s);
  }

  /**
   * Returns a connection listener instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement ConnectionListener.
   */
  private static ConnectionListener getConnectionListenerInstance(
                                    String classname) {
    Long key = NetworkServerPrms.connectionListener;
    Object obj = getInstance(key, classname);
    try {
      return (ConnectionListener)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement ConnectionListener: " + classname;
      throw new HydraConfigException(s);
    }
  }
}
