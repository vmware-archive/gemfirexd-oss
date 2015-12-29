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

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.client.am.Connection;
import com.pivotal.gemfirexd.jdbc.ClientAttribute;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.FileUtil;
import hydra.HostDescription;
import hydra.HydraConfigException;
import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.VmDescription;
import java.io.File;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Encodes information needed to describe and connect a thin client.
 */
public class ThinClientDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final String LOG_FILE_NAME = "log-file";

  /** The logical name of this thin client description */
  private String name;

  /** Remaining parameters, in alphabetical order */

  private List<String> clientNames; // HYDRA
  private Boolean queryHDFS; // GFXD
  private Integer readTimeout; // GFXD
  private Boolean singleHopEnabled; // GFXD
  private Integer singleHopMaxConnections; // GFXD

  private transient String logFile; // GFXD
  private transient String resourceDir; // HYDRA path to clientDir
  private transient String clientDir; // HYDRA path to client.log

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public ThinClientDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in alphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this thin client description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this thin client description.
   */
  private void setName(String str) {
    this.name = str;
  }

  public List<String> getClientNames() {
    return this.clientNames;
  }
  private void setClientNames(List<String> list) {
    this.clientNames = list;
  }

  public Boolean getQueryHDFS() {
    return this.queryHDFS;
  }

  private void setQueryHDFS(Boolean bool) {
    this.queryHDFS = bool;
  }

  public Integer getReadTimeout() {
    return this.readTimeout;
  }

  private void setReadTimeout(Integer i) {
    this.readTimeout = i;
  }

  public Boolean getSingleHopEnabled() {
    return this.singleHopEnabled;
  }

  private void setSingleHopEnabled(Boolean bool) {
    this.singleHopEnabled = bool;
  }

  public Integer getSingleHopMaxConnections() {
    return this.singleHopMaxConnections;
  }

  private void setSingleHopMaxConnections(Integer i) {
    this.singleHopMaxConnections = i;
  }

  /**
   * Client-side transient.  Returns absolute path to log file.
   */
  private synchronized String getLogFile() {
    if (this.logFile == null) {
      this.logFile = this.getClientDir() + "/client.log";
    }
    return this.logFile;
  }

  /**
   * Client-side transient.  Returns absolute path to the client directories.
   */
  private synchronized String getResourceDir() {
    if (this.resourceDir == null) {
      HostDescription hd = getHostDescription();
      String dir = hd.getResourceDir();
      if (dir == null) {
        dir = hd.getUserDir();
      }
      this.resourceDir = dir;
    }
    return this.resourceDir;
  }

  /**
   * Client-side transient.  Returns absolute path to client directory.
   * Lazily creates and registers it.  This directory contains the client log.
   */
  protected synchronized String getClientDir() {
    if (this.clientDir == null) {
      HostDescription hd = getHostDescription();
      String dir = this.getResourceDir() + "/"
                 + "vm_" + RemoteTestModule.getMyVmid()
                 + "_" + RemoteTestModule.getMyClientName()
                 + "_" + hd.getHostName()
                 + "_" + RemoteTestModule.getMyPid();
      FileUtil.mkdir(new File(dir));
      try {
        RemoteTestModule.Master.recordDir(hd, getName(), dir);
      } catch (RemoteException e) {
        String s = "Unable to access master to record directory: " + dir;
        throw new HydraRuntimeException(s, e);
      }
      this.clientDir = dir;
    }
    return this.clientDir;
  }

  /**
   * Returns the host description for this hydra client VM, after validating
   * that it is legal for it to use this description.
   */
  private HostDescription getHostDescription() {
    // make sure this is a hydra client
    String clientName = RemoteTestModule.getMyClientName();
    if (clientName == null) {
      String s = "This method can only be invoked from hydra client VMs";
      throw new HydraInternalException(s);
    }
    // make sure this client belongs to this description
    if (!this.clientNames.contains(clientName)) {
      String s = "Logical client " + clientName
               + " is not in the list of clients supported by "
               + BasePrms.nameForKey(ThinClientPrms.names) + "=" + getName();
      throw new HydraRuntimeException(s);
    }
    return TestConfig.getInstance().getClientDescription(clientName)
                                   .getVmDescription().getHostDescription();
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the connection properties for this thin client description.
   */
  protected Properties getConnectionProperties() {
    Properties p = new Properties();
    p.setProperty(LOG_FILE_NAME, getLogFile());
    p.setProperty(Attribute.QUERY_HDFS, getQueryHDFS().toString());
    p.setProperty(ClientAttribute.READ_TIMEOUT, getReadTimeout().toString());
    p.setProperty(ClientAttribute.SINGLE_HOP_ENABLED, getSingleHopEnabled().toString());
    return p;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap<String,Object> toSortedMap() {
    SortedMap<String,Object> map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "clientNames", getClientNames());
    map.put(header + "logFile", "autogenerated: client.log in client directory");
    map.put(header + "queryHDFS", this.getQueryHDFS());
    map.put(header + "readTimeout", this.getReadTimeout());
    map.put(header + "singleHopEnabled", this.getSingleHopEnabled());
    map.put(header + "singleHopMaxConnections", this.getSingleHopMaxConnections());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates thin client descriptions from the test configuration parameters.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();
    List<String> usedClients = new ArrayList();

    // create a description for each thin client name
    SortedMap<String,ThinClientDescription> tcds = new TreeMap();
    Vector names = tab.vecAt(ThinClientPrms.names, new HydraVector());
    for (int i = 0; i < names.size(); i++) {
      String name = (String)names.elementAt(i);
      if (tcds.containsKey(name)) {
        String s = BasePrms.nameForKey(ThinClientPrms.names)
                 + " contains duplicate entries: " + names;
        throw new HydraConfigException(s);
      }
      ThinClientDescription tcd = createThinClientDescription(name, config, i,
                                                              usedClients);
      tcds.put(name, tcd);
    }
    return tcds;
  }

  /**
   * Creates the thin client description using test configuration parameters
   * and product defaults.
   */
  private static ThinClientDescription createThinClientDescription(
                 String name, TestConfig config, int index,
                 List<String> usedClients) {

    ConfigHashtable tab = config.getParameters();

    ThinClientDescription tcd = new ThinClientDescription();
    tcd.setName(name);

    // defer clientDir
    // clientNames
    {
      Long key = ThinClientPrms.clientNames;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs == null) {
        String s = BasePrms.nameForKey(key)
                 + " is a required field and has no default value";
        throw new HydraConfigException(s);
      }
      List<String> cnames = getClientNames(strs, key, usedClients, tab);
      tcd.setClientNames(cnames);

      // add product jar to class paths
      // for now, include both gemfirexd.jar and gemfirexd-client.jar
      for (String cname : cnames) {
        VmDescription vmd = config.getClientDescription(cname)
                                  .getVmDescription();
        HostDescription hd = vmd.getHostDescription();
        List<String> classPaths = new ArrayList();
        String classPath2 = hd.getGemFireHome()
                          + hd.getFileSep() + ".."
                          + hd.getFileSep() + "product-gfxd"
                          + hd.getFileSep() + "lib"
                          + hd.getFileSep() + "gemfirexd.jar";
        classPaths.add(classPath2);
        String classPath = hd.getGemFireHome()
                         + hd.getFileSep() + ".."
                         + hd.getFileSep() + "product-gfxd"
                         + hd.getFileSep() + "lib"
                         + hd.getFileSep() + "gemfirexd-client.jar";
        classPaths.add(classPath);
        vmd.setGemFireXDClassPaths(classPaths);
      }
    }
    // defer logFile
    // queryHDFS
    {
      Long key = ThinClientPrms.queryHDFS;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      tcd.setQueryHDFS(bool);
    }
    // readTimeout
    {
      Long key = ThinClientPrms.readTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(Connection.DEFAULT_LOGIN_TIMEOUT);
      }
      tcd.setReadTimeout(i);
    }
    // defer resourceDir
    // singleHopEnabled
    {
      Long key = ThinClientPrms.singleHopEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      tcd.setSingleHopEnabled(bool);
    }
    // singleHopMaxConnections
    {
      Long key = ThinClientPrms.singleHopMaxConnections;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      int defaultVal = Connection.DEFAULT_SINGLE_HOP_MAX_CONN_PER_SERVER;
      if (i == null) {
        i = Integer.valueOf(Integer.valueOf(defaultVal));
      }
      tcd.setSingleHopMaxConnections(i);

      // add system property to extra vm args if using single hop
      // with non-default max connections
      if (tcd.getSingleHopEnabled() &&
          tcd.getSingleHopMaxConnections() != defaultVal) {
        String arg = "-D" + Attribute.CLIENT_JVM_PROPERTY_PREFIX
                   + ClientAttribute.SINGLE_HOP_MAX_CONNECTIONS
                   + "=" + tcd.getSingleHopMaxConnections();
        for (String cname : tcd.getClientNames()) {
          VmDescription vmd = config.getClientDescription(cname)
                                    .getVmDescription();
          vmd.addExtraVMArg(arg);
        }
      }
    }
    return tcd;
  }

//------------------------------------------------------------------------------
// clientNames configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the client names for the given string.
   */
  private static List<String> getClientNames(Vector strs, Long key,
                                             List<String> usedClients,
                                             ConfigHashtable tab) {
    // make sure there are no duplicates
    for (Iterator i = strs.iterator(); i.hasNext();) {
      String str = tab.getString(key, i.next());
      if (usedClients.contains(str)) {
        String s = BasePrms.nameForKey(key) + " contains duplicate: " + str;
        throw new HydraConfigException(s);
      }
      usedClients.add(str);
    }
    return new ArrayList(strs);
  }
}
