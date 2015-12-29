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

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CreateGatewaySenderNode;
import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.TestConfig;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a gateway sender.
 */
public class GatewaySenderDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final String SPACE = " ";

  public static enum Type {
    serial;
  }

  /** The logical name of this gateway sender description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer alertThreshold;
  private Boolean batchConflationEnabled;
  private Integer batchSize;
  private Integer batchTimeInterval;
  private String diskStoreName;
  private String distributedSystem;
  private Integer distributedSystemId;
  private String id;
  private Boolean isParallel;
  private Boolean manualStart;
  private Integer maximumQueueMemory;
  private Boolean persistenceEnabled;
  private List<String> remoteDistributedSystems;
  private List<Integer> remoteDistributedSystemIds;
  private List<String> serverGroups;
  private Integer socketBufferSize;
  private Integer socketReadTimeout;
  private Type type;

  private transient List<String> DDL;
  private transient List<String> IDS;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public GatewaySenderDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this gateway sender description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this gateway sender description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the alert threshold.
   */
  private Integer getAlertThreshold() {
    return this.alertThreshold;
  }

  /**
   * Sets the alert threshold.
   */
  private void setAlertThreshold(Integer i) {
    this.alertThreshold = i;
  }

  /**
   * Returns the batch conflation enabled.
   */
  private Boolean getBatchConflationEnabled() {
    return this.batchConflationEnabled;
  }

  /**
   * Sets the batch conflation enabled.
   */
  private void setBatchConflationEnabled(Boolean bool) {
    this.batchConflationEnabled = bool;
  }

  /**
   * Returns the batch size.
   */
  private Integer getBatchSize() {
    return this.batchSize;
  }

  /**
   * Sets the batch size.
   */
  private void setBatchSize(Integer i) {
    this.batchSize = i;
  }

  /**
   * Returns the batch time interval.
   */
  private Integer getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  /**
   * Sets the batch time interval.
   */
  private void setBatchTimeInterval(Integer i) {
    this.batchTimeInterval = i;
  }

  /**
   * Returns the disk store name.
   */
  private String getDiskStoreName() {
    return this.diskStoreName;
  }

  /**
   * Sets the disk store name.
   */
  private void setDiskStoreName(String str) {
    this.diskStoreName = str;
  }

  /**
   * Returns the distributed system.
   */
  public String getDistributedSystem() {
    return this.distributedSystem;
  }

  /**
   * Sets the distributed system.
   */
  private void setDistributedSystem(String str) {
    this.distributedSystem = str;
  }

  /**
   * Returns the distributed system id.
   */
  public Integer getDistributedSystemId() {
    return this.distributedSystemId;
  }

  /**
   * Sets the distributed system id.
   */
  private void setDistributedSystemId(Integer i) {
    this.distributedSystemId = i;
  }

  /**
   * Returns the id.
   */
  public String getId() {
    return this.id;
  }

  /**
   * Sets the id.
   */
  private void setId(String str) {
    this.id = str;
  }

  /**
   * Returns the is parallel.
   */
  public Boolean getIsParallel() {
    return this.isParallel;
  }

  /**
   * Sets the is parallel.
   */
  private void setIsParallel(Boolean bool) {
    this.isParallel = bool;
  }

  /**
   * Returns the manual start.
   */
  public Boolean getManualStart() {
    return this.manualStart;
  }

  /**
   * Sets the manual start.
   */
  private void setManualStart(Boolean bool) {
    this.manualStart = bool;
  }

  /**
   * Returns the maximum queue memory.
   */
  private Integer getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  /**
   * Sets the maximum queue memory.
   */
  private void setMaximumQueueMemory(Integer i) {
    this.maximumQueueMemory = i;
  }

  /**
   * Returns the persistence enabled.
   */
  private Boolean getPersistenceEnabled() {
    return this.persistenceEnabled;
  }

  /**
   * Sets the persistence enabled.
   */
  private void setPersistenceEnabled(Boolean bool) {
    this.persistenceEnabled = bool;
  }

  /**
   * Returns the remote distributed systems.
   */
  public List<String> getRemoteDistributedSystems() {
    return this.remoteDistributedSystems;
  }

  /**
   * Sets the remote distributed systems.
   */
  private void setRemoteDistributedSystems(List<String> systems) {
    this.remoteDistributedSystems = systems;
  }

  /**
   * Returns the remote distributed system ids.
   */
  public List<Integer> getRemoteDistributedSystemIds() {
    return this.remoteDistributedSystemIds;
  }

  /**
   * Sets the remote distributed system ids.
   */
  private void setRemoteDistributedSystemIds(List<Integer> systems) {
    this.remoteDistributedSystemIds = systems;
  }

  /**
   * Returns the server groups.
   */
  public List<String> getServerGroups() {
    return this.serverGroups;
  }

  /**
   * Returns the server groups string, suitable for DDL.
   */
  public String getServerGroupsProperty() {
    String str = "";
    for (String group : this.serverGroups) {
      str += str.length() == 0 ? group : "," + group;
    }
    return str.length() == 0 ? null : str;
  }

  /**
   * Sets the server groups.
   */
  private void setServerGroups(List<String> list) {
    this.serverGroups = list;
  }

  /**
   * Returns the socket buffer size.
   */
  private Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }

  /**
   * Sets the socket buffer size.
   */
  private void setSocketBufferSize(Integer i) {
    this.socketBufferSize = i;
  }

  /**
   * Returns the socket read timeout.
   */
  private Integer getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  /**
   * Sets the socket read timeout.
   */
  private void setSocketReadTimeout(Integer i) {
    this.socketReadTimeout = i;
  }

  /**
   * Returns the type.
   */
  protected Type getType() {
    return this.type;
  }

  /**
   * Sets the type.
   */
  private void setType(Type t) {
    this.type = t;
  }

//------------------------------------------------------------------------------
// Gateway sender configuration
//------------------------------------------------------------------------------

  /**
   * Returns DDL for gateway senders based on this description.
   */
  protected synchronized List<String> getDDL() {
    if (this.getDistributedSystemId() == null ||
        this.getRemoteDistributedSystemIds() == null) {
      String s = "Please ask for a supporting method";
      throw new UnsupportedOperationException(s);
    }
    if (DDL == null) {
      DDL = new ArrayList();
      IDS = new ArrayList();
      int localDSID = this.getDistributedSystemId();
      for (Integer remoteDSID : this.getRemoteDistributedSystemIds()) {
        String senderID = this.getId() + "_" + localDSID + "_to_" + remoteDSID;
        IDS.add(senderID);
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE GATEWAYSENDER ")
           .append(senderID)
           .append(" ( ")
           .append("remotedsid").append(SPACE)
           .append(remoteDSID).append(SPACE)
           .append("socketbuffersize").append(SPACE)
           .append(this.getSocketBufferSize()).append(SPACE)
           .append(CreateGatewaySenderNode.SOCKETREADTIMEOUT).append(SPACE)
           .append(this.getSocketReadTimeout()).append(SPACE)
           .append(CreateGatewaySenderNode.MANUALSTART).append(SPACE)
           .append(this.getManualStart()).append(SPACE)
           .append(CreateGatewaySenderNode.ISPARALLEL).append(SPACE)
           .append(this.getIsParallel()).append(SPACE)
           .append(CreateGatewaySenderNode.ENABLEBATCHCONFLATION).append(SPACE)
           .append(this.getBatchConflationEnabled()).append(SPACE)
           .append(CreateGatewaySenderNode.BATCHSIZE).append(SPACE)
           .append(this.getBatchSize()).append(SPACE)
           .append(CreateGatewaySenderNode.BATCHTIMEINTERVAL).append(SPACE)
           .append(this.getBatchTimeInterval()).append(SPACE)
           .append(CreateGatewaySenderNode.ENABLEPERSISTENCE).append(SPACE)
           .append(this.getPersistenceEnabled()).append(SPACE);
        if (this.getDiskStoreName() != null) {
          buf.append(CreateGatewaySenderNode.DISKSTORENAME).append(SPACE)
             .append(this.getDiskStoreName()).append(SPACE);
        }
        buf.append(CreateGatewaySenderNode.MAXQUEUEMEMORY).append(SPACE)
           .append(this.getMaximumQueueMemory()).append(SPACE)
           .append(CreateGatewaySenderNode.ALERTTHRESHOLD).append(SPACE)
           .append(this.getAlertThreshold()).append(SPACE)
           .append(")");
        if (this.getServerGroups() != null) {
          buf.append(" SERVER GROUPS ( ")
             .append(this.getServerGroupsProperty()).append(" )");
        }
        DDL.add(buf.toString());
      }
    }
    return DDL;
  }

  /**
   * Returns gateway sender IDs based on this description.
   */
  protected synchronized List<String> getIDs() {
    if (IDS == null) {
      getDDL();
    }
    return IDS;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "alertThreshold", this.getAlertThreshold());
    map.put(header + "batchConflationEnabled", this.getBatchConflationEnabled());
    map.put(header + "batchSize", this.getBatchSize());
    map.put(header + "batchTimeInterval", this.getBatchTimeInterval());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "distributedSystem", this.getDistributedSystem());
    map.put(header + "distributedSystemId", this.getDistributedSystemId());
    map.put(header + "id", this.getId());
    map.put(header + "isParallel", this.getIsParallel());
    map.put(header + "manualStart", this.getManualStart());
    map.put(header + "maximumQueueMemory", this.getMaximumQueueMemory());
    map.put(header + "persistenceEnabled", this.getPersistenceEnabled());
    map.put(header + "remoteDistributedSystems", this.getRemoteDistributedSystems());
    map.put(header + "serverGroups", this.getServerGroups());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    map.put(header + "socketReadTimeout", this.getSocketReadTimeout());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates gateway sender descriptions from the gateway sender parameters
   * in the test configuration.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each gateway sender name
    SortedMap<String,GatewaySenderDescription> gsds = new TreeMap();
    Vector names = tab.vecAt(GatewaySenderPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway sender description from test configuration parameters
      GatewaySenderDescription gsd = createGatewaySenderDescription(name,
                                                  config, sconfig, i);

      // save configuration
      gsds.put(name, gsd);
    }
    return gsds;
  }

  /**
   * Creates the gateway sender description using test configuration parameters
   * and product defaults.
   */
  private static GatewaySenderDescription createGatewaySenderDescription(
          String name, TestConfig config, GfxdTestConfig sconfig, int index) {

    ConfigHashtable tab = config.getParameters();

    GatewaySenderDescription gsd = new GatewaySenderDescription();
    gsd.setName(name);

    // alertThreshold
    {
      Long key = GatewaySenderPrms.alertThreshold;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_ALERT_THRESHOLD;
      }
      gsd.setAlertThreshold(i);
    }
    // batchConflationEnabled
    {
      Long key = GatewaySenderPrms.batchConflationEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_BATCH_CONFLATION;
      }
      gsd.setBatchConflationEnabled(bool);
    }
    // batchSize
    {
      Long key = GatewaySenderPrms.batchSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_BATCH_SIZE;
      }
      gsd.setBatchSize(i);
    }
    // batchTimeInterval
    {
      Long key = GatewaySenderPrms.batchTimeInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;
      }
      gsd.setBatchTimeInterval(i);
    }
    // diskStoreName
    {
      Long key = GatewaySenderPrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        gsd.setDiskStoreName(str);
      }
    }
    // distributedSystem
    {
      Long key = GatewaySenderPrms.distributedSystem;
      String str = tab.stringAtWild(key, index, null);
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        Integer dsid = getDistributedSystemId(str, key, sconfig);
        gsd.setDistributedSystemId(dsid);
        gsd.setDistributedSystem(str);
      }
    }
    // id
    {
      Long key = GatewaySenderPrms.id;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        gsd.setId(str);
      } else {
        gsd.setId(GatewaySenderPrms.DEFAULT_SENDER_ID);
      }
    }
    // isParallel
    {
      Long key = GatewaySenderPrms.isParallel;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      gsd.setIsParallel(bool);
    }
    // manualStart
    {
      Long key = GatewaySenderPrms.manualStart;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_MANUAL_START;
      }
      gsd.setManualStart(bool);
    }
    // maximumQueueMemory
    {
      Long key = GatewaySenderPrms.maximumQueueMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
      }
      gsd.setMaximumQueueMemory(i);
    }
    // persistenceEnabled
    {
      Long key = GatewaySenderPrms.persistenceEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
      }
      gsd.setPersistenceEnabled(bool);
    }
    // remoteDistributedSystems
    {
      Long key = GatewaySenderPrms.remoteDistributedSystems;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator it = strs.iterator(); it.hasNext();) {
          String str = tab.getString(key, it.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            it.remove();
          }
        }
        if (strs.size() > 0) {
          List<Integer> dsids =
                        getRemoteDistributedSystemIds(strs, key, sconfig);
          gsd.setRemoteDistributedSystemIds(dsids);
          gsd.setRemoteDistributedSystems(strs);
        }
      }
    }
    // serverGroups
    {
      Long key = GatewaySenderPrms.serverGroups;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) { // toss any occurrences of "default"
        for (Iterator it = strs.iterator(); it.hasNext();) {
          String str = tab.getString(key, it.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            it.remove();
          }
        }
        if (strs.size() != 0) {
          List<String> groups = getServerGroups(strs, key, sconfig);
          gsd.setServerGroups(groups);
        }
      }
    }
    // socketBufferSize
    {
      Long key = GatewaySenderPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;
      }
      gsd.setSocketBufferSize(i);
    }
    // socketReadTimeout
    {
      Long key = GatewaySenderPrms.socketReadTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;
      }
      gsd.setSocketReadTimeout(i);
    }
    // type
    {
      Long key = GatewaySenderPrms.type;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        gsd.setType(Type.serial);
      } else {
        gsd.setType(getType(str, key));
      }
    }
    return gsd;
  }

//------------------------------------------------------------------------------
// Distributed system configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the distributed system id for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         FabricServerPrms#distributedSystem}.
   */
  private static Integer getDistributedSystemId(String str, Long key,
                                                GfxdTestConfig config) {
    for (FabricServerDescription fsd :
               config.getFabricServerDescriptions().values()) {
      if (fsd.getDistributedSystem().equals(str)) {
        return fsd.getDistributedSystemId();
      }
    }
    String s = BasePrms.nameForKey(key) + " not found in "
             + BasePrms.nameForKey(FabricServerPrms.distributedSystem)
             + ": " + str;
    throw new HydraConfigException(s);
  }

//------------------------------------------------------------------------------
// Remote distributed system configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the distributed system ids for the given strings.
   * @throws HydraConfigException if the given strings are not listed in {@link
   *         FabricServerPrms#distributedSystem}.
   */
  private static List<Integer> getRemoteDistributedSystemIds(Vector strs,
                                        Long key, GfxdTestConfig config) {
    List<Integer> dsids = new ArrayList();
    for (Iterator it = strs.iterator(); it.hasNext();) {
      String str = (String)it.next();
      Integer dsid = null;
      for (FabricServerDescription fsd :
                 config.getFabricServerDescriptions().values()) {
        if (fsd.getDistributedSystem().equals(str)) {
          dsid = fsd.getDistributedSystemId();
          if (!dsids.contains(dsid)) {
            dsids.add(dsid);
          }
        }
      }
      if (dsid == null) {
        String s = BasePrms.nameForKey(key) + " not found in "
                 + BasePrms.nameForKey(FabricServerPrms.distributedSystem)
                 + ": " + str;
        throw new HydraConfigException(s);
      }
    }
    return dsids;
  }

//------------------------------------------------------------------------------
// Server groups configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the server groups for the given strings.
   * @throws HydraConfigException if the given strings are not listed in {@link
   *         FabricServerPrms#serverGroups}.
   */
  private static List<String> getServerGroups(Vector strs, Long key,
                                              GfxdTestConfig config) {
    List<String> groups = new ArrayList();
    for (Iterator it = strs.iterator(); it.hasNext();) {
      String str = (String)it.next();
      String group = null;
      for (FabricServerDescription fsd :
                 config.getFabricServerDescriptions().values()) {
        if (fsd.getServerGroups() != null) {
          if (fsd.getServerGroups().contains(str)) {
            group = str;
            if (!groups.contains(group)) {
              groups.add(group);
            }
          }
        }
      }
      if (group == null) {
        String s = BasePrms.nameForKey(key) + " not found in "
                 + BasePrms.nameForKey(FabricServerPrms.serverGroups)
                 + ": " + str;
        throw new HydraConfigException(s);
      }
    }
    return groups;
  }

//------------------------------------------------------------------------------
// Type configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the Type for the given string.
   */
  private static Type getType(String str, Long key) {
    if (str.equalsIgnoreCase(Type.serial.toString())) {
      return Type.serial;

    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }
}
