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

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.TestConfig;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a gateway receiver.
 */
public class GatewayReceiverDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this gateway receiver description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String distributedSystem;
  private Integer distributedSystemId;
  private Integer endPort;
  private String id;
  private Integer maximumTimeBetweenPings;
  private List<String> serverGroups;
  private Integer socketBufferSize;
  private Integer startPort;

  private transient String DDL;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public GatewayReceiverDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this gateway receiver description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this gateway receiver description.
   */
  private void setName(String str) {
    this.name = str;
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
   * Returns the end port.
   */
  private Integer getEndPort() {
    return this.endPort;
  }

  /**
   * Sets the end port.
   */
  private void setEndPort(Integer i) {
    this.endPort = i;
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
  private void setId(String s) {
    this.id = s;
  }

  /**
   * Returns the maximum time between pings.
   */
  private Integer getMaximumTimeBetweenPings() {
    return this.maximumTimeBetweenPings;
  }

  /**
   * Sets the maximum time between pings.
   */
  private void setMaximumTimeBetweenPings(Integer i) {
    this.maximumTimeBetweenPings = i;
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
   * Returns the start port.
   */
  private Integer getStartPort() {
    return this.startPort;
  }

  /**
   * Sets the start port.
   */
  private void setStartPort(Integer i) {
    this.startPort = i;
  }

//------------------------------------------------------------------------------
// Gateway receiver configuration
//------------------------------------------------------------------------------

  /**
   * Returns DDL for a gateway receiver using this description and the given id.
   */
  protected synchronized String getDDL() {
    if (DDL == null) {
      StringBuilder buf = new StringBuilder();
      buf.append("CREATE GATEWAYRECEIVER ")
         .append(this.getId())
         .append(" (")
         .append(" STARTPORT ")
         .append(this.getStartPort())
         .append(" ENDPORT ")
         .append(this.getEndPort())
         .append(" MAXTIMEBETWEENPINGS ")
         .append(this.getMaximumTimeBetweenPings())
         .append(" SOCKETBUFFERSIZE ")
         .append(this.getSocketBufferSize())
         .append(" )")
         ;
      if (this.getServerGroups() != null) {
        buf.append(" SERVER GROUPS (")
           .append(this.getServerGroupsProperty()).append(" )");
      }
      DDL = buf.toString();
    }
    return DDL;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "distributedSystem", this.getDistributedSystem());
    map.put(header + "distributedSystemId", this.getDistributedSystemId());
    map.put(header + "endPort", this.getEndPort());
    map.put(header + "id", this.getId());
    map.put(header + "maximumTimeBetweenPings", this.getMaximumTimeBetweenPings());
    map.put(header + "serverGroups", this.getServerGroups());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    map.put(header + "startPort", this.getStartPort());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates gateway receiver descriptions from the gateway receiver parameters
   * in the test configuration.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each gateway receiver name
    SortedMap<String,GatewayReceiverDescription> grds = new TreeMap();
    Vector names = tab.vecAt(GatewayReceiverPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway receiver description from test configuration parameters
      GatewayReceiverDescription grd = createGatewayReceiverDescription(
                                                    name, config, sconfig, i);

      // save configuration
      grds.put(name, grd);
    }
    return grds;
  }

  /**
   * Creates the gateway receiver description using test configuration
   * parameters and product defaults.
   */
  private static GatewayReceiverDescription createGatewayReceiverDescription(
          String name, TestConfig config, GfxdTestConfig sconfig, int index) {

    ConfigHashtable tab = config.getParameters();

    GatewayReceiverDescription grd = new GatewayReceiverDescription();
    grd.setName(name);

    // distributedSystem
    {
      Long key = GatewayReceiverPrms.distributedSystem;
      String str = tab.stringAtWild(key, index, null);
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        Integer dsid = getDistributedSystemId(str, key, sconfig);
        grd.setDistributedSystemId(dsid);
        grd.setDistributedSystem(str);
      }
    }
    // endPort
    {
      Long key = GatewayReceiverPrms.endPort;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_END_PORT);
      }
      grd.setEndPort(i);
    }
    // id
    {
      Long key = GatewayReceiverPrms.id;
      String str = tab.stringAtWild(key, index, null);
      if (str == null) {
        grd.setId(GatewayReceiverPrms.DEFAULT_GATEWAY_RECEIVER_ID);
      } else {
        grd.setId(str);
      }
    }
    // maximumTimeBetweenPings
    {
      Long key = GatewayReceiverPrms.maximumTimeBetweenPings;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
      }
      grd.setMaximumTimeBetweenPings(i);
    }
    // serverGroups
    {
      Long key = GatewayReceiverPrms.serverGroups;
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
          grd.setServerGroups(groups);
        }
      }
    }
    // socketBufferSize
    {
      Long key = GatewayReceiverPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;
      }
      grd.setSocketBufferSize(i);
    }
    // startPort
    {
      Long key = GatewayReceiverPrms.startPort;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_START_PORT);
      }
      grd.setStartPort(i);
    }
    return grd;
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
}
