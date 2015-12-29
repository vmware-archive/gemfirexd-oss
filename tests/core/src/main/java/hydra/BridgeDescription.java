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

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Encodes information needed to describe and create a bridge server.
 */
public class BridgeDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this bridge description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer capacity;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String  diskStoreName;
  private String  evictionPolicy;
  private String[] groups;
  private Long    loadPollInterval;
  private String  loadProbe;
  private Integer maxConnections;
  private Integer maxThreads;
  private Integer maximumMessageCount;
  private Integer maximumTimeBetweenPings;
  private Integer messageTimeToLive;
  private Integer socketBufferSize;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public BridgeDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this bridge description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this bridge description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the capacity.
   */
  private Integer getCapacity() {
    return this.capacity;
  }

  /**
   * Sets the capacity.
   */
  private void setCapacity(Integer i) {
    this.capacity = i;
  }

  /**
   * Returns the disk store description name.
   */
  private String getDiskStoreName() {
    return this.diskStoreName;
  }

  /**
   * Sets the disk store description name.
   */
  private void setDiskStoreName(String str) {
    this.diskStoreName = str;
  }

  /**
   * Returns the disk store description.
   */
  public DiskStoreDescription getDiskStoreDescription() {
    return this.diskStoreDescription;
  }

  /**
   * Sets the disk store description.
   */
  private void setDiskStoreDescription(DiskStoreDescription dsd) {
    this.diskStoreDescription = dsd;
  }

  /**
   * Returns the eviction policy.
   */
  private String getEvictionPolicy() {
    return this.evictionPolicy;
  }

  /**
   * Sets the eviction policy.
   */
  private void setEvictionPolicy(String str) {
    this.evictionPolicy = str;
  }

  /**
   * Returns the server groups.
   */
  protected String[] getGroups() {
    return this.groups;
  }

  /**
   * Sets the server groups.
   */
  private void setGroups(String[] s) {
    this.groups = s;
  }

  /**
   * Get the load poll interval
   */
  public Long getLoadPollInterval() {
    return loadPollInterval;
  }

  /**
   * Set the load poll interval
   */
  public void setLoadPollInterval(Long loadPollInterval) {
    this.loadPollInterval = loadPollInterval;
  }
  

  /**
   * Get the name of a load probe class
   */
  public String getLoadProbe() {
    return loadProbe;
  }

  /**
   * Set the name of the load probe class
   */
  public void setLoadProbe(String loadProbe) {
    this.loadProbe = loadProbe;
  }

  /**
   * Returns the max connections.
   */
  private Integer getMaxConnections() {
    return this.maxConnections;
  }

  /**
   * Sets the max connections.
   */
  private void setMaxConnections(Integer i) {
    this.maxConnections = i;
  }

  /**
   * Returns the max threads.
   */
  private Integer getMaxThreads() {
    return this.maxThreads;
  }

  /**
   * Sets the max threads.
   */
  private void setMaxThreads(Integer i) {
    this.maxThreads = i;
  }

  /**
   * Returns the maximum message count.
   */
  private Integer getMaximumMessageCount() {
    return this.maximumMessageCount;
  }

  /**
   * Sets the maximum message count.
   */
  private void setMaximumMessageCount(Integer i) {
    this.maximumMessageCount = i;
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
   * Returns the message time to live.
   */
  private Integer getMessageTimeToLive() {
    return this.messageTimeToLive;
  }

  /**
   * Sets the message time to live.
   */
  private void setMessageTimeToLive(Integer i) {
    this.messageTimeToLive = i;
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

//------------------------------------------------------------------------------
// Bridge configuration
//------------------------------------------------------------------------------

  /**
   * Configures the bridge server using this bridge description and the given
   * port.
   */
  protected void configure(CacheServer c, int port) {
    ClientSubscriptionConfig csg = c.getClientSubscriptionConfig();
    csg.setCapacity(this.getCapacity().intValue());
    if (this.getDiskStoreDescription() != null) {
      csg.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    csg.setEvictionPolicy(this.getEvictionPolicy());
    c.setGroups(this.getGroups());
    c.setLoadPollInterval(this.getLoadPollInterval().longValue());
    c.setLoadProbe(getLoadProbe(this.getLoadProbe()));
    c.setMaxConnections(this.getMaxConnections().intValue());
    c.setMaxThreads(this.getMaxThreads().intValue());
    c.setMaximumMessageCount(this.getMaximumMessageCount().intValue());
    c.setMaximumTimeBetweenPings(this.getMaximumTimeBetweenPings().intValue());
    c.setMessageTimeToLive(this.getMessageTimeToLive().intValue());
    c.setPort(port);
    c.setSocketBufferSize(this.getSocketBufferSize().intValue());
  }
  
  /**
   * Returns an instance of the load probe defined by
   * the load probe classname.
   */
  private ServerLoadProbe getLoadProbe(String probeClass) {
    try {
      return (ServerLoadProbe) getInstance(BridgePrms.loadProbe, probeClass);
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(BridgePrms.loadProbe)
        + " does not implement ServerLoadProbe: " + probeClass;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the bridge server as a string.  For use only by {@link
   * BridgeHelper#bridgeServerToString(CacheServer)}.
   */
  protected static synchronized String bridgeServerToString(CacheServer c) {
    StringBuffer buf = new StringBuffer();
    ClientSubscriptionConfig csg = c.getClientSubscriptionConfig();;
    buf.append("\n  capacity: " + csg.getCapacity());
    buf.append("\n  diskStoreName: " + csg.getDiskStoreName());
    buf.append("\n  evictionPolicy: " + csg.getEvictionPolicy());
    buf.append("\n  groups: " + Arrays.asList(c.getGroups()));
    buf.append("\n  loadPollInterval: " + c.getLoadPollInterval());
    buf.append("\n  loadProbe: " + c.getLoadProbe());
    buf.append("\n  maxConnections: " + c.getMaxConnections());
    buf.append("\n  maxThreads: " + c.getMaxThreads());
    buf.append("\n  maximumMessageCount: " + c.getMaximumMessageCount());
    buf.append("\n  maximumTimeBetweenPings: " + c.getMaximumTimeBetweenPings());
    buf.append("\n  messageTimeToLive: " + c.getMessageTimeToLive());
    buf.append("\n  port: " + c.getPort());
    buf.append("\n  socketBufferSize: " + c.getSocketBufferSize());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "capacity", this.getCapacity());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "evictionPolicy", this.getEvictionPolicy());
    map.put(header + "groups", Arrays.asList(this.getGroups()));
    map.put(header + "loadPollInterval", this.getLoadPollInterval());
    map.put(header + "loadProbe", this.getLoadProbe());
    map.put(header + "maxConnections", this.getMaxConnections());
    map.put(header + "maxThreads", this.getMaxThreads());
    map.put(header + "maximumMessageCount", this.getMaximumMessageCount());
    map.put(header + "maximumTimeBetweenPings", this.getMaximumTimeBetweenPings());
    map.put(header + "messageTimeToLive", this.getMessageTimeToLive());
    map.put(header + "port", "autogenerated");
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates bridge descriptions from the bridge parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each bridge name
    Vector names = tab.vecAt(BridgePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create bridge description from test configuration parameters
      BridgeDescription bd = createBridgeDescription(name, config, i);

      // save configuration
      config.addBridgeDescription(bd);
    }
  }

  /**
   * Creates the bridge description using test configuration parameters and
   * product defaults.
   */
  private static BridgeDescription createBridgeDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    BridgeDescription bd = new BridgeDescription();
    bd.setName(name);
    
    // capacity
    {
      Long key = BridgePrms.capacity;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(ClientSubscriptionConfig.DEFAULT_CAPACITY);
      }
      bd.setCapacity(i);
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = BridgePrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        bd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        bd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // evictionPolicy
    {
      Long key = BridgePrms.evictionPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = ClientSubscriptionConfig.DEFAULT_EVICTION_POLICY;
      } else {
        str = getEvictionPolicy(str, key);
      }
      bd.setEvictionPolicy(str);
    }
    // groups
    {
      Long key = BridgePrms.groups;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) { // toss any occurrences of "default"
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null) {
            i.remove();
          }
        }
      }
      if (strs == null || strs.size() == 0) {
        bd.setGroups(CacheServer.DEFAULT_GROUPS);
      } else {
        String[] stray = new String[strs.size()];
        for (int i = 0; i < strs.size(); i++) {
          stray[i] = (String)strs.get(i);
        }
        bd.setGroups(stray);
      }
    }
    // loadPollInterval
    {
      Long key = BridgePrms.loadPollInterval;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l == null) {
        l = new Long(CacheServer.DEFAULT_LOAD_POLL_INTERVAL);
      }
      bd.setLoadPollInterval(l);
    }
    // loadProbe
    {
      Long key = BridgePrms.loadProbe;
      String s = tab.getString(key, tab.getWild(key, index, null));
      if(s == null) {
        s = CacheServer.DEFAULT_LOAD_PROBE.getClass().getName();
      }
      bd.setLoadProbe(s);
    }
    // maxConnections
    {
      Long key = BridgePrms.maxConnections;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_MAX_CONNECTIONS);
      }
      bd.setMaxConnections(i);
    }
    // maxThreads
    {
      Long key = BridgePrms.maxThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_MAX_THREADS);
      }
      bd.setMaxThreads(i);
    }
    // maximumMessageCount
    {
      Long key = BridgePrms.maximumMessageCount;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT);
      }
      bd.setMaximumMessageCount(i);
    }
    // maximumTimeBetweenPings
    {
      Long key = BridgePrms.maximumTimeBetweenPings;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
      }
      bd.setMaximumTimeBetweenPings(i);
    }
    // messageTimeToLive
    {
      Long key = BridgePrms.messageTimeToLive;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE);
      }
      bd.setMessageTimeToLive(i);
    }
    // socketBufferSize
    {
      Long key = BridgePrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(CacheServer.DEFAULT_SOCKET_BUFFER_SIZE);
      }
      bd.setSocketBufferSize(i);
    }
    // require disk configuration with eviction
    {
      if (bd.getDiskStoreName() == null) {
        if (bd.getEvictionPolicy() != HARegionQueue.HA_EVICTION_POLICY_NONE) {
          String s = BasePrms.nameForKey(BridgePrms.evictionPolicy) + " ("
                   + bd.getEvictionPolicy() + ") requires a "
                   + BasePrms.nameForKey(BridgePrms.diskStoreName);
          throw new HydraRuntimeException(s);
        }
      }
    }
    return bd;
  }

//------------------------------------------------------------------------------
// Disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the disk store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getDiskStoreDescription(String str,
                                                  Long key, TestConfig config) {
    DiskStoreDescription dsd = config.getDiskStoreDescription(str);
    if (dsd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(DiskStorePrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else { 
      return dsd;
    }
  }

//------------------------------------------------------------------------------
// Eviction policy configuration support        
//------------------------------------------------------------------------------
      
  /** 
   * Returns the eviction policy for the given string.
   */ 
  private static String getEvictionPolicy(String str, Long key) {
    if (str.equalsIgnoreCase("entry") || str.equalsIgnoreCase("entries"))
    {
      return HARegionQueue.HA_EVICTION_POLICY_ENTRY;
    }
    else if (str.equalsIgnoreCase("mem") || str.equalsIgnoreCase("memory"))
    {
      return HARegionQueue.HA_EVICTION_POLICY_MEMORY;
    }
    else if (str.equalsIgnoreCase(BasePrms.NONE))
    {
      return HARegionQueue.HA_EVICTION_POLICY_NONE;
    }
    else
    {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    } 
  }
}
