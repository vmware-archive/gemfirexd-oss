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

import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create an gateway, as used by
 * bridge loaders, writers, and clients.
 */
public class GatewayDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Map of logical gateway configuration names to maps of listeners. */
  private static Map listenerInstanceMaps = new HashMap();

  /** The logical name of this gateway description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer alertThreshold;
  private Boolean batchConflation;
  private Integer batchSize;
  private Integer batchTimeInterval;
  private Integer concurrencyLevel;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String  diskStoreName;
  private Boolean enablePersistence;
  private Integer endpointNumPerDs;
  private List    listeners;
  private Boolean listenersSingleton;
  private Integer maximumQueueMemory;
  private OrderPolicy orderPolicy;
  private Integer socketBufferSize;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public GatewayDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this gateway description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this gateway description.
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
   * Returns the batch conflation.
   */
  private Boolean getBatchConflation() {
    return this.batchConflation;
  }

  /**
   * Sets the batch conflation.
   */
  private void setBatchConflation(Boolean bool) {
    this.batchConflation = bool;
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
   * Returns the concurrency level.
   */
  protected Integer getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  /**
   * Sets the concurrency level.
   */
  private void setConcurrencyLevel(Integer i) {
    this.concurrencyLevel = i;
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
   * Returns the enable persistence.
   */
  private Boolean getEnablePersistence() {
    return this.enablePersistence;
  }

  /**
   * Sets the enable persistence.
   */
  private void setEnablePersistence(Boolean bool) {
    this.enablePersistence = bool;
  }

  /**
   * Returns the endpoint num per distributed system.
   */
  private Integer getEndpointNumPerDs() {
    return this.endpointNumPerDs;
  }

  /**
   * Sets the endpoint num per distributed system.
   */
  private void setEndpointNumPerDs(Integer i) {
    this.endpointNumPerDs = i;
  }

  /**
   * Returns the classnames of the listeners.
   */
  private List getListeners() {
    return this.listeners;
  }

  /**
   * Sets the classnames of the listeners.
   */
  private void setListeners(List classnames) {
    this.listeners = classnames;
  }

  /**
   * Returns the listeners singleton.
   */
  private Boolean getListenersSingleton() {
    return this.listenersSingleton;
  }

  /**
   * Sets the listeners singleton.
   */
  private void setListenersSingleton(Boolean bool) {
    this.listenersSingleton = bool;
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
   * Returns the order policy.
   */
  private OrderPolicy getOrderPolicy() {
    return this.orderPolicy;
  }

  /**
   * Sets the order policy.
   */
  private void setOrderPolicy(OrderPolicy p) {
    this.orderPolicy = p;
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
// Gateway configuration
//------------------------------------------------------------------------------

  /**
   * Configures a gateway to the specified distributed system using this
   * gateway description and the endpoints.  The id and gatewayHubId are
   * already set.
   */
  protected void configure(Gateway g, String ds, List endpoints,
                           boolean connect) {

    if (endpoints != null) { // regular case
      // compute and add endpoints
      List list = GatewayHelper.getRandomEndpoints(ds, endpoints,
                                         this.getEndpointNumPerDs().intValue());
      for (Iterator i = list.iterator(); i.hasNext();) {
        GatewayHubHelper.Endpoint e = (GatewayHubHelper.Endpoint)i.next();
        String id = e.getId() + "_vmid_" + e.getVmid(); // ds, numbered by vmid
        // so all gateways in site will use same id for same ds
        g.addEndpoint(id, e.getAddress(), e.getPort());
      }
    }

    // add listeners
    List instances = this.getListenerInstances(connect);
      if (instances != null) { // wbcl case
      for (Iterator i = instances.iterator(); i.hasNext();) {
        GatewayEventListener l = (GatewayEventListener)i.next();
        g.addListener(l);
      }
    }

    // set queue attributes
    GatewayQueueAttributes gqa = new GatewayQueueAttributes();
    gqa.setAlertThreshold(this.getAlertThreshold().intValue());
    gqa.setBatchConflation(this.getBatchConflation().booleanValue());
    gqa.setBatchSize(this.getBatchSize().intValue());
    gqa.setBatchTimeInterval(this.getBatchTimeInterval().intValue());
    if (this.getDiskStoreDescription() != null) {
      gqa.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    gqa.setEnablePersistence(this.getEnablePersistence().booleanValue());
    gqa.setMaximumQueueMemory(this.getMaximumQueueMemory().intValue());
    g.setQueueAttributes(gqa);

    // set other attributes
    if (this.getOrderPolicy() != null) {
      g.setOrderPolicy(this.getOrderPolicy());
    }
    g.setSocketBufferSize(this.getSocketBufferSize().intValue());
    // note: concurrency level is set as an argument to hub.addGateway() 
  }

  /**
   * Returns the gateway as a string.  For use only by {@link
   * GatewayHelper#gatewayToString(Gateway)}.
   */
  protected static synchronized String gatewayToString(Gateway g) {
    StringBuffer buf = new StringBuffer();
    GatewayQueueAttributes gqa = g.getQueueAttributes();
    buf.append("\n  id: " + g.getId());
    buf.append("\n  gatewayHubId: " + g.getGatewayHubId());
    buf.append("\n  alertThreshold: " + gqa.getAlertThreshold());
    buf.append("\n  batchConflation: " + gqa.getBatchConflation());
    buf.append("\n  batchSize: " + gqa.getBatchSize());
    buf.append("\n  batchTimeInterval: " + gqa.getBatchTimeInterval());
    buf.append("\n  concurrencyLevel: " + g.getConcurrencyLevel());
    buf.append("\n  diskStoreName: " + gqa.getDiskStoreName());
    buf.append("\n  enablePersistence: " + gqa.getEnablePersistence());
    buf.append("\n  endpoints: " + g.getEndpoints());
    buf.append("\n  listeners: " + listenersFor(g.getListeners()));
    buf.append("\n  maximumQueueMemory: " + gqa.getMaximumQueueMemory());
    buf.append("\n  orderPolicy: " + g.getOrderPolicy());
    buf.append("\n  socketBufferSize: " + g.getSocketBufferSize());
    return buf.toString();
  }

  /**
   * Returns the listener class names.
   */
  private static List listenersFor(List instances) {
    List classnames = new ArrayList();
    for (Iterator i = instances.iterator(); i.hasNext();) {
      GatewayEventListener instance = (GatewayEventListener)i.next();
      classnames.add(instance.getClass().getName());
    }
    return classnames;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "alertThreshold", this.getAlertThreshold());
    map.put(header + "batchConflation", this.getBatchConflation());
    map.put(header + "batchSize", this.getBatchSize());
    map.put(header + "batchTimeInterval", this.getBatchTimeInterval());
    map.put(header + "concurrencyLevel", this.getConcurrencyLevel());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "enablePersistence", this.getEnablePersistence());
    if (this.getEndpointNumPerDs().intValue() == GatewayPrms.ALL_AVAILABLE) {
      map.put(header + "endpointNumPerDs", "all available");
    } else {
      map.put(header + "endpointNumPerDs", this.getEndpointNumPerDs());
    }
    map.put(header + "listeners", this.getListeners());
    map.put(header + "listenersSingleton", this.getListenersSingleton());
    map.put(header + "maximumQueueMemory", this.getMaximumQueueMemory());
    map.put(header + "orderPolicy", this.getOrderPolicy());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates gateway descriptions from the gateway parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each gateway name
    Vector names = tab.vecAt(GatewayPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway description from test configuration parameters
      GatewayDescription gd = createGatewayDescription(name, config, i);

      // save configuration
      config.addGatewayDescription(gd);
    }
  }

  /**
   * Creates the gateway description using test configuration parameters and
   * product defaults.
   */
  private static GatewayDescription createGatewayDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    GatewayDescription gd = new GatewayDescription();
    gd.setName(name);

    GatewayQueueAttributes defaults = new GatewayQueueAttributes();

    // alertThreshold
    {
      Long key = GatewayPrms.alertThreshold;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(defaults.getAlertThreshold());
      }
      gd.setAlertThreshold(i);
    }
    // batchConflation
    {
      Long key = GatewayPrms.batchConflation;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(defaults.getBatchConflation());
      }
      gd.setBatchConflation(bool);
    }
    // batchSize
    {
      Long key = GatewayPrms.batchSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(defaults.getBatchSize());
      }
      gd.setBatchSize(i);
    }
    // batchTimeInterval
    {
      Long key = GatewayPrms.batchTimeInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(defaults.getBatchTimeInterval());
      }
      gd.setBatchTimeInterval(i);
    }
    // concurrencyLevel
    {
      Long key = GatewayPrms.concurrencyLevel;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(Gateway.DEFAULT_CONCURRENCY_LEVEL);
      }
      gd.setConcurrencyLevel(i);
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = GatewayPrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        gd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        gd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // enablePersistence
    {
      Long key = GatewayPrms.enablePersistence;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(defaults.getEnablePersistence());
      }
      gd.setEnablePersistence(bool);
    }
    // endpointNumPerDs
    {
      Long key = GatewayPrms.endpointNumPerDs;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(GatewayPrms.ALL_AVAILABLE);
      }
      gd.setEndpointNumPerDs(i);
    }
    // listeners
    {
      Long key = GatewayPrms.listeners;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          gd.setListeners(new ArrayList(strs));
        }
      }
    }
    // cacheListenersSingleton
    {
      Long key = GatewayPrms.listenersSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        gd.setListenersSingleton(Boolean.FALSE);
      } else {
        gd.setListenersSingleton(bool);
      }
    }
    // maximumQueueMemory
    {
      Long key = GatewayPrms.maximumQueueMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(defaults.getMaximumQueueMemory());
      }
      gd.setMaximumQueueMemory(i);
    }
    // orderPolicy
    {
      Long key = GatewayPrms.orderPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      gd.setOrderPolicy(getOrderPolicy(str, key, gd.getConcurrencyLevel()));
    }
    // socketBufferSize
    {
      Long key = GatewayPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(Gateway.DEFAULT_SOCKET_BUFFER_SIZE);
      }
      gd.setSocketBufferSize(i);
    }
    // require disk configuration in all cases
    {
      if (gd.getDiskStoreName() == null) {
        String s = BasePrms.nameForKey(GatewayPrms.names) + " ("
                 + gd.getName() + ") requires a "
                 + BasePrms.nameForKey(GatewayPrms.diskStoreName);
        throw new HydraRuntimeException(s);
      }
    }
    return gd;
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
// Listeners configuration support
//------------------------------------------------------------------------------

  /**
   * Returns a listener instance for each listener classname.
   * <p>
   * Manages singletons if connect is true, which means that these are real
   * runtime instances rather than test configuration instances.
   *
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement GatewayEventListener.
   */
  private synchronized List getListenerInstances(boolean connect) {
    List classnames = this.getListeners();
    if (classnames == null) {
      return null;
    } else {
      Map instanceMap = (Map)listenerInstanceMaps.get(this.getName());
      Long key = GatewayPrms.listeners;
      List instances = new ArrayList();
      for (Iterator i = classnames.iterator(); i.hasNext();) {
        String classname = (String)i.next();
        if (connect && this.getListenersSingleton().booleanValue()) {
          // singleton case
          if (instanceMap == null) {
            instanceMap = new HashMap();
          }
          Object instance = instanceMap.get(classname);
          if (instance == null) { // create and save a new instance
            instance = getAppListenerInstance(classname);
            instanceMap.put(classname, instance);
            instances.add(instance);
          } else { // use the existing instance
            instances.add(instance);
          }
        } else {
          // non-singleton case, create a new instance
          instances.add(getAppListenerInstance(classname));
        }
      }
      return instances;
    }
  }

  /**
   * Returns an application-defined listener instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement GatewayEventListener.
   */
  private static GatewayEventListener getAppListenerInstance(String classname) {
    Long key = GatewayPrms.listeners;
    Object obj = getInstance(key, classname);
    try {
      return (GatewayEventListener)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement GatewayEventListener: " + classname;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Order policy configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the OrderPolicy for the given string.
   */
  private static OrderPolicy getOrderPolicy(String str, Long key,
                                            int concurrencyLevel) {
    if (str == null) {
      return (concurrencyLevel <= 1) ? null : OrderPolicy.KEY;

    } else if (str.equalsIgnoreCase("key")) {
      return OrderPolicy.KEY;

    } else if (str.equalsIgnoreCase("thread")) {
      return OrderPolicy.THREAD;

    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Version support
//------------------------------------------------------------------------------

  /**
   * Custom deserialization.
   */
  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.alertThreshold = (Integer)in.readObject();
    this.batchConflation = (Boolean)in.readObject();
    this.batchSize = (Integer)in.readObject();
    this.batchTimeInterval = (Integer)in.readObject();
    this.concurrencyLevel = (Integer)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.enablePersistence = (Boolean)in.readObject();
    this.endpointNumPerDs = (Integer)in.readObject();
    this.listeners = (List)in.readObject();
    this.listenersSingleton = (Boolean)in.readObject();
    this.maximumQueueMemory = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.orderPolicy = getOrderPolicy((String)in.readObject(),
                                       GatewayPrms.orderPolicy,
                                       this.concurrencyLevel);
    this.socketBufferSize = (Integer)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.alertThreshold);
    out.writeObject(this.batchConflation);
    out.writeObject(this.batchSize);
    out.writeObject(this.batchTimeInterval);
    out.writeObject(this.concurrencyLevel);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskStoreName);
    out.writeObject(this.enablePersistence);
    out.writeObject(this.endpointNumPerDs);
    out.writeObject(this.listeners);
    out.writeObject(this.listenersSingleton);
    out.writeObject(this.maximumQueueMemory);
    out.writeObject(this.name);
    if (this.orderPolicy == null) {
      out.writeObject(this.orderPolicy);
    } else {
      out.writeObject(this.orderPolicy.toString());
    }
    out.writeObject(this.socketBufferSize);
  }
}
