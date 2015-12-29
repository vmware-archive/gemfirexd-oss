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

import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

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

  /** The logical name of this gateway sender description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer alertThreshold;
  private Boolean batchConflationEnabled;
  private Integer batchSize;
  private Integer batchTimeInterval;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private Integer dispatcherThreads;
  private List<String> gatewayEventFilters;
  private Set<String> gatewayTransportFilters;
  private Boolean manualStart;
  private Integer maximumQueueMemory;
  private Boolean parallel;
  private Boolean persistenceEnabled;
  private String remoteDistributedSystemsClass;
  private String remoteDistributedSystemsMethod;
  private OrderPolicy orderPolicy;
  private Integer socketBufferSize;
  private Integer socketReadTimeout;

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
   * Returns the disk synchronous.
   */
  private Boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  /**
   * Sets the disk synchronous.
   */
  private void setDiskSynchronous(Boolean bool) {
    this.diskSynchronous = bool;
  }

  /**
   * Returns the dispatcher threads.
   */
  private Integer getDispatcherThreads() {
    return this.dispatcherThreads;
  }

  /**
   * Sets the dispatcher threads.
   */
  private void setDispatcherThreads(Integer i) {
    this.dispatcherThreads = i;
  }

  /**
   * Returns the classnames of the gateway event filters.
   */
  private List<String> getGatewayEventFilters() {
    return this.gatewayEventFilters;
  }

  /**
   * Sets the classnames of the gateway event filters.
   */
  private void setGatewayEventFilters(List<String> classnames) {
    this.gatewayEventFilters = classnames;
  }

  /**
   * Returns the classnames of the gateway transport filters.
   */
  private Set<String> getGatewayTransportFilters() {
    return this.gatewayTransportFilters;
  }

  /**
   * Sets the classnames of the gateway transport filters.
   */
  private void setGatewayTransportFilters(Set<String> classnames) {
    this.gatewayTransportFilters = classnames;
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
   * Returns whether to send in parallel.
   */
  public Boolean getParallel() {
    return this.parallel;
  }

  /**
   * Sets whether to send in parallel. 
   */
  private void setParallel(Boolean bool) {
    this.parallel = bool;
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
   * Returns the remote distributed systems algorithm signature.
   */
  private String getRemoteDistributedSystemsAlgorithm() {
    return this.getRemoteDistributedSystemsClass() + "."
         + this.getRemoteDistributedSystemsMethod() + "()";
  }

  /**
   * Returns the remote distributed systems class.
   */
  private String getRemoteDistributedSystemsClass() {
    return this.remoteDistributedSystemsClass;
  }

  /**
   * Sets the remote distributed systems class.
   */
  private void setRemoteDistributedSystemsClass(String str) {
    this.remoteDistributedSystemsClass = str;
  }

  /**
   * Returns the remote distributed systems method.
   */
  private String getRemoteDistributedSystemsMethod() {
    return this.remoteDistributedSystemsMethod;
  }

  /**
   * Sets the remote distributed systems method.
   */
  private void setRemoteDistributedSystemsMethod(String str) {
    this.remoteDistributedSystemsMethod = str;
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

//------------------------------------------------------------------------------
// Gateway sender configuration
//------------------------------------------------------------------------------

  /**
   * Configures a gateway sender factory using this description.
   */
  protected void configure(GatewaySenderFactory f) {
    f.setAlertThreshold(this.getAlertThreshold().intValue());
    f.setBatchConflationEnabled(this.getBatchConflationEnabled().booleanValue());
    f.setBatchSize(this.getBatchSize().intValue());
    f.setBatchTimeInterval(this.getBatchTimeInterval().intValue());
    if (this.getDiskStoreDescription() != null) {
      f.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    f.setDiskSynchronous(this.getDiskSynchronous().booleanValue());
    f.setDispatcherThreads(this.getDispatcherThreads().intValue());
    List<GatewayEventFilter> gefs = this.getGatewayEventFilterInstances();
    if (gefs != null) {
      for (GatewayEventFilter gef : gefs) {
        f.addGatewayEventFilter(gef);
      }
    }
    List<GatewayTransportFilter> gtfs =
                                 this.getGatewayTransportFilterInstances();
    if (gtfs != null) {
      for (GatewayTransportFilter gtf : gtfs) {
        f.addGatewayTransportFilter(gtf);
      }
    }
    f.setManualStart(this.getManualStart().booleanValue());
    f.setMaximumQueueMemory(this.getMaximumQueueMemory().intValue());
    f.setParallel(this.getParallel().booleanValue());
    f.setPersistenceEnabled(this.getPersistenceEnabled().booleanValue());
    if (this.getOrderPolicy() != null) {
      f.setOrderPolicy(this.getOrderPolicy());
    }
    f.setSocketBufferSize(this.getSocketBufferSize().intValue());
    f.setSocketReadTimeout(this.getSocketReadTimeout().intValue());
  }

  /**
   * Returns the gateway sender as a string.  For use only by {@link
   * GatewaySenderHelper#gatewaySenderToString(Gateway)}.
   */
  protected static synchronized String gatewaySenderToString(GatewaySender gs) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  id: " + gs.getId());
    buf.append("\n  alertThreshold: " + gs.getAlertThreshold());
    buf.append("\n  batchConflationEnabled: " + gs.isBatchConflationEnabled());
    buf.append("\n  batchSize: " + gs.getBatchSize());
    buf.append("\n  batchTimeInterval: " + gs.getBatchTimeInterval());
    buf.append("\n  diskStoreName: " + gs.getDiskStoreName());
    buf.append("\n  diskSynchronous: " + gs.isDiskSynchronous());
    buf.append("\n  dispatcherThreads: " + gs.getDispatcherThreads());
    buf.append("\n  gatewayEventFilters: "
              + gatewayEventFiltersFor(gs.getGatewayEventFilters()));
    buf.append("\n  gatewayTransportFilters: "
              + gatewayTransportFiltersFor(gs.getGatewayTransportFilters()));
    buf.append("\n  manualStart: " + gs.isManualStart());
    buf.append("\n  maximumQueueMemory: " + gs.getMaximumQueueMemory());
    buf.append("\n  orderPolicy: " + gs.getOrderPolicy());
    buf.append("\n  parallel: " + gs.isParallel());
    buf.append("\n  persistenceEnabled: " + gs.isPersistenceEnabled());
    int dsid = gs.getRemoteDSId();
    buf.append("\n  remoteDistributedSystem: " + dsid + " ("
              + DistributedSystemHelper.getDistributedSystemName(dsid) + ")");
    buf.append("\n  socketBufferSize: " + gs.getSocketBufferSize());
    buf.append("\n  socketReadTimeout: " + gs.getSocketReadTimeout());
    return buf.toString();
  }

  /**
   * Returns the result of invoking the remote distributed systems algorithm.
   */
  protected Set<String> getRemoteDistributedSystems() {
    MethExecutorResult result =
        MethExecutor.execute(this.getRemoteDistributedSystemsClass(),
                             this.getRemoteDistributedSystemsMethod());
    if (result.getStackTrace() != null){
      throw new HydraRuntimeException(result.toString());
    }
    return (Set<String>)result.getResult();
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
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "dispatcherThreads", this.getDispatcherThreads());
    map.put(header + "gatewayEventFilters", this.getGatewayEventFilters());
    map.put(header + "gatewayTransportFilters", this.getGatewayTransportFilters());
    map.put(header + "manualStart", this.getManualStart());
    map.put(header + "maximumQueueMemory", this.getMaximumQueueMemory());
    map.put(header + "parallel", this.getParallel());
    map.put(header + "persistenceEnabled", this.getPersistenceEnabled());
    map.put(header + "remoteDistributedSystemsAlgorithm", this.getRemoteDistributedSystemsAlgorithm());
    map.put(header + "orderPolicy", this.getOrderPolicy());
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
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each gateway sender name
    Vector names = tab.vecAt(GatewaySenderPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway sender description from test configuration parameters
      GatewaySenderDescription gsd = createGatewaySenderDescription(
                                                        name, config, i);

      // save configuration
      config.addGatewaySenderDescription(gsd);
    }
  }

  /**
   * Creates the gateway sender description using test configuration parameters
   * and product defaults.
   */
  private static GatewaySenderDescription createGatewaySenderDescription(
                 String name, TestConfig config, int index) {

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
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = GatewaySenderPrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        gsd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        gsd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // diskSynchronous
    {
      Long key = GatewaySenderPrms.diskSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;
      }
      gsd.setDiskSynchronous(bool);
    }
    // dispatcherThreads
    {
      Long key = GatewaySenderPrms.dispatcherThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_DISPATCHER_THREADS;
      }
      gsd.setDispatcherThreads(i);
    }
    // gateway event filters
    {
      Long key = GatewaySenderPrms.gatewayEventFilters;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          gsd.setGatewayEventFilters(new ArrayList(strs));
        }
      }
    }
    // gateway transport filters
    {
      Long key = GatewaySenderPrms.gatewayTransportFilters;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          gsd.setGatewayTransportFilters(new LinkedHashSet<String>(strs));
        }
      }
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
    // parallel
    {
      Long key = GatewaySenderPrms.parallel;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_IS_PARALLEL;
      }
      gsd.setParallel(bool);
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
    // remoteDistributedSystemsAlgorithm
    {
      Long key = GatewaySenderPrms.remoteDistributedSystemsAlgorithm;
      Vector strs = tab.vecAtWild(key, index, null);
      gsd.setRemoteDistributedSystemsClass(
          getRemoteDistributedSystemsClass(strs, key));
      gsd.setRemoteDistributedSystemsMethod(
          getRemoteDistributedSystemsMethod(strs, key));
    }
    // orderPolicy
    {
      Long key = GatewaySenderPrms.orderPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      gsd.setOrderPolicy(getOrderPolicy(str, key, gsd.getDispatcherThreads()));
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
    // require disk configuration in all cases
    {
      if (gsd.getDiskStoreName() == null) {
        String s = BasePrms.nameForKey(GatewaySenderPrms.names) + " ("
                 + gsd.getName() + ") requires a "
                 + BasePrms.nameForKey(GatewaySenderPrms.diskStoreName);
        throw new HydraRuntimeException(s);
      }
    }
    return gsd;
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
// Gateway event filter support
//------------------------------------------------------------------------------

  /**
   * Returns the classnames for the gateway event filters in the list.
   */
  private static List gatewayEventFiltersFor(List<GatewayEventFilter> gefs) {
    List classnames = new ArrayList();
    for (Object gef : gefs) {
      classnames.add(gef.getClass().getName());
    }
    return classnames;
  }

  /**
   * Returns a gateway event filter instance for each classname.
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement GatewayEventFilter.
   */
  private List<GatewayEventFilter> getGatewayEventFilterInstances() {
    List<String> classnames = this.getGatewayEventFilters();
    if (classnames == null) {
      return null;
    }
    Long key = GatewaySenderPrms.gatewayEventFilters;
    List<GatewayEventFilter> instances = new ArrayList();
    for (String classname : classnames) {
      try {
        GatewayEventFilter gef =
              (GatewayEventFilter)getInstance(key, classname);
        instances.add(gef);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key)
                 + " does not implement GatewayEventFilter: " + classname;
        throw new HydraConfigException(s);
      }
    }
    return instances;
  }

//------------------------------------------------------------------------------
// Gateway transport filter support
//------------------------------------------------------------------------------

  /**
   * Returns the classnames for the gateway transport filters in the list.
   */
  private static List gatewayTransportFiltersFor(List<GatewayTransportFilter> gtfs) {
    List classnames = new ArrayList();
    for (Object gtf : gtfs) {
      classnames.add(gtf.getClass().getName());
    }
    return classnames;
  }

  /**
   * Returns a gateway transport filter instance for each classname.
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement GatewayTransportFilter.
   */
  private List<GatewayTransportFilter> getGatewayTransportFilterInstances() {
    Set<String> classnames = this.getGatewayTransportFilters();
    if (classnames == null) {
      return null;
    }
    Long key = GatewaySenderPrms.gatewayTransportFilters;
    List<GatewayTransportFilter> instances = new ArrayList();
    for (String classname : classnames) {
      try {
        GatewayTransportFilter gtf =
              (GatewayTransportFilter)getInstance(key, classname);
        instances.add(gtf);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key)
                 + " does not implement GatewayTransportFilter: " + classname;
        throw new HydraConfigException(s);
      }
    }
    return instances;
  }

//------------------------------------------------------------------------------
// Order policy configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the OrderPolicy for the given string.
   */
  private static OrderPolicy getOrderPolicy(String str, Long key, int threads) {
    if (str == null) {
      return (threads <= 1) ? null : GatewaySender.DEFAULT_ORDER_POLICY;

    } else if (str.equalsIgnoreCase("key")) {
      return OrderPolicy.KEY;

    } else if (str.equalsIgnoreCase("partition")) {
      return OrderPolicy.PARTITION;

    } else if (str.equalsIgnoreCase("thread")) {
      return OrderPolicy.THREAD;

    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Remote distributed systems algorithm configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the class for the given remote distributed systems algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getRemoteDistributedSystemsClass(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "hydra.GatewaySenderHelper";
    } else if (strs.size() == 2) {
      return (String)strs.get(0);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the method for the given remote distributed systems algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getRemoteDistributedSystemsMethod(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "getRemoteDistributedSystems";
    } else if (strs.size() == 2) {
      return (String)strs.get(1);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
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
    this.batchConflationEnabled = (Boolean)in.readObject();
    this.batchSize = (Integer)in.readObject();
    this.batchTimeInterval = (Integer)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    this.dispatcherThreads = (Integer)in.readObject();
    this.gatewayEventFilters = (List<String>)in.readObject();
    this.gatewayTransportFilters = (Set<String>)in.readObject();
    this.manualStart = (Boolean)in.readObject();
    this.maximumQueueMemory = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.orderPolicy = getOrderPolicy((String)in.readObject(),
                                       GatewaySenderPrms.orderPolicy,
                                       this.dispatcherThreads);
    this.parallel = (Boolean)in.readObject();
    this.persistenceEnabled = (Boolean)in.readObject();
    this.remoteDistributedSystemsClass = (String)in.readObject();
    this.remoteDistributedSystemsMethod = (String)in.readObject();
    this.socketBufferSize = (Integer)in.readObject();
    this.socketReadTimeout = (Integer)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.alertThreshold);
    out.writeObject(this.batchConflationEnabled);
    out.writeObject(this.batchSize);
    out.writeObject(this.batchTimeInterval);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskStoreName);
    out.writeObject(this.diskSynchronous);
    out.writeObject(this.dispatcherThreads);
    out.writeObject(this.gatewayEventFilters);
    out.writeObject(this.gatewayTransportFilters);
    out.writeObject(this.manualStart);
    out.writeObject(this.maximumQueueMemory);
    out.writeObject(this.name);
    if (this.orderPolicy == null) {
      out.writeObject(this.orderPolicy);
    } else {
      out.writeObject(this.orderPolicy.toString());
    }
    out.writeObject(this.parallel);
    out.writeObject(this.persistenceEnabled);
    out.writeObject(this.remoteDistributedSystemsClass);
    out.writeObject(this.remoteDistributedSystemsMethod);
    out.writeObject(this.socketBufferSize);
    out.writeObject(this.socketReadTimeout);
  }
}
