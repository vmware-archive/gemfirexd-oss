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

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySender;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create an async event queue.
 */
public class AsyncEventQueueDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this async event queue description and actual id */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String asyncEventListener;
  private Boolean batchConflationEnabled;
  private Integer batchSize;
  private Integer batchTimeInterval;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private Integer dispatcherThreads;
  private Integer maximumQueueMemory;
  private OrderPolicy orderPolicy;
  private Boolean parallel;
  private Boolean persistent;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public AsyncEventQueueDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this async event queue description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this async event queue description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the classname of the async event listener.
   */
  private String getAsyncEventListener() {
    return this.asyncEventListener;
  }

  /**
   * Sets the classname of the async event listener.
   */
  private void setAsyncEventListener(String classname) {
    this.asyncEventListener = classname;
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
   * Returns the persistent.
   */
  private Boolean getPersistent() {
    return this.persistent;
  }

  /**
   * Sets the persistent.
   */
  private void setPersistent(Boolean bool) {
    this.persistent = bool;
  }

//------------------------------------------------------------------------------
// Async event queue configuration
//------------------------------------------------------------------------------

  /**
   * Configures an async event queue factory using this description.
   */
  protected void configure(AsyncEventQueueFactory f) {
    f.setBatchConflationEnabled(this.getBatchConflationEnabled().booleanValue());
    f.setBatchSize(this.getBatchSize().intValue());
    f.setBatchTimeInterval(this.getBatchTimeInterval().intValue());
    if (this.getDiskStoreDescription() != null) {
      f.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    f.setDiskSynchronous(this.getDiskSynchronous().booleanValue());
    f.setDispatcherThreads(this.getDispatcherThreads().intValue());
    f.setMaximumQueueMemory(this.getMaximumQueueMemory().intValue());
    if (this.getOrderPolicy() != null) {
      f.setOrderPolicy(this.getOrderPolicy());
    }
    f.setParallel(this.getParallel().booleanValue());
    f.setPersistent(this.getPersistent().booleanValue());
  }

  /**
   * Returns the async event queue as a string.  For use only by {@link
   * AsyncEventQueueHelper#asyncEventQueueToString(AsyncEventQueue)}.
   */
  protected static synchronized String asyncEventQueueToString(
                                       AsyncEventQueue q) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  id: " + q.getId());
    buf.append("\n  asyncEventListener: "
       + asyncEventListenerFor(q.getAsyncEventListener()));
    buf.append("\n  batchConflationEnabled: " + q.isBatchConflationEnabled());
    buf.append("\n  batchSize: " + q.getBatchSize());
    buf.append("\n  batchTimeInterval: " + q.getBatchTimeInterval());
    buf.append("\n  diskStoreName: " + q.getDiskStoreName());
    buf.append("\n  diskSynchronous: " + q.isDiskSynchronous());
    buf.append("\n  dispatcherThreads: " + q.getDispatcherThreads());
    buf.append("\n  maximumQueueMemory: " + q.getMaximumQueueMemory());
    buf.append("\n  orderPolicy: " + q.getOrderPolicy());
    buf.append("\n  parallel: " + q.isParallel());
    buf.append("\n  persistent: " + q.isPersistent());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "asyncEventListener", this.getAsyncEventListener());
    map.put(header + "batchConflationEnabled", this.getBatchConflationEnabled());
    map.put(header + "batchSize", this.getBatchSize());
    map.put(header + "batchTimeInterval", this.getBatchTimeInterval());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "dispatcherThreads", this.getDispatcherThreads());
    map.put(header + "maximumQueueMemory", this.getMaximumQueueMemory());
    map.put(header + "orderPolicy", this.getOrderPolicy());
    map.put(header + "parallel", this.getParallel());
    map.put(header + "persistent", this.getPersistent());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates async event queue descriptions from the async event queue
   * parameters in the test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each async event queue name
    Vector names = tab.vecAt(AsyncEventQueuePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create async event queue description from test configuration parameters
      AsyncEventQueueDescription aeqd = createAsyncEventQueueDescription(
                                                        name, config, i);

      // save configuration
      config.addAsyncEventQueueDescription(aeqd);
    }
  }

  /**
   * Creates the async event queue description using test configuration
   * parameters and product defaults.
   */
  private static AsyncEventQueueDescription createAsyncEventQueueDescription(
                 String name, TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    AsyncEventQueueDescription aeqd = new AsyncEventQueueDescription();
    aeqd.setName(name);

    // async event listener
    {
      Long key = AsyncEventQueuePrms.asyncEventListener;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(AsyncEventQueuePrms.names) + " ("
                 + aeqd.getName() + ") requires a "
                 + BasePrms.nameForKey(AsyncEventQueuePrms.asyncEventListener);
        throw new HydraRuntimeException(s);
      }
      aeqd.setAsyncEventListener(str);
    }
    // batchConflationEnabled
    {
      Long key = AsyncEventQueuePrms.batchConflationEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_BATCH_CONFLATION;
      }
      aeqd.setBatchConflationEnabled(bool);
    }
    // batchSize
    {
      Long key = AsyncEventQueuePrms.batchSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_BATCH_SIZE;
      }
      aeqd.setBatchSize(i);
    }
    // batchTimeInterval
    {
      Long key = AsyncEventQueuePrms.batchTimeInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;
      }
      aeqd.setBatchTimeInterval(i);
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = AsyncEventQueuePrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        aeqd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        aeqd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // diskSynchronous
    {
      Long key = AsyncEventQueuePrms.diskSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;
      }
      aeqd.setDiskSynchronous(bool);
    }
    // dispatcherThreads
    {
      Long key = AsyncEventQueuePrms.dispatcherThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_DISPATCHER_THREADS;
      }
      aeqd.setDispatcherThreads(i);
    }
    // maximumQueueMemory
    {
      Long key = AsyncEventQueuePrms.maximumQueueMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
      }
      aeqd.setMaximumQueueMemory(i);
    }
    // orderPolicy
    {
      Long key = AsyncEventQueuePrms.orderPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      aeqd.setOrderPolicy(getOrderPolicy(str, key, aeqd.getDispatcherThreads()));
    }
    // parallel
    {
      Long key = AsyncEventQueuePrms.parallel;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_IS_PARALLEL;
      }
      aeqd.setParallel(bool);
    }
    // persistent
    {
      Long key = AsyncEventQueuePrms.persistent;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
      }
      aeqd.setPersistent(bool);
    }
    // require disk configuration in all cases
    {
      if (aeqd.getDiskStoreName() == null) {
        String s = BasePrms.nameForKey(AsyncEventQueuePrms.names) + " ("
                 + aeqd.getName() + ") requires a "
                 + BasePrms.nameForKey(AsyncEventQueuePrms.diskStoreName);
        throw new HydraRuntimeException(s);
      }
    }
    return aeqd;
  }

//------------------------------------------------------------------------------
// Async event listener support
//------------------------------------------------------------------------------

  /**
   * Returns the classname for the async event listener.
   */
  private static String asyncEventListenerFor(AsyncEventListener ael) {
    return ael.getClass().getName();
  }

  /**
   * Returns a async event listener instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement AsyncEventListener.
   */
  protected AsyncEventListener getAsyncEventListenerInstance() {
    String classname = this.getAsyncEventListener();
    if (classname == null) {
      return null;
    }
    Long key = AsyncEventQueuePrms.asyncEventListener;
    try {
      return (AsyncEventListener)getInstance(key, classname);
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
               + " does not implement AsyncEventListener: "
               + classname;
      throw new HydraConfigException(s);
    }
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
// Version support
//------------------------------------------------------------------------------

  /**
   * Custom deserialization.
   */
  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.asyncEventListener = (String)in.readObject();
    this.batchConflationEnabled = (Boolean)in.readObject();
    this.batchSize = (Integer)in.readObject();
    this.batchTimeInterval = (Integer)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    this.dispatcherThreads = (Integer)in.readObject();
    this.maximumQueueMemory = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.orderPolicy = getOrderPolicy((String)in.readObject(),
                                       AsyncEventQueuePrms.orderPolicy,
                                       this.dispatcherThreads);
    this.parallel = (Boolean)in.readObject();
    this.persistent = (Boolean)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.asyncEventListener);
    out.writeObject(this.batchConflationEnabled);
    out.writeObject(this.batchSize);
    out.writeObject(this.batchTimeInterval);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskStoreName);
    out.writeObject(this.diskSynchronous);
    out.writeObject(this.dispatcherThreads);
    out.writeObject(this.maximumQueueMemory);
    out.writeObject(this.name);
    if (this.orderPolicy == null) {
      out.writeObject(this.orderPolicy);
    } else {
      out.writeObject(this.orderPolicy.toString());
    }
    out.writeObject(this.parallel);
    out.writeObject(this.persistent);
  }
}
