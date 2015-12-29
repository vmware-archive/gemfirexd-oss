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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;

import java.io.*;
import java.util.*;

/**
 * Encodes information needed to describe and create a region.
 */
public class ClientRegionDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this region description */
  private String name;

  /** Map of logical region configuration names to maps of cache listeners. */
  private static Map cacheListenerInstanceMaps = new HashMap();

  /** Remaining parameters, in alphabetical order */
  private List cacheListeners;
  private Boolean cacheListenersSingleton;
  private ClientRegionShortcut clientRegionShortcut;
  private Boolean cloningEnabled;
  private Integer concurrencyLevel;
  private String customEntryIdleTimeout;
  private String customEntryTimeToLive;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private ExpirationAttributes entryIdleTimeout;
  private ExpirationAttributes entryTimeToLive;
  private EvictionAttributes evictionAttributes;
  private Integer initialCapacity;
  private Class keyConstraint;
  private Float loadFactor;
  private PoolDescription poolDescription; // from poolName
  private String poolDescriptionName;
  private ExpirationAttributes regionIdleTimeout;
  private String regionName;
  private ExpirationAttributes regionTimeToLive;
  private Boolean statisticsEnabled;
  private Class valueConstraint;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public ClientRegionDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this region description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this region description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the classnames of the cache listeners.
   */
  private List getCacheListeners() {
    return this.cacheListeners;
  }

  /**
   * Sets the classnames of the cache listeners.
   */
  private void setCacheListeners(List classnames) {
    this.cacheListeners = classnames;
  }

  /**
   * Returns the cache listeners singleton.
   */
  private Boolean getCacheListenersSingleton() {
    return this.cacheListenersSingleton;
  }

  /**
   * Sets the cache listeners singleton.
   */
  private void setCacheListenersSingleton(Boolean bool) {
    this.cacheListenersSingleton = bool;
  }

  /**
   * Returns the client region shortcut
   */
  public ClientRegionShortcut getClientRegionShortcut() {
    return this.clientRegionShortcut;
  }

  /**
   * Sets the client region shortcut
   */
  private void setClientRegionShortcut(ClientRegionShortcut shortcut) {
    this.clientRegionShortcut = shortcut;
  }

  /**
   * Returns the cloning enabled.
   */
  private Boolean getCloningEnabled() {
    return this.cloningEnabled;
  }

  /**
   * Sets the cloning enabled.
   */
  private void setCloningEnabled(Boolean bool) {
    this.cloningEnabled = bool;
  }

  /**
   * Returns the concurrency level.
   */
  private Integer getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  /**
   * Sets the concurrency level.
   */
  private void setConcurrencyLevel(Integer i) {
    this.concurrencyLevel = i;
  }

  /**
   * Returns the classname for the custom entry idle timeout.
   */
  private String getCustomEntryIdleTimeout() {
    return this.customEntryIdleTimeout;
  }

  /**
   * Sets the classname for the custom entry idle timeout.
   */
  private void setCustomEntryIdleTimeout(String str) {
    this.customEntryIdleTimeout = str;
  }

  /**
   * Returns the classname for the custom entry time to live.
   */
  private String getCustomEntryTimeToLive() {
    return this.customEntryTimeToLive;
  }

  /**
   * Sets the classname for the custom entry time to live.
   */
  private void setCustomEntryTimeToLive(String str) {
    this.customEntryTimeToLive = str;
  }

  /**
   * Returns the disk store description name.
   */
  public String getDiskStoreName() {
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
   * Returns the expiration attributes for the entry idle timeout.
   */
  private ExpirationAttributes getEntryIdleTimeout() {
    return this.entryIdleTimeout;
  }

  /**
   * Sets the expiration attributes for the entry idle timeout.
   */
  private void setEntryIdleTimeout(ExpirationAttributes attributes) {
    this.entryIdleTimeout = attributes;
  }

  /**
   * Returns the expiration attributes for the entry time to live.
   */
  private ExpirationAttributes getEntryTimeToLive() {
    return this.entryTimeToLive;
  }

  /**
   * Sets the expiration attributes for the entry time to live.
   */
  private void setEntryTimeToLive(ExpirationAttributes attributes) {
    this.entryTimeToLive = attributes;
  }

  /**
   * Returns the eviction attributes.
   */
  public EvictionAttributes getEvictionAttributes() {
    return this.evictionAttributes;
  }

  /**
   * Sets the eviction attributes.
   */
  private void setEvictionAttributes(EvictionAttributes attributes) {
    this.evictionAttributes = attributes;
  }

  /**
   * Returns the initial capacity.
   */
  private Integer getInitialCapacity() {
    return this.initialCapacity;
  }

  /**
   * Sets the initial capacity.
   */
  private void setInitialCapacity(Integer i) {
    this.initialCapacity = i;
  }

  /**
   * Returns the class of the key constraint.
   */
  private Class getKeyConstraint() {
    return this.keyConstraint;
  }

  /**
   * Sets the class of the key constraint.
   */
  private void setKeyConstraint(Class cls) {
    this.keyConstraint = cls;
  }

  /**
   * Returns the load factor.
   */
  private Float getLoadFactor() {
    return this.loadFactor;
  }

  /**
   * Sets the load factor.
   */
  private void setLoadFactor(Float f) {
    this.loadFactor = f;
  }

  /**
   * Returns the pool description name.
   */
  private String getPoolDescriptionName() {
    return this.poolDescriptionName;
  }

  /**
   * Sets the pool description name.
   */
  private void setPoolDescriptionName(String str) {
    this.poolDescriptionName = str;
  }

  /**
   * Returns the pool description.
   */
  public PoolDescription getPoolDescription() {
    return this.poolDescription;
  }

  /**
   * Sets the pool description.
   */
  private void setPoolDescription(PoolDescription pd) {
    this.poolDescription = pd;
  }

  /**
   * Returns the expiration attributes for the region idle timeout.
   */
  private ExpirationAttributes getRegionIdleTimeout() {
    return this.regionIdleTimeout;
  }

  /**
   * Sets the expiration attributes for the region idle timeout.
   */
  private void setRegionIdleTimeout(ExpirationAttributes attributes) {
    this.regionIdleTimeout = attributes;
  }

  /**
   * Returns the actual name of this region.
   */
  public String getRegionName() {
    return this.regionName;
  }

  /**
   * Sets the actual name of this region.
   */
  private void setRegionName(String str) {
    this.regionName = str;
  }

  /**
   * Returns the expiration attributes for the region time to live.
   */
  private ExpirationAttributes getRegionTimeToLive() {
    return this.regionTimeToLive;
  }

  /**
   * Sets the expiration attributes for the region time to live.
   */
  private void setRegionTimeToLive(ExpirationAttributes attributes) {
    this.regionTimeToLive = attributes;
  }

  /**
   * Returns the statistics enabled.
   */
  private Boolean getStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  /**
   * Sets the statistics enabled.
   */
  private void setStatisticsEnabled(Boolean bool) {
    this.statisticsEnabled = bool;
  }

  /**
   * Returns the class of the value constraint.
   */
  private Class getValueConstraint() {
    return this.valueConstraint;
  }

  /**
   * Sets the class of the value constraint.
   */
  private void setValueConstraint(Class cls) {
    this.valueConstraint = cls;
  }

//------------------------------------------------------------------------------
// Region configuration
//------------------------------------------------------------------------------

  /**
   * Configures the client region factory using this client region description.
   * Has the option to skip instantiating cache listeners when 
   * creating dummy regions to generate cache XML files.
   */
  protected void configure(ClientRegionFactory f, boolean instantiate) {
    f.initCacheListeners(this.getCacheListenerInstances(instantiate));
    if (this.cloningEnabled != null) {
      f.setCloningEnabled(this.cloningEnabled.booleanValue());
    }
    if (this.concurrencyLevel != null) {
      f.setConcurrencyLevel(this.concurrencyLevel.intValue());
    }
    if (this.customEntryIdleTimeout !=  null) {
       f.setCustomEntryIdleTimeout(getCustomEntryIdleTimeoutInstance());
    }
    if (this.customEntryTimeToLive != null) {
      f.setCustomEntryTimeToLive(getCustomEntryTimeToLiveInstance());
    }
    if (this.diskStoreDescription != null) {
      f.setDiskStoreName(this.diskStoreDescription.getName());
    }
    if (this.diskSynchronous != null) {
      f.setDiskSynchronous(this.diskSynchronous.booleanValue());
    }
    if (this.entryIdleTimeout != null) {
      f.setEntryIdleTimeout(this.entryIdleTimeout);
    }
    if (this.entryTimeToLive != null) {
      f.setEntryTimeToLive(this.entryTimeToLive);
    }
    if (this.evictionAttributes != null) {
      f.setEvictionAttributes(this.evictionAttributes);
    }
    if (this.initialCapacity != null) {
      f.setInitialCapacity(this.initialCapacity.intValue());
    }
    if (this.keyConstraint != null ) {
      f.setKeyConstraint(this.keyConstraint);
    }
    if (this.loadFactor != null) {
      f.setLoadFactor(this.loadFactor.floatValue());
    }
    if (this.poolDescription != null) {
      f.setPoolName(this.poolDescription.getName());
    }
    if (this.regionIdleTimeout != null) {
      f.setRegionIdleTimeout(this.regionIdleTimeout);
    }
    if (this.regionTimeToLive != null) {
      f.setRegionTimeToLive(this.regionTimeToLive);
    }
    if (this.statisticsEnabled != null) {
      f.setStatisticsEnabled(this.statisticsEnabled.booleanValue());
    }
    if (this.valueConstraint != null) {
      f.setValueConstraint(this.valueConstraint);
    }
  }

  /**
   * Configures the region attributes using this client region description.
   * Has the option to skip instantiating cache listeners when 
   * creating dummy regions to generate cache XML files.
   */
  public void configure(RegionAttributesCreation attrs, boolean instantiate) {
    if (this.cacheListeners != null) {
      attrs.initCacheListeners(this.getCacheListenerInstances(instantiate));
    }
    if (this.cloningEnabled != null) {
      attrs.setCloningEnable(this.cloningEnabled.booleanValue());
    }
    if (this.concurrencyLevel != null) {
      attrs.setConcurrencyLevel(this.concurrencyLevel.intValue());
    }
    if (this.customEntryIdleTimeout !=  null) {
       attrs.setCustomEntryIdleTimeout(getCustomEntryIdleTimeoutInstance());
    }
    if (this.customEntryTimeToLive != null) {
      attrs.setCustomEntryTimeToLive(getCustomEntryTimeToLiveInstance());
    }
    if (this.diskStoreDescription != null) {
      attrs.setDiskStoreName(this.diskStoreDescription.getName());
    }
    if (this.diskSynchronous != null) {
      attrs.setDiskSynchronous(this.diskSynchronous.booleanValue());
    }
    if (this.entryIdleTimeout != null) {
      attrs.setEntryIdleTimeout(this.entryIdleTimeout);
    }
    if (this.entryTimeToLive != null) {
      attrs.setEntryTimeToLive(this.entryTimeToLive);
    }
    if (this.evictionAttributes != null) {
      attrs.setEvictionAttributes(this.evictionAttributes);
    }
    if (this.initialCapacity != null) {
      attrs.setInitialCapacity(this.initialCapacity.intValue());
    }
    if (this.keyConstraint != null ) {
      attrs.setKeyConstraint(this.keyConstraint);
    }
    if (this.loadFactor != null) {
      attrs.setLoadFactor(this.loadFactor.floatValue());
    }
    if (this.poolDescription != null) {
      attrs.setPoolName(this.poolDescription.getName());
    }
    if (this.regionIdleTimeout != null) {
      attrs.setRegionIdleTimeout(this.regionIdleTimeout);
    }
    if (this.regionTimeToLive != null) {
      attrs.setRegionTimeToLive(this.regionTimeToLive);
    }
    if (this.statisticsEnabled != null) {
      attrs.setStatisticsEnabled(this.statisticsEnabled.booleanValue());
    }
    if (this.valueConstraint != null) {
      attrs.setValueConstraint(this.valueConstraint);
    }
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "cacheListeners", this.getCacheListeners());
    map.put(header + "cacheListenersSingleton", this.getCacheListenersSingleton());
    map.put(header + "clientRegionShortcut", this.getClientRegionShortcut());
    map.put(header + "cloningEnabled", this.getCloningEnabled());
    map.put(header + "concurrencyLevel", this.getConcurrencyLevel());
    map.put(header + "customEntryIdleTimeout", this.getCustomEntryIdleTimeout());
    map.put(header + "customEntryTimeToLive", this.getCustomEntryTimeToLive());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "entryIdleTimeout", this.getEntryIdleTimeout());
    map.put(header + "entryTimeToLive", this.getEntryTimeToLive());
    map.put(header + "evictionAttributes", this.getEvictionAttributes());
    map.put(header + "initialCapacity", this.getInitialCapacity());
    map.put(header + "keyConstraint", this.getKeyConstraint());
    map.put(header + "loadFactor", this.getLoadFactor());
    map.put(header + "poolName", this.getPoolDescriptionName());
    map.put(header + "regionIdleTimeout", this.getRegionIdleTimeout());
    map.put(header + "regionName", this.getRegionName());
    map.put(header + "regionTimeToLive", this.getRegionTimeToLive());
    map.put(header + "statisticsEnabled", this.getStatisticsEnabled());
    map.put(header + "valueConstraint", this.getValueConstraint());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates region descriptions from the region parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each region name
    Vector names = tab.vecAt(ClientRegionPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create region description from test configuration parameters
      // and do hydra-level validation
      ClientRegionDescription rd = createClientRegionDescription(name, config, i);

      // save configuration
      config.addClientRegionDescription(rd);
    }
  }

  /**
   * Creates the initial client region description using test configuration 
   * parameters.
   */
  private static ClientRegionDescription createClientRegionDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    ClientRegionDescription rd = new ClientRegionDescription();
    rd.setName(name);

    // cacheListeners
    {
      Long key = ClientRegionPrms.cacheListeners;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          rd.setCacheListeners(new ArrayList(strs));
        }
      }
    }
    // cacheListenersSingleton
    {
      Long key = ClientRegionPrms.cacheListenersSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        rd.setCacheListenersSingleton(Boolean.FALSE);
      } else {
        rd.setCacheListenersSingleton(bool);
      }
    }
    // clientRegionShortcut
    {
      Long key = ClientRegionPrms.clientRegionShortcut;
      String str = tab.getString(key, tab.getWild(key, index, null));
      rd.setClientRegionShortcut(getClientRegionShortcut(str, key));
    }
    // cloningEnabled
    {
      Long key = ClientRegionPrms.cloningEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setCloningEnabled(bool);
      }
    }
    // concurrencyLevel
    {
      Long key = ClientRegionPrms.concurrencyLevel;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        rd.setConcurrencyLevel(i);
      }
    }
    // customEntryIdleTimeout
    {
      Long key = ClientRegionPrms.customEntryIdleTimeout;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCustomEntryIdleTimeout(str);
      }
    }
    // customEntryTimeToLive
    {
      Long key = ClientRegionPrms.customEntryTimeToLive;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCustomEntryTimeToLive(str);
      }
    }
    // diskStoreName 
    {
      Long key = ClientRegionPrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        rd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // diskSynchronous
    {
      Long key = ClientRegionPrms.diskSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setDiskSynchronous(bool);
      }
    }
    // entryIdleTimeout
    {
      Long key = ClientRegionPrms.entryIdleTimeout;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEntryIdleTimeout(attributes);
      }
    }
    // entryTimeToLive
    {
      Long key = ClientRegionPrms.entryTimeToLive;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEntryTimeToLive(attributes);
      }
    }
    // evictionAttributes
    {
      Long key = ClientRegionPrms.evictionAttributes;
      EvictionAttributes attributes =
        getEvictionAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEvictionAttributes(attributes);
      }
    }
    // initialCapacity
    {
      Long key = ClientRegionPrms.initialCapacity;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        rd.setInitialCapacity(i);
      }
    }
    // keyConstraint
    {
      Long key = ClientRegionPrms.keyConstraint;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setKeyConstraint(getClass(key, str));
      }
    }
    // loadFactor
    {
      Long key = ClientRegionPrms.loadFactor;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rd.setLoadFactor(Float.valueOf(d.floatValue()));
      }
    }
    // poolName (generates PoolDescription)
    {
      Long key = ClientRegionPrms.poolName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setPoolDescription(getPoolDescription(str, key, config));
        rd.setPoolDescriptionName("PoolDescription." + str);
      }
    }
    // regionIdleTimeout
    {
      Long key = ClientRegionPrms.regionIdleTimeout;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setRegionIdleTimeout(attributes);
      }
    }
    // regionName
    {
      Long key = ClientRegionPrms.regionName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        rd.setRegionName(RegionPrms.DEFAULT_REGION_NAME);
      } else {
        rd.setRegionName(str);
      }
    }
    // regionTimeToLive
    {
      Long key = ClientRegionPrms.regionTimeToLive;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setRegionTimeToLive(attributes);
      }
    }
    // statisticsEnabled
    {
      Long key = ClientRegionPrms.statisticsEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setStatisticsEnabled(bool);
      }
    }
    // valueConstraint
    {
      Long key = ClientRegionPrms.valueConstraint;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setValueConstraint(getClass(key, str));
      }
    }
    return rd;
  }

//------------------------------------------------------------------------------
// Cache listeners configuration support
//------------------------------------------------------------------------------

  /**
   * Returns a cache listener instance for each cache listener classname.
   * <p>
   * Manages singletons if instantiate is true, which means that these are real
   * runtime instances rather than test configuration instances.
   *
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement CacheListener.
   */
  private synchronized CacheListener[] getCacheListenerInstances(
                                                       boolean instantiate) {
    List classnames = this.getCacheListeners();
    if (classnames == null) {
      return null;
    } else {
      Map instanceMap = (Map)cacheListenerInstanceMaps.get(this.getName());
      Long key = ClientRegionPrms.cacheListeners;
      List listeners = new ArrayList();
      for (Iterator i = classnames.iterator(); i.hasNext();) {
        String classname = (String)i.next();
        if (instantiate && this.getCacheListenersSingleton().booleanValue()) {
          // singleton case
          if (instanceMap == null) {
            instanceMap = new HashMap();
          }
          Object instance = instanceMap.get(classname);
          if (instance == null) { // create and save a new instance
            CacheListener listener = getAppCacheListenerInstance(classname);
            instanceMap.put(classname, listener);
            listeners.add(listener);
          } else { // use the existing instance
            listeners.add(instance);
          }
        } else {
          // non-singleton case, create a new instance
          listeners.add(getAppCacheListenerInstance(classname));
        }
      }
      CacheListener[] result = new CacheListener[listeners.size()];
      for (int i = 0; i < listeners.size(); i++) {
        result[i] = (CacheListener)listeners.get(i);
      }
      return result;
    }
  }

  /**
   * Returns an application-defined cache listener instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement CacheListener.
   */
  private static CacheListener getAppCacheListenerInstance(String classname) {
    Long key = ClientRegionPrms.cacheListeners;
    Object obj = getInstance(key, classname);
    try {
      return (CacheListener)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement CacheListener: " + classname;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Client region shortcut configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the client region shortcut for the given string.
   */
  private static ClientRegionShortcut getClientRegionShortcut(String str,
                                                              Long key) {
    try { 
      return ClientRegionShortcut.valueOf(str);
    } catch (IllegalArgumentException e) {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s, e);
    } catch (NullPointerException e) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s, e);
    }
  }

//------------------------------------------------------------------------------
// Custom entry idle timeout and entry time to live support
//------------------------------------------------------------------------------

  /**
   * Returns a custom expiry instance for the custom entry idle timeout.
   */
  private synchronized CustomExpiry getCustomEntryIdleTimeoutInstance() {
    String classname = this.getCustomEntryIdleTimeout();
    if (classname == null) {
      return null;
    } else {
      return getCustomExpiry(classname, ClientRegionPrms.customEntryIdleTimeout);
    }
  }

  /**
   * Returns a custom expiry instance for the custom entry time to live.
   */
  private synchronized CustomExpiry getCustomEntryTimeToLiveInstance() {
    String classname = this.getCustomEntryTimeToLive();
    if (classname == null) {
      return null;
    } else {
      return getCustomExpiry(classname, ClientRegionPrms.customEntryTimeToLive);
    }
  }

  /**
   * Returns the CustomExpiry instance for the given string.
   */
  private static CustomExpiry getCustomExpiry(String classname, Long key) {
    if (classname.equalsIgnoreCase(BasePrms.NONE)) {
      return null;
    } else {
      try {
        return (CustomExpiry)getInstance(key, classname);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key)
                 + " does not implement CustomExpiry: " + classname;
        throw new HydraConfigException(s);
      }
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
// Eviction attributes configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the eviction attributes from the given vector, which is expected to
   * hold an algorithm field and optional support fields, or null if there is no   * eviction.
   * @throws HydraConfigException if the value is malformed or a field contains
   *         an illegal value or type.
   */
  private static EvictionAttributes getEvictionAttributes(Long key,
                                    Vector val, ConfigHashtable tab) {
    if (val == null) {
      return null;
    } else {
      // get the algorithm field
      String algorithmStr = tab.getString(key, val.get(0));
      if (algorithmStr == null) {
        return null;
      } else { // get algorithm-specific support fields
        int algorithm = getEvictionAlgorithm(algorithmStr, key);
        switch (algorithm) {
          case NONE: return null;
          case LRU_ENTRY: return getLRUEntryAttributes(key, val, tab);
          case LRU_HEAP: return getLRUHeapAttributes(key, val, tab);
          case LRU_MEMORY: return getLRUMemoryAttributes(key, val, tab);
          default: String s = "Should not happen: " + algorithmStr;
                   throw new HydraInternalException(s);
        }
      }
    }
  }

  /**
   * Returns eviction attributes from the vector for LRU entry count.
   */
  private static EvictionAttributes getLRUEntryAttributes(Long key, Vector val,
                                                          ConfigHashtable tab) {
    Integer maximum = null; // maximum entries
    EvictionAction action = null; // eviction action

    // read algorithm support fields
    if (val.size() > 1) {
      maximum = tab.getInteger(key, val.get(1));
    }
    if (val.size() > 2) {
      String actionStr = tab.getString(key, val.get(2));
      if (actionStr != null) {
        action = getEvictionAction(actionStr, key);
      }
    }
    if (val.size() > 3) {
      String s = BasePrms.nameForKey(key) + " has too many fields: " + val;
      throw new HydraConfigException(s);
    }

    // fill in defaults where needed
    if (maximum == null && action != null) {
      maximum = Integer.valueOf(EvictionAttributes.createLRUEntryAttributes()
                                                  .getMaximum());
    }

    // invoke appropriate constructor
    if (action != null) {
      return EvictionAttributes.createLRUEntryAttributes(
                     maximum.intValue(), action);
    } else if (maximum != null) {
      return EvictionAttributes.createLRUEntryAttributes(
                     maximum.intValue());
    } else {
      return EvictionAttributes.createLRUEntryAttributes();
    }
  }

  /**
   * Returns eviction attributes from the vector for LRU heap percentage.
   */
  private static EvictionAttributes getLRUHeapAttributes(Long key, Vector val,
                                                         ConfigHashtable tab) {
    EvictionAction action = null; // eviction action
    ObjectSizer sizer = null; // object sizer

    // read algorithm support fields
    if (val.size() > 1) {
      String sizerStr = tab.getString(key, val.get(1));
      if (sizerStr != null) {
        sizer = getObjectSizer(sizerStr, key);
      }
    }
    if (val.size() > 2) {
      String actionStr = tab.getString(key, val.get(2));
      if (actionStr != null) {
        action = getEvictionAction(actionStr, key);
      }
    }
    if (val.size() > 3) {
      String s = BasePrms.nameForKey(key) + " has too many fields: " + val;
      throw new HydraConfigException(s);
    }

    // invoke appropriate constructor
    if (action != null) {
      return EvictionAttributes.createLRUHeapAttributes(sizer, action);
    } else if (sizer != null) {
      return EvictionAttributes.createLRUHeapAttributes(sizer);
    } else {
      return EvictionAttributes.createLRUHeapAttributes();
    }
  }

  /**
   * Returns eviction attributes from the vector for LRU memory size.
   */
  private static EvictionAttributes getLRUMemoryAttributes(
                         Long key, Vector val, ConfigHashtable tab) {
    Integer maximum = null; // maximum megabytes
    ObjectSizer sizer = null; // object sizer
    EvictionAction action = null; // eviction action

    // read algorithm support fields
    if (val.size() > 1) {
      maximum = tab.getInteger(key, val.get(1));
    }
    if (val.size() > 2) {
      String sizerStr = tab.getString(key, val.get(2));
      if (sizerStr != null) {
        sizer = getObjectSizer(sizerStr, key);
      }
    }
    if (val.size() > 3) {
      String actionStr = tab.getString(key, val.get(3));
      if (actionStr != null) {
        action = getEvictionAction(actionStr, key);
      }
    }
    if (val.size() > 4) {
      String s = BasePrms.nameForKey(key) + " has too many fields: " + val;
      throw new HydraConfigException(s);
    }

    // fill in defaults where needed
    if (maximum == null && (sizer != null || action != null)) {
      maximum = Integer.valueOf(EvictionAttributes.createLRUMemoryAttributes().getMaximum());
    }
    if (sizer == null && action != null) {
      sizer = EvictionAttributes.createLRUMemoryAttributes().getObjectSizer();
    }

    // invoke appropriate constructor
    if (action != null) {
      return EvictionAttributes.createLRUMemoryAttributes(maximum.intValue(), sizer, action);
    } else if (sizer != null) {
      return EvictionAttributes.createLRUMemoryAttributes(maximum.intValue(), sizer);
    } else if (maximum != null) {
      return EvictionAttributes.createLRUMemoryAttributes(maximum.intValue());
    } else {
      return EvictionAttributes.createLRUMemoryAttributes();
    }
  }

  /**
   * Returns the eviction algorithm for the given string.
   */
  private static int getEvictionAlgorithm(String str, Long key) {
    if (str.equalsIgnoreCase(BasePrms.NONE)) {
      return NONE;
    } else if (str.equalsIgnoreCase("lru_entry_count")
            || str.equalsIgnoreCase("lruEntryCount")) {
      return LRU_ENTRY;
    } else if (str.equalsIgnoreCase("lru_heap_percentage")
            || str.equalsIgnoreCase("lruHeapPercentage")) {
      return LRU_HEAP;
    } else if (str.equalsIgnoreCase("lru_memory_size")
            || str.equalsIgnoreCase("lruMemorySize")) {
      return LRU_MEMORY;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal algorithm: " + str;
      throw new HydraConfigException(s);
    }
  }
  private static final int NONE       = 0;
  private static final int LRU_ENTRY  = 1;
  private static final int LRU_HEAP   = 2;
  private static final int LRU_MEMORY = 3;

  /**
   * Returns the EvictionAction for the given string.
   */
  private static EvictionAction getEvictionAction(String str, Long key) {
    if (str.equalsIgnoreCase("local_destroy")
            || str.equalsIgnoreCase("localDestroy")) {
      return EvictionAction.LOCAL_DESTROY;
    } else if (str.equalsIgnoreCase("overflow_to_disk")
            || str.equalsIgnoreCase("overflowToDisk")) {
      return EvictionAction.OVERFLOW_TO_DISK;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal action: " + str;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the ObjectSizer instance for the given string.
   */
  private static ObjectSizer getObjectSizer(String classname, Long key) {
    if (classname.equalsIgnoreCase(BasePrms.NONE)) {
      return null;
    } else {
      try {
        return (ObjectSizer)getInstance(key, classname);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key)
                 + " object sizer does not implement ObjectSizer: " + classname;
        throw new HydraConfigException(s);
      }
    }
  }

//------------------------------------------------------------------------------
// Expiration configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the expiration attributes from the given vector, which is expected
   * to hold a timeout and optional action, or null if there is no expiration.
   * @throws HydraConfigException if the value is malformed or a field contains
   *         an illegal value or type.
   */
  private static ExpirationAttributes getExpirationAttributes(Long key,
                                      Vector val, ConfigHashtable tab) {
    if (val == null) {
      return null;
    } else {
      Integer timeout = null;
      ExpirationAction action = null;

      // read fields
      if (val.size() > 0) {
        if (val.get(0) instanceof String) {
          String timeoutStr = tab.getString(key, val.get(0));
          if (timeoutStr != null) {
            timeout = getExpirationTimeout(timeoutStr, key);
          }
        } else {
          timeout = tab.getInteger(key, val.get(0));
        }
      }
      if (val.size() > 1) {
        String actionStr = tab.getString(key, val.get(1));
        if (actionStr != null) {
          action = getExpirationAction(actionStr, key);
        }
      }
      if (val.size() > 2) {
        String s = BasePrms.nameForKey(key) + " has too many fields: " + val;
        throw new HydraConfigException(s);
      }

      // fill in defaults where needed
      if (timeout == null && action != null) {
        timeout = Integer.valueOf((ExpirationAttributes.DEFAULT).getTimeout());
      }

      // invoke appropriate constructor
      if (action != null) {
        return new ExpirationAttributes(timeout.intValue(), action);
      } else if (timeout != null) {
        return new ExpirationAttributes(timeout.intValue());
      } else {
        return ExpirationAttributes.DEFAULT;
      }
    }
  }

  /**
   * Returns the expiration timeout for the given string.
   */
  private static Integer getExpirationTimeout(String timeout, Long key) {
    if (timeout.equalsIgnoreCase(BasePrms.NONE)) {
      return Integer.valueOf(0);
    } else {
      try {
        return Integer.valueOf(timeout);
      } catch (NumberFormatException e) {
        String s = BasePrms.nameForKey(key) + " has illegal type: " + timeout;
        throw new HydraConfigException(s);
      }
    }
  }

  /**
   * Returns the ExpirationAction for the given string.
   */
  private static ExpirationAction getExpirationAction(String str, Long key) {
    if (str.equalsIgnoreCase("destroy")) {
      return ExpirationAction.DESTROY;
    } else if (str.equalsIgnoreCase("local_destroy")
            || str.equalsIgnoreCase("localDestroy")) {
      return ExpirationAction.LOCAL_DESTROY;
    } else if (str.equalsIgnoreCase("invalidate")) {
      return ExpirationAction.INVALIDATE;
    } else if (str.equalsIgnoreCase("local_invalidate")
            || str.equalsIgnoreCase("localInvalidate")) {
      return ExpirationAction.LOCAL_INVALIDATE;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal action: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Pool configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the pool description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         PoolPrms#names}.
   */
  private static PoolDescription getPoolDescription(String str, Long key,
                                                    TestConfig config) {
    PoolDescription pd = config.getPoolDescription(str);
    if (pd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(PoolPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return pd;
    }
  }
}
