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
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.pdx.PdxSerializer;
import java.io.*;
import java.util.*;

/**
 * Encodes information needed to describe and create a cache.
 */
public class CacheDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this cache description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Boolean copyOnRead;
  private String gatewayConflictResolver;
  private Integer lockLease;
  private Integer lockTimeout;
  private Integer messageSyncInterval;
  private DiskStoreDescription pdxDiskStoreDescription; // from pdxDiskStoreName
  private String pdxDiskStoreName;
  private Boolean pdxIgnoreUnreadFields;
  private Boolean pdxPersistent;
  private Boolean pdxReadSerialized;
  private String pdxSerializerClass;
  private String pdxSerializerMethod;
  private ResourceManagerDescription resourceManagerDescription;
                                     // from resourceManagerName
  private String resourceManagerDescriptionName;
  private Integer searchTimeout;

  /** Cached fields */
  private transient GatewayConflictResolver gatewayConflictResolverInstance;
  private transient PdxSerializer pdxSerializerInstance;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public CacheDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this cache description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this cache description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the copy on read.
   */
  private Boolean getCopyOnRead() {
    return this.copyOnRead;
  }

  /**
   * Sets the copy on read.
   */
  private void setCopyOnRead(Boolean bool) {
    this.copyOnRead = bool;
  }

  /**
   * Returns the classname of the gateway conflict resolver.
   */
  private String getGatewayConflictResolver() {
    return this.gatewayConflictResolver;
  }

  /**
   * Sets the classname of the gateway conflict resolver.
   */
  private void setGatewayConflictResolver(String str) {
    this.gatewayConflictResolver = str;
  }

  /**
   * Returns the singleton gateway conflict resolver instance for this
   * description.
   *
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement GatewayConflictResolver.
   */
  private synchronized GatewayConflictResolver getGatewayConflictResolverInstance() {
    if (this.gatewayConflictResolverInstance == null) {
      this.gatewayConflictResolverInstance = 
           createGatewayConflictResolverInstance();
    }
    return this.gatewayConflictResolverInstance;
  }

  /**
   * Returns the lock lease.
   */
  private Integer getLockLease() {
    return this.lockLease;
  }

  /**
   * Sets the lock lease.
   */
  private void setLockLease(Integer i) {
    this.lockLease = i;
  }

  /**
   * Returns the lock timeout.
   */
  private Integer getLockTimeout() {
    return this.lockTimeout;
  }

  /**
   * Sets the lock timeout.
   */
  private void setLockTimeout(Integer i) {
    this.lockTimeout = i;
  }

  /**
   * Returns the message sync interval.
   */
  private Integer getMessageSyncInterval() {
    return this.messageSyncInterval;
  }

  /**
   * Sets the message sync interval.
   */
  private void setMessageSyncInterval(Integer i) {
    this.messageSyncInterval = i;
  }

  /**
   * Returns the pdx disk store description name.
   */
  private String getPdxDiskStoreName() {
    return this.pdxDiskStoreName;
  }

  /**
   * Sets the pdx disk store description name.
   */
  private void setPdxDiskStoreName(String str) {
    this.pdxDiskStoreName = str;
  }

  /**
   * Returns the pdx disk store description.
   */
  public DiskStoreDescription getPdxDiskStoreDescription() {
    return this.pdxDiskStoreDescription;
  }

  /**
   * Sets the pdx disk store description.
   */
  private void setPdxDiskStoreDescription(DiskStoreDescription dsd) {
    this.pdxDiskStoreDescription = dsd;
  }

  /**
   * Returns the pdx ignore unread fields.
   */
  private Boolean getPdxIgnoreUnreadFields() {
    return this.pdxIgnoreUnreadFields;
  }

  /**
   * Sets the pdx ignore unread fields.
   */
  private void setPdxIgnoreUnreadFields(Boolean bool) {
    this.pdxIgnoreUnreadFields = bool;
  }

  /**
   * Returns the pdx persistent.
   */
  private Boolean getPdxPersistent() {
    return this.pdxPersistent;
  }

  /**
   * Sets the pdx persistent.
   */
  private void setPdxPersistent(Boolean bool) {
    this.pdxPersistent = bool;
  }

  /**
   * Returns the pdx read serialized.
   */
  private Boolean getPdxReadSerialized() {
    return this.pdxReadSerialized;
  }

  /**
   * Sets the pdx read serialized.
   */
  private void setPdxReadSerialized(Boolean bool) {
    this.pdxReadSerialized = bool;
  }

  /**
   * Returns the pdx serializer class.
   */
  private String getPdxSerializerClass() {
    return this.pdxSerializerClass;
  }

  /**
   * Sets the pdx serializer class.
   */
  private void setPdxSerializerClass(String str) {
    this.pdxSerializerClass = str;
  }

  /**
   * Returns the pdx serializer method.
   */
  private String getPdxSerializerMethod() {
    return this.pdxSerializerMethod;
  }

  /**
   * Sets the pdx serializer method.
   */
  private void setPdxSerializerMethod(String str) {
    this.pdxSerializerMethod = str;
  }

  /**
   * Returns the singleton pdx serializer instance for this description.
   *
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PdxSerializer.
   */
  private synchronized PdxSerializer getPdxSerializerInstance() {
    if (this.pdxSerializerInstance == null) {
      this.pdxSerializerInstance = createPdxSerializerInstance();
    }
    return this.pdxSerializerInstance;
  }

  /**
   * Returns the resource manager description name.
   */
  private String getResourceManagerDescriptionName() {
    return this.resourceManagerDescriptionName;
  }

  /**
   * Sets the resource manager description name.
   */
  private void setResourceManagerDescriptionName(String str) {
    this.resourceManagerDescriptionName = str;
  }

  /**
   * Returns the resource manager description.
   */
  protected ResourceManagerDescription getResourceManagerDescription() {
    return this.resourceManagerDescription;
  }

  /**
   * Sets the resource manager description.
   */
  private void setResourceManagerDescription(ResourceManagerDescription rmd) {
    this.resourceManagerDescription = rmd;
  }

  /**
   * Returns the search timeout.
   */
  private Integer getSearchTimeout() {
    return this.searchTimeout;
  }

  /**
   * Sets the search timeout.
   */
  private void setSearchTimeout(Integer i) {
    this.searchTimeout = i;
  }

//------------------------------------------------------------------------------
// Cache configuration
//------------------------------------------------------------------------------

  /**
   * Configures the cache factory using this cache description.
   * Instantiates and caches the pdx serializer, if any.
   */
  public void configure(CacheFactory f) {
    if (this.getPdxDiskStoreDescription() != null) {
      f.setPdxDiskStore(this.getPdxDiskStoreDescription().getName());
    }
    f.setPdxIgnoreUnreadFields(this.getPdxIgnoreUnreadFields());
    f.setPdxPersistent(this.getPdxPersistent());
    f.setPdxReadSerialized(this.getPdxReadSerialized());
    if (this.getPdxSerializerClass() != null) {
      f.setPdxSerializer(this.getPdxSerializerInstance());
    }
  }

  /**
   * Configures the cache using this cache description.
   */
  public void configure(Cache c) {
    c.setCopyOnRead(this.getCopyOnRead().booleanValue());
    if (this.getGatewayConflictResolver() != null) {
      c.setGatewayConflictResolver(this.getGatewayConflictResolverInstance());
    }
    c.setLockLease(this.getLockLease().intValue());
    c.setLockTimeout(this.getLockTimeout().intValue());
    c.setMessageSyncInterval(this.getMessageSyncInterval().intValue());
    if (this.getResourceManagerDescription() != null) {
      this.getResourceManagerDescription().configure(c.getResourceManager());
    }
    c.setSearchTimeout(this.getSearchTimeout().intValue());
  }

  /**
   * Configures the dummy cache using this cache description.
   * Instantiates and caches the pdx serializer, if any.
   */
  public void configureDummy(CacheCreation c) {
    configure(c); // attributes that normally use the Cache API

    // configure attributes that normally use the CacheFactory API
    if (this.getPdxDiskStoreDescription() != null) {
      c.setPdxDiskStore(this.getPdxDiskStoreDescription().getName());
    }
    c.setPdxIgnoreUnreadFields(this.getPdxIgnoreUnreadFields());
    c.setPdxPersistent(this.getPdxPersistent());
    c.setPdxReadSerialized(this.getPdxReadSerialized());
    if (this.getPdxSerializerClass() != null) {
      c.setPdxSerializer(this.getPdxSerializerInstance());
    }
  }

  /**
   * Returns the cache as a string.  For use only by {@link
   * CacheHelper#cacheToString(Cache)}.
   */
  protected static synchronized String cacheToString(Cache c) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  copyOnRead: " + c.getCopyOnRead());
    buf.append("\n  gatewayConflictResolver: " + c.getGatewayConflictResolver());
    buf.append("\n  lockLease: " + c.getLockLease());
    buf.append("\n  lockTimeout: " + c.getLockTimeout());
    buf.append("\n  messageSyncInterval: " + c.getMessageSyncInterval());
    buf.append("\n  pdxDiskStoreName: " + c.getPdxDiskStore());
    buf.append("\n  pdxIgnoreUnreadFields: " + c.getPdxIgnoreUnreadFields());
    buf.append("\n  pdxPersistent: " + c.getPdxPersistent());
    buf.append("\n  pdxReadSerialized: " + c.getPdxReadSerialized());
    buf.append("\n  pdxSerializer: " + c.getPdxSerializer());
    buf.append("\n  resourceManager: " + c.getResourceManager());
    buf.append("\n  searchTimeout: " + c.getSearchTimeout());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "copyOnRead", this.getCopyOnRead());
    map.put(header + "gatewayConflictResolver", this.getGatewayConflictResolver());
    map.put(header + "lockLease", this.getLockLease());
    map.put(header + "lockTimeout", this.getLockTimeout());
    map.put(header + "messageSyncInterval", this.getMessageSyncInterval());
    map.put(header + "pdxDiskStoreName", this.getPdxDiskStoreName());
    map.put(header + "pdxIgnoreUnreadFields", this.getPdxIgnoreUnreadFields());
    map.put(header + "pdxPersistent", this.getPdxPersistent());
    map.put(header + "pdxReadSerialized", this.getPdxReadSerialized());
    map.put(header + "pdxSerializer", this.getPdxSerializerClass() + "." +
                                      this.getPdxSerializerMethod());
    map.put(header + "resourceManagerName", this.getResourceManagerDescriptionName());
    map.put(header + "searchTimeout", this.getSearchTimeout());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates cache descriptions from the cache parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each cache name
    Vector names = tab.vecAt(CachePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create cache description from test configuration parameters
      CacheDescription cd = createCacheDescription(name, config, i);

      // save configuration
      config.addCacheDescription(cd);
    }
  }

  /**
   * Creates the cache description using test configuration parameters and
   * product defaults.
   */
  private static CacheDescription createCacheDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    CacheDescription cd = new CacheDescription();
    cd.setName(name);

    // copyOnRead
    {
      Long key = CachePrms.copyOnRead;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        cd.setCopyOnRead(Boolean.valueOf(GemFireCacheImpl.DEFAULT_COPY_ON_READ));
      } else {
        cd.setCopyOnRead(bool);
      }
    }
    // gatewayConflictResolver
    {
      Long key = CachePrms.gatewayConflictResolver;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        cd.setGatewayConflictResolver(str);
      }
    }
    // lockLease
    {
      Long key = CachePrms.lockLease;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        cd.setLockLease(new Integer(GemFireCacheImpl.DEFAULT_LOCK_LEASE));
      } else {
        cd.setLockLease(i);
      }
    }
    // lockTimeout
    {
      Long key = CachePrms.lockTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        cd.setLockTimeout(new Integer(GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT));
      } else {
        cd.setLockTimeout(i);
      }
    }
    // messageSyncInterval
    {
      Long key = CachePrms.messageSyncInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        cd.setMessageSyncInterval(new Integer(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL));
      } else {
        cd.setMessageSyncInterval(i);
      }
    }
    // pdxDiskStoreName (generates pdxDiskStoreDescription)
    {
      Long key = CachePrms.pdxDiskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        cd.setPdxDiskStoreDescription(
           getPdxDiskStoreDescription(str, key, config));
        cd.setPdxDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // pdxIgnoreUnreadFields
    {
      Long key = CachePrms.pdxIgnoreUnreadFields;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        cd.setPdxIgnoreUnreadFields(CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS);
      } else {
        cd.setPdxIgnoreUnreadFields(bool);
      }
    }
    // pdxPersistent
    {
      Long key = CachePrms.pdxPersistent;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        cd.setPdxPersistent(CacheConfig.DEFAULT_PDX_PERSISTENT);
      } else {
        cd.setPdxPersistent(bool);
      }
    }
    // pdxReadSerialized
    {
      Long key = CachePrms.pdxReadSerialized;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        cd.setPdxReadSerialized(CacheConfig.DEFAULT_PDX_READ_SERIALIZED);
      } else {
        cd.setPdxReadSerialized(bool);
      }
    }
    // pdxSerializer
    {
      Long key = CachePrms.pdxSerializerInstantiator;
      Vector strs = tab.vecAtWild(key, index, null);
      cd.setPdxSerializerClass(getPdxSerializerClass(strs, key));
      cd.setPdxSerializerMethod(getPdxSerializerMethod(strs, key));
    }
    // resourceManagerName (generates resourceManagerDescription)
    {
      Long key = CachePrms.resourceManagerName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        cd.setResourceManagerDescription(
           getResourceManagerDescription(str, key, config));
        cd.setResourceManagerDescriptionName("ResourceManagerDescription." + str);
      }
    }
    // searchTimeout
    {
      Long key = CachePrms.searchTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        cd.setSearchTimeout(new Integer(GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT));
      } else {
        cd.setSearchTimeout(i);
      }
    }
    return cd;
  }

//------------------------------------------------------------------------------
// Gateway conflict resolver configuration support
//------------------------------------------------------------------------------

  /**
   * Returns a gateway conflict resolver instance for the given classname.
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement GatewayConflictResolver.
   */
  private GatewayConflictResolver createGatewayConflictResolverInstance() {
    String classname = this.getGatewayConflictResolver();
    if (classname == null) {
      return null;
    }
    Long key = CachePrms.gatewayConflictResolver;
    try {
      return (GatewayConflictResolver)getInstance(key, classname);
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
               + " does not implement GatewayConflictResolver: " + classname;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// PDX disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the pdx disk store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getPdxDiskStoreDescription(String str,
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
// PDX serializer configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the PdxSerializer class for the given PdxSerializer instantiator.
   * @throws HydraConfigException if the instantiator is malformed.
   */
  private static String getPdxSerializerClass(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return null;
    } else if (strs.size() == 2) {
      return (String)strs.get(0);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the PdxSerializer method for the given PdxSerializer instantiator.
   * @throws HydraConfigException if the instantiator is malformed.
   */
  private static String getPdxSerializerMethod(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return null;
    } else if (strs.size() == 2) {
      return (String)strs.get(1);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Creates a pdx serializer instance for this description.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PdxSerializer.
   */
  private PdxSerializer createPdxSerializerInstance() {
    Long key = CachePrms.pdxSerializerInstantiator;
    String classname = this.getPdxSerializerClass();
    String methodname = this.getPdxSerializerMethod();
    MethExecutorResult result = MethExecutor.execute(classname, methodname);
    if (result.getStackTrace() != null){
      throw new HydraRuntimeException(result.toString());
    }
    Object obj = result.getResult();
    Log.getLogWriter().info("Instantiated  " + obj);
    try {
      return (PdxSerializer)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key) + "="
               + this.getPdxSerializerClass() + "."
               + this.getPdxSerializerMethod()
               + " returned an instance of " + obj.getClass().getName()
               + " instead of a PdxSerializer";
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// ResourceManager configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the resource manager description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         ResourceManagerPrms#names}.
   */
  private static ResourceManagerDescription getResourceManagerDescription(
                                      String str, Long key, TestConfig config) {
    ResourceManagerDescription rmd = config.getResourceManagerDescription(str);
    if (rmd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(ResourceManagerPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return rmd;
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
    this.copyOnRead = (Boolean)in.readObject();
    this.gatewayConflictResolver = (String)in.readObject();
    this.lockLease = (Integer)in.readObject();
    this.lockTimeout = (Integer)in.readObject();
    this.messageSyncInterval = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.pdxDiskStoreName = (String)in.readObject();
    this.pdxDiskStoreDescription = (DiskStoreDescription)in.readObject();
    this.pdxIgnoreUnreadFields = (Boolean)in.readObject();
    this.pdxPersistent = (Boolean)in.readObject();
    this.pdxReadSerialized = (Boolean)in.readObject();
    this.pdxSerializerClass = (String)in.readObject();
    this.pdxSerializerMethod = (String)in.readObject();
    this.resourceManagerDescription =
                (ResourceManagerDescription)in.readObject();
    this.resourceManagerDescriptionName = (String)in.readObject();
    this.searchTimeout = (Integer)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.copyOnRead);
    out.writeObject(this.gatewayConflictResolver);
    out.writeObject(this.lockLease);
    out.writeObject(this.lockTimeout);
    out.writeObject(this.messageSyncInterval);
    out.writeObject(this.name);
    out.writeObject(this.pdxDiskStoreName);
    out.writeObject(this.pdxDiskStoreDescription);
    out.writeObject(this.pdxIgnoreUnreadFields);
    out.writeObject(this.pdxPersistent);
    out.writeObject(this.pdxReadSerialized);
    out.writeObject(this.pdxSerializerClass);
    out.writeObject(this.pdxSerializerMethod);
    out.writeObject(this.resourceManagerDescription);
    out.writeObject(this.resourceManagerDescriptionName);
    out.writeObject(this.searchTimeout);
  }
}
