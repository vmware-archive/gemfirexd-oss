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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.compression.Compressor;

/**
 * Encodes information needed to describe and create a region.
 */
public class RegionDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final String PARTITION = "PARTITION";

  /** Map of logical region configuration names to maps of cache listeners. */
  private static Map cacheListenerInstanceMaps = new HashMap();

  /** Map of logical region configuration names to cache loaders. */
  private static Map cacheLoaderInstances = new HashMap();

  /** Map of logical region configuration names to cache writers. */
  private static Map cacheWriterInstances = new HashMap();

  /** Map of logical region configuration names to compressors. */
  private static Map compressorInstances = new HashMap();
      
  /** The logical name of this region description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private List<AsyncEventQueueDescription> asyncEventQueueDescriptions; // from asyncEventQueueNames
  private List<String> asyncEventQueueNames;
  private List cacheListeners;
  private Boolean cacheListenersSingleton;
  private String cacheLoader;
  private Boolean cacheLoaderSingleton;
  private String cacheWriter;
  private Boolean cacheWriterSingleton;
  private Boolean cloningEnabled;
  private String compressor;
  private Boolean compressorSingleton;
  private Boolean concurrencyChecksEnabled;
  private Integer concurrencyLevel;
  private String customEntryIdleTimeout;
  private String customEntryTimeToLive;
  private DataPolicy dataPolicy;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private Boolean enableAsyncConflation;
  private Boolean enableGateway;
  private Boolean enableOffHeapMemory;
  private Boolean enableSubscriptionConflation;
  private ExpirationAttributes entryIdleTimeout;
  private ExpirationAttributes entryTimeToLive;
  private EvictionAttributes evictionAttributes;
  private List<GatewaySenderDescription> gatewaySenderDescriptions; // from gatewaySenderNames
  private List<String> gatewaySenderNames;
  private HDFSStoreDescription hdfsStoreDescription; // from hdfsStoreName
  private String hdfsStoreName;
  private Boolean hdfsWriteOnly;
  private Boolean ignoreJTA;
  private Boolean indexMaintenanceSynchronous;
  private Integer initialCapacity;
  private SubscriptionAttributes interestPolicy;
  private Class keyConstraint;
  private Float loadFactor;
  private Boolean lockGrantor;
  private MembershipAttributes membershipAttributes;
  private Boolean multicastEnabled; // can inherit from distributed system
  private PartitionDescription partitionDescription; // from partitionName
  private String partitionDescriptionName;
  private PoolDescription poolDescription; // from poolName
  private String poolDescriptionName;
  private ExpirationAttributes regionIdleTimeout;
  private String regionName;
  private ExpirationAttributes regionTimeToLive;
  private Scope scope;
  private Boolean statisticsEnabled;
  private Class valueConstraint;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public RegionDescription() {
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
   * Returns the async event queue descriptions.
   */
  public List<AsyncEventQueueDescription> getAsyncEventQueueDescriptions() {
    return this.asyncEventQueueDescriptions;
  }

  /**
   * Sets the async event queue descriptions.
   */
  private void setAsyncEventQueueDescriptions(List<AsyncEventQueueDescription> aeqds) {
    this.asyncEventQueueDescriptions = aeqds;
  }

  /**
   * Returns the async event queue names.
   */
  public List<String> getAsyncEventQueueNames() {
    return this.asyncEventQueueNames;
  }

  /**
   * Sets the async event queue names.
   */
  private void setAsyncEventQueueNames(List<String> strs) {
    this.asyncEventQueueNames = strs;
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
   * Returns the cache loader classname.
   */
  private String getCacheLoader() {
    return this.cacheLoader;
  }

  /**
   * Sets the cache loader classname.
   */
  private void setCacheLoader(String s) {
    this.cacheLoader = s;
  }

  /**
   * Returns the cache loader singleton.
   */
  private Boolean getCacheLoaderSingleton() {
    return this.cacheLoaderSingleton;
  }

  /**
   * Sets the cache loader singleton.
   */
  private void setCacheLoaderSingleton(Boolean bool) {
    this.cacheLoaderSingleton = bool;
  }

  /**
   * Returns the cache writer classname.
   */
  private String getCacheWriter() {
    return this.cacheWriter;
  }

  /**
   * Sets the cache writer classname.
   */
  private void setCacheWriter(String  s) {
    this.cacheWriter = s;
  }

  /**
   * Returns the cache writer singleton.
   */
  private Boolean getCacheWriterSingleton() {
    return this.cacheWriterSingleton;
  }

  /**
   * Sets the cache writer singleton.
   */
  private void setCacheWriterSingleton(Boolean bool) {
    this.cacheWriterSingleton = bool;
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
   * Returns the compressor class name.
   */
  private String getCompressor() {
    return this.compressor;
  }

  /**
   * Sets the compressor class name.
   */
  private void setCompressor(String str) {
    this.compressor = str;
  }

  /**
   * Returns the compressor singleton.
   */
  private Boolean getCompressorSingleton() {
    return this.compressorSingleton;
  }

  /**
   * Sets the compressor singleton.
   */
  private void setCompressorSingleton(Boolean bool) {
    this.compressorSingleton = bool;
  }

  /**
   * Returns concurrencyChecksEnabled
   */
  private Boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  /**
   * Sets concurrencyChecksEnabled
   */
  private void setConcurrencyChecksEnabled(Boolean bool) {
    this.concurrencyChecksEnabled = bool;
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
   * Returns the data policy.
   */
  public DataPolicy getDataPolicy() {
    return this.dataPolicy;
  }

  /**
   * Sets the data policy.
   */
  private void setDataPolicy(DataPolicy aDataPolicy) {
    this.dataPolicy = aDataPolicy;
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
   * Returns the enable async conflation.
   */
  private Boolean getEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  /**
   * Sets the enable async conflation.
   */
  private void setEnableAsyncConflation(Boolean bool) {
    this.enableAsyncConflation = bool;
  }

  /**
   * Returns the enable gateway.
   */
  private Boolean getEnableGateway() {
    return this.enableGateway;
  }

  /**
   * Sets the enable gateway.
   */
  private void setEnableGateway(Boolean bool) {
    this.enableGateway = bool;
  }

  /**
   * Returns enable off-heap memory.
   */
  private Boolean getEnableOffHeapMemory() {
    return this.enableOffHeapMemory;
  }

  /**
   * Sets enable off-heap memory.
   */
  private void setEnableOffHeapMemory(Boolean bool) {
    this.enableOffHeapMemory = bool;
  }

  /**
   * Returns the enable subscription conflation.
   */
  private Boolean getEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  /**
   * Sets the enable subscription conflation.
   */
  private void setEnableSubscriptionConflation(Boolean bool) {
    this.enableSubscriptionConflation = bool;
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
   * Returns the gateway sender descriptions.
   */
  public List<GatewaySenderDescription> getGatewaySenderDescriptions() {
    return this.gatewaySenderDescriptions;
  }

  /**
   * Sets the gateway sender descriptions.
   */
  private void setGatewaySenderDescriptions(List<GatewaySenderDescription> gsds) {
    this.gatewaySenderDescriptions = gsds;
  }

  /**
   * Returns the gateway sender names.
   */
  public List<String> getGatewaySenderNames() {
    return this.gatewaySenderNames;
  }

  /**
   * Sets the gateway sender names.
   */
  private void setGatewaySenderNames(List<String> strs) {
    this.gatewaySenderNames = strs;
  }

  /**
   * Returns the HDFS store description name.
   */
  public String getHDFSStoreName() {
    return this.hdfsStoreName;
  }

  /**
   * Sets the HDFS store description name.
   */
  private void setHDFSStoreName(String str) {
    this.hdfsStoreName = str;
  }

  /**
   * Returns the HDFS store description.
   */
  public HDFSStoreDescription getHDFSStoreDescription() {
    return this.hdfsStoreDescription;
  }

  /**
   * Sets the HDFS store description.
   */
  private void setHDFSStoreDescription(HDFSStoreDescription hsd) {
    this.hdfsStoreDescription = hsd;
  }

  /**
   * Returns the HDFS write-only.
   */
  private Boolean getHDFSWriteOnly() {
    return this.hdfsWriteOnly;
  }

  /**
   * Sets the HDFS write-only.
   */
  private void setHDFSWriteOnly(Boolean bool) {
    this.hdfsWriteOnly = bool;
  }

  /**
   * Returns the ignore JTA.
   */
  private Boolean getIgnoreJTA() {
    return this.ignoreJTA;
  }

  /**
   * Sets the ignore JTA.
   */
  private void setIgnoreJTA(Boolean bool) {
    this.ignoreJTA = bool;
  }

  /**
   * Returns the index maintenance synchronous.
   */
  private Boolean getIndexMaintenanceSynchronous() {
    return this.indexMaintenanceSynchronous;
  }

  /**
   * Sets the index maintenance synchronous.
   */
  private void setIndexMaintenanceSynchronous(Boolean bool) {
    this.indexMaintenanceSynchronous = bool;
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
   * Returns the interest policy (really subscription attributes).
   */
  private SubscriptionAttributes getInterestPolicy() {
    return this.interestPolicy;
  }

  /**
   * Sets the interest policy (really subscription attributes).
   */
  private void setInterestPolicy(SubscriptionAttributes attributes) {
    this.interestPolicy = attributes;
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
   * Returns the lock grantor.
   */
  private Boolean getLockGrantor() {
    return this.lockGrantor;
  }

  /**
   * Sets the lock grantor.
   */
  private void setLockGrantor(Boolean bool) {
    this.lockGrantor = bool;
  }

  /**
   * Returns the membership attributes.
   */
  private MembershipAttributes getMembershipAttributes() {
    return this.membershipAttributes;
  }

  /**
   * Sets the membership attributes.
   */
  private void setMembershipAttributes(MembershipAttributes attributes) {
    this.membershipAttributes = attributes;
  }

  /**
   * Returns the multicast enabled.
   * <p>
   * If the field was not configured with an explicit boolean, the value is
   * inherited from the distributed system configured for this VM.
   *
   * @throws HydraRuntimeException if inheritance is used but this VM has no
   *         distributed system configured.
   */
  private Boolean getMulticastEnabled() {
    if (this.multicastEnabled == null) {
      String gemfire = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
      if (gemfire == null) {
        String s = "This VM is not configured for a distributed system: "
                 + "missing property " + GemFirePrms.GEMFIRE_NAME_PROPERTY;
        throw new HydraRuntimeException(s);
      }
      this.multicastEnabled = TestConfig.getInstance()
                                        .getGemFireDescription(gemfire)
                                        .getEnableMcast();
    }
    return this.multicastEnabled;
  }

  /**
   * Sets the multicast enabled.
   */
  private void setMulticastEnabled(Boolean bool) {
    this.multicastEnabled = bool;
  }

  /**
   * Returns the partition description name.
   */
  private String getPartitionDescriptionName() {
    return this.partitionDescriptionName;
  }

  /**
   * Sets the partition description name.
   */
  private void setPartitionDescriptionName(String str) {
    this.partitionDescriptionName = str;
  }

  /**
   * Returns the partition description.
   */
  public PartitionDescription getPartitionDescription() {
    return this.partitionDescription;
  }

  /**
   * Sets the partition description.
   */
  private void setPartitionDescription(PartitionDescription pd) {
    this.partitionDescription = pd;
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
   * Returns the scope.
   */
  private Scope getScope() {
    return this.scope;
  }

  /**
   * Sets the scope.
   */
  private void setScope(Scope aScope) {
    this.scope = aScope;
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
   * Configures the attributes factory using this region description.  Has the
   * option to skip instantiating cache listeners, loaders, and writers when
   * creating dummy regions to generate cache XML files.  Passes the default
   * region name to the fixed partition attributes mapping algorithm, if any.
   */
  public void configure(AttributesFactory f, boolean instantiate) {
    configure(RegionPrms.DEFAULT_REGION_NAME, f, instantiate);
  }

  /**
   * Configures the attributes factory using this region description.  Has the
   * option to skip instantiating cache listeners, loaders, and writers when
   * creating dummy regions to generate cache XML files.  Passes the region
   * name, if any, to the fixed partition attributes mapping algorithm, if any.
   */
  protected void configure(String regionName, AttributesFactory f,
                                              boolean instantiate) {
    // configure the factory
    List<AsyncEventQueueDescription> aeqds =
                        this.getAsyncEventQueueDescriptions();
    if (aeqds != null) {
      for (AsyncEventQueueDescription aeqd : aeqds) {
        f.addAsyncEventQueueId(aeqd.getName());
      }
    }
    f.setCacheLoader(this.getCacheLoaderInstance(instantiate));
    f.setCacheWriter(this.getCacheWriterInstance(instantiate));
    f.initCacheListeners(this.getCacheListenerInstances(instantiate));
    f.setCloningEnabled(this.getCloningEnabled().booleanValue());
    if (this.getCompressor() != null) {
      f.setCompressor(this.getCompressorInstance(instantiate));
    }
    f.setConcurrencyChecksEnabled(this.getConcurrencyChecksEnabled().booleanValue());
    f.setConcurrencyLevel(this.getConcurrencyLevel().intValue());
    f.setCustomEntryIdleTimeout(this.getCustomEntryIdleTimeoutInstance());
    f.setCustomEntryTimeToLive(this.getCustomEntryTimeToLiveInstance());
    f.setDataPolicy(this.getDataPolicy());
    if (this.getDiskStoreDescription() != null) {
      f.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    f.setDiskSynchronous(this.getDiskSynchronous().booleanValue());
    f.setEnableAsyncConflation(this.getEnableAsyncConflation().booleanValue());
    f.setEnableGateway(this.getEnableGateway().booleanValue());
    f.setEnableOffHeapMemory(this.getEnableOffHeapMemory().booleanValue());
    f.setEnableSubscriptionConflation(this.getEnableSubscriptionConflation().booleanValue());
    f.setEntryIdleTimeout(this.getEntryIdleTimeout());
    f.setEntryTimeToLive(this.getEntryTimeToLive());
    f.setEvictionAttributes(this.getEvictionAttributes());
    List<GatewaySenderDescription> gsds = this.getGatewaySenderDescriptions();
    if (gsds != null) {
      for (GatewaySenderDescription gsd : gsds) {
        Set<String> serIds =
          GatewaySenderHelper.getSerialGatewaySenderIds(gsd.getName());
        for (String serId : serIds) {
          f.addGatewaySenderId(serId);
        }
        Set<String> parIds =
          GatewaySenderHelper.getParallelGatewaySenderIds(gsd.getName());
        for (String parId : parIds) {
          f.addGatewaySenderId(parId);
        }
      }
    }
    if (this.getHDFSStoreDescription() != null) {
      f.setHDFSStoreName(this.getHDFSStoreDescription().getName());
    }
    f.setHDFSWriteOnly(this.getHDFSWriteOnly().booleanValue());
    f.setIgnoreJTA(this.getIgnoreJTA().booleanValue());
    f.setIndexMaintenanceSynchronous(this.getIndexMaintenanceSynchronous().booleanValue());
    f.setInitialCapacity(this.getInitialCapacity().intValue());
    f.setSubscriptionAttributes(this.getInterestPolicy());
    f.setKeyConstraint(this.getKeyConstraint());
    f.setLoadFactor(this.getLoadFactor().floatValue());
    f.setLockGrantor(this.getLockGrantor().booleanValue());
    f.setMembershipAttributes(this.getMembershipAttributes());
    f.setMulticastEnabled(this.getMulticastEnabled().booleanValue());
    if (this.getPartitionDescription() != null) {
      f.setPartitionAttributes(this.getPartitionDescription()
                                   .getPartitionAttributes(this.regionName));
    }
    if (this.getPoolDescription() != null) {
      f.setPoolName(this.getPoolDescription().getName());
    }
    f.setRegionIdleTimeout(this.getRegionIdleTimeout());
    f.setRegionTimeToLive(this.getRegionTimeToLive());
    if (!isPartitioned(this.getDataPolicy())) {
      f.setScope(this.getScope());
    }
    f.setStatisticsEnabled(this.getStatisticsEnabled().booleanValue());
    f.setValueConstraint(this.getValueConstraint());
  }

  /**
   * Returns the region attributes as a string.  For use only by {@link
   * RegionHelper#regionAttributesToString(RegionAttributes)}.
   */
  protected static synchronized String regionAttributesToString(
                                             RegionAttributes r) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  asyncEventQueues: " + (new TreeSet(r.getAsyncEventQueueIds())));
    buf.append("\n  cacheListeners: " + cacheListenersFor(r.getCacheListeners()));
    buf.append("\n  cacheLoader: " + cacheLoaderFor(r.getCacheLoader()));
    buf.append("\n  cacheWriter: " + cacheWriterFor(r.getCacheWriter()));
    buf.append("\n  cloningEnabled: " + r.getCloningEnabled());
    buf.append("\n  compressor: " + compressorFor(r.getCompressor()));
    buf.append("\n  concurrencyChecksEnabled: " + r.getConcurrencyChecksEnabled());
    buf.append("\n  concurrencyLevel: " + r.getConcurrencyLevel());
    buf.append("\n  customEntryIdleTimeout: " + customExpiryFor(r.getCustomEntryIdleTimeout()));
    buf.append("\n  customEntryTimeToLive: " + customExpiryFor(r.getCustomEntryTimeToLive()));
    buf.append("\n  dataPolicy: " + r.getDataPolicy());
    buf.append("\n  diskStoreName: " + r.getDiskStoreName());
    buf.append("\n  diskSynchronous: " + r.isDiskSynchronous());
    buf.append("\n  enableAsyncConflation: " + r.getEnableAsyncConflation());
    buf.append("\n  enableGateway: " + r.getEnableGateway());
    buf.append("\n  enableOffHeapMemory: " + r.getEnableOffHeapMemory());
    buf.append("\n  enableSubscriptionConflation: " + r.getEnableSubscriptionConflation());
    buf.append("\n  entryIdleTimeout: " + r.getEntryIdleTimeout());
    buf.append("\n  entryTimeToLive: " + r.getEntryTimeToLive());
    buf.append("\n  evictionAttributes: " + r.getEvictionAttributes());
    buf.append("\n  gatewaySenders: " + (new TreeSet(r.getGatewaySenderIds())));
    buf.append("\n  hdfsStoreName: " + r.getHDFSStoreName());
    buf.append("\n  hdfsWriteOnly: " + r.getHDFSWriteOnly());
    buf.append("\n  ignoreJTA: " + r.getIgnoreJTA());
    buf.append("\n  indexMaintenanceSynchronous: " + r.getIndexMaintenanceSynchronous());
    buf.append("\n  initialCapacity: " + r.getInitialCapacity());
    buf.append("\n  interestPolicy: " + r.getSubscriptionAttributes());
    buf.append("\n  keyConstraint: " + r.getKeyConstraint());
    buf.append("\n  loadFactor: " + r.getLoadFactor());
    buf.append("\n  lockGrantor: " + r.isLockGrantor());
    buf.append("\n  membershipAttributes: " + r.getMembershipAttributes());
    buf.append("\n  multicastEnabled: " + r.getMulticastEnabled());
    buf.append("\n  partitionAttributes: " + PartitionDescription
       .partitionAttributesToString(r.getPartitionAttributes()));
    buf.append("\n  poolName: " + r.getPoolName());
    buf.append("\n  regionIdleTimeout: " + r.getRegionIdleTimeout());
    buf.append("\n  regionTimeToLive: " + r.getRegionTimeToLive());
    buf.append("\n  scope: " + r.getScope());
    buf.append("\n  statisticsEnabled: " + r.getStatisticsEnabled());
    buf.append("\n  valueConstraint: " + r.getValueConstraint());
    return buf.toString();
  }

  /**
   * Returns the partial region attributes as a string. Omits possibly HDFS
   * side-effected attributes. For use only by {@link RegionHelper
   * #regionAttributesToString(RegionAttributes)}.
   */
  protected static synchronized String regionAttributesToStringPartial(
                                       RegionAttributes r) {
    StringBuffer buf = new StringBuffer();
    //buf.append("\n  asyncEventQueues: " + (new TreeSet(r.getAsyncEventQueueIds()))); // HDFS
    buf.append("\n  cacheListeners: " + cacheListenersFor(r.getCacheListeners()));
    buf.append("\n  cacheLoader: " + cacheLoaderFor(r.getCacheLoader()));
    buf.append("\n  cacheWriter: " + cacheWriterFor(r.getCacheWriter()));
    buf.append("\n  cloningEnabled: " + r.getCloningEnabled());
    buf.append("\n  compressor: " + compressorFor(r.getCompressor()));
    buf.append("\n  concurrencyChecksEnabled: " + r.getConcurrencyChecksEnabled());
    buf.append("\n  concurrencyLevel: " + r.getConcurrencyLevel());
    buf.append("\n  customEntryIdleTimeout: " + customExpiryFor(r.getCustomEntryIdleTimeout()));
    buf.append("\n  customEntryTimeToLive: " + customExpiryFor(r.getCustomEntryTimeToLive()));
    buf.append("\n  dataPolicy: " + r.getDataPolicy());
    buf.append("\n  diskSynchronous: " + r.isDiskSynchronous());
    buf.append("\n  enableAsyncConflation: " + r.getEnableAsyncConflation());
    buf.append("\n  enableGateway: " + r.getEnableGateway());
    buf.append("\n  enableOffHeapMemory: " + r.getEnableOffHeapMemory());
    buf.append("\n  enableSubscriptionConflation: " + r.getEnableSubscriptionConflation());
    buf.append("\n  entryIdleTimeout: " + r.getEntryIdleTimeout());
    buf.append("\n  entryTimeToLive: " + r.getEntryTimeToLive());
    //buf.append("\n  evictionAttributes: " + r.getEvictionAttributes()); // HDFS
    buf.append("\n  gatewaySenders: " + (new TreeSet(r.getGatewaySenderIds())));
    buf.append("\n  hdfsStoreName: " + r.getHDFSStoreName());
    buf.append("\n  hdfsWriteOnly: " + r.getHDFSWriteOnly());
    buf.append("\n  ignoreJTA: " + r.getIgnoreJTA());
    buf.append("\n  indexMaintenanceSynchronous: " + r.getIndexMaintenanceSynchronous());
    buf.append("\n  initialCapacity: " + r.getInitialCapacity());
    buf.append("\n  interestPolicy: " + r.getSubscriptionAttributes());
    buf.append("\n  keyConstraint: " + r.getKeyConstraint());
    buf.append("\n  loadFactor: " + r.getLoadFactor());
    buf.append("\n  lockGrantor: " + r.isLockGrantor());
    buf.append("\n  membershipAttributes: " + r.getMembershipAttributes());
    buf.append("\n  multicastEnabled: " + r.getMulticastEnabled());
    buf.append("\n  partitionAttributes: " + PartitionDescription
       .partitionAttributesToString(r.getPartitionAttributes()));
    buf.append("\n  poolName: " + r.getPoolName());
    buf.append("\n  regionIdleTimeout: " + r.getRegionIdleTimeout());
    buf.append("\n  regionTimeToLive: " + r.getRegionTimeToLive());
    buf.append("\n  scope: " + r.getScope());
    buf.append("\n  statisticsEnabled: " + r.getStatisticsEnabled());
    buf.append("\n  valueConstraint: " + r.getValueConstraint());
    return buf.toString();
  }

  /**
   * Returns the compressor class name.
   */
  private static String compressorFor(Compressor compressor) {
    return (compressor == null ? null : compressor.getClass().getName());
  }
  
  /**
   * Returns the cache listener class names.
   */
  private static List cacheListenersFor(CacheListener[] listeners) {
    List classnames = new ArrayList();
    for (int i = 0; i < listeners.length; i++) {
      classnames.add(listeners[i].getClass().getName());
    }
    return classnames;
  }

  /**
   * Returns the cache loader class name.
   */
  private static String cacheLoaderFor(CacheLoader loader) {
    if (loader == null) {
      return null;
    } else {
      return loader.getClass().getName();
    }
  }

  /**
   * Returns the cache writer class name.
   */
  private static String cacheWriterFor(CacheWriter writer) {
    if (writer == null) {
      return null;
    } else {
      return writer.getClass().getName();
    }
  }

  /**
   * Returns the custom expiry class name.
   */
  private static String customExpiryFor(CustomExpiry expiry) {
    if (expiry == null) {
      return null;
    } else {
      return expiry.getClass().getName();
    }
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "asyncEventQueueNames", this.getAsyncEventQueueNames());
    map.put(header + "cacheListeners", this.getCacheListeners());
    map.put(header + "cacheListenersSingleton", this.getCacheListenersSingleton());
    map.put(header + "cacheLoader", this.getCacheLoader());
    map.put(header + "cacheLoaderSingleton", this.getCacheLoaderSingleton());
    map.put(header + "cacheWriter", this.getCacheWriter());
    map.put(header + "cacheWriterSingleton", this.getCacheWriterSingleton());
    map.put(header + "concurrencyChecksEnabled", this.getConcurrencyChecksEnabled());
    map.put(header + "cloningEnabled", this.getCloningEnabled());
    map.put(header + "compressor", this.getCompressor());
    map.put(header + "compressorSingleton", this.getCompressorSingleton());
    map.put(header + "concurrencyLevel", this.getConcurrencyLevel());
    map.put(header + "customEntryIdleTimeout", this.getCustomEntryIdleTimeout());
    map.put(header + "customEntryTimeToLive", this.getCustomEntryTimeToLive());
    map.put(header + "dataPolicy", this.getDataPolicy());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "enableAsyncConflation", this.getEnableAsyncConflation());
    map.put(header + "enableGateway", this.getEnableGateway());
    map.put(header + "enableOffHeapMemory", this.getEnableOffHeapMemory());
    map.put(header + "enableSubscriptionConflation", this.getEnableSubscriptionConflation());
    map.put(header + "entryIdleTimeout", this.getEntryIdleTimeout());
    map.put(header + "entryTimeToLive", this.getEntryTimeToLive());
    map.put(header + "evictionAttributes", this.getEvictionAttributes());
    map.put(header + "gatewaySenderNames", this.getGatewaySenderNames());
    map.put(header + "hdfsStoreName", this.getHDFSStoreName());
    map.put(header + "hdfsWriteOnly", this.getHDFSWriteOnly());
    map.put(header + "ignoreJTA", this.getIgnoreJTA());
    map.put(header + "indexMaintenanceSynchronous", this.getIndexMaintenanceSynchronous());
    map.put(header + "initialCapacity", this.getInitialCapacity());
    map.put(header + "interestPolicy", this.getInterestPolicy());
    map.put(header + "keyConstraint", this.getKeyConstraint());
    map.put(header + "loadFactor", this.getLoadFactor());
    map.put(header + "lockGrantor", this.getLockGrantor());
    map.put(header + "membershipAttributes", this.getMembershipAttributes());
    if (this.multicastEnabled == null) {
      map.put(header + "multicastEnabled", "inherited from distributed system");
    } else {
      map.put(header + "multicastEnabled", this.getMulticastEnabled());
    }
    map.put(header + "partitionName", this.getPartitionDescriptionName());
    map.put(header + "poolName", this.getPoolDescriptionName());
    map.put(header + "regionIdleTimeout", this.getRegionIdleTimeout());
    map.put(header + "regionName", this.getRegionName());
    map.put(header + "regionTimeToLive", this.getRegionTimeToLive());
    map.put(header + "scope", this.getScope());
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
    Vector names = tab.vecAt(RegionPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create region description from test configuration parameters
      // and do hydra-level validation
      RegionDescription rd = createRegionDescription(name, config, i);

      // create region attributes from non-default region description fields
      // and do product-level validation
      RegionAttributes ra = createRegionAttributes(rd);

      // reset region description based on region attributes to pick up product
      // defaults and side-effects
      resetRegionDescription(rd, ra);

      // save configuration
      config.addRegionDescription(rd);
    }
  }

  /**
   * Creates the initial region description using test configuration parameters
   * and does hydra-level validation.
   */
  private static RegionDescription createRegionDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    RegionDescription rd = new RegionDescription();
    rd.setName(name);

    // asyncEventQueueNames (generates AsyncEventQueueDescriptions)
    {
      Long key = RegionPrms.asyncEventQueueNames;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          rd.setAsyncEventQueueDescriptions(
             getAsyncEventQueueDescriptions(strs, key, config));
          rd.setAsyncEventQueueNames(new ArrayList(strs));
        }
      }
    }
    // cacheListeners
    {
      Long key = RegionPrms.cacheListeners;
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
      Long key = RegionPrms.cacheListenersSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        rd.setCacheListenersSingleton(Boolean.FALSE);
      } else {
        rd.setCacheListenersSingleton(bool);
      }
    }
    // cacheLoader
    {
      Long key = RegionPrms.cacheLoader;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCacheLoader(getCacheLoader(key, str, config));
      }
    }
    // cacheLoaderSingleton
    {
      Long key = RegionPrms.cacheLoaderSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setCacheLoaderSingleton(bool);
      } else if (rd.getCacheLoader() != null) {
        if (rd.getCacheLoader() instanceof String) {
          rd.setCacheLoaderSingleton(Boolean.FALSE); // app case
        } else {
          rd.setCacheLoaderSingleton(Boolean.TRUE);  // edge case
        }
      } // else leave it null rather than guess about later additions
    }
    // cacheWriter
    {
      Long key = RegionPrms.cacheWriter;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCacheWriter(getCacheWriter(key, str, config));
      }
    }
    // cacheWriterSingleton
    {
      Long key = RegionPrms.cacheWriterSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setCacheWriterSingleton(bool);
      } else if (rd.getCacheWriter() != null) {
        if (rd.getCacheWriter() instanceof String) {
          rd.setCacheWriterSingleton(Boolean.FALSE); // app case
        } else {
          rd.setCacheWriterSingleton(Boolean.TRUE);  // edge case
        }
      } // else leave it null rather than guess about later additions
    }
    // cloningEnabled
    {
      Long key = RegionPrms.cloningEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setCloningEnabled(bool);
      }
    }
    // compressor
    {
      Long key = RegionPrms.compressor;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCompressor(str);
      }
    }
    // compressorSingleton
    {
      Long key = RegionPrms.compressorSingleton;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        rd.setCompressorSingleton(Boolean.FALSE);
      } else {
        rd.setCompressorSingleton(bool);
      }
    }
    // concurrencyChecksEnabled
    {
      Long key = RegionPrms.concurrencyChecksEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setConcurrencyChecksEnabled(bool);
      }
    }
    // concurrencyLevel
    {
      Long key = RegionPrms.concurrencyLevel;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        rd.setConcurrencyLevel(i);
      }
    }
    // customEntryIdleTimeout
    {
      Long key = RegionPrms.customEntryIdleTimeout;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCustomEntryIdleTimeout(str);
      }
    }
    // customEntryTimeToLive
    {
      Long key = RegionPrms.customEntryTimeToLive;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setCustomEntryTimeToLive(str);
      }
    }
    // dataPolicy
    {
      Long key = RegionPrms.dataPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        rd.setDataPolicy(getDataPolicy(str, key));
      }
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = RegionPrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        rd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // diskSynchronous
    {
      Long key = RegionPrms.diskSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setDiskSynchronous(bool);
      }
    }
    // enableAsyncConflation
    {
      Long key = RegionPrms.enableAsyncConflation;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setEnableAsyncConflation(bool);
      }
    }
    // enableGateway
    {
      Long key = RegionPrms.enableGateway;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setEnableGateway(bool);
      }
    }
    // enableOffHeapMemory
    {
      Long key = RegionPrms.enableOffHeapMemory;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setEnableOffHeapMemory(bool);
      }
    }
    // enableSubscriptionConflation
    {
      Long key = RegionPrms.enableSubscriptionConflation;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setEnableSubscriptionConflation(bool);
      }
    }
    // entryIdleTimeout
    {
      Long key = RegionPrms.entryIdleTimeout;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEntryIdleTimeout(attributes);
      }
    }
    // entryTimeToLive
    {
      Long key = RegionPrms.entryTimeToLive;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEntryTimeToLive(attributes);
      }
    }
    // evictionAttributes
    {
      Long key = RegionPrms.evictionAttributes;
      EvictionAttributes attributes =
        getEvictionAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setEvictionAttributes(attributes);
      }
    }
    // gatewaySenderNames (generates GatewaySenderDescriptions)
    {
      Long key = RegionPrms.gatewaySenderNames;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          rd.setGatewaySenderDescriptions(
             getGatewaySenderDescriptions(strs, key, config));
          rd.setGatewaySenderNames(new ArrayList(strs));
        }
      }
    }
    // hdfsStoreName (generates HDFSStoreDescription)
    {
      Long key = RegionPrms.hdfsStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setHDFSStoreDescription(getHDFSStoreDescription(str, key, config));
        rd.setHDFSStoreName("HDFSStoreDescription." + str);
      }
    }
    // hdfsWriteOnly
    {
      Long key = RegionPrms.hdfsWriteOnly;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setHDFSWriteOnly(bool);
      }
    }
    // ignoreJTA
    {
      Long key = RegionPrms.ignoreJTA;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setIgnoreJTA(bool);
      }
    }
    // indexMaintenanceSynchronous
    {
      Long key = RegionPrms.indexMaintenanceSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setIndexMaintenanceSynchronous(bool);
      }
    }
    // initialCapacity
    {
      Long key = RegionPrms.initialCapacity;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        rd.setInitialCapacity(i);
      }
    }
    // interestPolicy (really subscriptionAttributes)
    {
      Long key = RegionPrms.interestPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        rd.setInterestPolicy(getSubscriptionAttributes(str, key));
      }
    }
    // keyConstraint
    {
      Long key = RegionPrms.keyConstraint;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setKeyConstraint(getClass(key, str));
      }
    }
    // loadFactor
    {
      Long key = RegionPrms.loadFactor;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rd.setLoadFactor(Float.valueOf(d.floatValue()));
      }
    }
    // lockGrantor
    {
      Long key = RegionPrms.lockGrantor;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setLockGrantor(bool);
      }
    }
    // membershipAttributes
    {
      Long key = RegionPrms.membershipAttributes;
      MembershipAttributes attributes =
        getMembershipAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setMembershipAttributes(attributes);
      }
    }
    // multicastEnabled
    {
      Long key = RegionPrms.multicastEnabled;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        rd.setMulticastEnabled(getBooleanFor(str, key));
      }
    }
    // partitionName (generates partitionDescription)
    {
      Long key = RegionPrms.partitionName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setPartitionDescription(getPartitionDescription(str, key, config));
        rd.setPartitionDescriptionName("PartitionDescription." + str);
      }
    }
    // poolName (generates PoolDescription)
    {
      Long key = RegionPrms.poolName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setPoolDescription(getPoolDescription(str, key, config));
        rd.setPoolDescriptionName("PoolDescription." + str);
      }
    }
    // regionIdleTimeout
    {
      Long key = RegionPrms.regionIdleTimeout;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setRegionIdleTimeout(attributes);
      }
    }
    // regionName
    {
      Long key = RegionPrms.regionName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        rd.setRegionName(RegionPrms.DEFAULT_REGION_NAME);
      } else {
        rd.setRegionName(str);
      }
    }
    // regionTimeToLive
    {
      Long key = RegionPrms.regionTimeToLive;
      ExpirationAttributes attributes =
        getExpirationAttributes(key, tab.vecAtWild(key, index, null), tab);
      if (attributes != null) {
        rd.setRegionTimeToLive(attributes);
      }
    }
    // scope
    {
      Long key = RegionPrms.scope;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        rd.setScope(getScope(str, key));
      }
    }
    // statisticsEnabled
    {
      Long key = RegionPrms.statisticsEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        rd.setStatisticsEnabled(bool);
      }
    }
    // valueConstraint
    {
      Long key = RegionPrms.valueConstraint;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        rd.setValueConstraint(getClass(key, str));
      }
    }
    // disallow partitioning without partitionName and vice versa
    {
      String pname = rd.getPartitionDescriptionName();
      DataPolicy policy = rd.getDataPolicy();
      if (pname != null && !isPartitioned(policy)) {
        String s = "Non-partitioned "
                 + BasePrms.nameForKey(RegionPrms.dataPolicy) + " (" + policy
                 + ") cannot be used with "
                 + BasePrms.nameForKey(RegionPrms.partitionName) + " (" + pname
                 + ")";
        throw new HydraRuntimeException(s);

      } else if (pname != null && policy == null) {
        String s = BasePrms.nameForKey(RegionPrms.dataPolicy)
                 + " must be set explicitly to partition the region when using "
                 + BasePrms.nameForKey(RegionPrms.partitionName) + " (" + pname
                 + ")";
        throw new HydraRuntimeException(s);

      } else if (pname == null && isPartitioned(policy)) {
        String s = BasePrms.nameForKey(RegionPrms.dataPolicy) + " (" + policy
                 + ") requires partition attributes to be set using "
                 + BasePrms.nameForKey(RegionPrms.partitionName);
        throw new HydraRuntimeException(s);
      }
    }
    // disallow use of scope with partitioning
    {
      DataPolicy policy = rd.getDataPolicy();
      Scope scope = rd.getScope();
      if (scope != null && isPartitioned(policy)) {
        String s = BasePrms.nameForKey(RegionPrms.dataPolicy)
                 + " (" + policy + ") requires "
                 + BasePrms.nameForKey(RegionPrms.scope)
                 + " (" + scope + ") to be either unset or set to \""
                 + BasePrms.DEFAULT + "\"";
        throw new HydraRuntimeException(s);
      }
    }
    return rd;
  }

  /**
   * Creates region attributes from the region description, setting only those
   * that are not null.  Picks up product defaults and side-effects, and does
   * product-level validation.
   */
  private static RegionAttributes createRegionAttributes(RegionDescription rd) {
    AttributesFactory f = new AttributesFactory();
    // defer asyncEventQueueDescriptions
    // defer asyncEventQueueNames
    // defer cacheListeners
    // defer cacheLoader
    // defer cacheWriter
    if (rd.cloningEnabled != null) {
      f.setCloningEnabled(rd.getCloningEnabled().booleanValue());
    }
    // defer compressor
    if (rd.concurrencyChecksEnabled != null) {
      f.setConcurrencyChecksEnabled(rd.getConcurrencyChecksEnabled().booleanValue());
    }
    if (rd.concurrencyLevel != null) {
      f.setConcurrencyLevel(rd.getConcurrencyLevel().intValue());
    }
    // defer customEntryIdleTimeout
    // defer customEntryTimeToLive
    if (rd.dataPolicy != null) {
      f.setDataPolicy(rd.getDataPolicy());
    }
    if (rd.diskStoreDescription != null) {
      f.setDiskStoreName(rd.getDiskStoreDescription().getName());
    }
    if (rd.diskSynchronous != null) {
      f.setDiskSynchronous(rd.getDiskSynchronous().booleanValue());
    }
    if (rd.enableAsyncConflation != null) {
      f.setEnableAsyncConflation(rd.getEnableAsyncConflation().booleanValue());
    }
    if (rd.enableGateway != null) {
      f.setEnableGateway(rd.getEnableGateway().booleanValue());
    }
    if (rd.enableOffHeapMemory != null) {
      f.setEnableOffHeapMemory(rd.getEnableOffHeapMemory().booleanValue());
    }
    if (rd.enableSubscriptionConflation != null) {
      f.setEnableSubscriptionConflation(rd.getEnableSubscriptionConflation().booleanValue());
    }
    if (rd.entryIdleTimeout != null) {
      f.setEntryIdleTimeout(rd.getEntryIdleTimeout());
    }
    if (rd.entryTimeToLive != null) {
      f.setEntryTimeToLive(rd.getEntryTimeToLive());
    }
    if (rd.evictionAttributes != null) {
      f.setEvictionAttributes(rd.getEvictionAttributes());
    }
    // defer gatewaySenderDescriptions
    // defer gatewaySenderNames
    if (rd.hdfsStoreDescription != null) {
      f.setHDFSStoreName(rd.getHDFSStoreDescription().getName());
    }
    if (rd.hdfsWriteOnly != null) {
      f.setHDFSWriteOnly(rd.getHDFSWriteOnly().booleanValue());
    }
    if (rd.ignoreJTA != null) {
      f.setIgnoreJTA(rd.getIgnoreJTA().booleanValue());
    }
    if (rd.indexMaintenanceSynchronous != null) {
      f.setIndexMaintenanceSynchronous(rd.getIndexMaintenanceSynchronous().booleanValue());
    }
    if (rd.initialCapacity != null) {
      f.setInitialCapacity(rd.getInitialCapacity().intValue());
    }
    if (rd.interestPolicy != null) {
      f.setSubscriptionAttributes(rd.getInterestPolicy());
    }
    if (rd.keyConstraint != null) {
      f.setKeyConstraint(rd.getKeyConstraint());
    }
    if (rd.loadFactor != null) {
      f.setLoadFactor(rd.getLoadFactor().floatValue());
    }
    if (rd.lockGrantor != null) {
      f.setLockGrantor(rd.getLockGrantor().booleanValue());
    }
    if (rd.membershipAttributes != null) {
      f.setMembershipAttributes(rd.getMembershipAttributes());
    }
    if (rd.multicastEnabled != null) {
      f.setMulticastEnabled(rd.getMulticastEnabled().booleanValue());
    }
    // defer partitionAttributes
    if (rd.poolDescription != null) {
      f.setPoolName(rd.getPoolDescription().getName());
    }
    if (rd.regionIdleTimeout != null) {
      f.setRegionIdleTimeout(rd.getRegionIdleTimeout());
    }
    if (rd.regionTimeToLive != null) {
      f.setRegionTimeToLive(rd.getRegionTimeToLive());
    }
    if (rd.scope != null && !isPartitioned(rd.dataPolicy)) {
      f.setScope(rd.getScope());
    }
    if (rd.statisticsEnabled != null) {
      f.setStatisticsEnabled(rd.getStatisticsEnabled().booleanValue());
    }
    if (rd.valueConstraint != null) {
      f.setValueConstraint(rd.getValueConstraint());
    }
    return f.create();
  }

  /**
   * Resets the region description based on region attributes, to pick up
   * product defaults and side-effects.
   */
  private static void resetRegionDescription(RegionDescription rd,
                                             RegionAttributes ra) {
    // defer asyncEventQueueDescriptions
    // defer asyncEventQueueNames
    // defer cacheListeners
    // defer cacheLoader
    // defer cacheWriter
    rd.setCloningEnabled(Boolean.valueOf(ra.getCloningEnabled()));
    // defer compressor
    rd.setConcurrencyChecksEnabled(Boolean.valueOf(ra.getConcurrencyChecksEnabled()));
    rd.setConcurrencyLevel(Integer.valueOf(ra.getConcurrencyLevel()));
    // defer customEntryIdleTimeout
    // defer customEntryTimeToLive
    rd.setDataPolicy(ra.getDataPolicy());
    rd.setDiskSynchronous(Boolean.valueOf(ra.isDiskSynchronous()));
    rd.setEnableAsyncConflation(Boolean.valueOf(ra.getEnableAsyncConflation()));
    rd.setEnableGateway(Boolean.valueOf(ra.getEnableGateway()));
    rd.setEnableOffHeapMemory(Boolean.valueOf(ra.getEnableOffHeapMemory()));
    rd.setEnableSubscriptionConflation(Boolean.valueOf(ra.getEnableSubscriptionConflation()));
    rd.setEntryIdleTimeout(ra.getEntryIdleTimeout());
    rd.setEntryTimeToLive(ra.getEntryTimeToLive());
    rd.setEvictionAttributes(ra.getEvictionAttributes());
    // defer gatewaySenderDescriptions
    // defer gatewaySenderNames
    rd.setHDFSStoreName(ra.getHDFSStoreName());
    rd.setHDFSWriteOnly(Boolean.valueOf(ra.getHDFSWriteOnly()));
    rd.setIgnoreJTA(Boolean.valueOf(ra.getIgnoreJTA()));
    rd.setIndexMaintenanceSynchronous(Boolean.valueOf(ra.getIndexMaintenanceSynchronous()));
    rd.setInitialCapacity(Integer.valueOf(ra.getInitialCapacity()));
    rd.setInterestPolicy(ra.getSubscriptionAttributes());
    rd.setKeyConstraint(ra.getKeyConstraint());
    rd.setLoadFactor(Float.valueOf(ra.getLoadFactor()));
    rd.setLockGrantor(Boolean.valueOf(ra.isLockGrantor()));
    rd.setMembershipAttributes(ra.getMembershipAttributes());
    if (rd.multicastEnabled != null) {
      rd.setMulticastEnabled(Boolean.valueOf(ra.getMulticastEnabled()));
    } // else leave null to defer to distributed system
    rd.setRegionIdleTimeout(ra.getRegionIdleTimeout());
    // regionName is not a region attribute
    rd.setRegionTimeToLive(ra.getRegionTimeToLive());
    rd.setScope(ra.getScope());
    rd.setStatisticsEnabled(Boolean.valueOf(ra.getStatisticsEnabled()));
    rd.setValueConstraint(ra.getValueConstraint());
  }

  //------------------------------------------------------------------------------
  //Compressor configuration support
  //------------------------------------------------------------------------------
  
  /**
   * Returns compressor info from the given string, which is expected to hold
   * an application classname, validated to exist and implement Compressor.
   *
   * @throws HydraConfigException if the value has an illegal value or type.
   */
  private static String getCompressor(Long key, String val, TestConfig config) {
    return getCompressorInstance(val).getClass().getName();
  }
  
  /**
   * Returns a compressor instance.
   * <p>
   * Manages singletons if instantiate is true, which means that this is a real
   * runtime instance rather than a test configuration instance.
   */
  private synchronized Compressor getCompressorInstance(boolean instantiate) {
    String s = this.getCompressor();
    if (s == null) {
      return null;
    } else {
      if (instantiate && this.getCompressorSingleton().booleanValue()) {
        // singleton case
        Object instance = compressorInstances.get(this.getName());
        if (instance == null) { // create and save a new instance
          Compressor compressor = getCompressorInstance(s);
          compressorInstances.put(this.getName(), compressor);
          return compressor;
        } else { // use the existing instance
          return (Compressor)instance;
        }
      } else {
        // non-singleton case, create a new instance
        return getCompressorInstance(s);
      }
    }
  }

  /**
   * Returns an application-defined compressor instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement Compressor.
   */
  private static Compressor getCompressorInstance(String classname) {
    Long key = RegionPrms.compressor;
    Object obj = getInstance(key, classname);
    try {
      return (Compressor)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement Compressor: " + classname;
      throw new HydraConfigException(s);
    }
  }
  
//------------------------------------------------------------------------------
// Async event queue configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the async event queue descriptions for the given strings.
   * @throws HydraConfigException if a given string is not listed in {@link
   *         AsyncEventQueuePrms#names}.
   */
  private static List<AsyncEventQueueDescription>
      getAsyncEventQueueDescriptions(List<String> strs, Long key,
                                     TestConfig config) {
    List<AsyncEventQueueDescription> aeqds = new ArrayList();
    for (String str : strs) {
      AsyncEventQueueDescription aeqd =
                config.getAsyncEventQueueDescription(str);
      if (aeqd == null) {
        String s = BasePrms.nameForKey(key) + " not found in "
                 + BasePrms.nameForKey(AsyncEventQueuePrms.names) + ": " + str;
        throw new HydraConfigException(s);
      }
      aeqds.add(aeqd);
    }
    return aeqds;
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
      Long key = RegionPrms.cacheListeners;
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
    Long key = RegionPrms.cacheListeners;
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
// Cache loader configuration support
//------------------------------------------------------------------------------

  /**
   * Returns cache loader info from the given string, which is expected to hold
   * an application classname, validated to exist and implement CacheLoader.
   *
   * @throws HydraConfigException if the value has an illegal value or type.
   */
  private static String getCacheLoader(Long key, String val,
                                       TestConfig config) {
    return getCacheLoaderInstance(val).getClass().getName();
  }

  /**
   * Returns a cache loader instance.
   * <p>
   * Manages singletons if instantiate is true, which means that this is a real
   * runtime instance rather than a test configuration instance.
   */
  private synchronized CacheLoader getCacheLoaderInstance(boolean instantiate) {
    String s = this.getCacheLoader();
    if (s == null) {
      return null;
    } else {
      if (instantiate && this.getCacheLoaderSingleton().booleanValue()) {
        // singleton case
        Object instance = cacheLoaderInstances.get(this.getName());
        if (instance == null) { // create and save a new instance
          CacheLoader loader = getCacheLoaderInstance(s);
          cacheLoaderInstances.put(this.getName(), loader);
          return loader;
        } else { // use the existing instance
          return (CacheLoader)instance;
        }
      } else {
        // non-singleton case, create a new instance
        return getCacheLoaderInstance(s);
      }
    }
  }

  /**
   * Returns an application-defined cache loader instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement CacheLoader.
   */
  private static CacheLoader getCacheLoaderInstance(String classname) {
    Long key = RegionPrms.cacheLoader;
    Object obj = getInstance(key, classname);
    try {
      return (CacheLoader)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement CacheLoader: " + classname;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Cache writer configuration support
//------------------------------------------------------------------------------

  /**
   * Returns cache writer info from the given string, which is expected to hold
   * an application classname, validated to exist and implement CacheWriter.
   *
   * @throws HydraConfigException if the value has an illegal value or type.
   */
  private static String getCacheWriter(Long key, String val,
                                       TestConfig config) {
    return getCacheWriterInstance(val).getClass().getName();
  }

  /**
   * Returns a cache writer instance.
   * <p>
   * Manages singletons if instantiate is true, which means that this is a real
   * runtime instance rather than a test configuration instance.
   * <p>
   * This method is public to support DynamicRegionFactory configuration only.
   */
  public synchronized CacheWriter getCacheWriterInstance(boolean instantiate) {
    String s = this.getCacheWriter();
    if (s == null) {
      return null;
    } else {
      if (instantiate && this.getCacheWriterSingleton().booleanValue()) {
        // singleton case
        Object instance = cacheWriterInstances.get(this.getName());
        if (instance == null) { // create and save a new instance
          CacheWriter writer = getCacheWriterInstance(s);
          cacheWriterInstances.put(this.getName(), writer);
          return writer;
        } else { // use the existing instance
          return (CacheWriter)instance;
        }
      } else {
        // non-singleton case, create a new instance
        return getCacheWriterInstance(s);
      }
    }
  }

  /**
   * Returns an application-defined cache writer instance for the classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement CacheWriter.
   */
  private static CacheWriter getCacheWriterInstance(String classname) {
    Long key = RegionPrms.cacheWriter;
    Object obj = getInstance(key, classname);
    try {
      return (CacheWriter)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement CacheWriter: " + classname;
      throw new HydraConfigException(s);
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
      return getCustomExpiry(classname, RegionPrms.customEntryIdleTimeout);
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
      return getCustomExpiry(classname, RegionPrms.customEntryTimeToLive);
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
// Data policy configuration support
//------------------------------------------------------------------------------

  private static boolean isPartitioned(DataPolicy policy) {
    return policy != null && policy.toString().contains(PARTITION);
  }

  /**
   * Returns the DataPolicy for the given string.
   */
  private static DataPolicy getDataPolicy(String str, Long key) {
    if (str.equalsIgnoreCase("empty")) {
      return DataPolicy.EMPTY;

    } else if (str.equalsIgnoreCase("normal")) {
      return DataPolicy.NORMAL;

    } else if (str.equalsIgnoreCase("hdfsPartition")
            || str.equalsIgnoreCase("hdfsPartitioned")
            || str.equalsIgnoreCase("hdfs_partition")
            || str.equalsIgnoreCase("hdfs_partitioned")) {
      return DataPolicy.HDFS_PARTITION;

    } else if (str.equalsIgnoreCase("hdfsPersistentPartition")
            || str.equalsIgnoreCase("hdfsPersistentPartitioned")
            || str.equalsIgnoreCase("hdfs_persistent_partition")
            || str.equalsIgnoreCase("hdfs_persistent_partitioned")) {
      return DataPolicy.HDFS_PERSISTENT_PARTITION;

    } else if (str.equalsIgnoreCase("preloaded")
            || str.equalsIgnoreCase("preload")) {
      return DataPolicy.PRELOADED;

    } else if (str.equalsIgnoreCase("partition")
            || str.equalsIgnoreCase("partitioned")) {
      return DataPolicy.PARTITION;

    } else if (str.equalsIgnoreCase("persistentPartition")
            || str.equalsIgnoreCase("persistentPartitioned")
            || str.equalsIgnoreCase("persistent_partition")
            || str.equalsIgnoreCase("persistent_partitioned")) {
      return DataPolicy.PERSISTENT_PARTITION;

    } else if (str.equalsIgnoreCase("persistentReplicate")
            || str.equalsIgnoreCase("persistentReplicated")
            || str.equalsIgnoreCase("persistent_replicate")
            || str.equalsIgnoreCase("persistent_replicated")) {
      return DataPolicy.PERSISTENT_REPLICATE;

    } else if (str.equalsIgnoreCase("replicate")
            || str.equalsIgnoreCase("replicated")) {
      return DataPolicy.REPLICATE;

    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
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
// Gateway sender configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the gateway sender descriptions for the given strings.
   * @throws HydraConfigException if a given string is not listed in {@link
   *         GatewaySenderPrms#names}.
   */
  private static List<GatewaySenderDescription> getGatewaySenderDescriptions(
                              List<String> strs, Long key, TestConfig config) {
    List<GatewaySenderDescription> gsds = new ArrayList();
    for (String str : strs) {
      GatewaySenderDescription gsd = config.getGatewaySenderDescription(str);
      if (gsd == null) {
        String s = BasePrms.nameForKey(key) + " not found in "
                 + BasePrms.nameForKey(GatewaySenderPrms.names) + ": " + str;
        throw new HydraConfigException(s);
      }
      gsds.add(gsd);
    }
    return gsds;
  }

//------------------------------------------------------------------------------
// HDFS store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the HDFS store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         HDFSStorePrms#names}.
   */
  private static HDFSStoreDescription getHDFSStoreDescription(String str,
                                                  Long key, TestConfig config) {
    HDFSStoreDescription hsd = config.getHDFSStoreDescription(str);
    if (hsd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(HDFSStorePrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return hsd;
    }
  }

//------------------------------------------------------------------------------
// Interest policy configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the SubscriptionAttributes for the given string.
   */
  private static SubscriptionAttributes getSubscriptionAttributes(String str,
                                                                  Long key) {
    InterestPolicy policy = getInterestPolicy(str, key);
    return new SubscriptionAttributes(policy);
  }

  /**
   * Returns the InterestPolicy for the given string.
   */
  private static InterestPolicy getInterestPolicy(String str, Long key) {
    if (str.equalsIgnoreCase("all")) {
      return InterestPolicy.ALL;
    } else if (str.equalsIgnoreCase("cache_content")
            || str.equalsIgnoreCase("cacheContent")) {
      return InterestPolicy.CACHE_CONTENT;
    } else {
      String s = BasePrms.nameForKey(key)
               + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Membership attributes configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the membership attributes from the given vector, which is expected
   * to hold roles and optional loss and resumption actions, or null if there
   * are no roles.
   * @throws HydraConfigException if the value is malformed or a field contains
   *         an illegal value or type.
   */
  private static MembershipAttributes getMembershipAttributes(Long key,
                                      Vector val, ConfigHashtable tab) {
    if (val == null) {
      return null;
    } else {
      String[] roles = null;
      LossAction loss = null;
      ResumptionAction resumption = null;

      // read fields
      if (val.size() > 0) {
        String rolesStr = tab.getString(key, val.get(0));
        if (rolesStr != null) {
          roles = getRequiredRoles(rolesStr);
        }
      }
      if (val.size() > 1) {
        String lossStr = tab.getString(key, val.get(1));
        if (lossStr != null) {
          loss = getLossAction(lossStr, key);
        }
      }
      if (val.size() > 2) {
        String resumptionStr = tab.getString(key, val.get(2));
        if (resumptionStr != null) {
          resumption = getResumptionAction(resumptionStr, key);
        }
      }
      if (val.size() > 3) {
        String s = BasePrms.nameForKey(key) + " has too many fields: " + val;
        throw new HydraConfigException(s);
      }

      // fill in defaults where needed
      if (loss == null && resumption != null) {
        // this is the default when roles exist but there is no way to get it
        // from the product since the default constructor sets it differently
        loss = LossAction.NO_ACCESS;
      }

      // invoke appropriate constructor
      if (resumption != null) {
        return new MembershipAttributes(roles, loss, resumption);
      } else if (loss != null) {
        return new MembershipAttributes(roles, loss, resumption);
      } else if (roles != null) {
        return new MembershipAttributes(roles);
      } else {
        return new MembershipAttributes();
      }
    }
  }

  /**
   * Returns the required roles for the given string.
   */
  private static String[] getRequiredRoles(String str) {
    if (str.equalsIgnoreCase(BasePrms.NONE)) {
      return null;
    } else {
      StringTokenizer st = new StringTokenizer(str, ":", false);
      int numTokens = st.countTokens();
      String[] roles = new String[numTokens];
      for (int i = 0; i < numTokens; i++) {
        roles[i] = st.nextToken().trim();
      }
      return roles;
    }
  }

  /**
   * Returns the LossAction for the given string.
   */
  private static LossAction getLossAction(String str, Long key) {
    if (str.equalsIgnoreCase("no_access")
            || str.equalsIgnoreCase("noAccess")) {
      return LossAction.NO_ACCESS;
    } else if (str.equalsIgnoreCase("limited_access")
            || str.equalsIgnoreCase("limitedAccess")) {
      return LossAction.LIMITED_ACCESS;
    } else if (str.equalsIgnoreCase("full_access")
            || str.equalsIgnoreCase("fullAccess")) {
      return LossAction.FULL_ACCESS;
    } else if (str.equalsIgnoreCase("reconnect")) {
      return LossAction.RECONNECT;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal loss action: " + str;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the ResumptionAction for the given string.
   */
  private static ResumptionAction getResumptionAction(String str, Long key) {
    if (str.equalsIgnoreCase("none")) {
      return ResumptionAction.NONE;
    } else if (str.equalsIgnoreCase("reinitialize")) {
      return ResumptionAction.REINITIALIZE;
    } else {
      String s = BasePrms.nameForKey(key)
               + " has illegal resumption action: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Partition configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the partition description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         PartitionPrms#names}.
   */
  private static PartitionDescription getPartitionDescription(
                                      String str, Long key, TestConfig config) {
    PartitionDescription pd = config.getPartitionDescription(str);
    if (pd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(PartitionPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return pd;
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

//------------------------------------------------------------------------------
// Scope configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the Scope for the given string.
   */
  private static Scope getScope(String str, Long key) {
    if (str.equalsIgnoreCase("local")) {
      return Scope.LOCAL;
    } else if (str.equalsIgnoreCase("global")) {
      return Scope.GLOBAL;
    } else if (str.equalsIgnoreCase("ack")
            || str.equalsIgnoreCase("dack")
            || str.equalsIgnoreCase("d_ack")
            || str.equalsIgnoreCase("distributedAck")
            || str.equalsIgnoreCase("distributed_ack")) {
      return Scope.DISTRIBUTED_ACK;
    } else if (str.equalsIgnoreCase("noAck")
            || str.equalsIgnoreCase("no_ack")
            || str.equalsIgnoreCase("dNoAck")
            || str.equalsIgnoreCase("d_no_ack")
            || str.equalsIgnoreCase("distributedNoAck")
            || str.equalsIgnoreCase("distributed_no_ack")) {
      return Scope.DISTRIBUTED_NO_ACK;
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
    this.asyncEventQueueDescriptions = (List<AsyncEventQueueDescription>)in.readObject();
    this.asyncEventQueueNames = (List<String>)in.readObject();
    this.cacheListeners = (List)in.readObject();
    this.cacheListenersSingleton = (Boolean)in.readObject();
    this.cacheLoader = (String)in.readObject();
    this.cacheLoaderSingleton = (Boolean)in.readObject();
    this.cacheWriter = (String)in.readObject();
    this.cacheWriterSingleton = (Boolean)in.readObject();
    this.cloningEnabled = (Boolean)in.readObject();
    this.compressor = (String)in.readObject();
    this.compressorSingleton = (Boolean)in.readObject();
    this.concurrencyChecksEnabled = (Boolean)in.readObject();
    this.concurrencyLevel = (Integer)in.readObject();
    this.customEntryIdleTimeout = (String)in.readObject();
    this.customEntryTimeToLive = (String)in.readObject();
    this.dataPolicy = readDataPolicy(in);
    this.diskStoreName = (String)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    this.enableAsyncConflation = (Boolean)in.readObject();
    this.enableGateway = (Boolean)in.readObject();
    this.enableOffHeapMemory = (Boolean)in.readObject();
    this.enableSubscriptionConflation = (Boolean)in.readObject();
    this.entryIdleTimeout = readExpirationAttributes(in);
    this.entryTimeToLive = readExpirationAttributes(in);
    this.evictionAttributes = readEvictionAttributes(in);
    this.gatewaySenderDescriptions = (List<GatewaySenderDescription>)in.readObject();
    this.gatewaySenderNames = (List<String>)in.readObject();
    this.hdfsStoreName = (String)in.readObject();
    this.hdfsStoreDescription = (HDFSStoreDescription)in.readObject();
    this.hdfsWriteOnly = (Boolean)in.readObject();
    this.ignoreJTA = (Boolean)in.readObject();
    this.indexMaintenanceSynchronous = (Boolean)in.readObject();
    this.initialCapacity = (Integer)in.readObject();
    this.interestPolicy = (SubscriptionAttributes)in.readObject();
    this.keyConstraint = (Class)in.readObject();
    this.loadFactor = (Float)in.readObject();
    this.lockGrantor = (Boolean)in.readObject();
    this.membershipAttributes = (MembershipAttributes)in.readObject();
    this.multicastEnabled = (Boolean)in.readObject();
    this.name = (String)in.readObject();
    this.partitionDescription = (PartitionDescription)in.readObject();
    this.partitionDescriptionName = (String)in.readObject();
    this.poolDescription = (PoolDescription)in.readObject();
    this.poolDescriptionName = (String)in.readObject();
    this.regionIdleTimeout = readExpirationAttributes(in);
    this.regionName = (String)in.readObject();
    this.regionTimeToLive = readExpirationAttributes(in);
    this.scope = readScope(in);
    this.statisticsEnabled = (Boolean)in.readObject();
    this.valueConstraint = (Class)in.readObject();

    // create region attributes from non-default region description fields
    // and do product-level validation
    RegionAttributes ra = createRegionAttributes(this);

    // reset region description based on region attributes to pick up product
    // defaults and side-effects
    resetRegionDescription(this, ra);
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.asyncEventQueueDescriptions);
    out.writeObject(this.asyncEventQueueNames);
    out.writeObject(this.cacheListeners);
    out.writeObject(this.cacheListenersSingleton);
    out.writeObject(this.cacheLoader);
    out.writeObject(this.cacheLoaderSingleton);
    out.writeObject(this.cacheWriter);
    out.writeObject(this.cacheWriterSingleton);
    out.writeObject(this.cloningEnabled);
    out.writeObject(this.compressor);
    out.writeObject(this.compressorSingleton);
    out.writeObject(this.concurrencyChecksEnabled);
    out.writeObject(this.concurrencyLevel);
    out.writeObject(this.customEntryIdleTimeout);
    out.writeObject(this.customEntryTimeToLive);
    out.writeObject(this.dataPolicy.toString());
    out.writeObject(this.diskStoreName);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskSynchronous);
    out.writeObject(this.enableAsyncConflation);
    out.writeObject(this.enableGateway);
    out.writeObject(this.enableOffHeapMemory);
    out.writeObject(this.enableSubscriptionConflation);
    writeExpirationAttributes(this.entryIdleTimeout, out);
    writeExpirationAttributes(this.entryTimeToLive, out);
    writeEvictionAttributes(this.evictionAttributes, out);
    out.writeObject(this.gatewaySenderDescriptions);
    out.writeObject(this.gatewaySenderNames);
    out.writeObject(this.hdfsStoreName);
    out.writeObject(this.hdfsStoreDescription);
    out.writeObject(this.hdfsWriteOnly);
    out.writeObject(this.ignoreJTA);
    out.writeObject(this.indexMaintenanceSynchronous);
    out.writeObject(this.initialCapacity);
    out.writeObject(this.interestPolicy);
    out.writeObject(this.keyConstraint);
    out.writeObject(this.loadFactor);
    out.writeObject(this.lockGrantor);
    out.writeObject(this.membershipAttributes);
    out.writeObject(this.multicastEnabled);
    out.writeObject(this.name);
    out.writeObject(this.partitionDescription);
    out.writeObject(this.partitionDescriptionName);
    out.writeObject(this.poolDescription);
    out.writeObject(this.poolDescriptionName);
    writeExpirationAttributes(this.regionIdleTimeout, out);
    out.writeObject(this.regionName);
    writeExpirationAttributes(this.regionTimeToLive, out);
    writeScope(this.scope, out);
    out.writeObject(this.statisticsEnabled);
    out.writeObject(this.valueConstraint);
  }

  /**
   * Required to throw better error messages when using a data policy that is
   * unsupported in a given version.
   */
  private DataPolicy readDataPolicy(ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    String str = (String)in.readObject();
    if (str == null) {
      return null;
    } else {
      if (str.equals(DataPolicy.EMPTY.toString())) {
        return DataPolicy.EMPTY;
      } else if (str.equals(DataPolicy.NORMAL.toString())) {
        return DataPolicy.NORMAL;
      } else if (str.equals(DataPolicy.HDFS_PARTITION.toString())) {
        return DataPolicy.HDFS_PARTITION;
      } else if (str.equals(DataPolicy.HDFS_PERSISTENT_PARTITION.toString())) {
        return DataPolicy.HDFS_PERSISTENT_PARTITION;
      } else if (str.equals(DataPolicy.PRELOADED.toString())) {
        return DataPolicy.PRELOADED;
      } else if (str.equals(DataPolicy.PARTITION.toString())) {
        return DataPolicy.PARTITION;
      } else if (str.equals(DataPolicy.PERSISTENT_PARTITION.toString())) {
        return DataPolicy.PERSISTENT_PARTITION;
      } else if (str.equals(DataPolicy.PERSISTENT_REPLICATE.toString())) {
        return DataPolicy.PERSISTENT_REPLICATE;
      } else if (str.equals(DataPolicy.REPLICATE.toString())) {
        return DataPolicy.REPLICATE;
      } else {
        String s = BasePrms.nameForKey(RegionPrms.dataPolicy)
                 + " has illegal value: " + str;
        throw new HydraConfigException(s);
      }
    }
  }

  /**
   * Required for 5.7 due to change from Serializable to DataSerializable.
   */
  private ExpirationAttributes readExpirationAttributes(ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    Object obj = in.readObject();
    if (obj == null) {
      return null;
    }
    else {
      return new ExpirationAttributes(
          /* timeout */ ((Integer)obj).intValue(),
          /* action */ (ExpirationAction)in.readObject());
    }
  }

  /**
   * Required for 5.7 due to change from Serializable to DataSerializable.
   */
  private void writeExpirationAttributes(ExpirationAttributes attributes,
                                         ObjectOutputStream out)
  throws IOException {
    if (attributes == null) {
      out.writeObject(null);
    }
    else {
      out.writeObject(Integer.valueOf(attributes.getTimeout()));
      out.writeObject(attributes.getAction());
    }
  }

  /**
   * Required for 5.7 due to change from Serializable to DataSerializable.
   */
  private EvictionAttributes readEvictionAttributes(ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    EvictionAlgorithm algorithm = (EvictionAlgorithm)in.readObject();
    if (algorithm == null) {
      return null;
    }
    else {
      if (algorithm.isNone()) {
        return null; // rely on the product to reset
      }
      else if (algorithm.isLRUEntry()) {
        return EvictionAttributes.createLRUEntryAttributes(
          /* maximum */ ((Integer)in.readObject()).intValue(),
          /* action */ (EvictionAction)in.readObject());
      }
      else if (algorithm.isLRUHeap()) {
        return EvictionAttributes.createLRUHeapAttributes(
          /* sizer */ (ObjectSizer)in.readObject(),
          /* action */ (EvictionAction)in.readObject());
      }
      else if (algorithm.isLRUMemory()) {
        return EvictionAttributes.createLRUMemoryAttributes(
          /* maximum */ ((Integer)in.readObject()).intValue(),
          /* sizer */ (ObjectSizer)in.readObject(),
          /* action */ (EvictionAction)in.readObject());
      }
      else {
        String s = "Unknown eviction algorithm: " + algorithm;
        throw new HydraInternalException(s);
      }
    }
  }

  /**
   * Required for 5.7 due to change from Serializable to DataSerializable.
   */
  private void writeEvictionAttributes(EvictionAttributes attributes,
                                       ObjectOutputStream out)
  throws IOException {
    if (attributes == null) {
      out.writeObject(null);
    }
    else {
      EvictionAlgorithm algorithm = attributes.getAlgorithm();
      out.writeObject(algorithm);
      if (algorithm.isNone()) {
        // nothing more to do
      }
      else if (algorithm.isLRUEntry()) {
        out.writeObject(Integer.valueOf(attributes.getMaximum()));
        out.writeObject(attributes.getAction());
      }
      else if (algorithm.isLRUHeap()) {
        out.writeObject(attributes.getObjectSizer());
        out.writeObject(attributes.getAction());
      }
      else if (algorithm.isLRUMemory()) {
        out.writeObject(Integer.valueOf(attributes.getMaximum()));
        out.writeObject(attributes.getObjectSizer());
        out.writeObject(attributes.getAction());
      }
      else {
        String s = "Unknown eviction algorithm: " + algorithm;
        throw new HydraInternalException(s);
      }
    }
  }

  /**
   * Required for 5.7 due to change from 1.4 source compatibility to pure 1.5.
   */
  private Scope readScope(ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    String s = (String)in.readObject();
    if (s == null) {
      return null;
    }
    else {
      return Scope.fromString(s);
    }
  }

  /**
   * Required for 5.7 due to change from 1.4 source compatibility to pure 1.5.
   */
  private void writeScope(Scope scope, ObjectOutputStream out)
  throws IOException {
    if (scope == null) {
      out.writeObject(null);
    }
    else {
      out.writeObject(scope.toString());
    }
  }
}
