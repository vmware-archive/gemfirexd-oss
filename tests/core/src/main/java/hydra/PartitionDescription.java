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
import com.gemstone.gemfire.cache.partition.PartitionListener;

import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a partition.
 */
public class PartitionDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this partition description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String  colocatedWith;
  private FixedPartitionDescription fixedPartitionDescription; // from fixedPartitionName
  private String  fixedPartitionDescriptionName;
  private Integer localMaxMemory;
  private List<String> partitionListeners;
  private String  partitionResolver;
  private Long    recoveryDelay;
  private Integer redundantCopies;
  private Long    startupRecoveryDelay;
  private Long    totalMaxMemory;
  private Integer totalNumBuckets;

  /** Cached fields */
  private transient PartitionAttributesFactory partitionAttributesFactory;
  private transient PartitionAttributes partitionAttributes;
  private transient List<PartitionListener> partitionListenerInstances;
  private transient PartitionResolver partitionResolverInstance;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public PartitionDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this partition description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this partition description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the colocated with.
   */
  private String getColocatedWith() {
    return this.colocatedWith;
  }

  /**
   * Sets the colocated with.
   */
  private void setColocatedWith(String str) {
    this.colocatedWith = str;
  }

  /**
   * Returns the fixed partition description name.
   */
  private String getFixedPartitionDescriptionName() {
    return this.fixedPartitionDescriptionName;
  }

  /**
   * Sets the fixed partition description name.
   */
  private void setFixedPartitionDescriptionName(String str) {
    this.fixedPartitionDescriptionName = str;
  }

  /**
   * Returns the fixed partition description.
   */
  public FixedPartitionDescription getFixedPartitionDescription() {
    return this.fixedPartitionDescription;
  }

  /**
   * Sets the fixed partition description.
   */
  private void setFixedPartitionDescription(FixedPartitionDescription fpd) {
    this.fixedPartitionDescription = fpd;
  }

  /**
   * Returns the local max memory.
   */
  private Integer getLocalMaxMemory() {
    return this.localMaxMemory;
  }

  /**
   * Sets the local max memory.
   */
  private void setLocalMaxMemory(Integer i) {
    this.localMaxMemory = i;
  }

  /**
   * Returns the partition listeners.
   */
  private List<String> getPartitionListeners() {
    return this.partitionListeners;
  }

  /**
   * Sets the partition listeners.
   */
  private void setPartitionListeners(List classnames) {
    this.partitionListeners = classnames;
  }

  /**
   * Returns the partition resolver.
   */
  private String getPartitionResolver() {
    return this.partitionResolver;
  }

  /**
   * Sets the partition resolver.
   */
  private void setPartitionResolver(String str) {
    this.partitionResolver = str;
  }

  /**
   * Returns the singleton partition listener instances for this description.
   *
   * @throws HydraConfigException if instantiation fails or the classes do not
   *                              implement PartitionListener.
   */
  private synchronized List<PartitionListener> getPartitionListenerInstances() {
    if (this.partitionListenerInstances == null) {
      this.partitionListenerInstances = createPartitionListenerInstances();
    }
    return this.partitionListenerInstances;
  }

  /**
   * Returns the singleton partition resolver instance for this description.
   *
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PartitionResolver.
   */
  private synchronized PartitionResolver getPartitionResolverInstance() {
    if (this.partitionResolverInstance == null) {
      this.partitionResolverInstance = createPartitionResolverInstance();
    }
    return this.partitionResolverInstance;
  }

  /**
   * Returns the recoveryDelay
   */
  private Long getRecoveryDelay() {
    return this.recoveryDelay;
  }

  /**
   * Sets the recoveryDelay
   */
  private void setRecoveryDelay(Long l) {
    this.recoveryDelay = l;
  }
  
  /**
   * Returns the redundant copies.
   */
  public Integer getRedundantCopies() {
    return this.redundantCopies;
  }

  /**
   * Sets the redundant copies.
   */
  private void setRedundantCopies(Integer i) {
    this.redundantCopies = i;
  }

  /**
   * Returns the startupRecoveryDelay
   */
  private Long getStartupRecoveryDelay() {
    return this.startupRecoveryDelay;
  }

  /**
   * Sets the startupRecoveryDelay
   */
  private void setStartupRecoveryDelay(Long l) {
    this.startupRecoveryDelay = l;
  }


  /**
   * Returns the total max memory.
   */
  private Long getTotalMaxMemory() {
    return this.totalMaxMemory;
  }

  /**
   * Sets the total max memory.
   */
  private void setTotalMaxMemory(Long l) {
    this.totalMaxMemory = l;
  }

  /**
   * Returns the total num buckets.
   */
  private Integer getTotalNumBuckets() {
    return this.totalNumBuckets;
  }

  /**
   * Sets the total num buckets.
   */
  private void setTotalNumBuckets(Integer i) {
    this.totalNumBuckets = i;
  }

//------------------------------------------------------------------------------
// Attribute creation
//------------------------------------------------------------------------------

  /**
   * Returns the cached instance of partition attributes factory for this
   * description.  Instantiates and caches the partition listeners and the
   * partition resolver, if any.  Makes the default region name available to
   * the fixed partition mapping algorithm, if any.
   */
  public synchronized PartitionAttributesFactory
                         getPartitionAttributesFactory() {
    return getPartitionAttributesFactory(RegionPrms.DEFAULT_REGION_NAME);
  }

  /**
   * Returns the cached instance of partition attributes factory for this
   * description.  Instantiates and caches the partition listeners and the
   * partition resolver, if any.  Makes the region name available to the
   * fixed partition mapping algorithm, if any.
   */
  protected synchronized PartitionAttributesFactory
                         getPartitionAttributesFactory(String regionName) {
    if (this.partitionAttributesFactory == null) {
      PartitionAttributesFactory f = new PartitionAttributesFactory();

      f.setColocatedWith(this.getColocatedWith());
      if (this.getFixedPartitionDescription() != null
          && (this.getLocalMaxMemory() == null || this.getLocalMaxMemory() > 0)) {
        List<FixedPartitionAttributes> fpas = getFixedPartitionDescription()
          .getFixedPartitionAttributes(regionName, this.getRedundantCopies());
        if (fpas != null) {
          for (FixedPartitionAttributes fpa : fpas) {
            f.addFixedPartitionAttributes(fpa);
          }
        }
      }
      if (this.getLocalMaxMemory() != null) {
        f.setLocalMaxMemory(this.getLocalMaxMemory().intValue());
      } // else use default computed by product
      f.setRecoveryDelay(this.getRecoveryDelay().longValue());
      f.setRedundantCopies(this.getRedundantCopies().intValue());
      f.setStartupRecoveryDelay(this.getStartupRecoveryDelay().longValue());
      f.setTotalMaxMemory(this.getTotalMaxMemory().longValue());
      f.setTotalNumBuckets(this.getTotalNumBuckets().intValue());
      if (this.getPartitionListeners() != null) {
        for (PartitionListener listener : this.getPartitionListenerInstances()) {
          f.addPartitionListener(listener);
        }
      }
      if (this.getPartitionResolver() != null) {
        f.setPartitionResolver(this.getPartitionResolverInstance());
      }

      this.partitionAttributesFactory = f;
    }
    return this.partitionAttributesFactory;
  }

  /**
   * Returns the cached instance of partition attributes for this description.
   * Instantiates and caches the partition listeners and the partition resolver,
   * if any.  Makes the default region name available to the fixed partition
   * mapping algorithm, if any.
   */
  public synchronized PartitionAttributes getPartitionAttributes() {
    return getPartitionAttributes(RegionPrms.DEFAULT_REGION_NAME);
  }

  /**
   * Returns the cached instance of partition attributes for this description.
   * Instantiates and caches the partition listeners and the partition resolver,
   * if any.  Makes the region name available to the fixed partition mapping
   * algorithm, if any.
   */
  protected synchronized PartitionAttributes
                         getPartitionAttributes(String regionName) {
    if (this.partitionAttributes == null) {
      this.partitionAttributes = this.getPartitionAttributesFactory(regionName)
                                     .create();
    }
    return this.partitionAttributes;
  }

  /**
   * Returns the partition attributes as a string.  For use only by {@link
   * RegionDescription#partitionAttributesToString(PartitionAttributes)}.
   */
  protected static synchronized String partitionAttributesToString(
                                       PartitionAttributes p) {
    if (p == null) {
      return "null";
    }
    StringBuffer buf = new StringBuffer();
    buf.append("\n    colocatedWith: " + p.getColocatedWith());
    buf.append("\n    fixedPartitionAttributes: " + p.getFixedPartitionAttributes());
    buf.append("\n    localMaxMemory: " + p.getLocalMaxMemory());
    buf.append("\n    partitionListeners: " + Arrays.toString(p.getPartitionListeners()));
    buf.append("\n    partitionResolver: " + p.getPartitionResolver());
    buf.append("\n    recoveryDelay: " + p.getRecoveryDelay());
    buf.append("\n    redundantCopies: " + p.getRedundantCopies());
    buf.append("\n    startupRecoveryDelay: " + p.getStartupRecoveryDelay());
    buf.append("\n    totalMaxMemory: " + p.getTotalMaxMemory());
    buf.append("\n    totalNumBuckets: " + p.getTotalNumBuckets());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "colocatedWith", this.getColocatedWith());
    map.put(header + "fixedPartitionName", this.getFixedPartitionDescriptionName());
    if (this.localMaxMemory == null) {
      map.put(header + "localMaxMemory", "computed");
    } else {
      map.put(header + "localMaxMemory", this.getLocalMaxMemory());
    }
    map.put(header + "partitionListeners", this.getPartitionListeners());
    map.put(header + "partitionResolver", this.getPartitionResolver());
    map.put(header + "recoveryDelay", this.getRecoveryDelay());
    map.put(header + "redundantCopies", this.getRedundantCopies());
    map.put(header + "startupRecoveryDelay", this.getStartupRecoveryDelay());
    map.put(header + "totalMaxMemory", this.getTotalMaxMemory());
    map.put(header + "totalNumBuckets", this.getTotalNumBuckets());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates partition descriptions from the partition parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each partition name
    Vector names = tab.vecAt(PartitionPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create partition description from test configuration parameters
      // and do hydra-level validation
      PartitionDescription pd = createPartitionDescription(name, config, i);

      // create partition attributes from non-default partition description
      // fields and do product-level validation
      PartitionAttributes pa = createPartitionAttributes(pd);

      // reset partition description based on partition attributes to pick up
      // product defaults and side-effects
      resetPartitionDescription(pd, pa);

      // save configuration
      config.addPartitionDescription(pd);
    }
  }

  /**
   * Creates the initial partition description using test configuration
   * parameters and does hydra-level validation.
   */
  private static PartitionDescription createPartitionDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    PartitionDescription pd = new PartitionDescription();
    pd.setName(name);

    // colocatedWith
    {
      Long key = PartitionPrms.colocatedWith;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        pd.setColocatedWith(str);
      }
    }
    // fixedPartitionName (generates fixedPartitionDescription)
    {
      Long key = PartitionPrms.fixedPartitionName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        pd.setFixedPartitionDescription(
           getFixedPartitionDescription(str, key, config));
        pd.setFixedPartitionDescriptionName("FixedPartitionDescription." + str);
      }
    }
    // localMaxMemory
    {
      Long key = PartitionPrms.localMaxMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        pd.setLocalMaxMemory(i);
      }
    }
    // partitionListeners
    {
      Long key = PartitionPrms.partitionListeners;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          pd.setPartitionListeners(new ArrayList(strs));
        }
      }
    }
    // partitionResolver
    {
      Long key = PartitionPrms.partitionResolver;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        pd.setPartitionResolver(str);
      }
    }
    // recoveryDelay
    {
      Long key = PartitionPrms.recoveryDelay;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l != null) {
        pd.setRecoveryDelay(l);
      }
    }
    // redundantCopies
    {
      Long key = PartitionPrms.redundantCopies;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        pd.setRedundantCopies(i);
      }
    }
    // startupRecoveryDelay
    {
      Long key = PartitionPrms.startupRecoveryDelay;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l != null) {
        pd.setStartupRecoveryDelay(l);
      }
    }
    // totalMaxMemory
    {
      Long key = PartitionPrms.totalMaxMemory;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l != null) {
        pd.setTotalMaxMemory(l);
      }
    }
    // totalNumBuckets
    {
      Long key = PartitionPrms.totalNumBuckets;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        pd.setTotalNumBuckets(i);
      }
    }
    return pd;
  }

  /**
   * Creates partition attributes from the partition description, setting only
   * those that are not null.  Picks up product defaults and side-effects, and
   * does product-level validation.
   */
  private static PartitionAttributes createPartitionAttributes(
                                     PartitionDescription pd) {
    PartitionAttributesFactory f = new PartitionAttributesFactory();
    // defer fixedPartitionAttributes
    if (pd.localMaxMemory != null) {
      f.setLocalMaxMemory(pd.getLocalMaxMemory().intValue());
    }
    if (pd.recoveryDelay != null) {
      f.setRecoveryDelay(pd.getRecoveryDelay().longValue());
    }
    if (pd.redundantCopies != null) {
      f.setRedundantCopies(pd.getRedundantCopies().intValue());
    }
    if (pd.startupRecoveryDelay != null) {
      f.setStartupRecoveryDelay(pd.getStartupRecoveryDelay().longValue());
    }
    if (pd.totalMaxMemory != null) {
      f.setTotalMaxMemory(pd.getTotalMaxMemory().longValue());
    }
    if (pd.totalNumBuckets != null) {
      f.setTotalNumBuckets(pd.getTotalNumBuckets().intValue());
    }
    return f.create();
  }

  /**
   * Resets the partition description based on partition attributes, to pick up
   * product defaults and side-effects.
   */
  private static void resetPartitionDescription(PartitionDescription pd,
                                                PartitionAttributes pa) {
    if (pd.getLocalMaxMemory() != null) {
      pd.setLocalMaxMemory(new Integer(pa.getLocalMaxMemory()));
    } // else deferred
    pd.setRecoveryDelay(new Long(pa.getRecoveryDelay()));
    pd.setRedundantCopies(new Integer(pa.getRedundantCopies()));
    pd.setStartupRecoveryDelay(new Long(pa.getStartupRecoveryDelay()));
    pd.setTotalMaxMemory(new Long(pa.getTotalMaxMemory()));
    pd.setTotalNumBuckets(new Integer(pa.getTotalNumBuckets()));
  }

//------------------------------------------------------------------------------
// Partition listener configuration support
//------------------------------------------------------------------------------

  /**
   * Creates the partition listener instances for this description.
   * @throws HydraConfigException if an instantiation fails or a class does not
   *                              implement PartitionListener.
   */
  private List<PartitionListener> createPartitionListenerInstances() {
    List l = new ArrayList();
    for (String classname : this.getPartitionListeners()) {
      l.add(createPartitionListenerInstance(classname));
    }
    return l;
  }

  /**
   * Creates a partition listener instance for the given classname.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PartitionListener.
   */
  private PartitionListener createPartitionListenerInstance(String classname) {
    Long key = PartitionPrms.partitionListeners;
    Object obj = getInstance(key, classname);
    try {
      Log.getLogWriter().info("Instantiated  " + obj);
      return (PartitionListener)obj;
    }
    catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement PartitionListener: " + classname;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Fixed partition configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the fixed partition description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         FixedPartitionPrms#names}.
   */
  private static FixedPartitionDescription getFixedPartitionDescription(
                                      String str, Long key, TestConfig config) {
    FixedPartitionDescription fpd = config.getFixedPartitionDescription(str);
    if (fpd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(FixedPartitionPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return fpd;
    }
  }

//------------------------------------------------------------------------------
// Partition resolver configuration support
//------------------------------------------------------------------------------

  /**
   * Creates a partition resolver instance for this description.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PartitionResolver.
   */
  private PartitionResolver createPartitionResolverInstance() {
    Long key = PartitionPrms.partitionResolver;
    String classname = this.getPartitionResolver();
    Object obj = getInstance(key, classname);
    try {
      Log.getLogWriter().info("Instantiated  " + obj);
      return (PartitionResolver)obj;
    }
    catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
        + " does not implement PartitionResolver: " + classname;
      throw new HydraConfigException(s);
    }
  }
}
