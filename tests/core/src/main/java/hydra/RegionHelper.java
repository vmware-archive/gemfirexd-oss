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

import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;

/**
 * Helps clients use {@link RegionDescription}.  Methods are thread-safe.
 */
public class RegionHelper {

  private static final String NO_EVICTION = "algorithm=none";
  private static final String DEFAULT_EVICTION_FOR_HDFS_PREFIX = "algorithm=lru-heap-percentage; action=overflow-to-disk; sizer=com.gemstone.gemfire.internal.size.SizeClassOnceObjectSizer";

//------------------------------------------------------------------------------
// Region
//------------------------------------------------------------------------------

  /**
   * Creates the region using the given region configuration in the current
   * cache.  The region is configured using the {@link RegionDescription}
   * corresponding to the region configuration from {@link RegionPrms#names}.
   * The region name is the corresponding value of {@link
   * RegionPrms#regionName}.
   * <p>
   * Lazily creates the disk store specified by {@link RegionPrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)}.
   * <p>
   * Lazily creates the HDFS store specified by {@link RegionPrms
   * #hdfsStoreName}, using {@link HDFSStoreHelper.createHDFSStore(String)}.
   * <p>
   * Lazily creates the pool specified by {@link RegionPrms#poolName}, using
   * {@link PoolHelper.createPool(String)}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing region.
   */
  public static synchronized Region createRegion(String regionConfig) {
    String regionName = getRegionDescription(regionConfig).getRegionName();
    return createRegion(regionName, regionConfig);
  }

  /**
   * Creates the region with the given name in the current cache.  The region
   * is configured using the {@link RegionDescription} corresponding to the
   * given region configuration from {@link RegionPrms#names}.
   * <p>
   * Lazily creates the disk store specified by {@link RegionPrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)}.
   * <p>
   * Lazily creates the HDFS store specified by {@link RegionPrms
   * #hdfsStoreName}, using {@link HDFSStoreHelper.createHDFSStore(String)}.
   * <p>
   * Lazily creates the pool specified by {@link RegionPrms#poolName}, using
   * {@link PoolHelper.createPool(String)}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing region.
   */
  public static synchronized Region createRegion(String regionName,
                                                 String regionConfig) {
    AttributesFactory factory = getAttributesFactory(regionName, regionConfig);
    return createRegion(regionName, factory);
  }

  /**
   * Creates the region with the given name in the current cache.  The region
   * is configured using the given factory.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing region.
   */
  public static synchronized Region createRegion(String regionName,
                                                 AttributesFactory factory) {
    RegionAttributes attributes = getRegionAttributes(factory);
    return createRegion(regionName, attributes);
  }

  /**
   * Creates the region with the given name in the current cache.  The region is
   * configured using the given region attributes.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing region.
   */
  public static synchronized Region createRegion(String regionName,
                                                 RegionAttributes attributes) {
    if (regionName == null) {
      throw new IllegalArgumentException("regionName cannot be null");
    }
    if (attributes == null) {
      throw new IllegalArgumentException("attributes cannot be null");
    }
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      log("Creating region named: " + regionName + " with attributes "
         + regionAttributesToString(attributes));
      String diskStoreConfig = attributes.getDiskStoreName();
      if (diskStoreConfig != null) {
        DiskStoreHelper.createDiskStore(diskStoreConfig);
      }
      String hdfsStoreConfig = attributes.getHDFSStoreName();
      if (hdfsStoreConfig != null) {
        HDFSStoreHelper.createHDFSStore(hdfsStoreConfig);
      }
      String poolConfig = attributes.getPoolName();
      if (poolConfig != null) {
        PoolHelper.createPool(poolConfig);
      }
      try {
        region = cache.createRegion(regionName, attributes);
      } catch (RegionExistsException e) {
        throw new HydraInternalException("Should not happen", e);
      }
      log("Created region named: " + regionName + " with attributes:"
         + regionAttributesToString(region.getAttributes()));
    } else {
      if (attributes.getPartitionAttributes() != null) {
        try {
          ((PartitionAttributesImpl) attributes.getPartitionAttributes()).computeLocalMaxMemory();
        } catch (NoSuchMethodError e) {
          log("Skipping com.gemstone.gemfire.internal.cache.PartitionAttributesImpl.computeLocalMaxMemory() for GemFireXD 1.0 since the method does not exist there");
        }
      }
      
      String results = compareRegionAttributes(regionName, attributes,
                                               region.getAttributes());
      if (results != null) {
        // block attempts to create region with clashing attributes
        String s = "Region " + regionName
          + " already exists with different attributes: " + results;
        throw new HydraRuntimeException(s);
      } // else it was already created with this configuration, which is fine
    }
    return region;
  }

  /**
   * Returns the region with the given name in the current cache, or null if
   * no region with that name exists.
   */
  public static synchronized Region getRegion(String regionName) {
    if (regionName == null) {
      throw new IllegalArgumentException("regionName cannot be null");
    }
    return CacheFactory.getAnyInstance().getRegion(regionName);
  }

//------------------------------------------------------------------------------
// RegionAttributes
//------------------------------------------------------------------------------

  /**
   * Returns a region attributes clone for the given region name and
   * configuration from {@link RegionPrms#names}, using a factory configured
   * using the corresponding {@link RegionDescription}.
   * <p>
   * Makes the specified region name available to the fixed partition mapping
   * algorithm, if any.
   */
  public static RegionAttributes getRegionAttributes(String regionName,
                                                     String regionConfig) {
    log("Configuring region attributes for region named " + regionName
                 + " with config: " + regionConfig);
    RegionAttributes ratts =
                     getAttributesFactory(regionName, regionConfig).create();
    log("Created region attributes: " + regionAttributesToString(ratts));
    return ratts;
  }

  /**
   * Returns a region attributes clone for the given region configuration
   * from {@link RegionPrms#names}, using a factory configured using the
   * corresponding {@link RegionDescription}.
   * <p>
   * Makes the configured region name available to the fixed partition mapping
   * algorithm, if any.
   */
  public static RegionAttributes getRegionAttributes(String regionConfig) {
    log("Creating region attributes for config: " + regionConfig);
    RegionAttributes ratts = getAttributesFactory(regionConfig).create();
    log("Created region attributes: " + regionAttributesToString(ratts));
    return ratts;
  }

  /**
   * Returns region attributes created using the given attributes factory.
   */
  public static RegionAttributes getRegionAttributes(
                                          AttributesFactory factory) {
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    return factory.create();
  }

  /**
   * Returns the given region attributes as a string.
   */
  public static String regionAttributesToString(RegionAttributes attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("attributes cannot be null");
    }
    return RegionDescription.regionAttributesToString(attributes);
  }

  /**
   * Compares two region attributes, accounting for side effects introduced by
   * the product for regions using HDFS. For use only by {@link RegionHelper}.
   *
   * @param regionName the name of the region being created
   * @param desired attributes being used to create the region
   * @param existing attributes for an existing region of the same name
   *                 that might contain hdfs side effects
   *
   * @return null if they are equal, else a string describing the problem
   */
  private static synchronized String compareRegionAttributes(
    String regionName, RegionAttributes datts, RegionAttributes eatts) {

    String desired = regionAttributesToString(datts);
    String existing = regionAttributesToString(eatts);

    if (desired.equals(existing)) {
      return null; // we're all done here
    }

    if (!datts.getDataPolicy().withHDFS() || !eatts.getDataPolicy().withHDFS()){
      // we are not expecting any side effects without HDFS involvement
      return "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
    }

    // both are using HDFS, so check for side effects in existing attributes

    // check async event queue ids
    Set<String> daeqids = new TreeSet(datts.getAsyncEventQueueIds());
    Set<String> eaeqids = new TreeSet(eatts.getAsyncEventQueueIds());
    if (!daeqids.equals(eaeqids)) {
      Set<String> removed = null;
      // remove the product-generated async event queues
      for (String eaeqid : eaeqids) {
        if (eaeqid.startsWith(HDFSStoreFactoryImpl.DEFAULT_ASYNC_QUEUE_ID_FOR_HDFS)) {
          eaeqids.remove(eaeqid);
          if (removed == null) {
            removed = new TreeSet();
          }
          removed.add(eaeqid);
        }
      }
      if (!daeqids.equals(eaeqids)) {
        String err = "asyncEventQueueIds do not match";
        if (removed != null) {
          err += " in spite of accounting for HDFS side effects: "
               + removed;
        }
        return err + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
      }
    }

    // check disk
    String dstore = datts.getDiskStoreName();
    String estore = eatts.getDiskStoreName();
    if (dstore != null && !dstore.equals(estore)) {
      String err = "disk store names do not match in spite of accounting "
                 + "for HDFS side effects";
      return err + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
    }

    // check eviction attributes
    EvictionAttributes devict = datts.getEvictionAttributes();
    EvictionAttributes eevict = eatts.getEvictionAttributes();
    if (!devict.equals(eevict)) {
      if (!devict.toString().trim().equals(NO_EVICTION) ||
          !eevict.toString().trim().startsWith(DEFAULT_EVICTION_FOR_HDFS_PREFIX)) {
        String err = "evictionAttributes do not match in spite of accounting "
                   + "for HDFS side effects: "
                   + DEFAULT_EVICTION_FOR_HDFS_PREFIX;
        return err + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
      }
    }

    // check everything else
    String dpart = RegionDescription.regionAttributesToStringPartial(datts);
    String epart = RegionDescription.regionAttributesToStringPartial(eatts);
    if (!dpart.equals(epart)) {
      // we are not expecting side effects from HDFS in any other attributes
      return "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
    }

    return null;
  }

//------------------------------------------------------------------------------
// AttributesFactory
//------------------------------------------------------------------------------

  /**
   * Returns an attributes factory for the given region name and configuration
   * from {@link RegionPrms#names}, configured using the corresponding
   * {@link RegionDescription}.
   * <p>
   * Makes the specified region name available to the fixed partition mapping
   * algorithm, if any.
   */
  public static AttributesFactory getAttributesFactory(String regionName,
                                                       String regionConfig) {
    // look up the region configuration
    RegionDescription rd = getRegionDescription(regionConfig);

    // create the attributes factory
    AttributesFactory factory = new AttributesFactory();

    // configure the attributes factory
    log("Configuring attributes factory for region named " + regionName
                 + " with config: " + regionConfig);
    rd.configure(regionName, factory, true);
    log("Configured attributes factory: " + factory);

    // return the result
    return factory;
  }

  /**
   * Returns an attributes factory for the given region configuration from
   * {@link RegionPrms#names}, configured using the corresponding
   * {@link RegionDescription}.
   * <p>
   * Makes the configured region name available to the fixed partition mapping
   * algorithm, if any.
   */
  public static AttributesFactory getAttributesFactory(String regionConfig) {
    // look up the region configuration
    RegionDescription rd = getRegionDescription(regionConfig);

    // create the attributes factory
    AttributesFactory factory = new AttributesFactory();

    // configure the attributes factory
    log("Configuring attributes factory for region config: " + regionConfig);
    rd.configure(rd.getRegionName(), factory, true);
    log("Configured attributes factory: " + factory);

    // return the result
    return factory;
  }

//------------------------------------------------------------------------------
// RegionDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link RegionDescription} with the given configuration name
   * from {@link RegionPrms#names}.
   */
  public static RegionDescription getRegionDescription(String regionConfig) {
    if (regionConfig == null) {
      throw new IllegalArgumentException("regionConfig cannot be null");
    }
    log("Looking up region config: " + regionConfig);
    RegionDescription rd = TestConfig.getInstance()
                                     .getRegionDescription(regionConfig);
    if (rd == null) {
      String s = regionConfig + " not found in "
               + BasePrms.nameForKey(RegionPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up region config:\n" + rd);
    return rd;
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}
