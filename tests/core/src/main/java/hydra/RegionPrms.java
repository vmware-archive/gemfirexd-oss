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

/**
 * A class used to store keys for region configuration settings.
 * The settings are used to create instances of {@link RegionDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values, fields, and subfields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values, fields, and subfields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 * <p>
 * Subfields are order-dependent, as stated in the javadocs for parameters that
 * use them.
 */
public class RegionPrms extends BasePrms {

  public static final String DEFAULT_REGION_NAME = "DefaultRegion";

  /**
   * (String(s))
   * Logical names of the region descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (Comma-separated lists of String(s))
   * Names of logical async event queue configurations (and actual ids) for
   * each region, as found in {@link AsyncEventQueuePrms#names}. Can be
   * specified as {@link #NONE} (default).
   *
   * @see AsyncEventQueuePrms
   */
  public static Long asyncEventQueueNames;

  /**
   * (Comma-separated String(s))
   * Class names of cache listeners for each region.  Can be specified as
   * {@link #NONE}.  See {@link #cacheListenersSingleton} for control over
   * instance creation.
   * <p>
   * Example: To use ClassA and ClassB for the first region, no listeners for
   * the second region, and ClassC for the third region, specify:
   *     <code>ClassA ClassB, none, ClassC</code>
   */
  public static Long cacheListeners;

  /**
   * (boolean(s))
   * Whether the {@link #cacheListeners} for each region are singletons across
   * all actual regions created with this description.  Defaults to false.
   */
  public static Long cacheListenersSingleton;

  /**
   * (String(s))
   * Cache loader for each region, consisting of an application classname.
   * Can be specified as {@link #NONE} (default).  See
   * {@link #cacheLoaderSingleton} for control over instance creation.
   * <p>
   * Example: To use an application cache loader in the first region, no loader
   * in the second, and another loader in the third, the value might look like:
   *
   *     <code>mypackage.MyRdbLoader, none, myotherpackage.MyOtherLoader</code>
   */
  public static Long cacheLoader;

  /**
   * (boolean(s))
   * Whether the {@link #cacheLoader} for each region is a singleton across
   * all actual regions created with this description.  Defaults to false.
   */
  public static Long cacheLoaderSingleton;

  /**
   * (String(s))
   * Cache writer for each region, consisting of an application classname.
   * Can be specified as {@link #NONE} (default).  See
   * {@link #cacheWriterSingleton} for control over instance creation.
   * <p>
   * Example: To use an application cache writer in the first region, no writer
   * in the second, and another writer in the third, the value might look like:
   *
   *     <code>mypackage.MyRdbWriter, none, myotherpackage.MyOtherWriter</code>
   */
  public static Long cacheWriter;

  /**
   * (boolean(s))
   * Whether the {@link #cacheWriter} for each region is a singleton across
   * all actual regions created with this description.  Defaults to false.
   */
  public static Long cacheWriterSingleton;

  /**
   * (boolean(s))
   * Cloning enabled for delta propagation on each region.
   */
  public static Long cloningEnabled;

  /**
   * (String(s))
   * Compressor for each region, consisting of an application classname.
   * Can be specified as {@link #NONE} (default).  See
   * {@link #compressorSingleton} for control over instance creation.
   * <p>
   * Example: To use an application compressor in the first region, none
   * in the second, and another in the third, the value might look like:
   *
   *     <code>mypackage.MyCompressor, none, myotherpackage.MyOtherCompressor</code>
   */
  public static Long compressor;

  /**
   * (boolean(s))
   * Whether the {@link #compressor} for each region is a singleton across
   * all actual regions created with this description.  Defaults to false.
   */
  public static Long compressorSingleton;

  /**
   * (boolean(s))
   * concurrencyChecksEnabled for each region.
   */
  public static Long concurrencyChecksEnabled;

  /**
   * (int(s))
   * Concurrency level for each region.
   */
  public static Long concurrencyLevel;

  /**
   * (String(s))
   * Classname of the custom entry idle timeout for each region.  Must
   * implement {@link com.gemstone.gemfire.cache.CustomExpiry}.  Can be
   * specified as {@link #NONE} (default).
   */
  public static Long customEntryIdleTimeout;

  /**
   * (String(s))
   * Classname of the custom entry time to live for each region.  Must
   * implement {@link com.gemstone.gemfire.cache.CustomExpiry}.  Can be
   * specified as {@link #NONE} (default).
   */
  public static Long customEntryTimeToLive;

  /**
   * (String(s))
   * Data policy for each region.  If the data policy partitions the region,
   * then {@link #partitionName} must also be set.  If the data policy causes
   * persistence, {@link #diskStoreName} must also be set.
   */
  public static Long dataPolicy;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * for each region, as found in {@link DiskStorePrms#names}.  Required when
   * using persistence.  Can be specified as {@link #NONE} (default).
   */
  public static Long diskStoreName;

  /**
   * (Boolean(s))
   * Disk synchronous for each region.  Meaningful only when {@link
   * #diskStoreName} is set.
   */
  public static Long diskSynchronous;

  /**
   * (boolean(s))
   * Enable async conflation for each region.
   */
  public static Long enableAsyncConflation;

  /**
   * (boolean(s))
   * Enable gateway for each region.
   */
  public static Long enableGateway;

  /**
   * (boolean(s))
   * Enable off-heap memory for each region.
   */
  public static Long enableOffHeapMemory;

  /**
   * (boolean(s))
   * Enable subscription conflation for each region.
   */
  public static Long enableSubscriptionConflation;

  /**
   * (Comma-separated int/String pair(s))
   * Entry idle timeout consisting for each region of the timeout in seconds
   * followed by an optional expiration action.  The timeout can be specified
   * as {@link #NONE} (no expiration).
   * <p>
   * Example: To use 30 seconds with destroy in the first region, the defaults
   * in the second region, 60 seconds with invalidate in the third region, and
   * no expiration in the fourth region, specify:
   *    <code>30 destroy, default, 60 invalidate, none</code>
   */
  public static Long entryIdleTimeout;

  /**
   * (Comma-separated int/String pair(s))
   * Entry time to live consisting for each region of the time in seconds
   * followed by an optional expiration action.  The time can be specified as
   * {@link #NONE} (no expiration).
   * <p>
   * See {@link #entryIdleTimeout} for an example.
   */
  public static Long entryTimeToLive;

  /**
   * (Comma-separated tuple(s))
   * Eviction attributes consisting for each region of the algorithm followed by
   * an ordered set of optional fields that depend on the algorithm.  The
   * algorithm can be specified as {@link #NONE} (no eviction), as can the
   * object sizer.
   * <p>
   * If the eviction algorithm is "lruHeapPercentage", also set {@link
   * ResourceManagerPrms#evictionHeapPercentage} as needed.
   * <p>
   * If the eviction action requires overflow to disk, {@link #diskStoreName}
   * must also be set.
   * <p>
   * Valid algorithms and optional fields:
   * <p>
   *   "none"
   * <p>
   *   "lruEntryCount"     maximumEntries(int)
   *                       evictionAction(String)
   * <p>
   *   "lruHeapPercentage" objectSizer(String), must be Serializable
   *                       evictionAction(String)
   * <p>
   *   "lruMemorySize"     maximumMegabytes(int)
   *                       objectSizer(String), must be Serializable
   *                       evictionAction(String)
   * <p>
   * Example: To use entry count eviction on the first region, heap eviction
   * on the second, memory size eviction on the third, and no eviction on the
   * fourth, the value might look something like this:
   *    <code>
   *          lruEntryCount default overflowToDisk,
   *          lruHeapPercentage default overflowToDisk,
   *          lruMemorySize default mytests.MyClass,
   *          none
   *    </code>
   */
  public static Long evictionAttributes;

  /**
   * (Comma-separated lists of String(s))
   * Names of logical gateway sender configurations for each region, as found
   * in {@link GatewaySenderPrms#names}.  Can be specified as {@link #NONE}
   * (default).
   *
   * @see GatewaySenderPrms
   */
  public static Long gatewaySenderNames;

  /**
   * (String(s))
   * HDFS store name for each region.
   */
  public static Long hdfsStoreName;

  /**
   * (boolean(s))
   * HDFS write-only for each region.
   */
  public static Long hdfsWriteOnly;

  /**
   * (boolean(s))
   * Ignore JTA for each region.
   */
  public static Long ignoreJTA;

  /**
   * (boolean(s))
   * Index maintenance synchronous for each region.
   */
  public static Long indexMaintenanceSynchronous;

  /**
   * (int(s))
   * Initial capacity for each region.
   */
  public static Long initialCapacity;

  /**
   * (String(s))
   * Interest policy for each region.  This is transparently converted to
   * subscription attributes.
   */
  public static Long interestPolicy;

  /**
   * (String(s))
   * Class name of key constraint for each region.  Can be specified as
   * {@link #NONE}.
   */
  public static Long keyConstraint;

  /**
   * (float(s))
   * Load factor for each region.
   */
  public static Long loadFactor;

  /**
   * (boolean(s))
   * Lock grantor for each region.
   */
  public static Long lockGrantor;

  /**
   * (Comma-separated String/String/String triplet(s))
   * Membership attributes consisting for each region of required roles,
   * followed by an optional loss action and an optional resumption action.
   * To specify multiple roles, use a colon (:) between them.  The required
   * roles and the resumption action can be specified as {@link #NONE}.
   * <p>
   * Example: To use roles "Buyer" and "Seller" on the first region, no roles
   * on the second, and role "Taxer" on the third, the value might look
   * something like this:
   *    <code>
   *          Buyer:Seller default reinitialize, none, Taxer reconnect
   *    </code>
   */
  public static Long membershipAttributes;

  /**
   * (boolean(s))
   * Multicast enabled for each region.  The default is to inherit the multicast
   * setting for the distributed system in which the region is created, as set
   * in {@link GemFirePrms#enableMcast}.
   */
  public static Long multicastEnabled;

  /**
   * (String(s))
   * Name of logical partition configuration for each region, as found in
   * {@link PartitionPrms#names}.  Can be specified as {@link #NONE} (default).
   * If the {@link #dataPolicy} partitions the region, this must be set to a
   * non-default value.
   */
  public static Long partitionName;

  /**
   * (String(s))
   * Name of logical pool configuration (and actual pool name) for each region,
   * as found in {@link PoolPrms#names}.  Can be specified as {@link #NONE}
   * (default).
   */
  public static Long poolName;

  /**
   * (Comma-separated int/String pair(s))
   * Region idle timeout consisting for each region of the timeout in seconds
   * followed by an optional expiration action.  The timeout can be specified
   * as {@link #NONE} (no expiration).
   * <p>
   * See {@link #entryIdleTimeout} for an example.
   */
  public static Long regionIdleTimeout;

  /**
   * (String(s))
   * Actual name for each region.  Defaults to {@link #DEFAULT_REGION_NAME}.
   */
  public static Long regionName;

  /**
   * (Comma-separated int/String pair(s))
   * Region time to live consisting for each region of the time in seconds
   * followed by an optional expiration action.  The time can be specified as
   * {@link #NONE} (no expiration).
   * <p>
   * See {@link #entryIdleTimeout} for an example.
   */
  public static Long regionTimeToLive;

  /**
   * (String(s))
   * Scope for each region.
   */
  public static Long scope;

  /**
   * (boolean(s))
   * Statistics enabled for each region.
   */
  public static Long statisticsEnabled;

  /**
   * (String(s))
   * Class name of value constraint for each region.  Can be specified as
   * {@link #NONE}.
   */
  public static Long valueConstraint;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(RegionPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("regionprms", "info");
    dumpKeys();
  }
}
