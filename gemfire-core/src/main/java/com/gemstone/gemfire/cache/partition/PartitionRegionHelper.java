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
package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RecoveryLock;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility methods for handling partitioned Regions, for example
 * during execution of {@link Function Functions} on a Partitioned
 * Region.
 * 
 * <p>Example of a Function using utility methods:
 * <pre>
 *  public Serializable execute(FunctionContext context) {
 *     if (context instanceof RegionFunctionContext) {
 *         RegionFunctionContext rc = (RegionFunctionContext) context;
 *         if (PartitionRegionHelper.isPartitionedRegion(rc.getDataSet())) {
 *             Region efficientReader =
 *              PartitionRegionHelper.getLocalDataForContext(rc);
 *             efficientReader.get("someKey");
 *             // ...
 *         }
 *      }
 *  // ...
 * </pre>
 *
 * @author Mitch Thomas
 * @author Yogesh Mahajan
 * 
 * @since 6.0
 * @see FunctionService#onRegion(Region)
 */
public final class PartitionRegionHelper {
  private PartitionRegionHelper() {}

  /**
   * Given a partitioned Region, return a map of {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated Regions}.
   * Given a local data reference to a partitioned region, return a map of local
   * {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated Regions}.
   * If there are no colocated regions, return an empty map.
   * @param r a partitioned Region
   * @throws IllegalStateException if the Region is not a {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable map of {@linkplain Region#getFullPath() region name} to {@link Region}
   * @since 6.0
   */
  public static Map<String, Region<?, ?>> getColocatedRegions(final Region<?, ?> r) {
    Map ret;
    if (isPartitionedRegion(r)) {
      final PartitionedRegion pr = (PartitionedRegion)r;
      ret = ColocationHelper.getAllColocationRegions(pr);
      if (ret.isEmpty()) {
        ret = Collections.emptyMap();
      }
    } else if (r instanceof LocalDataSet) {
      LocalDataSet lds = (LocalDataSet)r;
      InternalRegionFunctionContext fc = lds.getFunctionContext();
      if (fc != null) {
        ret = ColocationHelper.getAllColocatedLocalDataSets(lds.getProxy(), fc);
        if (ret.isEmpty()) {
          ret = Collections.emptyMap();
        }
      } else {
        final PartitionedRegion pr = lds.getProxy();
        ret = ColocationHelper.getColocatedLocalDataSetsForBuckets(pr,
            lds.getBucketSet(), pr.getTXState());
      }
    }
    else {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION
              .toLocalizedString(r.getFullPath()));
    }
    return Collections.unmodifiableMap(ret);
  }
  
  /**
   * Test a Region to see if it is a partitioned Region
   * 
   * @param r
   * @return true if it is a partitioned Region
   * @since 6.0
   */
  public static boolean isPartitionedRegion(final Region<?,?> r) {
    if (r != null) {
      return (r instanceof PartitionedRegion);
    }
    throw new IllegalArgumentException(
        LocalizedStrings.PartitionRegionHelper_ARGUMENT_REGION_IS_NULL
            .toString());
  }

  /**
   * Test a Region to see if it is a partitioned Region
   * 
   * @param r
   * @throws IllegalStateException
   * @return PartitionedRegion if it is a partitioned Region
   * @since 6.0
   */
  private static PartitionedRegion isPartitionedCheck(final Region<?,?> r) {
    if (! isPartitionedRegion(r)) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION
              .toLocalizedString(r.getFullPath()));
    }
    return (PartitionedRegion) r;
  }

  /**
   * Gathers a set of details about all partitioned regions in the local Cache.
   * If there are no partitioned regions then an empty set will be returned.
   *
   * @param cache the cache which has the regions
   * @return set of details about all locally defined partitioned regions
   * @since 6.0
   */
  public static Set<PartitionRegionInfo> getPartitionRegionInfo(
      final Cache cache) {
    Set<PartitionRegionInfo> prDetailsSet = 
      new TreeSet<PartitionRegionInfo>();
    fillInPartitionedRegionInfo((GemFireCacheImpl) cache, prDetailsSet, false);
    return prDetailsSet;
  }

  /**
   * Gathers details about the specified partitioned region. Returns null if
   * the partitioned region is not locally defined.
   * 
   * @param region the region to get info about
   * @return details about the specified partitioned region
   * @since 6.0
   */
  public static PartitionRegionInfo getPartitionRegionInfo(
      final Region<?,?> region) {
    try {
      PartitionedRegion pr = isPartitionedCheck(region);
      GemFireCacheImpl cache =  (GemFireCacheImpl) region.getCache();
      return pr.getRedundancyProvider().buildPartitionedRegionInfo(
          false, cache.getResourceManager().getLoadProbe()); // may return null
    } 
    catch (ClassCastException e) {
      // not a PR so return null
    }
    return null;
  }
  
  private static void fillInPartitionedRegionInfo(GemFireCacheImpl cache, final Set prDetailsSet, 
                                              final boolean internal) {
    // TODO: optimize by fetching all PR details from each member at once
    Set<PartitionedRegion> prSet = cache.getPartitionedRegions();
    if (prSet.isEmpty()) {
      return;
    }
    for (Iterator<PartitionedRegion> iter = prSet.iterator(); iter.hasNext();) {
      PartitionedRegion pr = iter.next();
      PartitionRegionInfo prDetails = pr.getRedundancyProvider().
          buildPartitionedRegionInfo(internal, cache.getResourceManager().getLoadProbe());
      if (prDetails != null) {
        prDetailsSet.add(prDetails);
      }
    }
  }

  /**
   * Decide which partitions will host which buckets. Gemfire normally assigns
   * buckets to partitions as needed when data is added to a partitioned region.
   * This method provides way to assign all of the buckets without putting any
   * data in partition region. This method should not be called until all of the
   * partitions are running because it will divide the buckets between the
   * running partitions. If the buckets are already assigned this method will
   * have no effect.
   * 
   * This method will block until all buckets are assigned.
   * 
   * @param region
   *          The region which should have it's buckets assigned.
   * @throws IllegalStateException
   *          if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @since 6.0
   */
  public static void assignBucketsToPartitions(Region<?,?> region) {
    PartitionedRegion pr = isPartitionedCheck(region);
    RecoveryLock lock = null;
    try {
      lock = pr.getRecoveryLock();
      lock.lock();
      for(int i = 0; i < getNumberOfBuckets(pr); i++) {
        //This method will return quickly if the bucket already exists
        pr.createBucket(i, 0, null);
      }
    } finally {
      if(lock != null) {
        lock.unlock();
      }
    }
  }
  
  private static int getNumberOfBuckets(PartitionedRegion pr) {
    if (pr.isFixedPartitionedRegion()) {
      int numBuckets = 0;
      Set<FixedPartitionAttributesImpl> fpaSet = new HashSet<FixedPartitionAttributesImpl>(
          pr.getRegionAdvisor().adviseAllFixedPartitionAttributes());
      if (pr.getFixedPartitionAttributesImpl() != null) {
        fpaSet.addAll(pr.getFixedPartitionAttributesImpl());
      }
      for (FixedPartitionAttributesImpl fpa : fpaSet) {
        numBuckets = numBuckets + fpa.getNumBuckets();
      }
      return numBuckets;
    }
    return pr.getTotalNumberOfBuckets();
  }

  /**
   * Get the current primary owner for a key.  Upon return there is no guarantee that
   * primary owner remains the primary owner, or that the member is still alive.
   * <p>This method is not a substitute for {@link Region#containsKey(Object)}.</p>
   * @param r a PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException
   *         if the provided region is something other than a
   *        {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return the primary member for the key, possibly null if a primary is not yet determined
   * @since 6.0
   */
  public static <K,V> DistributedMember getPrimaryMemberForKey(final Region<K,V> r, final K key) {
    PartitionedRegion pr = isPartitionedCheck(r);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    return pr.getBucketPrimary(bucketId);
  }

  /**
   * Get all potential redundant owners for a key.  If the key exists in the Region,
   * upon return there is no guarantee that key has not been moved or that
   * the members are still alive.
   *
   * <p>This method is not a substitute for {@link Region#containsKey(Object)}.</p>
   * <p> This method is equivalent to:
   * <code>
   *  DistributedMember primary = getPrimaryMemberForKey(r, key);
   *  Set<? extends DistributedMember> allMembers = getAllMembersForKey(r, key);
   *  allMembers.remove(primary);
   * </code></p>
   * @param r a PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException
   *        if the provided region is something other than a
   *        {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable set of members minus the primary
   * @since 6.0
   */
  public static <K,V> Set<DistributedMember> getRedundantMembersForKey(final Region<K,V> r, final K key) {
    DistributedMember primary = getPrimaryMemberForKey(r, key);
    Set<? extends DistributedMember> owners = getAllForKey(r, key);
    if (primary != null) {
      owners.remove(primary);
    }
    return Collections.unmodifiableSet(owners);
  }

  /**
   * Get all potential owners for a key.  If the key exists in the Region, upon return
   * there is no guarantee that it has not moved nor does it guarantee all members are still
   * alive.
   * <p>This method is not a substitute for {@link Region#containsKey(Object)}.
   *
   * @param r PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException
   *         if the provided region is something other than a
   *        {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable set of all members
   * @since 6.0
   */
  public static <K,V> Set<DistributedMember> getAllMembersForKey(final Region<K,V> r, final K key) {
    return Collections.unmodifiableSet(getAllForKey(r, key));
  }
  private static <K,V> Set<? extends DistributedMember> getAllForKey(final Region<K,V> r, final K key) {
    PartitionedRegion pr = isPartitionedCheck(r);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    return pr.getRegionAdvisor().getBucketOwners(bucketId);
  }
  
  /**
   * Given a RegionFunctionContext {@linkplain RegionFunctionContext#getDataSet()
   * for a partitioned Region}, return a map of {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated Regions}
   * with read access limited to the context of the function.
   * <p>
   * Writes using these Region have no constraints and behave the same as a partitioned Region.
   * <p>
   * If there are no colocated regions, return an empty map.
   *
   * @param c the region function context
   * @throws IllegalStateException if the Region is not a {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable map of {@linkplain Region#getFullPath() region name} to {@link Region}
   * @since 6.0
   */
  public static Map<String, Region<?, ?>> getLocalColocatedRegions(final RegionFunctionContext c) {
    final Region r = c.getDataSet();
    isPartitionedCheck(r);
    final InternalRegionFunctionContext rfci = (InternalRegionFunctionContext)c;
    Map ret = rfci.getColocatedLocalDataSets();
    return ret;
  }

  /**
   * Given a RegionFunctionContext
   * {@linkplain RegionFunctionContext#getDataSet() for a partitioned Region},
   * return a Region providing read access limited to the function context.<br>
   * Returned Region provides only one copy of the data although
   * {@link PartitionAttributes#getRedundantCopies() redundantCopies} configured
   * is more than 0. If the invoking Function is configured to have
   * {@link Function#optimizeForWrite() optimizeForWrite} as true,the returned
   * Region will only contain primary copy of the data.
   * <p>
   * Writes using this Region have no constraints and behave the same as a
   * partitioned Region.
   * 
   * @param c
   *          a functions context
   * @throws IllegalStateException
   *           if {@link RegionFunctionContext#getDataSet()} returns something
   *           other than a {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since 6.0
   */
  public static <K, V> Region<K, V> getLocalDataForContext(final RegionFunctionContext c) {
    final Region r = c.getDataSet();
    isPartitionedCheck(r);
    InternalRegionFunctionContext rfci = (InternalRegionFunctionContext)c;
    return rfci.getLocalDataSet(r);
  }
  
 /**
   * Given a partitioned Region return a Region providing read access limited to
   * the local heap, writes using this Region have no constraints and behave the
   * same as a partitioned Region.<br>
   * 
   * @param r
   *                a partitioned region
   * @throws IllegalStateException
   *                 if the provided region is something other than a
   *                 {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since 6.0
   */
  @SuppressWarnings("unchecked")
  public static <K,V> Region<K,V> getLocalData(final Region<K,V> r) {
    if (isPartitionedRegion(r)) {
      PartitionedRegion pr = (PartitionedRegion)r;
      final Set<Integer> buckets;
      if (pr.getDataStore() != null) {
        buckets = pr.getDataStore().getAllLocalBucketIds();
      } else {
        buckets = Collections.emptySet();
      }
      return new LocalDataSet(pr, buckets, pr.getTXState());
    } else if (r instanceof LocalDataSet) {
      return r;
    } else {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION
              .toLocalizedString(r.getFullPath()));
    }
  }
  /**
   * Given a partitioned Region return a Region providing read access to primary
   * copy of the data which is limited to the local heap, writes using this
   * Region have no constraints and behave the same as a partitioned Region.<br>
   * 
   * @param r
   *          a partitioned region
   * @throws IllegalStateException
   *           if the provided region is something other than a
   *           {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since 6.5
   */
  @SuppressWarnings("unchecked")
  public static <K,V> Region<K,V> getLocalPrimaryData(final Region<K,V> r) {
    if (isPartitionedRegion(r)) {
      PartitionedRegion pr = (PartitionedRegion)r;
      final Set<Integer> buckets;
      if (pr.getDataStore() != null) {
        buckets = pr.getDataStore().getAllLocalPrimaryBucketIds();
      } else {
        buckets = Collections.emptySet();
      }
      return new LocalDataSet(pr, buckets, pr.getTXState());
    } else if (r instanceof LocalDataSet) {
      return r;
    } else {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION
              .toLocalizedString(r.getFullPath()));
    }
  }
}
