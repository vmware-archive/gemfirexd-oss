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
/**
 * 
 */
package management.test.cli;

import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.Log;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import parReg.ParRegUtil;
import recovDelay.PrState;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.management.ManagementService;

/**
 * @author lynng
 *
 */
public class CliHelperFunction extends FunctionAdapter {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#execute(com.gemstone.gemfire.cache.execute.FunctionContext)
   */
  @Override
  public void execute(FunctionContext context) {
    // retrieve and log function arguments
    String[] arguments = (String[])(context.getArguments());
    if (arguments.length < 2) {
      throw new TestException("Expected three arguments for function, 1) hydra logical thread id 2) regionFullPath");
    }
    String initiatingThreadID = arguments[0];
    String regionFullPath = (String)arguments[1];
    String loggingID = "Initiated in hydra thread thr_" + initiatingThreadID + "_; ";
    Log.getLogWriter().info("In execute with context " + context + " " + loggingID + "regionFullPath " + regionFullPath);

    // process optional argument
    Region aRegion = CacheHelper.getCache().getRegion(regionFullPath);
    if (aRegion ==null) {
      throw new TestException("Unable to get region " + regionFullPath);
    }
    
    // gather the information to return by filling in the ValidationInfo object
    ValidationInfo info = new ValidationInfo();
    RegionAttributes attr = aRegion.getAttributes();
    Log.getLogWriter().info("region full path: " + regionFullPath + ", region name: " + aRegion.getName());
    Log.getLogWriter().info(loggingID + "Attributes for " + regionFullPath + ": " + attr);
    info.prAttr = attr.getPartitionAttributes();
    Log.getLogWriter().info(loggingID + "PartitionAttributes for " + regionFullPath + ": " + info.prAttr);
    info.membershipAttr = attr.getMembershipAttributes();
    Log.getLogWriter().info(loggingID + "MembershipAttributes for " + regionFullPath + ": " + info.membershipAttr);
    info.evictionAttr = attr.getEvictionAttributes();
    Log.getLogWriter().info(loggingID + "EvictionAttributes for " + regionFullPath + ": " + info.evictionAttr);
 
    
    Cache theCache = CacheHelper.getCache();
    info.isManager = false;
    if (theCache != null) {
      ManagementService service = ManagementService.getExistingManagementService(theCache);
      info.isManager = service.isManager();
    }
    info.member = DistributedSystemHelper.getDistributedSystem().getDistributedMember().toString();

    info.regionFullPath = regionFullPath;
    info.regionSize = aRegion.size();
    info.dataPolicy = attr.getDataPolicy();
    info.withPersistence = info.dataPolicy.withPersistence();

    Region parentRegion = aRegion.getParentRegion();
    if (parentRegion == null) {
      info.parentRegionName = null;
    } else {
      info.parentRegionName = parentRegion.getName();
    }
    
    info.cacheListeners = Arrays.asList(attr.getCacheListeners());
    info.cacheLoader = attr.getCacheLoader();
    info.cacheWriter = attr.getCacheWriter();
    info.compressor = attr.getCompressor();
    info.concurrencyLevel = attr.getConcurrencyLevel();
    info.customEntryIdleTimeout = attr.getCustomEntryIdleTimeout();
    info.customEntryTimeToLive = attr.getCustomEntryTimeToLive();
    info.diskStoreName = attr.getDiskStoreName();
    info.enableOffHeapMemory = attr.getEnableOffHeapMemory();
    info.entryIdleTimeout = attr.getEntryIdleTimeout();
    info.entryTimeToLive = attr.getEntryTimeToLive();
    info.gatewayHubId = attr.getGatewayHubId();
    info.initialCapacity = attr.getInitialCapacity();
    info.interestPolicy = attr.getSubscriptionAttributes().getInterestPolicy();
    info.keyConstraint = attr.getKeyConstraint();
    info.loadFactor = attr.getLoadFactor();
    info.poolName = attr.getPoolName();
    info.regionIdleTimeout = attr.getRegionIdleTimeout();
    info.regionTimeToLive = attr.getRegionTimeToLive();
    info.scope = attr.getScope();
    info.valueConstraint = attr.getValueConstraint();
    info.asyncConflationEnabled = attr.getEnableAsyncConflation();
    info.cloningEnabled = attr.getCloningEnabled();
    info.diskSynchronous = attr.isDiskSynchronous();
    info.gatewayEnabled = attr.getEnableGateway();
    info.ignoreJTA = attr.getIgnoreJTA();
    info.indexMaintenanceSynchronous = attr.getIndexMaintenanceSynchronous();
    info.lockGrantor = attr.isLockGrantor();
    info.multicastEnabled = attr.getMulticastEnabled();
    info.publisher = attr.getPublisher();
    info.statisticsEnabled = attr.getStatisticsEnabled();
    info.subscriptionConflationEnabled = attr.getEnableSubscriptionConflation();
    info.hdfsStoreName = attr.getHDFSStoreName();
    info.hdfsWriteOnly = attr.getHDFSWriteOnly();
    info.directSubregions = new HashSet();
    Set<Region> aSet = aRegion.subregions(false);
    for (Region reg: aSet) {
      info.directSubregions.add(reg.getFullPath());
    }
    info.allSubregions = new HashSet();
    aSet = aRegion.subregions(true);
    for (Region reg: aSet) {
      info.allSubregions.add(reg.getFullPath());
    }

    // use test hooks as much as possible to return the data to validate against 
    // for added validation (so as to not just get that information from the
    // same place the MBeans are getting it from)
    info.primaryBucketCount = 0;
    info.localEntryCount = aRegion.size();
    if (info.dataPolicy.withPartitioning()) {
      Log.getLogWriter().info(loggingID + PrState.getPrPicture(aRegion));
      info.localEntryCount = 0;
      int numBuckets = info.prAttr.getTotalNumBuckets();
      for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
        Boolean[] tmp = ParRegUtil.getBucketStatus(aRegion, bucketId);
        if (tmp != null) {
          boolean bucketIsLocal = tmp[0];
          boolean bucketIsPrimary = tmp[1];
          if (bucketIsLocal && bucketIsPrimary) {
            info.primaryBucketCount++;
            try {
              List aList = ((PartitionedRegion)aRegion).getAllBucketEntries(bucketId); // each element is 1 bucket copy
              if ((aList != null) && (aList.size() > 0)) {
                BucketDump bucket = (BucketDump) aList.get(0);
                info.localEntryCount += bucket.getValues().size();
              }
            } catch (ForceReattemptException e) {
              throw new TestException(TestHelper.getStackTrace(e));
            } 
          }
        }
      }
    }else{
      info.primaryBucketCount = -1;
    }
    
    Log.getLogWriter().info(loggingID + "Returning " + info);
    context.getResultSender().lastResult(info);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#getId()
   */
  @Override
  public String getId() {
    return this.getClass().getName();
  }

}
