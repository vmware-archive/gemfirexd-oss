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
package rebalance;

import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.ParRegPrms;
import parReg.ParRegUtil;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;
import recovDelay.BucketState;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class RebalanceUtil {

private static GsRandom rand = TestConfig.tab().getRandGen();

public static void primariesBalanced() {
   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("primariesRebalanced() expects hydra client to have created cache and PR via initialization tasks");
   }

   // wait for redundancy recovery to complete 
   waitForRecovery();

   ResourceManager rm = myCache.getResourceManager();
   RebalanceFactory factory = rm.createRebalanceFactory();
   Set<PartitionRegionInfo> prdSet = PartitionRegionHelper.getPartitionRegionInfo(myCache);
   for (PartitionRegionInfo prd : prdSet) {
      Log.getLogWriter().info(RebalanceUtil.partitionedRegionDetailsToString(prd));
   }
   StringBuffer aStr = new StringBuffer();
   for (PartitionRegionInfo prd: prdSet) {
      String balanceDetails = primariesBalanced(prd);
      aStr.append("Primaries for region " + getRegionName(prd) + " are balanced \n" + balanceDetails);
   }
   Log.getLogWriter().info("Primaries are balanced \n" + aStr.toString());
}

// waitForRecovery (1 VM recycled at a time, multipleRegions allowed)
public static void waitForRecovery() {
   // This depends on the ResourceObserver being defined in the test
   if ((parReg.ParRegPrms.getResourceObserver() == null) && 
       (rebalance.RebalancePrms.getResourceObserver() == null)) {
      return;
   }

   // wait for all regions (see colocated region tests) to recover
   int numRegions = RebalancePrms.getNumRegions();
   Log.getLogWriter().info("Waiting for recovery of " + numRegions + " regions");

   // wait for recovery to complete in this VM 
   // ResourceObserver sets this value on the BB
   SharedCounters counters = RebalanceBB.getBB().getSharedCounters();

   // wait for finished status (for numRegions)
   long recoveryRegionCount = counters.read(RebalanceBB.recoveryRegionCount);
   do {
      MasterController.sleepForMs(5000);
      recoveryRegionCount = counters.read(RebalanceBB.recoveryRegionCount);
      Log.getLogWriter().info("recoveryRegionCount = " + recoveryRegionCount);
   } while (recoveryRegionCount < numRegions);
   Log.getLogWriter().info(recoveryRegionCount + " regions have been recovered");
}

protected static String primariesBalanced(PartitionRegionInfo prd) {
   String regionName = getRegionName(prd);
   StringBuffer aStr = new StringBuffer();
   aStr.append("For region named " + regionName + "\n");

   // calculate the average number of primaries
   Set<PartitionMemberInfo> pmdSet = prd.getPartitionMemberInfo();
   double primaries = 0;
   int bucketCount = 0;
   for (PartitionMemberInfo pmd : pmdSet) {
      primaries += pmd.getPrimaryCount();
      bucketCount = pmd.getBucketCount();
      aStr.append("   member (" + pmd.getDistributedMember() + "): number of primaries = " + pmd.getPrimaryCount() + "\n");
   }
   double average = primaries/pmdSet.size();

   // hold allowable difference within 20%  (of average)
   double allowableDifference = (average/10) * 2;
   for (PartitionMemberInfo pmd : pmdSet) {
      double difference = Math.abs(pmd.getPrimaryCount() - average);
      if (pmd.getPrimaryCount() == 0) {
         throw new TestException(aStr.toString() + "For region " + regionName + " and member " + pmd.getDistributedMember() + " primaryCount is 0!");
      }
      if (difference > allowableDifference) {
         throw new TestException(aStr.toString() + "For region " + regionName + " and member " + pmd.getDistributedMember() + " difference (" + difference + ") between the primaryCount (" + pmd.getPrimaryCount() + ") and the average number of primaries (" + average + ") is greater than expected (" + allowableDifference + ")");
      }
   }
   return aStr.toString();
}

/** Check that rebalancing improved the state of things.
 *  @param results The results of a rebalance
 *  @param stableSystem True if the system is stable, meaning it is not
 *         doing ops, cycling vms or allowing buckets to move. In this
 *         case this method will verify with test hooks that assume the
 *         buckets are stable. False if system is not stable and could be
 *         doing ops, cycling vms, or allowing buckets to move. In this
 *         case the method will not use test hooks for verification.
 */
public static void isBalanceImproved(RebalanceResults results, boolean stableSystem) {

   Set<PartitionRebalanceInfo> prdSet = results.getPartitionRebalanceDetails();
   // For each region involved in rebalance, check heapUtilization standard dev
   for (PartitionRebalanceInfo prd : prdSet) {
      String regionName = RebalanceUtil.getRegionName(prd);
      Set<PartitionMemberInfo> before = prd.getPartitionMemberDetailsBefore();
      Set<PartitionMemberInfo> after = prd.getPartitionMemberDetailsAfter();
      try {
         isBalanceImproved(regionName, before, after);
      } catch (TestException e) {
         throw new TestException(e.getMessage() + "\n\n" + RebalanceResultsToString(results, "rebalance"));
      }
   }
   checkRebalancingActivity(results);
   if (stableSystem) {
      checkResultsWithRegion(prdSet);
   }
}

/** Check that rebalancing improved the state of things.
 *  This method should only be called when the test is silent (no ops)
 *  as it compares the rebalance results to the current state of the
 *  existing PRs in this vm.
 */
public static void isBalanceImproved(RebalanceResults results) {
   isBalanceImproved(results, true);
}

public static void isBalanceImproved(String regionName, Set<PartitionMemberInfo> before, Set<PartitionMemberInfo> after) {

   // Calculate and check overall standard deviation
   double heapUtilBefore = getHeapUtilizationStdDev(before);
   double heapUtilAfter = getHeapUtilizationStdDev(after);
   if (heapUtilAfter > heapUtilBefore) {
      throw new TestException("For region " + regionName + " the standard deviation of HeapUtilization increased during rebalance from " + heapUtilBefore + " to " + heapUtilAfter);
   } 

   // check lowRedundancy & localMaxMemory for each member
   PartitionState psBefore = new PartitionState(before);
   PartitionState psAfter = new PartitionState(after);
   isBalanceImproved(regionName, psBefore, psAfter);
}

private static void isBalanceImproved(String regionName, PartitionState before, PartitionState after) {

   if (!after.isRedundancySatisfied()) {
      Log.getLogWriter().warning("For region " + regionName + " redundancy has not been satisfied\n");
   }
   if (!after.isLocalMaxMemoryHonored()) {
      Log.getLogWriter().warning("For region " + regionName + " localMaxMemory has not been honored\n");
   }
   return; 
}

public static boolean isLocalMaxMemoryHonored(Set<PartitionMemberInfo> pmd) {
   boolean honored = true;
   for (PartitionMemberInfo memberDetails : pmd) {
      if (memberDetails.getSize() > memberDetails.getConfiguredMaxMemory()) {
         honored = false;
         break;
      }
   }
   return honored;
}

public static boolean isRedundancySatisfied(PartitionRegionInfo prd) {
   boolean satisfied = true;
   if (prd.getActualRedundantCopies() < prd.getConfiguredRedundantCopies()) {
      satisfied = false;
   }
   if (prd.getLowRedundancyBucketCount() > 0) {
      satisfied = false;
   }
   return satisfied;
}

public static double getAverageHeapUtilization(Set<PartitionMemberInfo> pmd) {
   double heapUtilization = 0;
   if (pmd.isEmpty()) return 0;
   for (PartitionMemberInfo memberDetails : pmd) {
      long localMaxMemory = memberDetails.getConfiguredMaxMemory();
      long size = memberDetails.getSize();
      double inUse = (double)size / localMaxMemory;
      heapUtilization += (inUse * 100);
   }
   Log.getLogWriter().fine("Average heapUtilization = " + heapUtilization / pmd.size());
   return (heapUtilization/pmd.size());
}

public static double getAverageBucketCount(Set<PartitionMemberInfo> pmd) {
   int bucketCount = 0;
   if (pmd.isEmpty()) return 0;
   for (PartitionMemberInfo memberDetails : pmd) {
      bucketCount += memberDetails.getBucketCount();
   }
   Log.getLogWriter().fine("Average bucketCount = " + (double)bucketCount / pmd.size());
   return ((double)bucketCount/pmd.size());
}

public static double getAveragePrimaryCount(Set<PartitionMemberInfo> pmd) {
   int primaryCount = 0;
   if (pmd.isEmpty()) return 0;
   for (PartitionMemberInfo memberDetails : pmd) {
      primaryCount += memberDetails.getPrimaryCount();
   }
   Log.getLogWriter().fine("Average primaryCount = " + (double)primaryCount / pmd.size());
   return ((double)primaryCount/pmd.size());
}

/*
 * Compute the (unbiased) standard deviation for heapUtilization
 *   x = one value from data set
 *   avg (x) = the mean (average) of all values x in data set
 *   n = the number of values x in data set
 *   
 *   For each value x, subtract the overall avg (x) from x 
 *   Square that result.
 *   Sum the squared values. 
 *   Divide that result by (n-1). 
 *   Take the square root of the result
 */
public static double getHeapUtilizationStdDev(Set<PartitionMemberInfo> pmdSet) {
   StringBuffer aStr = new StringBuffer();
   double average = getAverageHeapUtilization(pmdSet);
   aStr.append("average = " + average + "\n");
   double total = 0;
   double dev; 
   for (PartitionMemberInfo pmd : pmdSet) {
      long localMaxMemory = pmd.getConfiguredMaxMemory();
      long size = pmd.getSize();
      double heapUtilization = ((double)size) / localMaxMemory;
      dev = (heapUtilization * 100) - average;
      total += Math.pow(dev, 2);
      aStr.append(" maxMem = " + localMaxMemory + ", size = " + size + ", heapUtil = " + (heapUtilization*100) + ", dev = " + dev + ", subTotal = " + total + "\n");
   }
   double variance = total / (pmdSet.size()-1);
   dev = Math.sqrt( variance );
   Log.getLogWriter().fine(aStr.toString() + "total = " + total + ", variance = " + variance + ", heapStdDev = " + dev);
   return dev;
}

/*
 * Compute the (unbiased) standard deviation for primaryCount
 *   x = one value from data set
 *   avg (x) = the mean (average) of all values x in data set
 *   n = the number of values x in data set
 *   
 *   For each value x, subtract the overall avg (x) from x 
 *   Square that result.
 *   Sum the squared values. 
 *   Divide that result by (n-1). 
 *   Take the square root of the result
 */
public static double getPrimaryCountStdDev(Set<PartitionMemberInfo> pmdSet) {
   StringBuffer aStr = new StringBuffer();
   double average = getAveragePrimaryCount(pmdSet);
   aStr.append("average = " + average + "\n");
   double total = 0;
   double dev;
   for (PartitionMemberInfo pmd : pmdSet) {
      long count = pmd.getPrimaryCount();
      dev = count - average;
      total += Math.pow(dev, 2);
      aStr.append("  count = " + count + ", dev = " + dev + ", subTotal = " + total + "\n");
   }
   double variance = total / (pmdSet.size()-1);
   dev = Math.sqrt( variance );
   Log.getLogWriter().fine(aStr.toString() + "total = " + total + ", variance = " + variance + ", primaryCountStdDev = " + dev);
   return dev;
}

//---------------------------------------------------------------------------
// test utility methods related to stats
//---------------------------------------------------------------------------

/** Return the number of PR entries in this VM
 *
 *  @return The number of CachePerfStat-partition-<regionName> entries from stats
 *
 */

public static void HydraTask_displayStats() {
   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("displayStats() expects hydra client to have created cache and PR via initialization tasks");
   }

   ResourceManager rm = myCache.getResourceManager();
   RebalanceFactory factory = rm.createRebalanceFactory();
   Set<PartitionRegionInfo> prdSet = PartitionRegionHelper.getPartitionRegionInfo(myCache);
   String regionName = null;
   for (PartitionRegionInfo prd : prdSet) {
      regionName = getRegionName(prd);
      long entries = getPRLocalSize(regionName);
      Log.getLogWriter().info("entries in PartitionedRegion(" + regionName + ") = " + entries);
   }
}

protected static long getPRLocalSize(String regionName) {
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("getPRLocalSize() expects hydra client to have created cache and PR via initialization tasks");
   }
   Region aRegion = myCache.getRegion(regionName);
   long entries = ((PartitionedRegion)aRegion).getLocalSize();
   return entries;
}

//---------------------------------------------------------------------
// PR validation methods from parReg/knownKeysTest, which don't rely 
// on static aRegion field.  Uses CacheHelper.getCache().rootRegions()
// to get regions to work on.
//---------------------------------------------------------------------

/** Log the local size of the PR data store
 */
public synchronized static void HydraTask_logLocalSize() {
   Cache myCache = CacheHelper.getCache();
   Set<Region<?,?>> rootRegions = myCache.rootRegions();
   for (Region aRegion : rootRegions) {
      if (aRegion instanceof PartitionedRegion) {
         Log.getLogWriter().info("Number of entries in this dataStore for region" + aRegion.getName() + " : " + ParRegUtil.getLocalSize(aRegion));
      }
   } 
}  

/** Hydra task to verify metadata
 *  versions of parReg/KnownKeysTest validation methods which verify
 *  all partitionedRegions in the VM.
 */
public static void HydraTask_verifyPRMetaData() {
   Cache myCache = CacheHelper.getCache();
   Set<Region<?,?>> rootRegions = myCache.rootRegions();
   for (Region aRegion : rootRegions) {
      if (aRegion instanceof PartitionedRegion) {
         try {
            ParRegUtil.verifyPRMetaData(aRegion);
         } catch (Exception e) {
           // shutdownHook will cause all members to dump partitioned region info
           throw new TestException(TestHelper.getStackTrace(e));
         }
      }
   }
}

/** Hydra task to verify primaries
 */
public static void HydraTask_verifyPrimaries() {
   Cache myCache = CacheHelper.getCache();
   Set<Region<?,?>> rootRegions = myCache.rootRegions();
   boolean highAvailability = TestConfig.tab().booleanAt(ParRegPrms.highAvailability, false);
   for (Region aRegion : rootRegions) {
      if (aRegion instanceof PartitionedRegion) {
         try {
            RegionAttributes attr = aRegion.getAttributes();
            PartitionAttributes prAttr = attr.getPartitionAttributes();
            int redundantCopies = 0;
            if (prAttr != null) { 
               redundantCopies = prAttr.getRedundantCopies();
            }
            if (highAvailability) { // with HA, wait for things to settle down
               ParRegUtil.verifyPrimariesWithWait(aRegion, redundantCopies);
            } else {
               ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
            }
         } catch (Exception e) {
            // shutdownHook will cause all members to dump partitioned region info
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
   }
}

// used for checking bucket copies in a batched test
protected static ParRegUtil parRegUtilInstance = new ParRegUtil();

/** Hydra task to verify bucket copies. This must be called repeatedly
 *  by the same thread until StopSchedulingTaskOnClientOrder is thrown.
 */
public static void HydraTask_verifyBucketCopiesBatched() {
   Cache myCache = CacheHelper.getCache();
   Set<Region<?,?>> rootRegions = myCache.rootRegions();
   for (Region aRegion : rootRegions) {
      if (aRegion instanceof PartitionedRegion) {
         try {
            RegionAttributes attr = aRegion.getAttributes();
            PartitionAttributes prAttr = attr.getPartitionAttributes();
            int redundantCopies = 0;
            if (prAttr != null) { 
               redundantCopies = prAttr.getRedundantCopies();
            }
            // the following call throws StopSchedulingTaskOnClientOrder when completed
            parRegUtilInstance.verifyBucketCopiesBatched(aRegion, redundantCopies);
         } catch (TestException e) {
            // shutdownHook will cause all members to dump partitioned region info
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
   }
}


//---------------------------------------------------------------------------
// test utility display methods 
//---------------------------------------------------------------------------

// note that title will typically indicate simulate or rebalance (to reflect the operation) used to create the RebalanceResults
public static String RebalanceResultsToString(RebalanceResults results, String title) {
   if (results == null) {
      return "null";
   }
   StringBuffer aStr = new StringBuffer();
   aStr.append("Rebalance results (" + title + ") totalTime: " + results.getTotalTime() + "\n");

   // bucketCreates
   aStr.append("totalBucketCreatesCompleted: " + results.getTotalBucketCreatesCompleted());
   aStr.append(" totalBucketCreateBytes: " + results.getTotalBucketCreateBytes());
   aStr.append(" totalBucketCreateTime: " + results.getTotalBucketCreateTime() + "\n");

   // bucketTransfers
   aStr.append("totalBucketTransfersCompleted: " + results.getTotalBucketTransfersCompleted());
   aStr.append(" totalBucketTransferBytes: " + results.getTotalBucketTransferBytes());
   aStr.append(" totalBucketTransferTime: " + results.getTotalBucketTransferTime() + "\n");

   // primaryTransfers
   aStr.append("totalPrimaryTransfersCompleted: " + results.getTotalPrimaryTransfersCompleted());
   aStr.append(" totalPrimaryTransferTime: " + results.getTotalPrimaryTransferTime() + "\n");

   // PartitionRebalanceDetails (per region)
   Set<PartitionRebalanceInfo> prdSet = results.getPartitionRebalanceDetails();
   for (PartitionRebalanceInfo prd: prdSet) {
      aStr.append(partitionRebalanceDetailsToString(prd));
   }
   aStr.append("total time (ms): " + results.getTotalTime());
   return aStr.toString();
}

public static String partitionRebalanceDetailsToString(PartitionRebalanceInfo details) {
   if (details == null) {
      return "null\n";
   } 

   StringBuffer aStr = new StringBuffer();
   aStr.append("PartitionedRegionDetails for region named " + getRegionName(details) + " time: " + details.getTime() + "\n");

   // bucketCreates
   aStr.append("bucketCreatesCompleted: " + details.getBucketCreatesCompleted());
   aStr.append(" bucketCreateBytes: " + details.getBucketCreateBytes());
   aStr.append(" bucketCreateTime: " + details.getBucketCreateTime() + "\n");

   // bucketTransfers
   aStr.append("bucketTransfersCompleted: " + details.getBucketTransfersCompleted());
   aStr.append(" bucketTransferBytes: " + details.getBucketTransferBytes());
   aStr.append(" bucketTransferTime: " + details.getBucketTransferTime() + "\n");

   // primaryTransfers
   aStr.append("PrimaryTransfersCompleted: " + details.getPrimaryTransfersCompleted());
   aStr.append(" PrimaryTransferTime: " + details.getPrimaryTransferTime() + "\n");

   // PartitionMemberDetails (before)
   aStr.append("PartitionedMemberDetails (before)\n");
   Set<PartitionMemberInfo> pmdSet = details.getPartitionMemberDetailsBefore();
   for (PartitionMemberInfo pmd: pmdSet) {
      aStr.append(partitionMemberDetailsToString(pmd));
   }
   
   // PartitionMemberDetails (after)
   aStr.append("PartitionedMemberDetails (after)\n");
   pmdSet = details.getPartitionMemberDetailsAfter();
   for (PartitionMemberInfo pmd: pmdSet) {
      aStr.append(partitionMemberDetailsToString(pmd));
   }
   
   return aStr.toString();
}

public static String partitionedRegionDetailsToString(PartitionRegionInfo prd) {

   if (prd == null) {
      return "null\n";
   } 

   StringBuffer aStr = new StringBuffer();

   aStr.append("PartitionedRegionDetails for region named " + getRegionName(prd) + "\n");
   aStr.append("  configuredBucketCount: " + prd.getConfiguredBucketCount() + "\n");
   aStr.append("  createdBucketCount: " + prd.getCreatedBucketCount() + "\n");
   aStr.append("  lowRedundancyBucketCount: " + prd.getLowRedundancyBucketCount() + "\n");
   aStr.append("  configuredRedundantCopies: " + prd.getConfiguredRedundantCopies() + "\n");
   aStr.append("  actualRedundantCopies: " + prd.getActualRedundantCopies() + "\n");

   // memberDetails
   Set<PartitionMemberInfo> pmd = prd.getPartitionMemberInfo();
   for (PartitionMemberInfo memberDetails : pmd) {
      aStr.append(partitionMemberDetailsToString(memberDetails));
   }

   // colocatedWithDetails
   String colocatedWith = prd.getColocatedWith();
   aStr.append("  colocatedWith: " + colocatedWith + "\n");

   return aStr.toString();
}

public static String partitionMemberDetailsToString(PartitionMemberInfo pmd) {
   StringBuffer aStr = new StringBuffer();
   long localMaxMemory = pmd.getConfiguredMaxMemory();
   long size = pmd.getSize();
   aStr.append("    Member Details for: " + pmd.getDistributedMember()+ "\n");
   aStr.append("      configuredMaxMemory: " + localMaxMemory);
   double inUse = (double)size / localMaxMemory;
   double heapUtilization = inUse * 100;
   aStr.append(" size: " + size + " (" + heapUtilization + "%)");
   aStr.append(" bucketCount: " + pmd.getBucketCount());
   aStr.append(" primaryCount: " + pmd.getPrimaryCount() + "\n");
   return aStr.toString();
}

public static String getRegionName(PartitionRegionInfo prd) {
   return prd.getRegionPath().substring(1);
}

public static String getRegionName(PartitionRebalanceInfo prd) {
   return prd.getRegionPath().substring(1);
}

/** Check that rebalance did what simualte proposed.
 *
 *  @param simulateResults The details of the simulate step.
 *  @param rebalanceResults The details of the rebalance step.
 */
public static void checkSimulateAgainstRebalance(RebalanceResults simulateResults,
                                                 RebalanceResults rebalanceResults) {
   Log.getLogWriter().info("Checking simulate results against rebalance results...");
   StringBuffer aStr = new StringBuffer();

   if (simulateResults.getTotalBucketCreateBytes() != rebalanceResults.getTotalBucketCreateBytes()) {
      aStr.append("simulate totalBucketCreateBytes is " + simulateResults.getTotalBucketCreateBytes() +
                  " but rebalance totalBucketCreateBytes is " + rebalanceResults.getTotalBucketCreateBytes() + "\n");
   }
   if (simulateResults.getTotalBucketCreatesCompleted() != rebalanceResults.getTotalBucketCreatesCompleted()) {
      aStr.append("simulate totalBucketCreatesCompleted is " + simulateResults.getTotalBucketCreatesCompleted() +
                  " but rebalance totalBucketCreatesCompleted is " + rebalanceResults.getTotalBucketCreatesCompleted() + "\n");
   }
   if (simulateResults.getTotalBucketTransferBytes() != rebalanceResults.getTotalBucketTransferBytes()) {
      aStr.append("simulate totalBucketTransferBytes is " + simulateResults.getTotalBucketTransferBytes() +
                  " but rebalance totalBucketTransferBytes is " + rebalanceResults.getTotalBucketTransferBytes() + "\n");
   }
   if (simulateResults.getTotalBucketTransfersCompleted() != rebalanceResults.getTotalBucketTransfersCompleted()) {
      aStr.append("simulate totalBucketTransfersCompleted is " + simulateResults.getTotalBucketTransfersCompleted() +
                  " but rebalance totalBucketTransfersCompleted is " + rebalanceResults.getTotalBucketTransfersCompleted() + "\n");
   }
   if (simulateResults.getTotalPrimaryTransfersCompleted() != rebalanceResults.getTotalPrimaryTransfersCompleted()) {
      aStr.append("simulate totalPrimaryTransfersCompleted is " + simulateResults.getTotalPrimaryTransfersCompleted() +
                  " but rebalance totalPrimaryTransfersCompleted is " + rebalanceResults.getTotalPrimaryTransfersCompleted() + "\n");
   }

   // set is one element for each PR that experienced rebalancing
   Set<PartitionRebalanceInfo> simSet = simulateResults.getPartitionRebalanceDetails();
   Map<String, PartitionRebalanceInfo> simMap = new HashMap();
   for (PartitionRebalanceInfo detail : simSet) {
      simMap.put(detail.getRegionPath(), detail);
   }
   Set<PartitionRebalanceInfo> rebSet = rebalanceResults.getPartitionRebalanceDetails();
   Map<String, PartitionRebalanceInfo> rebMap = new HashMap();
   for (PartitionRebalanceInfo detail : rebSet) {
      rebMap.put(detail.getRegionPath(), detail);
   }
   Set simKeys = simMap.keySet();
   Set rebKeys = rebMap.keySet();
   Set inRebButNotSim = new HashSet(rebKeys);
   inRebButNotSim.removeAll(simKeys);
   if (inRebButNotSim.size() > 0) {
      aStr.append("The regions " + inRebButNotSim + " were present in rebalance, but not in rebalance simulation\n");
   }
   for (String regionName : simMap.keySet()) { // iterate simulation for each PR
      PartitionRebalanceInfo simRebalanceDetails = (PartitionRebalanceInfo)(simMap.get(regionName)); 
      PartitionRebalanceInfo rebRebalanceDetails = (PartitionRebalanceInfo)(rebMap.get(regionName)); 
      if (rebRebalanceDetails == null) {
         aStr.append("The region " + regionName + " was present in rebalance simulation, but is not in rebalance details\n");
      } else { // region name found in both simulation and rebalance details
         // for a single PR, look at it's member details
         Set<PartitionMemberInfo> simMemDetailsSet = simRebalanceDetails.getPartitionMemberDetailsAfter(); 
         Set<PartitionMemberInfo> rebMemDetailsSet = rebRebalanceDetails.getPartitionMemberDetailsAfter(); 
         Map<String, PartitionMemberInfo> simMemMap = new HashMap();
         for (PartitionMemberInfo memDetail : simMemDetailsSet) {
            simMemMap.put(memDetail.getDistributedMember().toString(), memDetail);
         }
         Map<String, PartitionMemberInfo> rebMemMap = new HashMap();
         for (PartitionMemberInfo memDetail : rebMemDetailsSet) {
            rebMemMap.put(memDetail.getDistributedMember().toString(), memDetail);
         }
         Set simMemKeys = simMemMap.keySet();
         Set rebMemKeys = rebMemMap.keySet();
         inRebButNotSim = new HashSet(rebMemKeys);
         inRebButNotSim.removeAll(simMemKeys);
         if (inRebButNotSim.size() > 0) {
            aStr.append("For " + regionName + " the members " + inRebButNotSim + 
                 " were present in rebalance, but not in rebalance simulation\n");
         }
         for (String memberStr : simMemMap.keySet()) { // iterate simulation for each member of PR
            PartitionMemberInfo simDetails = (PartitionMemberInfo)(simMemMap.get(memberStr)); 
            PartitionMemberInfo rebDetails = (PartitionMemberInfo)(rebMemMap.get(memberStr)); 
            if (rebDetails == null) {
               aStr.append("For " + regionName + " the member " + memberStr + 
                     " was present in rebalance simulation, but is not in rebalance details\n");
            } else { // member found in both simulation and rebalance details
               int simBucketCount = simDetails.getBucketCount();
               int rebBucketCount = rebDetails.getBucketCount();
               if (simBucketCount != rebBucketCount) {
                  aStr.append("For " + regionName + " simulated bucket count for member " + memberStr +
                              " is " + simBucketCount + ", but after rebalance the bucket count is " + 
                              rebBucketCount + "\n");
               }
               int simPrimaryCount = simDetails.getPrimaryCount();
               int rebPrimaryCount = rebDetails.getPrimaryCount();
               if (simPrimaryCount != rebPrimaryCount) {
                  aStr.append("For " + regionName + " simulated primary count for member " + memberStr +
                              " is " + simPrimaryCount + ", but after rebalance the primary count is " + 
                              rebPrimaryCount + "\n");
               }
            }
         }
      }
   }
   if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
   }
   Log.getLogWriter().info("Done checking simulate results against rebalance results; results are the same");
}

/** Check that rebalancing actually caused some movement of data.
 *  If not, find out why not (e.g. was there no more space)?
 *
 *  @param result  The RebalanceResults
 */
public static void checkRebalancingActivity(RebalanceResults results) {

   boolean actionRequired = TestConfig.tasktab().booleanAt(RebalancePrms.actionRequired, TestConfig.tab().booleanAt(RebalancePrms.actionRequired, false));

   boolean actionTaken = false;
   if ((results.getTotalBucketCreatesCompleted() > 0) ||
       (results.getTotalBucketTransfersCompleted() > 0) ||
       (results.getTotalPrimaryTransfersCompleted() > 0)) {
      actionTaken = true;
   }

   if (!actionTaken) {
      String s = "Rebalancing did not result in any bucket creates, bucket transfers or primary transfers";
      if (actionRequired) {
         throw new TestException(s);
      } else {
         Log.getLogWriter().warning(s);
      }
   }
}

/** Check that the details of the rebalance agrees with the current state of the region(s)
 *  that were rebalanced.
 *
 *  @param detailSet The details of the rebalance. Each element of this set is the
 *                   details of rebalancing one PR.
 */
public static void checkResultsWithRegion(Set<PartitionRebalanceInfo> detailSet) {
   StringBuffer errStr = new StringBuffer();
   for (PartitionRebalanceInfo prd : detailSet) {
      String regionName = prd.getRegionPath();
      Region aRegion = CacheHelper.getCache().getRegion(regionName);
      Map[] mapArr = BucketState.getBucketMaps(aRegion);
      Map primaryMap = mapArr[0];
      Map bucketMap = mapArr[1];
      Log.getLogWriter().info("Checking rebalance details with current state of region " + regionName +
          ", primaries: " + primaryMap + ", buckets: " + bucketMap);
      Set<PartitionMemberInfo> memberDetails = prd.getPartitionMemberDetailsAfter();
      StringBuffer aStr = new StringBuffer();
      for (PartitionMemberInfo memDetail : memberDetails) {
         DistributedMember detailMember = memDetail.getDistributedMember();
         int detailBucketCount = memDetail.getBucketCount(); 
         int detailPrimaryCount = memDetail.getPrimaryCount(); 

         // check bucket count against test hooks
         Integer vmIdForMember = null;
         try {
            vmIdForMember = RemoteTestModule.Master.getVmid(detailMember.getHost(), detailMember.getProcessId());
         } catch (java.rmi.RemoteException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
         Integer anInt = (Integer)(bucketMap.get(vmIdForMember));
         if (anInt == null) {
            anInt = new Integer(0);
         }
         int hookNumBuckets = anInt.intValue();
         if (hookNumBuckets != detailBucketCount) {
            aStr.append("For member " + detailMember + ", rebalance details has " + 
                 detailBucketCount + " buckets but testing hook shows " + hookNumBuckets + 
                 " buckets\n"); 
         }

         // check primary count against test hooks
         anInt = (Integer)(primaryMap.get(vmIdForMember));
         if (anInt == null) {
            anInt = new Integer(0);
         }
         int hookNumPrimaries = anInt.intValue();
         if (hookNumPrimaries != detailPrimaryCount) {
            aStr.append("For member " + detailMember + ", rebalance details shows " + 
                 detailPrimaryCount + " primaries but testing hook shows " + hookNumPrimaries + 
                 " primaries\n"); 
         }
      }
      if (aStr.length() > 0) {
         aStr.append("primary state from hook: " + primaryMap + "\n" +
                     "bucket state from hook: " + bucketMap);
         errStr.append(aStr);
      }
   }
   if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
   }
}


//-----------------------------------------------------------------------------------
// Eviction utilities
//-----------------------------------------------------------------------------------

/** Hydra CLOSETASK to verify that eviction occurred in rebalancing eviction tests.
 */
public static void HydraTask_verifyEviction() {

   double numEvictions = 0;
   Cache myCache = CacheHelper.getCache();

   Set regions = myCache.rootRegions();
   for (Iterator iter = regions.iterator(); iter.hasNext(); ) {
      Region aRegion = (Region)iter.next();
      EvictionAlgorithm ea = aRegion.getAttributes().getEvictionAttributes().getAlgorithm();
      if (ea.isLRUEntry()) {
         numEvictions = TestHelper.getNumLRUEvictions();
      } else if (ea.isLRUMemory()) {
         numEvictions = getNumMemLRUEvictions();
      } else if (ea.isLRUHeap()) {
         numEvictions = TestHelper.getNumHeapLRUEvictions();
      } else {
         throw new TestException("TestConfig issue: verifyEviction requires that the region have eviction enabled");
      }
   }

   if (numEvictions > 0) {
      Log.getLogWriter().info("Total evictions = " + numEvictions); 
   } else {
      throw new TestException("Tuning required: no evictions recorded");
   }
}

/** Return the number of MemLRU evictions that have occurred.
 *  Note: util.TestHelper() searches all bridgeServer archives
 *
 *  @return The number of MemLRU evictions, obtained from stats.
 *
 */
public static double getNumMemLRUEvictions() {
   String spec = "* " // search all archives
                 + "MemLRUStatistics "
                 + "* " // match all instances
                 + "lruEvictions "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double totalEvictions = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
   }
   return totalEvictions;
}

//============================================================================
// INITTASKS/CLOSETASKS
//============================================================================

/**
 * Creates a (disconnected) locator.
 */
public static void createLocatorTask() {
  DistributedSystemHelper.createLocator();
}

/**
 * Connects a locator to its distributed system.
 */
public static void startAndConnectLocatorTask() {
  DistributedSystemHelper.startLocatorAndAdminDS();
}

/**
 * Stops a locator.
 */
public static void stopLocatorTask() {
  DistributedSystemHelper.stopLocator();
}
  
}
