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
package resumeTx; 

import tx.*;
import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.cache.partition.*;

import java.util.*;

/**
 * A class to contain methods useful for all transactions tests.
 */
public class RtxUtil extends tx.TxUtil {

/**
 *  Create the cache and Region (non-colocated PartitionedRegions)
 */
public synchronized static void HydraTask_createPartitionedRegions() {
  if (txUtilInstance == null) {
    txUtilInstance = new RtxUtil();
    try {
      txUtilInstance.createPartitionedRegions();
    } catch (Exception e) {
      Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
      throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
    }
  }
}

/*
 *  Creates initial set of entries across colocated regions
 *  (non-transactional).
 */
public static void HydraTask_populateRegions() {
   txUtilInstance.initialize();
   ((RtxUtil)txUtilInstance).setNumDataStores();
   txUtilInstance.populateRegions();
}

protected void setNumDataStores() {
   // prime the PartitionRegion by getting at least one entry in each bucket
   // make sure that numBuckets (numDataStores * totalNumBuckets) < TxPrms.maxKeys!
   Set prRegionsInCache = PartitionRegionHelper.getPartitionRegionInfo(CacheHelper.getCache());
   if (!prRegionsInCache.isEmpty()) {
      Set membersHostingPR = ((PartitionRegionInfo)prRegionsInCache.iterator().next()).getPartitionMemberInfo();
      int numDataStores = membersHostingPR.size();
      Log.getLogWriter().info("Writing setNumDataStores to BB " + numDataStores);
      ResumeTxBB.getBB().getSharedMap().put(ResumeTxBB.NUM_DATASTORES, new Integer(numDataStores));
   }
}

/**
 *  Create the cache and colocated PRs
 */
public synchronized static void HydraTask_createColocatedRegions() {
  if (txUtilInstance == null) {
    txUtilInstance = new RtxUtil();
    try {
      ((RtxUtil)txUtilInstance).createColocatedRegions();
    } catch (Exception e) {
      Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
      throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
    }
  }
}

  /*
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  public void createColocatedRegions() {
    super.initialize();
    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
       Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
       CacheUtil.setCache(c);     // TxUtil and splitBrain/distIntegrityFD require this
       c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
       ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());
       if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ)) {
          c.setCopyOnRead(true);
       }

       String regionConfig = ConfigPrms.getRegionConfig();
       AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       String regionBase = rd.getRegionName();

       // override colocatedWith in the PartitionAttributes
       PartitionDescription pd = rd.getPartitionDescription();
       PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
       PartitionAttributes prAttrs = null;

       String colocatedWith = null;
       int numRegions = ResumeTxPrms.getNumColocatedRegions();
       for (int i = 0; i < numRegions; i++) {
          String regionName = regionBase + "_" + (i+1);
          if (i > 0) {
             colocatedWith = regionBase + "_" + i;
             prFactory.setColocatedWith(colocatedWith);
             prAttrs = prFactory.create();
             aFactory.setPartitionAttributes(prAttrs);
          }
          Region aRegion = RegionHelper.createRegion(regionName, aFactory);
       }
     }
  }

  /*
   * Creates cache and same regions as createColocatedRegions, but for edgeClients.
   * So, the regions will not be partitioned or colocatedWith other regions, but they
   * will have the same naming structure as the pr colocated server regions.
   */
  public synchronized static void HydraTask_createClientRegions() {
    if (txUtilInstance == null) {
      txUtilInstance = new RtxUtil();
      ((RtxUtil)txUtilInstance).createClientRegions();
    }
  }

  public void createClientRegions() {
    super.initialize();
    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
       Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
       CacheUtil.setCache(c);     // TxUtil and splitBrain/distIntegrityFD require this
       c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
       ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());
       if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ)) {
          c.setCopyOnRead(true);
       }

       String regionConfig = ConfigPrms.getRegionConfig();
       AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       String regionBase = rd.getRegionName();

       int numRegions = ResumeTxPrms.getNumColocatedRegions();
       for (int i = 0; i < numRegions; i++) {
          String regionName = regionBase + "_" + (i+1);
          Region aRegion = RegionHelper.createRegion(regionName, aFactory);

          // edge clients register interest in ALL_KEYS
          aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("registered interest in ALL_KEYS for " + regionName);
       }
     }
  }

 /*
  *  Creates initial set of entries across colocated regions
  *  (non-transactional).
  */
  public static void HydraTask_primeBuckets() {
     ((RtxUtil)txUtilInstance).primeBuckets();
  }
 
  protected void primeBuckets() {
     // prime the PartitionRegion by getting at least one entry in each bucket
     Set prRegionsInCache = PartitionRegionHelper.getPartitionRegionInfo(CacheHelper.getCache());
     int numDataStores = 0;
     int configuredBucketCount = 0;
     int numBuckets = 0;
     if (!prRegionsInCache.isEmpty()) {
        PartitionRegionInfo prRegionInfo = (PartitionRegionInfo)prRegionsInCache.iterator().next();
        Set membersHostingPR = prRegionInfo.getPartitionMemberInfo();
        numDataStores = membersHostingPR.size();
        ResumeTxBB.getBB().getSharedMap().put(ResumeTxBB.NUM_DATASTORES, new Integer(numDataStores));
        configuredBucketCount = prRegionInfo.getConfiguredBucketCount();
        numBuckets = configuredBucketCount * numDataStores;
     }
     Log.getLogWriter().info("primeBuckets: total number of buckets in test (dataStores(" + numDataStores + ") * configuredBucketCount(" + configuredBucketCount + ") = " + numBuckets + ")");

     for (int i = 0; i < numBuckets; i++) {
        Object key = NameFactory.getNextPositiveObjectName();
  
        // create this same key in each region 
        Set rootRegions = CacheHelper.getCache().rootRegions();
        for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
           Region aRegion = (Region)it.next();
           BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
           String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
           Log.getLogWriter().info("populateRegion: calling create for key " + key + ", object " + TestHelper.toString(vh) + ", region is " + aRegion.getFullPath());
           aRegion.create(key, vh, callback);
           Log.getLogWriter().info("populateRegion: done creating key " + key);
        }
     }
     dumpLocalKeys();
  }


/** Gets a new (never before seen) key
 *
 *  @return the next key from NameFactory
 */
public Object getNewKey(Region aRegion) {
   Object key = null;
   boolean found = false;
   if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     DistributedMember myDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
     // will this key be for this VM?
     DistributedMember primaryDM;
     boolean useColocatedEntries = ResumeTxPrms.getUseColocatedEntries();
     do {
       key = NameFactory.getNextPositiveObjectName();
       primaryDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
       if (primaryDM != null && primaryDM.equals(myDM)) {
         Log.getLogWriter().fine("getNewKey() ... primaryDM.equals(myDM) = " + primaryDM.toString());
         if (useColocatedEntries) {
           TransactionId txId = TxHelper.getTransactionId();
           //Map activeTxns = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
           Map activeTxnsWrapped = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
           try {
             Map activeTxns = RtxUtilVersionHelper.convertWrapperMapToActiveTxMap(activeTxnsWrapped);  
             Log.getLogWriter().fine("getRandomKey: " + ResumeTxTest.activeTxnsToString(activeTxns));
             if (activeTxns != null) {
                TxInfo txInfo = (TxInfo)activeTxns.get(txId);
                ModRoutingObject filterRoutingObject = txInfo.getRoutingObject();
                ModRoutingObject aRoutingObject = new ModRoutingObject(key);
                if (filterRoutingObject.hashCode() == aRoutingObject.hashCode()) {
                   Log.getLogWriter().fine("Found colocated entries : " + filterRoutingObject.getKey() + " and " + aRoutingObject.getKey() + " both have the same hashCode " + filterRoutingObject.hashCode());
                   found  = true;
                } 
             }  
           } catch (Exception e) {
             throw new TestException(e.toString() + e.getStackTrace().toString());
           }
         } else {  // no restriction on colocated entries
            found = true;
         }
       }
     } while (!found);
   } else {  // any key will do (replicated regions)
      key = NameFactory.getNextPositiveObjectName();
   }
   return key;
}

/** Get a random key from the given region, excluding the key specified
 *  by excludeKey. If no keys qualify in the region, return null.
 *  If suspendResume is true, then suspend the tx before finding
 *  the random key, then resume before returning.
 *
 *  @param aRegion - The region to get the key from.
 *  @param excludeKey - The region to get the key from.
 *
 *  @returns A key from aRegion, or null.
 */
public Object getRandomKey(Region aRegion, Object excludeKey) {
   // getting a random key can put all the keys into the remembered
   // set, so suspend the tx if suspendResume is true, then resume it 
   // before leaving this method so the test can test according to 
   // its strategy without the test framework getting in the way.
   if (suspendResume) {
      txState.set(TxHelper.internalSuspend()); 
   }
   if (aRegion == null) {
      return null;
   }
   Set aSet = null;
   try {
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        if (ResumeTxPrms.getUseColocatedEntries()) {  // first requirement is that this vm is primary
           aSet = new HashSet(PartitionRegionHelper.getLocalPrimaryData(aRegion).keySet());
           TransactionId txId = TxHelper.getTransactionId();
           //Map activeTxns = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
           Map activeTxnsWrapped = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
           try {
             Map activeTxns = RtxUtilVersionHelper.convertWrapperMapToActiveTxMap(activeTxnsWrapped);
             Log.getLogWriter().fine("getRandomKey: " + ResumeTxTest.activeTxnsToString(activeTxns));
             if (activeTxns != null) { 
                TxInfo txInfo = (TxInfo)activeTxns.get(txId);
                ModRoutingObject filterRoutingObject = txInfo.getRoutingObject();

                if (filterRoutingObject != null) {
                   for (Iterator it=aSet.iterator(); it.hasNext(); ) {
                      Object aKey = it.next();
                      ModRoutingObject aRoutingObject = new ModRoutingObject(aKey);
                      if (filterRoutingObject.hashCode() != aRoutingObject.hashCode()) {
                         it.remove();    // remove last entry returned by it.next()
                      }
                   }
                }
             }
           } catch (Exception e) {
            throw new TestException(e.toString() + e.getStackTrace().toString());
           }
           
        } else {
           aSet = new HashSet(PartitionRegionHelper.getLocalData(aRegion).keySet());
        }
        Log.getLogWriter().fine("getRandomKey: local keySet = " + aSet);
      } else {
        aSet = new HashSet(aRegion.keySet());
      }
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      if (suspendResume) {
         TxHelper.internalResume((TXStateProxy)txState.get()); 
      }
      return null;
   }
   Object[] keyArr = aSet.toArray();
   if (keyArr.length == 0) {
      Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + 
          " because the region has no keys");
      if (suspendResume) {
         TxHelper.internalResume((TXStateProxy)txState.get()); 
      }
      return null;
   }

   int randInt = TestConfig.tab().getRandGen().nextInt(0, keyArr.length-1);
   Object key = keyArr[randInt];
   if (key.equals(excludeKey)) { // get another key
      if (keyArr.length == 1) { // there are no other keys
         if (suspendResume) {
            TxHelper.internalResume((TXStateProxy)txState.get());
         }
         return null;
      }
      randInt++; // go to the next key
      if (randInt == keyArr.length)
         randInt = 0;
      key = keyArr[randInt];
   }
   if (suspendResume) {
      TxHelper.internalResume((TXStateProxy)txState.get()); 
   }
   return key;
}
 
 /*
  *  HydraTask to dump local keys (PR primary keys) along with their ModRoutingObject hashcodes.
  *  (non-transactional).
  */
  public static void HydraTask_dumpLocalKeys() {
     ((RtxUtil)txUtilInstance).dumpLocalKeys();
  }

  protected void dumpLocalKeys() {
     StringBuffer aStr = new StringBuffer();
     DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();

     aStr.append("Keys hosted as primary " + dm.toString() + " by region (key=<routingHashCode>)\n");
     Set rootRegions = CacheHelper.getCache().rootRegions();
     for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
        Region aRegion = (Region)it.next();
        Region localRegion = PartitionRegionHelper.getLocalData(aRegion);
        Set keySet = localRegion.keySet();
        HashMap primaryKeys = new HashMap();
        for (Iterator kit = keySet.iterator(); kit.hasNext();) {
          Object key = kit.next();
          DistributedMember primary = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
          if (primary.equals(dm)) {
            ModRoutingObject ro = new ModRoutingObject(key);
            primaryKeys.put(key, ro.hashCode());
          }
        }
        aStr.append("   " + aRegion.getName() + ": " + primaryKeys + "\n");
     }
     Log.getLogWriter().info(aStr.toString());
  }
}
