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
package hct; 

import getInitialImage.InitImageBB;
import getInitialImage.InitImagePrms;
import getInitialImage.InitImageTest;
import hydra.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import util.KeyIntervals;
import util.NameBB;
import util.NameFactory;
import util.SilenceListener;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

import cq.CQUtil;
import durableClients.DurableClientsBB;
import durableClients.DurableClientsPrms;

public class InterestPolicyTest extends InitImageTest {

// the name of the region used by this test
static protected final String REGION_NAME = "InterestPolicyRegion";



// static fields
static protected ArrayList keyList;              // a key list to register (contains all keys for this test)
static protected ArrayList partialKeyList;       // the partial list of keys to register (even number keys) 
                                                 //     plus new keys, plus extra keys never created by the test
static protected int partial_numDestroyKeys;     // number of keys in partialKeyList that are destroyed
static protected int partial_numInvalidateKeys;  // number of keys in partialKeyList that are destroyed
static protected int partial_numUpdateKeys;      // number of keys in partialKeyList that are destroyed
static protected final int NUM_EXTRA_KEYS = 100; // number of keys in keyList and partialKeyList that are never 
                                                 //     created by the test
static protected volatile InterestResultPolicy policy;  // The InterestResultPolicy used by this VM.
static protected KeyIntervals intervals;         // The keyIntevals for this test

// type of registerInterest (maintain for re-subscribing), see recycleClientWithCacheClose
static protected int subscription;
static final int ALL_KEYS     = 1;
static final int LIST         = 2;
static final int PARTIAL_LIST = 3;
static final int REGEX        = 4;

static long killInterval = 60000;
static Random rand = new Random();

// static fields for expected keys/values of the region 
// static tests:
   // these fields record the expected region state after a registerInterest and ops
      static protected ExpectedRegionContents static_RI_ops_keysValues;  
      static protected ExpectedRegionContents static_RI_ops_keys;        
      static protected ExpectedRegionContents static_RI_ops_none;        
      static protected ExpectedRegionContents staticPartial_RI_ops_keysValues;  
      static protected ExpectedRegionContents staticPartial_RI_ops_keys;        
      static protected ExpectedRegionContents staticPartial_RI_ops_none;        

   // these fields record the expected region state after a registerInterest, but no ops
      static protected ExpectedRegionContents static_RI_noops_keysValues; 
      static protected ExpectedRegionContents static_RI_noops_keys;       
      static protected ExpectedRegionContents static_RI_noops_none;       
      static protected ExpectedRegionContents staticPartial_RI_noops_keysValues; 
      static protected ExpectedRegionContents staticPartial_RI_noops_keys;       
      static protected ExpectedRegionContents staticPartial_RI_noops_none;       

   // these fields record the expected region state after ops, then a registerInterest
      static protected ExpectedRegionContents static_ops_RI_keysValues; 
      static protected ExpectedRegionContents static_ops_RI_keys;       
      static protected ExpectedRegionContents static_ops_RI_none;       
      static protected ExpectedRegionContents staticPartial_ops_RI_keysValues; 
      static protected ExpectedRegionContents staticPartial_ops_RI_keys;       
      static protected ExpectedRegionContents staticPartial_ops_RI_none;       

// dynamic tests:
   // these fields record the expected region state after the load and after all ops complete 
   // concurrently with a registerInterest with various interest policies
      static protected ExpectedRegionContents dynamicKeysValues;  
      static protected ExpectedRegionContents dynamicKeys;        
      static protected ExpectedRegionContents dynamicNone;        
      static protected ExpectedRegionContents dynamicPartialKeysValues;  
      static protected ExpectedRegionContents dynamicPartialKeys;        
      static protected ExpectedRegionContents dynamicPartialNone;        

// ======================================================================== 
// initialization tasks/methods 

/** Initialize the known keys for this test
 */
public static void StartTask_initialize() {
   // initialize keyIntervals
   int numKeys = TestConfig.tab().intAt(InitImagePrms.numKeys);
   intervals = new KeyIntervals(
      new int[] {KeyIntervals.NONE, KeyIntervals.INVALIDATE,
                 KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
                 KeyIntervals.GET, KeyIntervals.LOCAL_DESTROY, KeyIntervals.LOCAL_INVALIDATE}, 
                 numKeys);
   InitImageBB.getBB().getSharedMap().put(InitImageBB.KEY_INTERVALS, intervals);
   Log.getLogWriter().info("Created keyIntervals: " + intervals);

   // Set the counters for the next keys to use for each operation
   hydra.blackboard.SharedCounters sc = InitImageBB.getBB().getSharedCounters();
   sc.setIfLarger(InitImageBB.LASTKEY_INVALIDATE, intervals.getFirstKey(KeyIntervals.INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_DESTROY, intervals.getFirstKey(KeyIntervals.DESTROY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY, intervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_GET, intervals.getFirstKey(KeyIntervals.GET)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_INVALIDATE, intervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_DESTROY, intervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)-1);
   
   // for failovertests
    BBoard.getInstance().getSharedMap().put("lastKillTime", new Long(0));

   // show the blackboard
   InitImageBB.getBB().printSharedMap();
   InitImageBB.getBB().printSharedCounters();
}

/**
 * Initializes the test region in the cache server VM
 */
public static void initBridgeServer() {
   CacheHelper.createCache(ConfigPrms.getCacheConfig());
   RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
   BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
}

/** Initialize the single instance of this test class but not a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new InterestPolicyTest();
      ((InterestPolicyTest)testInstance).initInstance();
   }
}

/**
 *  Initialize this test instance
 */
public void initInstance() {
   super.initInstance();

   // edgeClients (even empty clients) support ConcurrentMap operations (see getInitialImage.InitImageTest)
   supportsConcurrentMap = true;

   // create cache and region
   Cache cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
   aRegion = RegionHelper.createRegion(REGION_NAME,
                                       ConfigPrms.getRegionConfig());
   // checking for durable clients
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
      if (!DurableClientsBB.getBB().getSharedMap().containsKey(VmDurableId)) {
        HashMap map = new HashMap();
        map.put("CRASH COUNT", new Integer(0));
        DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      }
      else {
        HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
            VmDurableId);
        int crashCount = ((Integer)map.get("CRASH COUNT")).intValue();
        crashCount++;
        map.put("CRASH COUNT", new Integer(crashCount));
        DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      }
    }

   // create CQ, if needed (CQUtil will only create if specified by CQUtilPrms)
   CQUtil.initialize();
   CQUtil.initializeCQService();
   CQUtil.registerCQ(aRegion);

   // create a list of all keys in key intervals
   keyList = new ArrayList();
   partialKeyList = new ArrayList();
   
   // initializing the partial key count again (because for durable clients
    // the count kept on incrementing for each reconnection
    partial_numDestroyKeys = 0;
    partial_numInvalidateKeys = 0;
    partial_numUpdateKeys = 0;
   
   int numKeyIntervalKeys = keyIntervals.getNumKeys();
   for (int i = 1; i <= numKeyIntervalKeys; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      keyList.add(key);
      if ((i % 2) == 0) { // key has even number
         partialKeyList.add(key);
         if (keyIntervals.keyInRange(KeyIntervals.DESTROY, i))
            partial_numDestroyKeys++;
         if (keyIntervals.keyInRange(KeyIntervals.INVALIDATE, i))
            partial_numInvalidateKeys++;
         if (keyIntervals.keyInRange(KeyIntervals.UPDATE_EXISTING_KEY, i))
            partial_numUpdateKeys++;
      }
   }
   int partialKeysFromKeyIntervals = partialKeyList.size();

   // add to the list of keys the new keys
   for (int i = numKeyIntervalKeys+1; i <= numKeyIntervalKeys+numNewKeys; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      keyList.add(key);
      partialKeyList.add(key);
   }

   // add some extra keys to the list that are never created by the test
   for (int i = numKeyIntervalKeys+numNewKeys + 1;
            i <= numKeyIntervalKeys+numNewKeys + NUM_EXTRA_KEYS; i++) { // do a few more keys than we really need
      Object key = NameFactory.getObjectNameForCounter(i);
      keyList.add(key);
      partialKeyList.add(key);
   }

   Log.getLogWriter().info("keyList size is " + keyList.size());
   Log.getLogWriter().info("partialKeyList size is " + partialKeyList.size());
   Log.getLogWriter().info("partial_numDestroyKeys is " + partial_numDestroyKeys);
   Log.getLogWriter().info("partial_numInvalidateKeys is " + partial_numInvalidateKeys);
   Log.getLogWriter().info("partial_numUpdateKeys is " + partial_numUpdateKeys);

// The following specify expected region contents for various testing scenarios
// static tests
   // expected values for region contents after a register interest, but no ops have executed
   static_RI_noops_keysValues = new ExpectedRegionContents(
      true,  true,  // none
      true,  true,  // invalidate
      true,  true,  // localInvalidate
      true,  true,  // destroy
      true,  true,  // localDestroy
      true,  true,  // update
      true,  true,  // get
      false, false, // newKey
      false,        // get allowed during validate
      false);       // update has occurred
      static_RI_noops_keysValues.exactSize(keyIntervals.getNumKeys());
   static_RI_noops_keys = new ExpectedRegionContents(
      true,  false,  // none
      true,  false,  // invalidate
      true,  false,  // localInvalidate
      true,  false,  // destroy
      true,  false,  // localDestroy
      true,  false,  // update
      true,  false,  // get
      false, false, // newKey
      false,        // get allowed during validate
      false);       // update has occurred
      static_RI_noops_keys.exactSize(keyIntervals.getNumKeys());
   static_RI_noops_none = new ExpectedRegionContents(
      false, false,  // none
      false, false,  // invalidate
      false, false,  // localInvalidate
      false, false,  // destroy
      false, false,  // localDestroy
      false, false,  // update
      false, false,  // get
      false, false, // newKey
      false,        // get allowed during validate
      false);       // update has occurred
      static_RI_noops_none.exactSize(0);
   staticPartial_RI_noops_keysValues = (ExpectedRegionContents)(static_RI_noops_keysValues.clone());
      staticPartial_RI_noops_keysValues.exactSize(partialKeyList.size() - numNewKeys - NUM_EXTRA_KEYS);
   staticPartial_RI_noops_keys = (ExpectedRegionContents)(static_RI_noops_keys.clone());
      staticPartial_RI_noops_keys.exactSize(partialKeyList.size() - numNewKeys - NUM_EXTRA_KEYS);
   staticPartial_RI_noops_none = (ExpectedRegionContents)(static_RI_noops_none.clone());
   
   // expected values for region contents after ops then a register interest
   int numDestroys = keyIntervals.getNumKeys(KeyIntervals.DESTROY);
   static_ops_RI_keysValues = new ExpectedRegionContents(
      true,  true,  // none
      true,  false, // invalidate
      true,  true,  // localInvalidate
      false, false, // destroy
      true,  true,  // localDestroy
      true,  true,  // update
      true,  true,  // get
      true,  true,  // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_ops_RI_keysValues.exactSize(keyIntervals.getNumKeys() - numDestroys + numNewKeys);
   static_ops_RI_keys = new ExpectedRegionContents(
      true,  false, // none
      true,  false, // invalidate
      true,  false, // localInvalidate
      false, false, // destroy
      true,  false, // localDestroy
      true,  false, // update
      true,  false, // get
      true,  false, // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_ops_RI_keys.exactSize(keyIntervals.getNumKeys() - numDestroys + numNewKeys);
   static_ops_RI_none = new ExpectedRegionContents(
      false, false, // none
      false, false, // invalidate
      false, false, // localInvalidate
      false, false, // destroy
      false, false, // localDestroy
      false, false, // update
      false, false, // get
      false, false, // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_ops_RI_none.exactSize(0);
   staticPartial_ops_RI_keysValues = (ExpectedRegionContents)(static_ops_RI_keysValues.clone());
      staticPartial_ops_RI_keysValues.exactSize(partialKeysFromKeyIntervals - partial_numDestroyKeys + numNewKeys);
   staticPartial_ops_RI_keys = (ExpectedRegionContents)(static_ops_RI_keys.clone());
      staticPartial_ops_RI_keys.exactSize(partialKeysFromKeyIntervals - partial_numDestroyKeys + numNewKeys);
   staticPartial_ops_RI_none = (ExpectedRegionContents)(static_ops_RI_none.clone());
   
   // expected values for region contents after a register interest, and after ops 
   static_RI_ops_keysValues = new ExpectedRegionContents(
      true,  true,  // none
      true,  false, // invalidate
      true,  true,  // localInvalidate
      false, false, // destroy
      true,  true,  // localDestroy
      true,  true,  // update
      true,  true,  // get
      true,  true,  // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_RI_ops_keysValues.exactSize(keyIntervals.getNumKeys()
                                         - keyIntervals.getNumKeys(KeyIntervals.DESTROY)
                                         + numNewKeys);
   staticPartial_RI_ops_keysValues = (ExpectedRegionContents)(static_RI_ops_keysValues.clone());
      staticPartial_RI_ops_keysValues.exactSize(partialKeyList.size()
                                                - partial_numDestroyKeys
                                                - NUM_EXTRA_KEYS);
   static_RI_ops_keys = new ExpectedRegionContents(
      true,  false, // none
      true,  false, // invalidate
      true,  false, // localInvalidate
      false, false, // destroy
      true,  false, // localDestroy
      true,  true,  // update
      true,  false, // get
      true,  true,  // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_RI_ops_keys.exactSize(keyIntervals.getNumKeys()
                                   - keyIntervals.getNumKeys(KeyIntervals.DESTROY)
                                   + numNewKeys);
   staticPartial_RI_ops_keys = (ExpectedRegionContents)(static_RI_ops_keys.clone());
      staticPartial_RI_ops_keys.exactSize(partialKeyList.size()
                                          - partial_numDestroyKeys
                                          - NUM_EXTRA_KEYS);
   static_RI_ops_none = new ExpectedRegionContents(
      false, false, // none
      false, false, // invalidate
      false, false, // localInvalidate
      false, false, // destroy
      false, false, // localDestroy
      true,  true,  // update
      false, false, // get
      true,  true,  // newKey
      true,         // get allowed during validate
      true);        // update has occurred
      static_RI_ops_none.exactSize(keyIntervals.getNumKeys() 
                                   - keyIntervals.getNumKeys(KeyIntervals.NONE) 
                                   - keyIntervals.getNumKeys(KeyIntervals.INVALIDATE) 
                                   - keyIntervals.getNumKeys(KeyIntervals.LOCAL_INVALIDATE) 
                                   - keyIntervals.getNumKeys(KeyIntervals.DESTROY) 
                                   - keyIntervals.getNumKeys(KeyIntervals.LOCAL_DESTROY) 
                                   - keyIntervals.getNumKeys(KeyIntervals.GET) 
                                   + numNewKeys);
   staticPartial_RI_ops_none = (ExpectedRegionContents)(static_RI_ops_none.clone());
      staticPartial_RI_ops_none.exactSize(partial_numUpdateKeys + numNewKeys);

// dynamic tests
   // expected values for region contents after a load where registerInterest
   // occurs concurrently with ops
   dynamicKeysValues = new ExpectedRegionContents(true, true, true);
      dynamicKeysValues.containsValue_invalidate(false);
      dynamicKeysValues.containsKey_destroy(false);
      dynamicKeysValues.containsValue_destroy(false);
      dynamicKeysValues.valueIsUpdated(true);
      dynamicKeysValues.exactSize(keyIntervals.getNumKeys()
                                  - keyIntervals.getNumKeys(KeyIntervals.DESTROY)
                                  + numNewKeys);
   dynamicKeys = new ExpectedRegionContents(true, false, true);
      dynamicKeys.containsKey_destroy(null);
      dynamicKeys.containsValue_update(null);
      dynamicKeys.containsValue_newKey(null);
      dynamicKeys.valueIsUpdated(true);
      dynamicKeysValues.exactSize(keyIntervals.getNumKeys()
                                  - keyIntervals.getNumKeys(KeyIntervals.DESTROY)
                                  + numNewKeys);
   dynamicNone = new ExpectedRegionContents(false, false, true);
      dynamicNone.containsKey_invalidate(null);
      dynamicNone.containsKey_update(null);
      dynamicNone.containsValue_update(null);
      dynamicNone.containsKey_newKey(null);
      dynamicNone.containsValue_newKey(null);
      dynamicNone.valueIsUpdated(true);
   dynamicPartialKeysValues = (ExpectedRegionContents)(dynamicKeysValues.clone());
      dynamicPartialKeysValues.exactSize(partialKeysFromKeyIntervals
                                         - partial_numDestroyKeys
                                         + numNewKeys);
   dynamicPartialKeys = (ExpectedRegionContents)(dynamicKeys.clone());
      dynamicPartialKeys.exactSize(partialKeysFromKeyIntervals
                                   - partial_numDestroyKeys
                                   + numNewKeys);
   dynamicPartialNone = (ExpectedRegionContents)(dynamicNone.clone());

}

// ======================================================================== 
// hydra tasks

/** Hydra task to initialize a region and load it according to hydra param settings. 
 */
public static void HydraTask_loadRegion() {
   testInstance.loadRegion();
}


/**
 * Kill the clients by crashing the VMs
 */
  public static void killClient() {
    try {
      hydra.MasterController.sleepForMs(1000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }
  
  /**
   * Kill the clients by closing the cache with keep-alive option as true
   */
  public static void killClientWithCacheClose() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      // close the cache and disconnect from the distributed system
      Cache cache = CacheHelper.getCache();
      // closing the cache with the keep-alive option true for the durable
      // clients
      cache.close(true);
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    testInstance.initInstance();
  }

  /**
   * Kill the clients by closing the cache with keep-alive option as true
   * re-start and register interest with same interest policy as earlier
   */
  public static void recycleClientWithCacheClose() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      // close the cache and disconnect from the distributed system
      Cache cache = CacheHelper.getCache();
      // closing the cache with the keep-alive option true for the durable
      // clients
      cache.close(true);
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    testInstance.initInstance();
    ((InterestPolicyTest)testInstance).renewSubscription();
  }

  // register interest with the same interest as previous invocations for this VM
  protected void renewSubscription() {
    switch (subscription) {
      case ALL_KEYS: 
         ((InterestPolicyTest)testInstance).registerInterest("ALL_KEYS");
         break;
      case LIST: 
       ((InterestPolicyTest)testInstance).registerInterest(keyList);
         break;
      case PARTIAL_LIST: 
       ((InterestPolicyTest)testInstance).registerInterest(partialKeyList);
         break;
      case REGEX: 
       ((InterestPolicyTest)testInstance).registerInterestRegex();
         break;
      default:
       throw new TestException("Unknown subscription type " + subscription); 
    }
  }
  
/**
 * Task to register interest with an interest policy with repeated calls with a
 * single key. No verification occurs after each registerInterest call.
 */
public static void HydraTask_registerInterestSingle() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   test.registerInterestSingle(false /*isPartial*/, null /*verify*/);
}

/** 
 *  Task to register interest with an interest policy with repeated calls with 
 *  a single key. This occurs after ops have executed. Verification occurs
 *  after each registerInterest call.
 */
public static void HydraTask_registerInterestSingle_RI_ops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   ExpectedRegionContents expected = null;
   if (policy.isKeysValues()) {
      expected = static_RI_ops_keysValues; 
   } else if (policy.isKeys()) {
      expected = static_RI_ops_keys;       
   } else if (policy.isNone()) {
      expected = static_RI_ops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.registerInterestSingle(false /*isPartial*/, expected); 
} 

/** 
 *  Task to register interest with an interest policy with repeated calls with 
 *  a single key. This occurs before ops have executed. Verification occurs 
 *  after each registerInterest call.  */
public static void HydraTask_registerInterestSingle_RI_noops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   ExpectedRegionContents expected = null;
   if (policy.isKeysValues()) {
      expected = static_RI_noops_keysValues; 
   } else if (policy.isKeys()) {
      expected = static_RI_noops_keys;       
   } else if (policy.isNone()) {
      expected = static_RI_noops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.registerInterestSingle(false /*isPartial*/, expected);
}

/** 
 *  Task to register interest with an interest policy with repeated calls with 
 *  a single key. This occurs after ops have executed. Verification occurs 
 *  after each registerInterest call.  */
public static void HydraTask_registerInterestSingle_ops_RI() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   ExpectedRegionContents expected = null;
   if (policy.isKeysValues()) {
      expected = static_ops_RI_keysValues; 
   } else if (policy.isKeys()) {
      expected = static_ops_RI_keys;       
   } else if (policy.isNone()) {
      expected = static_ops_RI_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.registerInterestSingle(false /*isPartial*/, expected);
}

/** 
 *  Task to register interest with an interest policy with a list.
 */
public static void HydraTask_registerInterestList() {
   subscription = LIST;
   ((InterestPolicyTest)testInstance).registerInterest(keyList);
}

/** 
 *  Task to register interest with an interest policy with a list.
 */
public static void HydraTask_registerInterestPartialList() {
   subscription = PARTIAL_LIST;
   ((InterestPolicyTest)testInstance).registerInterest(partialKeyList);
}
 
/** 
 *  Task to register interest with an interest policy with ALL_KEYS.
 */
public static void HydraTask_registerInterestAllKeys() {
   subscription = ALL_KEYS;
   ((InterestPolicyTest)testInstance).registerInterest("ALL_KEYS");
}


/** 
 *  Task to register interest with an interest policy.
 */
public static void HydraTask_registerInterestRegex() {
   subscription = REGEX;
   ((InterestPolicyTest)testInstance).registerInterestRegex();
}

/** 
 *  Task to verify the contents of a region after a load and a registerInterest,
 *  but before any ops have executed.
 *     Load
 *     RegisterInterest
 */
public static void HydraTask_verifyRegionContents_RI_noops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   ExpectedRegionContents expected = null;
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = static_RI_noops_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = static_RI_noops_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = static_RI_noops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(false /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region when ops come (after
 *  the registerInterest).
 *     Load
 *     RegisterInterest
 *     Ops
 */
public static void HydraTask_verifyRegionContents_RI_ops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   ExpectedRegionContents expected = null;
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = static_RI_ops_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = static_RI_ops_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = static_RI_ops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(false /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region when ops occur followed by
 *  a registerInterest.
 *     Load
 *     Ops
 *     RegisterInterest
 */
public static void HydraTask_verifyRegionContents_ops_RI() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   ExpectedRegionContents expected = null;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = static_ops_RI_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = static_ops_RI_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = static_ops_RI_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(false /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region.
 */
public static void HydraTask_verifyPartialRegionContents_RI_noops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   ExpectedRegionContents expected = null;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = staticPartial_RI_noops_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = staticPartial_RI_noops_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = staticPartial_RI_noops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(true /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region.
 */
public static void HydraTask_verifyPartialRegionContents_RI_ops() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   ExpectedRegionContents expected = null;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = staticPartial_RI_ops_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = staticPartial_RI_ops_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = staticPartial_RI_ops_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(true /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region.
 */
public static void HydraTask_verifyPartialRegionContents_ops_RI() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   ExpectedRegionContents expected = null;
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = staticPartial_ops_RI_keysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = staticPartial_ops_RI_keys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = staticPartial_ops_RI_none;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(true /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region for dynamic tests, which
 *  do ops concurrently with a registerInterest call. 
 */
public static void HydraTask_verifyRegionContentsDynamic() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   ExpectedRegionContents expected = test.getDynamicExpected(policy);
   test.verifyRegionContents(false /*isPartial*/, expected);
}

/** 
 *  Task to verify the contents of a region for dynamic tests, which
 *  do ops concurrently with a registerInterest call. This test used
 *  a partial list of keys to register interest.
 */
public static void HydraTask_verifyPartialRegionContentsDynamic() throws Exception {
   InterestPolicyTest test = (InterestPolicyTest)testInstance;
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }
   
   ExpectedRegionContents expected = null;
   if (InterestPolicyTest.policy.isKeysValues()) {
      expected = dynamicPartialKeysValues; 
   } else if (InterestPolicyTest.policy.isKeys()) {
      expected = dynamicPartialKeys;       
   } else if (InterestPolicyTest.policy.isNone()) {
      expected = dynamicPartialNone;       
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   test.verifyRegionContents(true /*isPartial*/, expected);
}

/** Hydra task to wait for client silence
 */
public static void HydraTask_waitForSilence() {
   SilenceListener.waitForSilence(30, 2000);
}

// ======================================================================== 
// register interest tasks

/** Repeatedly call registerInterest with a single key, along with 
 *  an interest policy. Calling registerInterest is like doing a 
 *  getInitialImage.
 *
 *  @param isPartial If true, we only registered a partial set of keys,
 *         rather than all keys used by the test.
 *         (Used only if verify is true)
 *  @param expected The expected region contents if we should verify after
 *         each registerInterest call, false otherwise.
 */
protected void registerInterestSingle(boolean isPartial, ExpectedRegionContents expected) throws Exception {
   final int NUM_CASES = 3;
   int size = aRegion.keys().size();
   Log.getLogWriter().info("Before calling register interest, region size is " + size);
   if (size != 0)
      throw new TestException("Expected region to be size 0, but it is " + size);
   Log.getLogWriter().info("Using expected " + expected);
   int sleepBeforeRegisterInterest = TestConfig.tab().intAt(HctPrms.sleepBeforeRegisterInterest, 0);
   Log.getLogWriter().info("Sleeping for " + sleepBeforeRegisterInterest + " millis");
   MasterController.sleepForMs(sleepBeforeRegisterInterest);
   StringBuffer errStr = new StringBuffer();
   for (int i = 0; i < keyList.size(); i++) {
      Object key = keyList.get(i);
      try {
         Log.getLogWriter().info("Calling registerInterest with single key of interest: " + 
                                 TestHelper.toString(key) +
                                 ", policy: " + policy);
         long start = System.currentTimeMillis();
         aRegion.registerInterest(key, policy);
         long end = System.currentTimeMillis();
//         Log.getLogWriter().info("Done calling registerInterest with single key of interest: " + 
//                                 TestHelper.toString(key) +
//                                 ", policy: " + policy +
//                                 " " + (end - start) + " millis");
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }

      if (expected != null) {
         try {
            Log.getLogWriter().info("Verifying entry for key " + key);
            verifyEntry(key, false /*isPartial*/, expected);
         } catch (TestException e) {
            // found a problem; save them up for the end
            Log.getLogWriter().info("Detected problem: " + e);
            errStr.append(e.getMessage() + "\n");
         }
      }
   }

   // report any error
   if (errStr.length() != 0) {
      final int strLimit = 1000;
      String tmp = errStr.toString();
      if (tmp.length() > strLimit) {
         Log.getLogWriter().info(errStr.toString());
         throw new TestException("Validate failed, see logs for all errors, partial errors are: " +
                              tmp.substring(0, strLimit) + "...");
      } else {
         throw new TestException(tmp);
      }
   } else {
      Log.getLogWriter().info("In registerInterestSingle, verified " + keyList.size() + " keys");
   }
}

/** Use the given keys of interest, along with an interest policy to call 
 *  registerInterest, and then check the contents of the region. Calling 
 *  registerInterest is like doing a getInitialImage.
 */
protected void registerInterest(Object keysOfInterest) {
   int size = aRegion.keys().size();
   Log.getLogWriter().info("Before calling register interest, region size is " + size);
   if (size != 0)
      throw new TestException("Expected region to be size 0, but it is " + size);
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

   if (!VmDurableId.equals("")) {
     // handle re-initialization
     if (BBoard.getInstance().getSharedMap().containsKey(VmDurableId)) {
        policy = (InterestResultPolicy)BBoard.getInstance().getSharedMap().get(VmDurableId);
     } 
     BBoard.getInstance().getSharedMap().put(VmDurableId, policy);
     Log.getLogWriter().info("Policy is " + policy.toString());

     HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(VmDurableId);
     map.put("Policy", policy);
     DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
   }

   int sleepBeforeRegisterInterest = TestConfig.tab().intAt(HctPrms.sleepBeforeRegisterInterest, 0);
   Log.getLogWriter().info("Sleeping for " + sleepBeforeRegisterInterest + " millis");
   MasterController.sleepForMs(sleepBeforeRegisterInterest);
   try {
      Log.getLogWriter().info("Calling registerInterest with keys of interest: " + 
                        TestHelper.toString(keysOfInterest) +
                        ", policy: " + policy);
      long start = System.currentTimeMillis();
      if (!VmDurableId.equals("")) {
        aRegion.registerInterest(keysOfInterest, policy, true);
        Log.getLogWriter().info("durable client invoked register interest");        
        CacheHelper.getCache().readyForEvents();
        Log.getLogWriter().info("durable client invoked readyForEvents()");
      }
      else {
        Log.getLogWriter().info("Doing non-durable register interest");
        aRegion.registerInterest(keysOfInterest, policy);
      }
      long end = System.currentTimeMillis();
      Log.getLogWriter().info("Done calling registerInterest with keys of interest: " + 
                        TestHelper.toString(keysOfInterest) +
                        ", policy: " + policy + ", time to register interest was " +
                        (end - start) + " millis");
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}


/** Use a regular expression for keys of interest, along with an interest
 *  policy to call registerInterest, and then check the
 *  contents of the region. Calling registerInterest is like doing
 *  a getInitialImage.
 */
protected void registerInterestRegex() {
   int size = aRegion.keys().size();
   Log.getLogWriter().info("Before calling register interest, region size is " + size);
   if (size != 0)
      throw new TestException("Expected region to be size 0, but it is " + size);
   int testCase = (int)(BBoard.getInstance().getSharedCounters().incrementAndRead(BBoard.testCase));
   Vector resultPolicyVec = TestConfig.tab().vecAt(HctPrms.resultPolicy);
   policy = TestHelper.getResultPolicy(
           (String)(resultPolicyVec.get(testCase % resultPolicyVec.size()))
   );
   
  String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

   if (!VmDurableId.equals("")) {
     // handle re-initialization
     if (BBoard.getInstance().getSharedMap().containsKey(VmDurableId)) {
        policy = (InterestResultPolicy)BBoard.getInstance().getSharedMap().get(VmDurableId);
     } 
     BBoard.getInstance().getSharedMap().put(VmDurableId, policy);
     Log.getLogWriter().info("Policy is " + policy.toString());

     HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(VmDurableId);
     map.put("Policy", policy);
     DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
   }

   int sleepBeforeRegisterInterest = TestConfig.tab().intAt(HctPrms.sleepBeforeRegisterInterest, 0);
   Log.getLogWriter().info("Sleeping for " + sleepBeforeRegisterInterest + " millis");
   MasterController.sleepForMs(sleepBeforeRegisterInterest);
   Object keysOfInterest = getRegex();
   try {
      Log.getLogWriter().info("Calling registerInterestRegex with keys of interest: " + 
                              TestHelper.toString(keysOfInterest) +
                              ", policy: " + policy);
      long start = System.currentTimeMillis();
      if (!VmDurableId.equals("")) {
        aRegion.registerInterestRegex((String)keysOfInterest, policy, true);
        Log.getLogWriter().info("durable client invoked register interest");        
        CacheHelper.getCache().readyForEvents();
        Log.getLogWriter().info("durable client invoked readyForEvents()");
      }
      else {
        Log.getLogWriter().info("Doing non-durable register interest");
        aRegion.registerInterestRegex((String)keysOfInterest, policy);
      }
      
      long end = System.currentTimeMillis();
      Log.getLogWriter().info("Calling registerInterestRegex with keys of interest: " + 
                              TestHelper.toString(keysOfInterest) +
                              ", policy: " + policy + ", time to register interest was " +
                              (end - start) + " millis");
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

// ======================================================================== 
// other methods

/** Return a regular expression that will match any object
 *  name from util.NameFactory.
 */
protected String getRegex() {
   String regex = "[a-zA-Z]+_[0-9]+";
   return regex;
}

/** Return the expected contents of the region for dynamic tests.
 *
 *  @param policy The InterestResultPolicy of a registerInterest call.
 */  
protected ExpectedRegionContents getDynamicExpected(InterestResultPolicy policy) {
   ExpectedRegionContents expected;
   if (policy.isKeysValues()) {
      expected = dynamicKeysValues;
   } else if (policy.isKeys()) {
      expected = dynamicKeys;
   } else if (policy.isNone()) {
      expected = dynamicNone;
   } else {
     throw new TestException("Unknown policy " + policy);
   } 
   Log.getLogWriter().info("Verifying with " + expected);
   return expected;
}

// ======================================================================== 
// verification methods 

/** Verify the contents of the region using ExectedRegionContents.
 *  Throw an error of any problems are detected.
 *
 *  @param isPartial If true, we only registered a partial set of keys,
 *         rather than all keys used by the test.
 *  @param expected The expected keys/values for the region
 */ 
public void verifyRegionContents(boolean isPartial, 
                                 ExpectedRegionContents expected) throws Exception {
   StringBuffer errStr = new StringBuffer();

   // check region size
   long numKeys = aRegion.keys().size();
   Log.getLogWriter().info("In verifyRegionContents, region has " + numKeys + " keys " + expected);
   int expectedNumKeys = -1;
   if (expected.exactSize() != null) {// specified an exact size
      expectedNumKeys = expected.exactSize().intValue();
      Log.getLogWriter().info("Expecting exact size of region " + expectedNumKeys);
      if (expectedNumKeys != numKeys) {
         String tmpStr = "Expected " + expectedNumKeys + " keys, but there are " + numKeys;
         Log.getLogWriter().info(tmpStr);
         errStr.append(tmpStr + "\n");
      }
   } else {
      if ((expected.minSize() != null) && (expected.maxSize() != null)) {
         int minSize = expected.minSize().intValue();
         int maxSize = expected.maxSize().intValue();
         Log.getLogWriter().info("Expecting min region size " + minSize + ", max region size " + maxSize);
         if ((numKeys < minSize) || (numKeys > maxSize)) {
            String tmpStr = "Expected a minimum of " + minSize + " keys, maximum of " + maxSize +
                            ", but there are " + numKeys;
            Log.getLogWriter().info(tmpStr);
            errStr.append(tmpStr + "\n");
         }
      } else {
         Log.getLogWriter().info("Not checking size of region, no size specified in " + expected);
      }
   }
/*
   if (errStr.length() > 0) {
      StringBuffer aStr = new StringBuffer();
      Iterator it = aRegion.keys().iterator();
      int count = 0;
      while (it.hasNext()) {
         aStr.append(it.next() + " ");
         if ((++count % 10) == 0) 
            aStr.append("\n");
      }
      Log.getLogWriter().info("Keys in region: " + aStr);
   }
*/

   // iterate keys
   Set keysToCheck = new HashSet(keyList); // check all keys in the keyList (including "extra" keys not in server)
   keysToCheck.addAll(aRegion.keys());     // also check any keys in the region that are not in the keyList
   Iterator it = keysToCheck.iterator();
   while (it.hasNext()) {
      Object key = it.next();
      try {
         verifyEntry(key, isPartial, expected);
      } catch (TestException e) {
         Log.getLogWriter().info(TestHelper.getStackTrace(e));
         errStr.append(e.getMessage() + "\n");
      }
   }

   InitImageBB.getBB().printSharedCounters();
   NameBB.getBB().printSharedCounters();

   // report any error
   if (errStr.length() != 0) {
      final int strLimit = 1000;
      String tmp = errStr.toString();
      if (tmp.length() > strLimit) {
         Log.getLogWriter().info(errStr.toString());
         throw new TestException("Validate failed, see logs for all errors, partial errors are: " +
                                 tmp.substring(0, strLimit) + "...");
      } else {
         throw new TestException(tmp);
      }
   } else {
      Log.getLogWriter().info("In verifyRegionContentsKeysValues, verified region with " + numKeys + " keys");
   }
}

/** Verify a single entry in aRegion with the given key
 *  and interest policy KEYS_VALUES.
 *
 *  @param key The key to verify
 *  @param isPartial If true, we only registered even numbered keys.
 *  @param expected The expected keys/values of the region
 */
protected void verifyEntry(Object key, 
                           boolean isPartial, 
                           ExpectedRegionContents expected) throws Exception {
   long i = NameFactory.getCounterForName(key);
   if (isPartial && (!partialKeyList.contains(key))) { // key was not registered, so it shouldn't be here
      checkContainsKey(key, false, "key was not registered");
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.NONE)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.NONE))) {
      checkContainsKey(key, expected.containsKey_none(), "key was untouched");
      checkContainsValueForKey(key, expected.containsValue_none(), "key was untouched");
      if (expected.getAllowed_none()) {
         Object value = aRegion.get(key);
         checkValue(key, value);
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.GET)) &&
               (i <= keyIntervals.getLastKey(KeyIntervals.GET))) {
      // this key was untouched after its creation
      checkContainsKey(key, expected.containsKey_get(), "get key");
      checkContainsValueForKey(key, expected.containsValue_get(), "get key");
      if (expected.getAllowed_get()) {
         Object value = aRegion.get(key);
         checkValue(key, value);
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
      checkContainsKey(key, expected.containsKey_invalidate(), "key was invalidated (Bug 35303)");
      Boolean expectValue = expected.containsValue_invalidate();
      checkContainsValueForKey(key, expectValue, "key was invalidated (Bug 35303)");
      if (expected.getAllowed_invalidate()) {
         Object value = aRegion.get(key);
         if ((expectValue != null) && (expectValue.booleanValue())) {
            checkValue(key, value);   
         } else {
            if (value != null) {
               throw new TestException("Bug 35303, after calling get(" + key + "), expected invalidated value to be null but it is " + TestHelper.toString(value));
            }
         }
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
      // this key was locally invalidated
      checkContainsKey(key, expected.containsKey_localInvalidate(), "key was locally invalidated");
      checkContainsValueForKey(key, expected.containsValue_localInvalidate(), "key was locally invalidated");
      if (expected.getAllowed_localInvalidate()) {
         Object value = aRegion.get(key);
         checkValue(key, value);
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.DESTROY)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
      // this key was destroyed
      checkContainsKey(key, expected.containsKey_destroy(), "key was destroyed");
      Boolean expectValue = expected.containsValue_destroy();
      checkContainsValueForKey(key, expectValue, "key was destroyed");
      if (expected.getAllowed_destroy()) {
         Object value = aRegion.get(key);
         if ((expectValue != null) && (expectValue.booleanValue())) {
            checkValue(key, value);   
         } else {
            if (value != null) {
               throw new TestException("Expected value for " + key + " to be null");
            }
         }
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
      // this key was locally destroyed
      checkContainsKey(key, expected.containsKey_localDestroy(), "key was locally destroyed");
      checkContainsValueForKey(key, expected.containsValue_localDestroy(), "key was locally destroyed");
      if (expected.getAllowed_localDestroy()) {
         Object value = aRegion.get(key);
         checkValue(key, value);
      }
   } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
      // this key was updated
      checkContainsKey(key, expected.containsKey_update(), "key was updated");
      checkContainsValueForKey(key, expected.containsValue_update(), "key was updated");
      if (expected.getAllowed_update()) {
         Object value = aRegion.get(key);
         if (expected.valueIsUpdated()) {
            checkUpdatedValue(key, value);
         } else {
            checkValue(key, value);
         }
      }
   } else if ((i > keyIntervals.getNumKeys()) && (i <= (keyIntervals.getNumKeys() + numNewKeys))) {
      // key was newly added
      checkContainsKey(key, expected.containsKey_newKey(), "key was new");
      checkContainsValueForKey(key, expected.containsValue_newKey(), "key was new");
      if (expected.getAllowed_newKey()) {
         Object value = aRegion.get(key);
         checkValue(key, value);
      }
   } else { // key is outside of keyIntervals and new keys; it was never loaded 
      // key was never loaded
      checkContainsKey(key, false, "key was never used");
      checkContainsValueForKey(key, false, "key was never used");
   }
}

/** Check that containsKey() called on the region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsKey, or null if we cannot
 *         expect any particular result.
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkContainsKey(Object key, Boolean expected, String logStr) {
   if (expected != null) {
      boolean bool = expected.booleanValue();
      boolean containsKey = aRegion.containsKey(key);
      if (containsKey != bool) {
         throw new TestException("Expected containsKey(" + key + ") to be " + expected + 
                   ", but it was " + containsKey + ": " + logStr);
      }
   }
}

/** Check that containsValueForKey() called on the region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsKey, or null if we cannot
 *         expect any particular result.
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkContainsValueForKey(Object key, Boolean expected, String logStr) {
   if (expected != null) {
      boolean bool = expected.booleanValue();
      boolean containsValue = aRegion.containsValueForKey(key);
      if (containsValue != bool) {
         throw new TestException("Expected containsValueForKey(" + key + ") to be " + expected + 
                   ", but it was " + containsValue + ": " + logStr);
      }
   }
}

  /**
   * Hydra task for recycling the server
   * 
   * @throws ClientVmNotFoundException
   */
  public static void killServer() throws ClientVmNotFoundException {

    Set dead;
    Set active;

    Region region = RegionHelper.getRegion(REGION_NAME);

    active = ClientHelper.getActiveServers(region);
       
    int minServersRequiredAlive = 3;

    if (active.size() < minServersRequiredAlive) {
      Log.getLogWriter().info(
          "No kill executed , a minimum of " + minServersRequiredAlive
              + " servers have to be kept alive");
      return;
    }

    long now = System.currentTimeMillis();
    Long lastKill = (Long)BBoard.getInstance().getSharedMap().get(
        "lastKillTime");
    long diff = now - lastKill.longValue();

    if (diff < killInterval) {
      Log.getLogWriter().info("No kill executed");
      return;
    }
    else {
      BBoard.getInstance().getSharedMap().put("lastKillTime", new Long(now));
    }

    List endpoints = BridgeHelper.getEndpoints();
    int index = rand.nextInt(endpoints.size() - 1);
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints
        .get(index);
    ClientVmInfo target = new ClientVmInfo(endpoint);
    target = ClientVmMgr.stop("Killing random cache server",
        ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, target);
    Log.getLogWriter().info("Server Killed : " + target);

    int sleepSec = TestConfig.tab().intAt(DurableClientsPrms.restartWaitSec, 1);
    Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds");
    MasterController.sleepForMs(sleepSec * 1000);

		active = ClientHelper.getActiveServers(region);
		List<ServerLocation> list = new ArrayList<ServerLocation>(active);
		ServerLocation server = new ServerLocation(endpoint.getHost(),
				endpoint.getPort());
		String serverHost = server.getHostName();
		int serverPort = server.getPort();
		String server1 = serverHost + ":" + serverPort;

		if (isAvailable(server1, active, list)) {
			Log.getLogWriter().info(
					"ERROR: Killed server " + server
							+ " found in Active Server List: " + active);
		}
		ClientVmMgr.start("Restarting the cache server", target);

		int sleepMs = ClientHelper.getRetryInterval(region) + 1000;
		Log.getLogWriter().info("Sleeping for " + sleepMs + " ms");
		MasterController.sleepForMs(sleepMs);

		active = ClientHelper.getActiveServers(region);
		list = new ArrayList<ServerLocation>(active);
		if (isAvailable(server1, active, list)) {
			Log.getLogWriter().info(
					"Restarted server " + server
							+ " found in Active Server List: " + active);
		}
		return;
	}

	private static boolean isAvailable(String server1, Set active,
			List<ServerLocation> list) {
		boolean isAvailable = false;
		for (int i = 0; i < list.size(); i++) {
			String hostName = list.get(i).getHostName();
			int port = list.get(i).getPort();
			String serverName = hostName.substring(0, hostName.indexOf("."))
					+ ":" + port;
			if (serverName.equalsIgnoreCase(server1)) {
				isAvailable = true;
				break;
			}
		}
		return isAvailable;
	}

  /**
   * Task to register interest (nondurable) for a durable client with an
   * interest policy with a list.
   */
  public static void HydraTask_nondurable_registerInterestList() {
    ((InterestPolicyTest)testInstance).nondurable_registerInterest(keyList);
  }

  /**
   * Task to register interest (nondurable) for a durable client with an
   * interest policy with a list.
   */
  public static void HydraTask_nondurable_registerInterestPartialList() {
    ((InterestPolicyTest)testInstance)
        .nondurable_registerInterest(partialKeyList);
  }

  /**
   * Task to register interest (nondurable) for a durable client with an
   * interest policy with ALL_KEYS.
   */
  public static void HydraTask_nondurable_registerInterestAllKeys() {
    ((InterestPolicyTest)testInstance).nondurable_registerInterest("ALL_KEYS");
  }

  /**
   * Task to register interest (nondurable) for a durable client with an
   * interest policy.
   */
  public static void HydraTask_nondurable_registerInterestRegex() {
    ((InterestPolicyTest)testInstance).nondurable_registerInterestRegex();
  }

  /**
   * Used for non_durable registerInterest for durable clients
   */
  protected void nondurable_registerInterest(Object keysOfInterest) {
    int size = aRegion.keys().size();
    Log.getLogWriter().info(
        "Before calling register interest, region size is " + size);

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }

    int sleepBeforeRegisterInterest = TestConfig.tab().intAt(
        HctPrms.sleepBeforeRegisterInterest, 0);
    Log.getLogWriter().info(
        "Sleeping for " + sleepBeforeRegisterInterest + " millis");
    MasterController.sleepForMs(sleepBeforeRegisterInterest);
    try {
      Log.getLogWriter().info(
          "Calling registerInterest with keys of interest: "
              + TestHelper.toString(keysOfInterest) + ", policy: " + policy);
      long start = System.currentTimeMillis();
      aRegion.registerInterest(keysOfInterest, policy);
      long end = System.currentTimeMillis();
      Log.getLogWriter().info(
          "Done calling registerInterest with keys of interest: "
              + TestHelper.toString(keysOfInterest) + ", policy: " + policy
              + ", time to register interest was " + (end - start) + " millis");
    }
    catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Used for non durable register interest of durable clients
   */
  protected void nondurable_registerInterestRegex() {
    int size = aRegion.keys().size();
    Log.getLogWriter().info(
        "Before calling register interest, region size is " + size);

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      InterestPolicyTest.policy = (InterestResultPolicy)BBoard.getInstance()
          .getSharedMap().get(VmDurableId);
      Log.getLogWriter().info(
          "Policy is " + InterestPolicyTest.policy.toString());
    }

    int sleepBeforeRegisterInterest = TestConfig.tab().intAt(
        HctPrms.sleepBeforeRegisterInterest, 0);

    Log.getLogWriter().info(
        "Sleeping for " + sleepBeforeRegisterInterest + " millis");
    MasterController.sleepForMs(sleepBeforeRegisterInterest);
    Object keysOfInterest = getRegex();
    try {
      Log.getLogWriter().info(
          "Calling registerInterestRegex with keys of interest: "
              + TestHelper.toString(keysOfInterest) + ", policy: " + policy);
      long start = System.currentTimeMillis();
      aRegion.registerInterestRegex((String)keysOfInterest, policy);
      long end = System.currentTimeMillis();
      Log.getLogWriter().info(
          "Calling registerInterestRegex with keys of interest: "
              + TestHelper.toString(keysOfInterest) + ", policy: " + policy
              + ", time to register interest was " + (end - start) + " millis");
    }
    catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Task to unregister interest for a durable client with a list.
   */
  public static void HydraTask_unRegisterInterestList() {
    ((InterestPolicyTest)testInstance).unRegisterInterest(keyList);
  }

  /**
   * Task to unregister interest for a durable client with a partial list.
   * 
   */
  public static void HydraTask_unRegisterInterestPartialList() {
    ((InterestPolicyTest)testInstance).unRegisterInterest(partialKeyList);
  }

  /**
   * Task to unregister interest for a durable client with ALL_KEYS.
   */
  public static void HydraTask_unRegisterInterestAllKeys() {
    ((InterestPolicyTest)testInstance).unRegisterInterest("ALL_KEYS");
  }

  /**
   * Task to unregister interest (Regex) for a durable client
   */
  public static void HydraTask_unRegisterInterestRegex() {
    ((InterestPolicyTest)testInstance).unRegisterInterestRegex();
  }

  /**
   * Used for unregisterInterest for durable clients
   */
  protected void unRegisterInterest(Object keysOfInterest) {
    int size = aRegion.keys().size();
    Log.getLogWriter().info(
        "Before calling unregister interest, region size is " + size);

    int sleepBeforeRegisterInterest = TestConfig.tab().intAt(
        HctPrms.sleepBeforeRegisterInterest, 0);
    Log.getLogWriter().info(
        "Sleeping for " + sleepBeforeRegisterInterest + " millis");
    MasterController.sleepForMs(sleepBeforeRegisterInterest);
    try {
      Log.getLogWriter().info(
          "Calling unregisterInterest with keys of interest: "
              + TestHelper.toString(keysOfInterest));
      long start = System.currentTimeMillis();
      //unregistering with the durable option
      aRegion.unregisterInterest(keysOfInterest);
      long end = System.currentTimeMillis();
      Log.getLogWriter().info(
          "Done calling unregisterInterest with keys of interest: "
              + TestHelper.toString(keysOfInterest)
              + ", time to un register interest was " + (end - start)
              + " millis");
    }
    catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Used for unregister interest (Regex) of durable clients
   */
  protected void unRegisterInterestRegex() {
    int size = aRegion.keys().size();
    Log.getLogWriter().info(
        "Before calling unregister interest, region size is " + size);

    int sleepBeforeRegisterInterest = TestConfig.tab().intAt(
        HctPrms.sleepBeforeRegisterInterest, 0);

    Log.getLogWriter().info(
        "Sleeping for " + sleepBeforeRegisterInterest + " millis");
    MasterController.sleepForMs(sleepBeforeRegisterInterest);
    Object keysOfInterest = getRegex();
    try {
      Log.getLogWriter().info(
          "Calling unregisterInterestRegex with keys of interest: "
              + TestHelper.toString(keysOfInterest));
      long start = System.currentTimeMillis();
      //for durable clients
      aRegion.unregisterInterestRegex((String)keysOfInterest);
      long end = System.currentTimeMillis();
      Log.getLogWriter().info(
          "Calling unregisterInterestRegex with keys of interest: "
              + TestHelper.toString(keysOfInterest)
              + ", time to un register interest was " + (end - start)
              + " millis");
    }
    catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * The task used for verifying that the region contents after the Unregister
   * operation.
   * 
   */

  public static void HydraTask_verifyRegionContents_UR() {
    ((InterestPolicyTest)testInstance).verifyRegionContents_UR();
  }

  /**
   * Used for verifying that the region is empty. This is used in a situation
   * where the restarted client does an unregister operation on all its durably
   * subscribed keys. No Feeder operations done prior to this.
   * 
   */
  protected void verifyRegionContents_UR() {
    int size = aRegion.keys().size();
    Log.getLogWriter().info("The region size is " + size);
    if (size != 0) {
      throw new TestException(" The region expected to be empty but has "
          + size + " entries");
    }

  }
  
  /**
   * Used for event validation for the interest policy tests.
   */

  public static void validateEventCounters() {
    //DurableClientsBB.getBB().printSharedMap();

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    HashMap currentVmMap = (HashMap)DurableClientsBB.getBB().getSharedMap()
        .get(VmDurableId);

    if (currentVmMap.isEmpty()) {
      throw new TestException("The map of threads is empty for the Vm " + VmDurableId);
    }

    int currentVmCrashCount = ((Integer)currentVmMap.get("CRASH COUNT"))
        .intValue();

    HashSet currentVmSet = (HashSet)currentVmMap.get("OPS KEYS : ");

    // If interestPolicy is NONE, it is possible to not have processed any events
    InterestResultPolicy currentPolicy = (InterestResultPolicy)currentVmMap.get("Policy");
    if (currentVmSet == null) {
      if (currentPolicy.equals(InterestResultPolicy.NONE)) {
        currentVmSet = new HashSet();
      } else {
        throw new TestException(VmDurableId + " with InterestResultPolicy." + currentPolicy + " did not process any events.");
      } 
    }

    Iterator iterator = ((Map)DurableClientsBB.getBB().getSharedMap().getMap())
        .entrySet().iterator();
    Map.Entry entry = null;
    Object key = null;
    Object value = null;

    Log.getLogWriter().info(VmDurableId + " with resultPolicy " + currentVmMap.get("Policy") + " and " + currentVmCrashCount + " killClient tasks processed events for " + currentVmSet.size() + " entries");
    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = entry.getKey();
      value = entry.getValue();

      HashMap otherVmMap = (HashMap)value;

      int otherVmCrashCount = ((Integer)otherVmMap.get("CRASH COUNT"))
          .intValue();

      HashSet otherVmSet = (HashSet)otherVmMap.get("OPS KEYS : ");
      // If interestPolicy is NONE, it is possible to not have processed any events
      InterestResultPolicy otherPolicy = (InterestResultPolicy)otherVmMap.get("Policy");
      if (otherVmSet == null) {
        if (otherPolicy.equals(InterestResultPolicy.NONE)) {
          otherVmSet = new HashSet();
        } else {
          throw new TestException(key + "with InterestResultPolicy." + otherPolicy + " did not process any events.");
        } 
      }

      if (currentVmCrashCount != 0 && otherVmCrashCount == 0) {
        if (otherVmMap.get("Policy") != null) {

          if ((otherVmMap.get("Policy")).equals(currentVmMap.get("Policy"))) {
          Log.getLogWriter().info(key + " with resultPolicy " + otherVmMap.get("Policy") + " and " + otherVmCrashCount + " killClient tasks processed events for " + otherVmSet.size() + " entries");

            if (currentVmSet.size() == otherVmSet.size()) {
              Log.getLogWriter().info(
                  " Both " + VmDurableId + " and " + key
                      + " processed events for " + otherVmSet.size() + " entries (as expected).");
            }
            else {
              StringBuffer aStr = new StringBuffer();

/* Evidently, after the client is recycled the HARegionQueue is completely played out without consideration
 * for ResultInterestPolicy or InterestPolicy (e.g. CACHE_CONTENT), so extra keys cannot be flagged as an issue
 * For example, initially with InterestResultPolicy.NONE and Region.InterestPolicy(CACHE_CONTENT) we won't have
 * any entries in the region ... yet after the durable client is recycled, we process everything updates, destroys and tx invalidates.  See BUG 43202.
 * Therefore, I'm only logging the keys for unexpected events
 */

              Set extraEvents = new HashSet(currentVmSet);
              extraEvents.removeAll(otherVmSet);
              if (extraEvents.size() != 0) {
                Log.getLogWriter().info(VmDurableId + " processed " + extraEvents.size() + " (extra) event entries " + extraEvents + "\n");
              }

              Set missingEvents = new HashSet(otherVmSet);
              missingEvents.removeAll(currentVmSet);
              if (missingEvents.size() != 0) {
                aStr.append(VmDurableId + " missed events for " + missingEvents.size() + " entries " + missingEvents);
                aStr.append(VmDurableId + " processed events for " + currentVmSet.size() + " entries and " + key + " processed events for " + otherVmSet.size() + " entries.  Both have resultPolicy " + currentVmMap.get("Policy") + "\n");
                throw new TestException(aStr.toString());
              }
            }
          }
        }
      }
    }
  }
}
