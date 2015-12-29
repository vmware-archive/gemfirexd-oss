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
package parReg.tx;

import parReg.ParRegPrms;
//import parReg.execute.PartitionObjectHolder;

import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.*;

import java.util.*;

import util.*;

/**
 * A simple test class that supports executing entry operations on a PartitionRegion
 * while using a colocated regions.
 *
 * For PartitionedRegion tx testing, the keySet is set by getLocal(Remote)KeySet.
 * KeySet determined via function execution and PartitionedRegionHelper.getLocalDataSet
 * to target entries in either a local or remote VMs). 
 *
 * Tasks available (executed via FunctionExecution):
 * - executeGetAllMembersInDS() - uses FunctionService.onMembers(ds) to get a list of 
 *   (Serialized)DistributedMembers
 * - getKeySet() - using FunctionService.onRegion() with 
 * PartitionedRegionHelper.getLocalDataSet to retrieve potential keySet for 
 * entry operations on members.  If PrTxPrms-useLocalKeySet is true (default),
 * it will return the keySet for the calling VM, otherwise, it will select a 
 * keySet for a remote VM.
 *  
 * @see PrTxPrms-useLocalKeySet
 * @see PrTxPrms-numColocatedRegions 
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.1
 */
public class ColocatedTxTest extends util.OperationsClient {

  // Single instance in this VM
  static protected ColocatedTxTest testInstance;
  static Region baseRegion;

  private ArrayList errorMsgs = new ArrayList();
  private ArrayList errorException = new ArrayList();

    /* 
     * Initialize BridgeServer as an OperationsClient
     */
    public synchronized static void HydraTask_initializeBridgeServer() {
       if (testInstance == null) {
          testInstance = new ColocatedTxTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeServer();
          testInstance.registerFunctions();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeServer() {
       // create cache/colocated PR
       if (CacheHelper.getCache() == null) {
         initialize();
         BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
       }
    }

    public synchronized static void HydraTask_initializeBridgeClient() {
       if (testInstance == null) {
          testInstance = new ColocatedTxTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeClient();
          testInstance.registerFunctions();
       }
    }

    /* 
     * Creates cache and region for edgeClients (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeClient() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         String regionConfig = ConfigPrms.getRegionConfig();

         AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
         RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
         String regionBase = rd.getRegionName();

         int numRegions = PrTxPrms.getNumColocatedRegions();
         for (int i = 0; i < numRegions; i++) {
            String regionName = regionBase + "_" + (i+1);
            Region aRegion = RegionHelper.createRegion(regionName, aFactory);
            aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
            Log.getLogWriter().info(aRegion.getName() + ": Registered interest in ALL_KEYS");
         }
       }
    }

  /**
   *  Create the cache and Region (for peers).  Check for forced disconnects (for gii tests)
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ColocatedTxTest();
      testInstance.initializeOperationsClient();

      try {
        testInstance.initialize();
        testInstance.registerFunctions();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }  
    }
  }

  /* 
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  protected void initialize() {

    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
       Cache aCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());

       String regionConfig = ConfigPrms.getRegionConfig();
       AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       String regionBase = rd.getRegionName();
   
       // override colocatedWith in the PartitionAttributes
       PartitionDescription pd = rd.getPartitionDescription();
       PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
       PartitionAttributes prAttrs = null;
   
       String colocatedWith = null;
       int numRegions = PrTxPrms.getNumColocatedRegions();
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
  *  Creates initial set of entries across colocated regions
  *  (non-transactional).
  */
  public static void HydraTask_populateRegions() {
     testInstance.populateRegions();
  }

  protected void populateRegions() {
     // prime the PartitionRegion by getting at least one entry in each bucket
     Set regions = CacheHelper.getCache().rootRegions();
     Region sampleRegion = (Region)regions.iterator().next();
     int numBuckets = sampleRegion.getAttributes().getPartitionAttributes().getTotalNumBuckets() * TestHelper.getNumVMs()-1;

     for (int i = 0; i < numBuckets; i++) { 
        Object key = NameFactory.getNextPositiveObjectName();

        // create this same key in each region
        Set rootRegions = CacheHelper.getCache().rootRegions();
        for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
           Region aRegion = (Region)it.next();
           BaseValueHolder anObj = getValueForKey(key);

           Log.getLogWriter().info("populateRegion: calling create for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.create(key, anObj);
            Log.getLogWriter().info("populateRegion: done creating key " + key);
        }
     }
  }

  protected void registerFunctions() {

    Function f = new GetAllMembersInDS();
    FunctionService.registerFunction(f);

    f = new GetKeySet();
    FunctionService.registerFunction(f);

    Log.getLogWriter().info("Registered functions: " + FunctionService.getRegisteredFunctions());
  }

  /**
   *  Performs puts/gets on entries in TestRegion
   */
  public static void HydraTask_doEntryOperations() {
    try {
       testInstance.doEntryOperations();
    } catch (Exception e) {
       Log.getLogWriter().info("doEntryOperations threw Exception " + e);
       throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
    }
}

  protected void doEntryOperations() {
    TestHelper.checkForEventError(PrTxBB.getBB());
  
    // Select a random region
    Object[] rootRegions = CacheHelper.getCache().rootRegions().toArray();
    int index = TestConfig.tab().getRandGen().nextInt(0, rootRegions.length-1);
    Region aRegion = (Region)rootRegions[index];

    // Set a keySet (specific to a random member) for this round of execution
    // getExistingKey and getNewKeyForRegion depend on this being set in the BB
    KeySetResult keySetResult = getKeySet(aRegion);
    String mapKey = PrTxBB.keySet + RemoteTestModule.getCurrentThread().getThreadId();
    PrTxBB.getBB().getSharedMap().put(mapKey, keySetResult);
    DistributedMember keySetDM = keySetResult.getDistributedMember();
    List keyList = (List)keySetResult.getKeySet();
    Object[] keySet = keyList.toArray();
    Log.getLogWriter().info("KeySet for " + keySetDM + "  = " + keyList);

    if (PrTxPrms.sameKeyColocatedRegions()) {
       Log.getLogWriter().info("Executing ops with single key across colocatedRegions");
       doEntryOperations(keySet);
    } else {
       Log.getLogWriter().info("Executing ops with keySet on Region");
       doEntryOperations(aRegion);
    }
  }

  protected KeySetResult getKeySet(Region aRegion) {
    KeySetResult ksResult = null;

    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();
    boolean useLocalKeySet = PrTxPrms.useLocalKeySet();

    Function f = FunctionService.getFunction("parReg.tx.GetKeySet");
    Execution e = FunctionService.onRegion(aRegion).withArgs(dm.toString());

    Log.getLogWriter().info("executing " + f.getId() + " on region " + aRegion.getName());
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult(); 
    DistributedMember remoteDM = null;
    boolean keySetAvailable = false;
    for (Iterator rit = results.iterator(); rit.hasNext(); ) {
       ksResult = (KeySetResult)rit.next();
       remoteDM = ksResult.getDistributedMember();

       if (useLocalKeySet) {
          if (dm.equals(remoteDM)) {
             // we've got a keySet for the local dataStore!
             keySetAvailable = true;
             Log.getLogWriter().info("Returning keySet for local dataStore");
             break;
          }
       } else {
          if (!dm.equals(remoteDM)) {
             // we've got a keySet for a remote dataStore!
             keySetAvailable = true;
             Log.getLogWriter().info("Returning keySet for remote dataStore");
             break;
          }
       }
    }

    if (!keySetAvailable) {
       ksResult = null; 
       throw new TestException("Test issue with getNewKey -- no keySet found");
    } else {
       Log.getLogWriter().info("Returning keySet from member " + remoteDM);
    }
    return ksResult;
  }

  protected List executeGetAllMembersInDS() {
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
    DistributedMember dm = ds.getDistributedMember();

    Function f = FunctionService.getFunction("parReg.tx.GetAllMembersInDS");
    Execution e = FunctionService.onMembers(ds).withArgs(dm.toString());

    Log.getLogWriter().info("executing " + f.getId());
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());

    List results = (List)rc.getResult(); 
    Log.getLogWriter().info("ResultCollector.getResult() = " + results);

    StringBuffer s = new StringBuffer();
    s.append("ResultCollector : \n");
    ArrayList dmList = new ArrayList();
    for (Iterator it = results.iterator(); it.hasNext(); ) {
       SerializableDistributedMember sdm = (SerializableDistributedMember)it
          .next(); 
       s.append("   " + sdm.toString() + "\n");
       dmList.add(sdm.getDistributedMember());
    }
    Log.getLogWriter().info(s.toString());
    return (dmList);
  }

//===========================================================
// Override OperationsClient methods 
// (for implementing PartitionResolver in Key or Callback)
//===========================================================

   /** Add a new entry to the given region.
    *
    *  @param aRegion The region to use for adding a new entry.
    */
   protected void addEntry(Region aRegion) {
      Object key = getNewKeyForRegion(aRegion);
      BaseValueHolder anObj = getValueForKey(key);

      Object callback = getCallback(key, createCallbackPrefix, ProcessMgr.getProcessId());
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
         Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " + TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " + aRegion.getFullPath());
         aRegion.create(key, anObj, callback);
         Log.getLogWriter().info("addEntry: done creating key " + key);
      } else { // use a put call
         Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " + TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
         aRegion.put(key, anObj, callback);
         Log.getLogWriter().info("addEntry: done putting key " + key);
      }
   }

   /** Add a new entry to the given region using the ConcurrentMap putIfAbsent api
    *
    *  @param aRegion The region to use for adding a new entry.
    */
   protected void putIfAbsent(Region aRegion) {
      putIfAbsent(aRegion, true);
   }

   protected void putIfAbsent(Region aRegion, boolean logAddition) {
      Object key = getNewKeyForRegion(aRegion);
      BaseValueHolder anObj = getValueForKey(key);

      if (logAddition) {
         Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
      }
      aRegion.putIfAbsent(key, anObj);
      Log.getLogWriter().info("putIfAbsent: done creating key " + key);
   }
       
   /** Invalidate an entry in the given region.
    *
    *  @param aRegion The region to use for invalidating an entry.
    *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
    */
   protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }
      try {
         Object callback = getCallback(key, invalidateCallbackPrefix, ProcessMgr.getProcessId());
         if (isLocalInvalidate) { // do a local invalidate
            Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback);
            aRegion.localInvalidate(key, callback);
            Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
         } else { // do a distributed invalidate
            Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback);
            aRegion.invalidate(key, callback);
            Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
         } 
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Destroy an entry in the given region.
    *
    *  @param aRegion The region to use for destroying an entry.
    *  @param isLocalDestroy True if the destroy should be local, false otherwise.
    */
   protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      try {
         Object callback = getCallback(key, destroyCallbackPrefix, ProcessMgr.getProcessId());
         if (isLocalDestroy) { // do a local destroy
            Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback);
            aRegion.localDestroy(key, callback);
            Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
         } else { // do a distributed destroy
            Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback);
            aRegion.destroy(key, callback);
            Log.getLogWriter().info("destroyEntry: done destroying key " + key);
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }

   /** Remove an entry in the given region using the ConcurrentMap remove method.
    *
    *  @param aRegion The region to use for removing an entry.
    *  
    */
   protected void remove(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }

      try {
         Log.getLogWriter().info("remove: removing key " + key);
         aRegion.remove(key, aRegion.get(key));
         Log.getLogWriter().info("remove: done removing key " + key);
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }

   /** Update an existing entry in the given region. If there are
    *  no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void updateEntry(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }
 
      BaseValueHolder anObj = null;
      anObj = getUpdateObject(aRegion, (String)key);
      Object callback = getCallback(key, updateCallbackPrefix, ProcessMgr.getProcessId());
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
      TestHelper.toString(anObj) + ", callback is " + callback);
      aRegion.put(key, anObj, callback);
      Log.getLogWriter().info("Done with call to put (update)");
   }

   /** Remove an existing entry in the given region. If there are
    *  no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void replace(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }
       
      BaseValueHolder anObj = null;
      anObj = getUpdateObject(aRegion, (String)key);
      BaseValueHolder oldValue = (BaseValueHolder)aRegion.get(key);
      Log.getLogWriter().info("replace: replacing key " + key + " value " + TestHelper.toString(oldValue) + " with " + TestHelper.toString(anObj));
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use previousValue api
         aRegion.replace(key, oldValue, anObj);
      } else {
         aRegion.replace(key, anObj);
      }
      Log.getLogWriter().info("Done with call to replace");
   }

   /** Get an existing key in the given region if one is available,
    *  otherwise get a new key. 
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getKey(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) { // no existing keys; get a new key then
         return;
      }
      Object callback = getCallback(key, getCallbackPrefix, ProcessMgr.getProcessId());
      Object anObj = null;
      Log.getLogWriter().info("getKey: getting key " + key + ", callback is " + callback);
      anObj = aRegion.get(key, callback);
      Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
   }
       
   /** Get a new key in the given region.
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getNewKey(Region aRegion) {
      Object key = getNewKeyForRegion(aRegion);
      Object callback = getCallback(key, getCallbackPrefix, ProcessMgr.getProcessId());
      Object anObj = null;
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback);
      anObj = aRegion.get(key, callback);
      Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
   }

  private Object getCallback(Object key, String prefix, int pid) {
     String sCallback = prefix + pid;
     return sCallback;
  }

   /* getExistingKeys
    * get numKeysToGet keys from keySets (so all will be colocated) 
    *
    *  @param aRegion The region to use for getting an entry.
    *  @numKeysToGet get numKeysToGet keys from the keySets on the BB
    */
   protected List getExistingKeys(Region aRegion, int numKeysToGet) {
     Log.getLogWriter().info("Trying to get " + numKeysToGet + " existing keys...");

     // get thread specific keySet from BB
     String mapKey = PrTxBB.keySet + RemoteTestModule.getCurrentThread().getThreadId();
     KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
     DistributedMember keySetDM = keySetResult.getDistributedMember();
     List aList = keySetResult.getKeySet();
     Object[] keySet = aList.toArray();

     List keyList = new ArrayList();
     for (int i=0; i < keySet.length; i++) {
       keyList.add(keySet[i]);
       if (keyList.size() >= numKeysToGet) {
         break;
       }
     }
     return keyList;
   }

   /** Return a random key currently in the given region.
    *  @param aRegion The region to use for getting an existing key
    *  @returns A key from the local keySet (given the functionContext)
    */
   protected Object getExistingKey(Region aRegion) {
      Object key = null;
      // get thread specific keySet from BB
      String mapKey = PrTxBB.keySet + RemoteTestModule.getCurrentThread().getThreadId();
      KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
      DistributedMember keySetDM = keySetResult.getDistributedMember();
      List keyList = keySetResult.getKeySet();
      Object[] keySet = keyList.toArray();

      int index = TestConfig.tab().getRandGen().nextInt(0, keySet.length-1);
      key = keySet[index];
      return key;
   }

   /** Return a new key, never before used in the test.
    *  This must be on the same VM as the existing keySet 
    */
   protected Object getNewKeyForRegion(Region aRegion) {
      // get the next key which maps to our keySetDM
      Object key = null;
  
      // get the threadSpecific keySetResult (contains DistributedMember for keySet)
      String mapKey = PrTxBB.keySet + RemoteTestModule.getCurrentThread().getThreadId();
      KeySetResult keySetResult = (KeySetResult)PrTxBB.getBB().getSharedMap().get(mapKey);
      DistributedMember keySetDM = keySetResult.getDistributedMember();

      DistributedMember dm = null;
      do {

         // Note PartitionRegionHelper.getPrimaryMemberForKey() can return null if a primary has not yet been determined.
         key = NameFactory.getNextPositiveObjectName();
         dm = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
         while (dm == null) {
            // try another key
            key = NameFactory.getNextPositiveObjectName();
            dm = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
         }
         Log.getLogWriter().info("Looking for a new key from " + keySetDM + ".  Found " + key + " from " + dm);
       } while (!dm.equals(keySetDM));
       Log.getLogWriter().info("getNewKey() returning key " + key + " for member " + keySetDM);
       return key;
   }

  /** putall a map to the given region.
   * putAll when test is configured with sameKeyColocatedRegions = false (need to use keySets to create putAll map)
   *
   *  @param aRegion The region to use for putall a map.
   *
   */
   protected void putAll(Region r) {
      // determine the number of new keys to put in the putAll
      int beforeSize = r.size();
      String numPutAllNewKeys = TestConfig.tab().stringAt(ParRegPrms.numPutAllNewKeys);
      int numNewKeysToPut = 0;
      if (numPutAllNewKeys.equalsIgnoreCase("useThreshold")) {
         numNewKeysToPut = upperThreshold - beforeSize; 
         if (numNewKeysToPut <= 0) {
            numNewKeysToPut = 1;
         } else {
            int max = TestConfig.tab().intAt(ParRegPrms.numPutAllMaxNewKeys,
                                                        numNewKeysToPut);
            max = Math.min(numNewKeysToPut, max);
            int min = TestConfig.tab().intAt(ParRegPrms.numPutAllMinNewKeys, 1);
            min = Math.min(min, max);
            numNewKeysToPut = TestConfig.tab().getRandGen().nextInt(min, max);
         }
      } else {
         numNewKeysToPut = Integer.valueOf(numPutAllNewKeys).intValue();
      }

      // get a map to put
      Map mapToPut = null;
      int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
      if (randInt <= 25) {
         mapToPut = new HashMap();
      } else if (randInt <= 50) {
         mapToPut = new Hashtable();
      } else if (randInt <= 75) {
         mapToPut = new TreeMap();
      } else {
         mapToPut = new LinkedHashMap();
      }

      // add new keys to the map
      StringBuffer newKeys = new StringBuffer();
      for (int i = 1; i <= numNewKeysToPut; i++) { // put new keys
         Object key = getNewKeyForRegion(r);
         BaseValueHolder anObj = getValueForKey(key);
         mapToPut.put(key, anObj);
         newKeys.append(key + " ");
         if ((i % 10) == 0) {
            newKeys.append("\n");
         }
      }

      // add existing keys to the map
      int numPutAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
      List keyList = getExistingKeys(r, numPutAllExistingKeys);
      StringBuffer existingKeys = new StringBuffer();
      if (keyList.size() != 0) { // no existing keys could be found
         for (int i = 0; i < keyList.size(); i++) { // put existing keys
            Object key = keyList.get(i);
            BaseValueHolder anObj = getUpdateObject(r, key);
            mapToPut.put(key, anObj);
            existingKeys.append(key + " ");
            if (((i+1) % 10) == 0) {
               existingKeys.append("\n");
            }
         }
      }
      Log.getLogWriter().info("Region size is " + r.size() + ", map to use as argument to putAll is " + 
          mapToPut.getClass().getName() + " containing " + numNewKeysToPut + " new keys and " + 
          keyList.size() + " existing keys (updates); total map size is " + mapToPut.size() +
          "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
      for (Object key: mapToPut.keySet()) {
         Log.getLogWriter().info("putAll map key " + key + ", value " + TestHelper.toString(mapToPut.get(key)));
      }

      // do the putAll
      Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries");
      r.putAll(mapToPut);
      
      Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");

   }

  // putAll when test is configured with sameKeyColocatedRegions = true
  protected void putAll(Region r, Object[] keySet) {
    int beforeSize = r.size();
    String numPutAllNewKeys = TestConfig.tab().stringAt(ParRegPrms.numPutAllNewKeys);
    int numNewKeysToPut = 0;
    if (numPutAllNewKeys.equalsIgnoreCase("useThreshold")) {
       numNewKeysToPut = upperThreshold - beforeSize; 
       if (numNewKeysToPut <= 0) {
          numNewKeysToPut = 1;
       } else {
          int max = TestConfig.tab().intAt(ParRegPrms.numPutAllMaxNewKeys,
                                                      numNewKeysToPut);
          max = Math.min(numNewKeysToPut, max);
          int min = TestConfig.tab().intAt(ParRegPrms.numPutAllMinNewKeys, 1);
          min = Math.min(min, max);
          numNewKeysToPut = TestConfig.tab().getRandGen().nextInt(min, max);
       }
    } else {
       numNewKeysToPut = Integer.valueOf(numPutAllNewKeys).intValue();
    }

    // get a map to put
    Map mapToPut = null;
    int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
    if (randInt <= 25) {
       mapToPut = new HashMap();
    } else if (randInt <= 50) {
       mapToPut = new Hashtable();
    } else if (randInt <= 75) {
       mapToPut = new TreeMap();
    } else {
       mapToPut = new LinkedHashMap();
    }

    // add new keys to the map
    StringBuffer newKeys = new StringBuffer();
    for (int i = 1; i <= numNewKeysToPut; i++) { // put new keys
       Object key = getNewKeyForRegion(r);
       BaseValueHolder anObj = getValueForKey(key);
       mapToPut.put(key, anObj);
       newKeys.append(key + " ");
       if ((i % 10) == 0) {
          newKeys.append("\n");
       }
    }

    // add existing keys to the map
    int numPutAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
    List keyList = getExistingKeys(r, keySet, numPutAllExistingKeys);
    StringBuffer existingKeys = new StringBuffer();
    if (keyList.size() != 0) { // no existing keys could be found
       for (int i = 0; i < keyList.size(); i++) { // put existing keys
          Object key = keyList.get(i);
          BaseValueHolder anObj = getUpdateObject(r, key);
          mapToPut.put(key, anObj);
          existingKeys.append(key + " ");
          if (((i+1) % 10) == 0) {
             existingKeys.append("\n");
          }
       }
    }
    Log.getLogWriter().info("Region size is " + r.size() + ", map to use as argument to putAll is " + 
        mapToPut.getClass().getName() + " containing " + numNewKeysToPut + " new keys and " + 
        keyList.size() + " existing keys (updates); total map size is " + mapToPut.size() +
        "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
    for (Object key: mapToPut.keySet()) {
       Log.getLogWriter().info("putAll map key " + key + ", value " + TestHelper.toString(mapToPut.get(key)));
    }

    // do the putAll
    Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries");
    r.putAll(mapToPut);
    
    Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");

  }
  
  protected List getExistingKeys(Region aRegion, Object[] keySet, int numKeysToGet) {
    List keyList = new ArrayList();
    for (int i=0; i < keySet.length; i++) {
      keyList.add(keySet[i]);
      if (keyList.size() >= numKeysToGet) {
        return keyList;
      }
    }
    return keyList;
  }
  
/**
 *  doEntryOperation support for colocatedRegions
 *  Operations are executed on a selected key, across colocated Regions
 *
 *  @see PrTxPrm.sameKeyColocatedRegions
 */

   /** Do random entry operations on the a key in the given keySet across all
    *  colocated regions.  Note that addEntry and getNewKey() will create a new 
    *  key in the same VM as the rest of the keySet.
    *  Note:  All regions must be colocated at root level.
    *
    *  @see PrTxPrms.sameKeyColocatedRegions - to target the same key across
    *                                          multiple colocated regions.
    */
    protected void doEntryOperations(Object[] keySet) {
       
       // Select the key to work on 
       int index = TestConfig.tab().getRandGen().nextInt(0, keySet.length-1);
       Object key = keySet[index];
       Log.getLogWriter().info("doEntryOperations on single key = " + key);

       // In transaction tests, package the operations into a single transaction
       if (useTransactions) {
         TxHelper.begin();
       }

       // do ops on the selected key across all colocated regions
       Set regions = CacheHelper.getCache().rootRegions();
       for (Iterator rit = regions.iterator(); rit.hasNext(); ) {
          Region aRegion = (Region)rit.next();
          Log.getLogWriter().info("Executing operation on " + aRegion.getName() + " with key = " + key);
          int whichOp = getOperation(OperationsClientPrms.entryOperations);
          int size = keySet.length;
          if (size >= upperThreshold) {
             whichOp = getOperation(OperationsClientPrms.upperThresholdOperations);
          } else if (size <= lowerThreshold) {
             whichOp = getOperation(OperationsClientPrms.lowerThresholdOperations);
          }   

          String lockName = null;
    
          boolean gotTheLock = false;
          if (lockOperations) {
             lockName = LOCK_NAME + TestConfig.tab().getRandGen().nextInt(1, 20);
             Log.getLogWriter().info("Trying to get distributed lock " + lockName + "...");
             gotTheLock = distLockService.lock(lockName, -1, -1);
             if (!gotTheLock) {
                throw new TestException("Did not get lock " + lockName);
             }
             Log.getLogWriter().info("Got distributed lock " + lockName + ": " + gotTheLock);
          }

          try {
             switch (whichOp) {
                case ENTRY_ADD_OPERATION:
                   addEntry(aRegion);
                   break;
                case PUT_IF_ABSENT_OPERATION:
                   putIfAbsent(aRegion);
                   break;
                case ENTRY_INVALIDATE_OPERATION:
                   invalidateEntry(aRegion, key, false);
                   break;
                case ENTRY_DESTROY_OPERATION:
                   destroyEntry(aRegion, key, false);
                   break;
                case REMOVE_OPERATION:
                   removeEntry(aRegion, key);
                   break;
                case ENTRY_UPDATE_OPERATION:
                   updateEntry(aRegion, key);
                   break;
                case REPLACE_OPERATION:
                   replaceEntry(aRegion, key);
                   break;
                case ENTRY_GET_OPERATION:
                   getKey(aRegion, key);
                   break;
                case ENTRY_GET_NEW_OPERATION:
                   getNewKey(aRegion);
                   break;
                case ENTRY_LOCAL_INVALIDATE_OPERATION:
                   invalidateEntry(aRegion, key, true);
                   break;
                case ENTRY_LOCAL_DESTROY_OPERATION:
                   destroyEntry(aRegion, key, true);
                   break;
                case ENTRY_PUTALL_OPERATION:
                  putAll(aRegion, keySet);
                  break;
                default: {
                   throw new TestException("Unknown operation " + whichOp);
                }
            }
         } finally {
            if (gotTheLock) {
               gotTheLock = false;
               distLockService.unlock(lockName);
               Log.getLogWriter().info("Released distributed lock " + lockName);
            }
         }
      }

       // finish transactions (commit or rollback)
       if (useTransactions) {
          int n = 0;
          int commitPercentage = OperationsClientPrms.getCommitPercentage();
          n = TestConfig.tab().getRandGen().nextInt(1, 100);

          if (n <= commitPercentage) {
            try {
               TxHelper.commit();
            } catch (ConflictException e) {
               Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
            }
          } else {
              TxHelper.rollback();
          }
       }
       Log.getLogWriter().info("Done in doEntryOperations with key " + key);
    }

   /** Invalidate an entry in the given region.
    *
    *  @param aRegion The region to use for invalidating an entry.
    *  @param key The key to invalidate
    *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
    */
   protected void invalidateEntry(Region aRegion, Object key, boolean isLocalInvalidate) {
      try {
         String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
         if (isLocalInvalidate) { // do a local invalidate
            if (TestConfig.tab().getRandGen().nextBoolean()) { // local invalidate with callback
               Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback);
               aRegion.localInvalidate(key, callback);
               Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
            } else { // local invalidate without callback
               Log.getLogWriter().info("invalidateEntry: local invalidate for " + key);
               aRegion.localInvalidate(key);
               Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
            }
         } else { // do a distributed invalidate
            if (TestConfig.tab().getRandGen().nextBoolean()) { // invalidate with callback
               Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback);
               aRegion.invalidate(key, callback);
               Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
            } else { // invalidate without callback
               Log.getLogWriter().info("invalidateEntry: invalidating key " + key);
               aRegion.invalidate(key);
               Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
            }
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Destroy an entry in the given region.
    *
    *  @param aRegion The region to use for destroying an entry.
    *  @param isLocalDestroy True if the destroy should be local, false otherwise.
    */
   protected void destroyEntry(Region aRegion, Object key, boolean isLocalDestroy) {
      try {
         String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
         if (isLocalDestroy) { // do a local destroy
            if (TestConfig.tab().getRandGen().nextBoolean()) { // local destroy with callback
               Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback);
               aRegion.localDestroy(key, callback);
               Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
            } else { // local destroy without callback
               Log.getLogWriter().info("destroyEntry: local destroy for " + key);
               aRegion.localDestroy(key);
               Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
            }
         } else { // do a distributed destroy
            if (TestConfig.tab().getRandGen().nextBoolean()) { // destroy with callback
               Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback);
               aRegion.destroy(key, callback);
               Log.getLogWriter().info("destroyEntry: done destroying key " + key);
            } else { // destroy without callback
               Log.getLogWriter().info("destroyEntry: destroying key " + key);
               aRegion.destroy(key);
               Log.getLogWriter().info("destroyEntry: done destroying key " + key);
            }
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }

   /** Destroy an entry in the given region using the ConcurrentMap remove API.
    *
    *  @param aRegion The region to use for destroying an entry.
    */
   protected void removeEntry(Region aRegion, Object key) {
      try {
         Object oldValue = aRegion.get(key);
         Log.getLogWriter().info("removeEntry: removing key " + key + " with previous value " + oldValue);
         aRegion.remove(key, oldValue);
         Log.getLogWriter().info("removeEntry: done removing key " + key);
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Update an existing entry in the given region. If there are
    *  no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void updateEntry(Region aRegion, Object key) {
      BaseValueHolder anObj = getUpdateObject(aRegion, (String)key);
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
      if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
            TestHelper.toString(anObj) + ", callback is " + callback);
         aRegion.put(key, anObj, callback);
         Log.getLogWriter().info("Done with call to put (update)");
      } else { // do a put without callback
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
         aRegion.put(key, anObj);
         Log.getLogWriter().info("Done with call to put (update)");
      }
   }

   /** Replace an existing entry in the given region using the ConcurrentMap API. 
    *  If there are no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void replaceEntry(Region aRegion, Object key) {
      BaseValueHolder anObj = getUpdateObject(aRegion, (String)key);
      if (TestConfig.tab().getRandGen().nextBoolean()) { // do a replace with oldValue
         BaseValueHolder oldValue = (BaseValueHolder)aRegion.get(key);
         Log.getLogWriter().info("replaceEntry: replacing key " + key + " value " + TestHelper.toString(oldValue) + " with " + TestHelper.toString(anObj));
         aRegion.replace(key, oldValue, anObj);
      } else {
         Log.getLogWriter().info("replaceEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
         aRegion.replace(key, anObj);
      }
      Log.getLogWriter().info("Done with call to replace");
   }
       
   /** Get an existing key in the given region if one is available,
    *  otherwise get a new key. 
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getKey(Region aRegion, Object key) {
      String callback = getCallbackPrefix + ProcessMgr.getProcessId();
      Object anObj = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getKey: getting key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      } else { // get without callback
         Log.getLogWriter().info("getKey: getting key " + key);
         anObj = aRegion.get(key);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      }
   }
}
