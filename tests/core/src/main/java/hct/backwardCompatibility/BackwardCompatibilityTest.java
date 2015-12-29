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
package hct.backwardCompatibility; 

import java.util.*;

import hydra.*;
import util.*;
import cq.*;
import parReg.ParRegPrms;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.GemFireVersion;

public class BackwardCompatibilityTest {

   static Random rand = new Random();
   static BackwardCompatibilityTest testInstance;
   static Region aRegion;

   // operations
   static protected final int ENTRY_ADD_OPERATION = 1;
   static protected final int ENTRY_DESTROY_OPERATION = 2;
   static protected final int ENTRY_INVALIDATE_OPERATION = 3;
   static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
   static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
   static protected final int ENTRY_UPDATE_OPERATION = 6;
   static protected final int ENTRY_GET_OPERATION = 7;
   static protected final int ENTRY_GET_NEW_OPERATION = 8;
   static protected final int PUT_IF_ABSENT_OPERATION = 9;
   static protected final int REMOVE_OPERATION = 10;
   static protected final int REPLACE_OPERATION = 11;
   static protected final int ENTRY_PUTALL_OPERATION = 12;

   // String prefixes for event callback object
   protected static final String getCallbackPrefix = "Get originated in pid ";
   protected static final String createCallbackPrefix = "Create event originated in pid ";
   protected static final String updateCallbackPrefix = "Update event originated in pid ";
   protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
   protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
   protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
   protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
   
   protected static final String VmIDStr = "VmId_";

   // instance fields
   protected boolean isOldClient = false; // used to determine a client that does not support ConcurrentMap API methods
   protected long minTaskGranularitySec;       // the task granularity in seconds
   protected long minTaskGranularityMS;        // the task granularity in milliseconds
   protected int numOpsPerTask;                // the number of operations to execute per task
   protected RandomValues randomValues =       // instance of random values, used as the value for puts
          new RandomValues();

   protected int upperThreshold;               
   protected int lowerThreshold;               

// ======================================================================== 
// initialization tasks/methods 
// ======================================================================== 
    public synchronized static void HydraTask_initializeBridgeServer() {
       if (testInstance == null) {
          testInstance = new BackwardCompatibilityTest();
          testInstance.initializePrms();
          testInstance.initializeBridgeServer();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeServer() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         String regionConfig = ConfigPrms.getRegionConfig();
         RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
         AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);

         // install the disk store before creating the region, if needed
         if (requiresPersistence(rd)) {
           String diskStoreConfig = ConfigPrms.getDiskStoreConfig();
           DiskStoreHelper.createDiskStore(diskStoreConfig);
           factory.setDiskStoreName(diskStoreConfig);
         }

         aRegion = RegionHelper.createRegion(rd.getRegionName(), factory);
         BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
       }
    }

    public synchronized static void HydraTask_initializeBridgeClient() {
       if (testInstance == null) {
          testInstance = new BackwardCompatibilityTest();
          testInstance.initializePrms();
          testInstance.initializeBridgeClient();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeClient() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         String regionConfig = ConfigPrms.getRegionConfig();
         RegionHelper.createRegion(regionConfig);
         RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);;
         aRegion = RegionHelper.getRegion(rd.getRegionName());
       }

       // initialize CQService, if needed
       if (TestConfig.tab().booleanAt(cq.CQUtilPrms.useCQ, false)) {
          CQUtil.initialize();
          CQUtil.initializeCQService();
          CQUtil.registerCQ(aRegion);
       } 

       if (BackwardCompatibilityPrms.registerInterest()) {
          aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in ALL_KEYS");
       }
    }

    protected boolean requiresPersistence(RegionDescription rd) {
      return rd.getDataPolicy().withPersistence()
             || (rd.getEvictionAttributes() != null &&
                 rd.getEvictionAttributes().getAction().isOverflowToDisk());
    }

    /**
     *  Performs puts/gets on entries in TestRegion
     *  Allows Shutdown and CacheClosedException if the result of forcedDisconnect
     */
    public static void HydraTask_doEntryOperations() {
        testInstance.doEntryOperations(aRegion);
    }

    protected void doEntryOperations(Region aRegion) {
       new BackwardCompatibilityVersionHelper().doEntryOperations(testInstance, aRegion);
    }

    protected void doRandomOp(Region aRegion) {

       int whichOp = getOperation(BackwardCompatibilityPrms.entryOperations);
       int size = aRegion.size();
       if (size >= upperThreshold) {
          whichOp = getOperation(BackwardCompatibilityPrms.upperThresholdOperations);
       } else if (size <= lowerThreshold) {
          whichOp = getOperation(BackwardCompatibilityPrms.lowerThresholdOperations);
       }   

       switch (whichOp) {
          case ENTRY_ADD_OPERATION:
             addEntry(aRegion);
             break;
          case ENTRY_INVALIDATE_OPERATION:
             invalidateEntry(aRegion, false);
             break;
          case ENTRY_DESTROY_OPERATION:
             destroyEntry(aRegion, false);
             break;
          case ENTRY_UPDATE_OPERATION:
             updateEntry(aRegion);
             break;
          case ENTRY_GET_OPERATION:
             getKey(aRegion);
             break;
          case ENTRY_GET_NEW_OPERATION:
             getNewKey(aRegion);
             break;
          case ENTRY_LOCAL_INVALIDATE_OPERATION:
             invalidateEntry(aRegion, true);
             break;
          case ENTRY_LOCAL_DESTROY_OPERATION:
             destroyEntry(aRegion, true);
             break;
          // Map concurrentMap entries to GemFire apis in 6.1.2 clients
          case PUT_IF_ABSENT_OPERATION:
              if (isOldClient) {
                 addEntry(aRegion);
              } else { 
                 putIfAbsent(aRegion, true);
              }
              break;
          case REMOVE_OPERATION:
              if (isOldClient) {
                 destroyEntry(aRegion, false);
              } else { 
                 remove(aRegion);
              }
              break;
          case REPLACE_OPERATION:
              if (isOldClient) {
                 updateEntry(aRegion);
              } else { 
                 replace(aRegion);
              }
              break;
          case ENTRY_PUTALL_OPERATION:
              putAll(aRegion);
              break;
          default: {
             throw new TestException("Unknown operation " + whichOp);
          }
       }
    }

    /**
     * Execute operations which invoke client/server messaging 
     */
    public static void HydraTask_testClientServerMessaging() {
        testInstance.testClientServerMessaging();
    }
 
    protected void testClientServerMessaging() {
       // aRegion has been established already ... dev should include messaging specific ops here.
       Log.getLogWriter().info("Invoking client/server cmds on " + aRegion.getFullPath());

       // add commands to test client/server messages here ... 
    }

  //=================
  // Support methods
  //=================
  public void initializePrms() {
     isOldClient = GemFireVersion.getGemFireVersion().equals("6.1.2");
     minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
     if (minTaskGranularitySec == Long.MAX_VALUE)
        minTaskGranularityMS = Long.MAX_VALUE;
     else
        minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
     numOpsPerTask = BackwardCompatibilityPrms.numOpsPerTask();

     upperThreshold = TestConfig.tab().intAt(BackwardCompatibilityPrms.upperThreshold, Integer.MAX_VALUE);
     lowerThreshold = TestConfig.tab().intAt(BackwardCompatibilityPrms.lowerThreshold, -1);

     Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " +
                             "minTaskGranularityMS " + minTaskGranularityMS + ", " +
                             "isOldClient " + isOldClient + ", " + 
                             "numOpsPerTask " + numOpsPerTask + ", " +
                             "upperThreshold " + upperThreshold + ", " +
                             "lowerThreshold " + lowerThreshold);

    }

   /** Add a new entry to the given region.
    *
    *  @param aRegion The region to use for adding a new entry.
    *
    */
   protected void addEntry(Region aRegion) {
      Object key = getNewKey();
      ValueHolder anObj = getValueForKey(key);
      String callback = createCallbackPrefix + ProcessMgr.getProcessId();
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
         if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " + 
               aRegion.getFullPath());
            aRegion.create(key, anObj, callback);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         } else { // use create with no cacheWriter arg
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.create(key, anObj);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         }
      } else { // use a put call
         if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
            aRegion.put(key, anObj, callback);
            Log.getLogWriter().info("addEntry: done putting key " + key);
         } else {
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.put(key, anObj);
            Log.getLogWriter().info("addEntry: done putting key " + key);
         }
      }
   }

   /** 
    *  ConcurrentMap API testing
    */
   protected void putIfAbsent(Region aRegion, boolean logAddition) {
       Object key = null;

       // Expect success most of the time (put a new entry into the cache)
       int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
       if (randInt <= 25) {
          Set aSet = aRegion.keySet();
          if (aSet.size() > 0) {
            Iterator it = aSet.iterator();
            if (it.hasNext()) {
                key = it.next();
            } 
         }
       }

       if (key == null) {
          key = getNewKey();
       }
       Object anObj = getValueForKey(key);

       if (logAddition) {
           Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath() + ".");
       }

       Object prevVal = null;
       prevVal = aRegion.putIfAbsent(key, anObj);
   }
    
   /** putall a map to the given region.
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
         Object key = getNewKey();
         ValueHolder anObj = getValueForKey(key);
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
            ValueHolder anObj = getUpdateObject(r, key);
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
   protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         int size = aRegion.size();
         return;
      }
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


   /**
    *  ConcurrentMap API testing
    **/  
   protected void remove(Region aRegion) {
       Set aSet = aRegion.keys();
       Iterator iter = aSet.iterator();
       if (!iter.hasNext()) {
           Log.getLogWriter().info("remove: No names in region");
           return;
       }
       try {
           Object name = iter.next();
           Object oldVal = aRegion.get(name);
           remove(aRegion, name, oldVal);
       } catch (NoSuchElementException e) {
           throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
       }
   }
       
   private void remove(Region aRegion, Object name, Object oldVal) {

       boolean removed;
       try {

         // Force the condition to not be met (small percentage of the time)
         int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
         if (randInt <= 25) {
           oldVal = getUpdateObject(aRegion, name);
          }

          Log.getLogWriter().info("remove: removing " + name + " with previous value " + oldVal + ".");
          removed = aRegion.remove(name, oldVal);
          Log.getLogWriter().info("remove: done removing " + name);
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
         int size = aRegion.size();
         return;
      }
      ValueHolder anObj = getUpdateObject(aRegion, key);
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


   /**
    * Updates the "first" entry in a given region
    */
   protected void replace(Region aRegion) {
       Set aSet = aRegion.keys();
       Iterator iter = aSet.iterator();
       if (!iter.hasNext()) {
           Log.getLogWriter().info("replace: No names in region");
           return;
       }
       Object name = iter.next();
       replace(aRegion, name);
   }
       
   /**
    * Updates the entry with the given key (<code>name</code>) in the
    * given region.
    */
   protected void replace(Region aRegion, Object name) {
       Object anObj = null;
       Object prevVal = null;
       boolean replaced = false;
       try {
           anObj = aRegion.get(name);
       } catch (CacheLoaderException e) {
           throw new TestException(TestHelper.getStackTrace(e));
       } catch (TimeoutException e) {
           throw new TestException(TestHelper.getStackTrace(e));
       }

       Object newObj = getUpdateObject(aRegion, name);
       // 1/2 of the time use oldVal => newVal method
       if (TestConfig.tab().getRandGen().nextBoolean()) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
             // Force the condition to not be met 
             // get a new oldVal to cause this
            anObj = getUpdateObject(aRegion, name);
          }

          Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) + ".");
           replaced = aRegion.replace(name, anObj, newObj);
       } else {

          if (TestConfig.tab().getRandGen().nextBoolean()) {
            // Force the condition to not be met
            // use a new key for this
            name = getNewKey();
          }
          Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + ".");
          prevVal = aRegion.replace(name, newObj);

          if (prevVal != null) replaced = true;
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
         int size = aRegion.size();
         return;
      }
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
       
   /** Get a new key int the given region.
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getNewKey(Region aRegion) {
      Object key = getNewKey();
      String callback = getCallbackPrefix + ProcessMgr.getProcessId();
      int beforeSize = aRegion.size();
      Object anObj = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
      } else { // get without callback
         Log.getLogWriter().info("getNewKey: getting new key " + key);
         anObj = aRegion.get(key);
      }
      Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
   }

   // ========================================================================
   // helper methods 

   /** Return a value for the given key
    */
   public ValueHolder getValueForKey(Object key) {
      return new ValueHolder((String)key, randomValues);
   }
    
   /** Return a new key, never before used in the test.
    */
   protected Object getNewKey() {
      Object key =  NameFactory.getNextPositiveObjectName();
      return key;
   }
       
   /** Return a random recently used key.
    *
    *  @param aRegion The region to use for getting a recently used key.
    *  @param recentHistory The number of most recently used keys to consider
    *         for returning.
    *
    *  @returns A recently used key, or null if none.
    */
   protected Object getRecentKey(Region aRegion, int recentHistory) {
      long maxNames = NameFactory.getPositiveNameCounter();
      if (maxNames <= 0) {
         return null;
      }
      long keyIndex = TestConfig.tab().getRandGen().nextLong(
                         Math.max(maxNames-recentHistory, (long)1), 
                         maxNames);
      Object key = NameFactory.getObjectNameForCounter(keyIndex);
      return key;
   }
   
   /** Return an object to be used to update the given key. If the
    *  value for the key is a ValueHolder, then get an alternate
    *  value which is similar to it's previous value (see
    *  ValueHolder.getAlternateValueHolder()).
    *
    *  @param aRegion The region which possible contains key.
    *  @param key The key to get a new value for.
    *  
    *  @returns An update to be used to update key in aRegion.
    */
   protected ValueHolder getUpdateObject(Region aRegion, Object key) {
      ValueHolder anObj = (ValueHolder)aRegion.get(key);
      ValueHolder newObj = null;
      if (anObj == null) {
        //TODO change the ValueHolder constructor to take into
        // account different parameters
        if (key instanceof String) {
          newObj = new ValueHolder((String)key, randomValues);
        } else {
          newObj = new ValueHolder(key, randomValues);
        }
      } else {
        newObj = (ValueHolder)anObj.getAlternateValueHolder(randomValues);
      }
      return newObj;
   }
   
   /** Get a random operation using the given hydra parameter.
    *
    *  @param whichPrm A hydra parameter which specifies random operations.
    *
    *  @returns A random operation.
    */
   protected int getOperation(Long whichPrm) {
      int op = 0;
      String operation = TestConfig.tab().stringAt(whichPrm);
      if (operation.equals("add"))
         op = ENTRY_ADD_OPERATION;
      else if (operation.equals("update"))
         op = ENTRY_UPDATE_OPERATION;
      else if (operation.equals("invalidate"))
         op = ENTRY_INVALIDATE_OPERATION;
      else if (operation.equals("destroy"))
         op = ENTRY_DESTROY_OPERATION;
      else if (operation.equals("get"))
         op = ENTRY_GET_OPERATION;
      else if (operation.equals("getNew"))
         op = ENTRY_GET_NEW_OPERATION;
      else if (operation.equals("localInvalidate"))
         op = ENTRY_LOCAL_INVALIDATE_OPERATION;
      else if (operation.equals("localDestroy"))
         op = ENTRY_LOCAL_DESTROY_OPERATION;
      else if (operation.equals("putIfAbsent"))
         op = PUT_IF_ABSENT_OPERATION;
      else if (operation.equals("remove"))
         op = REMOVE_OPERATION;
      else if (operation.equals("replace"))
         op = REPLACE_OPERATION;
      else if (operation.equals("putAll"))
        op = ENTRY_PUTALL_OPERATION;
      else
         throw new TestException("Unknown entry operation: " + operation);
      return op;
   }
   
   /** Return a random key currently in the given region.
    *
    *  @param aRegion The region to use for getting an existing key (may
    *         or may not be a partitioned region).
    *
    *  @returns A key in the region.
    */
   protected Object getExistingKey(Region aRegion) {
      Object key = null;
      Object[] keyList = aRegion.keySet().toArray();
      int index = TestConfig.tab().getRandGen().nextInt(0, keyList.length-1);
      if (index > 0) {
         key = keyList[index];
      }
      return key;
   }
   
   protected List getExistingKeys(Region aRegion, int numKeysToGet) {
     Log.getLogWriter().info("Trying to get " + numKeysToGet + " existing keys...");
     List keyList = new ArrayList();
     Set aSet = aRegion.keySet();
     Iterator it = aSet.iterator();
     while (it.hasNext()) {
       keyList.add(it.next());
       if (keyList.size() >= numKeysToGet) {
         return keyList;
       }
     }
     return keyList;
   }
}
