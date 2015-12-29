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

import hydra.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;

import java.util.*;
import util.*;

/**
 * Prototype for High Priority Use Case #1 (PartitionedRegions only)
 * - Concurrent tx on colocated entries (no conflict expected)
 * - tx consists of new entry in Region_1 with updates to entries in Regions 2-4
 * - other threads query these regions (we should not see any updates without
 * - being able to see ALL updates.
 *
 * This is a prototype for now which does the following:
 * creates the 4 colocated regions (Region_1 -> Region_4)
 * populates regions 2-4 with util/QueryObjects (extra.value = null) for both 
 * positive and negative counter values up to maxKeys
 * reset the positive and negative counters to zero
 * positiveTxThread creates an entry in Region_1 and updates the corresponding entries in Regions_2 through 4 by setting queryObject.extra.value = key
 * negativeTxThread creates an entry in Region_1 and updates the corresponding entries in Regions_2 through 4 by setting queryObject.extra.value = key
 *
 * Problem is currently how to figure out if we can see all or nothing (when
 * query is region specific.  Will need to synchronize (waitForCounter) for 
 * threads to coordinate:
 *  - readyForTx (tx and query threads at sync point)
 *  - query threads wait for positive or negative sync point
 *  - Tx thread can then validate that all (or none) saw the new entry + updates
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.1
 */
public class PrTxQueryTest {

  // Single instance in this VM
  static protected PrTxQueryTest testInstance;
  static Region baseRegion;

  static final String EXTRA_INDEX = "extraIndex";

   // instance fields
   protected long minTaskGranularitySec;       // the task granularity in seconds
   protected long minTaskGranularityMS;        // the task granularity in milliseconds
   protected int numOpsPerTask;                // the number of operations to execute per task
   private boolean isPartitioned;              // true if regions are partitioned (vs. replicate)

  private ArrayList errorMsgs = new ArrayList();
  private ArrayList errorException = new ArrayList();

  /**
   *  Create the cache and Region.  
   */
  public synchronized static void HydraTask_initializeWithPartitionedRegions() {
    if (testInstance == null) {
      testInstance = new PrTxQueryTest();

      try {
        testInstance.initializePartitionedRegions();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }  
    }
  }

  /* 
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  protected void initializePartitionedRegions() {

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
     isPartitioned = true;
     minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
     if (minTaskGranularitySec == Long.MAX_VALUE) {
        minTaskGranularityMS = Long.MAX_VALUE;
     } else {
        minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
     }
     numOpsPerTask = TestConfig.tab().intAt(PrTxPrms.numOpsPerTask, Integer.MAX_VALUE);
     Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " + "minTaskGranularityMS " + minTaskGranularityMS + ", " + "numOpsPerTask " + numOpsPerTask);

  }

  /**
   *  Create the cache and Region.  
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new PrTxQueryTest();

      try {
        testInstance.initialize();
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
       Region insertRegion = RegionHelper.createRegion(PrTxPrms.getInsertRegion(), regionConfig);
       createIndex(insertRegion, "r1");
  
       List updateRegions = PrTxPrms.getUpdateRegions();
       int i = 2;
       for (Iterator it = updateRegions.iterator(); it.hasNext(); i++) {
          String regionName = (String)it.next();
          Region aRegion = RegionHelper.createRegion(regionName, regionConfig);
          createIndex(aRegion, "r"+i);
       }
     }
     isPartitioned = false;
     minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
     if (minTaskGranularitySec == Long.MAX_VALUE) {
        minTaskGranularityMS = Long.MAX_VALUE;
     } else {
        minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
     }
     numOpsPerTask = TestConfig.tab().intAt(PrTxPrms.numOpsPerTask, Integer.MAX_VALUE);
     Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " + "minTaskGranularityMS " + minTaskGranularityMS + ", " + "numOpsPerTask " + numOpsPerTask);

  }

  private void createIndex(Region aRegion, String indexExpr) {
     Cache aCache = CacheHelper.getCache();
     QueryService qs = aCache.getQueryService();
     String regionName = aRegion.getFullPath();
     String fromClause = regionName + " " + indexExpr;
     String fieldName = indexExpr + ".extra";

     try {
        qs.createIndex(EXTRA_INDEX, IndexType.FUNCTIONAL, fieldName, fromClause);
        Log.getLogWriter().info("Created index " + EXTRA_INDEX + " with field " + fieldName + " with fromClause " + fromClause);
     } catch (IndexNameConflictException e) {
        Log.getLogWriter().info("createIndex(" + regionName + ") caught Exception " + e + TestHelper.getStackTrace(e));
        throw new TestException("createIndex(" + regionName + ") caught Exception " + e + " " + TestHelper.getStackTrace(e));
     } catch (IndexExistsException e) {
        Log.getLogWriter().info("createIndex(" + regionName + ") caught Exception " + e + TestHelper.getStackTrace(e));
        throw new TestException("createIndex(" + regionName + ") caught Exception " + e + " " + TestHelper.getStackTrace(e));
     } catch(QueryException e) {
        Log.getLogWriter().info("createIndex(" + regionName + ") caught Exception " + e + TestHelper.getStackTrace(e));
        throw new TestException("createIndex(" + regionName + ") caught Exception " + e + " " + TestHelper.getStackTrace(e));
     }
  }

 /*
  *  Creates initial set of entries across colocated regions
  *  (non-transactional).
  */
  public static void HydraTask_populateUpdateRegions() {
     testInstance.populateUpdateRegions();
  }

  protected void populateUpdateRegions() {
     for (int i = 0; i < PrTxPrms.getMaxKeys(); i++) { 
        Object pKey = NameFactory.getNextPositiveObjectName();
        Object nKey = NameFactory.getNextNegativeObjectName();

        // create this same key in each region
        List regionNames = PrTxPrms.getUpdateRegions();
        for (Iterator it = regionNames.iterator(); it.hasNext(); ) {
           String regionName = (String)it.next();
           Region aRegion = RegionHelper.getRegion(regionName);
           try {
              createEntry(aRegion, pKey, false);
              createEntry(aRegion, nKey, false);
           } catch (EntryExistsException e) {
              Log.getLogWriter().info("populateUpdateRegions caught " + e + ".  Expected, continuing with test");
           }
        }
     }
     // clear the counter, so that we'll use these in executeTx
     NameBB.getBB().zero ("NameBB.POSITIVE_NAME_COUNTER", NameBB.POSITIVE_NAME_COUNTER); 
     NameBB.getBB().zero ("NameBB.NEGATIVE_NAME_COUNTER", NameBB.NEGATIVE_NAME_COUNTER); 
  }

  /** Debug method to see what keys are on each VM at start of test
   *
   */
  public static void HydraTask_dumpLocalKeys() {
     testInstance.dumpLocalKeys();
  }

  protected void dumpLocalKeys() {
     StringBuffer aStr = new StringBuffer();
     DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
 
     aStr.append("Keys local to " + dm.toString() + " by region\n");
     Set rootRegions = CacheHelper.getCache().rootRegions();
     for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
        Region aRegion = (Region)it.next();
        Region localRegion = PartitionRegionHelper.getLocalData(aRegion);
        aStr.append("   " + aRegion.getName() + ": " + localRegion.keySet() + "\n");
     }
     Log.getLogWriter().info(aStr.toString());
  }

  protected void createEntry(Region aRegion, Object key, boolean setExtra) {
     long counter = NameFactory.getCounterForName(key);
     QueryObject anObj = new QueryObject(counter,
                             QueryObject.EQUAL_VALUES,
                             0, /* byte array size */
                             1  /* queryDepth */);
     if (setExtra) {
       anObj.extra = key;
     }
 
     Log.getLogWriter().info("operation for " + aRegion.getFullPath() + ":" + key + ", createEntry: calling create for key " + key + ", object " + anObj.toStringFull());
     aRegion.create(key, anObj);
     Log.getLogWriter().info("operation for " + aRegion.getFullPath() + ":" + key + ", createEntry: done creating key " + key);
  }

  protected void updateEntry(Region aRegion, Object key) {
     QueryObject oldValue = (QueryObject)(aRegion.get(key));
     QueryObject newValue = oldValue.modifyWithNewInstance(
                               QueryObject.INCREMENT,
                               0,    /* amount to increment */
                               false /* log modifications */ );
     newValue.extra = key;

     Log.getLogWriter().info("operation for " + aRegion.getFullPath() + ":" + key + ", updateEntry: replacing key " + key + " with " + newValue.toStringFull());
     QueryObject returnVal = (QueryObject)aRegion.put(key, newValue);
     Log.getLogWriter().info("operation for " + aRegion.getFullPath() + ":" + key + ", updateEntry: Done with call to put (update), returnVal is " + returnVal);
  }

  public static void HydraTask_positiveTx() {
     testInstance.positiveTx();
  }

  protected void positiveTx() {
    long startTime = System.currentTimeMillis();
    int numTxns = 0;

    do {
       // todo@lhughes -- right now we can only target entries in local vm
       // take this out once remote tx supported
       //executeTx(NameFactory.getNextPositiveObjectName());
       Object key = getNextPositiveKey();
       executeTx(key);
       numTxns++;
     } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) && (numTxns < numOpsPerTask));
  }

  public static void HydraTask_negativeTx() {
     testInstance.negativeTx();
  }

  protected void negativeTx() {
    long startTime = System.currentTimeMillis();
    int numTxns = 0;

    do {
       // todo@lhughes -- right now we can only target entries in local vm
       // take this out once remote tx supported
       //executeTx(NameFactory.getNextNegativeObjectName());
       Object key = getNextNegativeKey();
       executeTx(key);
       numTxns++;
     } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) && (numTxns < numOpsPerTask));
  }

  // remove this method and references to it once remote tx supported
  protected Object getNextPositiveKey() {
    DistributedMember localDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
    Region aRegion = RegionHelper.getRegion(PrTxPrms.getInsertRegion());
    Object key = null;
    while (key == null) {
       Object pKey = NameFactory.getNextPositiveObjectName();
       if (isPartitioned) {
          DistributedMember pDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, pKey);
          if (localDM.equals(pDM)) {
             key = pKey;
          }
       } else {  // distributed region
             key = pKey;
       }
    }
    return key;
  }

  // remove this method and references to it once remote tx supported
  protected Object getNextNegativeKey() {
    DistributedMember localDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
    Region aRegion = RegionHelper.getRegion(PrTxPrms.getInsertRegion());
    Object key = null;
    while (key == null) {
       Object pKey = NameFactory.getNextNegativeObjectName();
       if (isPartitioned) {
          DistributedMember pDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, pKey);
          if (localDM.equals(pDM)) {
             key = pKey;
          }
       } else {  // distributed region
             key = pKey;
       }
    }
    return key;
  }

  protected void executeTx(Object key) {
    TestHelper.checkForEventError(PrTxBB.getBB());

    // Are we done yet?
    long counter = NameFactory.getCounterForName(key);
    if (Math.abs(counter) > PrTxPrms.getMaxKeys()) {
      throw new StopSchedulingOrder("All available entries have been updated");
    }

    // execute tx 
    TxHelper.begin();
  
    // insert
    Region aRegion = RegionHelper.getRegion(PrTxPrms.getInsertRegion());
    createEntry(aRegion, key, true);

    // updates
    List regionNames = PrTxPrms.getUpdateRegions();
    for (Iterator it = regionNames.iterator(); it.hasNext(); ) {
       String regionName = (String)it.next();
       aRegion = RegionHelper.getRegion(regionName);
       updateEntry(aRegion, key);
    }

    // commit
    try {
       TxHelper.commit();
    } catch (ConflictException e) {
       Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
    }
  }

  public static void HydraTask_query() {
     testInstance.query();
  }

  // todo@lhughes - extend for remote tx
  // this whole method needs to be re-worked once we support remote tx
  // keys are currently forced to be for local entries 
  protected void query() {

    long startTime = System.currentTimeMillis();
    int numQueries= 0;

    do {
        // Select the key to target
        long counter = 0;
        Object key = null;
        if (TestConfig.tab().getRandGen().nextBoolean()) {    // latest key
           if (TestConfig.tab().getRandGen().nextBoolean()) { // latest positive
               //counter = NameFactory.getPositiveNameCounter();
               key = getNextPositiveKey();
           } else {                                           // latest negative
               //counter = NameFactory.getNegativeNameCounter();
               key = getNextNegativeKey();
           }
           //String key = NameFactory.getObjectNameForCounter(counter);
        } else {                                              // randomKey
           String regionName = PrTxPrms.getInsertRegion();
           Region aRegion = RegionHelper.getRegion(regionName);
           Object[] keySet;
           if (isPartitioned) {
              keySet = PartitionRegionHelper.getLocalData(aRegion).keySet().toArray();
           } else {
              keySet = aRegion.keySet().toArray();
           }

           // there may not be any keys in the insertRegion yet, allow
           if (keySet.length > 0) {
              int index = TestConfig.tab().getRandGen().nextInt(0, keySet.length-1);
              key = keySet[index];
           }
        }
        // randomKeys relies on entries in the insertRegion ... so allow for 
        // misses until we get some keys to work with
        if (key != null) {
           if (isPartitioned) {
             queryPartitionedRegionForKey(key);
           } else {
             queryForKey(key);
           }
           numQueries++;
        }
      } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) && (numQueries < numOpsPerTask));
  }

  protected void queryForKey(Object key) {
     QueryService queryService = CacheHelper.getCache().getQueryService();
     SelectResults results = null;
     int hits = 0;

     // handle distributed regions with a single query
     // expect all (4) or nothing (0)
     String regionName = PrTxPrms.getInsertRegion();
     Region insertRegion = RegionHelper.getRegion(regionName);

     StringBuffer aStr = new StringBuffer();
     aStr.append("SELECT DISTINCT * FROM " + insertRegion.getFullPath() + " r1");

     List regionNames = PrTxPrms.getUpdateRegions();
     int i = 2;
     for (Iterator it = regionNames.iterator(); it.hasNext(); i++) {
        regionName = (String)it.next();
        Region aRegion = RegionHelper.getRegion(regionName);
        aStr.append(", " + aRegion.getFullPath() + " r" + i);
     }
     aStr.append(" WHERE r1.extra = '" + key + "'");
     for (i=2; i <= 4; i++) {
       aStr.append(" AND r" + i + ".extra = '" + key + "'");
     }
     String qStr = aStr.toString();
     Query query = queryService.newQuery(qStr);
     try {
        Log.getLogWriter().info("Executing query " + qStr);
        results = (SelectResults)query.execute();
     } catch (Exception e) {
        Log.getLogWriter().info("query threw " + e + " " + TestHelper.getStackTrace(e));
     }
     Log.getLogWriter().info(qStr + " returns " + results.toString());

     aStr = new StringBuffer();
     for (Iterator it = results.iterator(); it.hasNext(); ) {
        Struct s = (Struct)it.next();
        Object[] qResults = s.getFieldValues();
        for (hits=0; hits < qResults.length; hits++) {
           QueryObject qo = (QueryObject)qResults[hits];
           aStr.append("qo[" + hits + "] = " + qo.toString() + "\n");
           if (!qo.extra.equals(key)) {
              throw new TestException("query returned unexpected entry " + qo.toString() + TestHelper.getStackTrace());
           }
        }
     }
     Log.getLogWriter().info("QueryResults for " + key + "\n" + aStr.toString());
    
     // If we can see the insert, we should also see the updates
     if (hits > 0 && hits != 4) {
        throw new TestException("Expected to find updated entries for " + key + " in all " + regionNames.size() + " regions, but query returned " + hits + " entries");
     }
     return;
  }

  protected void queryPartitionedRegionForKey(Object key) {
     QueryService queryService = CacheHelper.getCache().getQueryService();
     SelectResults results = null;
     int hits = 0;

     // handle query on PartitionedRegion (joins not supported)
     // query insert and updateRegions
     String regionName = PrTxPrms.getInsertRegion();
     Region aRegion = RegionHelper.getRegion(regionName);
     String qStr = "SELECT DISTINCT * FROM " + aRegion.getFullPath() + " WHERE extra = '" + key + "'";
     Query query = queryService.newQuery(qStr);
     try {
        Log.getLogWriter().info("Executing query " + qStr);
        results = (SelectResults)query.execute();
     } catch (Exception e) {
        Log.getLogWriter().info("query threw " + e + " " + TestHelper.getStackTrace(e));
     }
     Log.getLogWriter().info(qStr + " returns " + results);
  
     // If we can see the insert, we should also see the updates
     if (results.size() > 0) {
        List regionNames = PrTxPrms.getUpdateRegions();
        for (Iterator it = regionNames.iterator(); it.hasNext(); ) {
           regionName = (String)it.next();
           aRegion = RegionHelper.getRegion(regionName);
           qStr = "SELECT DISTINCT * FROM " + aRegion.getFullPath() + " WHERE extra = '" + key + "'";
           query = queryService.newQuery(qStr);
           try {
              results = (SelectResults)query.execute();
           } catch (Exception e) {
              Log.getLogWriter().info("query threw " + e + " " + TestHelper.getStackTrace(e));
           }
           Log.getLogWriter().info(qStr + " returns " + results + " with size " + results.size());
           hits += results.size();
        }
        if (hits != regionNames.size()) {
          throw new TestException("Expected to find updated entries for " + key + " in all " + regionNames.size() + " regions, but query returned " + hits + " entries");
        }
     }
  }
}
