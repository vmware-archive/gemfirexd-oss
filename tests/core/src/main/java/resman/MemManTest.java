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
package resman;

import hydra.CacheHelper;
import hydra.GsRandom;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.StopSchedulingOrder;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import parReg.ParRegBB;
import parReg.ParRegUtil;
import rebalance.RebalanceUtil;
import util.BaseValueHolder;
import util.NameFactory;
import util.PRObserver;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.TxHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionInDoubtException;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/** Test class for PR with critical heap settings and rebalancing.
 */
public class MemManTest {
    
/* The singleton instance of MemManTest in this VM */
static public MemManTest testInstance;
    
protected List<Region> regionList;        // The regions under test
protected int regionNameIndex = 0;        // Used for creating unique names for multiple PRs

protected AtomicLong lowMemCounter = new AtomicLong();
protected AtomicInteger gcController = new AtomicInteger();

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of MemManTest 
 *  in this VM.
 */
public synchronized static void HydraTask_initAccessor() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new MemManTest();
      int numRegions = ResourceManPrms.getNumRegions();
      testInstance.initializeInstance();
      for (int i = 1; i <= numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("accessorRegion", 0));
      }
   }
}
    
/** Creates and initializes the singleton instance of MemManTest in this VM.
 */
public synchronized static void HydraTask_initDataStore() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new MemManTest();
      testInstance.initializeInstance();
      int numRegions = ResourceManPrms.getNumRegions();
      for (int i = 1; i <= numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("dataStoreRegion", -1));
      }
   }
}

/** Creates and initializes the singleton instance of MemManTest in this VM.
 */
public synchronized static void HydraTask_initDistributedRegion() {
   if (testInstance == null) {
      testInstance = new MemManTest();
      testInstance.initializeInstance();
      int numRegions = ResourceManPrms.getNumRegions();
      for (int i = 1; i <= numRegions; i++) {
         testInstance.regionList.add(testInstance.initializeRegion("distributedRegion", -1));
      }
   }
}
    
/** Run a rebalance.
 */
public static void HydraTask_rebalance() {
   ResourceManager resMan = CacheHelper.getCache().getResourceManager();
   RebalanceFactory factory = resMan.createRebalanceFactory();
   try {
      Log.getLogWriter().info("Starting rebalancing");
      long startTime = System.currentTimeMillis();
      RebalanceOperation rebalanceOp = factory.start();
      RebalanceResults rebalanceResults = rebalanceOp.getResults();
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      Log.getLogWriter().info("Rebalance completed in " + duration + " ms");
      Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
/** Run a rebalance one time for all threads in all vms.
 *  This is run when things are silent, so validation can be done.
 */
public static void HydraTask_rebalanceOnce() {
   long rebalance = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.rebalance);
   if (rebalance == 1) {
      ResourceManager resMan = CacheHelper.getCache().getResourceManager();
      RebalanceFactory factory = resMan.createRebalanceFactory();
      try {
         Log.getLogWriter().info("Starting rebalancing");
         long startTime = System.currentTimeMillis();
         RebalanceOperation rebalanceOp = factory.start();
         RebalanceResults rebalanceResults = rebalanceOp.getResults();
         long endTime = System.currentTimeMillis();
         long duration = endTime - startTime;
         Log.getLogWriter().info("Rebalance completed in " + duration + " ms");
         Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));
         RebalanceUtil.isBalanceImproved(rebalanceResults); // this throws an exception if not improved
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
}
    
/** Verify PR metadata on all PRs.
 */
public static void HydraTask_verifyPRMetaData() {
   long verifyController = ResourceManBB.getBB().getSharedCounters().incrementAndRead(ResourceManBB.verifyController1);
   if (verifyController == 1) {
      for (Region aRegion : testInstance.regionList) {
         Log.getLogWriter().info("Verifying " + aRegion.getFullPath());
         ParRegUtil.verifyPRMetaData(aRegion);
      }
   }
}
    
/** Verify primaries on all PRs.
 */
public static void HydraTask_verifyPrimaries() {
   long verifyController = ResourceManBB.getBB().getSharedCounters().incrementAndRead(ResourceManBB.verifyController2);
   if (verifyController == 1) {
      for (Region aRegion : testInstance.regionList) {
         Log.getLogWriter().info("Verifying " + aRegion.getFullPath());
         int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
         ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
      }
   }
}
    
/** Verify data in the PRs.
 */
public static void HydraTask_verifyBucketCopies() {
   long verifyController = ResourceManBB.getBB().getSharedCounters().incrementAndRead(ResourceManBB.verifyController3);
   if (verifyController == 1) {
      for (Region aRegion : testInstance.regionList) {
         Log.getLogWriter().info("Verifying " + aRegion.getFullPath());
         int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
         ParRegUtil.verifyBucketCopies(aRegion, redundantCopies);
      }
   }
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 *  @param localMaxMemory The localMaxMemory to use for this region.
 *
 *  @returns The created region.
 */
public Region initializeRegion(String regDescriptName, int localMaxMemory) {
   CacheHelper.createCache("cache1");
   AttributesFactory factory = RegionHelper.getAttributesFactory(regDescriptName);
   if (localMaxMemory != -1) {
      RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      PartitionAttributesFactory prFactory = new PartitionAttributesFactory(prAttr);
      prFactory.setLocalMaxMemory(localMaxMemory);
      factory.setPartitionAttributes(prFactory.create());
   }
   String regionName = (String)((TestConfig.tab().vecAt(RegionPrms.regionName)).get(0));
   regionName = regionName + (++regionNameIndex);
   Region aRegion = RegionHelper.createRegion(regionName, factory);
   return aRegion;
}
    
/**
 *  Initialize this test instance
 */
public void initializeInstance() {
   regionList = new ArrayList();
   ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.firstLowMemoryExceptionTime); 
   ResourceManBB.getBB().getSharedCounters().add(ResourceManBB.firstLowMemoryExceptionTime, Long.MAX_VALUE); 
   ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.testStartTime); 
   ResourceManBB.getBB().getSharedCounters().add(ResourceManBB.testStartTime, Long.MAX_VALUE); 
   Log.getLogWriter().info("Resetting all the Counters in ResourceManBB");
}

// ========================================================================
// hydra task methods

/** Do random ops that are heavy towards creates until the system becomes
 *  critical, then run beyond that.
 */
public static void HydraTask_doOpsBeyondCritical() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   testInstance.doOpsWithHeavyCreates(minTaskGranularityMS, true);

   // check to see if we are done
   // we are done when we hit the first low memory exception, then run 2 times
   // longer than that to prove we will not run out of memory
   long firstExceptionTime = ResourceManBB.getBB().getSharedCounters().read(ResourceManBB.firstLowMemoryExceptionTime);
   if (firstExceptionTime < Long.MAX_VALUE) { // we have a first exception
      long testStartTime = ResourceManBB.getBB().getSharedCounters().read(ResourceManBB.testStartTime);
      long timeToFirstException = firstExceptionTime - testStartTime;
      Log.getLogWriter().info("It took " + timeToFirstException + " ms to get the first LowMemoryException. testStartTime="+testStartTime+" timeToFirstException:"+timeToFirstException+" firstExceptionTime:"+firstExceptionTime);
      long timeToEndTest = firstExceptionTime + (2 * timeToFirstException); 
      long now = System.currentTimeMillis();
      long testDuration = now - testStartTime;
      long testDurationSinceLowMemException = now - firstExceptionTime;
      if (now >= timeToEndTest) {
         throw new StopSchedulingOrder("Test has run for " + testDuration + 
               " ms and did not run out of memory, including " + timeToFirstException + 
               " ms to get the first LowMemoryException and " + testDurationSinceLowMemException +
               " ms beyond that");
      } else {
         Log.getLogWriter().info("Test has run for " + testDuration + 
               " ms including " + timeToFirstException + 
               " ms to get the first LowMemoryException; test will run for another " +
               (timeToEndTest - now) + " ms ");
      }
   }
}

/** Do query ops that will take the system to critical.  Continue to run to make sure
 * system does not reach OOME
 */
public static void HydraTask_doQueryAndIndexOps() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   testInstance.doQueryAndIndexOps(minTaskGranularityMS, true);

   // check to see if we are done
   // we are done when we hit the first low memory exception, then run 2 times
   // longer than that to prove we will not run out of memory
   long firstExceptionTime = ResourceManBB.getBB().getSharedCounters().read(ResourceManBB.firstLowMemoryExceptionTime);
   if (firstExceptionTime < Long.MAX_VALUE) { // we have a first exception
      long testStartTime = ResourceManBB.getBB().getSharedCounters().read(ResourceManBB.testStartTime);
      long timeToFirstException = firstExceptionTime - testStartTime;
      Log.getLogWriter().info("It took " + timeToFirstException + " ms to get the first LowMemoryException. testStartTime="+testStartTime+" timeToFirstException:"+timeToFirstException+" firstExceptionTime:"+firstExceptionTime);
      long timeToEndTest = firstExceptionTime + (2 * timeToFirstException); 
      long now = System.currentTimeMillis();
      long testDuration = now - testStartTime;
      long testDurationSinceLowMemException = now - firstExceptionTime;
      if (now >= timeToEndTest) {
         throw new StopSchedulingOrder("Test has run for " + testDuration + 
               " ms and did not run out of memory, including " + timeToFirstException + 
               " ms to get the first LowMemoryException and " + testDurationSinceLowMemException +
               " ms beyond that");
      } else {
         Log.getLogWriter().info("Test has run for " + testDuration + 
               " ms including " + timeToFirstException + 
               " ms to get the first LowMemoryException; test will run for another " +
               (timeToEndTest - now) + " ms ");
      }
   }
}

/** Do random ops that are heavy towards creates until the system becomes
 *  critical.
 */
public static void HydraTask_doOpsUntilCritical() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   boolean becameCritical = testInstance.doOpsWithHeavyCreates(minTaskGranularityMS, true);
   if (becameCritical) {
      throw new StopSchedulingTaskOnClientOrder("System has become critical");
   }
}

/** Do random ops without expecting system to go critical.
 */
public static void HydraTask_doOpsNotCritical() throws LowMemoryException {
   long msToRun = 10000;
   testInstance.doOpsWithHeavyCreates(msToRun, false);
}

/** Do a gc once in this VM
 */
public static void HydraTask_doGC() {
   int value = testInstance.gcController.incrementAndGet();
   if (value == 1) {
      Log.getLogWriter().info("Calling gc...");
      System.gc(); 
      Log.getLogWriter().info("Done calling gc...");
   }
   Log.getLogWriter().info("Sleeping 60000 to allow system to recognize we are not critical");
   MasterController.sleepForMs(60000);
}

// ========================================================================
// 

/** Do random operations that heavly lean toward new entry creation.
 *
 *  @param millisToRun The number of milliseconds to run the ops.
 *  @param allowCritical If true, then allow LowMemoryExceptions.
 *
 *  @returns True if the system became critical, false otherwise. 
 */
protected boolean doOpsWithHeavyCreates(long millisToRun, boolean allowCritical) throws LowMemoryException {
   Log.getLogWriter().info("Running ops for " + millisToRun + " ms");
   ResourceManBB.getBB().getSharedCounters().setIfSmaller(ResourceManBB.testStartTime, System.currentTimeMillis());
   RandomValues randomValues = new RandomValues();
   final long LOG_INTERVAL_MILLIS = 10000;
   long lastLogTime = System.currentTimeMillis();
   long startTime = System.currentTimeMillis();
   GsRandom rand = TestConfig.tab().getRandGen();
   int regionIndex = 0;
   boolean becameCritical = false;

   // useTransactions() defaults to false
   boolean useTransactions = getInitialImage.InitImagePrms.useTransactions();
   boolean rolledback;

   do {
      Region aRegion = regionList.get(regionIndex);
      regionIndex = (regionIndex+1) % regionList.size();
      int randInt = rand.nextInt(1, 100);
  
      rolledback = false;
      if (useTransactions) {
        TxHelper.begin();
      }

      try {
         if (randInt <= 80) { // 80% of the time run ops that increase the region size
            randInt = rand.nextInt(1, 100);
            if (randInt <= 33) { // do a get (with loader to create an entry)
               String key = NameFactory.getNextPositiveObjectName();
               Object result = aRegion.get(key);
               if (result == null) {
                  throw new TestException("get returned null for key " + key + " loader should be installed");
               }
            } else if (randInt <= 66 && !useTransactions) { // do a putAll
               int putAllSize = ResourceManPrms.getPutAllSize();
               if (putAllSize < 1) {
                  throw new TestException("Did not specify a value for ResourceManPrms.putAllSize");
               }
               Map putAllMap = new HashMap();
               for (int i = 1; i <= putAllSize; i++) {
                  String key = NameFactory.getNextPositiveObjectName();
                  BaseValueHolder anObj = new ValueHolder(key, randomValues);
                  putAllMap.put(key, anObj);           
               }
               aRegion.putAll(putAllMap);
            } else { // do a single put
               String key = NameFactory.getNextPositiveObjectName();
               BaseValueHolder anObj = new ValueHolder(key, randomValues);
               aRegion.create(key, anObj);
            }
         } else { // the rest of the time do ops that keep the region size even or decrease it
            randInt = rand.nextInt(1, 100);
            try {
               Iterator it = aRegion.keySet().iterator();
               if (it.hasNext()) {
                  Object key = it.next();
                  if (randInt <= 25) { // invalidate
                     aRegion.invalidate(key);
                  } else if (randInt <= 50) { // destroy
                     aRegion.destroy(key);
                  } else if (randInt <= 75) { // get with existing key
                     aRegion.get(key);
                  } else { // update
                     aRegion.put(key, new ValueHolder(key, randomValues));
                  }
               }
            } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
               // do nothing; this can happen with concurrent tests
            }
         }
      } catch (TransactionDataNodeHasDepartedException e) {
        if (!useTransactions) {
          throw new TestException("Unexpected TransactionDataNodeHasDepartedException " + TestHelper.getStackTrace(e));
        } else {
          Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
          Log.getLogWriter().info("Rolling back transaction.");
          try {
            TxHelper.rollback();
            Log.getLogWriter().info("Done Rolling back Transaction");
          } catch (TransactionException te) {
            Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.");
          }
          rolledback = true;
        }
      } catch (TransactionDataRebalancedException e) {
        if (!useTransactions) {
          throw new TestException("Unexpected Exception " + e + ". " + TestHelper.getStackTrace(e));
        } else {
          Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
          Log.getLogWriter().info("Rolling back transaction.");
          try {
            TxHelper.rollback();
            Log.getLogWriter().info("Done Rolling back Transaction");
          } catch (TransactionException te) {
            Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching Exception " + e + " during tx ops.  Expected, continuing test.");
          }
          rolledback = true;
        }
      } catch (LowMemoryException e) {
         becameCritical = true;
         if (allowCritical) {
            long timeMS = System.currentTimeMillis();
            ResourceManBB.getBB().getSharedCounters().setIfSmaller(ResourceManBB.firstLowMemoryExceptionTime,
                                 new Long(timeMS));
            long value = lowMemCounter.incrementAndGet();
            if (value == 1) { // this is the first low memory exception for this vm
               Log.getLogWriter().info("The first LowMemoryException for this vm has occurred at ms:"+timeMS);
            }
            if (useTransactions) {
               Log.getLogWriter().info("Caught expected LowMemoryException, continuing test");
               Log.getLogWriter().info("Rolling back transaction.");
               TxHelper.rollback();
               rolledback = true;
               Log.getLogWriter().info("Done Rolling back Transaction");
            }
         } else {
            throw e;
         }
      }

      if (useTransactions && !rolledback) {
        try {
          TxHelper.commit();
        } catch (TransactionDataNodeHasDepartedException e) {
          Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
        } catch (TransactionDataRebalancedException e) {
          Log.getLogWriter().info("Caught Exception " + e + " .  Expected with concurrent execution, continuing test.");
        } catch (TransactionInDoubtException e) {
          Log.getLogWriter().info("Caught TransactionInDoubtException.  Expected with concurrent execution, continuing test.");
        } catch (ConflictException e) {
          // can occur with concurrent execution
          Log.getLogWriter().info("Caught ConflictException. Expected with concurrent execution, continuing test.");
        }
      }
   
      // log progress occasionally
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         StringBuffer logStr = new StringBuffer();
         for (Region theRegion : regionList) {
            logStr.append(TestHelper.regionToString(theRegion, false) + " is size " + 
                          theRegion.size() + "\n");
         }
         Log.getLogWriter().info(logStr.toString());
         lastLogTime = System.currentTimeMillis();
      }
   } while (System.currentTimeMillis() - startTime < millisToRun);
   Log.getLogWriter().info("Done running ops for " + millisToRun + " ms");
   return becameCritical;
}

protected boolean doQueryAndIndexOps(long millisToRun, boolean allowCritical) throws LowMemoryException {
  Log.getLogWriter().info("Running query ops for " + millisToRun + " ms");
  ResourceManBB.getBB().getSharedCounters().setIfSmaller(ResourceManBB.testStartTime, System.currentTimeMillis());
  RandomValues randomValues = new RandomValues();
  final long LOG_INTERVAL_MILLIS = 10000;
  long lastLogTime = System.currentTimeMillis();
  final long GC_INTERVAL_MILLIS = 30000;
  long lastGcTime = System.currentTimeMillis();
  long startTime = System.currentTimeMillis();
  GsRandom rand = TestConfig.tab().getRandGen();
  int regionIndex = 0;
  boolean becameCritical = false;
  int numOperationsCanceled = 0;
  int totalQueries = 0;
  int totalIndexCreatesAttempted = 0;
  
  do {
     Region aRegion = regionList.get(regionIndex);
     regionIndex = (regionIndex+1) % regionList.size();
     try {
       QueryService queryService = CacheHelper.getCache().getQueryService();
       String queryString;
       int randInt = rand.nextInt(1,100);
       if (randInt < 5) {
         Index index = queryService.getIndex(aRegion, "testIndex");
         if (index == null) {
           try {
             totalIndexCreatesAttempted++;
             queryService.createIndex("testIndex", "ID", "/" + aRegion.getName());
           }
           catch (IndexExistsException iee) {
             //ignore
           }
           catch (IndexNameConflictException ince) {
             //ignore
           }
           catch (IndexInvalidException iie) {
             if (iie.getMessage() != null && iie.getMessage().equals(LocalizedStrings.IndexCreationMsg_CANCELED_DUE_TO_LOW_MEMORY.toLocalizedString())) {
               throw new QueryExecutionLowMemoryException();
             }
           }
           
           }
         else {
           queryService.removeIndex(index);
         }
       }
       else if (randInt < 15) {
         queryString = "Select * From /" + aRegion.getName();
         Query query = queryService.newQuery(queryString);
         totalQueries++;
         query.execute();
       }
       else {
         queryString = "Select * From /" + aRegion.getName() + " limit 5";
         Query query = queryService.newQuery(queryString);
         totalQueries++;
         query.execute();
       }
     } catch (FunctionDomainException e) {
       throw new TestException("Unexpected function domain exception", e);
     } catch (TypeMismatchException e) {
       throw new TestException("Unexpected type mismatch exception", e);
    } catch (NameResolutionException e) {
      throw new TestException("Unexpected name resolution exception", e);
    } catch (QueryInvocationTargetException e) {
      throw new TestException("Unexpected query invocation target exception", e);
    } catch (QueryExecutionLowMemoryException e) {
      numOperationsCanceled ++;
      becameCritical = true;
      if (allowCritical) {
         long timeMS = System.currentTimeMillis();
         ResourceManBB.getBB().getSharedCounters().setIfSmaller(ResourceManBB.firstLowMemoryExceptionTime,
                              new Long(timeMS));
         long value = lowMemCounter.incrementAndGet();
         if (value == 1) { // this is the first low memory exception for this vm
            Log.getLogWriter().info("The first LowMemoryException for this vm has occurred at ms:"+timeMS);
         }
      } else {
         throw e;
      }
    }
  
     // log progress occasionally
     if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        StringBuffer logStr = new StringBuffer();
        Log.getLogWriter().info("has canceled" + numOperationsCanceled + " operations out of " + totalQueries + " queries and " + totalIndexCreatesAttempted + " attempted index creations");
        lastLogTime = System.currentTimeMillis();
     }
     //execute gc occasionally
     if (System.currentTimeMillis() - lastGcTime > GC_INTERVAL_MILLIS) {
       System.gc();
       Log.getLogWriter().info("gc... hopefully clean up some query garbage");
       lastGcTime = System.currentTimeMillis();
     }
  } while (System.currentTimeMillis() - startTime < millisToRun);
  Log.getLogWriter().info("Done running query ops for " + millisToRun + " ms.  Low memory canceled " + numOperationsCanceled + " operations out of " + totalQueries + " queries and " + totalIndexCreatesAttempted + " attempted index creations");
  return becameCritical;
}



}
