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
package getInitialImage; 

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import parReg.ParRegUtilVersionHelper;
import parReg.colocation.Month;
import parReg.execute.RegionOperationsFunction;
import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import util.BaseValueHolder;
import util.CacheDefinition;
import util.CacheUtil;
import util.EventCountersBB;
import util.KeyIntervals;
import util.NameBB;
import util.NameFactory;
import util.RandomValues;
import util.RegionDefinition;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.TxHelper;
import util.ValueHolder;
import util.ValueHolderPrms;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import delta.DeltaValueHolder;

public class InitImageTest {

// the one instance of InitImageTest
static protected InitImageTest testInstance;

// region name
protected static final String REGION_NAME = "TestRegion";

// protected fields used by the test to do its work
protected RandomValues randomValues = null;
       // used for getting random values for the region
protected int numNewKeys;
       // the number of new keys to add to the region
public KeyIntervals keyIntervals;
       // the key intervals used for this test; test test does a different
       // operation on each interval, so it can be validated at the end
protected int totalNumKeys;
       // the total number of keys expected in the region after all operations are
       // done; includes those in keyIntervals and numNewKeys
public Region aRegion;
       // the region used in this test for getInitialImage
protected long createRegionDurationMS = 0;
       // the number of millis it took to do a create region, which includes getInitialImage
protected long giiDurationMS = 0;
       // the number of millis it took to do a getInitialImage 
protected hydra.blackboard.SharedCounters sc;
       // the shared counters for this tests's blackboard
protected int sleepMSAfterOps = -1;
       // MS to sleep after all ops are done to allow mirrors to receive data
protected boolean supportsConcurrentMap = true;
       // concurrentMap ops (putIfAbsent, replace and remove) are not supported on peers with empty or normal dataPolicy

// operations to do on the region
protected static final int INVALIDATE             = 1;
protected static final int LOCAL_INVALIDATE       = 2;
protected static final int DESTROY                = 3;
protected static final int LOCAL_DESTROY          = 4;
protected static final int UPDATE_EXISTING_KEY    = 5;
protected static final int GET                    = 6;
protected static final int ADD_NEW_KEY            = 7;
protected static final int PUTALL_NEW_KEY         = 8;
protected static final int FIRST_OP               = 1;
protected static final int LAST_OP                = 8;
protected static final int operations[] = new int[] {
   INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY, UPDATE_EXISTING_KEY, GET, ADD_NEW_KEY, PUTALL_NEW_KEY};

protected final long LOG_INTERVAL_MILLIS = 10000;

final static String objectType = TestConfig.tab().stringAt(
    ValueHolderPrms.objectType, "util.ValueHolder");

// ======================================================================== 
// initialization methods 

/** Initialize the single instance of this test class and a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
protected synchronized static void initializeRegion(DataPolicy dataPolicy) {
   if (testInstance == null) {
      testInstance = new InitImageTest();
      testInstance.initRegion(dataPolicy);
      testInstance.initInstance();
   }
}

/** Initialize the single instance of this test class but not a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
protected synchronized static void initializeInstance() {
   if (testInstance == null) {
      testInstance = new InitImageTest();
      testInstance.initInstance();
   }
}

/** Initialize a region with the desired attributes. Initialize instance
 *  fields for this instance. 
 */
protected void initRegion(DataPolicy dataPolicy) {
   CacheLoader loader = getCacheLoader();
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();

   regDef.setDataPolicy(dataPolicy);

   Log.getLogWriter().info("Using RegionDefinition " + regDef + " to create region");


   aRegion = regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, 
                                     null, loader, null);
   if ((dataPolicy.equals(DataPolicy.NORMAL)) || 
       (dataPolicy.equals(DataPolicy.EMPTY))) {
      supportsConcurrentMap = false;
   } 
   sleepMSAfterOps = 30000;
}

/** Initialize fields for this instance
 */
public void initInstance() {
   numNewKeys = TestConfig.tab().intAt(InitImagePrms.numNewKeys, -1);
   keyIntervals = (KeyIntervals)(InitImageBB.getBB().getSharedMap().get(InitImageBB.KEY_INTERVALS));
   Log.getLogWriter().info("initInstance, keyIntervals read from blackboard = " + keyIntervals.toString());
   int numDestroyed = keyIntervals.getNumKeys(KeyIntervals.DESTROY);
   int numKeyIntervals = keyIntervals.getNumKeys();
   totalNumKeys = numKeyIntervals + numNewKeys - numDestroyed;
   sc = InitImageBB.getBB().getSharedCounters();
   randomValues = new RandomValues();
   Log.getLogWriter().info("numKeyIntervals is " + numKeyIntervals);
   Log.getLogWriter().info("numNewKeys is " + numNewKeys);
   Log.getLogWriter().info("numDestroyed is " + numDestroyed);
   Log.getLogWriter().info("totalNumKeys is " + totalNumKeys);
   InitialImageOperation.slowImageProcessing = TestConfig.tab().intAt(InitImagePrms.giiPerEntrySleepMS, 0);
}

/** Hydra start task to initialize key intervals, which are ranges of
 *  keys which are to have an operation done on them (invalidate, destroy, etc)
 */
public synchronized static void StartTask_initialize() {
   // write the dataPolicy attributes to the blackboard for the caches used as sources to getInitialImage
   String giiSourceDataPolicy = TestConfig.tab().stringAt(InitImagePrms.giiSourceDataPolicy);
   StringTokenizer tokenizer = new StringTokenizer(giiSourceDataPolicy, "-");
   int numTokens = tokenizer.countTokens();
   if (numTokens != 2)
      throw new TestException("Unable to get two dataPolicy attributes from " +
            "InitImagePrms.giiSourceDataPolicy" + giiSourceDataPolicy);
   String dataPolicy1 = tokenizer.nextToken();
   String dataPolicy2 = tokenizer.nextToken();
   InitImageBB.getBB().getSharedMap().put(InitImageBB.DATAPOLICY1_ATTR, dataPolicy1);
   InitImageBB.getBB().getSharedMap().put(InitImageBB.DATAPOLICY2_ATTR, dataPolicy2);

   // initialize keyIntervals; dataPolicy2 is the dataPolicy attribute for the VM that
   // does the operations; if dataPolicy2 is 'replicate', 
   // then local invalidates and local destroys are not allowed (on a replicated region)

   // Starting with GemFire 7.0, transactions on regions with normal dataPolicy throw 
   // UnsupportedOperationException on local operations (so disallow these ops if useTransactions is set)
   boolean useTransactions = InitImagePrms.useTransactions();
   int numKeys = TestConfig.tab().intAt(InitImagePrms.numKeys);
   KeyIntervals intervals = null;
   if (!TestHelper.getDataPolicy(dataPolicy2).withReplication() && !useTransactions) { // allow all operations
      intervals = new KeyIntervals(numKeys);
   } else { // replication is present
      intervals = new KeyIntervals(new int[] {KeyIntervals.NONE, KeyIntervals.INVALIDATE,
                                              KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
                                              KeyIntervals.GET}, 
                                   numKeys);
   }
   InitImageBB.getBB().getSharedMap().put(InitImageBB.KEY_INTERVALS, intervals);
   Log.getLogWriter().info("Created keyIntervals: " + intervals);

   // Set the counters for the next keys to use for each operation
   hydra.blackboard.SharedCounters sc = InitImageBB.getBB().getSharedCounters();
   sc.setIfLarger(InitImageBB.LASTKEY_INVALIDATE, intervals.getFirstKey(KeyIntervals.INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_INVALIDATE, intervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_DESTROY, intervals.getFirstKey(KeyIntervals.DESTROY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_DESTROY, intervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY, intervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_GET, intervals.getFirstKey(KeyIntervals.GET)-1);

   // show the blackboard
   InitImageBB.getBB().printSharedMap();
   InitImageBB.getBB().printSharedCounters();
}

// ======================================================================== 
// hydra task methods

/** Hydra task to initialize a region and load it according to hydra param settings. 
 */
public static void HydraTask_loadRegion() {
   // this task gets its dataPolicy attribute from the InitImageBB.DATAPOLICY1_ATTR
   String dataPolicyStr = TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute, null);
   if (dataPolicyStr != null) {
      throw new TestException("This task must get its dataPolicy attribute from " +
            "InitimageBB.giiSourceDataPolicy, but the task attribute also specified dataPolicy " + 
            dataPolicyStr);
   }
   dataPolicyStr = (String)InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY1_ATTR);
   DataPolicy dataPolicyAttr = TestHelper.getDataPolicy(dataPolicyStr);
   initializeRegion(dataPolicyAttr);
   testInstance.loadRegion();
}

/** Hydra task to initialize a region and load it by getting values from another 
 *  distributed system using gets.
 */
public static void HydraTask_loadRegionWithGets() {
   // this task gets its dataPolicy attribute from the InitImageBB.DATAPOLICY2_ATTR
   String dataPolicyStr = TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute, null);
   if (dataPolicyStr != null) {
      throw new TestException("This task must get its dataPolicy attribute from " +
            "InitimageBB.giiSourceDataPolicy, but the task attribute also specified dataPolicy" + 
            dataPolicyStr);
   }
   dataPolicyStr = (String)InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY2_ATTR);
   DataPolicy dataPolicyAttr = TestHelper.getDataPolicy(dataPolicyStr);
   initializeRegion(dataPolicyAttr);
   testInstance.loadRegionWithGets();
}

/** Hydra task to perform operations according to keyIntervals on an previously
 *  initialized region.
 */
public static void HydraTask_doOps() {
   BitSet availableOps = new BitSet(operations.length);
   availableOps.flip(FIRST_OP, LAST_OP+1);
   testInstance.doOps(availableOps);
}

/** Hydra task to perform local destroy operations, according to keyIntervals on a previously
 *  initialized region.
 */
public static void HydraTask_doPutAllNewKey() {
   BitSet availableOps = new BitSet(operations.length);
   availableOps.set(PUTALL_NEW_KEY);
   testInstance.doOps(availableOps);
}

/** Hydra task to do a get region, which does a getInitialImage.
 */
public static void HydraTask_doGetInitImage() {
   initializeInstance();
   testInstance.doGetInitImage(true);
}
/** Hydra task to do a get region, which does a getInitialImage.
 */
public static void HydraTask_doGetInitImageWithoutMonitor() {
   initializeInstance();
   testInstance.doGetInitImage(false);
}

/** Hydra task to wait until another thread does a getInitialImage, then this 
 *  get region should block.
 */
public static void HydraTask_blockedGetRegion() {
   initializeInstance();
   testInstance.blockedGetRegion();
}

/** Hydra task to verify the keys and values in a region using keyIntervals.
 */
public static void HydraTask_verifyRegionContents() {
   try {
      testInstance.verifyRegionContents();
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Hydra task to verify event counters.
 */
public static void HydraTask_verifyEventCounters() {
   testInstance.verifyEventCounters();
}

/** Hydra task to initialize the cache
 */
public synchronized static void HydraTask_initCache() {
   if (CacheUtil.getCache() == null) {
      CacheDefinition cacheDef = CacheDefinition.createCacheDefinition(util.CacheDefPrms.cacheSpecs , "cache1");
      cacheDef.createCache(); 
   }
}

/** Hydra task to verify that operations concurrent to getInitialImage
 *  are non-blocking.
 */
public static void HydraTask_verifyNonBlocking() {
   testInstance.verifyNonBlocking();
}

/** Hydra task to do gets with new keys (ie not anywhere in the system)
 */
public static void HydraTask_doNewKeyGets() {
   testInstance.doNewKeyGets();
}

/** Hydra task to do operations that will cause a CacheWriter to be
 *  invoked in the VM doing a getInitialImage.
 */
public static void HydraTask_doCacheWriterTest() {
   initializeRegion(null); 
   testInstance.doOpsPlusPuts();
}

/** Hydra task to verify that a CacheWriters are not invoked during a GII.
 */
public static void HydraTask_verifyCacheWriterTest() {
   testInstance.verifyCacheWriterTest();
}

// ======================================================================== 
// methods to do the work of the tasks

/** Get the region REGION_NAME, which should do a getInitialImage. Verify
 *  that the getInitialImage occurred by checking the stat for completion
 *  of getInitialImage. Save the number of millis taken for the getRegion
 *  in the createRegionDurationMS instance field.
 */
protected void doGetInitImage(boolean withMonitor) {
  // start a thread to monitor when getInitialImage starts and stops
  // when it starts, increment a blackboard counter, when it stops, decrement the blackboard counter
  final StringBuffer errStr = new StringBuffer();
  Thread monitorThread = new Thread(new Runnable() {
    public void run() {
      try {
        long start = 0;
        Log.getLogWriter().info("Monitor thread: Waiting for getInitialImage to begin...");
        while (true) { // wait for gii to begin and increment a blackboard counter
          if (isLocalGiiInProgress()) {
            // region does not have a value yet; cannot use isLocalGiiInPrgress with region argument
            start = System.currentTimeMillis();
            Log.getLogWriter().info("Monitor thread: getInitialImage is in progress");
            sc.increment(InitImageBB.GII_IN_PROGRESS);
            break;
          }
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
        Log.getLogWriter().info("Monitor thread: Waiting for getInitialImage to complete...");
        while (true) { // wait for gii to end and decrement a blackboard counter
          if (hasLocalGiiCompleted()) {
            giiDurationMS = (System.currentTimeMillis() - start);
            Log.getLogWriter().info("Monitor thread: getInitialImage has completed");
            sc.decrement(InitImageBB.GII_IN_PROGRESS);
            break;
          }
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable aThrowable) {
        errStr.append(aThrowable.getMessage() + " " + TestHelper.getStackTrace(aThrowable));
      }
    }
  });
  if (withMonitor) {
    monitorThread.start();
  }

  // do the getInitialImage
  long begin = 0;
  long end = 0;
  Cache aCache = CacheUtil.createCache();
  RegionDefinition regDef = RegionDefinition.createRegionDefinition();

  // this task gets its useTransactions from the tasktab
  boolean useTransactions = InitImagePrms.useTransactions();

  // this task gets its dataPolicy attribute from the tasktab
  regDef.setDataPolicy(TestHelper.getDataPolicy(TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute)));

  CacheWriter writer = getCacheWriter();
  CacheLoader loader = getCacheLoader();

  RegionAttributes attr = regDef.getRegionAttributes(new GiiListener(), loader, writer);

  // put the gii code into a transaction to see that we don't
  // get any conflicts from the invocation of the loader
  if (useTransactions) {
    TxHelper.begin();
  }

  try {
    Log.getLogWriter().info("In doGetinitImage, creating VM region " + REGION_NAME +
                            " with " + TestHelper.regionAttributesToString(attr));
    begin = System.currentTimeMillis();
    aRegion = aCache.createVMRegion(REGION_NAME, attr);
    end = System.currentTimeMillis();
    Log.getLogWriter().info("In doGetinitImage, done creating VM region " + REGION_NAME);
    // Check to make sure that the GII process has finished
    int nbrOfGiiInProgress = TestHelper.getStat_getInitialImagesInProgress(REGION_NAME);
    Log.getLogWriter().info("In doGetinitImage, checking " + REGION_NAME + " for in progress: nbrOfGiiInProgress=" + nbrOfGiiInProgress);
    if (nbrOfGiiInProgress > 0) {
      throw new TestException("After creating region, GII is still in progress ");
    }
  } catch (RegionExistsException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  } catch (TimeoutException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  }

  if (useTransactions) {
    TxHelper.commitExpectSuccess();
  }

  // log results
  createRegionDurationMS = end - begin;
  int regionSize = aRegion.keys().size();
  Log.getLogWriter().info("In doGetInitImage, creating region took " + createRegionDurationMS +
                          " millis, current region size is " + regionSize);
  if (InitImagePrms.giiHasSourceMember()) {
    if (regionSize == 0) {
      throw new TestException("After creating region, unexpected size " + regionSize);
    }
  } else {
    if (regionSize != 0) {
      throw new TestException("After creating region, expected region size to be 0 (there are no source members for gii), but the size is " + regionSize);
    }
  }
  if (withMonitor) {
    waitForLocalGiiCompleted();
    try {
      monitorThread.join(30000);
    } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  if (errStr.length() != 0) {
    throw new TestException(errStr);
  }
}

/** Wait for another thread in this VM to begin a getInitialImage, then get the region 
 *  REGION_NAME, which should block while somebody else has a getInitialImage in progress.
 *  When finished, compare time to createRegionDurationMS (the time taken by the thread that did the
 *  getInitialImage) to verify that this blocked. Also, check stats to make sure this thread
 *  did not do another getInitialImage.
 */
protected void blockedGetRegion() {
   // creating a distAckRegion will cause one gii; save the current count
   Cache aCache = CacheUtil.createCache();
   waitForLocalGiiToBegin();
   Log.getLogWriter().info("In blockedGetRegion, getting region " + REGION_NAME);
   long start = System.currentTimeMillis();
   Region myRegion = aCache.getRegion(REGION_NAME);
   long end = System.currentTimeMillis();
   Log.getLogWriter().info("In blockedGetRegion, done getting region " + REGION_NAME);
   long duration = end - start;
   Log.getLogWriter().info("In blockedGetRegion, region creation took " + duration + " millis");

   // check that only the thread doing the getInitialImage actually did a getInitialImage;
   // we want to know that this thread's call to getRegion took about the same time as the
   // getInitialImage thread because it blocked, not because is also executed getInitialImage
   int numCompleted = TestHelper.getStat_getInitialImagesCompleted(REGION_NAME);
   if (numCompleted > 1) {
      throw new TestException("Expected only 1 getInitialImage to be completed, but num completed is " + numCompleted);
   } 

   // now check that the time for this thread is close to the time for the getInitialImageThread
   float blockPercent = ((float)duration / (float)giiDurationMS) * 100;
   if (blockPercent < 75) {
      throw new TestException("Expected the thread that blocked on getInitialImage to have a " +
         "similar time to the thread that did the getInitialImage, blocked thread millis: " + 
         duration + ", getInitialImage time: " + giiDurationMS);
   }
}

/** Load a region with keys and values. The number of keys and values is specified
 *  by the total number of keys in keyIntervals. This can be invoked by several threads
 *  to accomplish the work.
 */
public void loadRegion() {
   final long LOG_INTERVAL_MILLIS = 10000;
   int numKeysToCreate = keyIntervals.getNumKeys();
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
   long minTaskGranularityMS = -1;
   if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
      long shouldAddCount = sc.incrementAndRead(InitImageBB.SHOULD_ADD_COUNT);
      if (shouldAddCount > numKeysToCreate) {
         String aStr = "In loadRegion, shouldAddCount is " + shouldAddCount +
                       ", numOriginalKeysCreated is " + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED) +
                       ", numKeysToCreate is " + numKeysToCreate + ", region size is " + aRegion.size();
         Log.getLogWriter().info(aStr);
         NameBB.getBB().printSharedCounters();
         throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getNextPositiveObjectName();
      boolean useCacheLoader = TestConfig.tab().booleanAt(InitImagePrms.useCacheLoader);
      try {
         if (useCacheLoader) {
            Object value = aRegion.get(key);
//            Log.getLogWriter().info("Loading with cacheLoader, key " + key + ", value " + TestHelper.toString(value));
         } else {
            Object value = getValueToAdd(key);
            if (TestConfig.tab().booleanAt(
              parReg.ParRegPrms.isWithRoutingResolver, false)) {
            Month callBackArg = Month.months[TestConfig.tab().getRandGen()
                .nextInt(11)];
            InitImageBB.getBB().getSharedMap().put(key, callBackArg);
            if (TestConfig.tab().booleanAt(
                InitImagePrms.isFunctionExecutionTest, false)) {
              Log.getLogWriter().info("Populating with function execution.");
              ArrayList aList = new ArrayList();
              aList.add("addKey");
              aList.add(RemoteTestModule.getCurrentThread().getThreadId());
              final Set<Object> keySet = new HashSet<Object>();
              keySet.add(key);
              Function addKeyFunction = new RegionOperationsFunction();
              Execution dataSet = FunctionService.onRegion(aRegion);
              try {
                ResultCollector drc = dataSet.withFilter(keySet).withArgs(
                    aList).execute(addKeyFunction);
                drc.getResult();
              }
              catch (Exception e) {
                e.printStackTrace();
                throw new TestException("Got this exception " + e.getMessage()
                    + " Cause " + e.getCause(), e);
              }
            }
            else {
              aRegion.put(key, value, callBackArg);
            }
          }
          else {
            aRegion.put(key, value);
          }
// Log.getLogWriter().info("Loading with put, key " + key + ", value " +
// TestHelper.toString(value));
         }
         sc.increment(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + " out of " + numKeysToCreate + 
             " entries into " + TestHelper.regionToString(aRegion, false));
         lastLogTime = System.currentTimeMillis();
      }
   } while ((minTaskGranularitySec == -1) ||
            (System.currentTimeMillis() - startTime < minTaskGranularityMS));
}

/** Loads a region with keys and values by getting the value from another
 *  distributed system. The number of keys and values loaded are specified
 *  by the number of keys in keyIntervals. Can be invoked by several threads
 *  to accomplish the work.
 */
public void loadRegionWithGets() {
   final long LOG_INTERVAL_MILLIS = 10000;
   int totalSize = keyIntervals.getNumKeys();
   long lastLogTime = System.currentTimeMillis();
   while (true) {
      long numGets = sc.incrementAndRead(InitImageBB.NUM_GETS);
      if (numGets > totalSize) {
         Log.getLogWriter().info("In loadRegionWithGets, regionSize is " + aRegion.keys().size());
         return;
      }
      Object key = NameFactory.getObjectNameForCounter(numGets);
      try {
         Object aValue = aRegion.get(key);
//       Log.getLogWriter().info("Loading with gets, key " + key + ", value " + TestHelper.toString(aValue));
         if (aValue == null)
            throw new TestException("Unexpected value " + aValue + " from getting key " + key);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Got " + sc.read(InitImageBB.NUM_GETS) + " out of " + totalSize + 
             " entries into " + TestHelper.regionToString(aRegion, false));
         lastLogTime = System.currentTimeMillis();
      }
   }
}

/** Do operations on the REGION_NAME's keys using keyIntervals to specify
 *  which keys get which operations. This will return when all operations
 *  in all intervals have completed.
 *
 *  @param availableOps - Bits which are true correspond to the operations
 *                        that should be executed.
 */
public void doOps(BitSet availableOps) {
//   long startTime = System.currentTimeMillis();

   // this task gets its useTransactions from the tasktab
   boolean useTransactions = InitImagePrms.useTransactions();

   while (availableOps.cardinality() != 0) {
      int whichOp = getOp(availableOps, operations.length);
      boolean doneWithOps = false;

      if (useTransactions) {
        TxHelper.begin();
      }

      switch (whichOp) {
         case ADD_NEW_KEY: 
           doneWithOps = addNewKey();
            break;
         case PUTALL_NEW_KEY: 
             // todo@lhughes -- initially, tx putAll not supported in 6.5
             // re-enable once supported
             if (useTransactions) {
               doneWithOps = true;
             } else {
               doneWithOps = putAllNewKey();
             }
             break;
         case INVALIDATE:
            doneWithOps = invalidate();
            break;
         case DESTROY:
            doneWithOps = destroy();
            break;
         case UPDATE_EXISTING_KEY:
            doneWithOps = updateExistingKey();
            break;
         case GET:
            doneWithOps = get();
            break;
         case LOCAL_INVALIDATE:
            doneWithOps = localInvalidate();
            break;
         case LOCAL_DESTROY:
            doneWithOps = localDestroy();
            break;
         default: {
            throw new TestException("Unknown operation " + whichOp);
         }
      }

      if (useTransactions) {
        try {
          TxHelper.commit();
        } catch (ConflictException e) {
          // currently not expecting any conflicts ...
          throw new TestException("Unexpected ConflictException " + TestHelper.getStackTrace(e));
        }
      }

      if (doneWithOps) {
         Log.getLogWriter().info("Done with operation " + whichOp);
         availableOps.clear(whichOp);
      }
   }
   Log.getLogWriter().info("Done in doOps");
   
   if (sleepMSAfterOps > 0) {
      // sleep to allow data to be distributed (replication)
      try {
         Log.getLogWriter().info("Sleeping for " + sleepMSAfterOps + " millis to allow ops to be distributed");
         Thread.sleep(sleepMSAfterOps);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
}

/** Add a new key to REGION_NAME.
 *
 *  @return true if all new keys have been added (specified by InitImagePrms.numNewKeys)
 */
protected boolean addNewKey() {
   long numNewKeysCreated = sc.incrementAndRead(InitImageBB.NUM_NEW_KEYS_CREATED);
   if (numNewKeysCreated > numNewKeys) {
      Log.getLogWriter().info("All new keys created; returning from addNewKey");
      return true;
   }
   Object key = NameFactory.getNextPositiveObjectName();
   boolean useCacheLoader = TestConfig.tab().booleanAt(InitImagePrms.useCacheLoader);
   if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
        false)) {
      Month callBackArg = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      InitImageBB.getBB().getSharedMap().put(key, callBackArg);
    }
   checkContainsValueForKey(key, false, "before addNewKey");
   try {
      if (useCacheLoader) {
//         Log.getLogWriter().info("Adding new key " + key + " with get");
         boolean inProgressBefore = isAnyGiiInProgress();
         long start = System.currentTimeMillis();
         aRegion.get(key);
         long end = System.currentTimeMillis();
         boolean inProgressAfter = isAnyGiiInProgress();
         long duration = end - start;
         Log.getLogWriter().info("Done adding new key " + key + " with get, " +
             "time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
             ", inProgressAfter: " + inProgressAfter + 
             ", num remaining: " + (numNewKeys - numNewKeysCreated));
         sc.setIfLarger(InitImageBB.MAX_TIME_NEW_KEY, duration);
         if (inProgressBefore && inProgressAfter) {
            sc.increment(InitImageBB.NEW_KEY_COMPLETED);
         }
      } else {
         Object value = createObject((String)key, randomValues);
//         Log.getLogWriter().info("Adding new key " + key + " with put");
         boolean inProgressBefore = isAnyGiiInProgress();
         long start = System.currentTimeMillis();
         if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
            false)) {
          Month callBackArg = (Month)InitImageBB.getBB().getSharedMap()
              .get(key);
          aRegion.put(key, value, callBackArg);
          Log.getLogWriter().info(
              "Inside addNewKey(), did create with callback");

        }
        else {
          if (supportsConcurrentMap && TestConfig.tab().getRandGen().nextBoolean()) {
            Log.getLogWriter().info("Adding new key " + key + " with putIfAbsent");
            aRegion.putIfAbsent(key, value);
            Log.getLogWriter().info("Done with putIfAbsent for " + key);
          } else {
          aRegion.put(key, value);
        }
        }
         long end = System.currentTimeMillis();
         boolean inProgressAfter = isAnyGiiInProgress();
         long duration = end - start;
         Log.getLogWriter().info("Done adding new key " + key + " with put, " +
             "time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
             ", inProgressAfter: " + inProgressAfter + 
             ", num remaining: " + (numNewKeys - numNewKeysCreated));
         sc.setIfLarger(InitImageBB.MAX_TIME_NEW_KEY, duration);
         if (inProgressBefore && inProgressAfter) {
            sc.increment(InitImageBB.NEW_KEY_COMPLETED);
         }
      }
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } 
   return (numNewKeysCreated >= numNewKeys);
}

/** Add a group of new keys via putAll to REGION_NAME.
*
*  @return true if all new keys have been added (specified by InitImagePrms.numNewKeys)
*/
protected boolean putAllNewKey() {
  long numNewKeysCreated = sc.incrementAndRead(InitImageBB.NUM_NEW_KEYS_CREATED);
  if (numNewKeysCreated > numNewKeys) {
     Log.getLogWriter().info("All new keys created; returning from putAllNewKey");
     return true;
  }
  Object key = NameFactory.getNextPositiveObjectName();
  boolean useCacheLoader = TestConfig.tab().booleanAt(InitImagePrms.useCacheLoader);
  checkContainsValueForKey(key, false, "before addNewKey");
  try {
     if (useCacheLoader) {
//        Log.getLogWriter().info("Adding new key " + key + " with get");
        boolean inProgressBefore = isAnyGiiInProgress();
        long start = System.currentTimeMillis();
        aRegion.get(key);
        long end = System.currentTimeMillis();
        boolean inProgressAfter = isAnyGiiInProgress();
        long duration = end - start;
        Log.getLogWriter().info("Done adding new key " + key + " with get, " +
            "time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
            ", inProgressAfter: " + inProgressAfter + 
            ", num remaining: " + (numNewKeys - numNewKeysCreated));
        sc.setIfLarger(InitImageBB.MAX_TIME_NEW_KEY, duration);
        if (inProgressBefore && inProgressAfter) {
           sc.increment(InitImageBB.NEW_KEY_COMPLETED);
        }
     } else {
        Object value = createObject((String)key, randomValues);
//        Log.getLogWriter().info("Adding new key " + key + " with put");
        boolean inProgressBefore = isAnyGiiInProgress();
        long start = System.currentTimeMillis();
        HashMap map = new HashMap();
        map.put(key, value);
        aRegion.putAll(map);
        long end = System.currentTimeMillis();
        boolean inProgressAfter = isAnyGiiInProgress();
        long duration = end - start;
        Log.getLogWriter().info("Done adding new key " + key + " with putall, " +
            "time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
            ", inProgressAfter: " + inProgressAfter + 
            ", num remaining: " + (numNewKeys - numNewKeysCreated));
        sc.setIfLarger(InitImageBB.MAX_TIME_NEW_KEY, duration);
        if (inProgressBefore && inProgressAfter) {
           sc.increment(InitImageBB.NEW_KEY_COMPLETED);
        }
     }
  } catch (TimeoutException e) {
     throw new TestException(TestHelper.getStackTrace(e));
  } catch (CacheWriterException e) {
     throw new TestException(TestHelper.getStackTrace(e));
  } catch (CacheLoaderException e) {
     throw new TestException(TestHelper.getStackTrace(e));
  } 
  return (numNewKeysCreated >= numNewKeys);
}

/** Update an existing key in region REGION_NAME. The keys to update are specified
 *  in keyIntervals.
 *
 *  @return true if all keys to be updated have been completed.
 */
protected boolean updateExistingKey() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY);
   if (!keyIntervals.keyInRange(KeyIntervals.UPDATE_EXISTING_KEY, nextKey)) {
      Log.getLogWriter().info("All existing keys updated; returning from updateExistingKey");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
   try {
      BaseValueHolder existingValue = (BaseValueHolder)aRegion.get(key);
      BaseValueHolder newValue = createObject((String)key, randomValues);
      if (existingValue == null)
         throw new TestException("Get of key " + key + " returned unexpected " + existingValue);
      if (existingValue.myValue instanceof String)
         throw new TestException("Trying to update a key which was already updated: " + existingValue.myValue);
      newValue.myValue = "updated_" + NameFactory.getCounterForName(key);
//      Log.getLogWriter().info("Updating existing key " + key + " with value " + TestHelper.toString(newValue));
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        aRegion.put(key, newValue, callBackArg);
        Log.getLogWriter().info("Inside update(), did update with callback");
      }
      else {
        if (supportsConcurrentMap && TestConfig.tab().getRandGen().nextBoolean()) {
           Log.getLogWriter().info("Replacing existing key " + key + " with value " + TestHelper.toString(newValue));
           aRegion.replace(key, newValue);
           Log.getLogWriter().info("Replaced key " + key);
        } else {
        aRegion.put(key, newValue);
      }
      }
      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done updating existing key " + key + " with value " + 
          TestHelper.toString(newValue) + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY) - nextKey));
      sc.setIfLarger(InitImageBB.MAX_TIME_UPDATE_EXISTING_KEY, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.UPDATE_EXISTING_KEY_COMPLETED);
      }
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } 
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY));
}

/** Locally invalidate a key in region REGION_NAME. The keys to locally invalidate
 *  are specified in keyIntervals.
 *
 *  @return true if all keys to be locally invalidated have been completed.
 */
protected boolean localInvalidate() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_INVALIDATE);
   if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_INVALIDATE, nextKey)) {
      Log.getLogWriter().info("All local invalidates completed; returning from localInvalidate");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
//   Log.getLogWriter().info("Locally invalidating " + key);
   try {
      checkContainsValueForKey(key, true, "before localInvalidate");
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        aRegion.localInvalidate(key, callBackArg);
        Log.getLogWriter().info(
            "Inside local invalidate(), did invalidate with callback");
      }
      else {
        aRegion.localInvalidate(key);
      }
      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done locally invalidating " + key + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE) - nextKey));
      sc.setIfLarger(InitImageBB.MAX_TIME_LOCAL_INVALIDATE, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.LOCAL_INVALIDATE_COMPLETED);
      }
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE));
}
   
/** Invalidate a key in region REGION_NAME. The keys to invalidate
 *  are specified in keyIntervals.
 *
 *  @return true if all keys to be invalidated have been completed.
 */
protected boolean invalidate() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_INVALIDATE);
   if (!keyIntervals.keyInRange(KeyIntervals.INVALIDATE, nextKey)) {
      Log.getLogWriter().info("All existing keys invalidated; returning from invalidate");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
//   Log.getLogWriter().info("Invalidating " + key);
   checkContainsValueForKey(key, true, "before invalidate");
   try {
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        aRegion.invalidate(key, callBackArg);
        Log.getLogWriter().info(
            "Inside invalidate(), did invalidate with callback");
      }
      else {
        aRegion.invalidate(key);
      }

      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done invalidating " + key + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.INVALIDATE) - nextKey));
      sc.setIfLarger(InitImageBB.MAX_TIME_INVALIDATE, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.INVALIDATE_COMPLETED);
      }
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.INVALIDATE));
}
   
/** Locally destroy a key in region REGION_NAME. The keys to locally destroy
 *  are specified in keyIntervals.
 *
 *  @return true if all keys to be locally destroyed have been completed.
 */
protected boolean localDestroy() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_LOCAL_DESTROY);
   if (!keyIntervals.keyInRange(KeyIntervals.LOCAL_DESTROY, nextKey)) {
      Log.getLogWriter().info("All local destroys completed; returning from localDestroy");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
//   Log.getLogWriter().info("Locally destroying " + key);
   checkContainsValueForKey(key, true, "before localDestroy");
   try {
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        aRegion.localDestroy(key, callBackArg);
        Log.getLogWriter().info(
            "Inside local destroy(), did destroy with callback");
      }
      else {
        aRegion.localDestroy(key);
      }
      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done locally destroying " + key + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY) - nextKey));
      sc.setIfLarger(InitImageBB.MAX_TIME_LOCAL_DESTROY, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.LOCAL_DESTROY_COMPLETED);
      }
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY));
}
   
/** Destroy a key in region REGION_NAME. The keys to destroy
 *  are specified in keyIntervals.
 *
 *  @return true if all keys to be destroyed have been completed.
 */
protected boolean destroy() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_DESTROY);
   if (!keyIntervals.keyInRange(KeyIntervals.DESTROY, nextKey)) {
      Log.getLogWriter().info("All destroys completed; returning from destroy");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
   Log.getLogWriter().info("Destroying " + key);
   checkContainsValueForKey(key, true, "before destroy");
   try {
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        aRegion.destroy(key, callBackArg);
        Log.getLogWriter().info("Inside destroy(), did destroy with callback");
      }
      else {
        if (supportsConcurrentMap && TestConfig.tab().getRandGen().nextBoolean()) {
           Log.getLogWriter().info("Removing " + key);
           aRegion.remove(key, aRegion.get(key));
           Log.getLogWriter().info("Done removing " + key);
        } else {
        aRegion.destroy(key);
      }
      }
      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done Destroying " + key + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.DESTROY) - nextKey));
      sc.setIfLarger(InitImageBB.MAX_TIME_DESTROY, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.DESTROY_COMPLETED);
      }
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.DESTROY));
}

/** Do a get on a key in region REGION_NAME. Keys to get are specified
 *  in keyIntervals.
 *
 *  @return true if all keys to have get performaed have been completed.
 */
protected boolean get() {
   long nextKey = sc.incrementAndRead(InitImageBB.LASTKEY_GET);
   if (!keyIntervals.keyInRange(KeyIntervals.GET, nextKey)) {
      Log.getLogWriter().info("All gets completed; returning from get");
      return true;
   }
   Object key = NameFactory.getObjectNameForCounter(nextKey);
//   Log.getLogWriter().info("Getting " + key);
   try {
      boolean inProgressBefore = isAnyGiiInProgress();
      long start = System.currentTimeMillis();
      Object existingValue = null;
      if (TestConfig.tab().booleanAt(parReg.ParRegPrms.isWithRoutingResolver,
          false)) {
        Month callBackArg = (Month)InitImageBB.getBB().getSharedMap().get(key);
        existingValue = aRegion.get(key, callBackArg);
        Log.getLogWriter().info("Inside get(), did get with callback");
      }
      else {
        existingValue = aRegion.get(key);
      }
      long end = System.currentTimeMillis();
      boolean inProgressAfter = isAnyGiiInProgress();
      long duration = end - start;
      Log.getLogWriter().info("Done getting " + key + 
          " time was " + duration + " millis; inProgressBefore: " + inProgressBefore + 
          ", inProgressAfter: " + inProgressAfter + 
          ", num remaining: " + (keyIntervals.getLastKey(KeyIntervals.GET) - nextKey));
      if (existingValue == null)
         throw new TestException("Get of key " + key + " returned unexpected " + existingValue);
      sc.setIfLarger(InitImageBB.MAX_TIME_GET, duration);
      if (inProgressBefore && inProgressAfter) {
         sc.increment(InitImageBB.GET_COMPLETED);
      }
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return (nextKey >= keyIntervals.getLastKey(KeyIntervals.GET));
}

/** Do gets with keys that do not exist in the system. Do this until
 *  a getInitialImage has completed. */
protected void doNewKeyGets() {
   // wait for getInitialImage to begin
   waitForAnyGiiToBegin();

   // now get new keys while gii is in progress
   int newKeyGetsReturnedNull = 0;
   int newKeyGetsReturnedLoaderValue = 0;
   Log.getLogWriter().info("Beginning new key gets...");
   while (isAnyGiiInProgress()) {
      Object key = NameFactory.getNextPositiveObjectName();
      Object value = null;
      try {
         long start = System.currentTimeMillis();
         value = aRegion.get(key);
         long end = System.currentTimeMillis();
         sc.setIfLarger(InitImageBB.MAX_TIME_GET, (end - start)); 
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (value == null) {
         newKeyGetsReturnedNull++;
      } else {
         if (value.getClass() == String.class) {
            String strValue = (String)value;
            if (strValue.equals(GiiLoader.returnStr + key)) {
               newKeyGetsReturnedLoaderValue++;
            } else {
               throw new TestException("Expected get to return value from GiiLoader, but got " +
                         TestHelper.toString(value));
            }
         } else {
            throw new TestException("Expected get to return String from GiiLoader, but got " +
                      TestHelper.toString(value));
         }
      }
   }
   long maxTime = sc.read(InitImageBB.MAX_TIME_GET);
   Log.getLogWriter().info("Done with new key gets, newKeyGetsReturnedNull: " + newKeyGetsReturnedNull +
       ", numKeyGetsReturnedLoaderValue: " + newKeyGetsReturnedLoaderValue + ", maxGetTime: " +
       maxTime + " millis");
   double percent = (((double)newKeyGetsReturnedLoaderValue) /
                                  ((double)newKeyGetsReturnedNull)) * 100;
   if (percent > 1.0) {
      throw new TestException("Getting new keys might not have returned null from cache loader " +
            "installed in region doing a getInitialImage, number of new key gets that returned " +
            "null during gii: " + newKeyGetsReturnedNull + 
            ", number of new key gets that returned a value from GiiLoader: " + 
            newKeyGetsReturnedLoaderValue);
   }
   int threshold = 10;
   if (newKeyGetsReturnedNull <= threshold) {
      throw new TestException("Suspiciously low number of gets " + newKeyGetsReturnedNull);
   }
   
   // We need to wait until we see that another member is initialized and that it has a loader.
   // This happens a bit after gii completes. See bug 48578.
   {
     final int MS_TO_WAIT_FOR_NET_LOADER = 1777;
     final long endTimeWaitForNetLoader = System.currentTimeMillis() + MS_TO_WAIT_FOR_NET_LOADER;
     while (!((DistributedRegion)aRegion).hasNetLoader()) {
       if (System.currentTimeMillis() > endTimeWaitForNetLoader) {
         throw new TestException("No net loader for region " + aRegion + " after waiting " + MS_TO_WAIT_FOR_NET_LOADER + "ms.");
       }
       try {
         Thread.sleep(10);
       } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
       }
     }
     long waitTime = System.currentTimeMillis() - (endTimeWaitForNetLoader-MS_TO_WAIT_FOR_NET_LOADER);
     if (waitTime > 0) {
       Log.getLogWriter().info("Had to wait " + waitTime + "ms for net loader to be available after gii completed.");
     }
   }

   // now gii is not in progress, new gets should return the cache loader value from
   // the gii VM
   Log.getLogWriter().info("GetInitialImage inProgress is " + isAnyGiiInProgress() + 
       ", doing more new key gets...");
   long timeToRunMS = 30000;
   long startTime = System.currentTimeMillis();
   newKeyGetsReturnedLoaderValue = 0; // start counter over
   newKeyGetsReturnedNull = 0;
   StringBuffer errStr = new StringBuffer();
   do {
      try {
         Object key = NameFactory.getNextPositiveObjectName();
         Object value = aRegion.get(key);
         if (value == null) {
            newKeyGetsReturnedNull++;
            String aStr = new Date() + " GiiInProgress is " + isAnyGiiInProgress() +
               ", expected a value from the GiiLoader for " + key + ", but got " + value + "; " +
               "number of non-null values return from loader so far: " + newKeyGetsReturnedLoaderValue;
            Log.getLogWriter().info(aStr);
            errStr.append(aStr + "\n");
         } else if (value instanceof String) {
            String strValue = (String)value;
            if (strValue.equals(GiiLoader.returnStr + key)) {
               newKeyGetsReturnedLoaderValue++;
            } else {
               throw new TestException("GiiInProgress is " + isAnyGiiInProgress() +
                  ", expected a value from the GiiLoader for " + key + ", but got " + TestHelper.toString(value));
            }
         } else {
            throw new TestException("GiiInProgress is " + isAnyGiiInProgress() +
               ", expected a value from the GiiLoader for " + key + ", but got " + TestHelper.toString(value));
         }
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("After getInitialImageCompleted, got " + newKeyGetsReturnedLoaderValue + 
       " values from GiiLoader, got " + newKeyGetsReturnedNull + " null values");

   // see if any gets returned null after gii completed
   if (newKeyGetsReturnedNull != 0) {
      throw new TestException("After gii finished, got unexpected null values from loader; " +
            "num null values " + newKeyGetsReturnedNull + ", num non-null values " + 
            newKeyGetsReturnedLoaderValue + "\n" + errStr.toString());
   }
}

/** 
 *  Do keyInterval operations during the gii, then wait for the gii to complete.
 *  After it has completed, do more puts with new keys.
 */
protected void doOpsPlusPuts() {
   BitSet availableOps = new BitSet(operations.length);
   availableOps.flip(FIRST_OP, LAST_OP+1);
   testInstance.doOps(availableOps);
  
   waitForAllGiiToComplete(); 

   InitImageBB.getBB().printSharedCounters();

   // now gii is not in progress, puts with new keys should invoke the cacheWriter
   Log.getLogWriter().info("GetInitialImage inProgress is " + isAnyGiiInProgress());
   long timeToRunMS = 30000;
   long startTime = System.currentTimeMillis();
   int putCount = 0;
   do {
      try {
         Object key = NameFactory.getNextPositiveObjectName();
         Object value = "put to invoke cache writer: " + key;
         aRegion.put(key, value);
         putCount++;
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("After getInitialImageCompleted, put " + putCount);
}

// ======================================================================== 
// verification methods 

/** Verify the contents of the region, taking into account the keys
 *  that were destroyed, invalidted, etc (as specified in keyIntervals)
 *  Throw an error of any problems are detected.
 */ 
public void verifyRegionContents() {
   InitImageBB.getBB().printSharedCounters();
   NameBB.getBB().printSharedCounters();
   StringBuffer errStr = new StringBuffer();

   // check region size
   long numKeys = aRegion.size();
   if (totalNumKeys != numKeys) {
      String tmpStr = "Expected " + totalNumKeys + " keys, but there are " + numKeys;
      Log.getLogWriter().info(tmpStr);
      errStr.append(tmpStr + "\n");
   }
   Log.getLogWriter().info("In verifyRegionContents, region has " + numKeys + " keys");

   // iterate keys
   int numKeysToCheck = keyIntervals.getNumKeys() + numNewKeys;
   Log.getLogWriter().info("In verifyRegionContents, iterating through " + numKeysToCheck + " key names");
   long lastLogTime = System.currentTimeMillis();
   for (int i = 1; i <= numKeysToCheck; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      try {
         if (((i >= keyIntervals.getFirstKey(KeyIntervals.NONE)) &&
              (i <= keyIntervals.getLastKey(KeyIntervals.NONE)))    ||
             ((i >= keyIntervals.getFirstKey(KeyIntervals.GET)) &&
             (i <= keyIntervals.getLastKey(KeyIntervals.GET)))) {
            // this key was untouched after its creation
            checkContainsKey(key, true, "key was untouched");
            checkContainsValueForKey(key, true, "key was untouched");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE)) &&
                    (i <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
            checkContainsKey(key, true, "key was invalidated");
            checkContainsValueForKey(key, false, "key was invalidated");
         } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)) &&
                    (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
            // this key was locally invalidated
            checkContainsKey(key, true, "key was locally invalidated");
            checkContainsValueForKey(key, true, "key was locally invalidated");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.DESTROY)) &&
                    (i <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
            // this key was destroyed
            checkContainsKey(key, false, "key was destroyed");
            checkContainsValueForKey(key, false, "key was destroyed");
         } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)) &&
                    (i <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
            // this key was locally destroyed
            checkContainsKey(key, true, "key was locally destroyed");
            checkContainsValueForKey(key, true, "key was locally destroyed");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((i >= keyIntervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)) &&
                    (i <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
            // this key was updated
            checkContainsKey(key, true, "key was updated");
            checkContainsValueForKey(key, true, "key was updated");
            Object value = aRegion.get(key);
            checkUpdatedValue(key, value);
         } else if (i > keyIntervals.getNumKeys()) {
            // key was newly added
            checkContainsKey(key, true, "key was new");
            checkContainsValueForKey(key, true, "key was new");
            Object value = aRegion.get(key);
            checkValue(key, value);
         }
      } catch (TestException e) {
         Log.getLogWriter().info(TestHelper.getStackTrace(e));
         errStr.append(e.getMessage() + "\n");
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified key " + i + " out of " + totalNumKeys);
         lastLogTime = System.currentTimeMillis();
      }
   }

   if (errStr.length() > 0)
      throw new TestException(errStr.toString());
}

/** Verify that the region operations that occurred did not block.
 *  Do this by looking at the number of operations that occurred
 *  while creating the region that did getInitialImage.
 */
public void verifyNonBlocking() {
   InitImageBB.getBB().printSharedCounters();

   // look for suspicious number of operations during gii
   long threshold = 10;
   long completed = sc.read(InitImageBB.INVALIDATE_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.INVALIDATE) > 0) && (completed <= threshold))
      throw new TestException("Invalidate might have blocked during getInitialImage, " +
            "num invalidates completed during getInitialImage: " + completed + 
            ", total num invalidates: " + keyIntervals.getNumKeys(KeyIntervals.INVALIDATE));
   completed = sc.read(InitImageBB.LOCAL_INVALIDATE_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.LOCAL_INVALIDATE) > 0) && (completed <= threshold))
      throw new TestException("Local invalidate might have blocked during getInitialImage, " +
            "num local invalidates completed during getInitialImage: " + completed + 
            ", total num local invalidates: " + keyIntervals.getNumKeys(KeyIntervals.LOCAL_INVALIDATE));
   completed = sc.read(InitImageBB.DESTROY_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.DESTROY) > 0) && (completed <= threshold))
      throw new TestException("Destroy might have blocked during getInitialImage, " +
            "num destroys completed during getInitialImage: " + completed + 
            ", total num destroys: " + keyIntervals.getNumKeys(KeyIntervals.DESTROY));
   completed = sc.read(InitImageBB.LOCAL_DESTROY_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.LOCAL_DESTROY) > 0) && (completed <= threshold))
      throw new TestException("Local destroy might have blocked during getInitialImage, " +
            "num local destroys completed during getInitialImage: " + completed + 
            ", total num local destroys: " + keyIntervals.getNumKeys(KeyIntervals.LOCAL_DESTROY));
   completed = sc.read(InitImageBB.NEW_KEY_COMPLETED);
   if ((numNewKeys > 0) && (completed <= threshold))
      throw new TestException("Adding new keys might have blocked during getInitialImage, " +
            "num new keys added during getInitialImage: " + completed + 
            ", total num new keys: " + numNewKeys);
   completed = sc.read(InitImageBB.UPDATE_EXISTING_KEY_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.UPDATE_EXISTING_KEY) > 0) && (completed <= threshold))
      throw new TestException("Updating existing keys might have blocked during getInitialImage, " +
            "num updates completed during getInitialImage: " + completed + 
            ", total num updates: " + keyIntervals.getNumKeys(KeyIntervals.UPDATE_EXISTING_KEY));
   completed = sc.read(InitImageBB.GET_COMPLETED);
   if ((keyIntervals.getNumKeys(KeyIntervals.GET) > 0) && (completed <= threshold))
      throw new TestException("Gets might have blocked during getInitialImage, " +
            "num gets completed during getInitialImage: " + completed + 
            ", total num gets: " + keyIntervals.getNumKeys(KeyIntervals.GET));

   // look for suspicious max times
   Log.getLogWriter().info("time to create region: " + createRegionDurationMS + " millis");
   threshold = (int)(giiDurationMS * 0.9);
   long maxTimeMS = sc.read(InitImageBB.MAX_TIME_INVALIDATE);
   if (maxTimeMS > threshold)
      throw new TestException("Invalidate might have blocked during getIntialImage, " +
         "max time for an invalidate: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_LOCAL_INVALIDATE);
   if (maxTimeMS > threshold)
      throw new TestException("Local invalidate might have blocked during getIntialImage, " +
         "max time for a local invalidate: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_DESTROY);
   if (maxTimeMS > threshold)
      throw new TestException("Destroy might have blocked during getIntialImage, " +
         "max time for a destroy: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_LOCAL_DESTROY);
   if (maxTimeMS > threshold)
      throw new TestException("Local destroy might have blocked during getIntialImage, " +
         "max time for a local destroy: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_NEW_KEY);
   if (maxTimeMS > threshold)
      throw new TestException("Creating a new key might have blocked during getIntialImage, " +
         "max time for creating a new key: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_UPDATE_EXISTING_KEY);
   if (maxTimeMS > threshold)
      throw new TestException("Updating an existing key might have blocked during getIntialImage, " +
         "max time for updating an existing key: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
   maxTimeMS = sc.read(InitImageBB.MAX_TIME_GET);
   if (maxTimeMS > threshold)
      throw new TestException("Getting might have blocked during getIntialImage, " +
         "max time for a get: " + maxTimeMS + " millis, " +
         "time to create region: " + createRegionDurationMS + " millis");
}

protected void verifyEventCounters() {
   long threshold = 10;

   // check event counters
   EventCountersBB.getBB().printSharedCounters();
   long[] eventCounters = EventCountersBB.getBB().getSharedCounters().getCounterValues();

   // after create
   long counter = eventCounters[EventCountersBB.numAfterCreateEvents_isDist] +
                  eventCounters[EventCountersBB.numAfterCreateEvents_isNotDist];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isExp] +
             eventCounters[EventCountersBB.numAfterCreateEvents_isNotExp];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterCreateEvents_isNotRemote];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(remote sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isLoad] +
                  eventCounters[EventCountersBB.numAfterCreateEvents_isNotLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isLocalLoad] +
                  eventCounters[EventCountersBB.numAfterCreateEvents_isNotLocalLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(local load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isNetLoad] +
                  eventCounters[EventCountersBB.numAfterCreateEvents_isNotNetLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(net load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterCreateEvents_isNetSearch] +
                  eventCounters[EventCountersBB.numAfterCreateEvents_isNotNetSearch];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterCreateEvents(net search sum) count (during gii): " + counter);

   // after destroy
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isDist] +
             eventCounters[EventCountersBB.numAfterDestroyEvents_isNotDist];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isExp] +
             eventCounters[EventCountersBB.numAfterDestroyEvents_isNotExp];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterDestroyEvents_isNotRemote];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(remote sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isLoad] +
                  eventCounters[EventCountersBB.numAfterDestroyEvents_isNotLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isLocalLoad] +
                  eventCounters[EventCountersBB.numAfterDestroyEvents_isNotLocalLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(local load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isNetLoad] +
                  eventCounters[EventCountersBB.numAfterDestroyEvents_isNotNetLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(net load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterDestroyEvents_isNetSearch] +
                  eventCounters[EventCountersBB.numAfterDestroyEvents_isNotNetSearch];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterDestroyEvents(net search sum) count (during gii): " + counter);

   // after invalidate
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isDist] +
                  eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotDist];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isExp] +
             eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotExp];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotRemote];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(remote sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isLoad] +
                  eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isLocalLoad] +
                  eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotLocalLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(local load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isNetLoad] +
                  eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotNetLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(net load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterInvalidateEvents_isNetSearch] +
                  eventCounters[EventCountersBB.numAfterInvalidateEvents_isNotNetSearch];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterInvalidateEvents(net search sum) count (during gii): " + counter);

   // after update
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isDist] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotDist];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isExp] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotExp];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotRemote];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(remote sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isLoad] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isLocalLoad] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotLocalLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(local load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isNetLoad] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotNetLoad];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(net load sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterUpdateEvents_isNetSearch] +
             eventCounters[EventCountersBB.numAfterUpdateEvents_isNotNetSearch];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterUpdateEvents(net search sum) count (during gii): " + counter);

   // afterRegionDestroy events
   counter = eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isDist] +
             eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isNotDist];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionDestroyEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isExp] +
             eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isNotExp];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionDestroyEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterRegionDestroyEvents_isNotRemote];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionDestroyEvents(remote sum) count (during gii): " + counter);

   // afterRegionInvalidate events
   counter = eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isDist] +
             eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isNotDist];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionInvalidateEvents(dist sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isExp] +
             eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isNotExp];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionInvalidateEvents(exp sum) count (during gii): " + counter);
   counter = eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isRemote] +
             eventCounters[EventCountersBB.numAfterRegionInvalidateEvents_isNotRemote];
   if (counter > 0)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "numAfterRegionInvalidateEvents(remote sum) count (during gii): " + counter);

   // close events
   counter = eventCounters[EventCountersBB.numClose];
   if (counter > threshold)
      throw new TestException("Event listener might have been invoked during getInitialImage, " +
            "num close count (during gii): " + counter);

}

/** 
 *  Verify counters to show that the cache writer installed in the VM doing
 *  the getInitialImage was not invoked while GII was in progress, but gets
 *  invoked when GII is not in progress.
 */
protected void verifyCacheWriterTest() {
   InitImageBB.getBB().printSharedCounters();
   long duringGii = InitImageBB.getBB().getSharedCounters().read(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   long noGii = InitImageBB.getBB().getSharedCounters().read(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
   int threshold = 3;
   if (duringGii > threshold)
      throw new TestException("GiiWriter might have been invoked during getInitialImage, " +
            "number of cache writer invocations while gii was in progress: " + duringGii);
   if (noGii < threshold)
      throw new TestException("GiiWriter might not have been invoked without getInitialImage in progress, " +
            "number of cache writer invocations while gii was not in progress: " + noGii);
}

/** Check that containsKey() called on the tests' region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsKey
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkContainsKey(Object key, boolean expected, String logStr) {
   boolean containsKey = aRegion.containsKey(key);
   if (containsKey != expected)
      throw new TestException("Expected containsKey(" + key + ") to be " + expected + 
                ", but it was " + containsKey + ": " + logStr);
}

/** Check that containsValueForKey() called on the tests' region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsValueForKey
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkContainsValueForKey(Object key, boolean expected, String logStr) {
   boolean containsValue = aRegion.containsValueForKey(key);
   if (containsValue != expected)
      throw new TestException("Expected containsValueForKey(" + key + ") to be " + expected + 
                ", but it was " + containsValue + ": " + logStr);
}

/** Check that the value of the given key is expected for this test.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param value The value for the key.
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkValue(Object key, Object value) {
   Object baseValue = PdxTestVersionHelper.toBaseObject(value);
   if (baseValue instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)baseValue;
      long keyCounter = NameFactory.getCounterForName(key);
      if (vh.myValue instanceof Long) {
         Long aLong = (Long)vh.myValue;
         long longValue = aLong.longValue();
         if (keyCounter != longValue)
            throw new TestException("Inconsistent ValueHolder.myValue for key " + key + ":" + TestHelper.toString(vh));      
      } else {
         throw new TestException("Expected ValueHolder.myValue for key " + key + " to be a Long for " + TestHelper.toString(vh));      
      }
   } else {
      throw new TestException("For key " + key + ", expected value " + TestHelper.toString(baseValue) + " to be a ValueHolder");
   }
}
      
/** Check that the value of the given key is expected as an updated value.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param value The value for the key.
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkUpdatedValue(Object key, Object value) {
   Object baseObject = PdxTestVersionHelper.toBaseObject(value);
   if (baseObject instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)baseObject;
      long keyCounter = NameFactory.getCounterForName(key);
      if (vh.myValue instanceof String) {
         String aStr = (String)vh.myValue;
         String expectedStr = "updated_" + keyCounter;
         if (!aStr.equals(expectedStr))
            throw new TestException("Inconsistent ValueHolder.myValue for key " + key + ":" + TestHelper.toString(vh));      
      } else {
         throw new TestException("Expected ValueHolder.myValue for key " + key + 
            " to be a String indicating it was updated, but the value for this key is " + 
            TestHelper.toString(vh));      
      }
   } else {
      throw new TestException("Expected value " + TestHelper.toString(baseObject) + " to be a ValueHolder");
   }
}
      
// ======================================================================== 
// internal methods to provide support to task methods

/** Get a random operation to perform. The set of operations available are
 *  in the BitSet. These bits correspond to the operations defined in this
 *  class.
 *
 *  @param bs True bits correspond to operations available to be chosen.
 *  @param bsSize The number of bits to consider.
 *
 *  @return An operation as defined in this class.
 */
protected int getOp(BitSet bs, int bsSize) {
   int randInt;
   do {
      randInt = TestConfig.tab().getRandGen().nextInt(1, bsSize);
   } while (!bs.get(randInt));
   return randInt;
}

/** Return a cache loader to install into a region, or null of none
 *  InitImagePrms.useCacheLoader is used to determine if a cache loader
 *  should be installed.
 *
 *  @returns A cache loader or null.
 */
protected CacheLoader getCacheLoader() {
   boolean useCacheLoader = TestConfig.tasktab().booleanAt(InitImagePrms.useCacheLoader, false);
   if (useCacheLoader) {
      try {
         String cacheLoaderClassName = TestConfig.tasktab().stringAt(InitImagePrms.cacheLoaderClass);
         Class cacheLoaderClass = Class.forName(cacheLoaderClassName);
         CacheLoader loader = (CacheLoader)(cacheLoaderClass.newInstance());
         return loader;
      } catch (java.lang.ClassNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (java.lang.InstantiationException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (java.lang.IllegalAccessException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } else {
      return null;
   }
}

/** Return a cache writer to install into a region, or null if none
 *  InitImagePrms.useCacheWriter is used to determine if a cache writer
 *  should be installed.
 *
 *  @returns A cache writer or null.
 */
protected CacheWriter getCacheWriter() {
   boolean useCacheWriter = TestConfig.tasktab().booleanAt(InitImagePrms.useCacheWriter, false);
   if (useCacheWriter)
      return new GiiWriter();
   else
      return null;
}

/** Return a value to add to a region for the given key
 */
protected Object getValueToAdd(Object key) {
   return createObject((String)key);
}

protected BaseValueHolder createObject (Object key) {
  return createObject(key, randomValues);
}

protected BaseValueHolder createObject (Object key, RandomValues values) {
  if (objectType.equals("delta.DeltaValueHolder")) {
    return new DeltaValueHolder((String)key, values);
  } else if (objectType.equals("util.PdxVersionedValueHolder") ||
            (objectType.equals("util.VersionedValueHolder"))) {
    return PdxTest.getVersionedValueHolder(objectType, (String)key, values);
  }
  else {
    return new ValueHolder((String)key, values);
  }
}

protected Set getRegionKeySet(Region aRegion) {
   return aRegion.keys();
}

// ======================================================================== 
// methods to wait for or test whether getInitialImage is in progress or 
// completed

/** Test whether a local GII is in progress. Not as accurate as
 *  isLocalGiiInProgress(Region), but works if you don't have a region
 *  available.
 * 
 *  @returns true if a GII is in progress for this VM, false otherwise.
 *
 */
protected static boolean isLocalGiiInProgress() {
   int inProgress = TestHelper.getStat_getInitialImagesInProgress(REGION_NAME);
   if (inProgress > 1)
      throw new TestException("Expected local gii in progress to always be 0 or 1, but it is " + inProgress);
   return (inProgress > 0);
}

/** Test whether a local GII is in progress. Is more accurate than
 *  isLocalGiiInProgress().
 *
 *  @param The region that may be in a getInitialImage
 * 
 *  @returns true if a GII is in progress for this VM, false otherwise.
 *
 */
protected static boolean isLocalGiiInProgress(Region aRegion) {
   if (aRegion == null) 
      return false;
   boolean inProgress = !(((com.gemstone.gemfire.internal.cache.LocalRegion)aRegion).isInitialized());
   return inProgress;
}

/** Test whether the local GII has completed.
 *
 *  @returns true if a GII for this VM has completed, false otherwise.
 *
 */
protected static boolean hasLocalGiiCompleted() {
   return (TestHelper.getStat_getInitialImagesCompleted(REGION_NAME) > 0);
}

/** Test whether any GII is in progress.
 */
protected static boolean isAnyGiiInProgress() {
   return (InitImageBB.getBB().getSharedCounters().read(InitImageBB.GII_IN_PROGRESS) > 0);
}

/** Wait for a getInitialImage in ANY vm to be in progress. 
 */
public static void waitForAnyGiiToBegin() {
   // wait for getInitialImage to begin
   Log.getLogWriter().info("Waiting for getInitialImage to begin in any VM...");
   hydra.blackboard.SharedCounters counters = InitImageBB.getBB().getSharedCounters();
   while (counters.read(InitImageBB.GII_IN_PROGRESS) <= 0) {
      try {
         Thread.sleep(10);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   Log.getLogWriter().info("Done waiting for getInitialImage to begin in any VM");
}

/** Wait for getInitialImage in ALL vms to complete.
 */
protected static void waitForAllGiiToComplete() {
   // wait for getInitialImage to begin
   Log.getLogWriter().info("Waiting for getInitialImage to complete in all VMs...");
   hydra.blackboard.SharedCounters counters = InitImageBB.getBB().getSharedCounters();
   while (counters.read(InitImageBB.GII_IN_PROGRESS) > 0) {
      try {
         Thread.sleep(10);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   Log.getLogWriter().info("Done waiting for getInitialImage to complete in all VMs");
}

/** Wait for stats to indicate that a getInitialImage is in progress. Note that
 *  this will wait for stats for this VM only.
 */
protected static void waitForLocalGiiToBegin() {
   final long MAX_WAIT_MILLIS = 300000;
   Log.getLogWriter().info("Waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to begin...");
   final long start = System.currentTimeMillis();
   int inProgress = TestHelper.getStat_getInitialImagesInProgress(REGION_NAME);
   while ((inProgress <= 0) && (System.currentTimeMillis() - start < MAX_WAIT_MILLIS)) {
     try {
       Thread.sleep(10);
     } catch (InterruptedException e) {
       throw new TestException(TestHelper.getStackTrace(e));
     }
     inProgress = TestHelper.getStat_getInitialImagesInProgress(REGION_NAME);
   }
   Log.getLogWriter().info("Done waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to begin, inProgress is " + inProgress);
   if (inProgress <= 0)
      throw new TestException("Waited " + MAX_WAIT_MILLIS + " for getInitialImage to begin, but inProgress is " + inProgress);
}

/** Wait for stats to indicate that a getInitialImage has completed. Note that
 *  this will wait for stats for this VM only.
 */
protected static void waitForLocalGiiCompleted() {
   final long MAX_WAIT_MILLIS = 300000;
   long giiCompleted;
   Log.getLogWriter().info("Waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to complete...");
   long start = System.currentTimeMillis();
   do {
      giiCompleted = TestHelper.getStat_getInitialImagesCompleted(REGION_NAME);
   } while ((giiCompleted <= 0) && (System.currentTimeMillis() - start < MAX_WAIT_MILLIS));
   Log.getLogWriter().info("Done waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to complete, giiCompleted is " + giiCompleted);
   if (giiCompleted <= 0)
      throw new TestException("Waited " + MAX_WAIT_MILLIS + " for getInitialImage to complete, but giiCompleted is " + giiCompleted);
}

  public static void HydraTask_verifyGII() {
    testInstance.verifyGII();
  }

  /**
   * Verify that all regions were gii'ed as expected (full gii). throws a TestException if gii is not as configured via
   * expectDeltaGII.
   */
  private void verifyGII() {
    StringBuffer aStr = new StringBuffer("verifyGII invoked. ");

    String regionName = testInstance.aRegion.getName();

    DiskRegion diskRegion = ((LocalRegion) testInstance.aRegion).getDiskRegion();
    if (diskRegion != null && diskRegion.getStats().getRemoteInitializations() == 0) {
      aStr.append(regionName + " was recovered from disk (Remote Initializations = " +
                  diskRegion.getStats().getRemoteInitializations() + ").");
    } else {
      int giisCompleted = TestHelper.getStat_getInitialImagesCompleted(regionName);
      int deltaGiisCompleted = TestHelper.getStat_deltaGetInitialImagesCompleted(regionName);
      if ((giisCompleted < 1) || (deltaGiisCompleted > 0)) {
        throw new TestException("Did not perform expected type of GII. Expected a full GII for region " +
                                testInstance.aRegion.getFullPath() +
                                " GIIsCompleted = " + giisCompleted +
                                " DeltaGIIsCompleted = " + deltaGiisCompleted);
      } else {
        aStr.append(regionName + " Remote Initialization (GII): GIIsCompleted = " + giisCompleted +
                    " DeltaGIIsCompleted = " + deltaGiisCompleted + ".");
      }
    }
    Log.getLogWriter().info(aStr.toString());
  }
}
