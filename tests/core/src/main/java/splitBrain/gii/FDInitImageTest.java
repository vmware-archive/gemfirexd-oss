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
package splitBrain.gii; 

import hydra.*;
import util.*;
import getInitialImage.InitImagePrms;
import getInitialImage.InitImageBB;
import getInitialImage.GiiListener;
import splitBrain.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;

public class FDInitImageTest {

// the one instance of InitImageTest
static protected FDInitImageTest testInstance;

// region name
protected static final String REGION_NAME = "TestRegion";
// desired region size
protected int numKeys = 0;

// protected fields used by the test to do its work
protected RandomValues randomValues = null;
       // used for getting random values for the region
protected Region aRegion;
       // the region used in this test for getInitialImage
protected Region FDRegion = null;
       // the region used to help cause a forced disconnect
protected long createRegionDurationMS = 0;
       // the number of millis it took to do a create region, which includes getInitialImage
protected long giiDurationMS = 0;
       // the number of millis it took to do a getInitialImage 

protected final long LOG_INTERVAL_MILLIS = 10000;

// These variables are used to track the state of checking large regions
// for repeated batched calls. They must be reinitialized if a second check is
// called in this vm within a single test.
protected static int verifyRegionContentsIndex = 0;
protected static StringBuffer verifyRegionContentsErrStr = new StringBuffer();
protected static boolean verifyRegionContentsCompleted = false;
protected static long verifyRegionContentsAccumulatedTime = 0;

// ======================================================================== 
// initialization methods 

/** Initialize the single instance of this test class and a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
protected synchronized static void initializeRegion(DataPolicy dataPolicy) {
   if (testInstance == null) {
      testInstance = new FDInitImageTest();
      testInstance.initRegion(dataPolicy);
      testInstance.initInstance();
   }
}

/** Initialize the single instance of this test class but not a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
protected synchronized static void initializeInstance() {
   if (testInstance == null) {
      testInstance = new FDInitImageTest();
      testInstance.initInstance();
   }
}

/** Initialize a region with the desired attributes. Initialize instance
 *  fields for this instance. 
 */
protected void initRegion(DataPolicy dataPolicy) {
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   regDef.setDataPolicy(dataPolicy);
   Log.getLogWriter().info("Using RegionDefinition " + regDef + " to create region");
   aRegion = regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, 
                                     null, null, null);
}

/** Initialize fields for this instance
 */
protected void initInstance() {
   randomValues = new RandomValues();
   // set the number of keys to a value such that the gii will run
   // long enough for a forced disconnect to occur
   String OsName = System.getProperty("os.name") ;
   if (OsName.indexOf("Linux") >= 0) {
      numKeys = 200000;
   } else {
      numKeys = 50000;
   }
   Log.getLogWriter().info("numKeys for this run is " + numKeys);
}

/** Hydra start task to initialize dataPolicy on the blackboard.
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
}

public synchronized static void HydraTask_createFDRegion() {
   FDInitImageTest instance = (FDInitImageTest)testInstance;
   if (instance.FDRegion == null) {
      RegionDefinition regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, "FDRegion");
      instance.FDRegion = regDef.createRootRegion(CacheUtil.getCache(), null, null, null, null);
      Log.getLogWriter().info("Installing MembershipNotifierHook");
      SBUtil.addMembershipHook(new MembershipNotifierHook());
   }
}

// ======================================================================== 
// hydra task methods

/** Task to verify a potentially partial gii. 
 */
public static void HydraTask_verifyPossiblePartialGII() {
   testInstance.verifyPossiblePartialGII();
}

/** Verify a potentially partial gii. The gii could be partial if the test
 *  caused a forced disconnect in a replicate vm. Since gii prefers replicates
 *  it could have targeted the replicate that got the forced disconnect, causing
 *  a partial gii. 
 *  This method is not called for the case where all sources are normal (union). 
 *  This is batched.
 */
protected void verifyPossiblePartialGII() {
   Log.getLogWriter().info(testInstance.aRegion.getFullPath() + " is size " + 
       testInstance.aRegion.size() + "; complete gii would expect " + numKeys);
   if (aRegion.size() == numKeys) { // we got a full gii no matter what the test configuation is
      HydraTask_verifyRegionContents(); 
   } else if (aRegion.size() > numKeys) { // not likely
      throw new TestException(aRegion.getFullPath() + " is size " + aRegion.size() + 
            " and has more keys than the expected " + numKeys);
   } else { // we have < numKeys; a partial gii
      String aStr = "GII received a partial image of " + aRegion.size() + " keys out of " + numKeys;
      Log.getLogWriter().info(aStr);
      throw new StopSchedulingTaskOnClientOrder(aStr);
   }
}

/** Verify the size of the region.
 */
public static void HydraTask_verifyRegionSize() {
   int size = testInstance.aRegion.size();
   if (size != testInstance.numKeys) { // find the keys that are different
      StringBuffer aStr = new StringBuffer();
      for (int i = 1; i <= testInstance.numKeys; i++) {
         Object key = NameFactory.getObjectNameForCounter(i);
         if (!testInstance.aRegion.containsKey(key)) {
            aStr.append(key + " is missing from " + testInstance.aRegion.getFullPath() + "\n");
         }
      }
      throw new TestException("Expected region to be of size " + testInstance.numKeys + " but it is " 
            + size + "; missing entries are:\n" + aStr.toString());
   }
}

/** Do puts to help a forced disconnect to occur in another vm.
 */
public static void HydraTask_putFDRegion() {
   int counter = 0;
   waitForAnyGiiToBegin();
   while (true) {
      Log.getLogWriter().info("Putting into " + testInstance.FDRegion.getFullPath() + " until forced disconnect has occurred in another vm");
      counter++;
      testInstance.FDRegion.put(new Integer(counter), "whatever"); // this region evicts so it won't grow that large
      MasterController.sleepForMs(1000);
      long metCounter = ControllerBB.getBB().getSharedCounters().read(ControllerBB.metCounter);
      if (metCounter >= 2) {
        throw new StopSchedulingTaskOnClientOrder(metCounter + " members recognized a forced disconnect");
      }
   }
}

/** Cause a forced disconnect in the current vm
 */
public static void HydraTask_becomeDead() {
   boolean isUnion = 
           (InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY1_ATTR).equals("normal")) &&
           (InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY2_ATTR).equals("normal"));
   waitForAnyGiiToBegin();
   MasterController.sleepForMs(5000); // just give a little more time for gii to get going 
   ControllerBB.checkForError();
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   Cache theCache = CacheHelper.getCache();
   ControllerBB.enablePlayDead();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // slow
      ControllerBB.enableSlowListener();
   } else { // sick
      ControllerBB.enableSickness();
   }
   ControllerBB.checkForError();

   // recognize the forced disconnect
   waitForForcedDiscConditions(ds, theCache);
   ControllerBB.waitMembershipFailureComplete();
   if (isUnion) {
      // we really want this test to complete the FD while gii is in progress;
      // for preference tests the gii will end early, possibly
      // just prior to the test noticing the forced disconnect, so it's ok
      // not to check if the gii is in progress for preference (the test 
      // causes forced disconnect in all source regions
      if (!isAnyGiiInProgress()) {
         throw new TestException("Gii did not run long enough to complete a forced disconnect; test might need tuning");
      }
   }
   ControllerBB.checkForError();
}

/** Hydra task to initialize a region.
 */
public static void HydraTask_initRegion() {
   // this task gets its dataPolicy attribute from the blackboard
   String dataPolicyStr = TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute, null);
   if (dataPolicyStr != null) {
      throw new TestException("This task must get its dataPolicy attribute from " +
            "InitimageBB.giiSourceDataPolicy, but the task attribute also specified dataPolicy " + 
            dataPolicyStr);
   }
   if (SplitBrainBB.getBB().getSharedCounters().incrementAndRead(SplitBrainBB.ReadyToBegin) == 1) {
     dataPolicyStr = (String)InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY1_ATTR);
   } else {
      dataPolicyStr = (String)InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY2_ATTR);
   }
   DataPolicy dataPolicyAttr = TestHelper.getDataPolicy(dataPolicyStr);
   initializeRegion(dataPolicyAttr);
}

/** Hydra batched task to load a region according to hydra param settings. 
 */
public static void HydraTask_loadRegion() {
   testInstance.loadRegion();
}

/** Hydra batched task to load a region by getting values from another 
 *  distributed system using gets.
 */
public static void HydraTask_loadRegionWithGets() {
   // bring in values if not replicated; if replicated then all keys and 
   // values are already there
   DataPolicy dataPolicyAttr = testInstance.aRegion.getAttributes().getDataPolicy();
   if (dataPolicyAttr.withReplication()) {
      int regionSize = testInstance.aRegion.size();
      long expectedNumKeys = NameFactory.getPositiveNameCounter();
      if (regionSize != expectedNumKeys) {
         throw new TestException("DataPolicy is withReplication, expected region size to be " + 
               expectedNumKeys + ", but it is " + regionSize);
      }
      String aStr = "Not getting keys/values because dataPolicy is withReplication and region has " + 
          regionSize + " keys";
      Log.getLogWriter().info(aStr);
      throw new StopSchedulingTaskOnClientOrder(aStr);
   } else {
      testInstance.loadRegionWithGets();
   }
}

/** Hydra task to do a get region, which does a getInitialImage.
 */
  public static void HydraTask_doGetInitImage() {
    initializeInstance();
   testInstance.doGetInitImage();
  }

  /** Hydra task to wait until another thread does a getInitialImage, then this
 *  get region should block.
 */
public static void HydraTask_blockedGetRegion() {
   initializeInstance();
   testInstance.blockedGetRegion();
}

/** Hydra task to verify the keys and values in a region 
 */
public static void HydraTask_verifyRegionContents() {
   testInstance.verifyRegionContents();
}

/** Hydra task to initialize the cache
 */
public static synchronized void HydraTask_initCache() {
   if (testInstance == null) {
      testInstance = new FDInitImageTest();
      testInstance.initInstance();
      CacheUtil.createCache();
   }
}

// ======================================================================== 
// methods to do the work of the tasks

/** Get the region REGION_NAME, which should do a getInitialImage. Verify
 *  that the getInitialImage occurred by checking the stat for completion
 *  of getInitialImage. Save the number of millis taken for the getRegion
 *  in the createRegionDurationMS instance field.
   */
protected void doGetInitImage() {
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
            InitImageBB.getBB().getSharedCounters().increment(InitImageBB.GII_IN_PROGRESS);
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
          if (!isLocalGiiInProgress()) {
            giiDurationMS = (System.currentTimeMillis() - start);
            Log.getLogWriter().info("Monitor thread: getInitialImage has completed");
            InitImageBB.getBB().getSharedCounters().decrement(InitImageBB.GII_IN_PROGRESS);
            break;
          }
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
      } catch (Exception e) {
        errStr.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
      }
    }
  });
  monitorThread.start();

  // do the getInitialImage
  long begin = 0;
  long end = 0;
  Cache aCache = CacheUtil.createCache();
  RegionDefinition regDef = RegionDefinition.createRegionDefinition();

  // this task gets its useTransactions from the tasktab
  boolean useTransactions = InitImagePrms.useTransactions();

  // this task gets its dataPolicy attribute from the tasktab
  regDef.setDataPolicy(TestHelper.getDataPolicy(TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute)));
  RegionAttributes attr = regDef.getRegionAttributes(new GiiListener(), null, null);
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

  // log results
  createRegionDurationMS = end - begin;
  int regionSize = aRegion.keys().size();
  Log.getLogWriter().info("In doGetInitImage, creating region took " + createRegionDurationMS +
                          " millis, current region size is " + regionSize);
  waitForLocalGiiCompleted();
  try {
    monitorThread.join(30000);
  } catch (InterruptedException e) {
    throw new TestException(TestHelper.getStackTrace(e));
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
   int numCompletedBefore = TestHelper.getStat_getInitialImagesCompleted(REGION_NAME);
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
   numCompleted = numCompleted = numCompletedBefore;
  Log.getLogWriter().info("In blockedGetRegion, numCompleted=" + numCompleted);
   if (numCompleted > 1) {
      throw new TestException("Expected only 1 getInitialImage to be completed, but num completed is " + numCompleted);
   } 

   // now check that the time for this thread is close to the time for the getInitialImageThread
  float blockPercent;
  Log.getLogWriter().info("In blockedGetRegion, giiDurationMS=" + giiDurationMS);
  if(giiDurationMS == 0) {
    long giiTime = TestHelper.getStat_getInitialImageTime(REGION_NAME) / 1000000;
    Log.getLogWriter().info("In blockedGetRegion, giiTime=" + giiTime);
    blockPercent = ((float)duration / (float)giiTime) * 100;
  } else {
    blockPercent = ((float)duration / (float)giiDurationMS) * 100;
  }
  Log.getLogWriter().info("In blockedGetRegion, blockPercent=" + blockPercent);
   if (blockPercent < 75) {
      throw new TestException("Expected the thread that blocked on getInitialImage to have a " +
         "similar time to the thread that did the getInitialImage, blocked thread millis: " + 
         duration + ", getInitialImage time: " + giiDurationMS);
   }
}

/** Load a region with keys and values. The number of keys and values is specified
 *  by numKeys (hydra param). This can be invoked by several threads in numerous
 *  vms to accomplish the work. Note that, for example, if two vms invoke this, 
 *  and they are not replicates, then the union of the two vms will include
 *  numKeys entries. 
 */
public void loadRegion() {
   final long LOG_INTERVAL_MILLIS = 10000;
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
   long minTaskGranularityMS = -1;
   if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
//   StringBuffer historyStr = new StringBuffer();
   do {
      long shouldAddCount = InitImageBB.getBB().getSharedCounters().incrementAndRead(InitImageBB.SHOULD_ADD_COUNT);
      if (shouldAddCount > numKeys) {
         String aStr = "In loadRegion, shouldAddCount is " + shouldAddCount +
                       ", numOriginalKeysCreated is " + InitImageBB.getBB().getSharedCounters().read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED) +
                       ", numKeys is " + numKeys + ", region size (in this vm) is " + aRegion.size();
//         Log.getLogWriter().info("Load history with puts: " + historyStr);
         Log.getLogWriter().info(aStr);
         NameBB.getBB().printSharedCounters();
         throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getNextPositiveObjectName();
      try {
         Object value = getValueToAdd(key);
         aRegion.put(key, value);
//         historyStr.append("put " + key + "\n");
//         Log.getLogWriter().info("Loading with put, key " + key + ", value " + TestHelper.toString(value));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + " out of " + numKeys + 
             " entries (across all vms) into " + TestHelper.regionToString(aRegion, false));
         lastLogTime = System.currentTimeMillis();
      }
   } while ((minTaskGranularitySec == -1) ||
            (System.currentTimeMillis() - startTime < minTaskGranularityMS));
//   Log.getLogWriter().info("Load history with puts: " + historyStr);
}

/** Loads a region with keys and values by getting the value from another
 *  distributed system. The number of keys and values loaded are specified
 *  by the number of keys in numKeys (hydra param). Can be invoked by several threads
 *  in several vms to accomplish the work.
 */
private static int keyIndex = 0;
public void loadRegionWithGets() {
   final long LOG_INTERVAL_MILLIS = 10000;
   long maxKey = NameFactory.getPositiveNameCounter();
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
   long minTaskGranularityMS = -1;
   if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
//   StringBuffer historyStr = new StringBuffer();
   do {
      int keyIndexToGet = 0;
      synchronized (FDInitImageTest.class) {
         keyIndex++;
         keyIndexToGet = keyIndex;
      }
      if (keyIndexToGet > maxKey) {
         String aStr = "loadRegionWithGets, regionSize is " + aRegion.size();
//         Log.getLogWriter().info("Load history with gets: " + historyStr);
         Log.getLogWriter().info(aStr);
         throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getObjectNameForCounter(keyIndexToGet);
      try {
         Object aValue = aRegion.get(key);
//         historyStr.append("get with " + key + "\n");
//         Log.getLogWriter().info("Loading with gets, key " + key + ", value " + TestHelper.toString(aValue));
         if (aValue == null) {
            throw new TestException("Unexpected value " + aValue + " from getting key " + key);
         }
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Got " + keyIndexToGet + " out of " + maxKey + 
             " entries into " + TestHelper.regionToString(aRegion, false));
         lastLogTime = System.currentTimeMillis();
      }
   } while ((minTaskGranularitySec == -1) ||
            (System.currentTimeMillis() - startTime < minTaskGranularityMS));
}

// ======================================================================== 
// verification methods 

/** Verify the contents of the region.
 *  Throw an error of any problems are detected.
 */ 
public void verifyRegionContents() {
   // we already completed this check once; we can't do it again without reinitializing the
   // verify state variables
   if (verifyRegionContentsCompleted) {
      throw new TestException("Test configuration problem; already verified region contents, cannot call this task again without resetting batch variables");
   }

   long totalNumKeys = NameFactory.getPositiveNameCounter();
   if (verifyRegionContentsIndex == 0) { // check size only the first time of the batch
      // check region size
      long size = aRegion.size();
      if (totalNumKeys != size) {
         String tmpStr = "Expected " + totalNumKeys + " keys, but there are " + size;
         Log.getLogWriter().info(tmpStr);
         verifyRegionContentsErrStr.append(tmpStr + "\n");
      }
      Log.getLogWriter().info("In verifyRegionContents, region has " + size + " keys");
   }

   // iterate keys
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
   long minTaskGranularityMS = -1;
   if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   while (verifyRegionContentsIndex < totalNumKeys) {
      verifyRegionContentsIndex++;
      Object key = NameFactory.getObjectNameForCounter(verifyRegionContentsIndex);
      checkContainsKey(key, true);
      checkContainsValueForKey(key, true);
      Object value = aRegion.get(key);
      checkValue(key, value);
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified key " + verifyRegionContentsIndex + " out of " + totalNumKeys);
         lastLogTime = System.currentTimeMillis();
      }
      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {       
         long endTime = System.currentTimeMillis();
         long duration = endTime - startTime;
         verifyRegionContentsAccumulatedTime += duration;
         Log.getLogWriter().info("In verifyRegionContents, returning before completing verify " +
             "because of task granularity (this task must be batched to complete); last key verified is " +  
             key);
         return;  // task is batched; we are done with this batch
      }
   }
   long endTime = System.currentTimeMillis();
   long duration = endTime - startTime;
   verifyRegionContentsAccumulatedTime += duration;

   if (verifyRegionContentsErrStr.length() > 0) {
      throw new TestException(verifyRegionContentsErrStr.toString());
   }
   String aStr = "In verifyRegionContents, verified " + verifyRegionContentsIndex + " keys/values in "
      + verifyRegionContentsAccumulatedTime + " millis";
   Log.getLogWriter().info(aStr);
   throw new StopSchedulingTaskOnClientOrder(aStr);
}

/** Check that containsKey() called on the tests' region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsKey
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkContainsKey(Object key, boolean expected) {
   boolean containsKey = aRegion.containsKey(key);
   if (containsKey != expected)
      throw new TestException("Expected containsKey(" + key + ") to be " + expected + 
                ", but it was " + containsKey);
}

/** Check that containsValueForKey() called on the tests' region has the expected result.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param expected The expected result of containsValueForKey
 */
protected void checkContainsValueForKey(Object key, boolean expected) {
   boolean containsValue = aRegion.containsValueForKey(key);
   if (containsValue != expected)
      throw new TestException("Expected containsValueForKey(" + key + ") to be " + expected + 
                ", but it was " + containsValue);
}

/** Check that the value of the given key is expected for this test.
 *  Throw an error if any problems.
 *  
 *  @param key The key to check.
 *  @param value The value for the key.
 *  @param logStr Used if throwing an error due to an unexpected value.
 */
protected void checkValue(Object key, Object value) {
   if (value instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)value;
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
      throw new TestException("For key " + key + ", expected value " + TestHelper.toString(value) + " to be a ValueHolder");
   }
}
      
// ======================================================================== 
// internal methods to provide support to task methods

/** Return a value to add to a region for the given key
 */
protected Object getValueToAdd(Object key) {
   return new ValueHolder((String)key, randomValues);
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
   while (!isAnyGiiInProgress()) {
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
   while (isAnyGiiInProgress()) {
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
   long giiOngoing;
   Log.getLogWriter().info("Waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to complete...");
   long start = System.currentTimeMillis();
   do {
      giiOngoing = TestHelper.getStat_getInitialImagesInProgress(REGION_NAME);
   } while ((giiOngoing > 0) && (System.currentTimeMillis() - start < MAX_WAIT_MILLIS));
   Log.getLogWriter().info("Done waiting " + MAX_WAIT_MILLIS + " millis for getIntialImage to complete, giiOngoing is " + giiOngoing);
   if (giiOngoing > 0)
      throw new TestException("Waited " + MAX_WAIT_MILLIS + " for getInitialImage to complete, but giiOngoing is " + giiOngoing);
}

/** Wait for forced disconnect conditions
 *     1) Cache.isClosed() should be true
 *     2) DistributedSystem.isConnected() should be false
 *     3) Notifier hooks for forced disconnect before and after should complete.
 *     4) An afterRegionDestroy with a forcedDisconnect operation should occur.
 *
 *  This relies on ControllerBB.incMapCounter(ControllerBB.NumForcedDiscEvents)
 *  being called when an afterRegionDestroyed event with a forced disconnect  
 *  Operation is detected.
 *  Also, splitBrain.MemberNotifyHook must be installed in the vm receiving
 *  the forced disconnect.
 */
public static void waitForForcedDiscConditions(DistributedSystem ds,
                                               Cache theCache) {
   long startTime = System.currentTimeMillis();
   Log.getLogWriter().info("Waiting for a forced disconnect to occur...");
   boolean isUnion = 
           (InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY1_ATTR).equals("normal")) &&
           (InitImageBB.getBB().getSharedMap().get(InitImageBB.DATAPOLICY2_ATTR).equals("normal"));
   while (true) { // wait for conditions of a forced disconnect to occur
      boolean isConnected = ds.isConnected();
      boolean cacheClosed = theCache.isClosed();
      int forcedDiscEventCount = ControllerBB.getMapCounter(ControllerBB.NumForcedDiscEventsKey);
      boolean membershipFailureComplete = ControllerBB.isMembershipFailureComplete();
      Log.getLogWriter().info("Waiting for forced disconnect conditions," +
                              "\nis connected (looking for false): " + isConnected + 
                              "\nafterRegionDestroyedEvent count (looking for 1): " + forcedDiscEventCount + 
                              "\nmembership failure complete (looking for true): " + membershipFailureComplete + 
                              "\ncacheClosed (looking for true): " + cacheClosed);
      if ((forcedDiscEventCount == 1) && !isConnected && cacheClosed && membershipFailureComplete) {
         // conditions met for a forced disconnect
         Log.getLogWriter().info("Forced disconnect conditions have been met");
         ControllerBB.getBB().getSharedCounters().incrementAndRead(ControllerBB.metCounter);
         // we really want this test to complete the FD while gii is in progress
         // is preference; for preference tests the gii will end early, possibly
         // just prior to the test noticing the forced disconnect, so it's ok
         // not to check if the gii is in progress for perference (the test 
         // causes forced disconnect in all source regions
         if (isUnion) { 
            if (!isAnyGiiInProgress()) {
               // we really want this test to complete the FD while gii is in progress
               long gaveUpTime = System.currentTimeMillis();
               long waitTime = gaveUpTime - startTime;
               throw new TestException("Giving up waiting for a forced disconnect after " + 
                  waitTime + " millis, all giis have already completed");
            }
         }
         break;
      } else if (forcedDiscEventCount > 1) { 
         String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + 
                         "; Number of forced disconnect events for this vm is " + forcedDiscEventCount +
                         ", expected 1";
         Log.getLogWriter().info(errStr);
         ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
      } else {
         // we really want this test to complete the FD while gii is in progress;
         // for preference tests the gii will end early, possibly
         // just prior to the test noticing the forced disconnect, so it's ok
         // not to check if the gii is in progress for perference (the test 
         // causes forced disconnect in all source regions
         if (isUnion) { 
            if (!isAnyGiiInProgress()) {
               long gaveUpTime = System.currentTimeMillis();
               long waitTime = gaveUpTime - startTime;
               throw new TestException("Giving up waiting for a forced disconnect after " + 
                  waitTime + " millis, all giis have already completed");
            }
            MasterController.sleepForMs(1500);
         }
      }
   }      
   Log.getLogWriter().info("Done waiting for forced disconnect conditions...");
   startTime = ControllerBB.getMembershipFailureStartTime();  
   long endTime = ControllerBB.getMembershipFailureCompletionTime();  
   long duration = endTime - startTime;
   String aStr = "It took " + duration + " ms to complete membership failure " +
      "(time between MembershipNotifyHook.beforeMembershipFailure and " +
      "MembershipNotifyHook.afterMembershipFailure.";
   Log.getLogWriter().info(aStr);
   if (duration > 60000) { // if it takes more than 1 minute throw an exception
      throw new TestException(aStr);
   }
}

}

