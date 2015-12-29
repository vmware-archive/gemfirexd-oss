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
package versioning;

import java.util.*;
import util.*;
import hydra.*;
import cq.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.LocalRegion;

public class ConflictedOpsTest {
    
/* The singleton instance of ConflictedOpsTest in this VM */
static ConflictedOpsTest testInstance;

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
    
static protected final String REGION_NAME = "testRegion";

// instance fields
protected ConflictedOpsListener myEventListener;    // the ConflictedOpsListener
protected long minTaskGranularitySec;       // the task granularity in seconds
protected long minTaskGranularityMS;        // the task granularity in milliseconds
protected int numOpsPerTask;                // the number of operations to execute per task
protected int totalNumThreadsInTest;        // the total number of client threads in this test (across all VMs)
protected RandomValues randomValues = new RandomValues();        // instance of random values, used as the value for puts
protected Region aRegion;                   // the region for this client
protected int upperThreshold;               // value of ConflictedOpsPrms.upperThreshold
protected int lowerThreshold;               // value of ConflictedOpsPrms.lowerThreshold
protected int concurrentLeaderTid;          // the thread id of the concurrent leader
protected int myTid;
protected int myVmId;                       
protected int secondsToRun;                 // number of seconds to allow tasks
protected volatile long taskStartTime;      // the starting time for tasks, used with secondsToRun

// String prefixes for event callback object
protected static final String getCallbackPrefix = "Get originated in pid ";
protected static final String createCallbackPrefix = "Create event originated in pid ";
protected static final String updateCallbackPrefix = "Update event originated in pid ";
protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
    
// used for comparing objects in verification methods
// exact means objects must be .equal()
// equivalent, which applies only to ValueHolders, means that the myValue field
//    in a ValueHolder can be the equals after doing toString(), but not necessarily equals()
public static int EQUAL = 1;
public static int EQUIVALENT = 2;

//============================================================================
// INITTASKS
//============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
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

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  public static void HydraTask_startGatewayHubTask() {
    testInstance.startGatewayHub(ConfigPrms.getGatewayConfig());
  }

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  protected void startGatewayHub(String gatewayConfig) {
    GatewayHubHelper.addGateways(gatewayConfig);
    GatewayHubHelper.startGatewayHub();
  }

  /**
   * Creates a gateway hub with the configured HubConfig.
   */
  protected void createGatewayHub() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
    if (gatewayHubConfig != null) {
      GatewayHubHelper.createGatewayHub(gatewayHubConfig);
    }
  }

  /**
   * Re-creates a gateway hub (within HAController method)
   */
  protected void createGatewayHub(String gatewayHubConfig) {
    if (gatewayHubConfig != null) {
      GatewayHubHelper.createGatewayHub(gatewayHubConfig);
    }
  }

  //============================================================================
  // INITTASKS 
  //============================================================================

  /** Creates and initializes the singleton instance of ConflictedOpsTest in this VM.
   */
  public synchronized static void HydraTask_initialize() {
     if (testInstance == null) {
        testInstance = new ConflictedOpsTest();
        // create cache and region (before creating gatewayHub)
        CacheHelper.createCache(ConfigPrms.getCacheConfig());
        testInstance.createRegion();
        testInstance.createGatewayHub();
        testInstance.initializeInstance();
     }
  }

/**
 *  Initialize this test instance
 */
protected void initializeInstance() {

   // initialize parameters
   minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
   if (minTaskGranularitySec == Long.MAX_VALUE) {
      minTaskGranularityMS = Long.MAX_VALUE;
   } else {
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   }
   numOpsPerTask = TestConfig.tab().intAt(ConflictedOpsPrms.numOpsPerTask, Integer.MAX_VALUE);
   upperThreshold = TestConfig.tab().intAt(ConflictedOpsPrms.upperThreshold, Integer.MAX_VALUE);
   lowerThreshold = TestConfig.tab().intAt(ConflictedOpsPrms.lowerThreshold, -1);
   secondsToRun = TestConfig.tab().intAt(ConflictedOpsPrms.secondsToRun, 1800);
   concurrentLeaderTid = -1;
   
   Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " +
                           "minTaskGranularityMS " + minTaskGranularityMS + ", " +
                           "numOpsPerTask " + numOpsPerTask + ", " +
                           "secondsToRun " + secondsToRun + ", " +
                           "upperThreshold " + upperThreshold + ", " +
                           "lowerThreshold " + lowerThreshold); 
}

// ========================================================================
// hydra task methods

/** Do random operations until the region contains roughly upperThreshold
 *  entries.
 */
public static void HydraTask_loadRegion() {
  long startTime = System.currentTimeMillis();
  long lastLogTime = System.currentTimeMillis();
  do {
    Object key = testInstance.getNewKey();
    ValueHolder anObj = testInstance.getValueForKey(key);
    testInstance.aRegion.put(key, anObj);
    long counter = NameFactory.getCounterForName(key);
    if (counter >= testInstance.upperThreshold) {
      String s = "NameFactory nameCounter has reached " + counter;
      throw new StopSchedulingTaskOnClientOrder(s);
    }
    if (System.currentTimeMillis() - lastLogTime >= 10000) {
      Log.getLogWriter().info("Current nameCounter is " + counter);
      lastLogTime = System.currentTimeMillis();
    }
  } while (System.currentTimeMillis() - startTime < testInstance.minTaskGranularityMS);
}

/** Hydra task method for concurrent tests with verification.
 */
public static void HydraTask_conflictedOpsTest() {
   testInstance.conflictedOpsTest();
}

// ========================================================================
// methods to do the work of the hydra tasks

/** All threads (assigned this task) attempt to do an operation
 *  on the same key at the same time to cause a conflict which will
 *  result in only one op winning.  Threads are synchronized via the 
 *  BB to simultaneously update the same entry (with callback and value
 *  based on the vm/thread id.
 * 
 */
protected void conflictedOpsTest() {
   // wait for all threads to be ready to do this task, then do random ops
   totalNumThreadsInTest = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
   myTid = RemoteTestModule.getCurrentThread().getThreadId();
   myVmId = RemoteTestModule.getMyVmid();
   long numWaiting = ConflictedOpsBB.getInstance().getSharedCounters().incrementAndRead(ConflictedOpsBB.ReadyToBegin);
   if (numWaiting == 1) {   
     logExecutionNumber();
   }
   TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                             "ConflictedOpsBB.ReadyToBegin", 
                             ConflictedOpsBB.ReadyToBegin, 
                             totalNumThreadsInTest, 
                             true, 
                             -1,
                             1000);
   checkForLastIteration();

   // Log this task is starting
   boolean leader = false;
   leader = ConflictedOpsBB.getInstance().getSharedCounters().incrementAndRead(ConflictedOpsBB.ConcurrentLeader) == 1;
   if (leader) {
      // verify no issues found in events during last round of execution
      ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.WaitForSelectedKey);
      ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.processEvents);
      concurrentLeaderTid = RemoteTestModule.getCurrentThread().getThreadId();
      Log.getLogWriter().info("In conflictedOpsTest, concurrentLeaderTid is vm_" + myVmId + " thr_" + concurrentLeaderTid);

      // select a random key to work on and write this to the BB
      Object key = getExistingKey(aRegion);
      ConflictedOpsBB.getInstance().getSharedMap().put(ConflictedOpsBB.SelectedKey, key);
      ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.WaitForSelectedKey);

   } else { // wait for leader to set things up
      TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                                "ConflictedOpsBB.WaitForSelectedKey", 
                                ConflictedOpsBB.WaitForSelectedKey, 
                                1, 
                                true, 
                                -1,
                                500);
   }

   // Here we are really referring to the Selected key (vs. the Snapshot) 
   Log.getLogWriter().info("Zeroing ShapshotWritten");
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.SnapshotWritten);
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.ReadyToVerify);
   
   // compute newValue and callback for this threads update to the selected key
   Object key = ConflictedOpsBB.getInstance().getSharedMap().get(ConflictedOpsBB.SelectedKey);
   ValueHolder value = getValueForKey(key, myTid);
   String callback = "vm_" + myVmId + "_thr_" + myTid;

   // everyone attempts to update the same entry simultaneously
   aRegion.put(key, value, callback);

   // wait for all threads to pause (then clear counters and validate)
   Log.getLogWriter().info("Zeroing FinishedVerify");
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.FinishedVerify);
   ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.Pausing);
   TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                             "ConflictedOpsBB.Pausing", 
                             ConflictedOpsBB.Pausing, 
                             totalNumThreadsInTest, 
                             true, 
                             -1,
                             5000);


   Log.getLogWriter().info("Zeroing ReadyToBegin");
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.ReadyToBegin);
   ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.ReadyToVerify);
   TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                             "ConflictedOpsBB.ReadyToVerify", 
                             ConflictedOpsBB.ReadyToVerify, 
                             totalNumThreadsInTest, 
                             true, 
                             -1,
                             1000);
   SilenceListener.waitForSilence(30, 1000);
   conflictedOpsVerify();   

   // wait for everybody to finish verify, then exit
   ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.FinishedVerify);
   TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                             "ConflictedOpsBB.FinishedVerify", 
                             ConflictedOpsBB.FinishedVerify, 
                             totalNumThreadsInTest, 
                             true, 
                             -1,
                             1000);

   Log.getLogWriter().info("Zeroing ConcurrentLeader, Pausing");
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.ConcurrentLeader);
   ConflictedOpsBB.getInstance().getSharedCounters().zero(ConflictedOpsBB.Pausing);
   concurrentLeaderTid = -1;
   myEventListener.setLastNewValue(null);
   myEventListener.setNumEvents(0);

   long counter = ConflictedOpsBB.getInstance().getSharedCounters().read(ConflictedOpsBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num executions is " + 
            ConflictedOpsBB.getInstance().getSharedCounters().read(ConflictedOpsBB.ExecutionNumber));
}

/** Do random entry operations on the given region ending either with
 *  minTaskGranularityMS or numOpsPerTask.
 *  Uses ConflictedOpsPrms.entryOperations to determine the operations to execute.
 */
protected void doEntryOperations(Region aRegion) {
   Log.getLogWriter().info("In doEntryOperations with " + aRegion.getFullPath());
   long startTime = System.currentTimeMillis();
   int numOps = 0;
   do {
      int whichOp = getOperation(ConflictedOpsPrms.entryOperations);
      int size = aRegion.keys().size();
      Log.getLogWriter().info("Selecting operation, region size is " + size);
      if (size >= upperThreshold) {
         whichOp = getOperation(ConflictedOpsPrms.upperThresholdOperations);
      } else if (size <= lowerThreshold) {
         whichOp = getOperation(ConflictedOpsPrms.lowerThresholdOperations);
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
         case PUT_IF_ABSENT_OPERATION:
            putIfAbsent(aRegion);
            break;
         case REMOVE_OPERATION:
            remove(aRegion);
            break;
         case REPLACE_OPERATION:
            replace(aRegion);
            break;
         default: {
            throw new TestException("Unknown operation " + whichOp);
         }
      }
      numOps++;
      Log.getLogWriter().info("Completed op " + numOps + " for this task");
   } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) &&
            (numOps < numOpsPerTask));
}
        
/** Add a new entry to the given region.
 *
 *  @param aRegion The region to use for adding a new entry.
 *
 *  @returns The key that was added.
 */
protected Object addEntry(Region aRegion) {
   Object key = getNewKey();
   ValueHolder anObj = getValueForKey(key);
   String callback = createCallbackPrefix + ProcessMgr.getProcessId();
   int beforeSize = aRegion.keys().size();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
         try {
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " + 
               aRegion.getFullPath());
            aRegion.create(key, anObj, callback);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         } catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         //} catch (CapacityControllerException e) {
         //   throw new TestException(TestHelper.getStackTrace(e));
         } catch (EntryExistsException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      } else { // use create with no cacheWriter arg
         try {
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.create(key, anObj);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         } catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         //} catch (CapacityControllerException e) {
         //   throw new TestException(TestHelper.getStackTrace(e));
         } catch (EntryExistsException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
   } else { // use a put call
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
         Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
               TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
         try {
            aRegion.put(key, anObj, callback);
         } catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         //} catch (CapacityControllerException e) {
         //   throw new TestException(TestHelper.getStackTrace(e));
         } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
         Log.getLogWriter().info("addEntry: done putting key " + key);
      } else {
         Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
               TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
         try {
            aRegion.put(key, anObj);
         } catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         //} catch (CapacityControllerException e) {
         //   throw new TestException(TestHelper.getStackTrace(e));
         } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
         Log.getLogWriter().info("addEntry: done putting key " + key);
      }
   }
   return key;
}
    
/** Invalidate an entry in the given region.
 *
 *  @param aRegion The region to use for invalidating an entry.
 *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
 */
protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
   int beforeSize = aRegion.keys().size();
   Object key = getExistingKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("invalidateEntry: No keys in region");
      return;
   }
   boolean containsKey = aRegion.containsKey(key);
   boolean containsValueForKey = aRegion.containsValueForKey(key);
   Log.getLogWriter().info("containsKey for " + key + ": " + containsKey);
   Log.getLogWriter().info("containsValueForKey for " + key + ": " + containsValueForKey);
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
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
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
      Log.getLogWriter().info("destroyEntry: No keys in region");
      return;
   }
   int beforeSize = aRegion.keys().size();
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
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheWriterException e) {
      if (e instanceof com.gemstone.gemfire.cache.util.BridgeWriterException) {
         if (e.toString().indexOf(EntryNotFoundException.class.getName()) >= 0) {
            Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
            return;
         } else {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
   }
}

/** Add a new entry to the given region via putIfAbsent
 *
 *  @param aRegion The region to use for adding a new entry.
 *
 *  @returns The key that was added.
 */
protected Object putIfAbsent(Region aRegion) {
   Object key = getNewKey(); // guaranteed to be a new key in the test
   ValueHolder anObj = getValueForKey(key);
   int beforeSize = aRegion.keys().size();

   try {
      Log.getLogWriter().info("putIfAbsent: creating key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
      Object retVal = aRegion.putIfAbsent(key, anObj);
      Log.getLogWriter().info("putIfAbsent: done creating key " + key);

      if (retVal != null) {
        Log.getLogWriter().info("putIfAbsent for key " + key + " expected successful operation (with return value of null), but entry already exists with value " + retVal);
        throw new TestException(TestHelper.getStackTrace());
      }
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } 
   return key;
}

/** Replace an existing entry in the given region. If there are
 *  no available keys in the region, then this is a noop.
 *
 *  @param aRegion The region to use for updating an entry.
 */
protected void replace(Region aRegion) {
   Object key = getExistingKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("replace: No keys in region");
      return;
   }
   int beforeSize = aRegion.keys().size();
   ValueHolder anObj = getUpdateObject(aRegion, (String)key);
      Log.getLogWriter().info("Replacing key " + key);
      Object retVal = aRegion.replace(key, anObj);
      Log.getLogWriter().info("Done with call to replace " + key + " with return value " + retVal);
}

/** Remove an entry from the given region.
 *
 *  @param aRegion The region to use for removing an entry.
 */
protected void remove(Region aRegion) {
   Object key = getExistingKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("remove: No keys in region");
      return;
   }
   int beforeSize = aRegion.keys().size();
   try {
      Log.getLogWriter().info("remove: removing key " + key);
      aRegion.remove(key, aRegion.get(key));
      Log.getLogWriter().info("remove: done removing key " + key);
   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
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
      Log.getLogWriter().info("updateEntry: No keys in region");
      return;
   }
   int beforeSize = aRegion.keys().size();
   ValueHolder anObj = getUpdateObject(aRegion, (String)key);
   String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
         TestHelper.toString(anObj) + ", callback is " + callback);
      try {
         aRegion.put(key, anObj, callback);
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      //} catch (CapacityControllerException e) {
      //   throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      Log.getLogWriter().info("Done with call to put (update)");
   } else { // do a put without callback
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
      try {
         aRegion.put(key, anObj);
      //} catch (CapacityControllerException e) {
      //   throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      Log.getLogWriter().info("Done with call to put (update)");
   }
}
    
/** Get an existing key in the given region if one is available,
 *  otherwise get a new key. 
 *
 *  @param aRegion The region to use for getting an entry.
 */
protected void getKey(Region aRegion) {
   Object key = getExistingKey(aRegion);
   if (key == null) { // no existing keys; get a new key then
      getNewKey(aRegion);
      return;
   }
   String callback = getCallbackPrefix + ProcessMgr.getProcessId();
   int beforeSize = aRegion.keys().size();
   boolean beforeContainsValueForKey = aRegion.containsValueForKey(key);
   Object anObj;
   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getKey: getting key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      } else { // get without callback
         Log.getLogWriter().info("getKey: getting key " + key);
         anObj = aRegion.get(key);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      }
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
/** Get a new key int the given region.
 *
 *  @param aRegion The region to use for getting an entry.
 */
protected void getNewKey(Region aRegion) {
   Object key = getNewKey();
   String callback = getCallbackPrefix + ProcessMgr.getProcessId();
   int beforeSize = aRegion.keys().size();
   boolean beforeContainsValueForKey = aRegion.containsValueForKey(key);
   Object anObj;
   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
         Log.getLogWriter().info("getNewKey: getting value for key " + key + ": " + TestHelper.toString(anObj));
      } else { // get without callback
         Log.getLogWriter().info("getNewKey: getting key " + key);
         anObj = aRegion.get(key);
         Log.getLogWriter().info("getNewKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      }
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
// ========================================================================
// other methods to help out in doing the tasks

/** Check if we have run for the desired length of time. We cannot use 
 *  hydra's taskTimeSec parameter because of a small window of opportunity 
 *  for the test to hang due to the test's "concurrent round robin" type 
 *  of strategy. Here we set a blackboard counter if time is up and this
 *  is the last concurrent round.
 */
protected void checkForLastIteration() {
   // determine if this is the last iteration
   if (taskStartTime == 0) {
      taskStartTime = System.currentTimeMillis();
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
   }
   if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      ConflictedOpsBB.getInstance().getSharedCounters().increment(ConflictedOpsBB.TimeToStop);
   } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
         ((System.currentTimeMillis() - taskStartTime) / 1000) + " seconds");
   }
}

/** Return a value for the given key and initialize modVal with the given value
 */
protected ValueHolder getValueForKey(Object key, Integer modVal) {
   return new ValueHolder((String)key, randomValues, modVal);
}

/** Return a value for the given key
 */
protected ValueHolder getValueForKey(Object key) {
   return new ValueHolder((String)key, randomValues);
}
    
/** Return a new key
 */
protected Object getNewKey() {
   return NameFactory.getNextPositiveObjectName();
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
protected ValueHolder getUpdateObject(Region aRegion, String key) {
   try {
      Object anObj = aRegion.get(key);
      if ((anObj != null) && !(anObj instanceof ValueHolder)) {
         throw new TestException("Getting key " + key + " returned " + TestHelper.toString(anObj) +
                   ", but an instance of ValueHolder was expected");
      }
      ValueHolder vh = (ValueHolder)anObj;
      ValueHolder newObj = (vh == null) ? new ValueHolder(key, randomValues) :
                                          (ValueHolder)vh.getAlternateValueHolder(randomValues);
      return newObj;
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Log the execution number of this serial task.
 */
static protected void logExecutionNumber() {
   long exeNum = ConflictedOpsBB.getInstance().getSharedCounters().incrementAndRead(ConflictedOpsBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
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
   if (operation.equals("add")) {
      op = ENTRY_ADD_OPERATION;
   } else if (operation.equals("update")) {
      op = ENTRY_UPDATE_OPERATION;
   } else if (operation.equals("invalidate")) {
      op = ENTRY_INVALIDATE_OPERATION;
   } else if (operation.equals("destroy")) {
      op = ENTRY_DESTROY_OPERATION;
   } else if (operation.equals("get")) {
      op = ENTRY_GET_OPERATION;
   } else if (operation.equals("getNew")) {
      op = ENTRY_GET_NEW_OPERATION;
   } else if (operation.equals("localInvalidate")) {
      op = ENTRY_LOCAL_INVALIDATE_OPERATION;
   } else if (operation.equals("localDestroy")) {
      op = ENTRY_LOCAL_DESTROY_OPERATION;
   } else if (operation.equals("putIfAbsent")) {
      op = PUT_IF_ABSENT_OPERATION;
   } else if (operation.equals("remove")) {
      op = REMOVE_OPERATION;
   } else if (operation.equals("replace")) {
      op = REPLACE_OPERATION;
   } else {
      throw new TestException("Unknown entry operation: " + operation);
   }
   return op;
}

// ========================================================================
// verification methods

protected void verifySelectedKeyValue(Object key) {
   ValueHolder expectedValue = (ValueHolder)ConflictedOpsBB.getInstance().getSharedMap().get(ConflictedOpsBB.ExpectedValue);

   StringBuffer aStr = new StringBuffer();

   // containsKey
   try {
      verifyContainsKey(aRegion, key, true);
   } catch (TestException e) {
      aStr.append(e.getMessage() + "\n");
   }

   // containsValueForKey
   boolean containsValueForKey = aRegion.containsValueForKey(key);
   try {
      verifyContainsValueForKey(aRegion, key, (expectedValue != null));
   } catch (TestException e) {
      aStr.append(e.getMessage());
   }

   if (containsValueForKey) { // only call get if it's already there
                              // otherwise we could cause an event
      try {                  
         // verify value in cache matches leader's value
         ValueHolder actualValue = (ValueHolder)aRegion.get(key);   
         verifyMyValue(key, expectedValue, actualValue, EQUAL);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (TestException e) {
        aStr.append(e.getMessage() + "\n");
      }

      if (aStr.length() == 0) {
         Log.getLogWriter().info("Done (successfully) verifying value of selected key " + key + ":" +  expectedValue.toString());
      } else {
         Log.getLogWriter().info("Validation of local cache value against concurrentLeader's cache value failed: " + aStr.toString());
      }

      try {                  
         // verify final event processed by this vms listener matches leader's value
         // this can be null with PRs (since events are only fired int he primary
         int numEvents = myEventListener.getNumEvents();
         if (numEvents > TestHelper.getNumThreads()) {
           throw new TestException("numEvents in this vm (" + numEvents + ") for last operation > numThreadsInTest (" + TestHelper.getNumThreads() + ").  Suspect that we handled an elided event");
         }
         Log.getLogWriter().info("This vm processed " + numEvents + " events for this operation");
         ValueHolder lastNewValueFromMyListener = (ValueHolder)myEventListener.getLastNewValue();
         if (lastNewValueFromMyListener != null) {
           verifyMyValue(key, expectedValue, lastNewValueFromMyListener, EQUAL);
         }
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (TestException e) {
        aStr.append(e.getMessage() + "\n");
      }
   }

   if (aStr.length() == 0) {
      Log.getLogWriter().info("Done (successfully) verifying value last update in this vm for key " + key + ":" +  expectedValue.toString());
   } else {
      Log.getLogWriter().info("Validation of newValue in last update (fired in this vm) against concurrentLeader's cache value failed: " + aStr.toString());
   }
   
   try {
      TestHelper.checkForEventError(ConflictedOpsBB.getInstance());
   } catch (TestException e) {
      aStr.append("Listener threw Exception:" + e);
   }
 
   if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
   }
}

/** 
 */
protected void conflictedOpsVerify() {
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   Object key = ConflictedOpsBB.getInstance().getSharedMap().get(ConflictedOpsBB.SelectedKey);
   Log.getLogWriter().info("In conflictedOpsVerify, with myTid " + myTid + ", concurrentLeaderTid is " + concurrentLeaderTid);
   if (myTid == concurrentLeaderTid) { 
      // this is the first thread to verify; all other threads will wait for this thread to
      // write its view of the region to the blackboard and they will read it and match it
      Log.getLogWriter().info("This thread is the concurrentLeader, providing the selected key and expected final value after this op");
      ValueHolder value = null;
      try {
        value = (ValueHolder)aRegion.get(key);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      ConflictedOpsBB.getInstance().getSharedMap().put(ConflictedOpsBB.ExpectedValue, value);
      Log.getLogWriter().info("Done posting expected value" + value);
      long snapshotWritten = ConflictedOpsBB.getInstance().getSharedCounters().incrementAndRead(ConflictedOpsBB.SnapshotWritten);
      Log.getLogWriter().info("Incremented SnapshotWritten, now is " + snapshotWritten);
   } else { 
      // this thread is not the first to verify; it will wait until the first thread has
      // written its state to the blackboard, then it will read it and verify that it's state matches
      TestHelper.waitForCounter(ConflictedOpsBB.getInstance(), 
                                "ConflictedOpsBB.SnapshotWritten", 
                                ConflictedOpsBB.SnapshotWritten, 
                                1, 
                                true, 
                                -1,
                                2000);
   }
   // Since we are also validating listener events, the leader must also do validation
   verifySelectedKeyValue(key);
}

/** Verify that the size of the given region is expectedSize.
 *
 * @param aRegion The region to verify.
 * @param expectedSize The expected size of aRegion.
 *
 * @throws TestException if size() has the wrong value
 */
protected static void verifySize(Region aRegion, final int expectedSize) {
  int size = aRegion.keys().size();
  if (size != expectedSize) {
      throw new TestException("Expected size of " + aRegion.getFullPath() + " to be " +
         expectedSize + ", but it is " + size + ", keys are: " + aRegion.keys());
   }
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key.
 *
 *  @returns A key in the region.
 */
protected Object getExistingKey(Region aRegion) {
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   Set aSet = aRegion.keys();
   if (aSet.size() == 0) {
      return null;
   }
   Iterator it = aSet.iterator();
   Object key = null;
   while (it.hasNext()) {
      key = it.next();
      return key;
   } 
   return null;
}

/** Verify containsKey for the given region and key.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expected The expected value of containsKey()
 *
 * @throws TestException if containsKey() has the wrong value
 */
protected static void verifyContainsKey(Region aRegion, Object key, boolean expected) {
   boolean containsKey = aRegion.containsKey(key);
   if (containsKey != expected) {
      throw new TestException("Expected containsKey() for " + key + " to be " + expected + 
                " in " + aRegion.getFullPath() + ", but it is " + containsKey);
   }
}

/** Verify containsValueForKey for the given region and key.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expected The expected value of containsKey()
 *
 * @throws TestException if containsValueforKey() has the wrong value
 */
protected static void verifyContainsValueForKey(Region aRegion, Object key, boolean expected) {
   boolean containsValueForKey = aRegion.containsValueForKey(key);
   if (containsValueForKey != expected) {
      throw new TestException("Expected containsValueForKey() for " + key + " to be " + expected + 
                " in " + aRegion.getFullPath() + ", but it is " + containsValueForKey);
   }
}

/** Verify that the given object is an instance of ValueHolder
 *  with expectedValue as the myValue field.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expectedValue The expected myValue field of a ValueHolder in aRegion, or null
 *        if the expected value should be null.
 * @param valuetoCheck This is expected to be a ValueHolder, whose myValue field compares
 *        to expectedValue, according to comparStrategy
 * @param compareStrategy Whether the compare is equals or equivalent (for ValueHolders)
 *
 * @throws TestException if the result of a get on key does not have the expected value.
 */
protected static void verifyMyValue(Object key, ValueHolder expectedValue, ValueHolder valueToCheck, int compareStrategy) {
   if (valueToCheck == null) {
      if (expectedValue != null) {
         throw new TestException("For key " + key + ", expected myValue to be " + 
                   TestHelper.toString(expectedValue) + 
                   ", but it is " + TestHelper.toString(valueToCheck));
      }
   } else if (valueToCheck instanceof ValueHolder) {
      if (compareStrategy == EQUAL) {
         if (!expectedValue.equals(valueToCheck)) {
            throw new TestException("For key " + key + ", expected ValueHolder to be " + 
                   TestHelper.toString(expectedValue) + ", but it is " + 
                   TestHelper.toString(valueToCheck));
         }
      } else if (compareStrategy == EQUIVALENT) { 
         if (!expectedValue.toString().equals(valueToCheck.toString())) {
            throw new TestException("For key " + key + ", expected ValueHolder to be " + 
                      TestHelper.toString(expectedValue) + ", but it is " + 
                      TestHelper.toString(valueToCheck));
         }
      }
   } else {
      throw new TestException("Expected value for key " + key + " to be an instance of ValueHolder, but it is " +
         TestHelper.toString(valueToCheck));
   }
}

/** create region based on config prms and reset test instance 'aRegion'
 */
protected void createRegion() {
  aRegion = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
  Log.getLogWriter().info("After creating region, region size is " + aRegion.keys().size());

  // make ConflictedOpsListener available to all threads in this vm
  CacheListener[] listeners = aRegion.getAttributes().getCacheListeners();
  myEventListener = (ConflictedOpsListener)listeners[0];

  // if configured as a bridgeServer, start the server
  String bridgeConfig = ConfigPrms.getBridgeConfig();
  if (bridgeConfig != null) {
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  // edge clients register interest in ALL_KEYS
  if (aRegion.getAttributes().getPoolName() != null) {
     aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
     Log.getLogWriter().info("registered interest in ALL_KEYS for " + REGION_NAME);
  }
}

}
