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
package splitBrain;

import java.util.*;
import util.*;
import hydra.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;

public class ForcedDiscTest {
    
/* The singleton instance of ForcedDiscTest in this VM */
static public ForcedDiscTest testInstance;
    
// operations
static protected final int ENTRY_ADD_OPERATION = 1;
static protected final int ENTRY_DESTROY_OPERATION = 2;
static protected final int ENTRY_INVALIDATE_OPERATION = 3;
static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
static protected final int ENTRY_UPDATE_OPERATION = 6;
static protected final int ENTRY_GET_OPERATION = 7;
static protected final int ENTRY_GET_NEW_OPERATION = 8;
    
// instance fields
protected long minTaskGranularitySec;       // the task granularity in seconds
protected long minTaskGranularityMS;        // the task granularity in milliseconds
protected int numOpsPerTask;                // the number of operations to execute per task
protected RandomValues randomValues =       // instance of random values, used as the value for puts
          new RandomValues();               
public Region aRegion;                      // the region used for ops in this test
protected int upperThreshold;               // value of SplitBrainPrms.upperThreshold
protected int lowerThreshold;               // value of SplitBrainPrms.lowerThreshold
protected int numVMsToStop;                 // value of SplitBrainPrms.numVMsToStop
protected int secondsToRun;                 // number of seconds to allow tasks to run
protected boolean lockOperations;           // value of SplitBrainPrms.lockOperations
protected DistributedLockService distLockService; // the distributed lock service for this VM
protected boolean isBridgeConfiguration;    // true if this test is being run in a bridge configuration, false otherwise
protected boolean isBridgeClient;           // true if this vm is a bridge client, false otherwise
protected boolean expectingForcedDisconnect;// true if the threads in this vm should expect a forced
                                            // disconnect, false otherwise
protected int numExceptionThreads;          // the number of thread in this vm that got an exception due to a fd
protected static Object syncObject = new Object();

// lock names
protected static String LOCK_SERVICE_NAME = "MyLockService";
protected static String LOCK_NAME = "MyLock_";
    
// String prefixes for event callback object
protected static final String getCallbackPrefix = "Get originated in pid ";
protected static final String createCallbackPrefix = "Create event originated in pid ";
protected static final String updateCallbackPrefix = "Update event originated in pid ";
protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
    
protected static final String VmIDStr = "VmId_";
    
// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of ForcedDiscTest in this VM.
 *  This is used for initializing clients in bridge configuration, or initializing
 *  a client in a peer configuration.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new ForcedDiscTest();
      testInstance.initializeRegion("clientRegion");
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         registerInterest(testInstance.aRegion);
      } else {
         Log.getLogWriter().info("Installing MembershipNotifierHook");
         SBUtil.addMembershipHook(new MembershipNotifierHook()); 
      }
   }
}
    
/** Creates and initializes the singleton instance of ForcedDiscTest in this VM.
 *  This is used for initializing servers in bridge configurations.
 */
public synchronized static void HydraTask_initializeServer() {
   if (testInstance == null) {
      testInstance = new ForcedDiscTest();
      testInstance.initializeRegion("serverRegion");
      testInstance.initializeInstance();
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
   }
   Log.getLogWriter().info("Installing MembershipNotifierHook");
   SBUtil.addMembershipHook(new MembershipNotifierHook()); 
}
    
/**
 *  Initialize this test instance
 */
public void initializeInstance() {
   minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
   if (minTaskGranularitySec == Long.MAX_VALUE)
      minTaskGranularityMS = Long.MAX_VALUE;
   else 
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   numOpsPerTask = TestConfig.tab().intAt(SplitBrainPrms.numOpsPerTask, Integer.MAX_VALUE);
   upperThreshold = TestConfig.tab().intAt(SplitBrainPrms.upperThreshold, Integer.MAX_VALUE);
   lowerThreshold = TestConfig.tab().intAt(SplitBrainPrms.lowerThreshold, -1);
   secondsToRun = TestConfig.tab().intAt(SplitBrainPrms.secondsToRun, 600);
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   isBridgeConfiguration = bridgeNames != null;
   isBridgeClient = false;
   lockOperations = TestConfig.tab().booleanAt(SplitBrainPrms.lockOperations, false);
   if (lockOperations) {
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      distLockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
      Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
   }
   Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " +
                           "minTaskGranularityMS " + minTaskGranularityMS + ", " +
                           "numOpsPerTask " + numOpsPerTask + ", " +
                           "lockOperations " + lockOperations + ", " +
                           "upperThreshold " + upperThreshold + ", " +
                           "lowerThreshold " + lowerThreshold + ", " +
                           "isBridgeConfiguration " + isBridgeConfiguration);
}

/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
public void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   try {
      CacheHelper.generateCacheXmlFile("cache1", regDescriptName, xmlFile);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // ok; we use this to reinitialize returning VMs, so the xml file is already there
      } else {
         throw e;
      }
   }   
   aRegion = RegionHelper.createRegion(regDescriptName);
}
    
protected void reinitializeClient() {
   testInstance.initializeRegion("clientRegion");
   if (testInstance.isBridgeConfiguration) {
      registerInterest(testInstance.aRegion);
   } else {
      Log.getLogWriter().info("Installing MembershipNotifierHook");
      SBUtil.addMembershipHook(new MembershipNotifierHook()); 
   }
   ControllerBB.signalInitIsComplete();
}

protected void reinitializeServer() {
   testInstance.initializeRegion("serverRegion");
   BridgeHelper.startBridgeServer("bridge");
   Log.getLogWriter().info("Installing MembershipNotifierHook");
   SBUtil.addMembershipHook(new MembershipNotifierHook()); 
   ControllerBB.signalInitIsComplete();
}

/** Reinitialize either a client or server, depending on which this vm is.
 */
protected void reinitialize() {
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   if ((ds == null) || (!ds.isConnected())) {
      DistributedSystemHelper.disconnect(); // let hydra know we disconnected, so it will work on the reconnect
      if (isBridgeConfiguration) {
         if (isBridgeClient) {
            reinitializeClient();
         } else {
            reinitializeServer();
         }
      } else {
         reinitializeClient();
      }
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      distLockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
      Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
   }
}

// ========================================================================
// hydra task methods

/** Hydra task method to do random entry operations.
 */
public static void HydraTask_doEntryOps() {
   try {
      testInstance.doEntryOperations(testInstance.aRegion);
   } catch (Exception e) {
      testInstance.handleException(e);
   }
   testInstance.checkForTimeToStop();
}

/**
 * Cause a forced disconnect in the current vm.
 */
public static void HydraTask_causeForcedDisconnect() {
   logExecutionNumber();
   testInstance.causeForcedDisconnect();
   testInstance.checkForLastIteration();
   testInstance.checkForTimeToStop();
}
    
/**
 * Allow conditions for a forced disconnect in the current vm, 
 * then reinitialize without regard for what other threads in
 * this vm is doing.
 */
public static void HydraTask_cycleWellness() {
   logExecutionNumber();
   // we can't validate in a random test that's not carefully causing forced disconnects
   ControllerListener.validateForcedDisconnects = false;
   try {
      testInstance.cycleWellness();
   } catch (Exception e) {
      testInstance.handleExceptionWithCycleWellness(e);
   }
}
    
/** Hydra task method to do random entry operations in a fd test.
 */
public static void HydraTask_doEntryOpsWithCycleWellness() {
   try {
      testInstance.doEntryOperations(testInstance.aRegion);
   } catch (Exception e) {
      testInstance.handleExceptionWithCycleWellness(e);
   }
}

// ========================================================================
// methods to do the work of the hydra tasks

/** Do random entry operations on the given region ending either with
 *  minTaskGranularityMS or numOpsPerTask.
 */
protected void doEntryOperations(Region aRegion) {
   Log.getLogWriter().info("In doEntryOperations with " + aRegion.getFullPath());
   long startTime = System.currentTimeMillis();
   int numOps = 0;
   do {
      int whichOp = getOperation(SplitBrainPrms.entryOperations);
      int size = aRegion.size();
      if (size >= upperThreshold) {
         whichOp = getOperation(SplitBrainPrms.upperThresholdOperations);
      } else if (size <= lowerThreshold) {
         whichOp = getOperation(SplitBrainPrms.lowerThresholdOperations);
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

      Log.getLogWriter().info("expecting forced disconnect: " + expectingForcedDisconnect);
      try {
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
      ControllerBB.checkForError();
      numOps++;
      Log.getLogWriter().info("Completed op " + numOps + " for this task");
   } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) &&
            (numOps < numOpsPerTask));
   Log.getLogWriter().info("Done in doEntryOperations with " + aRegion.getFullPath());
}
        
/** Add a new entry to the given region.
 *
 *  @param aRegion The region to use for adding a new entry.
 *
 */
protected void addEntry(Region aRegion) {
   Object key = getNewKey();
   BaseValueHolder anObj = getValueForKey(key);
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
   BaseValueHolder anObj = getUpdateObject(aRegion, (String)key);
   String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
         TestHelper.toString(anObj) + ", callback is " + callback);
      Log.getLogWriter().info("Done with call to put (update)");
   } else { // do a put without callback
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
      aRegion.put(key, anObj);
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
    
/** Attempt to cause a forced disconnect in the current vm, either through a 
 *  slow listener with playDead or by becoming sick with playDead. Wait for a 
 *  forced disconnect to occur and don't return until it occurs.
 */
protected void causeForcedDisconnect() {
   numExceptionThreads = 0;
   ControllerBB.checkForError();
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   Cache theCache = CacheHelper.getCache();
   expectingForcedDisconnect = true;
   boolean fdBySlowListener = false;
   ControllerBB.enablePlayDead(); // so afterRegionDestroy can validate a forced disconnect
   SBUtil.playDead();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // cause a FD by becoming sick
      SBUtil.beSick();
   } else { // cause a FD by being slow
      fdBySlowListener = true;
      ControllerBB.enableSlowListener();
   }
   ControllerBB.checkForError();

   // recognize the forced disconnect
   ForcedDiscUtil.waitForForcedDiscConditions(ds, theCache);
   ControllerBB.waitMembershipFailureComplete();
   ControllerBB.reset(RemoteTestModule.getMyVmid());
   ControllerBB.checkForError();

   // wait for all threads in this vm to get an exception (except this thread)
   int expectedNumExceptionThr = (Integer.getInteger("numThreads")).intValue() - 1;
   while (numExceptionThreads != expectedNumExceptionThr) {
      Log.getLogWriter().info("Waiting for " + expectedNumExceptionThr + " threads in this vm to " +
         "get an exception due to a forced disconnect, current number of exception threads is " + 
         numExceptionThreads);
      MasterController.sleepForMs(2000);
   }
   Log.getLogWriter().info("Done waiting for " + expectedNumExceptionThr + " threads in this vm to " +
      "get an exception due to a forced disconnect, current number of exception threads is " + 
      numExceptionThreads);
   expectingForcedDisconnect = false; // this allows the exception threads to continue
   
   // reinitialize the vm
   reinitialize();
   Log.getLogWriter().info("Notifying all threads that reinitialization is complete");
   synchronized (syncObject) {
      syncObject.notifyAll();
   }
   Log.getLogWriter().info("Done notifying all threads that reinitialization is complete");
   ControllerBB.checkForError();
   int numThreadsInThisVM = (Integer.getInteger("numThreads")).intValue();
}

/** Allow conditions for a forced disconnect, then
 *  reinitialize after the forced disconnect without waiting for other
 *  threads to get exception from the forced disconnect.
 */
protected void cycleWellness() {
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   Cache theCache = CacheHelper.getCache();

   // cause conditions that could trigger a forced disconnect
   ControllerBB.checkForError();
   boolean fdBySlowListener = false;
   SBUtil.playDead();
   if (TestConfig.tab().getRandGen().nextBoolean()) { // cause a FD by becoming sick
      SBUtil.beSick();
   } else { // cause a FD by being slow
      fdBySlowListener = true;
      ControllerBB.enableSlowListener();
   }
   ControllerBB.checkForError();
   ForcedDiscUtil.waitForForcedDiscConditions(ds, theCache);
   ControllerBB.reset(RemoteTestModule.getMyVmid());
   ControllerBB.checkForError();
   reinitialize(); 
}

// ========================================================================
// other methods to help out in doing the tasks

/** Return a value for the given key
 */
protected BaseValueHolder getValueForKey(Object key) {
   return new ValueHolder((String)key, randomValues);
}
    
/** Return a new key, never before used in the test.
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
protected BaseValueHolder getUpdateObject(Region aRegion, String key) {
   BaseValueHolder anObj = (BaseValueHolder)aRegion.get(key);
   BaseValueHolder newObj = (anObj == null) ? new ValueHolder(key, randomValues) :
                              anObj.getAlternateValueHolder(randomValues);
   return newObj;
}

/** Log the execution number of this serial task.
 */
static protected void logExecutionNumber() {
   long exeNum = SplitBrainBB.getBB().getSharedCounters().incrementAndRead(SplitBrainBB.ExecutionNumber);
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
   else
      throw new TestException("Unknown entry operation: " + operation);
   return op;
}

/** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
 *  which is equivalent to a full GII.
 */
static protected void registerInterest(Region aRegion) {
   Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES");
   aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
   Log.getLogWriter().info("Done calling registerInterest for all keys, " +
       "result interest policy KEYS_VALUES, " + aRegion.getFullPath() + 
       " size is " + aRegion.size());
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key (may
 *         or may not be a partitioned region).
 *
 *  @returns A key in the region.
 */
public static Object getExistingKey(Region aRegion) {
   Object key = null;
   Iterator it = aRegion.keys().iterator();
   if (it.hasNext()) {
      return it.next();
   } else {
      return null;
   }
}

/** Check if we have run for the desired length of time. We cannot use 
 *  hydra's taskTimeSec parameter because of a small window of opportunity 
 *  for the test to hang due to the test's tasks being dependent on each
 *  other.
 */
protected synchronized void checkForLastIteration() {
   // determine if this is the last iteration
   long taskStartTime = 0;
   final String bbKey = "TaskStartTime";
   Object anObj = SplitBrainBB.getBB().getSharedMap().get(bbKey);
   if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      SplitBrainBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
   } else {
      taskStartTime = ((Long)anObj).longValue();
   }
   if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.TimeToStop);
   } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
         (secondsToRun - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
   }
}

/** Handle an exception thrown by doing general operations while
 *  a forced disconnect occurs.
 */
protected void handleException(Exception anExcept) {
   if (anExcept instanceof CancelException) {
      if (expectingForcedDisconnect) {
         // this vm is expecting a forced disconnect; OK
      } else { // no reason to get this error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof LockServiceDestroyedException) {
      if (expectingForcedDisconnect) {
         // this vm is expecting a forced disconnect; OK
      } else { // no reason to get this error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else {
      throw new TestException("Got unexpected exception " + TestHelper.getStackTrace(anExcept));
   }
   Log.getLogWriter().info("Caught " + anExcept + "; expected, continuing test");
   // we got here becaue we expected an exception; wait until reinitialize completes to continue
   synchronized (this.getClass()) {
      numExceptionThreads++;
   }
   Log.getLogWriter().info("Waiting for reinitialization");
   try {
      synchronized (syncObject) {
         syncObject.wait();
      }
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().info("Done waiting for reinitialization");
}

/** If we have a signal to stop, then throw StopSchedulingOrder
 */
protected void checkForTimeToStop() {
   long counter = SplitBrainBB.getBB().getSharedCounters().read(SplitBrainBB.TimeToStop);
   if (counter >= 1) {
      throw new StopSchedulingOrder("Tasks have signalled it's time to stop");
   }
}

/** Handle an exception thrown by doing general operations while
 *  a forced disconnect occurs.
 */
protected void handleExceptionWithCycleWellness(Exception anExcept) {
   if (anExcept instanceof com.gemstone.gemfire.CancelException ||
       (anExcept instanceof RegionDestroyedException) ||
       (anExcept instanceof LockServiceDestroyedException)) {
       // allow a fd exception
       Log.getLogWriter().info("Caught " + anExcept + "; continuing test");
       MasterController.sleepForMs(2000); // if we have a fd, give some time for the cause method to reinit
   } else if (anExcept instanceof LockNotHeldException) {
       String errStr = anExcept.toString();
       if (errStr.indexOf("MyLock_") > 0) {
          // this is OK; we we got a forced disconnect in the doOps after we
          // got a lock, but before we released it, then we could get this
          // exception; testing for "MyLock_" ensures this is due to the test's
          // usage of dlocks, and not the product's 
       } else {
          throw new TestException("Got unexpected exception " + TestHelper.getStackTrace(anExcept));
       }
   } else {
      throw new TestException("Got unexpected exception " + TestHelper.getStackTrace(anExcept));
   }
}

}

