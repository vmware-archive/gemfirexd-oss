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
package cq;

import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import event.EventBB;
import util.EventCountersBB;
import util.NameFactory;
import vsphere.vijava.VMotionTestBase;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.distributed.*;

public class CQEventTest {
    
// The singleton instance of CQEventTest in this VM
static protected CQEventTest testInstance;
    
protected static final String VmIDStr = "VmId_";
protected boolean isBridgeClient;
protected boolean isSerialExecution;
protected boolean useTransactions;
protected long minTaskGranularitySec;   // the doEntryOperations task granularity in Seconds
protected long minTaskGranularityMS;    // the doEntryOperations task granularity in millis
protected int upperThreshold;               // value of CQUtilPrms.upperThreshold
protected int lowerThreshold;               // value of CQUtilPrms.lowerThreshold
protected Region aRegion;
protected static boolean isVMotionEnabled;

// operations
static protected final int ENTRY_ADD_OPERATION = 1;
static protected final int ENTRY_DESTROY_OPERATION = 2;
static protected final int ENTRY_INVALIDATE_OPERATION = 3;
static protected final int ENTRY_UPDATE_OPERATION = 6;

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of a CQ in a bridgeClient
 */
public synchronized static void HydraTask_initializeClient() throws CqClosedException, RegionNotFoundException {
   if (testInstance == null) {
      testInstance = new CQEventTest();
      testInstance.isBridgeClient = true;
      testInstance.initializeRegion("clientRegion");

      // If configured, registerInterest
      boolean registerInterest = TestConfig.tab().booleanAt(CQUtilPrms.registerInterest, false);
      if (registerInterest) {
         testInstance.aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
         CQUtilBB.incrementCounter("CQUtilBB.NUM_RI_VMS", CQUtilBB.NUM_RI_VMS);
      }
   }
   testInstance.initializeCQ();
   testInstance.initializePrms();
   CQUtilBB.incrementCounter("CQUtilBB.NUM_CQS", CQUtilBB.NUM_CQS);
}

protected void initializePrms() {
   isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution, false);
   useTransactions = getInitialImage.InitImagePrms.useTransactions();

   upperThreshold = TestConfig.tab().intAt(CQUtilPrms.upperThreshold, Integer.MAX_VALUE);
   lowerThreshold = TestConfig.tab().intAt(CQUtilPrms.lowerThreshold, -1);

   minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
   if (minTaskGranularitySec == Long.MAX_VALUE)
      minTaskGranularityMS = Long.MAX_VALUE;
   else
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
}

/** Creates and initializes an instance of a CQ in a bridgeServer
 */
public synchronized static void HydraTask_initializeBridgeServer() {
    isVMotionEnabled = TestConfig.tab().booleanAt(
        vsphere.vijava.VIJavaPrms.vMotionEnabled, false);
    if (isVMotionEnabled) {
      VMotionTestBase.setvMotionDuringCQRegistartion();
    }
 if (testInstance == null) {
      testInstance = new CQEventTest();
      testInstance.isBridgeClient = false;
      testInstance.initializeRegion("serverRegion");
      BridgeHelper.startBridgeServer("bridge");
   }
}

/** Creates and initializes a PR in a bridge server.
 */
public synchronized static void HydraTask_initializeFeed() {
   if (testInstance == null) {
      testInstance = new CQEventTest();
      testInstance.isBridgeClient = false;
      testInstance.initializeRegion("feedRegion");
   }
}

/** gets all entries from the server
 */
public synchronized static void HydraTask_getAllEntries() {
   if (testInstance == null) {
      testInstance = new CQEventTest();
   }
   testInstance.getAllEntries("clientRegion");
}

/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
protected void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   CacheHelper.generateCacheXmlFile("cache1", regDescriptName, xmlFile);
   aRegion = RegionHelper.createRegion(regDescriptName);
}

/**
 *  getAllEntries (from server)
 */
protected void getAllEntries(String regionConfig) {
   String regionName = RegionHelper.getRegionDescription(regionConfig).getRegionName();
   Region aRegion = RegionHelper.getRegion(regionName);
   for (int i = 1; i <= 10; i++) {
      Log.getLogWriter().info("getting entry Object" + i);
      Object o = aRegion.get(NameFactory.OBJECT_NAME_PREFIX + i);
      Log.getLogWriter().info("done getting entry Object" + i + " = " + o.toString());
   }
}

protected void initializeCQ() throws CqClosedException, RegionNotFoundException {
   CQUtil.initialize();
   CQUtil.initializeCQService();
   CQUtil.registerCQ(aRegion);
   // add once supported
   // CQUtil.displaySelectResults();
}
    
// ========================================================================
// test methods
    
public static void HydraTask_loadRegion() {
   testInstance.loadRegion();
}

public void loadRegion() {
   Log.getLogWriter().info("creating entries");
   for (int i = 1; i <= 10; i++) {
      if (useTransactions) {
         TxHelper.begin();
      }
      aRegion.put(NameFactory.OBJECT_NAME_PREFIX + i, new Integer(i));
      EventBB.getBB().getSharedCounters().increment(EventBB.NUM_CREATE);

      if (useTransactions) {
         TxHelper.commit();
      }
      checkEventCounters();
   }
}

public static void HydraTask_doUpdates() {
   testInstance.doUpdates();
}

public void doUpdates() {
   Log.getLogWriter().info("updating entries");
   for (int i = 1; i <= 10; i++) {
      if (useTransactions) {
         TxHelper.begin();
      }

      Object o = aRegion.get(NameFactory.OBJECT_NAME_PREFIX + i);
      int n = ((Integer)o).intValue() + 1;
      aRegion.put(NameFactory.OBJECT_NAME_PREFIX + i, new Integer(n));
      EventBB.getBB().getSharedCounters().increment(EventBB.NUM_UPDATE);

      if (useTransactions) {
         TxHelper.commit();
      }
      checkEventCounters();
   }
}

public static void HydraTask_doUpdatesSameValues() {
   testInstance.doUpdatesSameValues(); 
}

public void doUpdatesSameValues() {
   Log.getLogWriter().info("updating entries with same value");
   for (int i = 1; i <= 10; i++) {
      if (useTransactions) {
         TxHelper.begin();
      }

      Object o = aRegion.get(NameFactory.OBJECT_NAME_PREFIX + i);
      int n = ((Integer)o).intValue() + 1;
      aRegion.put(NameFactory.OBJECT_NAME_PREFIX + i, o);
      EventBB.getBB().getSharedCounters().increment(EventBB.NUM_UPDATE);

      if (useTransactions) {
         TxHelper.commit();
      }
      checkEventCounters();
   }
}

public static void HydraTask_doDestroys() {
   testInstance.doDestroys();
}

public void doDestroys() {
   Log.getLogWriter().info("destroying entries");
   for (int i = 1; i <=10; i++) {
      if (useTransactions) {
         TxHelper.begin();
      }

      try {
         aRegion.destroy(NameFactory.OBJECT_NAME_PREFIX + i);
      } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      EventBB.getBB().getSharedCounters().increment(EventBB.NUM_DESTROY);

      if (useTransactions) {
         TxHelper.commit();
      }
      checkEventCounters();
   }
}

public static void HydraTask_doInvalidates() {
   testInstance.doInvalidates();
}

public void doInvalidates() {
   Log.getLogWriter().info("invalidating entries");
   for (int i = 1; i <=10; i++) {

      if (useTransactions) {
         TxHelper.begin();
      }

      try {
         aRegion.invalidate(NameFactory.OBJECT_NAME_PREFIX + i);
      } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      EventBB.getBB().getSharedCounters().increment(EventBB.NUM_INVALIDATE);

      if (useTransactions) {
         TxHelper.commit();
      }

      checkEventCounters();
   }
}

public static void HydraTask_doOperations() {
   testInstance.loadRegion();
   testInstance.doUpdates();
   testInstance.doUpdatesSameValues();
   testInstance.doDestroys();
}

public static void HydraTask_doEntryOperations() {
   testInstance.doEntryOperations(testInstance.aRegion);
}

/** Do random entry operations on the given region ending either with
 *  minTaskGranularityMS.
 *  Uses CQUtilPrms.entryOperations to determine the operations to execute.
 */
protected void doEntryOperations(Region aRegion) {
   Log.getLogWriter().info("In doEntryOperations with " + aRegion.getFullPath());
   long startTime = System.currentTimeMillis();
   int numOps = 0;
   do {

      TestHelper.checkForEventError(ListenerBB.getBB());

      int whichOp = 0;
      whichOp = getOperation(CQUtilPrms.entryOperations);
      int size = aRegion.size();
      if (size >= upperThreshold) {
         whichOp = getOperation(CQUtilPrms.upperThresholdOperations);
      } else if (size <= lowerThreshold) {
         whichOp = getOperation(CQUtilPrms.lowerThresholdOperations);
      }

      switch (whichOp) {
         case ENTRY_ADD_OPERATION:
            addEntry(aRegion);
            break;
         case ENTRY_INVALIDATE_OPERATION:
            invalidateEntry(aRegion);
            break;
         case ENTRY_DESTROY_OPERATION:
            destroyEntry(aRegion);
            break;
         case ENTRY_UPDATE_OPERATION:
            updateEntry(aRegion);
            break;
         default: {
            throw new TestException("Unknown operation " + whichOp);
         }
      }
      numOps++;
      Log.getLogWriter().info("Completed op " + numOps + " for this task, region size is " + aRegion.size());
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   Log.getLogWriter().info("Done in doEntryOperations with " + aRegion.getFullPath() + ", completed " + numOps + " ops in " + (System.currentTimeMillis() - startTime) + " millis");

   ClientHelper.release(testInstance.aRegion);
}

/** Get a random operation using the given hydra parameter.
 *
 *  @param whichPrm A hydra parameter which specifies random operations.
 *
 *  @returns A random operation.
 */
protected int getOperation(Long whichPrm) {
   long limit = 60000;
   long startTime = System.currentTimeMillis();
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
   else
      throw new TestException("Unknown entry operation: " + operation);
   return op;
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key.
 *  @returns A key in the region.
 */
public static Object getExistingKey(Region aRegion) {
   Object key = null;
   Object[] keys = aRegion.keySet().toArray();
   int numEntries = keys.length;
   if (numEntries <= 0) {
      Log.getLogWriter().info("getExistingKey(): No keys in region " + aRegion.getFullPath());
      return key;
   }
 
   key = keys[TestConfig.tab().getRandGen().nextInt(0, numEntries-1)];
   return key;
}

/** Add a new entry to the given region.
 *
 *  @param aRegion The region to use for adding a new entry.
 *
 */
protected void addEntry(Region aRegion) {
   Object key = NameFactory.getNextPositiveObjectName();
   try {
      Log.getLogWriter().info("addEntry: creating key " + key + " in region  " + aRegion.getFullPath());
      aRegion.create(key, new Integer( (int)(NameFactory.getCounterForName(key))) );
      Log.getLogWriter().info("addEntry: done creating key " + key);
   } catch (EntryExistsException e) {
      if (isSerialExecution) { 
         // cannot get this exception in serialExecution mode
         throw new TestException(TestHelper.getStackTrace(e));
      } else {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         // in concurrent execution, somebody could have updated this key causing it to exist
      }
   }
}
    
/** Invalidate an entry in the given region.
 *
 *  @param aRegion The region to use for invalidating an entry.
 */
protected void invalidateEntry(Region aRegion) {
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
      Log.getLogWriter().info("invalidateEntry: invalidating key " + key);
      aRegion.invalidate(key);
      Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      else {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
}
    
/** Destroy an entry in the given region.
 *
 *  @param aRegion The region to use for destroying an entry.
 */
protected void destroyEntry(Region aRegion) {
   Object key = getExistingKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("destroyEntry: No keys in region");
      return;
   }
   try {
      Log.getLogWriter().info("destroyEntry: destroying key " + key);
      aRegion.destroy(key);
      Log.getLogWriter().info("destroyEntry: done destroying key " + key);
   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      else {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
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

   Log.getLogWriter().info("updateEntry: replacing key " + key);
   Object val = aRegion.get(aRegion, key);
   // If no object to start with, create newVal based on key
   if (val == null) {
      val = new Integer((int)(NameFactory.getCounterForName(key)));
   }
   Object returnVal = aRegion.put(key, new Integer( ((Integer)val).intValue()+1) );

   Log.getLogWriter().info("updateEntry: replaced (put) key " +  key + " returnVal is " + returnVal);
}
        

public static void HydraTask_checkCQ() throws CqException {
   testInstance.checkCQ();
}

public void checkCQ() {
   CQUtil.displaySelectResults();

   Set regions  = CacheHelper.getCache().rootRegions();
   StringBuffer aStr = new StringBuffer("rootRegions = \n");
   for (Iterator iter = regions.iterator(); iter.hasNext();) {
      Region aRegion = (Region)iter.next();
      aStr.append("   " + aRegion.getFullPath() + " named " + aRegion.getName() + "\n");
   }
   Log.getLogWriter().info(aStr.toString());
}

public static void HydraTask_printEventCounts() {
   EventBB.getBB().printSharedCounters();
   EventCountersBB.getBB().printSharedCounters();
   CQUtilBB.printBB();
}

/** Check event counters.  Check the operation counts (EventBB) against the Events processed 
 *  by the CQListener (CQUtilBB) and the Client CacheListeners (EventCountersBB).
 */
private void checkEventCounters() {
   SharedCounters counters = EventBB.getBB().getSharedCounters();
   EventBB.getBB().printSharedCounters();

   long numCreate = counters.read(EventBB.NUM_CREATE);
   long numUpdate = counters.read(EventBB.NUM_UPDATE);
   long numDestroy = counters.read(EventBB.NUM_DESTROY);
   long numInvalidate = counters.read(EventBB.NUM_INVALIDATE);

   StringBuffer errMsg = new StringBuffer();

   long numCQListeners =  CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.NUM_CQS);
   Log.getLogWriter().info("num threads with registered CQs = " + numCQListeners);

   // If we've registeredInterest, all registered Client VMs expect CacheListener events
   boolean registerInterest = TestConfig.tab().booleanAt(CQUtilPrms.registerInterest, false);
   boolean clientInFeederVm = TestConfig.tab().booleanAt(CQUtilPrms.clientInFeederVm, false);
   long numRIListeners =  CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.NUM_RI_VMS);
   // If we haven't registeredInterest, then we need to determine if operations are performed
   // by the feeder (expect no CacheListener events or the edgeClient (expect the client 
   // performing ops to get the local CacheListener invocation)
   if (!registerInterest) {
      numRIListeners = (clientInFeederVm) ? 0 : 1;
   }
   Log.getLogWriter().info("num VMs with cacheListeners = " + numRIListeners);

   ArrayList al = new ArrayList();

   // expected CQEvents 
   util.TestHelper.waitForCounter( CQUtilBB.getBB(), "NUM_CREATE", CQUtilBB.NUM_CREATE, (numCreate*numCQListeners), true, 60000);
   util.TestHelper.waitForCounter( CQUtilBB.getBB(), "NUM_UPDATE", CQUtilBB.NUM_UPDATE, (numUpdate*numCQListeners), true, 60000);
   util.TestHelper.waitForCounter( CQUtilBB.getBB(), "NUM_DESTROY", CQUtilBB.NUM_DESTROY, (numDestroy*numCQListeners), true, 60000);
   util.TestHelper.waitForCounter( CQUtilBB.getBB(), "NUM_INVALIDATE", CQUtilBB.NUM_INVALIDATE, (numInvalidate*numCQListeners), true, 60000);

   // expected CacheListener EntryEvents 
   util.TestHelper.waitForCounterSum( util.EventCountersBB.getBB(), "numAfterCreateEvents_isDist", "numAfterCreateEvents_isNotDist", (numCreate*numRIListeners), true, 60000);
   util.TestHelper.waitForCounterSum( util.EventCountersBB.getBB(), "numAfterUpdateEvents_isDist", "numAfterUpdateEvents_isNotDist", (numUpdate*numRIListeners), true, 60000);
   util.TestHelper.waitForCounterSum( util.EventCountersBB.getBB(), "numAfterDestroyEvents_isDist", "numAfterDestroyEvents_isNotDist", (numDestroy*numRIListeners), true, 60000);

   util.TestHelper.waitForCounterSum( util.EventCountersBB.getBB(), "numAfterInvalidateEvents_isDist", "numAfterInvalidateEvents_isNotDist", (numInvalidate*numRIListeners), true, 60000);
}

}
