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
package asyncMsg; 

import util.*;
import getInitialImage.InitImageTest;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import objects.*;
import java.util.*;
import tx.EntryValidator;
import tx.RegionValidator;
import tx.OpList;

/**
 * A class to contain methods useful for all asyncMessaging functional tests.
 */
public class AsyncMsgTest {

// the only instance of asyncMsgTest
static protected AsyncMsgTest asyncTest = null;
static GsRandom randGen = TestConfig.tab().getRandGen();

// instance fields
public RandomValues randomValues = null; // for random object creation
protected int maxKeys;            // initial object count (per region)
int modValInitializer=0;          // track ValueHolder.modVal
boolean isSerialExecution; 
boolean verifyConflationBehavior;      // used by serialRRConflation test
protected int concurrentLeaderTid;     // the thread id of the concurrent leader
protected int numPeerThreadsInTest;   // the total number of client threads in this test (across all VMs)

// static fields
// Keys with Special meaning for our SleepListener
public static final String SLEEP_KEY = "SleepKey";

public static final String DIST_ACK_REGION = "distAckRegion";

public static final int NO_MIRRORING_RESTRICTION = 0;
public static final int NO_MIRRORING = 1;
public static final int NO_MIRRORING_KV = 2;
public static final String REGION_NAME = "AsyncRegion";
public static int numUpdatesThisRound = 0;

// used for comparing objects in verification methods
// exact means objects must be .equal()
// equivalent, which applies only to ValueHolders, means that the myValue field
//    in a ValueHolder can be the equals after doing toString(), but not necessarily equals()
public static int EQUAL = 1;
public static int EQUIVALENT = 2;

/////////////////////////////////////////////////////////////////////////
// hydra tasks
/////////////////////////////////////////////////////////////////////////

  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  public static void startLocatorAndAdminDSTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

/*
 *  Initialize info for test (random number generator, isSerialExecution,
 *  etc.
 */
public static void HydraTask_initialize() {
  Log.getLogWriter().info("In HydraTask_initialize");
  if (asyncTest == null) {
    asyncTest = new AsyncMsgTest();
    asyncTest.initialize();
  }
}
                                                                              
public void initialize() {
   randomValues = new RandomValues();
   isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
   verifyConflationBehavior = TestConfig.tab().booleanAt(AsyncMsgPrms.verifyConflationBehavior, false);
   numPeerThreadsInTest = TestHelper.getNumThreads() - 1; // subtract the admin vm thread
}

/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_createRegionForest() {
   Cache c = CacheUtil.createCache();
   if (asyncTest == null) {
      asyncTest = new AsyncMsgTest();
      asyncTest.initialize();
      asyncTest.createRegionHierarchy();
      AsyncMsgTest.createDistAckRegion();
   }
   asyncTest.summarizeRegionHier();
}

/** Do random operations on random regions using the hydra parameter 
 *  operations as the list of available operations and 
 *  AsyncMsgPrms.numOps as the number of operations to do.
 */
public static void HydraTask_doOperationsWithPrimedQueue() {
   Log.getLogWriter().info("In HydraTask_doOperationsWithPrimedQueue");
   if (asyncTest == null) {
     asyncTest = new AsyncMsgTest();
     asyncTest.initialize();
   }
   asyncTest.primeQueue();
   asyncTest.doOperations();
}

/** Do AsyncMsgPrms.numOps updates on a random region/entry
 *  (after priming the queue for asyncQueuing).
 */
public static void HydraTask_doUpdatesWithPrimedQueue() {
   Log.getLogWriter().info("In HydraTask_doUpdatesWithPrimedQueue");
   if (asyncTest == null) {
     asyncTest = new AsyncMsgTest();
     asyncTest.initialize();
   }
   Region aRegion = asyncTest.primeQueue();
   asyncTest.doUpdates(aRegion);
}

/** Do random operations on random regions using the hydra parameter 
 *  operations as the list of available operations and 
 *  AsyncMsgPrms.numOps as the number of operations to do.
 */
public static void HydraTask_doOperations() {
   Log.getLogWriter().info("In HydraTask_doOperations");
   if (asyncTest == null) {
     asyncTest = new AsyncMsgTest();
     asyncTest.initialize();
   }
   try {
      asyncTest.doOperations();
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** verify that the publishing thread actually performed asyncConflation
 *  by using the DistributionStats (asyncConflatedMsgs)
 */
public synchronized static void HydraTask_checkForConflation() {
   Log.getLogWriter().info("In HydraTask_checkForConflation");
   if (asyncTest == null) {
     asyncTest = new AsyncMsgTest();
     asyncTest.initialize();
   }
   asyncTest.checkForConflation();
}

private void checkForConflation() {
   if (!queuedMessagesConflated()) {
      throw new TestException("TuningRequired: AsyncConflatedMsgs is 0");
   } 
}

/** Creates and initializes the singleton instance of AsyncMsgTest in this VM.
 */
public synchronized static void HydraTask_initProducer() {
   if (asyncTest == null) {
      asyncTest = new AsyncMsgTest();
      asyncTest.initialize();
      RegionDefinition regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, "producer");
      regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, null, new Loader(), null);
      AsyncMsgTest.createDistAckRegion();
   }
}

/** Creates and initializes the singleton instance of AsyncMsgTest in this VM.
 */
public synchronized static void HydraTask_initConsumer() {
   if (asyncTest == null) {
      asyncTest = new AsyncMsgTest();
      asyncTest.initialize();
      RegionDefinition regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, "consumer");
      regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, new BurstListener(), null, null);
      AsyncMsgTest.createDistAckRegion();
   }
}

/** Creates and initializes the singleton instance of AsyncMsgTest in this VM.
 */
public synchronized static void HydraTask_initProducerConsumer() {
   if (asyncTest == null) {
      asyncTest = new AsyncMsgTest();
      asyncTest.initialize();
      RegionDefinition regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, "producerConsumer");
      regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, new BurstListener(), new Loader(), null);
      asyncTest.createDistAckRegion();
   }
}

/** Hydra task method for producer for concurrent tests with verification.
 */
public static void HydraTask_doProducer() {
   try {
      asyncTest.doProducer();
   } catch (SchedulingOrder e) {
      throw e;
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task method for consumer for concurrent tests with verification.
 */
public static void HydraTask_doConsumer() {
   try {
      asyncTest.doConsumer();
   } catch (SchedulingOrder e) {
      throw e;
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task method for consumer for concurrent tests with verification.
 */
public static void HydraTask_doControlledOperations() {
   try {
      asyncTest.doControlledOperations();
      AsyncMsgTest.waitForEventsByPut(); // wait for other VMs to catch up
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task method to ensure that messages have been queued.
 */
public static void HydraTask_verifyQueuedMessages() {
   synchronized (AsyncMsgTest.class) {
      if (asyncTest == null) {
         asyncTest = new AsyncMsgTest();
      }
   }
   try {
      asyncTest.verifyQueuedMessages();
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task method to ensure that no aync buffer writes have occurred.
 *  Used to validate async messaging disabled in SSL tests
 */
public static void HydraTask_verifyNoAsyncBufferWrites() {
   synchronized (AsyncMsgTest.class) {
      if (asyncTest == null) {
         asyncTest = new AsyncMsgTest();
      }
   }
   asyncTest.verifyNoAsyncBufferWrites();
}

/** Hydra task method to create a dist ack region.
 */
public synchronized static void HydraTask_createDistAckRegion() {
   try {
      CacheUtil.createCache();
      Region distAckRegion = CacheUtil.getRegion(DIST_ACK_REGION);
      if (distAckRegion == null) {
         createDistAckRegion();
      }
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task method to put into a dist ack region to cause all
 *  slow receivers to catch up.
 */
public static void HydraTask_waitForEventsByPut() {
   try {
      waitForEventsByPut(); // wait for other VMs to catch up
   } catch (Exception e) {
      handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task to verify that operations concurrent to getInitialImage
 *  are non-blocking.
 */
public static void HydraTask_verifyNonBlocking() {
  if (AsyncMsgBB.getBB().getSharedMap().containsKey(AsyncMsgBB.SlowReceiverDetectedKey)) {
    Log.getLogWriter().info("Returning because a slow receiver forced disconnect has been detected");
    return;
  }
  InitImageTest.HydraTask_verifyNonBlocking();
}

/** Hydra task to verify event counters.
 */
public static void HydraTask_verifyEventCounters() {
  if (AsyncMsgBB.getBB().getSharedMap().containsKey(AsyncMsgBB.SlowReceiverDetectedKey)) {
    Log.getLogWriter().info("Returning because a slow receiver forced disconnect has been detected");
    return;
  }
  InitImageTest.HydraTask_verifyEventCounters();
}

/** Hydra task to verify the keys and values in a region using keyIntervals.
 */
public static void HydraTask_asyncMsgVerifyRegionContents() {
  if (AsyncMsgBB.getBB().getSharedMap().containsKey(AsyncMsgBB.SlowReceiverDetectedKey)) {
    Log.getLogWriter().info("Returning because a slow receiver forced disconnect has been detected");
    return;
  }
   InitImageTest.HydraTask_verifyRegionContents();
}

// ======================================================================== 
// methods for task execution and round robin counters

/** Log the number of tasks that have been executed in this test.
 *  Useful for debugging/analyzing serial execution tests.
 */
public static void logExecutionNumber() {
   long exeNum = AsyncMsgBB.getBB().getSharedCounters().incrementAndRead(AsyncMsgBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}

// ======================================================================== 
// methods to create region hierarchy

/** Creates a distAck region for use in coordinating between publisher and
 *  subscribers.  When the publisher completes this operation, we know that
 *  all events have beeen processed.  The publisher can then return and the
 *  validators can execute.
 */
public static void createDistAckRegion() {
   AttributesFactory factory = new AttributesFactory();
   factory.setScope(Scope.DISTRIBUTED_ACK);
   factory.setMirrorType(MirrorType.KEYS_VALUES);
   RegionAttributes ratts = factory.createRegionAttributes();
   Region distAckRegion = CacheUtil.createRegion(DIST_ACK_REGION, ratts);
   Log.getLogWriter().info("Created Region " + TestHelper.regionToString(distAckRegion, true));
}

/** 
 *  Create a forest of regions based on numRootRegions,
 *  numSubRegions, regionDepth
 */
public void createRegionHierarchy() {

   CacheListener aListener = AsyncMsgPrms.getCacheListener();
   CacheLoader aLoader = getCacheLoader();
   CacheWriter cacheWriter = getCacheWriter();

   int numRoots = TestConfig.tab().intAt(AsyncMsgPrms.numRootRegions);
   int breadth = TestConfig.tab().intAt(AsyncMsgPrms.numSubRegions);
   int depth = TestConfig.tab().intAt(AsyncMsgPrms.regionDepth);

   Cache c = CacheUtil.createCache();

   Vector roots = new Vector();
   Region r = null;
   for (int i=0; i<numRoots; i++) {
      String rootName = "root" + (i+1);
      RegionDefinition regDef = RegionDefinition.createRegionDefinition();
      Region rootRegion = regDef.createRootRegion(c, rootName, aListener, aLoader, cacheWriter);
      Log.getLogWriter().info("Created root region " + rootName);
      recordRegionDefInBB(rootRegion.getFullPath(), regDef);
      createEntries( rootRegion );
      createSubRegions(rootRegion, breadth, depth, "Region");
   }
}

/** 
 *  Create the subregion hierarchy of a root. 
 */
private void createSubRegions( Region r, int numChildren, int levelsLeft, String parentName) {
   CacheListener aListener = AsyncMsgPrms.getCacheListener();
   CacheLoader aLoader = getCacheLoader();
   CacheWriter cacheWriter = getCacheWriter();
   String currentName;

   boolean randomlyEnableAsyncConflation = TestConfig.tab().booleanAt(AsyncMsgPrms.randomEnableAsyncConflation, false);

   for (int i=1; i<=numChildren; i++) {
      currentName = parentName + "-" + i;
      Region child = null;
      try {
         RegionDefinition regDef = RegionDefinition.createRegionDefinition();
         if (randomlyEnableAsyncConflation) {
           int randInt = randGen.nextInt(0, 100);
           boolean enableAsyncConflation = (randInt < 50) ? true : false;
           regDef.setAsyncConflation(true);
         }
         RegionAttributes regionAttrs = regDef.getRegionAttributes(aListener, aLoader, cacheWriter);
         child = r.createSubregion(currentName, regionAttrs);
         Log.getLogWriter().info("Created subregion " + TestHelper.regionToString(child, true));
         recordRegionDefInBB(child.getFullPath(), regDef);
      } catch (RegionExistsException e) {
         child = r.getSubregion(currentName);
         Log.getLogWriter().info("Got subregion " + TestHelper.regionToString(child, true));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e)); 
      }
      createEntries( child );
      if (levelsLeft > 1)
         createSubRegions(child, numChildren, levelsLeft-1, currentName);
   }
}

  /**
   *  Create maxKeys entries in the given region
   *  Initializes ValueHolder.modVal sequentially 1-n 
   *  across all regions
   */
  private void createEntries(Region aRegion) {
    this.maxKeys = TestConfig.tab().intAt(AsyncMsgPrms.maxKeys, 10);
    long startKey = 0;
    for (int i=0; i < maxKeys; i++) {
      String key = NameFactory.getObjectNameForCounter(startKey + i);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, startKey + i);
      Object val = new ValueHolder( key, randomValues, new Integer(modValInitializer));
      modValInitializer++;
      try {
        aRegion.create( key, val );
      } catch (RegionDestroyedException e) {
        if (isSerialExecution) // not expected in serial tests
           throw e;
        Log.getLogWriter().info("Created " + i + " keys in " + aRegion.getFullPath() + 
               " before getting " + e + "; continuing test");
        break;
      } catch (EntryExistsException e) {
        // we received this entry via distribution (created by another VM)
        Log.getLogWriter().fine("Created via distribution in region <" + aRegion.getFullPath() + "> " + key + " = " + val.toString());
        continue;
      } catch (CacheRuntimeException ce) {
        throw new TestException("Cannot create key " + key + " CacheException(" + ce + ")");
      }
      Log.getLogWriter().fine("Created in region <" + aRegion.getFullPath() + "> " + key + " = " + val.toString());
    }
  }

  /** Return a currently existing random region. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegion(boolean allowRootRegion) {
     return getRandomRegion(allowRootRegion, CacheUtil.getCache().getRegion(DIST_ACK_REGION), NO_MIRRORING_RESTRICTION);
  }

  /** Return a currently existing random region that is not mirrored, nor
   *  is any of its subregion. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegionNoMirroring(boolean allowRootRegion) {
     return getRandomRegion(allowRootRegion, CacheUtil.getCache().getRegion(DIST_ACK_REGION), NO_MIRRORING);
  }

  /** Return a currently existing random region that is not mirrored with keys/values,
   *  nor is any of its subregions. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegionNoMirroringKV(boolean allowRootRegion) {
     return getRandomRegion(allowRootRegion, CacheUtil.getCache().getRegion(DIST_ACK_REGION), NO_MIRRORING_KV);
  }

  /** Return a currently existing random region. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *  @param excludeRegion Return a region other than this region, or null if none to exclude.
   *  @param restriction Restriction on what region can be returned, for example
   *            NO_MIRRORING_RESTRICTION, NO_MIRRORING, NO_MIRRORING_KV
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegion(boolean allowRootRegion, Region excludeRegion, int restriction) {
     // Get the set of all regions available
     Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
     if (rootRegionArr.length == 0) {
        return null;
     }
     ArrayList regionList = new ArrayList();
     if (allowRootRegion) {
        for (int i = 0; i < rootRegionArr.length; i++) 
           regionList.add(rootRegionArr[i]);
     };
     for (int i = 0; i < rootRegionArr.length; i++) {
        Region rootRegion = (Region)(rootRegionArr[i]);
        Object[] regionArr = getSubregions(rootRegion, true).toArray();
        for (int j = 0; j < regionArr.length; j++) 
           regionList.add(regionArr[j]);
     }

     // choose a random region
     if (regionList.size() == 0) {
        return null;
     }
     int randInt = TestConfig.tab().getRandGen().nextInt(0, regionList.size() - 1);
     Region aRegion = (Region)regionList.get(randInt);
     if ((restriction != NO_MIRRORING_RESTRICTION) || (excludeRegion != null)) { // we have a restriction
        int startIndex = randInt;
        boolean done;
        do {
           done = true;
           try {
              if (restriction == NO_MIRRORING)
                 done = !(isHierMirrored(aRegion));
              else if (restriction == NO_MIRRORING_KV)
                 done = !(isHierMirroredKV(aRegion));
              if ((excludeRegion != null) && (aRegion.getFullPath().equals(excludeRegion.getFullPath()))) {
                 done = false;
              }
           } catch (RegionDestroyedException e) {
              done = false;
           }
           if (done)
              break;
           randInt++; // go to the next region
           if (randInt == regionList.size()) // wrap if necessary
              randInt = 0; 
           if (randInt == startIndex) { // went all the way through regionList
              return null;
           }
           aRegion = (Region)(regionList.get(randInt));
        } while (!done);
     } 
     return aRegion;
  }

  /**
   *  Method to traverse tree & display all keys
   */
  public void dispAllKeys() {

     Cache c = CacheUtil.getCache();
     Set roots = c.rootRegions();

     for (Iterator it=roots.iterator(); it.hasNext(); ) {
       Region aRegion = (Region)it.next();
       Set subRegions = getSubregions(aRegion, true);
       for (Iterator sit=subRegions.iterator(); sit.hasNext(); ) {
         Region sRegion = (Region)sit.next();
         Set keys = sRegion.keys();
         for (Iterator kit=keys.iterator(); kit.hasNext(); ) {
           String key = (String)kit.next();
        
           BaseValueHolder value = null;
           try {
             value = (BaseValueHolder)sRegion.get(key);
           } catch (Exception e) {
             throw new HydraRuntimeException( "Error in get(key)", e);
           }
           if (value != null) {
             Log.getLogWriter().info("Key(" + key + ") = {" + value.getMyValue() + ", " + value.getModVal() + "}");
           } else {
             throw new HydraRuntimeException( "Key(" + key + ") has value null");
           }
         }
       }
     }
  }

/** Log the regions and the number of keys in each.  */
public void summarizeRegionHier() {
   class RegionComparator implements Comparator {
      public int compare(Object o1, Object o2) {
         return (((Region)o1).getFullPath().compareTo(((Region)o2).getFullPath()));
      }
      public boolean equals(Object anObj) {
         return ((Region)this).getFullPath().equals(((Region)anObj).getFullPath());
      }
   }

   StringBuffer aStr = new StringBuffer();
   Cache c = CacheUtil.getCache();
   Set roots = c.rootRegions();
   if (roots.size() == 0) {
      Log.getLogWriter().info("No region roots");
      return;
   }
   for (Iterator it = roots.iterator(); it.hasNext(); ) {
      Region root = (Region)it.next();
      aStr.append(root.getFullPath() + " (" + root.getAttributes().getScope() + "): " + 
                  root.keys().size() + " keys\n");
      TreeSet aSet = new TreeSet(new RegionComparator());
      aSet.addAll(getSubregions(root, true));
      for (Iterator subit = aSet.iterator(); subit.hasNext(); ) {
         Region aRegion = (Region)subit.next();
         String regionName = aRegion.getFullPath();
         try {
            Set keySet = new TreeSet(aRegion.keys());
            aStr.append("   " + regionName + " (" + aRegion.getAttributes().getScope() + "): " + 
                        keySet.size() + " keys\n");
            if (keySet.size() > 0) {
               Iterator keyIt = keySet.iterator();
               aStr.append("      ");
               while (keyIt.hasNext())
                 aStr.append(keyIt.next() + " ");
               aStr.append("\n");
            }
         } catch (RegionDestroyedException e) {
            aStr.append("   " + regionName + " is destroyed\n");
         }
      }
   }
   Log.getLogWriter().info(aStr.toString());
}

// ======================================================================== 
// methods to do operations

/** Do AsyncMsgPrms.numOps updates on an entry in the given region
 */
public void doUpdates(Region aRegion) {
   int numOps = TestConfig.tab().intAt(AsyncMsgPrms.numOps);
                                                                               
   SharedCounters sc = AsyncMsgBB.getBB().getSharedCounters();
   sc.zero(AsyncMsgBB.OPLIST_READY_FOR_VALIDATION);

   AsyncMsgBB.getBB().getSharedMap().put( AsyncMsgBB.conflationEnabled, new Boolean(aRegion.getAttributes().getEnableAsyncConflation()));
   Log.getLogWriter().info("enableAsyncConflation for region " + aRegion.getFullPath() + " = " + aRegion.getAttributes().getEnableAsyncConflation());
                                                                               
   logExecutionNumber(); 

   OpList opList = new OpList();
   Object key = getRandomKey(aRegion);
   for (int i = 0; i < numOps; i++) {
      tx.Operation op = null;
      op = updateEntry(aRegion, key);
      if (op != null) {
        opList.add(op);
      }
   }

   AsyncMsgBB.getBB().getSharedMap().put( AsyncMsgBB.OpListKey, opList );
   Log.getLogWriter().info(" OpList written to BB with key = " + AsyncMsgBB.OpListKey + " OpList = " + opList );
                                                                               
   // allow time for distribution, Let validator threads know we're ready!
   // do this by waiting for a put to a distributedAck region
   // we'll tuck the executionNumber in as the value (in case that's useful)
   if (isSerialExecution) {       
        waitForEventsByPut();
   }    
   sc.increment(AsyncMsgBB.OPLIST_READY_FOR_VALIDATION);
                                                                               
   // temp -- check stats
   Log.getLogWriter().info(" asyncMessagesQueued = " + messagesQueued());
   Log.getLogWriter().info(" queuedMessagesConflated = " + queuedMessagesConflated());
}

/** Do random operations on random regions using the hydra parameter
 *  AsyncMsgPrms.operations as the list of available operations and
 *  AsyncMsgPrms.numOps as the number of operations to do.  */
public OpList doOperations() {
   Vector operations = TestConfig.tab().vecAt(AsyncMsgPrms.operations);
   int numOps = TestConfig.tab().intAt(AsyncMsgPrms.numOps);

   SharedCounters sc = AsyncMsgBB.getBB().getSharedCounters();
   sc.zero(AsyncMsgBB.OPLIST_READY_FOR_VALIDATION);

   logExecutionNumber();

   OpList opList = doOperations(operations, numOps);
   AsyncMsgBB.getBB().getSharedMap().put( AsyncMsgBB.OpListKey, opList );
   Log.getLogWriter().info(" OpList written to BB with key = " + AsyncMsgBB.OpListKey + " OpList = " + opList );
  
   // allow time for distribution, Let validator threads know we're ready!
   // do this by waiting for a put to a distributedAck region
   // we'll tuck the executionNumber in as the value (in case that's useful)
   if (isSerialExecution) {
      waitForEventsByPut();
   }
   sc.increment(AsyncMsgBB.OPLIST_READY_FOR_VALIDATION);

   // temp -- check stats 
   Log.getLogWriter().info(" asyncMessagesQueued = " + messagesQueued());
   Log.getLogWriter().info(" queuedMessagesConflated = " + queuedMessagesConflated());
   return opList;
}

/** Do random operations on random regions.
 *  
 *  @param operations - a Vector of operations to choose from.
 *  @param numOperationsToDo - the number of operations to execute.
 */
public OpList doOperations(Vector operations, int numOperationsToDo) {
   Log.getLogWriter().info("Executing " + numOperationsToDo + " random operations...");
   final long TIME_LIMIT_MS = 60000;  
      // limit on how long this method will try to do UNSUCCESSFUL operations; 
      // for example, if the test is configured such that it destroys
      // entries but does not create any, eventually no entries will be left
      // to destroy and this method will be unable to fulfill its mission
      // to execute numOperationsToDo operations; This is a consistency
      // check for test configuration problems
   long timeOfLastOp = System.currentTimeMillis();
   OpList opList = new OpList();
   int numOpsCompleted = 0;
   while (numOpsCompleted < numOperationsToDo) {
      tx.Operation op = null;

      // choose a random operation, forcing a region create if there are no regions
      String operation = (String)(operations.get(TestConfig.tab().getRandGen().nextInt(0, operations.size()-1)));
      if (!operation.equals(tx.Operation.CACHE_CLOSE) && !operation.equals(tx.Operation.REGION_CREATE)) {
         // operation requires a choosing a random region
         Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
         if ((rootRegionArr.length == 0) || ((rootRegionArr.length == 1) && (((Region)rootRegionArr[0]).getName().equals(DIST_ACK_REGION) ))) { // no regions available; force a create region op
            if (operations.indexOf(tx.Operation.REGION_CREATE) < 0) // create not specified
               throw new TestException("No regions are available and no create region operation is specified");
            Log.getLogWriter().info("In doOperations, forcing region create because no regions are present");
            operation = tx.Operation.REGION_CREATE;
         }
      }
      Log.getLogWriter().info("Operation is " + operation);

      if (operation.equalsIgnoreCase(tx.Operation.ENTRY_CREATE)) {
         op = createEntry(getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_UPDATE)) {
         Region aRegion = getRandomRegion(true);
         op = updateEntry(aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_DESTROY)) {
         Region aRegion = getRandomRegion(true);
         op = destroyEntry(false, aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_LOCAL_DESTROY)) {
         Region aRegion = getRandomRegionNoMirroring(true);
         if (aRegion != null)
            op = destroyEntry(true, aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_INVAL)) {
         Region aRegion = getRandomRegion(true);
         op = invalEntry(false, aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_LOCAL_INVAL)) {
         Region aRegion = getRandomRegionNoMirroringKV(true);
         if (aRegion != null)
            op = invalEntry(true, aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_NEW_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithNewKey(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_EXIST_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithExistingKey(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.ENTRY_GET_PREV_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithPreviousKey(aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.REGION_CREATE)) {
         op = createRegion();
      } else if (operation.equalsIgnoreCase(tx.Operation.REGION_DESTROY)) {
         op = destroyRegion(false, getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(tx.Operation.REGION_LOCAL_DESTROY)) {
         Region aRegion = getRandomRegionNoMirroring(true);
         if (aRegion != null)
            op = destroyRegion(true, aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.REGION_INVAL)) {
         Region aRegion = getRandomRegion(true);
         op = invalRegion(false, getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(tx.Operation.REGION_LOCAL_INVAL)) {
         Region aRegion = getRandomRegionNoMirroringKV(true);
         if (aRegion != null)
            op = invalRegion(true, aRegion);
      } else if (operation.equalsIgnoreCase(tx.Operation.CACHE_CLOSE)) {
         op = closeCache();
      } else { // unknown operation
        throw new TestException("Unknown operation " + operation);
      }

      if (op == null) { // op could be null if, for example, we tried to do an op on an empty region
         // make sure we don't continually try operations that cannot be successful; see
         // comments on TIME_LIMIT_MS above
         if (System.currentTimeMillis() - timeOfLastOp > TIME_LIMIT_MS) {
            throw new TestException("Could not execute a successful operation in " + TIME_LIMIT_MS +
                                    " millis; possible test config problem");
         }
      } else {
         opList.add(op);
         numOpsCompleted++;
         timeOfLastOp = System.currentTimeMillis();
      }
   } 
//   Log.getLogWriter().info("Completed execution of " + opList);
   return opList;
}

// ======================================================================== 
// methods to do operations on region entries

/** Creates a new key/value in the given region by creating a new
 *  (never-used-before) key and a random value.
 *  
 *  @param aRegion The region to create the new key in.
 *  
 *  @return An instance of tx.Operation describing the create operation.
 */
public tx.Operation createEntry(Region aRegion) {
   Object key = NameFactory.getNextPositiveObjectName();
   BaseValueHolder vh = getNewValue(key);
   try {
      Log.getLogWriter().info("createEntry: putting key " + key + ", object " + 
        vh.toString() + " in region " + aRegion.getFullPath());
      aRegion.put(key, vh);
      Log.getLogWriter().info("createEntry: done putting key " + key + ", object " + 
        vh.toString() + " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
//   } catch (CacheRuntimeException e) {
//      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_CREATE, null, vh.modVal);
}

/** Updates an existing entry in aRegion. The value in the key
 *  is increment by 1.
 *  
 *  @param aRegion The region to modify a key in.
 *  @param key The key to modify.
 *
 *  @return An instance of tx.Operation describing the update operation.
 */
public tx.Operation updateEntry(Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not update a key in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }

   // get the old value
   BaseValueHolder vh = null;

   Log.getLogWriter().info("updateEntry: Getting value to prepare for update for key " + key + " in region " + aRegion.getFullPath());

   Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder) {
      vh = (BaseValueHolder)oldValue;
      oldValue = ((BaseValueHolder)oldValue).modVal;
   } else {
      vh = getNewValue(key);
   }

   Log.getLogWriter().info("updateEntry: Value to update is " + vh + " for key " + key + " in region " + aRegion.getFullPath());

   // we MUST use CopyHelper here (vs. copyOnRead) since we are using
   // getValueInVM() vs. a public 'get' api
   vh = (BaseValueHolder)CopyHelper.copy(vh);
   vh.modVal = new Integer(vh.modVal.intValue() + 1);
   try {
      Log.getLogWriter().info("updateEntry: Putting new value " + vh + " for key " + key + 
          " in region " + aRegion.getFullPath());
      aRegion.put(key, vh);
      Log.getLogWriter().info("updateEntry: Done putting new value " + vh + " for key " + key + 
          " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   }
   return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_UPDATE, oldValue, vh.modVal);
}

/** Destroys an entry in aRegion. 
 *  
 *  @param isLocalDestroy True if the opertion is a local destroy, false otherwise.
 *  @param aRegion The region to destroy the entry in.
 *  @param key The key to destroy.
 *
 *  @return An instance of tx.Operation describing the destroy operation.
 */
public tx.Operation destroyEntry(boolean isLocalDestroy, Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not destroy an entry in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      if (isLocalDestroy) {
         Log.getLogWriter().info("destroyEntry: locally destroying key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.localDestroy(key);
         Log.getLogWriter().info("destroyEntry: done locally destroying key " + key +
             " in region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_LOCAL_DESTROY, oldValue, null);
      } else {
         Log.getLogWriter().info("destroyEntry: destroying key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.destroy(key);
         Log.getLogWriter().info("destroyEntry: done destroying key " + key +
             " in region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_DESTROY, oldValue, null);
      }
   } catch (RegionDestroyedException e) { // somebody else beat us to it
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      // if concurrent, this is OK
      return null;
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Invalidates an entry in aRegion. 
 *  
 *  @param isLocalInval True if the opertion is a local invalidate, false otherwise.
 *  @param aRegion The region to invalidate the entry in.
 *  @param key The key to invalidate.
 *
 *  @return An instance of tx.Operation describing the invalidate operation.
 */
public tx.Operation invalEntry(boolean isLocalInval, Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not invalidate an entry in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      if (isLocalInval) {
         Log.getLogWriter().info("invalEntry: locally invalidating key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.localInvalidate(key);
         Log.getLogWriter().info("invalEntry: done locally invalidating key " + key +
             " in region " + aRegion.getFullPath());
         Object newValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_LOCAL_INVAL, oldValue, newValue);
      } else {
         Log.getLogWriter().info("invalEntry: invalidating key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.invalidate(key);
         Log.getLogWriter().info("invalEntry: done invalidating key " + key +
             " in region " + aRegion.getFullPath());
         Object newValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_INVAL, oldValue, newValue);
      }
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      // if concurrent, this is OK
      return null;
   }
}

/** Gets a value in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of tx.Operation describing the get operation.
 */
public tx.Operation getEntryWithExistingKey(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntryWithExistingKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      Object value = aRegion.get(key);
      BaseValueHolder vh = (BaseValueHolder)value;
      Log.getLogWriter().info("getEntryWithExistingKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_EXIST_KEY, oldValue, null);
      else
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_EXIST_KEY, oldValue, vh.modVal);
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Gets a value in aRegion with a random key that was previously used in the test.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of tx.Operation describing the get operation.
 */
public tx.Operation getEntryWithPreviousKey(Region aRegion) {
   long keysUsed = NameFactory.getPositiveNameCounter();
   Object key = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)keysUsed));
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      Log.getLogWriter().info("getEntryWithPreviousKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      Object value = aRegion.get(key);
      BaseValueHolder vh = (BaseValueHolder)value;
      Log.getLogWriter().info("getEntryWithPreviousKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, null);
      else
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_PREV_KEY, oldValue, vh.modVal);
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Gets a value in aRegion with a the next new (never-before_used) key.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of tx.Operation describing the get operation.
 */
public tx.Operation getEntryWithNewKey(Region aRegion) {
   Object key = NameFactory.getNextPositiveObjectName();
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntryWithNewKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getEntryWithNewKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_NEW_KEY, oldValue, null);
      else
         return new tx.Operation(aRegion.getFullPath(), key, tx.Operation.ENTRY_GET_NEW_KEY, oldValue, vh.modVal);
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Puts a new key/value in the given region.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param key The key to use for the put.
 *  @param value The value to put.
 *  @param opName The operation to use for the returned tx.Operation instance.
 *  
 *  @return An instance of tx.Operation describing the put operation.
 */
public tx.Operation putEntry(Region aRegion, Object key, BaseValueHolder value, String opName) {
   Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder)
      oldValue = ((BaseValueHolder)oldValue).modVal;
   try {
      Log.getLogWriter().info("putEntry: putting key " + key + ", object " + 
        value + " in region " + aRegion.getFullPath());
      aRegion.put(key, value);
      Log.getLogWriter().info("putEntry: done putting key " + key + ", object " + 
        value + " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheRuntimeException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new tx.Operation(aRegion.getFullPath(), key, opName, oldValue, value.modVal);
}

/** Does a get with the given key.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param key The key to use for the put.
 *  @param opName The operation to use for the returned tx.Operation instance.
 *  
 *  @return An instance of tx.Operation describing the get operation.
 */
public tx.Operation getEntry(Region aRegion, Object key, String opName) {
   try {
      Object oldValue = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntry: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getEntry: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new tx.Operation(aRegion.getFullPath(), key, opName, oldValue, null);
      else
         return new tx.Operation(aRegion.getFullPath(), key, opName, oldValue, vh.modVal);
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

// ======================================================================== 
// methods to do operations on regions

/** Invalidates the given region. 
 *  
 *  @param isLocalInval True if the opertion is a local invalidate, false otherwise.
 *  @param aRegion The region to invalidate.
 *
 *  @return An instance of tx.Operation describing the invalidate operation.
 */
public tx.Operation invalRegion(boolean isLocalInval, Region aRegion) {
   try {
      if (isLocalInval) {
         Log.getLogWriter().info("invalRegion: locally invalidating region " + aRegion.getFullPath());
         aRegion.localInvalidateRegion();
         Log.getLogWriter().info("invalRegion: done locally invalidating region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), null, tx.Operation.REGION_LOCAL_INVAL, null, null);
      } else {
         Log.getLogWriter().info("invalRegion: invalidating region " + aRegion.getFullPath());
         aRegion.invalidateRegion();
         Log.getLogWriter().info("invalRegion: done invalidating region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), null, tx.Operation.REGION_INVAL, null, null);
      }
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Destroys the given region. 
 *  
 *  @param isLocalDestroy True if the opertion is a local destroy, false otherwise.
 *  @param aRegion The region to destroy.
 *
 *  @return An instance of tx.Operation describing the destroy operation.
 */
public tx.Operation destroyRegion(boolean isLocalDestroy, Region aRegion) {
   if (aRegion == null) {
     return null;
   }

   try {
      recordDestroyedRegion(aRegion);
      if (isLocalDestroy) {
         Log.getLogWriter().info("destroyRegion: locally destroying region " + aRegion.getFullPath());
         aRegion.localDestroyRegion();
         Log.getLogWriter().info("destroyRegion: done locally destroying region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), null, tx.Operation.REGION_LOCAL_DESTROY, null, null);
      } else {
         Log.getLogWriter().info("destroyRegion: destroying region " + aRegion.getFullPath());
         aRegion.destroyRegion();
         Log.getLogWriter().info("destroyRegion: done destroying region " + aRegion.getFullPath());
         return new tx.Operation(aRegion.getFullPath(), null, tx.Operation.REGION_DESTROY, null, null);
      }
   } catch (RegionDestroyedException e) { // somebody else beat us to it
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Creates a region that was previously destroyed.
 *  If this test is using the BB to track destroyed regions, then
 *  choose a region from the BB.  Otherwise, look for a previously
 *  destroyed region by attempting to create all regions in the
 *  hierarchy until one is found to create. This is less efficient
 *  than getting the values from the blackboard, but the blackboard
 *  is used only for serial execution tests as it can't be maintained
 *  properly for concurrently execution tests.
 *  
 *  @return An instance of tx.Operation describing the destroy operation.
 */
public tx.Operation createRegion() {
   ArrayList list = (ArrayList)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.DestroyedRegionsKey));
   if (list != null) { // using BB to track destroyed regions
      for (int i = 0; i < list.size(); i++) {
         String regionPath = (String)(list.get(i));
         Object[] tmp = createRegionWithPath(regionPath, false);
         Region aRegion = (Region)(tmp[0]);
         boolean regionCreated = ((Boolean)(tmp[1])).booleanValue();
         if (regionCreated)
            return new tx.Operation(regionPath, null, tx.Operation.REGION_CREATE, null, null);
      }
   } else { // not using BB to track regions
      Map aMap = getRegionDefMap();
      Iterator it = aMap.keySet().iterator();
      while (it.hasNext()) {
         String regionPath = (String)(it.next());
         Object value = aMap.get(regionPath);
         if (value instanceof RegionDefinition) {
            Object[] tmp = createRegionWithPath(regionPath, false);
            Region aRegion = (Region)(tmp[0]);
            boolean regionCreated = ((Boolean)(tmp[1])).booleanValue();
            if (regionCreated)
               return new tx.Operation(regionPath, null, tx.Operation.REGION_CREATE, null, null);
         }
      }
   }
   return null;
}

// ======================================================================== 
// methods for cache operations

/** Closes the cache.
 *  
 *  @return An instance of tx.Operation describing the close operation.
 */
public tx.Operation closeCache() {
   CacheUtil.closeCache();
   return new tx.Operation(null, null, tx.Operation.CACHE_CLOSE, null, null);
}

// ======================================================================== 
// other methods 

/** Get a random key from the given region. If no keys are present in the
 *  region, return null.
 *
 *  @param aRegion - The region to get the key from.
 *
 *  @returns A key from aRegion.
 */
public Object getRandomKey(Region aRegion) {
   return getRandomKey(aRegion, null);
}

/** Get a random key from the given region, excluding the key specified
 *  by excludeKey. If no keys qualify in the region, return null.
 *  This looks only for keys which begin with the prefix "Object_".
 *
 *  @param aRegion - The region to get the key from.
 *  @param excludeKey - The region to get the key from.
 *
 *  @returns A key from aRegion, or null.
 */
public Object getRandomKey(Region aRegion, Object excludeKey) {
   if (aRegion == null)
      throw new TestException("aRegion is " + aRegion);
   Set aSet = null;
   try {
      aSet = aRegion.keys();
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      return null;
   }
   // reduce this down to keys which startWith "Object_"
   // to avoid using special SleepKey & objects which were created to
   // prime the asyncMsgQueue
   Set validKeys = new HashSet();
   for (Iterator it = aSet.iterator(); it.hasNext();) {
     Object key = (String)it.next();
     if (key instanceof String) {
       if (((String)key).startsWith("Object_")) {
         validKeys.add(key);
       }
     } 
   }
   Object[] keyArr = validKeys.toArray();
   if (keyArr.length == 0) {
      Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + 
          " because the region has no keys");
      return null;
   }
   int randInt = TestConfig.tab().getRandGen().nextInt(0, keyArr.length-1);
   Object key = keyArr[randInt];
   if (key.equals(excludeKey)) { // get another key
      if (keyArr.length == 1) { // there are no other keys
         return null;
      }
      randInt++; // go to the next key
      if (randInt == keyArr.length)
         randInt = 0; 
      key = keyArr[randInt];
   }
   return key;
}

public CacheLoader getCacheLoader() {
  return null;
}

public CacheWriter getCacheWriter() {
  return null;
}

public BaseValueHolder getNewValue(Object key) {
   return new ValueHolder(key, randomValues, new Integer(modValInitializer++));
}

/** Given a region name, return the region instance for it. If the 
 *  region with regionName currently does not exist, create it (and 
 *  any parents required to create it).
 *
 *  @param regionName - The full path name of a region.
 *  @param fill - If true, then fill each created region with entries
 *                according to AsyncMsgPrms.maxEntries
 *  
 *  @returns [0] The region specified by regionName
 *           [1] Whether any new regions were created to return [0]
 */
public Object[] createRegionWithPath(String regionName, boolean fill) {
   boolean regionCreated = false;
   Cache theCache = CacheUtil.getCache();
   StringTokenizer st = new StringTokenizer(regionName, "/", false);

   // see if we need to create a root region
   String currentRegionName = st.nextToken();
   Region aRegion = theCache.getRegion(currentRegionName); // root region
   if (aRegion == null) { // create the root
      try {
         RegionDefinition regDef = getRegionDefFromBB("/" + currentRegionName);
         RegionAttributes attr = regDef.getRegionAttributes(
            AsyncMsgPrms.getCacheListener(), getCacheLoader(), getCacheWriter());
         aRegion = theCache.createVMRegion(currentRegionName, attr);
         Log.getLogWriter().info("Created root region " + aRegion.getFullPath());
         if (fill)
            createEntries(aRegion);
         regionCreated = true;
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (RegionExistsException e) {
         if (isSerialExecution)
            throw new TestException("Test error; unexpected " + TestHelper.getStackTrace(e));
         // ok if concurrent
         aRegion = CacheUtil.getRegion(currentRegionName);
         if (aRegion == null) { // somebody must have destroyed it after we got the regionExistsException
            return new Object[] {null, new Boolean(false)};
         }
      }
   }
   Region previousRegion = aRegion;

   // now that we have the root, see what regions along the region path need to be created
   while (st.hasMoreTokens()) {
      currentRegionName = st.nextToken();
      String regionPath = previousRegion.getFullPath() + "/" + currentRegionName;
      try {
         aRegion = theCache.getRegion(regionPath);
      } catch (RegionDestroyedException e) {
         if (isSerialExecution)
            throw e; // should not get this exception in serial tests
         // in concurrent tests, another thread could be destroying this region
         // or its parent
         return new Object[] {null, new Boolean(false)};
      }
      if (aRegion == null) {
         try {
            RegionDefinition regDef = getRegionDefFromBB(regionPath);
            RegionAttributes attr = regDef.getRegionAttributes(
               AsyncMsgPrms.getCacheListener(), getCacheLoader(), getCacheWriter());
            Log.getLogWriter().info("Attempting to create region " + currentRegionName + " with " + regDef);
            aRegion = previousRegion.createSubregion(currentRegionName, attr);
            Log.getLogWriter().info("Created region " + aRegion.getFullPath());
            if (fill)
               createEntries(aRegion);
            regionCreated = true;
         } catch (RegionDestroyedException e) {
            if (isSerialExecution)
               throw e; // should not get this exception in serial tests
            // in concurrent tests, another thread could be destroying this region
            // or its parent; without the destroy we would have got a RegionExistsException
            return new Object[] {null, new Boolean(false)};
         } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
         } catch (RegionExistsException e) {
            if (isSerialExecution)
               throw new TestException("Test error; unexpected " + TestHelper.getStackTrace(e));
            // have concurrent execution
            aRegion = CacheUtil.getRegion(regionPath);
            if (aRegion == null) { // somebody must have destroyed it after we got the regionExistsException
               return new Object[] {null, new Boolean(false)};
            }
         }
      }
      previousRegion = aRegion;
   }
   return new Object[] {aRegion, new Boolean(regionCreated)};
}

/** Clear destroyed regions from the blackboard.
 *
 *  @param aRegion The region being destroyed. Save it and all its children. 
 *
 */
public void clearDestroyedRegions() {
   AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.DestroyedRegionsKey, new ArrayList());
   Log.getLogWriter().info("Cleared destroyed region list from blackboard");
}

/** Save destroyed region(s) in the blackboard. 
 *
 *  @param aRegion The region being destroyed. Save it and all its children. 
 *
 */
public void recordDestroyedRegion(Region aRegion) {
   // if concurrent then don't try to save destroyed regions
   if (!isSerialExecution) 
      return;
   ArrayList list = (ArrayList)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.DestroyedRegionsKey));
   if (list == null) { 
      list = new ArrayList();
   }
   list.add(aRegion.getFullPath());
   Set regionSet = aRegion.subregions(true);
   Iterator it = regionSet.iterator();
   while (it.hasNext()) {
      Region currRegion = (Region)it.next();
      list.add(currRegion.getFullPath());
   }
   AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.DestroyedRegionsKey, list);
   Log.getLogWriter().info("AsyncMsgBB.DestroyedRegionsKey = " + list);
}

/** Creates all regions previously destroyed that were recorded in the
 *  AsyncMsgBB shared map entry AsyncMsgBB.DestroyedRegionsKey.
 *  
 *  @param fill True if each newly created region should be populated with
 *              entries according to AsyncMsgPrms.maxKeys
 */
public void createDestroyedRegionsFromBB(boolean fill) {
   if (!isSerialExecution)
      throw new TestException("Do not call this from concurrent tests as DestroyedRegionsKey is not maintained");
   ArrayList list = (ArrayList)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.DestroyedRegionsKey));
   if (list == null) { 
      list = new ArrayList();
   }
   Log.getLogWriter().info("In createDestroyedRegionsFromBB with destroyed regions " + list);
   for (int i = 0; i < list.size(); i++) {
      String regionPath = (String)(list.get(i));
      createRegionWithPath(regionPath, fill);
   }
   Log.getLogWriter().info("Done in createDestroyedRegionsFromBB");
}

/** Creates all regions that currently don't exist.
 *  
 *  @param fill True if each newly created region should be populated with
 *              entries according to AsyncMsgPrms.maxKeys
 */
public void createAllDestroyedRegions(boolean fill) {
   Map aMap = getRegionDefMap();
   Iterator it = aMap.keySet().iterator();
   Log.getLogWriter().info("In createAllDestroyedRegions");
   while (it.hasNext()) {
      String key = (String)(it.next());
      Object value = aMap.get(key);
      if (value instanceof RegionDefinition) {
         createRegionWithPath(key, fill);
      }
   }
   Log.getLogWriter().info("Done in createAllDestroyedRegions");
}

/** Return true if the region or any of its subregions is mirrored,
 *  return false otherwise.
 *
 *  @param aRegion The region parent to test for mirroring.
 *
 */
public boolean isHierMirrored(Region aRegion) {
   boolean isMirrored = aRegion.getAttributes().getMirrorType().isMirrored();
   if (isMirrored)
      return true;
   Object[] regionArr = getSubregions(aRegion, true).toArray();
   for (int j = 0; j < regionArr.length; j++) {
      Region subR = (Region)(regionArr[j]);
      if (subR.getAttributes().getMirrorType().isMirrored())
         return true;
   }
   return false;
}
                                                                                
/** Return true if the region or any of its subregions is mirrored
 *  with keys/values, return false otherwise.
 *
 *  @param aRegion The region parent to test for mirroring keys/values.
 *
 */
public boolean isHierMirroredKV(Region aRegion) {
   boolean isMirrored = aRegion.getAttributes().getMirrorType().isKeysValues();
   if (isMirrored)
      return true;
   Object[] regionArr = getSubregions(aRegion, true).toArray();
   for (int j = 0; j < regionArr.length; j++) {
      Region subR = (Region)(regionArr[j]);
      if (subR.getAttributes().getMirrorType().isKeysValues())
         return true;
   }
   return false;
}
                                                                                
/** Return a Set of subregions of the given region, while handling
 *  regionDestroyedExceptions.
 *
 *  @param aRegion - The region to get subregions of.
 *  @param recursive - If true, return all subregions, otherwise
 *                     only aRegion's subregions.
 *
 *  @returns A set of all subregions. If aRegion has been
 *           destroyed, return an empty set.
 */
public Set getSubregions(Region aRegion, boolean recursive) {
   try {
      Set regionSet = aRegion.subregions(recursive);
      return regionSet;
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e; // not expected in serial tests
      // This is OK for a concurrent test as long as the exception
      // is thrown for aRegion and not one of its children
      String regionName = aRegion.getFullPath();
      String errorRegionName = e.getRegionFullPath();
      if (!regionName.equals(errorRegionName)) // exception caused by a child
         throw e; 
      return new HashSet();
   }
}

/** Get a map containing the RegionDefintions for this VM. Keys are
 *  the full path name for the region, values are the RegionDefinition.
 *
 *  @returns Map - The map containing the full path of the region as
 *                 the key, and its RegionDefinition as the value.
 */
public Map getRegionDefMap() {
   Map aMap = (Map)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.RegDefForPIDKey));
   if (aMap == null)
      return new HashMap();
   return aMap;
}

/** Given a full path for a region, and its RegionDefinition instance,
 *  write it to the AsyncMsgBB blackboard shared map.
 *
 *  The shared map has one key per VM in the form:
 *     AsyncMsgBB.RegDefForPIDKey_xxxxx, where xxxxs is the PID for the current VM.
 *     The value for this key is a Map, containing the fullPathName for the
 *        key, and the RegionDefinition for the value.
 *  The blackboard must record  the region definition for each VM (PID)
 *  because if any disk regions are used, disk dirs include the PID of the VM (so 
 *  another VM with the same region name will use different disk dirs).
 *  The PID of the disk dirs is specified in util.RegionDefinition.
 *
 *  @param fullPathOfRegion - the full path of the region to get the key for.
 *  @param regDef - The RegionDefinition instance for fullPathOfRegion.
 */
public void recordRegionDefInBB(String fullPathOfRegion, RegionDefinition regDef) {
   Map aMap = getRegionDefMap();
   aMap.put(fullPathOfRegion, regDef);
   AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.RegDefForPIDKey, aMap);
}

/** Given a full path for a region, return its RegionDefinition.
 *  The shared map has one key per VM in the form:
 *     AsyncMsgBB.RegDefForPIDKey_xxxxx, where xxxxs is the PID for the current VM.
 *     The value for this key is a Map, containing the fullPathName for the
 *        key, and the RegionDefinition for the value.
 *  The blackboard must record  the region definition for each VM (PID)
 *  because if any disk regions are used, disk dirs include the PID of the VM (so 
 *  another VM with the same region name will use different disk dirs).
 *  The PID of the disk dirs is specified in util.RegionDefinition.
 *
 *  @param fullPathOfRegion - the full path of the region to get the key for.
 */
public RegionDefinition getRegionDefFromBB(String fullPathOfRegion) {
   Map aMap = getRegionDefMap();
   RegionDefinition regDef = (RegionDefinition)aMap.get(fullPathOfRegion);
   return regDef;
}

/** HydraTask to for validation threads to verify that they have processed
 *  all messages in the OpList (saved in the BBoard by the putter thread
 *  Note that we must conflate the updates (or we'll try to verify intermediate
 *  state.
 */
public static void HydraTask_verifyRegionContents() {
   Log.getLogWriter().info("In HydraTask_verifyRegionContents");
   if (asyncTest == null) {
     asyncTest = new AsyncMsgTest();
     asyncTest.initialize();
   }
   asyncTest.verifyRegionContents();
}

public void verifyRegionContents() {
   // region & entry validators need to know that we expect to see the 
   // new values for each operation (e.g. since not part of a tx, we'll
   // always see the updated values.
   boolean isVisible = true;
   // If we aren't the first in the RR, simply return
   if (AsyncMsgBB.getBB().getSharedCounters().read(AsyncMsgBB.OPLIST_READY_FOR_VALIDATION) != 1) {
      return;
   }

   // Log ExecutionNumber used by RR leader
   long exeNum = AsyncMsgBB.getBB().getSharedCounters().read(AsyncMsgBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);

   // given the opList in the BB, attempt to validate the region in this VM
   OpList opList = (OpList)AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.OpListKey);
   Log.getLogWriter().info("validate opList retrieved with key = " + AsyncMsgBB.OpListKey + " OpList = " + opList);

   // verify that we got the same number of events as there were operations
   // unless conflation enabled, then allow fewer events ...
   if (verifyConflationBehavior) {
      int numOps = opList.numOps();
      int numEvents = AsyncMsgTest.numUpdatesThisRound;
      Boolean conflationEnabled = (Boolean)AsyncMsgBB.getBB().getSharedMap().get( AsyncMsgBB.conflationEnabled );
      if ((numOps != numEvents) && !conflationEnabled.booleanValue()) {
        throw new TestException("Expected " + opList.numOps() + " events, but actual number of events received by this VM = " + AsyncMsgTest.numUpdatesThisRound);
    } 
    Log.getLogWriter().info("conflationEnabled = " + conflationEnabled + " numOps = " + numOps + " numEvents = " + numEvents);
 
    // numEvents should always be <= numOps
    if (numOps < numEvents) {
       throw new TestException("Received " + numEvents + " events, but this is greater than the total number of operations performed, " + numOps);
     }

     // clear event count
     AsyncMsgTest.clearNumUpdatesThisRound();
   }

   opList = opList.collapse();
   Log.getLogWriter().info("collapsed opList = " + opList);
   
   for (int i=0; i < opList.numOps(); i++) {
     tx.Operation op = opList.getOperation(i);

     if (op.isEntryOperation()) {
       EntryValidator expectedValues = EntryValidator.getExpected(op, isVisible);
       EntryValidator actualValues = EntryValidator.getActual(op);
       expectedValues.compare(actualValues);
     } else if (op.isRegionOperation()) {

       // We can't validate regionCreates (as they aren't propagated) 
       if (!isRegionCreate(op)) {
         RegionValidator expectedValues = RegionValidator.getExpected(op, isVisible);
         RegionValidator actualValues = RegionValidator.getActual(op);
         expectedValues.compare(actualValues);
       }
     } else if (op.getOpName().equalsIgnoreCase(tx.Operation.CACHE_CLOSE)) {
       //validateCacheClosed(isVisible);
     } else { // unknown operation
       throw new TestException("Unknown operation " + op); 
     }
   }
   // re-create any destroyed regions (so we'll continue to get entry-create events/distribution
   if (isSerialExecution) {
     createDestroyedRegionsFromBB(true);
   }

}

private static boolean isRegionCreate(tx.Operation op) {
   boolean isRegionCreate = false;
   if (op.getOpName().equalsIgnoreCase(tx.Operation.REGION_CREATE)) {
     isRegionCreate = true;
   }
   return isRegionCreate;
}

  //--------------------------------------------------------------------------
  // Utility methods
  //--------------------------------------------------------------------------
  /** Prime the Q by putting large objects (for distribution)
   *  use unique keys (so they don't get conflated).
   */
  public Region primeQueue() {
    // clean up primeQ data from last round
    String dirKey = AsyncMsgBB.PrimeQDir + RemoteTestModule.getMyClientName();
    String primeQDir = (String)AsyncMsgBB.getBB().getSharedMap().get( dirKey );

    if (primeQDir != null) {

      String countKey = AsyncMsgBB.PrimeQCount + RemoteTestModule.getMyClientName();
      Integer primeQCount = (Integer)AsyncMsgBB.getBB().getSharedMap().get( countKey );

      Region aRegion = CacheUtil.getCache().getRegion( primeQDir );
      // The region may have been destroyed by the previous series of Operations
      if (aRegion != null) {
        for (int i = 0; i < primeQCount.intValue(); i++) {
          String key = (String)ObjectHelper.createName(i);
          try {
             aRegion.destroy(key);
          } catch (EntryNotFoundException e) {
             // This can happen if the region was destroyed & created in same doOperations loop, ignore
             break;
          } catch (CacheRuntimeException anException) {
             // This should never happen!
             throw new TestException(TestHelper.getStackTrace(anException));
          }
        }
      }
    }

    // Reclaim any Regions lost in last round
    if (isSerialExecution) {
      createDestroyedRegionsFromBB(true);
      clearDestroyedRegions();
    }

    // prime queue for next round of activity
    Region aRegion = getRandomRegion(true);
    if (aRegion == null) {
      throw new TestException("primeQueue: no regions available.  Possible test config issue");
    }
    
    String objectType = TestConfig.tab().stringAt(AsyncMsgPrms.objectType, "objects.ArrayOfByte");
                                                                                
    Log.getLogWriter().info("primeQueue: creating entries of type " + objectType);

    Object val = null;
    int i = 0;
    putSleepKey(aRegion, AsyncMsgPrms.getPrimeQueueSleepMs());    
    putSleepKey(aRegion, 0);    
    while (!messagesQueued()) {
      String key = (String)ObjectHelper.createName(i);
      val = ObjectHelper.createObject( objectType, i++ );
      try {
        Log.getLogWriter().fine("primeQ: putting key " + key + ", object " + val.toString() + " in region " + aRegion.getFullPath());
        aRegion.put(key, val);
        Log.getLogWriter().fine("primeQ: done putting key " + key + ", object " + val.toString() + " in region " + aRegion.getFullPath());
      } catch (RegionDestroyedException e) {
        // if concurrent, this is OK
        if (isSerialExecution)
          throw e;
      } catch (CacheRuntimeException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
    Log.getLogWriter().info("Put " + i + " objects of type " + objectType + " to initiate queuing");

    // Save the new dir & count to the BB, to be cleared next round
    dirKey = AsyncMsgBB.PrimeQDir + RemoteTestModule.getMyClientName();
    AsyncMsgBB.getBB().getSharedMap().put( dirKey, aRegion.getFullPath() );

    String countKey = AsyncMsgBB.PrimeQCount + RemoteTestModule.getMyClientName();
    AsyncMsgBB.getBB().getSharedMap().put( countKey, new Integer(i) );

    return aRegion;
  }

  protected DMStats getDMStats() {

     DMStats dmStats = null;
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     if (ds == null) {
       // The original asyncMsg tests rely on DistributedConnectionMgr: see if we can get a handle to the DistributedSystem there.
       ds = DistributedConnectionMgr.getConnection();
     }
       
     if (ds == null) {
       // It is possible that this VM was forcefully disconnected (by exceeding asyncMsgQueue size or configured timeout)
       Boolean slowReceiverDetected = (Boolean)AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.SlowReceiverDetectedKey);
       if (slowReceiverDetected.booleanValue()) {
          Log.getLogWriter().info("This VM cannot access the stats.  Probably forcefully disconnected from DS");
       } else {
          throw new TestException("DistributedSystem is null -- cannot access statistics");
       }
     } else {
       DM dm = ((InternalDistributedSystem)ds).getDistributionManager();
       dmStats = dm.getStats();
     }
     return dmStats;  
  }

  protected void verifyQueuedMessages() {
     DMStats dmStats = getDMStats();

     if (dmStats != null) {
        // asyncQueuedMessages
        long queuedMessages = dmStats.getAsyncQueuedMsgs();
        Log.getLogWriter().info("asyncQueuedMsgs = " + queuedMessages);
   
        if (queuedMessages <= 0) {
           throw new TestException("TuningRequired: AsyncQueuedMsgs is " + queuedMessages);
        }
     }
  }

  protected void verifyNoAsyncBufferWrites() {

     DMStats dmStats = getDMStats();

     if (dmStats != null) {
        // asyncBufferWrites
        long asyncWrites = dmStats.getAsyncSocketWrites();
        Log.getLogWriter().info("asyncSocketWrites = " + asyncWrites);
   
        if (asyncWrites > 0) {
           throw new TestException("Did not expect asyncSocketWrites, but found " + asyncWrites + " asyncSocketWrites");
        }
     }
  }

  // Various helpful stat checks 
  public boolean messagesQueued() {
    boolean queuedMessages = false;

    DMStats dmStats = getDMStats();

    if (dmStats != null) {
      // asyncQueuedMessages
      long queueSize = dmStats.getAsyncQueueSize();
      if (queueSize > 0) {
        Log.getLogWriter().info("asyncQueueSize = " + queueSize);
        queuedMessages = true;
      }
    }
    return queuedMessages;
  }

  public boolean queuedMessagesConflated() {
    boolean messagesConflated = false;
                                                                                
    DMStats dmStats = getDMStats();

    if (dmStats != null) {
      // asyncQueuedMessages
      long numConflatedMessages = dmStats.getAsyncConflatedMsgs();
      Log.getLogWriter().info("asyncConflatedMsgs = " + numConflatedMessages);
      if (numConflatedMessages > 0) {
        messagesConflated = true;
      }
    }
    return messagesConflated;
  }

  public boolean queueSizeExceeded() {
    boolean queueSizeExceeded = false;

    DMStats dmStats = getDMStats();

    if (dmStats != null) {
      // asyncQueuedMessages
      long sizeExceeded = dmStats.getAsyncQueueSizeExceeded();
      Log.getLogWriter().info("queueSizeExceeded = " + queueSizeExceeded);
      if (sizeExceeded > 0) {
        queueSizeExceeded = true;
      }
    }
    return queueSizeExceeded;
  }

  public boolean queueTimeoutExceeded() {
    boolean queueTimeouts = false;

    DMStats dmStats = getDMStats();

    if (dmStats != null) {
      // asyncQueuedMessages
      long timeouts = dmStats.getAsyncQueueTimeouts();
      Log.getLogWriter().info("asyncQueueTimeouts = " + timeouts);
      if (timeouts > 0) {
        queueTimeouts = true;
      }
    }
    return queueTimeouts;
  }

  // SleepListener special keys (for specific actions in listener)
  public void putSleepKey(Region aRegion, int sleepMs) {
    Object key = SLEEP_KEY;
    Integer val = new Integer(sleepMs);

    try {
      Log.getLogWriter().info("putSleepKey: putting key " + key + ", value " + val + " in region " + aRegion.getFullPath());
      aRegion.put(key, val);
      Log.getLogWriter().info("putSleepKey: done putting key " + key + ", value " + val + " in region " + aRegion.getFullPath());
    } catch (RegionDestroyedException e) {
      if (isSerialExecution)
        throw e;
    } catch (CacheRuntimeException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

// ======================================================================== 
// methods for concurrent tests

/** Producer in a non-mirrored VM.
 *  1) Do random operations.
 *  2) Signal a pause when random ops are done.
 *  3) Wait for consumers to report they are no longer receiving events.
 *  4) Write a snapshot of the region to the blackboard.
 *  5) Wait for consumer VMs to signal they have finished verification.
 *  After threads are done with verification, the task ends.
 */
protected void doProducer() {
   // wait for all threads to be ready to do this task, then do random ops
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.ReadyToBegin);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.ReadyToBegin", 
                                   AsyncMsgBB.ReadyToBegin, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   1000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }

   // Log this task is starting
   if (AsyncMsgBB.getBB().getSharedCounters().incrementAndRead(AsyncMsgBB.ConcurrentLeader) == 1) {
      logExecutionNumber();
      concurrentLeaderTid = RemoteTestModule.getCurrentThread().getThreadId();
   }
   Log.getLogWriter().info("In doConcAsyncMsg, concurrentLeaderTid is " + concurrentLeaderTid);

   Log.getLogWriter().info("Zeroing ShapshotWritten, NoMoreEvents");
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.SnapshotWritten);
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.NoMoreEvents);

   // do random operations 
   doControlledOperations();

   // wait for all threads to pause (meaning they are done doing operations)
   Log.getLogWriter().info("Zeroing FinishedVerify");
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.FinishedVerify);
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.Pausing);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.Pausing", 
                                   AsyncMsgBB.Pausing, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   5000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }
   Log.getLogWriter().info("Zeroing ReadyToBegin, ConcurrentLeader");
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.ReadyToBegin);
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.ConcurrentLeader);

   // wait for consumer VMs to process all their events
   waitForEventsByPut();

   // write a snapshot of the region to the blackboard
   writeSnapshot();   

   // wait for everybody to finish verify, then exit
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.FinishedVerify);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.FinishedVerify", 
                                   AsyncMsgBB.FinishedVerify, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   5000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }
   Log.getLogWriter().info("Zeroing concurrentLeaderTid, Pausing");
   concurrentLeaderTid = -1;
   AsyncMsgBB.getBB().getSharedCounters().zero(AsyncMsgBB.Pausing);

   long counter = AsyncMsgBB.getBB().getSharedCounters().read(AsyncMsgBB.ExecutionNumber);
   int numExecutionsToTerminate = TestConfig.tab().intAt(AsyncMsgPrms.numExecutionsToTerminate);
   Log.getLogWriter().info("Determining termination, execution number is " + counter + 
       " numExecutionsToTerminate is " + numExecutionsToTerminate);
   if (counter >= numExecutionsToTerminate) {
      Log.getLogWriter().info("Getting ready to throw StopSchedulingOrder...");
      throw new StopSchedulingOrder("Num executions is " + counter);
   }
   if (AdminListener.slowReceiverAlertOccurred()) {
      throw new StopSchedulingOrder("a slow receiver alert was recognized");
   }
}

/** Consumer in a mirrored VM.
 *  1) Wait for the producer VM to finish its ops and pause.
 *  2) Wait for all events to occur in this VM.
 *  3) Wait for the producer VM to write the region snapshot to the BB.
 *  4) Verify the current state of the mirrored VM against the BB.
 *  After all threads are done with verification, the task ends.
 */
protected void doConsumer() {
   // wait for all threads to be ready to do this task, then do random ops
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.ReadyToBegin);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.ReadyToBegin", 
                                   AsyncMsgBB.ReadyToBegin, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   1000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }

   // wait for all threads to pause (meaning the producers are done doing operations)
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.Pausing);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.Pausing", 
                                   AsyncMsgBB.Pausing, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   5000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }

   // now that the producer has paused, don't slow down, just let all events come in
   BurstListener.enableBurstBehavior(false); 

   // wait for the producer VM to show that it has written a snapshot to the blackboard
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.SnapshotWritten", 
                                   AsyncMsgBB.SnapshotWritten, 
                                   1, 
                                   true, 
                                   10000,
                                   2000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }

   // reset for the next round
   BurstListener.enableBurstBehavior(true); 

   // verify
   verifyFromSnapshot();

   // wait for everybody to finish verify, then exit
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.FinishedVerify);
   while (true) {
      try {
         TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                                   "AsyncMsgBB.FinishedVerify", 
                                   AsyncMsgBB.FinishedVerify, 
                                   numPeerThreadsInTest, 
                                   true, 
                                   10000,
                                   5000);
         break;
      } catch (TestException e) { // we waited but did not recognize the desired counter value
         // check for a slow receiver alert
         if (AdminListener.slowReceiverAlertOccurred()) {
            throw new StopSchedulingOrder("a slow receiver alert was recognized");
         }
      }
   }

   long counter = AsyncMsgBB.getBB().getSharedCounters().read(AsyncMsgBB.ExecutionNumber);
   int numExecutionsToTerminate = TestConfig.tab().intAt(AsyncMsgPrms.numExecutionsToTerminate);
   Log.getLogWriter().info("Determining termination, execution number is " + counter + 
       " numExecutionsToTerminate is " + numExecutionsToTerminate);
   if (counter >= numExecutionsToTerminate) {
      Log.getLogWriter().info("Getting ready to throw StopSchedulingOrder...");
      throw new StopSchedulingOrder("Num executions is " + counter);
   }
   if (AdminListener.slowReceiverAlertOccurred()) {
      throw new StopSchedulingOrder("a slow receiver alert was recognized");
   }
}

/** Wait for all threads to report they have not received an event for
 *  NO_EVENTS_MILLIS milliseconds. This returns when the wait is complete.
 */
protected void waitForEventsByTime() {
   // the producer has stopped, no need to be slow now as we are just waiting
   // for all the events to come in that have been queued; we might as well do
   // it fast.
   BurstListener.enableBurstBehavior(false); 
   long startTime = System.currentTimeMillis();
   final int NO_EVENTS_MILLIS = 20000;
   // wait until all threads report they have not received an event for NO_EVENTS_MILLIS
   long currentTime = 0;
   long lastInvocation = 0;
   do {
      currentTime = System.currentTimeMillis();
      lastInvocation = BurstListener.getLastInvocationTime();
      Log.getLogWriter().info("Waiting for " + NO_EVENTS_MILLIS + " ms to elapse since last BurstListener " +
          "invocation, last BurstListener occurred at " + lastInvocation +
          ", current time is " + currentTime + ", elapsed is " + (currentTime - lastInvocation));
      try {
         Thread.sleep(1000);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } while (currentTime - lastInvocation < NO_EVENTS_MILLIS);
   long endTime = System.currentTimeMillis();
   Log.getLogWriter().info("It took this thread " + (endTime - startTime) + " ms to observe " + NO_EVENTS_MILLIS + " ms without receiving an event");
   AsyncMsgBB.getBB().getSharedCounters().increment(AsyncMsgBB.NoMoreEvents);
   TestHelper.waitForCounter(AsyncMsgBB.getBB(), 
                             "AsyncMsgBB.NoMoreEvents", 
                             AsyncMsgBB.NoMoreEvents, 
                             numPeerThreadsInTest, 
                             true, 
                             -1,
                             5000);
   endTime = System.currentTimeMillis();
   Log.getLogWriter().info("It took all threads in this test " + (endTime - startTime) + " ms to observe " + NO_EVENTS_MILLIS + " ms without receiving an event");
   BurstListener.enableBurstBehavior(true);  // set up to become slow/fast again
}

/** Wait for all other vms to process their events by doing a put
 *  into an ack region. When the put returns, all VMs have processed
 *  their events.
 */
public static void waitForEventsByPut() {
   Region aRegion = CacheUtil.getCache().getRegion(DIST_ACK_REGION);
   Log.getLogWriter().info("Waiting for events by putting into " + aRegion.getFullPath());
   String key = "ReadyForValidation";
   long startTime = System.currentTimeMillis();
   Log.getLogWriter().info("Putting " + key + " with value " + startTime + "  into " + aRegion.getFullPath());
   aRegion.put(key, new Long(startTime));
   long endTime = System.currentTimeMillis();
   Log.getLogWriter().info("Done putting " + key + " with value " + startTime + " into " + aRegion.getFullPath() + ", put took " + (endTime - startTime) + " millis");
   // we're guaranteed that all messages were delivered once the above
   // operation completes
}

/** Do random operations by honoring the lower and upper threshold settings.
 *  This controls the size of the region.
 */
protected void doControlledOperations() {
   Vector operations = TestConfig.tab().vecAt(AsyncMsgPrms.operations);
   Vector lowerThresholdOperations = TestConfig.tab().vecAt(AsyncMsgPrms.lowerThresholdOperations);
   Vector upperThresholdOperations = TestConfig.tab().vecAt(AsyncMsgPrms.upperThresholdOperations);
   int lowerThreshold = TestConfig.tab().intAt(AsyncMsgPrms.lowerThreshold);
   int upperThreshold = TestConfig.tab().intAt(AsyncMsgPrms.upperThreshold);
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("Doing controlled operations for " + minTaskGranularitySec + " seconds");
   Region aRegion = CacheUtil.getRegion(REGION_NAME);
   if (aRegion == null) {
      throw new TestException("Region " + REGION_NAME + " is null, probably due to a forcible disconnect because of a slow receiver");
   }
   long startTime = System.currentTimeMillis();
   do {
      if (aRegion.keys().size() <= lowerThreshold) 
         doOperations(lowerThresholdOperations, 1);
      else if (aRegion.keys().size() >= upperThreshold)
         doOperations(upperThresholdOperations, 1);
      else
         doOperations(operations, 1);
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   Log.getLogWriter().info("Done doing controlled operations for " + minTaskGranularitySec + " seconds");
}

// ======================================================================== 
// methods to do verification

/** One thread in the producer VM writes the current state of the region
 *  to the blackboard. 
 */
protected void writeSnapshot() {
   Region aRegion = CacheUtil.getRegion(REGION_NAME);
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   Log.getLogWriter().info("In writeSnapshot, with myTid " + myTid + ", concurrentLeaderTid is " + concurrentLeaderTid);
   if (myTid == concurrentLeaderTid) { 
      // this is the first thread to verify; all other threads will wait for this thread to
      // write its view of the region to the blackboard and they will read it and match it
      Map regionSnapshot = new HashMap();
      Log.getLogWriter().info("This thread is the concurrentLeader, creating region snapshot..."); 
      Set keySet = aRegion.keys();
      Iterator it = keySet.iterator();
      while (it.hasNext()) {
          Object key = it.next();
          Object value = null;
          if (aRegion.containsValueForKey(key)) {
             try {
                 value = aRegion.get(key);
             } catch (TimeoutException e) {
                 throw new TestException(TestHelper.getStackTrace(e));
             } catch (CacheLoaderException e) {
                 throw new TestException(TestHelper.getStackTrace(e));
             }
          }
          if (value instanceof BaseValueHolder)
             regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
          else
             regionSnapshot.put(key, value);
      }
      Log.getLogWriter().info("Done creating region snapshot with " + regionSnapshot.size() + " entries");
      AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.RegionSnapshot, regionSnapshot);
      long snapshotWritten = AsyncMsgBB.getBB().getSharedCounters().incrementAndRead(AsyncMsgBB.SnapshotWritten);
      Log.getLogWriter().info("Incremented SnapshotWritten, now is " + snapshotWritten);
   } 
}

/** Verify this thread's view of the keys and values in the region
 *  by reading the region snapshot and destroyed keys from the blackboad.
 */
protected void verifyFromSnapshot() {
   StringBuffer aStr = new StringBuffer();
   Map regionSnapshot = (Map)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.RegionSnapshot));
   int snapshotSize = regionSnapshot.size();
   Region aRegion = CacheUtil.getRegion(REGION_NAME);
   int regionSize = aRegion.keys().size();
   if (snapshotSize != regionSize) {
      aStr.append("Expected region " + aRegion.getFullPath() + " to be size " + snapshotSize + 
           ", but it is " + regionSize + "\n");
      if (snapshotSize < regionSize) { // our region has extra elements, find out what they are
         Set regionKeySet = new HashSet(aRegion.keys());
         Set snapShotKeySet = regionSnapshot.keySet();
         regionKeySet.removeAll(snapShotKeySet);
         Iterator it = regionKeySet.iterator();
         while (it.hasNext()) {
            Object key = it.next();
            Region.Entry entry = aRegion.getEntry(key);
            if (entry != null) {
               Object value = entry.getValue();
               aStr.append("   region contains key " + key + ", value " + TestHelper.toString(value) +
                           ", but this is is not in the region snapshot (region snapshot.containsKey() is " +
                           regionSnapshot.containsKey(key) + ")\n");
            } else {
               aStr.append("   Error in test; region should contain key " + key + ", but it could not be obtained");
            }
         }
      }
   }
   Log.getLogWriter().info("Verifying from snapshot containing " + snapshotSize + " entries...");
   Iterator it = regionSnapshot.entrySet().iterator();
   while (it.hasNext()) {
      Map.Entry entry = (Map.Entry)it.next();
      Object key = entry.getKey();
      Object expectedValue = entry.getValue();

      // containsKey
      boolean containsKey = aRegion.containsKey(key);
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
         aStr.append(e.getMessage() + "\n");
      }

      // test the value in the region
      if (containsKey) {
         Region.Entry regionEntry = aRegion.getEntry(key);
         Object actualValue = regionEntry.getValue();
         try {
            verifyMyValue(aRegion, key, expectedValue, actualValue, EQUAL);
         } catch (TestException e) {
            aStr.append(e.getMessage() + "\n");
         }
      }
   }

   if (aStr.length() > 0)
      throw new TestException(aStr.toString());
   Log.getLogWriter().info("Done verifying from snapshot containing " + snapshotSize + " entries...");
}

/** Verify that the given object is an instance of ValueHolder
 *  with expectedValue as the myValue field.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expectedValue The expected myValue field of a ValueHolder in aRegion, or null
 *        if the expected value should be null.
 * @param valuetoCheck The value that is expected to be expectedValue.
 * @param compareStrategy Whether the compare is equals or equivalent (for ValueHolders)
 *
 * @throws TestException if the result of a get on key does not have the expected value.
 */
protected static void verifyMyValue(Region aRegion, Object key, Object expectedValue, Object valueToCheck, int compareStrategy) {
   if (valueToCheck == null) {
      if (expectedValue != null) {
         throw new TestException("For key " + key + ", expected myValue to be " + 
                   TestHelper.toString(expectedValue) + 
                   ", but it is " + TestHelper.toString(valueToCheck));
      }
   } else if (valueToCheck instanceof BaseValueHolder) {
      BaseValueHolder actualVH = (BaseValueHolder)valueToCheck;
      if (compareStrategy == EQUAL) {
         if (!actualVH.myValue.equals(expectedValue)) {
            throw new TestException("For key " + key + ", expected ValueHolder.myValue to be " + 
                   TestHelper.toString(expectedValue) + 
                   ", but it is " + TestHelper.toString(valueToCheck));
         }
      } else if (compareStrategy == EQUIVALENT) { 
         if (actualVH.myValue.toString().equals(expectedValue.toString())) {
            throw new TestException("For key " + key + ", expected ValueHolder.myValue to be " + 
                      expectedValue + ", but it is " + actualVH.myValue);
         }
      }
   } else {
      throw new TestException("Expected value for key " + key + " to be an instance of ValueHolder, but it is " +
         TestHelper.toString(valueToCheck));
   }
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

/** increments numUpdatesThisRound
 */
public static int incNumUpdatesThisRound() {
  return ++AsyncMsgTest.numUpdatesThisRound;
}

/** clears counter for numUpdatesThisRound
 */
public static void clearNumUpdatesThisRound() {
  AsyncMsgTest.numUpdatesThisRound = 0;
}

/** Check for the possibility we were disconnected because of a slow receiver
 */
public static void handlePossibleSlowReceiverException(Exception e) {
   if ((e instanceof CancelException) ||
       (e instanceof DistributedSystemDisconnectedException)) {
      Throwable causedBy = e.getCause();
      if (causedBy instanceof ForcedDisconnectException) {
         AdminListener.waitForSlowReceiverAlert(60);
         AsyncMsgBB.checkForError();
         AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.SlowReceiverDetectedKey, new Boolean(true));
         throw new StopSchedulingOrder("Stopping tasks; slow receiver detected");
      }
   }
   throw new TestException(TestHelper.getStackTrace(e));
}

}
