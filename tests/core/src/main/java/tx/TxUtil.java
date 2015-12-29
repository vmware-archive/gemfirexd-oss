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
package tx; 

import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.Token;
import java.util.*;

import parReg.ParRegPrms;
import parReg.ParRegUtil;

/**
 * A class to contain methods useful for all transactions tests.
 */
public class TxUtil {

// the only instance of TxUtil
static public TxUtil txUtilInstance = null;

// instance fields
public RandomValues randomValues = null; // for random object creation
protected int maxKeys;            // initial object count (per region)
public int modValInitializer=0;          // track ValueHolder.modVal
protected boolean isSerialExecution; 
protected boolean isBridgeClient;
protected HydraThreadLocal txState = new HydraThreadLocal();
public boolean suspendResume = false;

// static fields
public static final int NO_REPLICATION_RESTRICTION = 0;
public static final int NO_REPLICATION = 1;

// String prefixes for event callback object
protected static final String createCallbackPrefix = "Create event originated in pid ";
protected static final String updateCallbackPrefix = "Update event originated in pid ";
protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";    
protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";

protected static final String memberIdString = " memberId=";

//=========================================================================
// start tasks 

/** Hydra start task to initialize dataPolicies to use in ViewTests
 *  This allows specification of the txThread (client1) as Proxy or Cached
 *  and the same for the validator (view) threads.
 */
public synchronized static void StartTask_initialize() {
   // write dataPolicy attributes to the blackboard 
   // the '-' separate list of dataPolicies are assigned to 
   // TxBB.DataPolicyPrefix_client<n> where n is the token in the viewDataPolicies
   String viewDataPolicies = TestConfig.tab().stringAt(TxPrms.viewDataPolicies, "none");
   if (viewDataPolicies.equals("none")) {
     return;
   }

   StringTokenizer tokenizer = new StringTokenizer(viewDataPolicies, "-");
   for (int i=1; tokenizer.hasMoreTokens(); i++) {
      String dataPolicy = tokenizer.nextToken();
      TxBB.getBB().getSharedMap().put(TxBB.DataPolicyPrefix + "client" + i, dataPolicy);
   }
   TxBB.getBB().printSharedMap();
}

// ======================================================================== 
// initialization
public synchronized void initialize() {
   randomValues = new RandomValues();   
   isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
}

// ======================================================================== 
// hydra tasks
/**
 * Stops the bridge server in this VM.
 */
public static void HydraTask_startBridgeServer() {
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
}

/**
 * Stops the bridge server in this VM.
 */
public static void HydraTask_stopBridgeServer() {
   BridgeHelper.stopBridgeServer();
   CacheHelper.closeCache();
}

/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_createRegionForest() {
   Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
   CacheUtil.setCache(c);   // required by splitBrain/distIntegrityFD.conf
   if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ))
      c.setCopyOnRead(true);
   if (txUtilInstance == null) {
      txUtilInstance = new TxUtil();
      txUtilInstance.initialize();
      txUtilInstance.createRegionHierarchy();
   }
   txUtilInstance.summarizeRegionHier();
}

/**
 *  Create the cache and Region (non-colocated PartitionedRegions)
 */
public synchronized static void HydraTask_createPartitionedRegions() {
  if (txUtilInstance == null) {
    txUtilInstance = new TxUtil();
    try {
      txUtilInstance.createPartitionedRegions();
    } catch (Exception e) {
      Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
      throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
    }
  }
}

/*
 * Creates cache and regions (CacheHelper/RegionHelper)
 */
public void createPartitionedRegions() {
  // create cache/region (and connect to the DS)
  if (CacheHelper.getCache() == null) {
     Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
     CacheUtil.setCache(c);     // TxUtil and splitBrain/distIntegrityFD require this
     c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
     ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());
     if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ)) {
        c.setCopyOnRead(true);
     }

     String regionConfig = ConfigPrms.getRegionConfig();
     AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
     RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
     String regionBase = rd.getRegionName();

     int numRegions = TestConfig.tab().intAt(TxPrms.numRootRegions);
     for (int i = 0; i < numRegions; i++) {
        String regionName = regionBase + "_" + (i+1);
        Region aRegion = RegionHelper.createRegion(regionName, aFactory);

        // edge clients register interest in ALL_KEYS
        if (aRegion.getAttributes().getPoolName() != null) {
           aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
           isBridgeClient = true;
           Log.getLogWriter().info("registered interest in ALL_KEYS for " + regionName);
        }

        recordRegionConfigInBB(aRegion.getFullPath(), regionConfig);

     }
   }
}

/*
 *  Creates initial set of entries across colocated regions
 *  (non-transactional).
 */
public static void HydraTask_populateRegions() {
   txUtilInstance.initialize();
   txUtilInstance.populateRegions();
}
 
public void populateRegions() {
   // prime the PartitionRegion by getting at least one entry in each bucket
   // make sure that numBuckets = TxPrms.maxKeys!
   Set regions = CacheUtil.getCache().rootRegions();
   for (Iterator it = regions.iterator(); it.hasNext(); ) {
     Region aRegion = (Region)it.next();
     createEntries(aRegion);
   }
}

public static void HydraTask_doOperations() {
   OpList opList = doOperations();
   TxBB.putOpList(opList);
   Log.getLogWriter().info("Read opList: " + TxBB.getOpList());
}

// ======================================================================== 
// methods for task execution and round robin counters

/** Log the number of tasks that have been executed in this test.
 *  Useful for debugging/analyzing serial execution tests.
 */
public static void logExecutionNumber() {
   long exeNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}

/** Log the "round" number for this test. This is called for serial execution
 *  tests that use round-robin scheduling. Each task can call this method and
 *  it will remember the first thread in the round, then increment and log a
 *  counter each time a new round begins.
 *
 *  @returns True if the current thread is the first one in a round, false
 *           otherwise.
 */
public static boolean logRoundRobinNumber() {
   Blackboard BB = TxBB.getBB();
   String rrStartThread = (String)(BB.getSharedMap().get(TxBB.RoundRobinStartThread));
   String currentThreadName = Thread.currentThread().getName();
   if (rrStartThread == null) { // first task execution; save who is the first in the round
      rrStartThread = currentThreadName;
      BB.getSharedMap().put(TxBB.RoundRobinStartThread, rrStartThread);
   }
   if (currentThreadName.equals(rrStartThread)) { // starting a new round
      long rrNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.RoundRobinNumber);
      Log.getLogWriter().info("Beginning round " + rrNum);
      return true;
   }
   return false;
}

/** Return the current roundRobin number.
 */
public static long getRoundRobinNumber() {
   return TxBB.getBB().getSharedCounters().read(TxBB.RoundRobinNumber);
}

// ======================================================================== 
// methods to create region hierarchy

/** 
 *  Create a forest of regions based on numRootRegions,
 *  numSubRegions, regionDepth
 */
public void createRegionHierarchy() {

   int numRoots = TestConfig.tab().intAt(TxPrms.numRootRegions);
   int breadth = TestConfig.tab().intAt(TxPrms.numSubRegions);
   int depth = TestConfig.tab().intAt(TxPrms.regionDepth);

   Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
   CacheUtil.setCache(c);     // required by splitBrain/distIntegrityFD
   c.getCacheTransactionManager().setListener(TxPrms.getTxListener());
   ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(TxPrms.getTxWriter());

//   Vector roots = new Vector();
   Region r = null;
   for (int i=0; i<numRoots; i++) {
      String rootName = "root" + (i+1);
      String regionConfig = ConfigPrms.getRegionConfig();
      AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);

      // View tests & proxySerialConflict tests require overwriting the 
      // dataPolicy to allow specific combinations betweeen clients
      String clientName = System.getProperty( "clientName" );
      String mapKey = TxBB.DataPolicyPrefix + clientName;
      String dataPolicy = (String)(TxBB.getBB().getSharedMap().get(mapKey));
      if (dataPolicy != null) {
        aFactory.setDataPolicy(TestHelper.getDataPolicy(dataPolicy));
      }

      Region rootRegion = RegionHelper.createRegion(rootName, aFactory);
      Log.getLogWriter().info("Created root region " + rootName);

      // edge clients register interest in ALL_KEYS
      if (rootRegion.getAttributes().getPoolName() != null) {
         rootRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
         isBridgeClient = true;
         Log.getLogWriter().info("registered interest in ALL_KEYS for " + rootName);
      }

      recordRegionConfigInBB(rootRegion.getFullPath(), regionConfig);
      createEntries( rootRegion );
      createSubRegions(rootRegion, breadth, depth, "Region");
   }
}

/** 
 *  Create the subregion hierarchy of a root. 
 */
private void createSubRegions( Region r, int numChildren, int levelsLeft, String parentName) {
   String currentName;
   for (int i=1; i<=numChildren; i++) {
      currentName = parentName + "-" + i;
      String regionConfig = ConfigPrms.getRegionConfig();
      AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);

      // View tests & proxySerialConflict tests require overwriting the
      // dataPolicy to allow specific combinations betweeen clients
      String clientName = System.getProperty( "clientName" );
      String mapKey = TxBB.DataPolicyPrefix + clientName;
      String dataPolicy = (String)(TxBB.getBB().getSharedMap().get(mapKey));
      if (dataPolicy != null) {
        aFactory.setDataPolicy(TestHelper.getDataPolicy(dataPolicy));
      }
 
      Region child = null;
      try {
         RegionAttributes regionAttrs = RegionHelper.getRegionAttributes(aFactory);
         if (regionAttrs.getDataPolicy().withPersistence() ||
             regionAttrs.getEvictionAttributes().getAction().isOverflowToDisk()) {
           DiskStoreHelper.createDiskStore(regionAttrs.getDiskStoreName());
         }
         child = r.createSubregion(currentName, regionAttrs);
         Log.getLogWriter().info("Created subregion " + TestHelper.regionToString(child, true));
         // edge clients register interest in ALL_KEYS
         if (child.getAttributes().getPoolName() != null) {
            child.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
            Log.getLogWriter().info("registered interest in ALL_KEYS for " + currentName);
         }

         recordRegionConfigInBB(child.getFullPath(), regionConfig);
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
   *  Create TxPrms.maxKeys entries in the given region
   *  Initializes ValueHolder.modVal sequentially 1-n 
   *  across all regions
   */
  public void createEntries(Region aRegion) {
    this.maxKeys = TestConfig.tab().intAt(TxPrms.maxKeys, 10);
    long startKey = 0;
    for (int i=0; i < maxKeys; i++) {
      String key = NameFactory.getObjectNameForCounter(startKey + i);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, startKey + i);
      Object val = new ValueHolder( key, randomValues, new Integer(modValInitializer));
      modValInitializer++;
      String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      try {
        aRegion.create( key, val, callback );
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
      } 
//      catch (CacheException ce) {
//        throw new TestException("Cannot create key " + key + " CacheException(" + ce + ")");
//      }
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
     Region aRegion = null;
     String regionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName, null);
     if (regionName != null) {
        aRegion = CacheUtil.getCache().getRegion(regionName);
     }
     return getRandomRegion(allowRootRegion, aRegion, NO_REPLICATION_RESTRICTION);
  }

  /** Return a currently existing random region that is not replicated, nor
   *  are any of its subregions. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegionNoReplication(boolean allowRootRegion) {
     Region aRegion = null;
     String regionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName, null);
     if (regionName != null) {
        aRegion = CacheUtil.getCache().getRegion(regionName);
     }
     return getRandomRegion(allowRootRegion, null, NO_REPLICATION);
  }

  /** Return a currently existing random region. 
   *
   *  @param allowRootRegion true if this call can return the root region, false otherwise.
   *  @param excludeRegion Return a region other than this region, or null if none to exclude.
   *  @param restriction Restriction on what region can be returned, for example
   *            NO_REPLICATION_RESTRICTION, NO_REPLICATION
   *
   *  @return A random region, or null of none available. 
   */
  public Region getRandomRegion(boolean allowRootRegion, Region excludeRegion, int restriction) {
     // Get the set of all regions available
     Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
     if (rootRegionArr.length == 0)
        return null;
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
     if (regionList.size() == 0)
        return null;
     int randInt = TestConfig.tab().getRandGen().nextInt(0, regionList.size() - 1);
     Region aRegion = (Region)regionList.get(randInt);
     if ((restriction != NO_REPLICATION_RESTRICTION) || (excludeRegion != null)) { // we have a restriction
        int startIndex = randInt;
        boolean done = true;
        do {
           done = true;
           try {
              if (restriction == NO_REPLICATION)
                 done = !(isHierReplicated(aRegion));
              if ((excludeRegion != null) && (aRegion.getFullPath().equals(excludeRegion.getFullPath())))
                 done = false;
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
         Set keys = sRegion.keySet();
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
                  root.keySet().size() + " keys\n");
      TreeSet aSet = new TreeSet(new RegionComparator());
      aSet.addAll(getSubregions(root, true));
      for (Iterator subit = aSet.iterator(); subit.hasNext(); ) {
         Region aRegion = (Region)subit.next();
         String regionName = aRegion.getFullPath();
         try {
            Set keySet = new TreeSet(aRegion.keySet());
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
// methods to do random operations

/** Do random operations on random regions using the hydra parameter 
 *  TxPrms.operations as the list of available operations and 
 *  TxPrms.numOps as the number of operations to do.
 */
public static OpList doOperations() {
   boolean allowGetOperations = true;
   return doOperations(allowGetOperations);
}

/** Do random operations on random regions using the hydra parameter 
 *  TxPrms.operations as the list of available operations and 
 *  TxPrms.numOps as the number of operations to do.
 *  
 *  @param allowGetOperations - to prevent proxy clients from attempting
 *                              gets (since they have no storage this always
 *                              invokes the loader and oldValue is always
 *                              reported as null).
 */
public static OpList doOperations(boolean allowGetOperations) {
   Vector operations = TestConfig.tab().vecAt(TxPrms.operations);
   int numOps = TestConfig.tab().intAt(TxPrms.numOps);
   OpList opList = null;
   if (txUtilInstance != null) {    // needed for HA tests (txUtilInstance may not be initialized yet
      opList = txUtilInstance.doOperations(operations, numOps, allowGetOperations); 
   }
   return opList;
}

/** Do random operations on random regions.
 *  
 *  @param operations - a Vector of operations to choose from.
 *  @param numOperationsToDo - the number of operations to execute.
 *
 *  @returns An instance of OpList, which is a list of operations that
 *           were executed.
 */
public OpList doOperations(Vector operations, int numOperationsToDo) {
   boolean allowGetOperations = true;
   return doOperations(operations, numOperationsToDo, allowGetOperations);
}

/** Do random operations on random regions.
 *  
 *  @param operations - a Vector of operations to choose from.
 *  @param numOperationsToDo - the number of operations to execute.
 *  @param allowGetOperations - if false, replace entry-get ops with entry-create
 *
 *  @returns An instance of OpList, which is a list of operations that
 *           were executed.
 */
public OpList doOperations(Vector operations, int numOperationsToDo, boolean allowGetOperations) {
   Log.getLogWriter().info("Executing " + numOperationsToDo + " random operations...");
   final long TIME_LIMIT_MS = 60000;  
      // limit on how long this method will try to do UNSUCCESSFUL operations; 
      // for example, if the test is configured such that it destroys
      // entries but does not create any, eventually no entries will be left
      // to destroy and this method will be unable to fulfill its mission
      // to execute numOperationsToDo operations; This is like a consistency
      // check for test configuration problems
   long timeOfLastOp = System.currentTimeMillis();
   OpList opList = new OpList();
   int numOpsCompleted = 0;
   while (numOpsCompleted < numOperationsToDo) {
      Operation op = null;

      // choose a random operation, forcing a region create if there are no regions
      String operation = (String)(operations.get(TestConfig.tab().getRandGen().nextInt(0, operations.size()-1)));
      if (!operation.equals(Operation.CACHE_CLOSE) && !operation.equals(Operation.REGION_CREATE)) {
         // operation requires a choosing a random region
         Object[] rootRegionArr = CacheUtil.getCache().rootRegions().toArray();
         if (rootRegionArr.length == 0) { // no regions available; force a create region op
            if (operations.indexOf(Operation.REGION_CREATE) < 0) // create not specified
               throw new TestException("No regions are available and no create region operation is specified");
            Log.getLogWriter().info("In doOperations, forcing region create because no regions are present");
            operation = Operation.REGION_CREATE;
         }
      }

      // override get operations with create (for proxy view clients)
      if (operation.startsWith("entry-get") && !allowGetOperations) {
         operation = Operation.ENTRY_CREATE;
      }

      Log.getLogWriter().info("Operation is " + operation);
  
      if (operation.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
         op = createEntry(getRandomRegion(true), false);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {
         Region aRegion = getRandomRegion(true);
         op = updateEntry(aRegion, getRandomKey(aRegion));
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_DESTROY)) {
         Region aRegion = getRandomRegion(true);
         op = destroyEntry(false, aRegion, getRandomKey(aRegion), false);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY)) {
         Region aRegion = getRandomRegionNoReplication(true);
         if (aRegion != null)
            op = destroyEntry(true, aRegion, getRandomKey(aRegion), false);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_INVAL)) {
         Region aRegion = getRandomRegion(true);
         op = invalEntry(false, aRegion, getRandomKey(aRegion), false);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL)) {
         Region aRegion = getRandomRegionNoReplication(true);
         if (aRegion != null)
            op = invalEntry(true, aRegion, getRandomKey(aRegion), false);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_NEW_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithNewKey(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getWithExistingKey(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithPreviousKey(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.REGION_CREATE)) {
         op = createRegion();
      } else if (operation.equalsIgnoreCase(Operation.REGION_DESTROY)) {
         op = destroyRegion(false, getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(Operation.REGION_LOCAL_DESTROY)) {
         Region aRegion = getRandomRegionNoReplication(true);
         if (aRegion != null)
            op = destroyRegion(true, aRegion);
      } else if (operation.equalsIgnoreCase(Operation.REGION_INVAL)) {
         Region aRegion = getRandomRegion(true);
         op = invalRegion(false, getRandomRegion(true));
      } else if (operation.equalsIgnoreCase(Operation.REGION_LOCAL_INVAL)) {
         Region aRegion = getRandomRegionNoReplication(true);
         if (aRegion != null)
            op = invalRegion(true, aRegion);
      } else if (operation.equalsIgnoreCase(Operation.CACHE_CLOSE)) {
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
   Log.getLogWriter().info("Completed execution of " + opList);
   return opList;
}

/** Do random repeatable read operations on random regions.
 *  See bugmail for 36688 for a list of supported repeatable read operations.
 *
 *  @param numOperationsToDo The number of repeatable read operations to do.
 *
 */
public OpList doRepeatableReadOperations(int numOperationsToDo) {
   Log.getLogWriter().info("Executing " + numOperationsToDo + " random repeatable read operations...");

   // edgeClient tx doesn't support localInvalidate, localDestroy ...
   List repeatableReadOps = isBridgeClient ? Operation.clientRepeatableReadOps : Operation.repeatableReadOps;

   OpList opList = new OpList();
   int numOpsCompleted = 0;
   GsRandom rand = TestConfig.tab().getRandGen();
   while (numOpsCompleted < numOperationsToDo) {
      Operation op = null;
      int nextRepeatableRead = (int)(TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.nextRepeatableRead));
      if (nextRepeatableRead >= repeatableReadOps.size())
         nextRepeatableRead = 0; 
      String operation = (String)(repeatableReadOps.get(nextRepeatableRead));
      Log.getLogWriter().info("Repeatable read operation is " + operation);

      if (operation.equalsIgnoreCase(Operation.ENTRY_GET_EXIST_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getWithExistingKey(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_GET_ENTRY_EXIST_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = getEntryWithExistingKey(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
         Region aRegion = getRandomRegion(true);
         op = createEntry(aRegion, true);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_CONTAINS_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = containsKeyExisting(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_CONTAINS_VALUE_FOR_KEY)) {
         Region aRegion = getRandomRegion(true);
         op = containsValueForKeyExisting(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_CONTAINS_VALUE)) {
         Region aRegion = getRandomRegion(true);
         op = containsValueExisting(aRegion);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_INVAL)) {
         Region aRegion = getRandomRegion(true);
         Object key = NameFactory.getNextPositiveObjectName(); // force EntryNotFoundException
         op = invalEntry(false, aRegion, key, true);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL)) {
         Region aRegion = getRandomRegion(true);
         Object key = NameFactory.getNextPositiveObjectName(); // force EntryNotFoundException
         op = invalEntry(true, aRegion, key, true);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_DESTROY)) {
         Region aRegion = getRandomRegion(true);
         Object key = NameFactory.getNextPositiveObjectName(); // force EntryNotFoundException
         op = destroyEntry(false, aRegion, key, true);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY)) {
         Region aRegion = getRandomRegion(true);
         Object key = NameFactory.getNextPositiveObjectName(); // force EntryNotFoundException
         op = destroyEntry(true, aRegion, key, true);
      } else if (operation.equalsIgnoreCase(Operation.ENTRY_REMOVE)) {
         Region aRegion = getRandomRegion(true);
         op = removeEntry(aRegion, false);
      } else if (operation.equalsIgnoreCase(Operation.REGION_KEYS)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.keySet().toArray() on " + aRegion.getFullPath());
         List aList = Arrays.asList(aRegion.keySet().toArray());
         Log.getLogWriter().info("Done calling aRegion.keySet().toArray() on " + aRegion.getFullPath() 
             + " with result " + aList);
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_KEYS, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_KEY_SET)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.keySet().toArray() on " + aRegion.getFullPath());
         List aList = Arrays.asList(aRegion.keySet().toArray());
         Log.getLogWriter().info("Done calling aRegion.keySet().toArray() on " + aRegion.getFullPath()
             + " with result " + aList);
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_KEY_SET, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_VALUES)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.values().toArray() on " + aRegion.getFullPath());
         List aList = Arrays.asList(aRegion.values().toArray());
         Log.getLogWriter().info("Done calling aRegion.values().toArray() on " + aRegion.getFullPath()
             + " with result " + aList);
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_VALUES, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_ENTRIES)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.entries(false).toArray() on " + aRegion.getFullPath());
         List aList = Arrays.asList(aRegion.entries(false).toArray());
         Log.getLogWriter().info("Done calling aRegion.entries(false).toArray() on " + aRegion.getFullPath()
             + " with result " + aList);
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_ENTRIES, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_ENTRY_SET)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.entrySet().toArray() on " + aRegion.getFullPath());
         List aList = Arrays.asList(aRegion.entrySet().toArray());
         Log.getLogWriter().info("Done calling aRegion.entrySet().toArray() on " + aRegion.getFullPath()
             + " with result " + aList);
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_ENTRY_SET, null, null);
      } else  {
         throw new TestException("Test config problem; Unknown repeatable read op " + operation);
      }
      if (op != null) {
         op.setRepeatableRead(true);
         opList.add(op);
         numOpsCompleted++;
      }
   } 
   Log.getLogWriter().info("Completed execution of " + opList);
   return opList;
}

/** Do random non-repeatable read operations on random regions.
 *  See bugmail for 36688 for a list of supported repeatable read operations.
 *  TxPrms.numOps as the number of operations to do.
 *
 *  @param numOperationsToDo The number of repeatable read operations to do.
 */
public OpList doNonrepeatableReadOperations(int numOperationsToDo) {
   Log.getLogWriter().info("Executing " + numOperationsToDo + " random NON-repeatable read operations...");
   OpList opList = new OpList();
   int numOpsCompleted = 0;
   GsRandom rand = TestConfig.tab().getRandGen();
   while (numOpsCompleted < numOperationsToDo) {
      Operation op = null;
      String operation = (String)(Operation.nonrepeatableReadOps.get(rand.nextInt(0, Operation.nonrepeatableReadOps.size()-1)));
      Log.getLogWriter().info("Nonrepeatable read operation is " + operation);

      if (operation.equalsIgnoreCase(Operation.REGION_KEYS)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.keySet() on " + aRegion.getFullPath());
         aRegion.keySet();
         Log.getLogWriter().info("Done calling aRegion.keySet() on " + aRegion.getFullPath());
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_KEYS, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_KEY_SET)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.keySet() on " + aRegion.getFullPath());
         aRegion.keySet();
         Log.getLogWriter().info("Done calling aRegion.keySet() on " + aRegion.getFullPath());
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_KEY_SET, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_VALUES)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.values() on " + aRegion.getFullPath());
         aRegion.values();
         Log.getLogWriter().info("Done calling aRegion.values() on " + aRegion.getFullPath());
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_VALUES, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_ENTRIES)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.entries(true) on " + aRegion.getFullPath());
         aRegion.entries(true);
         Log.getLogWriter().info("Done calling aRegion.entries(true) on " + aRegion.getFullPath());
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_ENTRIES, null, null);
      } else if (operation.equalsIgnoreCase(Operation.REGION_ENTRY_SET)) {
         Region aRegion = getRandomRegion(true);
         Log.getLogWriter().info("Calling aRegion.entrySet() on " + aRegion.getFullPath());
         aRegion.entrySet();
         Log.getLogWriter().info("Done calling aRegion.entrySet() on " + aRegion.getFullPath());
         op = new Operation(aRegion.getFullPath(), null, Operation.REGION_ENTRY_SET, null, null);
      } else {
         throw new TestException("Test config problem; Unknown nonrepeatable read op " + operation);
      }
      opList.add(op);
      numOpsCompleted++;
   } 
   Log.getLogWriter().info("Completed execution of " + opList);
   return opList;
}

// ======================================================================== 
// methods to do operations on region entries

/** Does a creates for a key.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param exists True if the key to create already exists, false
 *         if the key has never before been created. 
 *  
 *  @return An instance of Operation describing the create operation.
 */
public Operation createEntry(Region aRegion, boolean exists) {
   Object key = null;
   if (exists) {
      key = getRandomKey(aRegion);
      if (key == null) {
         Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + 
             " because no keys are available");
         return null;
      }
   } else {
      key = getNewKey(aRegion);
   }
   Object oldValue = null;
   BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
   String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + 
          DistributedSystemHelper.getDistributedSystem().getDistributedMember();

   try {
      if (supportsConcurrentMapOps(aRegion) && TestConfig.tab().getRandGen().nextBoolean()) {
        Log.getLogWriter().info("createEntry: putting key (putIfAbsent) " + key + ", object " + vh.toString() + " in region " + aRegion.getFullPath());
        Object v = aRegion.putIfAbsent(key, vh);
        if (v != null) {
          if (exists) { // this was expected
            Log.getLogWriter().info("putIfAbsent returned " + v + ".  Expected as this entry already existed");
          } else {
             throw new TestException(TestHelper.getStackTrace());
          }
        }
      } else {
        Log.getLogWriter().info("createEntry: putting key (create) " + key + ", object " + vh.toString() + " in region " + aRegion.getFullPath());
      aRegion.create(key, vh, callback);
      }
      Log.getLogWriter().info("createEntry: done putting key " + key + ", object " + 
        vh.toString() + " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (EntryExistsException e) {
      if (exists) { // this was expected
         Log.getLogWriter().info("Caught expected " + e.toString());
      } else {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CREATE, oldValue, vh.modVal);
}

/** Updates an existing entry in aRegion. The value in the key
 *  is increment by 1.
 *  
 *  @param aRegion The region to modify a key in.
 *  @param key The key to modify.
 *
 *  @return An instance of Operation describing the update operation.
 */
public Operation updateEntry(Region aRegion, Object key) {
   if (key == null) {
      Log.getLogWriter().info("Could not update a key in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }

   // get the old value
   BaseValueHolder vh = null;

   Log.getLogWriter().info("updateEntry: Getting value to prepare for update for key " + key + " in region " + aRegion.getFullPath());

   Object oldValue = getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder) {
      vh = (BaseValueHolder)oldValue;
      oldValue = ((BaseValueHolder)oldValue).modVal;
   } else {
      vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
   }

   Log.getLogWriter().info("updateEntry: Value to update is " + vh + " for key " + key + " in region " + aRegion.getFullPath());

   // we MUST use CopyHelper here (vs. copyOnRead) since we are using
   // getValueInVM() vs. a public 'get' api
   vh = (BaseValueHolder)CopyHelper.copy(vh);
   vh.modVal = new Integer(vh.modVal.intValue() + 1);
   String callback = updateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

   try {
      Log.getLogWriter().info("updateEntry: Putting new value " + vh + " for key " + key + " in region " + aRegion.getFullPath());
      if (supportsConcurrentMapOps(aRegion) && TestConfig.tab().getRandGen().nextBoolean()) {
        Log.getLogWriter().info("updateEntry: Putting (replace) new value " + vh + " for key " + key + " in region " + aRegion.getFullPath());
        aRegion.replace(key, vh);
      } else {
        Log.getLogWriter().info("updateEntry: Putting (put) new value " + vh + " for key " + key + " in region " + aRegion.getFullPath());
      aRegion.put(key, vh, callback);
      }
      Log.getLogWriter().info("updateEntry: Done putting new value " + vh + " for key " + key + 
          " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_UPDATE, oldValue, vh.modVal);
}

public List<Operation> putAll(Region r) {
  List<Operation> opList = new ArrayList<Operation>();
  Random random = new Random();
  int beforeSize = r.size();
  String numPutAllNewKeys = TestConfig.tab().stringAt(ParRegPrms.numPutAllNewKeys);
  int numNewKeysToPut = 0;
  if (numPutAllNewKeys.equalsIgnoreCase("useThreshold")) {
     numNewKeysToPut = random.nextInt(5);//TODO parameterize this 
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
  Map mapToPut = new LinkedHashMap();//this is a linked hash map because prTxListener
                                     //and writer try to validate order of operations,
                                    //which should match with the opList

  // add new keys to the map
  StringBuffer newKeys = new StringBuffer();
  for (int i = 1; i <= numNewKeysToPut; i++) { // put new keys
     Object key = getNewKey(r);
     BaseValueHolder anObj = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
     mapToPut.put(key, anObj);
     opList.add(new Operation(r.getFullPath(), key, Operation.ENTRY_CREATE, null, anObj.modVal));
     newKeys.append(key + " ");
     if ((i % 10) == 0) {
        newKeys.append("\n");
     }
  }

  // add existing keys to the map
  int numPutAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
  List keyList = new ArrayList();
  int keyCount = 0;
  while (keyCount < numPutAllExistingKeys) {
    Object key = getRandomKey(r);
    if (key != null) {
      keyList.add(key);
      keyCount++;
    }
    if (beforeSize < numPutAllExistingKeys) {
      break;
    }
  }
  StringBuffer existingKeys = new StringBuffer();
  if (keyList.size() != 0) { // no existing keys could be found
     for (int i = 0; i < keyList.size(); i++) { // put existing keys
        Object key = keyList.get(i);
        BaseValueHolder anObj = getUpdateObject(r, key);
        mapToPut.put(key, anObj);
        Object oldValue = getValueInVM(r, key);
        if (oldValue instanceof BaseValueHolder) {
           oldValue = ((BaseValueHolder)oldValue).modVal;
        }
        opList.add(new Operation(r.getFullPath(), key, Operation.ENTRY_UPDATE, oldValue, anObj.modVal));
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

  return opList;
}

private BaseValueHolder getUpdateObject(Region r, Object key) {
  BaseValueHolder vh = null;
  Object oldValue = getValueInVM(r, key);
  if (oldValue instanceof BaseValueHolder) {
     vh = (BaseValueHolder)oldValue;
     oldValue = ((BaseValueHolder)oldValue).modVal;
  } else {
     vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
  }

  Log.getLogWriter().info("updateEntry: Value to update is " + vh + " for key " + key + " in region " + r.getFullPath());

  // we MUST use CopyHelper here (vs. copyOnRead) since we are using
  // getValueInVM() vs. a public 'get' api
  vh = (BaseValueHolder)CopyHelper.copy(vh);
  vh.modVal = new Integer(vh.modVal.intValue() + 1);

  return vh;
}

/** Destroys an entry in aRegion. 
 *  
 *  @param isLocalDestroy True if the opertion is a local destroy, false otherwise.
 *  @param aRegion The region to destroy the entry in.
 *  @param key The key to destroy.
 *  @param entryNotFoundExcOK True if it is ok to get an EntryNotFoundException on this
 *         destroy.
 *
 *  @return An instance of Operation describing the destroy operation.
 */
public Operation destroyEntry(boolean isLocalDestroy, Region aRegion, Object key, boolean entryNotFoundExcOK) {
   if (key == null) {
      Log.getLogWriter().info("Could not destroy an entry in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object conditionValue = getValueInVM(aRegion, key, false);  // do not allow transition to INVALID for use in remove
      Object oldValue = getValueInVM(aRegion, key, true);   // allow translation to INVALID (goes into Operation)
      if (conditionValue instanceof BaseValueHolder) {
         oldValue = ((BaseValueHolder)conditionValue).modVal;
      }
         String callback = destroyCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      if (isLocalDestroy) {
         Log.getLogWriter().info("destroyEntry: locally destroying key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.localDestroy(key, callback);
         Log.getLogWriter().info("destroyEntry: done locally destroying key " + key +
             " in region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_LOCAL_DESTROY, oldValue, null);
      } else {
         Log.getLogWriter().info("destroyEntry: destroying key " + key + " in region " + aRegion.getFullPath());
         if (supportsConcurrentMapOps(aRegion) && TestConfig.tab().getRandGen().nextBoolean()) {
           Log.getLogWriter().info("destroyEntry: destroying (remove(" + key + ", " + conditionValue + ")) in region " + aRegion.getFullPath());
           boolean removed = aRegion.remove(key, conditionValue);
           if (!removed) {
             if (isSerialExecution) {
               throw new TestException("remove(" + key + ", " + conditionValue + ") returned false, expected successful remove\n" + TestHelper.getStackTrace());
             } else {
               return null;
             }
           }
         } else {
           Log.getLogWriter().info("destroyEntry: destroying (destroy) key " + key + " in region " + aRegion.getFullPath());
         aRegion.destroy(key, callback);
         }
         Log.getLogWriter().info("destroyEntry: done destroying key " + key + " in region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_DESTROY, oldValue, null);
      }
   } catch (RegionDestroyedException e) { // somebody else beat us to it
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      if (isSerialExecution) {
         if (entryNotFoundExcOK) {
            Log.getLogWriter().info("Caught expected exception " + e);
            return null;
         } else {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
      // if concurrent, this is OK
      return null;
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Removes an entry in aRegion. 
 *  
 *  @param aRegion The region to remove the entry from.
 *  @param exists True if the key exists in the region, false otherwise.
 *
 *  @return An instance of Operation describing the remove operation.
 */
public Operation removeEntry(Region aRegion, boolean exists) {
   Object key = null;
   if (exists) {
      key = getRandomKey(aRegion);
      if (key == null) {
         Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + 
             " because no keys are available");
         return null;
      }
   } else {
      key = NameFactory.getNextPositiveObjectName();
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      Log.getLogWriter().info("removeEntry: removing key " + key +
          " in region " + aRegion.getFullPath());
      Object result = aRegion.remove(key);
      Log.getLogWriter().info("removeEntry: done removing key " + key +
          " in region " + aRegion.getFullPath() + ", result is " + result);
      return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_REMOVE, oldValue, null);
   } catch (RegionDestroyedException e) { // somebody else beat us to it
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Invalidates an entry in aRegion. 
 *  
 *  @param isLocalInval True if the opertion is a local invalidate, false otherwise.
 *  @param aRegion The region to invalidate the entry in.
 *  @param key The key to invalidate.
 *  @param entryNotFoundExcOK True if it is ok to get an EntryNotFoundException on this
 *         invalidate.
 *
 *  @return An instance of Operation describing the invalidate operation.
 */
public Operation invalEntry(boolean isLocalInval, Region aRegion, Object key, boolean entryNotFoundExcOK) {
   if (key == null) {
      Log.getLogWriter().info("Could not invalidate an entry in " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }

   try {
      String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      if (isLocalInval) {
         Log.getLogWriter().info("invalEntry: locally invalidating key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.localInvalidate(key, callback);
         Log.getLogWriter().info("invalEntry: done locally invalidating key " + key +
             " in region " + aRegion.getFullPath());
         Object newValue = getValueInVM(aRegion, key);
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_LOCAL_INVAL, oldValue, newValue);
      } else {
         Log.getLogWriter().info("invalEntry: invalidating key " + key +
             " in region " + aRegion.getFullPath());
         aRegion.invalidate(key, callback);
         Log.getLogWriter().info("invalEntry: done invalidating key " + key +
             " in region " + aRegion.getFullPath());
         Object newValue = getValueInVM(aRegion, key);
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_INVAL, oldValue, newValue);
      }
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (EntryNotFoundException e) {
      if (isSerialExecution) {
         if (entryNotFoundExcOK) {
            Log.getLogWriter().info("Caught expected exception " + e);
            return null;
         } else {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      }
      // if concurrent, this is OK
      return null;
   }
}

/** Gets a value in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation getWithExistingKey(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getWithExistingKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getWithExistingKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_EXIST_KEY, oldValue, null);
      else
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_EXIST_KEY, oldValue, vh.modVal);
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

public Operation getAll(Region r) {
  int beforeSize = r.size();
  int numGetAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
  List keyList = new ArrayList();
  int keyCount = 0;
  while (keyCount < numGetAllExistingKeys) {
    Object key = getRandomKey(r);
    if (key != null) {
      keyList.add(key);
      keyCount++;
    }
    if (beforeSize < numGetAllExistingKeys) {
      break;
    }
  }
  Log.getLogWriter().info("Doing a getAll with keys:"+keyList+" count is:"+keyList.size());
  Map result = r.getAll(keyList);
  Log.getLogWriter().info("completed getAll: result size:"+result.size());
  return new Operation(r.getFullPath(), keyList.toArray(), Operation.ENTRY_GETALL, null, null);
}
/** Does a getEntry in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation getEntryWithExistingKey(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not get with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntryWithExistingKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      Region.Entry entry = aRegion.getEntry(key);
      BaseValueHolder vh = (BaseValueHolder)(entry.getValue());
      Log.getLogWriter().info("getEntryWithExistingKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_ENTRY_EXIST_KEY, oldValue, null);
      else
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_ENTRY_EXIST_KEY, oldValue, vh.modVal);
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

/** Does a containsKey in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation containsKeyExisting(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not do containsKey with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("containsKeyExisting: calling containsKey for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      boolean result = aRegion.containsKey(key);
      Log.getLogWriter().info("containsKeyExisting: result is " + result);
      return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CONTAINS_KEY, oldValue, null);
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

/** Does a containsValueForKey in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation containsValueForKeyExisting(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not do containsValueForKey with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("containsValueForKeyExisting: calling containsValueForKey for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      boolean result = aRegion.containsValueForKey(key);
      Log.getLogWriter().info("containsValueForKeyExisting: result is " + result);
      return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CONTAINS_VALUE_FOR_KEY, oldValue, null);
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

/** Does a containsValue in aRegion with a key existing in aRegion.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation containsValueExisting(Region aRegion) {
   Object key = getRandomKey(aRegion);
   if (key == null) {
      Log.getLogWriter().info("Could not do containsValue with an existing key " + aRegion.getFullPath() + 
          " because no keys are available");
      return null;
   }
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("containsValueExisting: calling containsValue for key " + key +
             " in region " + aRegion.getFullPath());
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      boolean result = aRegion.containsValue(key);
      Log.getLogWriter().info("containsValueExisting: result is " + result);
      return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CONTAINS_VALUE, oldValue, null);
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
 *  @return An instance of Operation describing the get operation.
 */
public Operation getEntryWithPreviousKey(Region aRegion) {
   long keysUsed = NameFactory.getPositiveNameCounter();
   Object key = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)keysUsed));
   try {
      Object oldValue = getValueInVM(aRegion, key);
      if (oldValue instanceof BaseValueHolder)
         oldValue = ((BaseValueHolder)oldValue).modVal;
      Log.getLogWriter().info("getEntryWithPreviousKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getEntryWithPreviousKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_PREV_KEY, oldValue, null);
      else
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_PREV_KEY, oldValue, vh.modVal);
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

/** Gets a new (never before seen) key
 *
 *  @return the next key from NameFactory
 */
public Object getNewKey(Region aRegion) {
   // In tx tests on distributed regions, the region doesn't matter
   // It is needed in PrViewUtil (parReg/tx) and when operating on PartitionedRegions.
   Object key = NameFactory.getNextPositiveObjectName();
   boolean found = false;
   if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
     DistributedMember myDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
     // will this key be for this VM?
     DistributedMember primaryDM;
     do {
       primaryDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
       if (primaryDM != null && primaryDM.equals(myDM)) {
         found = true;
       } else {  // try the next key
         key = NameFactory.getNextPositiveObjectName();
       }
     } while (!found);
   } 
   return key;
}

/** Gets a value in aRegion with a the next new (never-before_used) key.
 *  
 *  @param aRegion The region to get from.
 *
 *  @return An instance of Operation describing the get operation.
 */
public Operation getEntryWithNewKey(Region aRegion) {
   Object key = getNewKey(aRegion);
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntryWithNewKey: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getEntryWithNewKey: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_NEW_KEY, oldValue, null);
      else
         return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_GET_NEW_KEY, oldValue, vh.modVal);
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
 *  @param opName The operation to use for the returned Operation instance.
 *  
 *  @return An instance of Operation describing the put operation.
 */
public Operation putEntry(Region aRegion, Object key, BaseValueHolder value, String opName) {
   Object oldValue = getValueInVM(aRegion, key);
   if (oldValue instanceof BaseValueHolder)
      oldValue = ((BaseValueHolder)oldValue).modVal;
   try {
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      Log.getLogWriter().info("putEntry: putting key " + key + ", object " + 
        value + " in region " + aRegion.getFullPath());
      aRegion.put(key, value, callback);
      Log.getLogWriter().info("putEntry: done putting key " + key + ", object " + 
        value + " in region " + aRegion.getFullPath());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      // if concurrent, this is OK
      return null;
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new Operation(aRegion.getFullPath(), key, opName, oldValue, value.modVal);
}

/** Does a get with the given key.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param key The key to use for the put.
 *  @param opName The operation to use for the returned Operation instance.
 *  
 *  @return An instance of Operation describing the get operation.
 */
public Operation getEntry(Region aRegion, Object key, String opName) {
   try {
      Object oldValue = getValueInVM(aRegion, key);
      Log.getLogWriter().info("getEntry: getting value for key " + key +
             " in region " + aRegion.getFullPath());
      BaseValueHolder vh = (BaseValueHolder)(aRegion.get(key));
      Log.getLogWriter().info("getEntry: got value for key " + key + ": " + vh +
             " in region " + aRegion.getFullPath());
      if (vh == null)
         return new Operation(aRegion.getFullPath(), key, opName, oldValue, null);
      else
         return new Operation(aRegion.getFullPath(), key, opName, oldValue, vh.modVal);
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
 *  @return An instance of Operation describing the invalidate operation.
 */
public Operation invalRegion(boolean isLocalInval, Region aRegion) {
   try {
      String callback = regionInvalidateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();

      if (isLocalInval) {
         Log.getLogWriter().info("invalRegion: locally invalidating region " + aRegion.getFullPath());
         aRegion.localInvalidateRegion(callback);
         Log.getLogWriter().info("invalRegion: done locally invalidating region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), null, Operation.REGION_LOCAL_INVAL, null, null);
      } else {
         Log.getLogWriter().info("invalRegion: invalidating region " + aRegion.getFullPath());
         aRegion.invalidateRegion(callback);
         Log.getLogWriter().info("invalRegion: done invalidating region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), null, Operation.REGION_INVAL, null, null);
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
 *  @return An instance of Operation describing the destroy operation.
 */
public Operation destroyRegion(boolean isLocalDestroy, Region aRegion) {
   if (aRegion == null) {
     return null;
   }

   try {
      recordDestroyedRegion(aRegion);
      String callback = regionDestroyCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      if (isLocalDestroy) {
         Log.getLogWriter().info("destroyRegion: locally destroying region " + aRegion.getFullPath());
         aRegion.localDestroyRegion(callback);
         Log.getLogWriter().info("destroyRegion: done locally destroying region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), null, Operation.REGION_LOCAL_DESTROY, null, null);
      } else {
         Log.getLogWriter().info("destroyRegion: destroying region " + aRegion.getFullPath());
         aRegion.destroyRegion(callback);
         Log.getLogWriter().info("destroyRegion: done destroying region " + aRegion.getFullPath());
         return new Operation(aRegion.getFullPath(), null, Operation.REGION_DESTROY, null, null);
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
 *  @return An instance of Operation describing the destroy operation.
 */
public Operation createRegion() {
   ArrayList list = (ArrayList)(TxBB.getBB().getSharedMap().get(TxBB.DestroyedRegionsKey));
   if (list != null) { // using BB to track destroyed regions
      for (int i = 0; i < list.size(); i++) {
         String regionPath = (String)(list.get(i));
         Object[] tmp = createRegionWithPath(regionPath, false);
         Region aRegion = (Region)(tmp[0]);
         boolean regionCreated = ((Boolean)(tmp[1])).booleanValue();
         if (regionCreated)
            return new Operation(regionPath, null, Operation.REGION_CREATE, null, null);
      }
   } else { // not using BB to track regions
      Map aMap = getRegionConfigMap();
      Iterator it = aMap.keySet().iterator();
      while (it.hasNext()) {
         String regionPath = (String)(it.next());
         Object value = aMap.get(regionPath);
         if (value instanceof String) {
            Object[] tmp = createRegionWithPath(regionPath, false);
            Region aRegion = (Region)(tmp[0]);
            boolean regionCreated = ((Boolean)(tmp[1])).booleanValue();
            if (regionCreated)
               return new Operation(regionPath, null, Operation.REGION_CREATE, null, null);
         }
      }
   }
   return null;
}

// ======================================================================== 
// methods for cache operations

/** Closes the cache.
 *  
 *  @return An instance of Operation describing the close operation.
 */
public Operation closeCache() {
   CacheUtil.setCache(null);
   CacheHelper.closeCache();
   return new Operation(null, null, Operation.CACHE_CLOSE, null, null);
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
 *  If suspendResume is true, then suspend the tx before finding
 *  the random key, then resume before returning.
 *
 *  @param aRegion - The region to get the key from.
 *  @param excludeKey - The region to get the key from.
 *
 *  @returns A key from aRegion, or null.
 */
public Object getRandomKey(Region aRegion, Object excludeKey) {
   // getting a random key can put all the keys into the remembered
   // set, so suspend the tx if suspendResume is true, then resume it 
   // before leaving this method so the test can test according to 
   // its strategy without the test framework getting in the way.
   if (suspendResume) {
      txState.set(TxHelper.internalSuspend()); 
   }
   if (aRegion == null) {
      return null;
   }
   Set aSet = null;
   try {
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        aSet = PartitionRegionHelper.getLocalData(aRegion).keySet();
      } else {
        aSet = new HashSet(aRegion.keySet());
      }
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      if (suspendResume) {
         TxHelper.internalResume((TXStateInterface)txState.get()); 
      }
      return null;
   }
   Object[] keyArr = aSet.toArray();
   if (keyArr.length == 0) {
      Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + 
          " because the region has no keys");
      if (suspendResume) {
         TxHelper.internalResume((TXStateInterface)txState.get()); 
      }
      return null;
   }
   int randInt = TestConfig.tab().getRandGen().nextInt(0, keyArr.length-1);
   Object key = keyArr[randInt];
   if (key.equals(excludeKey)) { // get another key
      if (keyArr.length == 1) { // there are no other keys
         if (suspendResume) {
            TxHelper.internalResume((TXStateInterface)txState.get()); 
         }
         return null;
      }
      randInt++; // go to the next key
      if (randInt == keyArr.length)
         randInt = 0;
      key = keyArr[randInt];
   }
   if (suspendResume) {
      TxHelper.internalResume((TXStateInterface)txState.get()); 
   }
   return key;
}

public Object getNewValue(Object key) {
   return new ValueHolder(key, randomValues, new Integer(modValInitializer++));
}

/** Given a region name, return the region instance for it. If the 
 *  region with regionName currently does not exist, create it (and 
 *  any parents required to create it).
 *
 *  @param regionName - The full path name of a region.
 *  @param fill - If true, then fill each created region with entries
 *                according to TxPrms.maxEntries
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
         String regionConfig = getRegionConfigFromBB("/" + currentRegionName);
         AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);

         // View tests & proxySerialConflict tests require overwriting the
         // dataPolicy to allow specific combinations betweeen clients
         String clientName = System.getProperty( "clientName" );
         String mapKey = TxBB.DataPolicyPrefix + clientName;
         String dataPolicy = (String)(TxBB.getBB().getSharedMap().get(mapKey));
         if (dataPolicy != null) {
           aFactory.setDataPolicy(TestHelper.getDataPolicy(dataPolicy));
         }

         RegionAttributes attrs = RegionHelper.getRegionAttributes(aFactory);
         aRegion =theCache.createVMRegion(currentRegionName, attrs);
         Log.getLogWriter().info("Created root region " + aRegion.getFullPath());

         // edge clients register interest in ALL_KEYS
         if (aRegion.getAttributes().getPoolName() != null) {
            aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
            Log.getLogWriter().info("registerInterest(ALL_KEYS) for " + currentRegionName);
         }

         if (fill)
            createEntries(aRegion);
         regionCreated = true;
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (RegionExistsException e) {
         if (isSerialExecution)
            throw new TestException("Test error; unexpected " + TestHelper.getStackTrace(e));
         // ok if concurrent
         aRegion = CacheUtil.getCache().getRegion(currentRegionName);
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
            String regionConfig = getRegionConfigFromBB(regionPath);
            AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);

            // View tests & proxySerialConflict tests require overwriting the
            // dataPolicy to allow specific combinations betweeen clients
            String clientName = System.getProperty( "clientName" );
            String mapKey = TxBB.DataPolicyPrefix + clientName;
            String dataPolicy = (String)(TxBB.getBB().getSharedMap().get(mapKey));
            if (dataPolicy != null) {
              aFactory.setDataPolicy(TestHelper.getDataPolicy(dataPolicy));
            }

            RegionAttributes attr = RegionHelper.getRegionAttributes(aFactory);
            Log.getLogWriter().info("Attempting to create region " + currentRegionName + " with " + TestHelper.regionAttributesToString(attr));
            aRegion = previousRegion.createSubregion(currentRegionName, attr);
            Log.getLogWriter().info("Created region " + aRegion.getFullPath());

            // edge clients register interest in ALL_KEYS
            if (aRegion.getAttributes().getPoolName() != null) {
               aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
               Log.getLogWriter().info("registerInterest(ALL_KEYS) for " + currentRegionName);
            }

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
            aRegion = CacheUtil.getCache().getRegion(regionPath);
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
   TxBB.getBB().getSharedMap().put(TxBB.DestroyedRegionsKey, new ArrayList());
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
   ArrayList list = (ArrayList)(TxBB.getBB().getSharedMap().get(TxBB.DestroyedRegionsKey));
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
   TxBB.getBB().getSharedMap().put(TxBB.DestroyedRegionsKey, list);
   Log.getLogWriter().info("TxBB.DestroyedRegionsKey = " + list);
}

/** Creates all regions previously destroyed that were recorded in the
 *  TxBB shared map entry TxBB.DestroyedRegionsKey.
 *  
 *  @param fill True if each newly created region should be populated with
 *              entries according to TxPrms.maxKeys
 */
public void createDestroyedRegionsFromBB(boolean fill) {
   if (!isSerialExecution)
      throw new TestException("Do not call this from concurrent tests as DestroyedRegionsKey is not maintained");
   ArrayList list = (ArrayList)(TxBB.getBB().getSharedMap().get(TxBB.DestroyedRegionsKey));
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
 *              entries according to TxPrms.maxKeys
 */
public void createAllDestroyedRegions(boolean fill) {
   Map aMap = getRegionConfigMap();
   Iterator it = aMap.keySet().iterator();
   Log.getLogWriter().info("In createAllDestroyedRegions");
   while (it.hasNext()) {
      String key = (String)(it.next());
      Object value = aMap.get(key);
      if (value instanceof String) {
         createRegionWithPath(key, fill);
      }
   }
   Log.getLogWriter().info("Done in createAllDestroyedRegions");
}

static public boolean inTxThreadWithTxInProgress() {
  return (TxHelper.getTransactionId()==null) ? false : true;
}

/*
 * Helper function to determine if we're in the transacational VM (not
 * necessarily the txThread -- helps when we validate state for LOCAL scope
 */
static public boolean inTxVm() {
  Integer txVmPid = (Integer)TxBB.getBB().getSharedMap().get(TxBB.TX_VM_PID);
  if (txVmPid == null) { // tests not using tx (like AsyncMsgTests which also use operations/opList)
    return false;
  }
  int myVmPid = ProcessMgr.getProcessId();

  return (txVmPid.intValue()==myVmPid);
}

/** Return true if the region or any of its subregions is replicated,
 *  return false otherwise.
 *
 *  @param aRegion The region parent to test for replication.
 *
 */
public boolean isHierReplicated(Region aRegion) {
   boolean isReplicated = aRegion.getAttributes().getDataPolicy().withReplication();
   if (isReplicated)
      return true;
   Object[] regionArr = getSubregions(aRegion, true).toArray();
   for (int j = 0; j < regionArr.length; j++) {
      Region subR = (Region)(regionArr[j]);
      if (subR.getAttributes().getDataPolicy().withReplication())
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
 *  the full path name for the region, values are the (String) regionConfig
 *
 *  @returns Map - The map containing the full path of the region as
 *                 the key, and its (String) regionConfig as the value.
 */
public Map getRegionConfigMap() {
   Map aMap = (Map)(TxBB.getBB().getSharedMap().get(TxBB.RegConfigForPIDKey));
   if (aMap == null)
      return new HashMap();
   return aMap;
}

/** Given a full path for a region, and its (String) regionConfig,
 *  write it to the TxBB blackboard shared map.
 *
 *  The shared map has one key per VM in the form:
 *     TxBB.RegConfigForPIDKey_xxxxx, where xxxxs is the PID for the current VM.
 *     The value for this key is a Map, containing the fullPathName for the
 *        key, and the (String) regionConfig for the value.
 *  The blackboard maintains the region config string for each VM (PID)
 *
 *  @param fullPathOfRegion - the full path of the region to get the key for.
 *  @param regionConfig - The (String) regionConfig for this region
 */
public void recordRegionConfigInBB(String fullPathOfRegion, String regionConfig) {
   Map aMap = getRegionConfigMap();
   aMap.put(fullPathOfRegion, regionConfig);
   TxBB.getBB().getSharedMap().put(TxBB.RegConfigForPIDKey, aMap);
}

/** Given a full path for a region, return its (String) regionConfig
 *  The shared map has one key per VM in the form:
 *     TxBB.RegConfigForPIDKey_xxxxx, where xxxxs is the PID for the current VM.
 *     The value for this key is a Map, containing the fullPathName for the
 *        key, and the String (RegionConfig) for the value.
 *  The blackboard records the (String) region config for each VM (PID)
 *
 *  @param fullPathOfRegion - the full path of the region to get the key for.
 */
public String getRegionConfigFromBB(String fullPathOfRegion) {
   Map aMap = getRegionConfigMap();
   String regionConfig = (String)aMap.get(fullPathOfRegion);
   return regionConfig;
}

/** Call containsKey on the given region and key using suspend and resume.
 *
 *  @param aRegion - The region to test for the key.
 *  @param key - The key to use for containsKey.
 *
 *  @returns true if the region contains an entry with key, false otherwise.
 */
public boolean containsKey(Region aRegion, Object key) {
   if (suspendResume) {
      txState.set(TxHelper.internalSuspend()); 
   }
   boolean result = aRegion.containsKey(key);
   if (suspendResume) {
      TxHelper.internalResume((TXStateInterface)txState.get()); 
   }
   return result;
}

/** Call getValueInVM on the given region and key 
 *
 *  @param aRegion - The region to use for the call.
 *  @param key - The key to use for the call.
 *
 *  @returns The value in the VM of key.  Returns null if value not found.
 */
public Object getValueInVM(Region aRegion, Object key) {
   return getValueInVM(aRegion, key, true);
}

/* Call getValueInVM with a choice of whether or not to translate null (invalid) entries to 
 * the INVALID_TOKEN.  The tx tests require this in most cases (which is why we assume 'true'
 * if not specified (see method above)).  However, with concurrentMap operations this causes 
 * us to expect the operation to be successful, but it may not be if the value was invalid.
 */
public Object getValueInVM(Region aRegion, Object key, boolean translateToInvalidToken) {

   Object value = null;
   Region.Entry entry = aRegion.getEntry(key);
   if (entry != null) {
     try {
       value = entry.getValue();
       if (value == null && translateToInvalidToken) {    // keyExists with null value => INVALID
         value = Token.INVALID;
       } 
     } catch (EntryDestroyedException e) {  
         value = null;
     }
   } 
   return value;
}

/** Determine whether or not concurrent map operations are supported for the given region.
 *  EMPTY and NORMAL regions are not supported in GemFire 6.5.
 *
 *  @param aRegion - The region to check
 *
 *  @returns boolean indicating support of ConcurrentMap ops 
 */
public boolean supportsConcurrentMapOps(Region aRegion) {
   DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();
   if (dataPolicy.equals(DataPolicy.NORMAL) || dataPolicy.equals(DataPolicy.EMPTY)) {
     return false;
   }
   return true;
}

}
