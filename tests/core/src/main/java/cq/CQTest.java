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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.util.BridgeWriterException;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.pdx.PdxInstance;

import delta.DeltaObject;
import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.PoolHelper;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import rebalance.RebalanceUtil;
import util.CacheUtil;
import util.MethodCoordinator;
import util.NameFactory;
import util.QueryObject;
import util.RandomValues;
import util.SilenceListener;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

public class CQTest {
    
/* The singleton instance of CQTest in this VM */
static public CQTest testInstance;
static public CQTestVersionHelper versionHelperInstance;
    
protected static final String VmIDStr = "VmId_";
protected static final String ThreadIDStr = "ThreadId_";

/* instance fields */
protected long minTaskGranularitySec;       // the task granularity in seconds
protected long minTaskGranularityMS;        // the task granularity in milliseconds
protected int numOpsPerTask;                // the number of operations to execute per task
public boolean isBridgeConfiguration;    // true if this test is being run in a bridge configuration, false otherwise
public boolean isBridgeClient;           // true if this vm is a bridge client, false otherwise

protected int numThreadsInClients;          // the total number of client threads in this test 
public Region aRegion;                   // the region in this VM
protected QueryService qService;            // the QueryService in this VM
public HydraThreadLocal uniqueKeyIndex = new HydraThreadLocal();
protected boolean isSerialExecution;        // true if this test is serial, false otherwise
protected boolean highAvailability;         // value of CQUtilPrms.highAvailability
protected int upperThreshold;               // value of CQUtilPrms.upperThreshold
protected int lowerThreshold;               // value of CQUtilPrms.lowerThreshold
protected boolean uniqueKeys = false;       // whether each thread should use unique keys or not
public int queryDepth;                   // value of CQUtilPrms.queryDepth
protected int numQueriesPerClientVM;        // value of CQUtilPrms.numQueriesPerClientVM
public Map queryMap;                     // the random queries used by this VM; keys are query names
                                            // (String) and values are queries (String)
protected int secondsToRun;                 // number of seconds to allow tasks
protected boolean cacheIsClosed;            // true if this VM has closed the cache, false otherwise
protected boolean disconnected;             // true if this VM is disconnected from the distributed system, false otherwise
protected int numThreadsInThisVM = 0;       // the number of threads in this VM
protected static boolean CQsOn = true;
protected boolean useDeltaObjects = false;
protected boolean isOldClient = false; // used to determine a client that does not support ConcurrentMap API methods

// instance fields used to verify the contents of a region in serial tests
protected Map regionSnapshot;               // a "picture" of the expected region
protected Map txRegionSnapshot;             // a "picture" of the expected region (prior to transactional ops)
                                            // used for save/restore when handling TransactionExceptions in serial tests

// operations
static protected final int ENTRY_ADD_OPERATION = 1;
static protected final int ENTRY_DESTROY_OPERATION = 2;
static protected final int ENTRY_INVALIDATE_OPERATION = 3;
static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
static protected final int ENTRY_UPDATE_OPERATION = 6;
static protected final int ENTRY_GET_OPERATION = 7;
static protected final int ENTRY_GET_NEW_OPERATION = 8;
static protected final int ENTRY_MODIFY_VALUE = 9;
    
// String prefixes for event callback object
protected static final String getCallbackPrefix = "Get originated in pid ";
protected static final String createCallbackPrefix = "Create event originated in pid ";
protected static final String updateCallbackPrefix = "Update event originated in pid ";
protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";

// fields to coordinate running a method once per VM
protected static volatile MethodCoordinator concVerifyCoordinator = 
          new MethodCoordinator(CQTest.class.getName(), "concVerify");

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of CQTest in a client.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      // We need to determine oldClient vs. newClient prior to region creation 
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         testInstance.isOldClient = GemFireVersion.getGemFireVersion().equals("6.1.2");
      }
      testInstance.aRegion = testInstance.initializeRegion("clientRegion");
      if (testInstance.isBridgeConfiguration) {
         testInstance.registerInterest(testInstance.aRegion);
         if (CQsOn) {
            testInstance.initializeQueryService();
            testInstance.queryMap = testInstance.generateQueries(testInstance.queryDepth);
            testInstance.initializeCQs();
         } else {
            Log.getLogWriter().info("Not creating CQs because CQUtilPrms.CQsOn is " + CQsOn);
         }
      }
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes the singleton instance of CQTest in a client.
 */
public synchronized static void HydraTask_initializeClient1() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("clientRegion1");
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         testInstance.registerInterest(testInstance.aRegion);
         if (CQsOn) {
            testInstance.initializeQueryService();
            testInstance.queryMap = testInstance.generateQueries(testInstance.queryDepth);
            testInstance.initializeCQs();
         } else {
            Log.getLogWriter().info("Not creating CQs because CQUtilPrms.CQsOn is " + CQsOn);
         }
      }
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Creates and initializes the singleton instance of CQTest in a client.
 */
public synchronized static void HydraTask_initializeClient2() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("clientRegion2");
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         testInstance.registerInterest(testInstance.aRegion);
         if (CQsOn) {
            testInstance.initializeQueryService();
            testInstance.queryMap = testInstance.generateQueries(testInstance.queryDepth);
            testInstance.initializeCQs();
         } else {
            Log.getLogWriter().info("Not creating CQs because CQUtilPrms.CQsOn is " + CQsOn);
         }
      }
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}
    
/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverRegion");
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeAccessorServer1() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverAccessor");
      BridgeHelper.startBridgeServer("bridge1");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeAccessorServer2() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverAccessor");
      BridgeHelper.startBridgeServer("bridge2");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeServer1() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverDataHost");
      BridgeHelper.startBridgeServer("bridge1");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeServer2() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverDataHost");
      BridgeHelper.startBridgeServer("bridge2");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Creates and initializes a bridge server.
 */
public synchronized static void HydraTask_initializeDataHostServer() {
   if (testInstance == null) {
      testInstance = new CQTest();
      testInstance.initializeInstance();
      testInstance.aRegion = testInstance.initializeRegion("serverDataHost");
      BridgeHelper.startBridgeServer("bridge3");
      testInstance.isBridgeClient = false;
   }
   testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
}

/** Test for missing antlr.jar.
 *  We would like to see a NoClassDefFoundError thrown every time so the user
 *  has a clue what is wrong.
 *  This should be executed by only one thread per VM.
 */
public static void HydraTask_missingJarTest() {
   try {
      HydraTask_initializeClient();
      // We should have gotten an Exception back (with no antlr jar)
      Log.getLogWriter().info("Expected NoClassDefFoundError due to missing antlr jar on CLASSPATH " + TestHelper.getStackTrace());
      throw new TestException(TestHelper.getStackTrace("Expected NoClassDefFoundError due to missing antlr jar on CLASSPATH"));
   } catch (NoClassDefFoundError e) {
      if ((e.toString().indexOf("antlr/TokenStream") >= 0) ||
          (e.toString().indexOf("antlr.TokenStream") >= 0)) {
         // we got the correct exception for not having the antlr jar on the classpath; ok
         Log.getLogWriter().info("Caught expected exception " + e);
      } else {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } 
}

/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
public Region initializeRegion(String regDescriptName) {

   // versioning allows testing with ClientCacheFactory 
   Region aRegion = versionHelperInstance.initializeRegion(regDescriptName);

   // re-initialize the TransactionManager (for the newly created cache) (since 6.6)
   CQTestVersionHelper.setTxMgr();

   return aRegion;
}

public void initializeQueryService() {
      Log.getLogWriter().info("Creating QueryService.");

      // versioning allows testing with ClientCacheFactory
      versionHelperInstance.initializeQueryService();
}
    
/** Initialize instance fields of this test class */
public void initializeInstance() {
   Integer anInt = Integer.getInteger("numThreads");
   if (anInt != null) {
      numThreadsInThisVM = anInt.intValue();
   }
   numThreadsInClients = TestHelper.getNumThreads();
   CQsOn = TestConfig.tab().booleanAt(CQUtilPrms.CQsOn, true);
   isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
   highAvailability = TestConfig.tab().booleanAt(CQUtilPrms.highAvailability, false);
   upperThreshold = TestConfig.tab().intAt(CQUtilPrms.upperThreshold, Integer.MAX_VALUE);
   lowerThreshold = TestConfig.tab().intAt(CQUtilPrms.lowerThreshold, -1);
   numOpsPerTask = TestConfig.tab().intAt(CQUtilPrms.numOpsPerTask, Integer.MAX_VALUE);
   queryDepth = TestConfig.tab().intAt(CQUtilPrms.queryDepth, 1);
   numQueriesPerClientVM = TestConfig.tab().intAt(CQUtilPrms.numQueriesPerClientVM, 10);
   uniqueKeys = TestConfig.tab().booleanAt(CQUtilPrms.useUniqueKeys, false);
   useDeltaObjects = TestConfig.tab().booleanAt(CQUtilPrms.useDeltaObjects, false);
   secondsToRun = TestConfig.tab().intAt(CQUtilPrms.secondsToRun, 1800);
   minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
   if (minTaskGranularitySec == Long.MAX_VALUE)
      minTaskGranularityMS = Long.MAX_VALUE;
   else 
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   isBridgeConfiguration = bridgeNames != null;
   regionSnapshot = new HashMap();
   CQUtilBB.getBB().getSharedMap().put(CQUtilBB.RegionSnapshot, regionSnapshot); 

   versionHelperInstance = new CQTestVersionHelper(this);
}

/** Initialize (create) the continuous queries for this test
 */
public void initializeCQs() {
   CqAttributesFactory cqFac = new CqAttributesFactory();
   cqFac.addCqListener(new CQGatherListener());
   CqAttributes cqAttr = cqFac.create();
   Iterator it = queryMap.keySet().iterator();
   while (it.hasNext()) {
      String queryName = (String)(it.next());
      String query = (String)(queryMap.get(queryName));
      try {
         Log.getLogWriter().info("Creating CQ with name " + queryName + ": " + query + ", cq attributes: " + cqAttr);
         CqQuery cq = qService.newCq(queryName, query, cqAttr);
         Log.getLogWriter().info("Calling executeWithInitialResults on " + cq);
         // SelectResults sr = cq.executeWithInitialResults();
         SelectResults sr = new CQExecuteVersionHelper().executeWithInitialResults(cq);
         if (sr == null) {
            throw new TestException("For cq " + cq + " with name " + cq.getName() + " executeWithInitialResults returned " + sr);
         }
      } catch (CqExistsException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (RegionNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CqException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
}

// ========================================================================
// hydra task methods
    
/** Hydra task method for serial round robin tests.
 */
public static void HydraTask_doRROpsAndVerify() {
   PdxTest.initClassLoader();
   testInstance.doRROpsAndVerify();
}

public static void HydraTask_load() {
   for (int i = 1; i <= 10; i++) {
      testInstance.aRegion.put("Object_" + i, new Integer(i));
   }
}
    
/** Hydra task method for concurrent tests with verification.
 */
public static void HydraTask_doConcOpsAndVerify() {
   testInstance.doConcOpsAndVerify();
}

/** Hydra task method to execute all the registered CQs and verify
 *  results. 
 */
public static synchronized void HydraTask_executeCQsAndVerify() {
   if (CQsOn) {
      testInstance.verifyQueryResults();
   } else {
      Log.getLogWriter().info("Skipping verification of queries because CQUtilPrms.CQsOn is " + CQsOn);
   }
}

/** Hydra task method to do random entry operations.
 */
public static void HydraTask_HADoEntryOps() {
   PdxTest.initClassLoader();
   testInstance.HADoEntryOps();
}

/** Hydra task method to control VMs going down/up one at a time.
 */
public static void HydraTask_HAController() {
   testInstance.HAController();
}

/** Hydra task method to load the region with N entries, specified by
 *  CQUtilPrms.upperThreshold.
 */
public static void HydraTask_loadToUpperThreshold() {
   testInstance.loadToUpperThreshold();
}

/** Hydra task method to follow HydraTask_loadToUpperThreshold to
 *  get the current state of the region recorded to the BB for
 *  serial tests.
 */
public static void HydraTask_recordLoadToBB() {
   testInstance.recordLoadToBB();
}

// ========================================================================
// methods to generate random queries

/** Generate a series of queries based on the given parameters. This can only be
 *  called for numeric (int, long, float etc) fields.
 *
 *  @param fieldName The name of the field to query on.
 *  @param minValue Choose a random value between minValue and maxValue to query against.
 *  @param maxValue Choose a random value between minValue and maxValue to query against.
 *  @param pathLength Choose a random value between 1 and pathLength to generate the 
 *                    query path. For example, 1 might be "anInt = 1", 2 might be
 *                    "aQueryObject.anInt = 1", etc.
 *
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 * 
 */
protected Map generateNumericQueries(String fieldName, long minValue, long maxValue, int maxPathLength) {
    GsRandom rand = TestConfig.tab().getRandGen();
    Map queryMap = new HashMap();

    // queries that compare to 0
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < 0");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= 0");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > 0");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= 0");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = 0");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> 0");

    // queries that compare to a random number
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < " +
                 rand.nextLong(minValue, maxValue));
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= " +
                 rand.nextLong(minValue, maxValue));
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > " +
                 rand.nextLong(minValue, maxValue));
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= " +
                 rand.nextLong(minValue, maxValue));
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = " +
                 rand.nextLong(minValue, maxValue));
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> " +
                 rand.nextLong(minValue, maxValue));
    return queryMap;
}

/** Generate a series of queries based on the given parameters. This can only be
 *  called for String fields.
 *
 *  @param fieldName The name of the field to query on.
 *  @param pathLength Choose a random value between 1 and pathLength to generate the 
 *                    query path. For example, 1 might be "aString = "abc"", 2 might be
 *                    "aQueryObject.aString = "def"", etc.
 *
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 * 
 */
protected Map generateStringQueries(String fieldName, int maxPathLength) {
    GsRandom rand = TestConfig.tab().getRandGen();
    Map queryMap = new HashMap();
    RandomValues rv = new RandomValues();

    // queries that compare to "" (empty string)
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < ''");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= ''");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > ''");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= ''");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = ''");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> ''");

    // queries that compare to a random String
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> " +
                 "'" + rv.getRandom_String('\'', -1) + "'");
    return queryMap;
}

/** Generate a series of queries based on the given parameters. This can only be
 *  called for character fields.
 *
 *  @param fieldName The name of the field to query on.
 *  @param pathLength Choose a random value between 1 and pathLength to generate the 
 *                    query path. For example, 1 might be "aCharacter = "a"", 2 might be
 *                    "aQueryObject.aCharacter = "d"", etc.
 *
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 * 
 */
protected Map generateCharacterQueries(String fieldName, int maxPathLength) {
    GsRandom rand = TestConfig.tab().getRandGen();
    Map queryMap = new HashMap();
    RandomValues rv = new RandomValues();

    // queries that compare to a random Character
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    queryMap.put(getNextQueryName(),
                 "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> " +
                 "CHAR '" + rv.getRandom_String('\'', 1) + "'");
    return queryMap;
}

/** Generate a series of queries the query against border case values (min/max) based on the given 
 *  parameters. 
 *
 *  @param fieldName The name of the field to query on.
 *  @param pathLength Choose a random value between 1 and pathLength to generate the 
 *                    query path. For example, 1 might be "anInt = 1", 2 might be
 *                    "aQueryObject.anInt = 1", etc.
 *
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 * 
 */
protected Map generateBorderCaseQueries(int maxPathLength) {
    GsRandom rand = TestConfig.tab().getRandGen();
    Map queryMap = new HashMap();
    RandomValues rv = new RandomValues();
    String[] ops = new String[] {"=", "<", ">", "<=", ">=", "<>", "!="};
    for (int i = 0; i < ops.length; i++) {
       String op = ops[i];

       // long/Long
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveLong", maxPathLength) + 
                    " " + op + " L" +
                    rv.getRandom_longBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aLong", maxPathLength) + 
                    " " + op + " L" +
                    rv.getRandom_longBorderCase());
   
       // int/Integer
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveInt", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_intBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("anInteger", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_intBorderCase());
   
       // short/Short
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveShort", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_shortBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aShort", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_shortBorderCase());
   
       // float/Float
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveFloat", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_floatBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aFloat", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_floatBorderCase());
   
       // double/Double
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveDouble", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_doubleBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aDouble", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_doubleBorderCase());
   
       // byte/Byte
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveByte", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_byteBorderCase());
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aByte", maxPathLength) + 
                    " " + op + " " +
                    rv.getRandom_byteBorderCase());
   
       // char/Character
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aPrimitiveChar", maxPathLength) + 
                    " " + op + " CHAR '" +
                    rv.getRandom_charBorderCase() + "'");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aCharacter", maxPathLength) + 
                    " " + op + " CHAR '" +
                    rv.getRandom_charBorderCase() + "'");
   
       // String
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aString", maxPathLength) + 
                    " " + op + " '" +
                    rv.getRandom_StringBorderCase() + "'");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath("aString", maxPathLength) + 
                    " " + op + " '" +
                    rv.getRandom_StringBorderCase() + "'");
    } 
    return queryMap;
}

/** Generate a series of queries against null values based on the given parameters. 
 *
 *  @param pathLength Choose a random value between 1 and pathLength to generate the 
 *                    query path. For example, 1 might be "anInteger = NULL", 2 might be
 *                    "aQueryObject.anInteger = NULL", etc.
 *
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 * 
 */
protected Map generateNullQueries(int maxPathLength) {
    Map queryMap = new HashMap();

    String[] nonPrimFields = new String[] {"aLong", "anInteger", "aShort", "aFloat", "aDouble", 
             "aByte", "aCharacter", "aString", "aQueryObject"};
    for (int i = 0; i < nonPrimFields.length; i++) {
       String fieldName = (String)(nonPrimFields[i]);
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " < NULL");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <= NULL");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " > NULL");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " >= NULL");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " = NULL");
       queryMap.put(getNextQueryName(),
                    "select * from /testRegion where " + getQueryPath(fieldName, maxPathLength) + " <> NULL");
   }
   return queryMap;
}

/** Initialize the queries to be used in the test by putting them in the queryMap
 * 
 *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
 *           Every query has a different unique name.
 */
public Map generateQueries(int maxQueryDepth) {
   Map aMap = new HashMap();

   // numeric fields
   aMap.putAll(generateNumericQueries("aPrimitiveLong", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aLong", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aPrimitiveInt", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("anInteger", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aPrimitiveShort", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aShort", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aPrimitiveFloat", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aFloat", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aPrimitiveDouble", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aDouble", -500, 500, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aPrimitiveByte", Byte.MIN_VALUE, Byte.MAX_VALUE, maxQueryDepth));
   aMap.putAll(generateNumericQueries("aByte", Byte.MIN_VALUE, Byte.MAX_VALUE, maxQueryDepth));

   // String fields
   aMap.putAll(generateStringQueries("aString", maxQueryDepth));

   // character fields
   aMap.putAll(generateCharacterQueries("aCharacter", maxQueryDepth));
   aMap.putAll(generateCharacterQueries("aPrimitiveChar", maxQueryDepth));

// Don't allow border case queries because of bug 37119
// Border case (min/max) queries are now tested in simpleCQ.conf
//   aMap.putAll(generateBorderCaseQueries(maxQueryDepth));

   // the select all query 
   aMap.put(getNextQueryName(), "select * from /testRegion");

   // null queries
   aMap.put(getNextQueryName(), "select * from /testRegion entry where entry = NULL");
   aMap.putAll(generateNullQueries(maxQueryDepth));

   // select a random number of queries
   Map queryMap = aMap;
   if (numQueriesPerClientVM >= 0) {
      GsRandom rand = TestConfig.tab().getRandGen();
      List aList = new ArrayList(queryMap.keySet());
      queryMap = new HashMap();
      while ((queryMap.size() < numQueriesPerClientVM) && (aList.size() > 0)) { 
         int randInt = rand.nextInt(0, aList.size()-1);   
         Object key = aList.get(randInt);
         queryMap.put(key, aMap.get(key));
         aList.remove(randInt);
      }
      Log.getLogWriter().info("Randomly selected " + queryMap.size() + " queries, " + aList.size() + " queries were rejected");
   }

   // log the queries that were selected
   StringBuffer aStr = new StringBuffer();
   aStr.append("Selected " + queryMap.size() + " queries\n");
   Iterator it = queryMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      String value = (String)(queryMap.get(key));
      aStr.append("Query name: " + key + ", query: " + getReadableQueryString(value) + "\n");
   }
   Log.getLogWriter().info(aStr.toString());

   return queryMap;
}

/** Given a field name and maximum path depth, return a random path
 *  to the field.
 *
 *  @param fieldName The name of the field the path should end with.
 *  @param maxPathLength The maximum path length to return. 
 *  @return A query path ending with fieldName, between 1 and
 *          maxPathLength for the query path depth.
 */
protected String getQueryPath(String fieldName, int maxPathLength) {
   int pathLength = TestConfig.tab().getRandGen().nextInt(1, maxPathLength);
   String queryPath = fieldName;
   for (int i = 2; i <= pathLength; i++) {
      queryPath = "aQueryObject." + queryPath;
   }
   return queryPath;
}

// ========================================================================
// test methods

/** Do random operations and verification for serial round robin test.
 *  If this is the first task in the round, then do a random operation
 *  and record it to the blackboard. If this is not the first in the 
 *  round, then verify this client's view of the operation done by the
 *  first thread in the round. If this is the last thread in the round,
 *  then do the verification and become the new first thread in the round
 *  by doing a random operation. Thus, a different thread in each round
 *  will do the random entry opertion.
 *
 */
protected void doRROpsAndVerify() {
  try {
   logExecutionNumber();
   long roundPosition = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.RoundPosition);
   Log.getLogWriter().info("In doRROpsAndVerify, roundPosition is " + roundPosition);
   regionSnapshot = CQUtilBB.getSnapshot();
   if (roundPosition == numThreadsInClients) { // this is the last in the round
      Log.getLogWriter().info("In doRROpsAndVerify, last in round");
      verifyRegionContents();
      if (isBridgeClient) { // only clients have query results
         if (CQsOn) {
            verifyQueryResults();
         } else {
            Log.getLogWriter().info("Skipping verification of queries because CQUtilPrms.CQsOn is " + CQsOn);
         }
      } else {
         Log.getLogWriter().info("No queries to verify in bridge server, region size is " + aRegion.size());
      }

      // now become the first in the round 
      CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.RoundPosition);
      roundPosition = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.RoundPosition);
   }

   if (roundPosition == 1) { // first in round, do random ops
      long roundNumber = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.RoundNumber);
      Log.getLogWriter().info("In doRROpsAndVerify, first in round, round number " + roundNumber);
      // do ops while concurrently stopping vms
      List threads = null;
      List vmList = null;
      if (highAvailability) {
         int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
         Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "bridge");
         vmList = (List)(tmpArr[0]);
         List stopModeList = (List)(tmpArr[1]);
         threads = StopStartVMs.stopStartAsync(vmList, stopModeList);
                     // this asychronously starts the thread to stop and start
                     // meanwhile, we will do operations
      }
      doEntryOperations(aRegion);
      if (highAvailability) {
         StopStartVMs.joinStopStart(vmList, threads);
      }

      if (isBridgeConfiguration) { 
         // wait for 30 seconds of client silence to allow everything to be pushed to clients
         util.SilenceListener.waitForSilence(30, 5000);
      }

      // write the expected region state to the blackboard
      Log.getLogWriter().info("Writing regionSnapshot to blackboard, snapshot size is " + regionSnapshot.size() + ": " + regionSnapshot);
      CQUtilBB.putSnapshot(regionSnapshot);
   } else if (roundPosition != numThreadsInClients) { // neither first nor last
      regionSnapshot = CQUtilBB.getSnapshot();
      Log.getLogWriter().info("In doRROpsAndVerify, neither first nor last");
      verifyRegionContents();
      if (isBridgeClient) { // only clients have query results
         if (CQsOn) {
            verifyQueryResults();
         } else {
            Log.getLogWriter().info("Skipping verification of queries because CQUtilPrms.CQsOn is " + CQsOn);
         }
      } else {
         Log.getLogWriter().info("No queries to verify in bridge server");
      }
   }
  } finally {
    if (isBridgeClient) {
      ClientHelper.release(testInstance.aRegion);
    }
  }
}

/** Do random operations and verification for concurrent tests.
 *  The task starts up and all threads concurrently do random
 *  operations. The operations run for maxTaskGranularitySec or
 *  numOpsPerTask, depending the on the defined hydra parameters, 
 *  then all threads will pause. During the pause, one thread goes
 *  first and writes the regionSnapshot to the blackboard. Then
 *  all other client threads read the blackboard and verify the
 *  state of the cqs they have registered. After all threads are 
 *  done with verification, the task ends.
 */
protected void doConcOpsAndVerify() {
   PdxTest.initClassLoader();
   // wait for all threads to be ready to do this task, then do random ops
   long counter = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.ReadyToBegin);
   if (counter == 1) {
      logExecutionNumber();
   }
   concVerifyCoordinator = new MethodCoordinator(CQTest.class.getName(), "concVerify");
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ConcurrentLeader);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.ReadyToBegin", 
                             CQUtilBB.ReadyToBegin, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             1000);
   checkForLastIteration();

   Log.getLogWriter().info("Zeroing ShapshotWritten");
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.SnapshotWritten);

   // do random operations 
   doEntryOperations(aRegion);
    
   // versioning allows testing with ClientCacheFactory
   GemFireCache cache = versionHelperInstance.getCache();
   RebalanceFactory factory = cache.getResourceManager().createRebalanceFactory();
   RebalanceOperation rop = factory.start();
   RebalanceResults results = null;
   try {
      results = rop.getResults();
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }

   Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(results, "rebalance"));

   // wait for all threads to pause, then do the verify
   Log.getLogWriter().info("Zeroing FinishedVerify");
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.FinishedVerify);
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.Pausing);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.Pausing", 
                             CQUtilBB.Pausing, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   Log.getLogWriter().info("Zeroing ReadyToBegin");
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ReadyToBegin);

   // in a bridge configuration, wait for the message queues to finish pushing
   // to the clients
   if (isBridgeConfiguration) {
      // wait for 30 seconds of client silence
      SilenceListener.waitForSilence(30, 5000);
   }

   // do verification
   concVerifyCoordinator.executeOnce(this, new Object[0]);
   if (!concVerifyCoordinator.methodWasExecuted()) {
      throw new TestException("Test problem: concVerify did not execute");
   }
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.FinishedVerify);

   // wait for everybody to finish verify, then exit
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.FinishedVerify", 
                             CQUtilBB.FinishedVerify, 
                             numThreadsInClients,
                             true, 
                             -1,
                             5000);
   Log.getLogWriter().info("Zeroing Pausing");
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Pausing);

   counter = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num executions is " + 
            CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.ExecutionNumber));
}

/** While other threads are doing ops, stop or close the region of servers 
 *  (not clients), reinstate the servers/regions, pause and verify. This 
 *  method coordinates the use of blackboard counters with HADoEntryOps 
 *  for pausing.
 */
protected void HAController() {
   logExecutionNumber();
   concVerifyCoordinator = new MethodCoordinator(CQTest.class.getName(), "concVerify");
   checkForLastIteration();
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ExceptionCounter);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.ConcurrentLeader);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Reinitialized);

   // if this vm is a client, then stop/start some servers
   // if this vm is a server, then either stop/start other servers OR
   //    either close the cache or disconnect
   boolean stopServers = isBridgeClient ||
                         ((!isBridgeClient) && TestConfig.tab().getRandGen().nextInt(1,100) <= 60);
   if (stopServers) {  
      int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
      Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "bridge");
      List vmList = (List)(tmpArr[0]);
      List stopModeList = (List)(tmpArr[1]);
      StopStartVMs.stopStartVMs(vmList, stopModeList);
   } else { // either close the cache or disconnect
      if (TestConfig.tab().getRandGen().nextBoolean()) { // close the cache for this VM
         Log.getLogWriter().info("Closing the cache...");
         cacheIsClosed = true;
         // versioning allows testing with ClientCacheFactory
         versionHelperInstance.closeCache();
      } else { // disconnect
         Log.getLogWriter().info("Disconnecting from the distributed system...");
         disconnected = true;
         DistributedSystemHelper.disconnect();
      }
      // wait for all threads in this VM to get the appropriate exception
      TestHelper.waitForCounter(CQUtilBB.getBB(), 
                                "CQUtilBB.ExceptionCounter", 
                                CQUtilBB.ExceptionCounter, 
                                numThreadsInThisVM-1, 
                                true, 
                                -1,
                                1000);
      Log.getLogWriter().info("Recreating the region...");
      testInstance.aRegion = testInstance.initializeRegion("serverRegion");
      Log.getLogWriter().info("Done recreating the region...");
      BridgeHelper.startBridgeServer("bridge");
      CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.Reinitialized);
   }
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.SnapshotWritten);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.FinishedVerify);
   cacheIsClosed = false;
   disconnected = false;

   // now get all vms to pause for verification
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.Pausing);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.Pausing", 
                             CQUtilBB.Pausing, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.SyncUp);

   if (isBridgeConfiguration) { 
      // wait for 30 seconds of client silence to allow everything to be pushed to clients
      SilenceListener.waitForSilence(30, 5000);
   }
   PdxTest.initClassLoader();
   concVerifyCoordinator.executeOnce(this, new Object[0]);
   if (!concVerifyCoordinator.methodWasExecuted()) {
      throw new TestException("Test problem: concVerify did not execute");
   }

   // wait for everybody to finish verify
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.FinishedVerify);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.FinishedVerify", 
                             CQUtilBB.FinishedVerify, 
                             numThreadsInClients, 
                             true, 
                             -1,
                             5000);
   CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Pausing);

   // sync up with the HADoEntryOps threads; this will avoid a new
   // HAController task getting scheduled before all the HADoEntryOPs
   // threads have zeroed any counters after the verify
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.SyncUp);
   TestHelper.waitForCounter(CQUtilBB.getBB(),
                             "CQUtilBB.SyncUp",
                             CQUtilBB.SyncUp,
                             numThreadsInClients,
                             true,
                             -1,
                             1000);

   // see if it's time to stop the test
   long counter = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.TimeToStop);
   if (counter >= 1)
      throw new StopSchedulingOrder("Num HAController executions is " + 
            CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.ExecutionNumber));
}

/** Do entry ops and handle disconnects or cache closes. This coordinates counters
 *  with HAController to allow for verification.
 */
protected void HADoEntryOps() {
   checkForLastIteration();
   // disconnect can cause cacheClosedException if the thread is accessing the cache
   try {
      testInstance.doEntryOperations(testInstance.aRegion);
      if (CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.Pausing) > 0) { // we are pausing
         CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.Pausing);
         TestHelper.waitForCounter(CQUtilBB.getBB(), 
                                   "CQUtilBB.Pausing", 
                                   CQUtilBB.Pausing, 
                                   numThreadsInClients, 
                                   true, 
                                   -1,
                                   5000);
         if (isBridgeConfiguration) { 
            // wait for 30 seconds of client silence to allow everything to be pushed to clients
            SilenceListener.waitForSilence(30, 5000);
         }
         concVerifyCoordinator.executeOnce(this, new Object[0]);
         if (!concVerifyCoordinator.methodWasExecuted()) {
            throw new TestException("Test problem: concVerify did not execute");
         }

         // wait for everybody to finish verify
         CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.FinishedVerify);
         TestHelper.waitForCounter(CQUtilBB.getBB(), 
                                   "CQUtilBB.FinishedVerify", 
                                   CQUtilBB.FinishedVerify, 
                                   numThreadsInClients, 
                                   true, 
                                   -1,
                                   5000);
         CQUtilBB.getBB().getSharedCounters().zero(CQUtilBB.Pausing);
         concVerifyCoordinator = new MethodCoordinator(CQTest.class.getName(), "concVerify");

         // sync up with the HAController thread; this will ensure that
         // the above zeroing of counters doesn't happen before a new
         // invocation of HAController
         CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.SyncUp);
         TestHelper.waitForCounter(CQUtilBB.getBB(),
                                   "CQUtilBB.SyncUp",
                                   CQUtilBB.SyncUp,
                                   numThreadsInClients,
                                   true,
                                   -1,
                                   1000);
      }
   } catch (Exception e) {
      handleException(e);
   }
}

/** Do random entry operations on the given region ending either with
 *  minTaskGranularityMS or numOpsPerTask.
 *  Uses CQUtilPrms.entryOperations to determine the operations to execute.
 */
protected void doEntryOperations(Region aRegion) {
  versionHelperInstance.doEntryOperations(aRegion);
}

protected void doRandomOp(Region aRegion) {

   int whichOp = getOperation(aRegion);

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
      case ENTRY_MODIFY_VALUE:
         modifyValue(aRegion);
         break;
      default: {
         throw new TestException("Unknown operation " + whichOp);
      }
   }
}

/** Get an operation to perform on the region
 *  @param reg The region to get an operation for. 
 *  @returns An operation.
 */
protected int getOperation(Region reg) {
  int op = -1;
  int size = aRegion.size();
  if (isBridgeConfiguration) {
    if (isBridgeClient) {
      if (size >= upperThreshold) {
        op = getOperation(CQUtilPrms.upperThresholdClientOperations);
      } else if (size <= lowerThreshold) {
        op = getOperation(CQUtilPrms.lowerThresholdClientOperations);
      } else {
        op = getOperation(CQUtilPrms.clientEntryOperations);
      }
    } else { // is bridge server
      if (size >= upperThreshold) {
        op = getOperation(CQUtilPrms.upperThresholdServerOperations);
      } else if (size <= lowerThreshold) {
        op = getOperation(CQUtilPrms.lowerThresholdServerOperations);
      } else {
        op = getOperation(CQUtilPrms.serverEntryOperations);
      }
    }
  } else { // for peer tests
    if (size >= upperThreshold) {
      op = getOperation(CQUtilPrms.upperThresholdOperations);
    } else if (size <= lowerThreshold) {
      op = getOperation(CQUtilPrms.lowerThresholdOperations);
    } else {
      op = getOperation(CQUtilPrms.entryOperations);
    }
  }
  return op;
}

/** Load the region with N entries, specified by CQUtilPrms.upperThreshold.
 *
 */
protected void loadToUpperThreshold() {
   int logIntervalMS = 10000;
   long startTime = System.currentTimeMillis();
   long lastLogTime = System.currentTimeMillis();
   int upperThreshold = TestConfig.tab().intAt(CQUtilPrms.upperThreshold);
   Log.getLogWriter().info("Loading region to size " + upperThreshold + 
          ", current region size is " + aRegion.size());
   while (aRegion.size() < upperThreshold) {
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMS) {
         Log.getLogWriter().info("Loading region to size " + upperThreshold + 
             ", current region size is " + aRegion.size());
         lastLogTime = System.currentTimeMillis();
      }
      addEntry(aRegion);
      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {
         break;
      }
   }
   if (aRegion.size() >= upperThreshold) {
      String aStr = "Finished loadToUpperThreshold, " + aRegion.getFullPath() +
          " is size " + aRegion.size();
      Log.getLogWriter().info(aStr);
      throw new StopSchedulingTaskOnClientOrder(aStr);
   }
}

/** Record the state of the region to the blackbloard for serial tests.
 *
 */
protected void recordLoadToBB() {
   long concurrentLeader = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.ConcurrentLeader);
   if (concurrentLeader == 1) {
      Log.getLogWriter().info("Recording region to blackbloard...");
      regionSnapshot = new HashMap();
      Iterator it = aRegion.keys().iterator();
      while (it.hasNext()) {
         Object key = it.next();
         Object value = aRegion.get(key);
         regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(value));      
      }
      Log.getLogWriter().info("Writing regionSnapshot to blackboard, snapshot size is " + regionSnapshot.size() + ": " + regionSnapshot);
      CQUtilBB.putSnapshot(regionSnapshot); 
   }
}

/** Add a new entry to the given region.
 *
 *  @param aRegion The region to use for adding a new entry.
 *
 *  @returns The key that was added.
 */
protected Object addEntry(Region aRegion) {
   Object key = getNewKey();
   QueryObject anObj = null;
   int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
   // todo@lhughes -- for 6.6 we cannot have 2 ops (get/create in the same tx or we will 
   // make the wrong assumption about which server hosts the primary.
   // remove this (if !useTransactions) after 6.6
   boolean useTransactions = getInitialImage.InitImagePrms.useTransactions();
   if (!useTransactions && (randInt <= 20)) { // put a duplicate value
      Object existingKey = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
      if (existingKey != null) {
         anObj = toQueryObject(aRegion.get(existingKey));
         if (anObj != null) {
            Log.getLogWriter().info("Object to put is from existing key " + existingKey + ": " + anObj.toStringFull());
            try {
               anObj = (QueryObject)(anObj.clone());
               if (anObj instanceof DeltaObject) {
                 ((DeltaObject)anObj).extra = key;
               }
            } catch (CloneNotSupportedException e) {
               throw new TestException(TestHelper.getStackTrace(e));
            }
         }
      }
   }
   if (anObj == null) {
      anObj = getValueForKey(key);
   }
   String callback = createCallbackPrefix + ProcessMgr.getProcessId();
   int beforeSize = aRegion.size();
   randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
   if (isOldClient) { // cannot use putIfAbsent, not supported in this older client
     randInt = TestConfig.tab().getRandGen().nextInt(1, 66); // eliminates putIfAbsent
   }
   if (randInt <= 33) { // use a create call
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
         try {
            Log.getLogWriter().info("operation for " + key + ", addEntry: calling create for key " + key + ", object " +
               anObj.toStringFull() + " cacheWriterParam is " + callback + ", region is " + 
               aRegion.getFullPath());
            aRegion.create(key, anObj, callback);
            Log.getLogWriter().info("operation for " + key + ", addEntry: done creating key " + key);
         } catch (EntryExistsException e) {
            if (isSerialExecution) { 
               // cannot get this exception; nobody else can have this key
               throw new TestException(TestHelper.getStackTrace(e));
            } else {
               Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
               // in concurrent execution, somebody could have updated this key causing it to exist
            }
         }
      } else { // use create with no cacheWriter arg
         try {
            Log.getLogWriter().info("operation for " + key + ", addEntry: calling create for key " + key + ", object " +
               anObj.toStringFull() + ", region is " + aRegion.getFullPath());
            aRegion.create(key, anObj);
            Log.getLogWriter().info("operation for " + key + ", addEntry: done creating key " + key);
         } catch (EntryExistsException e) {
            if (isSerialExecution) { 
               // cannot get this exception; nobody else can have this key
               throw new TestException(TestHelper.getStackTrace(e));
            } else {
               Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
               // in concurrent execution, somebody could have updated this key causing it to exist
            }
         }
      }
   } else if (randInt <= 66) { // use a put call
      Object returnVal = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
         Log.getLogWriter().info("operation for " + key + ", addEntry: calling put for key " + key + ", object " +
               anObj.toStringFull() + " callback is " + callback + ", region is " + aRegion.getFullPath());
         returnVal = aRegion.put(key, anObj, callback);
         Log.getLogWriter().info("operation for " + key + ", addEntry: done putting key " + key + ", returnVal is " + returnVal);
      } else {
         Log.getLogWriter().info("operation for " + key + ", addEntry: calling put for key " + key + ", object " +
               anObj.toStringFull() + ", region is " + aRegion.getFullPath());
         returnVal = aRegion.put(key, anObj);
         Log.getLogWriter().info("operation for " + key + ", addEntry: done putting key " + key + ", returnVal is " + returnVal);
      }
   } else { // use putIfAbsent
     Log.getLogWriter().info("operation for " + key + ", addEntry: calling putIfAbsent for key " + key + ", object " +
         anObj.toStringFull()  + ", region is " + aRegion.getFullPath());
      Object returnValue = aRegion.putIfAbsent(key, anObj);
      Log.getLogWriter().info("operation for " + key + ", addEntry: done creating key with putIfAbsent" + key
          + ", return value is " + returnValue);

   }

   // validation
   if (isSerialExecution) {
      // record the current state
      regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(anObj));
   }
   return key;
}

/** Get a value for the given key
 * @param key The key to be used for this value
 * @return A new entry value.
 */
private QueryObject getValueForKey(Object key) {
  QueryObject anObj = null;
  if (TestConfig.tab().getRandGen().nextBoolean()) { // sequential values
     if (useDeltaObjects) {
        anObj = new DeltaObject(
           NameFactory.getCounterForName(key), QueryObject.SEQUENTIAL_VALUES, 
           0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        anObj.extra = key;
     } else {
        String className = TestConfig.tab().stringAt(CQUtilPrms.objectType, null);
        if (className == null) { // use QueryObject
           anObj = new QueryObject(
              NameFactory.getCounterForName(key), QueryObject.SEQUENTIAL_VALUES, 
              0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        } else if (className.equals("util.PdxVersionedQueryObject") ||
                   className.equals("util.VersionedQueryObject")) {
           anObj = PdxTest.getVersionedQueryObject(className, NameFactory.getCounterForName(key), 
               QueryObject.SEQUENTIAL_VALUES, 0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        } else {
           throw new TestException("Unknown value of CQUtilPrms.objectType: " + className);
        }
     }
  } else { // random values
     if (useDeltaObjects) {
        anObj = new DeltaObject(
           NameFactory.getCounterForName(key), QueryObject.RANDOM_VALUES, 
           0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        anObj.extra = key;
     } else {
        String className = TestConfig.tab().stringAt(CQUtilPrms.objectType, null);
        if (className == null) { // use QueryObject
           anObj = new QueryObject(
              NameFactory.getCounterForName(key), QueryObject.RANDOM_VALUES, 
              0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        } else if (className.equals("util.PdxVersionedQueryObject") ||
                   className.equals("util.VersionedQueryObject")) {
           anObj = PdxTest.getVersionedQueryObject(className, NameFactory.getCounterForName(key),
               QueryObject.RANDOM_VALUES, 
               0, TestConfig.tab().getRandGen().nextInt(1, queryDepth));
        } else {
           throw new TestException("Unknown value of CQUtilPrms.objectType: " + className);
        }
     }
  }
  return anObj;
}
    
/** Invalidate an entry in the given region.
 *
 *  @param aRegion The region to use for invalidating an entry.
 *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
 */
protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
  Object key = null;
  if (!isOldClient && TestConfig.tab().getRandGen().nextInt(1, 100) <= 50) { // do a putIfAbsent with null
    key = getNewKey();
    Log.getLogWriter().info("operation for " + key + ", invalidateEntry: putIfAbsent with null");
    Object returnValue = aRegion.putIfAbsent(key, null);
    Log.getLogWriter().info("operation for " + key + ", invalidateEntry: done with putIfAbsent with null, return value is " +
        TestHelper.toString(returnValue));
  } else {
   int beforeSize = aRegion.size();
    key = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
   if (key == null) {
      if (isSerialExecution && (beforeSize != 0))
         throw new TestException("getExistingKey returned " + key + ", but region size is " + beforeSize);
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
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: local invalidate for " + key + " callback is " + callback);
            aRegion.localInvalidate(key, callback);
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: done with local invalidate for " + key);
         } else { // local invalidate without callback
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: local invalidate for " + key);
            aRegion.localInvalidate(key);
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: done with local invalidate for " + key);
         }
      } else { // do a distributed invalidate
         if (TestConfig.tab().getRandGen().nextBoolean()) { // invalidate with callback
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: invalidating key " + key + " callback is " + callback);
            aRegion.invalidate(key, callback);
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: done invalidating key " + key);
         } else { // invalidate without callback
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: invalidating key " + key);
            aRegion.invalidate(key);
            Log.getLogWriter().info("operation for " + key + ", invalidateEntry: done invalidating key " + key);
         }
      }

   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      else {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
  }
  if (isSerialExecution) {
    // record the current state
    regionSnapshot.put(key, null);
  }

}
    
/** Destroy an entry in the given region.
 *
 *  @param aRegion The region to use for destroying an entry.
 *  @param isLocalDestroy True if the destroy should be local, false otherwise.
 */
protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
   Object key = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
   if (key == null) {
      int size = aRegion.size();
      if (isSerialExecution && (size != 0))
         throw new TestException("getExistingKey returned " + key + ", but region size is " + size);
      Log.getLogWriter().info("destroyEntry: No keys in region");
      return;
   }
  if (!isOldClient && TestConfig.tab().getRandGen().nextInt(1, 100) <= 50) { // do a remove
    Object value = aRegion.get(key);
    Log.getLogWriter().info("operation for " + key + ", destroyEntry: remove with value " + TestHelper.toString(value));
    boolean returnValue = false;
    try {
      returnValue = aRegion.remove(key, value);
    } catch (RuntimeException e) {
      PdxTestVersionHelper.handleException(e);
      // if we get here then we allowed the exception
      return;
    }
    if (isSerialExecution && (returnValue == false)) {
      throw new TestException("aRegion.remove(" + key + ", " + value + ") returned " + returnValue + ".  Expected to be successful in serialExecution mode.");
    }
    Log.getLogWriter().info("operation for " + key + ", destroyEntry: done with remove, return value is " + returnValue);
  } else { // do a destroy
   try {
      String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
      if (isLocalDestroy) { // do a local destroy
         if (TestConfig.tab().getRandGen().nextBoolean()) { // local destroy with callback
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: local destroy for " + key + " callback is " + callback);
            aRegion.localDestroy(key, callback);
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: done with local destroy for " + key);
         } else { // local destroy without callback
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: local destroy for " + key);
            aRegion.localDestroy(key);
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: done with local destroy for " + key);
         }
      } else { // do a distributed destroy
         if (TestConfig.tab().getRandGen().nextBoolean()) { // destroy with callback
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: destroying key " + key + " callback is " + callback);
            aRegion.destroy(key, callback);
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: done destroying key " + key);
         } else { // destroy without callback
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: destroying key " + key);
            aRegion.destroy(key);
            Log.getLogWriter().info("operation for " + key + ", destroyEntry: done destroying key " + key);
         }
      }

   } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isSerialExecution)
         throw new TestException(TestHelper.getStackTrace(e));
      else {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
  }
  if (isSerialExecution) {
    // record the current state
    regionSnapshot.remove(key);
  }

}
    
/** Update an existing entry in the given region. If there are
 *  no available keys in the region, then this is a noop.
 *
 *  @param aRegion The region to use for updating an entry.
 */
protected void updateEntry(Region aRegion) {
   Object key = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
   if (key == null) {
      int size = aRegion.size();
      if (isSerialExecution && (size != 0))
         throw new TestException("getExistingKey returned " + key + ", but region size is " + size);
      Log.getLogWriter().info("updateEntry: No keys in region");
      return;
   }
   int beforeSize = aRegion.size();
   QueryObject previousObj = toQueryObject(aRegion.get(key));
   QueryObject anObj = null;
   if (previousObj == null) { // entry was previously invalidated
      anObj = getValueForKey(key);
   } else {
      int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
      if (randInt <= 33) {
         anObj = previousObj.modifyWithNewInstance(previousObj.depth(), QueryObject.INCREMENT, 1, true, true);
      } else if (randInt <= 66) {
         anObj = previousObj.modifyWithNewInstance(previousObj.depth(), QueryObject.NEGATE, 0, true, true);
      } else {
         anObj = previousObj.modifyWithNewInstance(previousObj.depth(), QueryObject.NULL_NONPRIM_FIELDS, 0, true, true);
      }
   }
   String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
   Object returnVal = null;
   int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
   if (!isOldClient && (randInt <= 50)) { // update with replace
     randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
     if (randInt <= 50) { // replace(K,V)
       Log.getLogWriter().info("operation for " + key + ", updateEntry: replace(K,V) key " + key + " value " +
           anObj.toStringFull());
       QueryObject returnValue = toQueryObject(aRegion.replace(key, anObj));
       Log.getLogWriter().info("operation for " + key + ", updateEntry: done with replace(K,V) key " + key + " value " +
           anObj.toStringFull() + ", return value is " + 
           ((returnValue == null) ? "null" : returnValue.toStringFull()));
     } else { // replace(K,V,V)
       QueryObject oldValue = toQueryObject(aRegion.get(key));
       Log.getLogWriter().info("operation for " + key + ", updateEntry: replace(K,V,V) key " + key + " old value " +
           ((oldValue == null) ? "null" : oldValue.toStringFull()) + 
           ", new value " + anObj.toStringFull());
       boolean returnValue = false;
       try {
         returnValue = aRegion.replace(key, oldValue, anObj);
       } catch (RuntimeException e) {
         PdxTestVersionHelper.handleException(e);
         // if we get here then we allowed the exception
         return;
       }
       Log.getLogWriter().info("operation for " + key + ", updateEntry: done with replace(K,V,V) key " + key + 
           " old value " + ((oldValue == null) ? "null" : oldValue.toStringFull()) 
           + ", new value " + anObj.toStringFull() + ", return value is " + returnValue);
       if (isSerialExecution && !returnValue) {
         throw new TestException("Expected replace(K,V,V) to return true, but it returned " + returnValue);
       }
     }
   } else { // update with put
   if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
       Log.getLogWriter().info("operation for " + key + ", updateEntry: putting key " + key + " with " +
         anObj.toStringFull() + ", callback is " + callback);
      returnVal = aRegion.put(key, anObj, callback);
      Log.getLogWriter().info("operation for " + key + ", updateEntry: Done with call to put (update), returnVal is " + returnVal);
   } else { // do a put without callback
       Log.getLogWriter().info("operation for " + key + ", updateEntry: putting key " + key + " with " + anObj.toStringFull());
      returnVal = aRegion.put(key, anObj);
      Log.getLogWriter().info("operation for " + key + ", updateEntry: Done with call to put (update), returnVal is " + returnVal);
   }
   }

   // validation
   if (isSerialExecution) {
      // record the current state
      regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(anObj));
   }
}
    
/** Modifies an existing value in an entry in the given region, but does not
 *  do a put. 
 *
 *  @param aRegion The region to use for updating an entry.
 */
protected void modifyValue(Region aRegion) {
   Object key = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
   if (key == null) {
      int size = aRegion.size();
      if (isSerialExecution && (size != 0))
         throw new TestException("getExistingKey returned " + key + ", but region size is " + size);
      Log.getLogWriter().info("modifyEntry: No keys in region");
      return;
   }
   QueryObject anObj = toQueryObject(aRegion.get(key));
   Log.getLogWriter().info("operation for " + key + ", modifyEntry: Modifying " + anObj + "  directly");
   if (TestConfig.tab().getRandGen().nextBoolean()) {
      anObj.modify(anObj.depth(), QueryObject.INCREMENT, 1, true, true);
   } else {
      anObj.modify(anObj.depth(), QueryObject.NEGATE, 0, true, true);
   }

   // validation
   if (isSerialExecution) {
      // record the current state
      regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(anObj));
   }
}
    
/** Get an existing key in the given region if one is available,
 *  otherwise get a new key. 
 *
 *  @param aRegion The region to use for getting an entry.
 */
protected void getKey(Region aRegion) {
   Object key = getExistingKey(aRegion, uniqueKeys, numThreadsInClients);
   if (key == null) { // no existing keys; get a new key then
      int size = aRegion.size();
      if (isSerialExecution && (size != 0))
         throw new TestException("getExistingKey returned " + key + ", but region size is " + size);
      getNewKey(aRegion);
      return;
   }
   String callback = getCallbackPrefix + ProcessMgr.getProcessId();
   int beforeSize = aRegion.size();
   boolean beforeContainsValueForKey = aRegion.containsValueForKey(key);
   Object anObj;
   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("operation for " + key + ", getKey: getting key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
         Log.getLogWriter().info("operation for " + key + ", getKey: got value for key " + key + ": " + ((anObj == null) ? "null" : toQueryObject(anObj).toStringFull()));
      } else { // get without callback
         Log.getLogWriter().info("operation for " + key + ", getKey: getting key " + key);
         anObj = aRegion.get(key);
         Log.getLogWriter().info("operation for " + key + ", getKey: got value for key " + key + ": " + ((anObj == null) ? "null" : toQueryObject(anObj).toStringFull()));
      }

      // validation 
      if (isSerialExecution) { 
         // record the current state
         // in case the get works like a put because there is a cacheLoader
         if (anObj == null)
            regionSnapshot.put(key, null);
         else
            regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(anObj));

      }
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
/** Get a new key in the given region.
 *
 *  @param aRegion The region to use for getting an entry.
 */
protected void getNewKey(Region aRegion) {
   Object key = getNewKey();
   String callback = getCallbackPrefix + ProcessMgr.getProcessId();
   Object anObj;
   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("operation for " + key + ", getNewKey: getting new key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
      } else { // get without callback
         Log.getLogWriter().info("operation for " + key + ", getNewKey: getting new key " + key);
         anObj = aRegion.get(key);
      }
      if (anObj == null) {
         Log.getLogWriter().info("operation for " + key + ", getNewKey: done getting value for new key " + key + ": " + anObj);
      } else {
         Log.getLogWriter().info("operation for " + key + ", getNewKey: done getting value for new key " + key + ": " + toQueryObject(anObj).toStringFull());
      }

      // validation 
      if (isSerialExecution) { 
         // record the current state in case the get works like a put because there is a cacheLoader
         if (anObj == null)
            regionSnapshot.put(key, null);
         else
            regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(anObj));
      }
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
/** Return a new key, never before used in the test.
 */
protected Object getNewKey() {
   if (uniqueKeys) {
      int anInt = ((Integer)(uniqueKeyIndex.get())).intValue(); 
      anInt += numThreadsInClients;
      uniqueKeyIndex.set(new Integer(anInt));
      return NameFactory.getObjectNameForCounter(anInt);
   } else {
      return NameFactory.getNextPositiveObjectName();
   }
}
    
/** Log the execution number of this serial task.
 */
static protected void logExecutionNumber() {
   long exeNum = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}
    
/** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
 *  which is equivalent to a full GII.
 *
 *  @param aRegion The region to register interest on.
 */
protected static void registerInterest(Region aRegion) {
   Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES");
   aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
   Log.getLogWriter().info("Done calling registerInterest for all keys, " +
       "result interest policy KEYS_VALUES, " + aRegion.getFullPath() +
       " size is " + aRegion.size());
}

// ========================================================================
// verification methods

/**  Create a region snapshot from one vm's point of view, while any other
 *   threads in this or the same the vm also executing this method will 
 *   compare to that snapshot. 
 */
protected void concVerify() {
   if (CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.ConcurrentLeader) == 1) {
      Log.getLogWriter().info("In concVerify, this thread is the concurrent leader");

      // this is the first thread to verify; all other threads will wait for this thread to
      // write its view of the local region to the blackboard and they will read it and
      // match it
      regionSnapshot = new HashMap();
      Log.getLogWriter().info("This thread is the concurrentLeader, creating region snapshot..."); 
      Set keySet = aRegion.keySet();
      Iterator it = keySet.iterator();
      while (it.hasNext()) {
         Object key = it.next();
         Object value = null;
         if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
            value = aRegion.get(key);
         }
         regionSnapshot.put(key, PdxTestVersionHelper.toBaseObject(value));
      }
      Log.getLogWriter().info("Done creating region snapshot with " + regionSnapshot.size() + 
          " entries; " + QueryObject.toStringAbbreviated(regionSnapshot));
      CQUtilBB.putSnapshot(regionSnapshot);
      long snapshotWritten = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.SnapshotWritten);
      Log.getLogWriter().info("Incremented SnapshotWritten, now is " + snapshotWritten);
   } else { 
      Log.getLogWriter().info("In concVerify, this thread is waiting for the concurrent leader to write the snapshot");
      // this thread is not the first to verify; it will wait until the first thread has
      // written its state to the blackboard, then it will read it and verify that its state matches
      TestHelper.waitForCounter(CQUtilBB.getBB(), 
                                "CQUtilBB.SnapshotWritten", 
                                CQUtilBB.SnapshotWritten, 
                                1, 
                                true, 
                                -1,
                                2000);
      regionSnapshot = CQUtilBB.getSnapshot();
      verifyRegionContents();
      if (isBridgeClient) { // only clients have query results
         if (CQsOn) {
            verifyQueryResults();
         } else {
            Log.getLogWriter().info("Skipping verification of queries because CQUtilPrms.CQsOn is " + CQsOn);
         }
      } else {
         Log.getLogWriter().info("No queries to verify in bridge server, region size is " + aRegion.size());
      }
   }
}

/** Verifies that for a given CQ, the following are all consistent with each other:
 *   1) expected results derived from the regionSnapshot
 *   2) selectResults
 *   3) gatherMap
 *
 *  @param cq The cq to check results for.
 *  @param sr The SelectResults for the cq.
 */
protected void verify(CqQuery cq, SelectResults sr) {
   String cqName = cq.getName();
   String queryStr = cq.getQueryString();
   String readableQueryStr = getReadableQueryString(queryStr);
   List expectedResults = getExpectedResults(queryStr);
   List srList = sr.asList();
   int srListSize = srList.size();
   int srSize = sr.size();
   Log.getLogWriter().info("Verifying query results for " + cqName + ": " + readableQueryStr + 
       ", SelectResults is size " + sr.size());
   if (srListSize != srSize) {
      throw new TestException("selectResults.size() is " + srSize + ", but selectResults.asList.size() is " + srListSize);
   }

   // remove null values from a copy of the gather map, because queries are on values only 
   Map gatherMap = new HashMap(CQGatherListener.getGatherMap(cqName));
//   Log.getLogWriter().info("In verify, gather map for " + cqName +
//       " is size " + gatherMap.size() + ", now removing entries with null values...");
   Iterator it = gatherMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      if (gatherMap.get(key) == null) {
         it.remove();
      }
   }
//   Log.getLogWriter().info("After removing entries with null values, gather map for " +
//       cqName + " is size " + gatherMap.size());

   // get inconsistencies
   //Log.getLogWriter().info("### DEBUG : expectedResults :" + 
   //  expectedResults);
   //Log.getLogWriter().info("### DEBUG : srList :" + 
   //  srList);

   Object[] tmp = getInconsistencies(expectedResults, srList);
   List unexpectedInSelectResults = (List)(tmp[0]);
   List missingInSelectResults = (List)(tmp[1]);
   tmp = getInconsistencies(expectedResults, new ArrayList(gatherMap.values()));
   List unexpectedInGatherMap = (List)(tmp[0]);
   List missingInGatherMap = (List)(tmp[1]);

   //Log.getLogWriter().info("### DEBUG : unexpectedInSelectResults :" + 
   //  unexpectedInSelectResults);
   //Log.getLogWriter().info("### DEBUG : missingInSelectResults :" + 
   //  missingInSelectResults);
   //Log.getLogWriter().info("### DEBUG : unexpectedInGatherMap :" + 
   //  unexpectedInGatherMap);
   //Log.getLogWriter().info("### DEBUG : missingInGatherMap :" + 
   //  missingInGatherMap);

   // prepare error Strings
   StringBuffer aStr = new StringBuffer();
   if (unexpectedInSelectResults.size() > 0) {
      String tmpStr = getLocationString(unexpectedInSelectResults, expectedResults, sr, gatherMap) + "\n" +
             "Found the following " + unexpectedInSelectResults.size() + 
             " unexpected elements in SelectResults for cq " + cqName + ", " +
             readableQueryStr + ": " + QueryObject.toStringFull(unexpectedInSelectResults);
      Log.getLogWriter().info(tmpStr);
      aStr.append(tmpStr);
   }
   if (missingInSelectResults.size() > 0) {
      String tmpStr = getLocationString(missingInSelectResults, expectedResults, sr, gatherMap) + "\n" +
             "The following " + missingInSelectResults.size() + 
             " elements were missing from SelectResults for cq " + cqName + ", " +
             readableQueryStr + ": " + QueryObject.toStringFull(missingInSelectResults);
      Log.getLogWriter().info(tmpStr);
      aStr.append(tmpStr);
   }
   if (unexpectedInGatherMap.size() > 0) {
      String tmpStr = getLocationString(unexpectedInGatherMap, expectedResults, sr, gatherMap) + "\n" +
             "Found the following " + unexpectedInGatherMap.size() + 
             " unexpected elements in gatherMap for cq " + cqName + ", " +
             readableQueryStr + ": " + QueryObject.toStringFull(unexpectedInGatherMap);
      Log.getLogWriter().info(tmpStr);
      aStr.append(tmpStr);
   }
   if (missingInGatherMap.size() > 0) {
      String tmpStr = getLocationString(missingInGatherMap, expectedResults, sr, gatherMap) + "\n" +
             "The following " + missingInGatherMap.size() + 
             " elements were missing from gatherMap for cq " + cqName + ", " +
             readableQueryStr + ": " + QueryObject.toStringFull(missingInGatherMap);
      Log.getLogWriter().info(tmpStr);
      aStr.append(tmpStr);
   }

   // verify selectResults as a Set (only in tests where the sr size is reasonable
   final int SR_sizeLimitForVerify = 600;
   if (srListSize <= SR_sizeLimitForVerify) {
      List srAsSet_inListForm = new ArrayList(sr.asSet());
      tmp = getInconsistencies(expectedResults, srAsSet_inListForm);
      List unexpectedInSet = (List)(tmp[0]);
      List missingInSet = (List)(tmp[1]);
      if (unexpectedInSet.size() > 0) {
         String tmpStr = getLocationString(unexpectedInSet, expectedResults, sr, gatherMap) + "\n" +
                "Found the following " + unexpectedInSet.size() + 
                " unexpected elements in SelectResults.asSet() for cq " + cqName + ", " +
                readableQueryStr + ": " + QueryObject.toStringFull(unexpectedInSet);
         Log.getLogWriter().info(tmpStr);
         aStr.append(tmpStr);
      }
      if (missingInSet.size() > 0) {
         String tmpStr = getLocationString(missingInSet, expectedResults, sr, gatherMap) + "\n" +
                "The following " + missingInSet.size() + 
                " elements were missing from SelectResults.asSet for cq " + cqName + ", " +
                readableQueryStr + ": " + QueryObject.toStringFull(missingInSet);
         Log.getLogWriter().info(tmpStr);
         aStr.append(tmpStr);
      }
   } else {
      Log.getLogWriter().info("Skipping validation of SelectResults as a Set because the size is " + srListSize);
   }
   
   if (aStr.length() > 0) {
      throw new TestException("For cq " + cqName + ", " + readableQueryStr + "\n" + aStr.toString());
   }
   Log.getLogWriter().info("Done verifying query results for " + cqName);
}

/** Given a List of expected objects, and a list to check, return
 *  unexpected and missing elements from the list to check.
 *
 *  @param expected A List of objects expected to be in listToCheck.
 *  @param listToCheck The List to check against expected.
 *  @returns [0] a List of objects that were unexpected in listToCheck
 *           [1] a List of objects that were missing in listToCheck
 */
static private Object[] getInconsistencies(List expected, List listToCheck) {
   List aList = new ArrayList();
   for (Object element: listToCheck) {
     aList.add(PdxTestVersionHelper.toBaseObject(element));
   }
   // find unexpected and missing results
   List unexpected = new ArrayList(aList); 
   List missing = new ArrayList(expected); 
   unexpected.removeAll(expected);
   missing.removeAll(aList);
   return new Object[] {unexpected, missing};
}

/** Given a List of QueryObjects known to be inconsistent as determined by validation,
 *  log where the suspect objects are found by checking for it in 
 *      1) the expected List
 *      2) the query resultsSet
 *      3) the gatherMap for the query
 *      4) the regionSnapshot
 *      5) the localRegion
 *
 *  @param inconsistencies A List of suspect QueryObjects to check in each location.
 *  @param expected The expected List of objects for a query.
 *  @param selResults The select results for a query.
 *  @param gatherMap The gatherMap for a query.
 *
 *  @returns 
 */
private String getLocationString(List inconsistencies, 
                                 List expected, 
                                 SelectResults selResults, 
                                 Map gatherMap) {
   StringBuffer aStr = new StringBuffer();
   for (int i = 0; i < inconsistencies.size(); i++) {
      QueryObject suspect = toQueryObject(inconsistencies.get(i));
      
      // check the local region
      boolean found = false;
      Iterator it = aRegion.keySet().iterator();
      while (it.hasNext()) {
         Object key = it.next();
         Region.Entry entry = aRegion.getEntry(key);
         QueryObject qo = toQueryObject(entry.getValue());
         if ((qo != null) && (qo.equals(suspect))) {
            found = true;
            aStr.append(qo.toStringAbbreviated() + " was found in " + aRegion.getFullPath() + " at key " + key + "\n");
         }
      } 
      if (!found) {
         Log.getLogWriter().info("suspect is: " + suspect + 
          " at (i) :" + i + " inconsistencies.size : " +  
          inconsistencies.size());
         aStr.append(suspect.toStringAbbreviated() + " was NOT found in " + aRegion.getFullPath() + "\n");
      }

      // check the region snapshot
      found = false;
      it = regionSnapshot.keySet().iterator();
      while (it.hasNext()) {
         Object key = it.next();
         QueryObject qo = toQueryObject(regionSnapshot.get(key));
         if ((qo != null) && (qo.equals(suspect))) {
            found = true;
            aStr.append(qo.toStringAbbreviated() + " was found in regionSnapshot at key " + key + "\n");
         }
      } 
      if (!found) {
         aStr.append(suspect.toStringAbbreviated() + " was NOT found in regionSnapshot\n");
      }

      // seach for all occurrences in expected list
      found = false;
      it = expected.iterator();
      while (it.hasNext()) {
         QueryObject qo = toQueryObject(it.next());
         if (qo.equals(suspect)) {
            found = true;
            aStr.append(qo.toStringAbbreviated() + " was found in expected results\n");
         }
      } 
      if (!found) {
         aStr.append(suspect.toStringAbbreviated() + " was NOT found in expected results\n");
      }

      // seach for all occurrences in selectResults
      found = false;
      it = selResults.iterator();
      while (it.hasNext()) {
         QueryObject qo = toQueryObject(it.next());
         if (qo.equals(suspect)) {
            found = true;
            aStr.append(qo.toStringAbbreviated() + " was found in SelectResults\n");
         }
      } 
      if (!found) {
         aStr.append(suspect.toStringAbbreviated() + " was NOT found in SelectResults\n");
      }

      // seach for all occurrences in gatherMap
      found = false;
      it = gatherMap.keySet().iterator();
      while (it.hasNext()) {
         Object key = it.next();
         QueryObject qo = toQueryObject(gatherMap.get(key));
         if (qo.equals(suspect)) {
            found = true;
            aStr.append(qo.toStringAbbreviated() + " was found in gatherMap at key " + key + "\n");
         }
      } 
      if (!found) {
         aStr.append(suspect.toStringAbbreviated() + " was NOT found in gatherMap\n");
      }
   }
   return aStr.toString();
}

/** Verify the state of the region and the regionSnapshot.
 */
protected void verifyRegionContents() {
   Log.getLogWriter().info("Verifying contents of " + aRegion.getFullPath() + ", size is " + aRegion.size());
   // find unexpected and missing results

   Set regionKeySet = aRegion.keySet();
   int regionSize = aRegion.size();
   int regionKeySetSize = regionKeySet.size();
   Log.getLogWriter().info("regionSize is " + regionSize + " " +
                           "regionKeySetSize is " + regionKeySetSize);
   if (regionSize != regionKeySetSize) {
       throw new TestException("Inconsistent sizes: regionSize is " + regionSize + " " +
                           "regionKeySetSize is " + regionKeySetSize);
   }
   Set snapshotKeySet = regionSnapshot.keySet();
   List unexpectedInRegion = new ArrayList(regionKeySet); 
   List missingInRegion = new ArrayList(snapshotKeySet);
   Log.getLogWriter().info("Finding any unexpected entries in region...");
   unexpectedInRegion.removeAll(snapshotKeySet);
   Log.getLogWriter().info("Finding any missing entries in region...");
   missingInRegion.removeAll(regionKeySet);
   Log.getLogWriter().info("Found " + unexpectedInRegion.size() + " unexpected entries and " +
       missingInRegion.size() + " missing entries");

   // Log any entries which could be inconsistent through TransactionInDoubtExceptions
   Set inDoubtOps = CQUtilBB.getBB().getFailedOps(CQUtilBB.INDOUBT_TXOPS);
   if (inDoubtOps.size() > 0) {
      Log.getLogWriter().info(inDoubtOps.size() + " TransactionInDoubtExceptions occurred on the following keys:" + inDoubtOps);
   }

   // prepare an error string
   StringBuffer aStr = new StringBuffer();
   if (aRegion.size() != regionSnapshot.size()) {
       aStr.append("Expected " + aRegion.getFullPath() + " to be size " + regionSnapshot.size() +
                   ", but it is size " + aRegion.size());
       Log.getLogWriter().info(aStr.toString());
   }
   if (unexpectedInRegion.size() > 0) {
      String tmpStr = "Found the following " + unexpectedInRegion.size() + 
         " unexpected keys in " + aRegion.getFullPath() + ": " + unexpectedInRegion;
      Log.getLogWriter().info(tmpStr.toString());
      if (aStr.length() > 0) {
         aStr.append("\n");
      }
      aStr.append(tmpStr);
   }
   if (missingInRegion.size() > 0) {
      String tmpStr = "The following " + missingInRegion.size() + 
         " keys were missing from " + aRegion.getFullPath() + ": " + missingInRegion;
      Log.getLogWriter().info(tmpStr);
      if (aStr.length() > 0) {
         aStr.append("\n");
      }
      aStr.append(tmpStr);
   }

   if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
   }
   Log.getLogWriter().info("Done verifying contents of " + aRegion.getFullPath() + ", size is " + aRegion.size());
}

/** Verify the result of the CQs registered for this VM.
 */
protected void verifyQueryResults() {
   Log.getLogWriter().info("In verifyQueryResults");
   Iterator it = queryMap.keySet().iterator();
   int count = 0;
   while (it.hasNext()) {
      count++;
      String cqName = (String)(it.next());
      Log.getLogWriter().info("Verifying query " + count + " out of " + queryMap.size() + " with name " + cqName);
      verifyQuery(cqName);
   }
   Log.getLogWriter().info("Done verifying " + count + " queries");
}


/** Verify the query with the given name
 */
protected void verifyQuery(String cqName) {
   Log.getLogWriter().info("Verifying query " + cqName);
   CqQuery cq = qService.getCq(cqName);
   try {
      if (cq.isRunning()) {
         Log.getLogWriter().info("Stopping " + cq);
         long startTime = System.currentTimeMillis();
         try {
            cq.stop();
         } catch (IllegalStateException e) {
            throw new TestException("cq with name " + cqName + " isRunning: " + cq.isRunning() + ", caught " +
                  TestHelper.getStackTrace(e));
         }
         long endTime = System.currentTimeMillis();
         long duration = endTime - startTime;
         Log.getLogWriter().info("Done stopping " + cq + ", stop completed in " + duration + " millis");
      }

      Log.getLogWriter().info("Calling executeWithInitialResults on " + cq.getName());
      long startTime = System.currentTimeMillis();
      SelectResults sr = null;
      try {
         // CqResults rs = cq.executeWithInitialResults();
         // sr = CQUtil.getSelectResults(rs);
         sr = new CQExecuteVersionHelper().executeWithInitialResults(cq);
      } catch (IllegalStateException e) {
         throw new TestException("cq with name " + cqName + " isRunning: " + cq.isRunning() + ", caught " +
               TestHelper.getStackTrace(e));
      }
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      Log.getLogWriter().info("Done calling executeWithInitialResults on " + cq.getName() + ", completed in " +
          duration + " millis, select results size is " + sr.size());

      if (sr == null) {
         throw new TestException("Bug 37060 detected: For cq " + cq + " with name " + 
               cqName + " executeWithInitialResults returned " + sr);
      }
      verify(cq, sr);
   } catch (CqException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (RegionNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CqClosedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Given a query string, return the query path.
 *
 *  @param queryStr A string containing a query.
 *
 *  @param Returns a string containing the path being queried, or null 
 *         if none.
 */
protected static String getPathInQuery(String queryStr) {
   String[] tokens = queryStr.split("\\s");
   for (int i = 0; i < tokens.length; i++) {
      if (tokens[i].equals("where")) {
         if (tokens[i+1].equals("entry")) {
            // this is a query against invalidated (null) values
            // there is no path in the query
            // for example "select * from /regionName entry where entry = null"
            return null;
         } else {
            return tokens[i+1];
         }
      }
   }
   return null;
}

/** Given a query string, return the field in it being queried 
 *  (the last field in the path).
 *
 *  @param queryStr A string containing a query.
 *
 *  @param Returns a string containing the field being queried, or null 
 *         if none.
 */
protected static String getFieldInQuery(String queryStr) {
   String path = getPathInQuery(queryStr);
   if (path == null) {
      return null;
   }
   int index  = path.lastIndexOf(".");
   String fieldName = path.substring(index+1, path.length());
   fieldName = fieldName.trim();
   return fieldName;
}

/** Given a query string, return the comparator in it.
 *  Queries supported are well-defined and simple. Here is a sample:
 *     "select * from /testRegion"
 *     "select * from /testRegion where p.aPrimitiveInt > 0"
 *
 *  @param queryStr A string containing a query.
 *
 *  @param Returns a string containing the comparator, or null 
 *         if none.
 */
protected static String getComparatorInQuery(String queryStr) {
   String[] tokens = queryStr.split("\\s");
   for (int i = 0; i < tokens.length; i++) {
      if (tokens[i].equals("<>")) {
         return "<>";
      } else if (tokens[i].equals("!=")) {
         return "!=";
      } else if (tokens[i].equals(">=")) {
         return ">=";
      } else if (tokens[i].equals(">")) {
         return ">";
      } else if (tokens[i].equals("<=")) {
         return "<=";
      } else if (tokens[i].equals("<")) {
         return "<";
      } else if (tokens[i].equals("=")) {
         return "=";
      } 
   }
   return null;
}

/** Given a query string, return the value being compared to.
 *  Queries supported are well-defined and simple. Here is a sample:
 *     "select * from /testRegion"
 *     "select * from /testRegion where p.aPrimitiveInt > 0"
 *
 *  @param queryStr A string containing a query.
 *
 *  @param Returns a string containing the value being comparied, or null 
 *         if none.
 */
protected static String getValueToCompareInQuery(String queryStr) {
   if (queryStr.indexOf("where") < 0) {
      return null;
   }
   String[] tokens = queryStr.split("\\s");
   String aStr = tokens[tokens.length-1];
   if (aStr.startsWith("'")) { // strip off the beginning ' (used for String queries)
      aStr = aStr.substring(1, aStr.length());
   }
   if (aStr.endsWith("'")) { // strip off the ending ' (used for String queries)
      aStr = aStr.substring(0, aStr.length()-1);
   }
   // eliminate double '' (which is used to escape ' in a query)
   StringBuffer returnStr = new StringBuffer(aStr);
   for (int i = 0; i < returnStr.length(); i++) {
      if (returnStr.charAt(i) == '\'') {
         int next = i+1;
         if (next < returnStr.length()) {
            if (returnStr.charAt(next) == '\'') {
               returnStr.deleteCharAt(next);
            }
         }
      }
   }
   return returnStr.toString();
}

/** Return the depth of the query in queryStr. For example,
 *  for "aPrimitiveInt > 0", depth is 1, and for 
 *  "aQueryObject.aQueryObject.aFloat < 2", depth is 3
 *
 *  @param queryStr A string containing a query.
 *  @returns The depth of the field being queried in queryStr.
 */
protected static int getDepthInQuery(String queryStr) {
   int depth = 1;
   String field = getPathInQuery(queryStr);
   if (field == null) { // it's a select all
      return depth;
   }
   int index = field.indexOf(".");
   while (index > 0) {
      depth++;
      index = field.indexOf(".", index+1);
   }
   return depth;
}

/** Return a Set of expected results of the given queryStr using
 *  regionSnapshot as the basis of determining what to expect.
 *
 *  @param queryStr A string containing a query.
 *
 *  @returns A List containing the values expected by the query.
 */
protected List getExpectedResults(String queryStr) {
   String fieldName = getFieldInQuery(queryStr);
   String comparator = getComparatorInQuery(queryStr);
   String valueToCompare = getValueToCompareInQuery(queryStr);
   int queryDepth = getDepthInQuery(queryStr);
   Log.getLogWriter().info("In getExpectedResults, queryStr = " + queryStr + 
       ", fieldName = " + fieldName + ", comparator = " + comparator + 
       ", valueToCompare = " + valueToCompare);
   Field aField = null;
   if (fieldName != null) {
      try {
         aField = QueryObject.class.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         throw new TestException("Test problem " + TestHelper.getStackTrace(e));
      }
   }
   List expectedResults = new ArrayList();
   Iterator it = regionSnapshot.keySet().iterator();
   Log.getLogWriter().info("regionSnapShot is size " + regionSnapshot.size());
   while (it.hasNext()) {
      Object key = it.next();
      QueryObject originalQO = toQueryObject(regionSnapshot.get(key));
      if (satisfiesQuery(queryStr, originalQO)) {
         expectedResults.add(originalQO);
      }
   }
   Log.getLogWriter().info("Expected result size is " + expectedResults.size());
   return expectedResults;
}

/** Return true of the given qo satisfies the queryStr, false otherwise.
 *
 *  @param queryStr A string containing a query.
 *  @param qo The QueryObject to test against the query.
 *
 *  @returns true if qo satisfies the query, false otherwise.
 */
public static boolean satisfiesQuery(String queryStr, QueryObject qo) {
   String fieldName = getFieldInQuery(queryStr);
   String comparator = getComparatorInQuery(queryStr);
   String valueToCompare = getValueToCompareInQuery(queryStr);
   int queryDepth = getDepthInQuery(queryStr);
//   Log.getLogWriter().info("In satisfiesQuery, queryStr = " + queryStr + 
//       ", fieldName = " + fieldName + 
//       ", comparator = " + comparator + 
//       ", valueToCompare = " + valueToCompare);
   Field aField = null;
   if (fieldName != null) {
      try {
         aField = QueryObject.class.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         throw new TestException("Test problem " + TestHelper.getStackTrace(e));
      }
   }
   if (qo == null) { 
      return false;
   }
   QueryObject qoAtDepth = qo.getAtDepth(queryDepth);
//   Log.getLogWriter().info("QueryObject is " + qo.toStringAbbreviated());
   if (qoAtDepth == null) { // object was not deep enough for queryDepth so it can't be in the query results
//      Log.getLogWriter().info("QueryObject at depth " + queryDepth + " is " + qoAtDepth);
      return false;
   }
//   Log.getLogWriter().info("QueryObject at depth " + queryDepth + " is " + qoAtDepth.toStringOneLevel());
   if (comparator == null) { // selecting all 
      return true;
   } else {
      Object fieldValue = null;
      if (aField != null) { 
         try {
            fieldValue = aField.get(qoAtDepth);
//            Log.getLogWriter().info("Field value for field " + fieldName + " is " + fieldValue);
         } catch (IllegalAccessException e) {
            throw new TestException("Test problem " + TestHelper.getStackTrace(e));
         }
      } else { 
         fieldValue = qo;
      }
      if (valueToCompare.equalsIgnoreCase("null")) { // query against null
         if ((comparator.equals("=")  && (fieldValue == null)) ||
             (comparator.equals("<>") && (fieldValue != null)) ||
             (comparator.equals("!=") && (fieldValue != null))) {
//            Log.getLogWriter().info(qo.toStringFull() + " satisfies " + getReadableQueryString(queryStr));
            return true;
         }
      } else if (fieldName.equals("aString") || fieldName.equals("aPrimitiveChar") ||
                 fieldName.equals("aCharacter")) { 
         int compare = fieldValue.toString().compareTo(valueToCompare);
//         Log.getLogWriter().info("fieldValue is " + fieldValue + ", valueToCompare is " + valueToCompare + 
//             ", compare is " + compare);
         if ((comparator.equals(">")  && (compare > 0))  ||
             (comparator.equals(">=") && (compare >= 0)) ||
             (comparator.equals("<")  && (compare < 0))  ||
             (comparator.equals("<=") && (compare <= 0)) ||
             (comparator.equals("=")  && (compare == 0)) ||
             (comparator.equals("<>") && (compare != 0)) ||
             (comparator.equals("!=") && (compare != 0))) {
//            Log.getLogWriter().info(qo.toStringFull() + " satisfies " + getReadableQueryString(queryStr));
            return true;
         }
      } else if (fieldName.equals("aPrimitiveFloat") || fieldName.equals("aFloat") ||
                 fieldName.equals("aPrimitiveDouble") || fieldName.equals("aDouble")) {
         Double doubleFieldValue = Double.valueOf(fieldValue.toString());
         Double doubleValueToCompare = Double.valueOf(valueToCompare);
//       Log.getLogWriter().info("doubleFieldValue is " + doubleFieldValue + ", doubleToCompare is " + doubleValueToCompare);
         int compare = doubleFieldValue.compareTo(doubleValueToCompare);
         if ((comparator.equals(">")  && (compare > 0))  ||
             (comparator.equals(">=") && (compare >= 0)) ||
             (comparator.equals("<")  && (compare < 0))  ||
             (comparator.equals("<=") && (compare <= 0)) ||
             (comparator.equals("=")  && (compare == 0)) ||
             (comparator.equals("<>") && (compare != 0)) ||
             (comparator.equals("!=") && (compare != 0))) {
//            Log.getLogWriter().info(qo.toStringFull() + " satisfies " + getReadableQueryString(queryStr));
            return true;
         }
      } else {
         long longFieldValue = new Long(fieldValue.toString()).longValue();
         long longValueToCompare = (new Long(valueToCompare)).longValue();
//         Log.getLogWriter().info("longFieldValue is " + longFieldValue + ", valueToCompare is " + valueToCompare);
         if ((comparator.equals(">")  && (longFieldValue > longValueToCompare))  ||
             (comparator.equals(">=") && (longFieldValue >= longValueToCompare)) ||
             (comparator.equals("<")  && (longFieldValue < longValueToCompare))  ||
             (comparator.equals("<=") && (longFieldValue <= longValueToCompare)) ||
             (comparator.equals("=")  && (longFieldValue == longValueToCompare)) ||
             (comparator.equals("<>") && (longFieldValue != longValueToCompare)) ||
             (comparator.equals("!=") && (longFieldValue != longValueToCompare))) {
//            Log.getLogWriter().info(qo.toStringFull() + " satisfies " + getReadableQueryString(queryStr));
            return true;
         }
      }
   }
   return false;
}

// ========================================================================
// other methods

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
   else if (operation.equals("get"))
      op = ENTRY_GET_OPERATION;
   else if (operation.equals("getNew"))
      op = ENTRY_GET_NEW_OPERATION;
   else if (operation.equals("localInvalidate"))
      op = ENTRY_LOCAL_INVALIDATE_OPERATION;
   else if (operation.equals("localDestroy"))
      op = ENTRY_LOCAL_DESTROY_OPERATION;
   else if (operation.equals("modifyValue"))
      op = ENTRY_MODIFY_VALUE;
   else
      throw new TestException("Unknown entry operation: " + operation);
   return op;
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key.
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of
 *         threads using uniqueKeys.
 *
 *  @returns A key in the region.
 */
public static Object getExistingKey(Region aRegion, boolean uniqueKeys, int numThreads) {
   Set aSet = aRegion.keySet();
   Iterator it = aSet.iterator();
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   while (it.hasNext()) {
      Object key = it.next();
      if (uniqueKeys) {
         long keyIndex = NameFactory.getCounterForName(key);
         if ((keyIndex % numThreads) == myTid) {
            return key;
         }
      } else {
         return key;
      }
   }
   return null;
}

/** Return a unique query name.
 */
protected String getNextQueryName() {
   long counter = CQUtilBB.getBB().getSharedCounters().incrementAndRead(CQUtilBB.QueryNumber); 
   return "Query_" + counter;
}

/** Returns the given query string with addition information if
 *  necessary to make it more readable, such as converting any
 *  characters to their byte equivalent (to aid with debugging).
 *
 *  @param queryStr A query string.
 */
public static String getReadableQueryString(String queryStr) {
   if (queryStr.indexOf("aChar") >= 0) {
      String value = getValueToCompareInQuery(queryStr);
      if (!value.equals("NULL")) {
         if (value.length() != 1) {
            throw new TestException("Test problem; expected value in query string to be " +
                  "length() 1 but it is " + value.length());
         }
         char aChar = value.charAt(0);
         byte byteValue = (byte)aChar;
         queryStr = queryStr + " (char byte value: " + byteValue + ")";
      }
   }
   return queryStr;   
}

/** Check if we have run for the desired length of time. We cannot use 
 *  hydra's taskTimeSec parameter because of a small window of opportunity 
 *  for the test to hang due to the test's "concurrent round robin" type 
 *  of strategy. Here we set a blackboard counter if time is up and this
 *  is the last concurrent round.
 */
protected void checkForLastIteration() {
   // determine if this is the last iteration
   long taskStartTime = 0;
   final String bbKey = "TaskStartTime";
   Object anObj = CQUtilBB.getBB().getSharedMap().get(bbKey);
   if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      CQUtilBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
   } else {
      taskStartTime = ((Long)anObj).longValue();
   }
   if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.TimeToStop);
   } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
         (secondsToRun - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
   }
}

/** Handle an exception thrown by doing general operations during HA.
 */
protected void handleException(Exception anExcept) {
   boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
   String errStr = anExcept.toString();
   Throwable causedBy = anExcept.getCause();
   if (causedBy != null) {
      errStr = errStr + causedBy.toString();
   }
   boolean disconnectError = 
           (errStr.indexOf(
              "This connection to a distributed system has been disconnected") >= 0) ||
           (errStr.indexOf(
              "System is disconnecting") >= 0);

   if (anExcept instanceof CancelException) {
      if ( thisVMReceivedNiceKill || cacheIsClosed || disconnected) {
         // a thread in this VM closed the cache or disconnected from the dist system
         // or we are undergoing a nice_kill; all is OK
      } else { // no reason to get this error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof IllegalStateException) {
      if (disconnectError) {
         if (disconnected || thisVMReceivedNiceKill) {  
            // we got a disconnect error or we are undergoing a nice_kill; all is ok
         } else { // no reason to get this error
            throw new TestException(TestHelper.getStackTrace(anExcept));
         }
      } else if (isBridgeConfiguration && thisVMReceivedNiceKill) { 
         // got IllegalStateException, is bridge config, and we are undergoing a niceKill
         if (errStr.indexOf("Proxy not properly initialized") >= 0) {
            // OK, sockets/endpoints are shutting down during a niceKill
         } else {
            throw new TestException(TestHelper.getStackTrace(anExcept));
         }
      } else { // got IllegalStateException, but it's not a disconnect error
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof CacheLoaderException) {
      if (isBridgeConfiguration && thisVMReceivedNiceKill) {
         if (anExcept.toString().indexOf("The BridgeLoader has been closed") >= 0) {
            // we got a BridgeLoader exception while undergoing a nice_kill; all is ok
   } else {
            throw new TestException(TestHelper.getStackTrace(anExcept));
         }
      } else { // got exception, but it's not a bridge loader being shutdown
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof BridgeWriterException) {
      if (isBridgeConfiguration && thisVMReceivedNiceKill) {
         if (anExcept.toString().indexOf("The BridgeWriter has been closed") >= 0) {
            // we got a BridgeWriter exception while undergoing a nice_kill; all is ok
         } else {
            throw new TestException(TestHelper.getStackTrace(anExcept));
         }
      } else { // got exception, but it's not a bridge writer being shutdown
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
   } else if (anExcept instanceof NullPointerException) {
      // This has happened when a vm is undergoing a shutdown and the util.QueryObject
      // initializer tries to access the blackboard, but the blackboard is not available.
      // Only accept an NPE if it is from the QueryObject initializer
      String stackStr = TestHelper.getStackTrace(anExcept);
      if (stackStr.indexOf("QueryObject.<init>") >= 0) {
         // allow it
      } else {
         throw new TestException(stackStr);
      }
   } else {
      throw new TestException("Got unexpected exception " + TestHelper.getStackTrace(anExcept));
   }
   Log.getLogWriter().info("Caught " + anExcept + "; expected, continuing test");
   CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.ExceptionCounter);
   TestHelper.waitForCounter(CQUtilBB.getBB(), 
                             "CQUtilBB.Reinitialized", 
                             CQUtilBB.Reinitialized, 
                             1, 
                             true, 
                             -1,
                             1000);
}

/** saveRegionSnapshot() - save this VMs internal RegionSnapshot (HashMap) in case of tx failure during commit.
 *     for use in serial tx tests only (to return the internal region snapshot to its previosus state
 *     when a TransactionDataNodeHasDeparted, TransactionDataRebalanced or TransactionInDoubt Exception
 *     encountered at commit time.
 */
protected void saveRegionSnapshot() {
  txRegionSnapshot = new HashMap(regionSnapshot);
}

private QueryObject toQueryObject(Object anObj) {
  return (QueryObject)(PdxTestVersionHelper.toBaseObject(anObj));
}

/** restoreRegionSnapshot()  - restore this VMs internal RegionSnapshot (HashMap) after a tx failure during commit.
 *     for use in serial tx tests only (to return the internal region snapshot to its previosus state
 *     when a TransactionDataNodeHasDeparted, TransactionDataRebalanced or TransactionInDoubt Exception
 *     encountered at commit time.
 */
protected void restoreRegionSnapshot() {
  regionSnapshot = txRegionSnapshot;
  // cleanup
  txRegionSnapshot = null;
}

}
