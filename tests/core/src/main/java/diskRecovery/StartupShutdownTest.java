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
package diskRecovery;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.DistributedSystemHelper;
import hydra.GatewayHubHelper;
import hydra.GsRandom;
import hydra.HydraSubthread;
import hydra.HydraThreadGroup;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.PartitionDescription;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.newedge.account.domain.BackOfficeAccount;
import com.newedge.account.domain.BackOfficeAccountCollection;
import com.newedge.staticdata.domain.Product;
import parReg.ParRegUtil;
import util.AdminHelper;
import util.BaseValueHolder;
import util.NameFactory;
import util.RandomValues;
import util.SilenceListener;
import util.StopStartVMs;
import util.SummaryLogListener;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;
import wan.query.WANQueryPrms;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.persistence.PersistentReplicatesOfflineException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.GatewayCancelledException;
import com.gemstone.gemfire.internal.cache.PartitionedRegionException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;

/** Tests to exercise startup with disk recovery and shutDownAll
 * 
 * @author lynn
 *
 */
public class StartupShutdownTest {

  public static StartupShutdownTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;

  static int maxPRs = -1;
  static int maxReplicates = -1;
  
  static private HydraThreadLocal currentNumKeys = new HydraThreadLocal(); // used for batched loading

  private static final String allRegionsSnapshotKey = "allRegions";

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new StartupShutdownTest();
      testInstance.initializeInstance();
      CacheHelper.createCache("cache1");
      createRegions("clientRegion", "clientRegion", false);
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        boolean registerInterest = RecoveryPrms.getRegisterInterest();
        if (registerInterest) {
          registerInterest();
        }
      }
    }
    currentNumKeys.set(new Integer(0));
    Log.getLogWriter().info("currentNumKeys is " + currentNumKeys);
  }

  /** Create the regions for this test.
   * 
   * @param PRconfigName The hydra config name for PR regions.
   * @param replicateConfigName The hydra config name for replicate regions.
   * @param colocated If true, colocate some of the PR regions, if false then no PRs are colocated
   */
  private static void createRegions(String PRconfigName, String replicateConfigName, boolean colocated) {
    for (int i = 1; i <= maxPRs; i++) {
      if (colocated) {
        // colocate all odd numbered regions with PR_1
        if ((i != 1) && ((i % 2) == 1)) { // colocate all odd numbered PRs with PR_1
          AttributesFactory aFactory = RegionHelper.getAttributesFactory(PRconfigName);
          RegionDescription rd = RegionHelper.getRegionDescription(PRconfigName);
          PartitionDescription pd = rd.getPartitionDescription();
          PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
          prFactory.setColocatedWith("PR_1");
          PartitionAttributes prAttrs = prFactory.create();
          aFactory.setPartitionAttributes(prAttrs);
          RegionHelper.createRegion("PR_" + i, aFactory);
        } else {
          RegionHelper.createRegion("PR_" + i, PRconfigName);
        }
      } else {
        RegionHelper.createRegion("PR_" + i, PRconfigName);
      }
    }
    for (int i = 1; i <= maxReplicates; i++) {
      RegionHelper.createRegion("replicate_" + i, replicateConfigName);
    }
    Log.getLogWriter().info("After creating regions, root regions is " + CacheHelper.getCache().rootRegions());
    //@todo lynn generateCacheXmlFile("vm_" + RemoteTestModule.getMyVmid() + ".xml");
  }

  /** Creates and initializes a server or peer.
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new StartupShutdownTest();
      testInstance.initializeInstance();
      CacheHelper.createCache("cache1");
      createRegions("persistPR", "persistReplicate", RecoveryPrms.getUseColocatedPRs());
      int myVmId = RemoteTestModule.getMyVmid();
      RecoveryBB.getBB().getSharedMap().put("dataStoreJvm_" + myVmId, new ClientVmInfo(myVmId));
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    currentNumKeys.set(new Integer(0));
    Log.getLogWriter().info("currentNumKeys is " + currentNumKeys);
  }

  /** Creates and initializes a server or peer with colocated PRs allowing exceptions
   *  due to shutDownAll being called.
   */
  public synchronized static void HydraTask_initializeDuringShutDownAll() {
    try {
      HydraTask_initialize();
    } catch (CacheClosedException e) {
      Log.getLogWriter().info("Caught " + e);
    } catch (IllegalStateException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Region specified in 'colocated-with' is not present. It should be " +
          "created before setting 'colocated-with' to this region.") >= 0) {
        // OK, this is accepted during a shutDownAll
        Log.getLogWriter().info("Caught " + e);
      } else {
        throw e;
      }
    } catch (NullPointerException e) { // only allow this if hydra is the first thing in the call stack
      // hydra gets the cache, which is null
      if (e.getCause() != null) {
        throw e;
      }
      String errStr = TestHelper.getStackTrace(e);
      int index1 = errStr.lastIndexOf(NullPointerException.class.getName()); // next line is first line of call stack
      if (index1 < 0) {
        throw e;
      }
      int index2 = errStr.indexOf("\n", index1); // end of NPE line, next line is first line of call stack
      if (index2 < 0) {
        throw e;
      }
      int index3 = errStr.indexOf("\n", index2+1);
      if (index3 < 0) {
        throw e;
      }
      String firstLine = errStr.substring(index2, index3);
      if ((firstLine.indexOf("hydra.DiskStoreHelper.createDiskStore") >= 0) ||
          (firstLine.indexOf("hydra.DiskStoreHelper.getDiskStoreFactory") >= 0)) {
        Log.getLogWriter().info("Caught " + errStr + " while initializing during shutDownAll");
      } else {
        throw e;
      }
    } catch (DistributedSystemDisconnectedException e) {
      Log.getLogWriter().info("Caught " + e);
    }
  }

  /** Create gateway hub
   * 
   */
  public static void HydraTask_createGatewayHub() {
    String hubConfigName = "gatewayHub";
    Log.getLogWriter().info("Creating gateway hub with hub config: " + hubConfigName);
    GatewayHubHelper.createGatewayHub(hubConfigName);
  }

  /** Create gateway hub
   * 
   */
  public static void HydraTask_addGatewayHub() {
    String gatewayConfigName = "gateway";
    Log.getLogWriter().info("Adding gateway with gateway config: " + gatewayConfigName);
    GatewayHubHelper.addGateways(gatewayConfigName);
  }

  /** Start gateway hub
   * 
   */
  public static void HydraTask_startGatewayHub() {
    Log.getLogWriter().info("Starting gateway hub");
    GatewayHubHelper.startGatewayHub();
  }

  public static void HydraTask_serversAreBack() {
    long counter = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.serversAreBack);
    Log.getLogWriter().info("serversAreBack counter is " + counter);
  }

  /** Creates and initializes a server or peer with all empty or accessor regions.
   */
  public synchronized static void HydraTask_initializeProxy() {
    if (testInstance == null) {
      testInstance = new StartupShutdownTest();
      testInstance.initializeInstance();
      CacheHelper.createCache("cache1");
      createRegions("proxyPR", "proxyReplicate", RecoveryPrms.getUseColocatedPRs());
      int myVmId = RemoteTestModule.getMyVmid();
      RecoveryBB.getBB().getSharedMap().put("proxyJvm_" + myVmId, new ClientVmInfo(myVmId));
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    currentNumKeys.set(new Integer(0));
    Log.getLogWriter().info("currentNumKeys is " + currentNumKeys);
  }

  public static void HydraTask_verifyFromLeaderSnapshot() {
    TestHelper.waitForCounter(RecoveryBB.getBB(), "snapshotWritten", RecoveryBB.snapshotWritten , 1, true, -1, 3000);
    testInstance.verifyFromSnapshot();
  }

  /** Creates and initializes a server or peer with all empty or accessor regions.
   */
  public synchronized static void HydraTask_initializeProxyDuringShutDownAll() {
    try {
      HydraTask_initializeProxy();
    } catch (CacheClosedException e) {
      Log.getLogWriter().info("Caught " + e);
    } catch (IllegalStateException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Region specified in 'colocated-with' is not present. It should be " +
          "created before setting 'colocated-with' to this region.") >= 0) {
        // OK, this is accepted during a shutDownAll
        Log.getLogWriter().info("Caught " + e);
      } else {
        throw e;
      }
    } catch (DistributedSystemDisconnectedException e) {
      Log.getLogWriter().info("Caught " + e);
    } catch (PartitionedRegionException e) {
      Throwable causedBy = e.getCause();
      if (causedBy instanceof CacheClosedException) {
        Log.getLogWriter().info("Caught " + e + " caused by " + causedBy);
      } else {
        throw e;
      }
    }
  }

  /** Initialize an instance of this test class, called once per vm.
   *
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    maxPRs = RecoveryPrms.getMaxPRs();
    maxReplicates = RecoveryPrms.getMaxReplicates();
  }

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }


  /** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
   *  which is equivalent to a full GII.
   */
  protected static void registerInterest() {
    Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
    for (Region aRegion: rootRegions) {
      Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES for region " +
          aRegion.getFullPath());
      aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      Log.getLogWriter().info("Done calling registerInterest for all keys, " +
          "result interest policy KEYS_VALUES, " + aRegion.getFullPath() +
          " size is " + aRegion.size());
    }
  }

  /** Loads regions with data. This is a batched task, and each thread will repeatedly
   *  run this task until it loads RecoveryPrms.numToLoad new entries. Each invocation
   *  of this task will keep working until this thread loads the specified number of entries
   *  then it will throw a StopSchedulingTaskOnclientOrder exception.
   */
  public static void HydraTask_load() {
    int numToLoad = RecoveryPrms.getNumToLoad(); // number of keys to be put by each thread
    int CHUNK_SIZE = 50;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    int currentNumber = (Integer) currentNumKeys.get();

    HashMap putAllMap = new HashMap();
    int putAllMapSize = Math.min(CHUNK_SIZE,  numToLoad-currentNumber);
    for (int i = 1; i <= putAllMapSize; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      Object value = new ValueHolder((String)key, rv);
      putAllMap.put(key, value);
    }
    currentNumber += putAllMapSize;
    currentNumKeys.set(new Integer(currentNumber));

    Log.getLogWriter().info("Created putAll map of size " + putAllMap.size() + ", loading " + currentNumber + " entries out of " + numToLoad + " for this load thread");
    for (Region aRegion: regionSet) {
      Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
          aRegion.getFullPath());
      aRegion.putAll(putAllMap);
    }

    if (currentNumber >= numToLoad) {
      throw new StopSchedulingTaskOnClientOrder("All " + numToLoad + " entries created in all regions by this load thread");
    }
  }
  
  /** Loads regions with data. This is a batched task, and each thread will repeatedly
   *  run this task until it loads RecoveryPrms.numToLoad new entries. Each invocation
   *  of this task will keep working until this thread loads the specified number of entries
   *  then it will throw a StopSchedulingTaskOnclientOrder exception.
   */
  public static void HydraTask_loadXmlRegions() {
    int numToLoad = RecoveryPrms.getNumToLoad(); // number of keys to be put by each thread
    int CHUNK_SIZE = 50;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    int currentNumber = (Integer) currentNumKeys.get();

    HashMap putAllMap = new HashMap();
    int putAllMapSize = Math.min(CHUNK_SIZE,  numToLoad-currentNumber);
    for (int i = 1; i <= putAllMapSize; i++) {
      String key = NameFactory.getNextPositiveObjectName();
      Object value = new ValueHolder(key, rv);
      putAllMap.put(key, value);
    }
    currentNumber += putAllMapSize;
    currentNumKeys.set(new Integer(currentNumber));

    Log.getLogWriter().info("Created putAll map of size " + putAllMap.size() + ", loading " + currentNumber + " entries out of " + numToLoad + " for this load thread");
    for (Region aRegion: regionSet) {
      String regName = aRegion.getName();
      if (!regName.equals("product") && (!regName.equals("backOfficeAccount")) && (!regName.equals("backOfficeAccountCollection"))) {
        Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
            aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    }
    Region aRegion = CacheHelper.getCache().getRegion("product");
    for (Object key: putAllMap.keySet()) {
      Product value = new Product();
      value.productCode = (String) key;
      value.instrumentId = (String) key;
      putAllMap.put(key, value);
    }
    aRegion.putAll(putAllMap);
    aRegion = CacheHelper.getCache().getRegion("backOfficeAccount");
    for (Object key: putAllMap.keySet()) {
      BackOfficeAccount value = new BackOfficeAccount();
      value.account = (String) key;
      putAllMap.put(key, value);
    }
    aRegion.putAll(putAllMap);
    aRegion = CacheHelper.getCache().getRegion("backOfficeAccountCollection");
    for (Object key: putAllMap.keySet()) {
      BackOfficeAccountCollection value = new BackOfficeAccountCollection();
      putAllMap.put(key, value);
    }
    aRegion.putAll(putAllMap);
    
    Log.getLogWriter().info("Printing current region hierarchy with sizes");
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    if (currentNumber >= numToLoad) {
      Log.getLogWriter().info("All regions loaded ");
      Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
      throw new StopSchedulingTaskOnClientOrder("All " + numToLoad + " entries created in all regions by this load thread");
    }
  }
  

  /** Do continuous updates on existing keys using putAll, catching any exceptions.
   *  Exceptions can be expected due to jvms being stopped. 
   */
  public static void HydraTask_doContinuousUpdates() {
    int PUT_ALL_SIZE = 50;
    RandomValues rv = new RandomValues();
    GsRandom rand = TestConfig.tab().getRandGen();
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) { // we might be shutting down
      return;
    }
    try {
      Set<Region<?, ?>> regionSet = theCache.rootRegions();
      long maxKey = NameFactory.getPositiveNameCounter();

      HashMap putAllMap = new HashMap();
      for (int i = 1; i <= PUT_ALL_SIZE; i++) {
        long randInt = rand.nextLong(1, maxKey);
        Object key = NameFactory.getObjectNameForCounter(randInt);
        Object value = new ValueHolder((String)key, rv);
        putAllMap.put(key, value);
      }

      Log.getLogWriter().info("Created putAll map of size " + putAllMap.size() + ", updating all regions with putAll...);");
      for (Region aRegion: regionSet) {
        Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
            aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    } catch (CacheClosedException e) {  // shutdownAll is occurring during updates
      Log.getLogWriter().info("HydraTask_doContinuousUpdates caught " + e);
    } catch (RegionDestroyedException e) {
      Log.getLogWriter().info("HydraTask_doContinuousUpdates caught " + e);
    } catch (DistributedSystemDisconnectedException e) {
      Log.getLogWriter().info("HydraTask_doContinuousUpdates caught " + e);
    } catch (PartitionedRegionStorageException e) {
      String errStr = e.getMessage().toString();
      if (errStr.indexOf("Unable to find any members to host a bucket") >= 0) {
        Log.getLogWriter().info("HydraTask_doContinuousUpdates caught " + e);
      } else {
        throw e;
      }
    } catch (PersistentReplicatesOfflineException e) {
      Log.getLogWriter().info("HydraTask_doContinuousUpdates caught " + e);
    }
  }

  /** Do updates in wan test. 
   *  Do updates for 1/3 of the existing keys until we get the signal that shutDownAll has
   *  completed (and then some) in the other wan site.
   *  Then do updates for the another 1/3 of the keys while the vms in the other wan site
   *  recover.
   *  This is so we can know that updates happening during shutDownAll and recovery are
   *  consistent. If we didn't separate the keys in groups, the ops occurring during/after
   *  recovery might overwrite the ops we expect to eventually see during shutDownAll.
   *  Note: the other 1/3 keys is updated in the wan site that shuts down.
   */
  public static void HydraTask_doWanUpdates() {
    long maxKey = NameFactory.getPositiveNameCounter();
    int firstLimit = (int) (maxKey / 3);
    int secondLimit = firstLimit * 2;
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    HydraThreadGroup tg = TestConfig.getInstance().getThreadGroup(tgname);
    int numThreads = tg.getTotalThreads() * 2; // number of threads in this thread group AND the other site's threads
    Log.getLogWriter().info("maxKey is " + maxKey + ", firstLimit is " + firstLimit + 
        ", secondLimit is " + secondLimit);

    // do updates until we get the signal that shutDownAll has completed in the other wan site
    SharedCounters sc = RecoveryBB.getBB().getSharedCounters();
    long threadBaseIndex = sc.incrementAndRead(RecoveryBB.currValue); // used for unique keys per thread
    long keyIndex = threadBaseIndex;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info("Doing updates before/during shutDownAll with threadBaseIndex " + threadBaseIndex +
        " and key increment " + numThreads);
    while (sc.read(RecoveryBB.shutDownAllCompleted) == 0) {
      Object key = NameFactory.getObjectNameForCounter(keyIndex);
      for (Region aRegion: regionSet) {
        BaseValueHolder currValue = (BaseValueHolder) aRegion.get(key);
        BaseValueHolder updatedValue = new ValueHolder((String)key, rv);
        updatedValue.myValue = ((Long)(currValue.myValue)) + 1;
        Log.getLogWriter().info("Putting " + key + ", " + TestHelper.toString(updatedValue) + " in region " +
            aRegion.getFullPath());
        aRegion.put(key, updatedValue);
      }
      keyIndex += numThreads;
      if (keyIndex > firstLimit) {
        keyIndex = threadBaseIndex; // start over
      }
    }

    // now do updates until we get the signal that recovery has completed in the other wan site
    while (keyIndex < firstLimit) {
      keyIndex += numThreads;
    }
    threadBaseIndex = keyIndex; // 2nd half key range base index
    Log.getLogWriter().info("Doing updates during recovery with threadBaseIndex " + threadBaseIndex +
        " and key increment " + numThreads);
    int numThreadsPerSite = tg.getTotalThreads();
    while (sc.read(RecoveryBB.serversAreBack) != numThreadsPerSite) {
      Object key = NameFactory.getObjectNameForCounter(keyIndex);
      for (Region aRegion: regionSet) {
        BaseValueHolder currValue = (BaseValueHolder) aRegion.get(key);
        BaseValueHolder updatedValue = new ValueHolder((String)key, rv);
        updatedValue.myValue = ((Long)(currValue.myValue)) + 1;
        Log.getLogWriter().info("Putting " + key + ", " + TestHelper.toString(updatedValue) + " in region " +
            aRegion.getFullPath());
        aRegion.put(key, updatedValue);
      }
      keyIndex += numThreads;
      if (keyIndex > secondLimit) {
        keyIndex = threadBaseIndex; // start over
      }
    }

    // pause and write to the blackboard
    Log.getLogWriter().info("Signaling pause...");
    sc.incrementAndRead(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "pausing", RecoveryBB.pausing, numThreadsPerSite, true, -1, 3000);
    // all threads have paused
    SilenceListener.waitForSilence(30, 2000);
    long leader = sc.incrementAndRead(RecoveryBB.leader);
    if (leader ==  1) {
      Log.getLogWriter().info("This thread is the leader, writing snapshot...");
      writeSnapshot();
      sc.incrementAndRead(RecoveryBB.snapshotWritten);
    } else {
      TestHelper.waitForCounter(RecoveryBB.getBB(), "snapshotWritten", RecoveryBB.snapshotWritten, 1, true, -1, 3000);
      testInstance.verifyFromSnapshot();
    }
    throw new StopSchedulingTaskOnClientOrder();
  }

  /** Do updates in wan test in a wan site that will undergo shutDownAll. Ops
   *  continue until the wan site is shutdown.
   *  Do updates for the final 1/3 of keys (see HydraTask_doWanUpdates for 
   *  updates on the other 2/3 of keys.
   */
  public static void HydraTask_doWanUpdatesDuringShutDownAll() {
    SharedCounters sc = RecoveryBB.getBB().getSharedCounters();
    if (sc.read(RecoveryBB.shutDownAllCompleted) >= 1) { // no ops after recovery
      throw new StopSchedulingTaskOnClientOrder();
    }
    // it's before the shutDownAll, do ops
    long maxKey = NameFactory.getPositiveNameCounter();
    int lowerLimit = (int) ((maxKey / 3) * 2) + 1;
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    HydraThreadGroup tg = TestConfig.getInstance().getThreadGroup(tgname);
    int numThreads = tg.getTotalThreads() * 2; // number of threads in this thread group AND the other site's threads
    Log.getLogWriter().info("maxKey is " + maxKey + ", lowerLimit is " + lowerLimit);

    // do updates until we get the signal that shutDownAll has completed in this wan site
    long threadBaseIndex = sc.incrementAndRead(RecoveryBB.currValue); // used for unique keys per thread
    long keyIndex = threadBaseIndex;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info("Doing ops until this jvm is stopped with shutDownAll, threadBaseIndex is  " + threadBaseIndex +
        " and key increment " + numThreads);
    try {
      while (true) { // do ops until shutDownAll stops them
        Object key = NameFactory.getObjectNameForCounter(keyIndex);
        for (Region aRegion: regionSet) {
          BaseValueHolder currValue = (BaseValueHolder) aRegion.get(key);
          BaseValueHolder updatedValue = new ValueHolder((String)key, rv);
          updatedValue.myValue = ((Long)(currValue.myValue)) + 1;
          Log.getLogWriter().info("Putting " + key + ", " + TestHelper.toString(updatedValue) + " in region " + 
              aRegion.getFullPath());
          aRegion.put(key, updatedValue);
        }
        keyIndex += numThreads;
        if (keyIndex > maxKey) {
          keyIndex = threadBaseIndex; // start over
        }
      }
    } catch (CacheClosedException e) {
      Log.getLogWriter().info("Caught expected " + e);
      MasterController.sleepForMs(3600000); // sleep until this jvm is killed
    } catch (RegionDestroyedException e) {
      Log.getLogWriter().info("Caught expected " + e);
      MasterController.sleepForMs(3600000); // sleep until this jvm is killed
    } catch (GatewayCancelledException e) {
      Log.getLogWriter().info("Caught expected " + e);
      MasterController.sleepForMs(3600000); // sleep until this jvm is killed
    }
  }

  /** Execute customer provided queries
   * 
   */
  public static void HydraTask_doQueries() {
    long maxKeyIndex = NameFactory.getPositiveNameCounter();
    long midPointKeyIndex = maxKeyIndex / 2;
    String maxKey = NameFactory.getObjectNameForCounter(maxKeyIndex);
    String midPointKey = NameFactory.getObjectNameForCounter(midPointKeyIndex);
    QueryService qs = CacheHelper.getCache().getQueryService();
    String[] queries = new String[] {
        "select * from /product",
        "select * from /product where productCode <= '" + midPointKey + "'",
        "select * from /product where productCode > '" + midPointKey + "'",
        "select * from /product where productCode = '" + midPointKey + "'",
        "select * from /product where productCode <> '" + midPointKey + "'",
        "select * from /product where productCode < '" + maxKey + "'",
        "select * from /product where productCode >= '" + maxKey + "'",
        "select * from /product where productCode = '" + maxKey + "'",
        "select * from /product where productCode <> '" + maxKey + "'",
        "select * from /product where productCode < 'Object_1'",
        "select * from /product where productCode >= 'Object_1'",
        "select * from /product where productCode = 'Object_1'",
        "select * from /product where productCode <> 'Object_1'",  
        
        "select * from /backOfficeAccount",
        "select * from /backOfficeAccount where account <= '" + midPointKey + "'",
        "select * from /backOfficeAccount where account > '" + midPointKey + "'",
        "select * from /backOfficeAccount where account = '" + midPointKey + "'",
        "select * from /backOfficeAccount where account <> '" + midPointKey + "'",
        "select * from /backOfficeAccount where account < '" + maxKey + "'",
        "select * from /backOfficeAccount where account >= '" + maxKey + "'",
        "select * from /backOfficeAccount where account = '" + maxKey + "'",
        "select * from /backOfficeAccount where account <> '" + maxKey + "'",
        "select * from /backOfficeAccount where account < 'Object_1'",
        "select * from /backOfficeAccount where account >= 'Object_1'",
        "select * from /backOfficeAccount where account = 'Object_1'",
        "select * from /backOfficeAccount where account <> 'Object_1'",  
    };
    for (String qStr: queries) {
      Log.getLogWriter().info("Executing query: " + qStr);
      Query aQuery = qs.newQuery(qStr);
      try {
        SelectResults results = (SelectResults) aQuery.execute();
        Log.getLogWriter().info("Result size is " + results.size());
//        for (Object singleResult: results) {
//          Log.getLogWriter().info("   result is " + singleResult);
//        }
      } catch (FunctionDomainException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (TypeMismatchException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (NameResolutionException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (QueryInvocationTargetException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }
  
  /** Execute customer provided queries
   * 
   */
  public static void HydraTask_doQueriesDuringShutdown() {
    HydraTask_doQueries();
  }

  /** Write a snapshot of all regions to the blackboard.
   * 
   */
  private static void writeSnapshot() {
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info("Preparing to write snapshot for " + regionSet.size() + " regions");
    Map allRegionsSnapshot = new HashMap();
    for (Region aRegion: regionSet) {
      Map regionSnapshot = new HashMap();
      if (aRegion.getAttributes().getDataPolicy().withPersistence()) { // only persistent regions will have values when restarted
        for (Object key: aRegion.keySet()) {
          Object value = null;
          if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
            value = aRegion.get(key);
          }
          if (value instanceof BaseValueHolder) {
            regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
          } else {
            regionSnapshot.put(key, value);
          }
        }
      } else {
        Log.getLogWriter().info(aRegion.getFullPath() + " is size " + aRegion.size() +
            " but it is not persistent, so snapshot size is " + regionSnapshot.size() +
            " (the region will be empty after stop/restart)");
      }
      allRegionsSnapshot.put(aRegion.getFullPath(), regionSnapshot);
      Log.getLogWriter().info("Region snapshot for " + aRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    }
    RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    Log.getLogWriter().info("Put snapshot for " + regionSet.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm. 
   * 
   */
  private void verifyFromSnapshot() {
    Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
    Set snapshotRegionNames = allRegionsSnapshot.keySet();
    Set<String> definedRegionNames = new HashSet();
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    for (Region aRegion: regionSet) {
      definedRegionNames.add(aRegion.getFullPath());
      Set<Region> subRegSet = aRegion.subregions(true);
      for (Region subReg: subRegSet) {
        definedRegionNames.add(subReg.getFullPath());
      }
    }
    Set missingRegionsInCache = new HashSet(snapshotRegionNames);
    missingRegionsInCache.removeAll(definedRegionNames);
    Set extraRegionsInCache = new HashSet(definedRegionNames);
    extraRegionsInCache.removeAll(snapshotRegionNames);
    if (missingRegionsInCache.size() != 0) {
      throw new TestException("Expected to find regions " + missingRegionsInCache + " defined in cache");
    }
    if (extraRegionsInCache.size() != 0) {
      throw new TestException("Found unexpected regions defined in cache: " + extraRegionsInCache);
    }

    Cache theCache = CacheHelper.getCache();
    for (String regionName: allRegionsSnapshot.keySet()) {
      Map regionSnapshot = allRegionsSnapshot.get(regionName);
      Region aRegion = theCache.getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      }
      verifyFromSnapshot(aRegion, regionSnapshot);
    }
  }

  /** Verify that the given region is consistent with the given snapshot.
   * 
   * @param aRegion The region to verify
   * @param snapshot The expected contents of aRegion.
   */
  public void verifyFromSnapshot(Region aRegion, Map regionSnapshot) {
    // init
    StringBuffer errStr = new StringBuffer();
    int snapshotSize = regionSnapshot.size();
    int regionSize = aRegion.size();
    long startVerifyTime = System.currentTimeMillis();
    Log.getLogWriter().info("Verifying " + aRegion.getFullPath() + "  of size " + aRegion.size() +
        " against snapshot containing " + regionSnapshot.size() + " entries...");

    // verify
    if (snapshotSize != regionSize) {
      errStr.append("Expected region " + aRegion.getFullPath() + " to be size " + snapshotSize +
          ", but it is " + regionSize + "\n");
    }
    for (Object key: regionSnapshot.keySet()) {
      // validate using ParRegUtil calls even if the region is not PR; these calls do the desired job
      // containsKey
      try {
        ParRegUtil.verifyContainsKey(aRegion, key, true);
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }
      // containsValueForKey
      boolean containsValueForKey = aRegion.containsValueForKey(key);
      Object expectedValue = regionSnapshot.get(key);
      try {
        ParRegUtil.verifyContainsValueForKey(aRegion, key, (expectedValue != null));
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }

      // check the value
      if (containsValueForKey) {
        try {
          Object actualValue = aRegion.get(key);
          if (actualValue instanceof BaseValueHolder) {
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else {
            if ((actualValue instanceof byte[]) &&
                (expectedValue instanceof byte[])) {
              byte[] actual = (byte[])actualValue;
              byte[] expected = (byte[])expectedValue;
              if (actual.length != expected.length) {
                throw new TestException("Expected value for key " + key + " to be " +
                    TestHelper.toString(expectedValue) + ", but it is " +
                    TestHelper.toString(actualValue) + " in " + aRegion.getFullPath());
              }
            } else {
              throw new TestException("Expected value for key " + key + " to be " +
                  TestHelper.toString(expectedValue) + ", but it is " +
                  TestHelper.toString(actualValue) + " in " + aRegion.getFullPath());
            }
          }
        } catch (TestException e) {
          errStr.append(e.getMessage() + "\n");
        }
      }
      // else value in region is null; the above check for containsValueForKey
      // checked that  the snapshot is also null
    }

    // check for extra keys in the region that were not in the snapshot
    Set aRegionKeySet = new HashSet(aRegion.keySet());
    Set snapshotKeySet = regionSnapshot.keySet();
    aRegionKeySet.removeAll(snapshotKeySet);
    if (aRegionKeySet.size() != 0) {
      errStr.append("Found the following unexpected keys in " + aRegion.getFullPath() +
          ": " + aRegionKeySet + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
    Log.getLogWriter().info("Done verifying " + aRegion.getFullPath() + " from snapshot containing " + snapshotSize + " entries, " +
        "verification took " + (System.currentTimeMillis() - startVerifyTime) + "ms");
  }

  /** Task to do a shutdownAll, then start the accessors before the dataStores
   *  to reproduce bug 43899.
   */
  public static void HydraTask_shutDownAll() {
    logExecutionNumber();
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> locators = StopStartVMs.getMatchVMs(vmList, "locator");
    List<ClientVmInfo> vmListExcludeLocators = new ArrayList<ClientVmInfo>();
    vmListExcludeLocators.addAll(vmList);
    vmListExcludeLocators.removeAll(locators);
    List<ClientVmInfo> dataStoreList = StopStartVMs.getMatchVMs(vmList, "dataStore");
    List<ClientVmInfo> accessorList = StopStartVMs.getMatchVMs(vmList, "accessor");
    List stopModes = new ArrayList();
    stopModes.add(ClientVmMgr.MeanKill);
    Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS, stopModes);
    List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
    Set shutDownAllResults = (Set)(tmp[1]);
    if (shutDownAllResults.size() != vmListExcludeLocators.size()) { // shutdownAll did not return the expected number of members
      throw new TestException("Expected shutDownAllMembers to return " + vmListExcludeLocators.size() +
          " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
    }   
    
    StopStartVMs.startVMs(accessorList);
    MasterController.sleepForMs(10000); // allow the accessors to start doing ops
    StopStartVMs.startVMs(dataStoreList);
  }

  /** Task to do a shutdownAll in wan test to reproduce bug 43945.
   */
  public static void HydraTask_wanShutDownAll() {
    logExecutionNumber();
    int msToSleep = 30000;
    Log.getLogWriter().info("Sleeping for " + msToSleep + " ms to allow ops to run before shutDownAll");
    MasterController.sleepForMs(msToSleep); // let ops occur while the vms are down
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs(); // contains vms from only this wan site
    List stopModes = new ArrayList();
    stopModes.add(ClientVmMgr.MeanKill);
    Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS, stopModes);
    List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0]; 
    Set shutDownAllResults = (Set)(tmp[1]);
    if (shutDownAllResults.size() != vmList.size()) { // shutdownAll did not return the expected number of members
      throw new TestException("Expected shutDownAllMembers to return " + vmList.size() +
          " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
    }   

    // sleep to allow ops to occur and build up in the wan queue
    msToSleep = 30000;
    Log.getLogWriter().info("Sleeping for " + msToSleep + " ms to let wan queue grow after shutDownAll");
    MasterController.sleepForMs(msToSleep); // let ops occur while the vms are down
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.shutDownAllCompleted);
    StopStartVMs.startVMs(shutDownAllVMs); // this does validation in the init tasks
    throw new StopSchedulingTaskOnClientOrder();
  }

  /** Task to do a shutdownAll, then start the accessors before the dataStores
   *  to reproduce bug 43899.
   */
  public static void HydraTask_shutDownAllDuringRecovery() {
    logExecutionNumber();
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> locators = StopStartVMs.getMatchVMs(vmList, "locator");
    List<ClientVmInfo> vmListExcludeLocators = new ArrayList<ClientVmInfo>();
    vmListExcludeLocators.addAll(vmList);
    vmListExcludeLocators.removeAll(locators);
    Log.getLogWriter().info("vmList is " + vmListExcludeLocators);
    List stopModes = new ArrayList();
    for (int i = 0; i < vmListExcludeLocators.size(); i++) {
      stopModes.add(ClientVmMgr.MeanKill);
    }
    Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS, stopModes);
    List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
    Set shutDownAllResults = (Set)(tmp[1]);
    if (shutDownAllResults.size() != vmListExcludeLocators.size()) { // shutdownAll did not return the expected number of members
      throw new TestException("Expected shutDownAllMembers to return " + vmListExcludeLocators.size() +
          " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
    }

    // start the vms 
    List<Thread> threads = StopStartVMs.startAsync(shutDownAllVMs);

    // start looping shutDownAll calls until all jvms are again shutdown; we loop here because we
    // don't know at any given moment how many of the restarted jvms have become part of the DS
    // when shutDownAll is called here
    int msToSleep = TestConfig.tab().getRandGen().nextInt(10, 60) * 1000;
    Log.getLogWriter().info("Sleeping for " + msToSleep + " ms before running shutDownAll again");
    MasterController.sleepForMs(msToSleep);
    Log.getLogWriter().info("AdminDS " + adminDS + " is shutting down all members again...");
    Set<DistributedMember> memberSet = new HashSet();
    while (memberSet.size() < vmListExcludeLocators.size()) {
      MasterController.sleepForMs(1000);
      try {
        long startTime = System.currentTimeMillis();
        memberSet.addAll(adminDS.shutDownAllMembers());
        long duration = System.currentTimeMillis() - startTime;
        Log.getLogWriter().info("AdminDS " + adminDS + " shut down (disconnected) the following members " +
            "(vms remain up): " + memberSet + "; shutDownAll duration " + duration + "ms");
        Log.getLogWriter().info(memberSet.size() + " members shut down during recovery phase: " + memberSet + ", target number is " + vmList.size());
      } catch (AdminException e1) {
        throw new TestException(TestHelper.getStackTrace(e1));
      }
    }

    for (Thread aThread: threads) {
      try {
        aThread.join();
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    // restart the jvms from the second round of shutDownAll
    StopStartVMs.stopStartVMs(vmListExcludeLocators, stopModes);
  }

  /** Verify that we have the expected number of regions and that the regions are of
   *  the expected sizes.
   */
  public static void HydraTask_verifyRegionSizes() {
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    long expectedSize = NameFactory.getPositiveNameCounter();
    long expectedNumRegions = maxPRs + maxReplicates;
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    if (regionSet.size() != expectedNumRegions) {
      throw new TestException("Expected " + expectedNumRegions + " but have " + regionSet.size() +
          regionSet);
    }
    StringBuffer regSizeStr = new StringBuffer();
    for (Region aRegion: regionSet) {
      int size = aRegion.size();
      if (aRegion.getAttributes().getDataPolicy().isEmpty()) {
        if (size != 0) {
          throw new TestException("Expected " + aRegion.getFullPath() + " to be size 0 but it is " + size);
        }
      } else {
        if (size != expectedSize) {
          throw new TestException("Expected " + aRegion.getFullPath() + " to be size " + expectedSize 
              + " but it is " + size);
        }
      }
      regSizeStr.append(aRegion.getFullPath() + " is size " + size + "\n");
    }
    Log.getLogWriter().info("Verified that regions have correct sizes: " + regSizeStr);
  }
  
  /** Verify that we have the expected number of regions and that the regions are of
   *  the expected sizes for those regions created by customer xml.
   */
  public static void HydraTask_verifyXmlRegionSizes() {
    verifyXmlRegionSizes(false);
  }
  
  public static void HydraTask_verifyXmlRegionSizesForWAN() {
    verifyXmlRegionSizes(true);
  }
  
  static void verifyXmlRegionSizes(boolean isWANSetup) {
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    long expectedSize = NameFactory.getPositiveNameCounter();
    final long expectedNumRegions = 30;
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region<?, ?>> regionSet = new HashSet(rootRegions);
    Log.getLogWriter().info("Printing region hierarchy with sizes on Server side: ");
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    if (regionSet.size() != expectedNumRegions) {
      throw new TestException("Expected " + expectedNumRegions + " but have " + regionSet.size() +
          regionSet);
    }
    // the following regions have all values put by clients; other regions do not due to being local scope
    // or being defined in the server but not  the client (clients do the load step on the regions they know about)
    String[] regionsWithFullSize = new String[] {
        "/matchRule", "/allocationRules", "/resequence", "/break", "/product", "/routingEndpoint", "/ldn_applianceAllocation",
        "/chi_applianceAllocation", 
        "/cacheAdmin", "/backOfficeAccount", "/kpiRuleMetric", "/backOfficeAccountCollection", "/routingRule", "/tradeInstructionCommand",
        "/latencyMetric", "/adapterAmqpState", "/chi_audit", "/adapterState", "/tradeInstruction", "/cacheResponsibility", "/ldn_audit"
    };
    List regionsWithFullSizeList = Arrays.asList(regionsWithFullSize);
    StringBuffer regSizeStr = new StringBuffer();
    for (Region aRegion: regionSet) {
      if (regionsWithFullSizeList.contains(aRegion.getFullPath())) {
        int size = aRegion.size();
        if (isWANSetup) {
          verifyXmlRegionSizeForWAN(aRegion, size, expectedSize);
        } else {
          verifyXmlRegionSize(aRegion, size, expectedSize);
        }
        regSizeStr.append(aRegion.getFullPath() + " is size " + size + "\n");
      }
    }
    Log.getLogWriter().info("Verified that regions have correct sizes: " + regSizeStr);
  }
  
  public static void verifyXmlRegionSize(Region aRegion, int size, long expectedSize) {
    if (aRegion.getAttributes().getDataPolicy().isEmpty()) {
      if (size != 0) {
        throw new TestException("Expected " + aRegion.getFullPath() + " to be size 0 but it is " + size);
      }
    } else {
      if (size != expectedSize) {
        throw new TestException("Expected " + aRegion.getFullPath() + " to be size " + expectedSize 
            + " but it is " + size);
      }
    }
  }
  
  public static void verifyXmlRegionSizeForWAN(Region aRegion, int size, long expectedSize) {
    Log.getLogWriter().info("Region size verfication for region: " + aRegion.getName());
    Log.getLogWriter().info("Region's size = " + size);
    Log.getLogWriter().info(
        "Expected region Size = "
            + expectedSize);
    if (aRegion.getAttributes().getDataPolicy().isEmpty()) {
      if (size != 0) {
        throw new TestException("Expected " + aRegion.getFullPath() + " to be size 0 but it is " + size);
      }
    } else if (aRegion.getAttributes().getEntryTimeToLive().getTimeout() != 0) {
        int timeOut = aRegion.getAttributes().getEntryTimeToLive().getTimeout();
        if (timeOut > 0 && timeOut <= 30) {
          if (size != 0) {
            throw new TestException("Expected " + aRegion.getFullPath()
                + " to be size 0 because ttl = "
                + aRegion.getAttributes().getEntryTimeToLive().getTimeout()
                + "  But it is " + size);
          }
        }
      } else if (!aRegion.getAttributes().getEnableGateway()) {
        boolean singleSitePopulation = TestConfig.tab().booleanAt(WANQueryPrms.populateDataSingleSite, false);
        if (singleSitePopulation) {
          if (!(size == expectedSize || size == 0)) {
            throw new TestException("Expected " + aRegion.getFullPath() + " size to be either 0 or " + expectedSize
                + " but it is " + size);
          } 
        } else if (size != expectedSize/2) {
          throw new TestException("Expected " + aRegion.getFullPath() + " to be size " + expectedSize/2 
              + " but it is " + size);
        } 
      } else if (size != expectedSize) {
        throw new TestException("Expected " + aRegion.getFullPath() + " to be size " + expectedSize 
            + " but it is " + size);
      } else {
        Log.getLogWriter().info(
            "Region size matched expected: for region " + aRegion
                + " Size: " + size + " expectedSize" + expectedSize);
      }
  }
  
  /** Verify that we have the expected number of regions and that the regions are of
   *  the expected sizes for those regions created by customer xml.
   */
  public static void HydraTask_verifyXmlRegionSizesAfterRecovery() {
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    long expectedSize = NameFactory.getPositiveNameCounter();
    final long expectedNumRegions = 30;
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region<?, ?>> regionSet = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    if (regionSet.size() != expectedNumRegions) {
      throw new TestException("Expected " + expectedNumRegions + " but have " + regionSet.size() +
          regionSet);
    }
    // the following regions have all values put by clients; other regions do not due to being local scope
    // or being defined in the server but not  the client (clients do the load step on the regions they know about)
    // or because they are not persistent (and this method is run after disk recovery)
    String[] regionsWithFullSize = new String[] {
        "/matchRule", "/allocationRules", "/break", "/product", "/routingEndpoint", 
        "/backOfficeAccount", "/backOfficeAccountCollection", "/routingRule", "/tradeInstructionCommand",
        "/chi_audit", "/adapterState", "/tradeInstruction", "/ldn_audit"
    };
    List regionsWithFullSizeList = Arrays.asList(regionsWithFullSize);
    StringBuffer regSizeStr = new StringBuffer();
    for (Region aRegion: regionSet) {
      if (regionsWithFullSizeList.contains(aRegion.getFullPath())) {
        int size = aRegion.size();
        if (aRegion.getAttributes().getDataPolicy().isEmpty()) {
          if (size != 0) {
            throw new TestException("Expected " + aRegion.getFullPath() + " to be size 0 but it is " + size);
          }
        } else {
          if (size != expectedSize) {
            throw new TestException("Expected " + aRegion.getFullPath() + " to be size " + expectedSize 
                + " but it is " + size);
          }
        }
        regSizeStr.append(aRegion.getFullPath() + " is size " + size + "\n");
      }
    }
    Log.getLogWriter().info("Verified that regions have correct sizes: " + regSizeStr);
  }

  /** Log the execution number of this serial task.
   */
  static protected long  logExecutionNumber() {
    long exeNum = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.executionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    return exeNum;
  }

  public static void HydraTask_waitForSilence() {
    SummaryLogListener.waitForSilence(30, 1000);
  }

  public static void HydraTask_waitLongerForSilence() {
    SummaryLogListener.waitForSilence(240, 1000);
  }

  /** Task to do a shutDownAll, then restart
   * 
   */
  public static void HydraTask_startupShutdown() {
    logExecutionNumber();
    long sleepSecBeforeShutDownAll = RecoveryPrms.getSleepSecBeforeShutDownAll(); // defaults to 0
    if (sleepSecBeforeShutDownAll > 0) {
      Log.getLogWriter().info("Sleeping for " + sleepSecBeforeShutDownAll + " seconds");
      MasterController.sleepForMs((int)sleepSecBeforeShutDownAll * 1000);
    }
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> locators = StopStartVMs.getMatchVMs(vmList, "locator");
    List<ClientVmInfo> vmListExcludeLocators = new ArrayList<ClientVmInfo>();
    vmListExcludeLocators.addAll(vmList);
    vmListExcludeLocators.removeAll(locators);
    Log.getLogWriter().info("vmList is " + vmListExcludeLocators);
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    Cache theCache = CacheHelper.getCache();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    List stopModes = new ArrayList();
    stopModes.add(ClientVmMgr.MeanKill);
    Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS, stopModes);
    List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
    Set shutdownAllResults = (Set)(tmp[1]);
    if (shutdownAllResults.size() != vmListExcludeLocators.size()) { // shutdownAll did not return the expected number of members
      throw new TestException("Expected shutDownAllMembers to return " + vmListExcludeLocators.size() +
          " members in its result, but it returned " + shutdownAllResults.size() + ": " + shutdownAllResults);
    }
    if (shutdownAllResults.size() > shutDownAllVMs.size()) {
      // shutdownAllResults is bigger because we shutdown this vm also, but it was not stopped
      // just disconnected
      boolean cacheClosed = theCache.isClosed();
      if (cacheClosed) {
        Log.getLogWriter().info("shutDownAllMembers disconnected this vm");
        testInstance = null;
      } else {
        throw new TestException("shutDownAllMembers should have disconnected this vm, but the cache " +
            theCache + " isClosed is " + cacheClosed);
      }
    }

    // restart the stopped vms; this will run a dynamic init task to recover from disk
    Log.getLogWriter().info("Restarting the vms");
    List dataStoreVMs = new ArrayList();
    List proxyVMs = new ArrayList();
    Map sharedMap = RecoveryBB.getBB().getSharedMap().getMap();
    for (Object key: sharedMap.keySet()) {
      if (key instanceof String)  {
        if (((String)key).startsWith("proxy")) {
          proxyVMs.add(sharedMap.get(key));
        } else if (((String)key).startsWith("dataStore")) {
          dataStoreVMs.add(sharedMap.get(key));
        }
      }
    }
    Log.getLogWriter().info("Restarting the vms, dataStoreVMS: " + dataStoreVMs + ", proxyVMs: " + proxyVMs);
    StopStartVMs.startVMs(dataStoreVMs);
    if (proxyVMs.size() > 0) {
      StopStartVMs.startVMs(proxyVMs);
    }

    long serversAreBack = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.serversAreBack);
    Log.getLogWriter().info("serversAreBack counter is " + serversAreBack);
  }

  // @todo lynn Note this task needs
  // to be upgraded to know when shutDownAll is running. Currently this is just based
  // on sleeping, which may or may not kill during a shutDownAll.
  /** Task to run shutdownAll, then kill the vms being shutdown.
   */
  public static void HydraTask_startupShutdownWithKill() {
    logExecutionNumber();
    final List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    Log.getLogWriter().info("vmList is " + vmList);
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    final List stopModes = new ArrayList();
    for (int i = 0; i < vmList.size(); i++) {
      stopModes.add(ClientVmMgr.MeanKill);
    }

    boolean killDuringShutDownAll = RecoveryPrms.getKillDuringShutDownAll();
    Thread killThread = null;
    if (killDuringShutDownAll) {
      killThread = new Thread(new Runnable() {
        public void run() {
          int msToSleep = TestConfig.tab().getRandGen().nextInt(5, 35);
          msToSleep = msToSleep * 1000;
          Log.getLogWriter().info("Sleeping for " + msToSleep + " ms before killing");
          MasterController.sleepForMs(msToSleep);
          StopStartVMs.stopVMs(vmList, stopModes);
          Log.getLogWriter().info("killThread is terminating");
        }
      });
      killThread = new HydraSubthread(killThread);
      killThread.start();
    }

    // Invoke shutDownAllMembers
    Log.getLogWriter().info("AdminDS " + adminDS + " is shutting down all members...");
    Set<DistributedMember> memberSet;
    try {
      long startTime = System.currentTimeMillis();
      memberSet = adminDS.shutDownAllMembers();
      long duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("AdminDS " + adminDS + " shut down (disconnected) the following members " +
          "(vms remain up): " + memberSet + "; shutDownAll duration " + duration + "ms");
    } catch (AdminException e1) {
      throw new TestException(TestHelper.getStackTrace(e1));
    }

    // can't do this check in case the kill stopped shutdownAll from completing
    //    if (memberSet.size() != vmList.size()) { // shutdownAll did not return the expected number of members
    //      throw new TestException("Expected shutDownAllMembers to return " + vmList.size() +
    //          " members in its result, but it returned " + memberSet.size() + ": " + memberSet);
    //    }

    if (killThread == null) {
      StopStartVMs.stopVMs(vmList, stopModes);
    } else {
      Log.getLogWriter().info("Waiting for killThread to terminate");
      try {
        killThread.join();
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    // restart the stopped vms; this will run a dynamic init task to recover from disk
    StopStartVMs.startVMs(vmList);
  }
  
  public synchronized static void HydraTask_initWithXml() {
    if (testInstance == null) {
      testInstance = new StartupShutdownTest();
      testInstance.initializeInstance();
      String newXmlFile = "VmId_" + RemoteTestModule.getMyVmid() + ".xml";
      File aFile = new File(newXmlFile);
      if (!aFile.exists()) {
        String JTESTS = System.getProperty("JTESTS");
        String xmlTemplate = JTESTS + File.separator + "largeScale" + File.separator + "newedge" + File.separator + "gemfire-server.xml.template";
        modifyXml(xmlTemplate, newXmlFile);
      }
      CacheHelper.createCacheFromXml(newXmlFile);
      Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
      int myVmId = RemoteTestModule.getMyVmid();
      RecoveryBB.getBB().getSharedMap().put("dataStoreJvm_" + myVmId, new ClientVmInfo(myVmId));
    }
  }
 
  public synchronized static void HydraTask_initClientWithXml() {
    if (testInstance == null) {
      testInstance = new StartupShutdownTest();
      testInstance.initializeInstance();

      // create client region to get the ports set in the xml; this is a workaround done
      // 11/2011 until Lise finishes gemfirexd work and has time for hydra support for this
      String newXmlFile = "VmId_" + RemoteTestModule.getMyVmid() + ".xml";
      File aFile = new File(newXmlFile);
      if (!aFile.exists()) {
        CacheHelper.createCache("cache1");
        RegionHelper.createRegion("clientRegion");
        CacheHelper.generateCacheXmlFile("cache1", "clientRegion", null, "clientPool", newXmlFile);

        // disconnect and modify the xml to include customer defined regions; then we can
        // reinit this jvm using the desired xml
        DistributedSystemHelper.disconnect(); 
        String JTESTS = System.getProperty("JTESTS");
        String xmlTemplate = JTESTS + File.separator + "largeScale" + File.separator + "newedge" + File.separator + "gemfire-client.xml.template";
        modifyClientXml(xmlTemplate, newXmlFile);
      }
      CacheHelper.createCacheFromXml(newXmlFile);
      Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
    }
    currentNumKeys.set(new Integer(0));
    Log.getLogWriter().info("currentNumKeys is " + currentNumKeys);
  }
  
  /** Modify the client xml file by replacing the temporary region already in it
   *  with the customer defined regions in the template. 
   * @param xmlTemplate Contains customer defined regions; not a whole and complete xml file
   * @param newXmlFile A whole and complete xml file containing a temporary region; replace it
   *                   with the customer regions in xmlTemplate. 
   */
  private static void modifyClientXml(String xmlTemplate, String newXmlFile) {
    StringBuffer newXml = new StringBuffer();
    try {
      File aFile = new File(newXmlFile); 
      BufferedReader reader = new BufferedReader(new FileReader(aFile.getAbsoluteFile()));
      String line = reader.readLine();
      while (line != null) {
        String searchStr = "region name=";
        int index = line.indexOf(searchStr);
        if (index >= 0) { // add the template to the rest of the newXml text
          File templateFile = new File(xmlTemplate);
          BufferedReader templateReader = new BufferedReader(new FileReader(templateFile.getAbsoluteFile()));
          String templateLine = templateReader.readLine();
          while (templateLine != null) {
            newXml.append(templateLine + "\n");
            templateLine = templateReader.readLine();
          }
          newXml.append("</cache>");
          break;
        }
        newXml.append(line + "\n");
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    PrintWriter outFile;
    try {
      outFile = new PrintWriter(new FileOutputStream(new File(newXmlFile)));
      outFile.print(newXml.toString());
      outFile.flush();
      outFile.close();
    } catch (FileNotFoundException e) {
      Log.getLogWriter().info("Unable to create " + newXmlFile + " due to " + e.toString());
    }
  }

  /** Modify an xml template file provided by New Edge
   * 
   * @param xmlFilePath The path to a file containing xml to modify
   * @param newXmlFilePath The modified xml is written to this file.
   */
  private static void modifyXml(String xmlFilePath, String newXmlFilePath) {
    StringBuffer newXml = new StringBuffer();
    try {
      File aFile = new File(xmlFilePath); 
      BufferedReader reader = new BufferedReader(new FileReader(aFile.getAbsoluteFile()));
      String line = reader.readLine();
      String currDir = System.getProperty("user.dir");
      String diskDirStr = currDir + File.separator + "vm_" + RemoteTestModule.getMyVmid() + "_disk_1";
      File diskDir = new File(diskDirStr);
      diskDir.mkdirs();
      String osName= System.getProperty("os.name");
      Log.getLogWriter().info("diskDirStr before = " + diskDirStr);
      if (osName.indexOf("Win") != -1 || osName.indexOf("win") != -1) {
        diskDirStr = replaceSingleBackSlashWithDoublBackSlash(diskDirStr).toString();
      }
      Log.getLogWriter().info("diskDirStr after = " + diskDirStr);
      while (line != null) {
        String searchStr = "%DISK_DIR%";
        int index = line.indexOf(searchStr);
        if (index >= 0) {
          line = line.replaceAll("%DISK_DIR%", diskDirStr);
        }
        newXml.append(line + "\n");
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    PrintWriter outFile;
    try {
      outFile = new PrintWriter(new FileOutputStream(new File(newXmlFilePath)));
      outFile.print(newXml.toString());
      outFile.flush();
      outFile.close();
    } catch (FileNotFoundException e) {
      Log.getLogWriter().info("Unable to create " + newXmlFilePath + " due to " + e.toString());
    }
  }
  
  static StringBuffer replaceSingleBackSlashWithDoublBackSlash(String input) {
    StringBuffer output = new StringBuffer();
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == '\\') {
        output.append('\\');
        output.append('\\');
      } else {
        output.append(c);
      }
    }
    return output;
  }

  /** Generate a cache xml file
   * 
   * @param fileName The name of the xml file to create. 
   */
  private static void generateCacheXmlFile(String fileName) {
    File aFile = new File(fileName);
    if (aFile.exists()) {
      return;
    }
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new FileWriter(new File(fileName)));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    CacheXmlGenerator.generate(CacheHelper.getCache(), pw);
    pw.close();
    Log.getLogWriter().info("Generated XML file: " + fileName);
  }


}
