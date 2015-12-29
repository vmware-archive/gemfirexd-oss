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
import hydra.ClientVmNotFoundException;
import hydra.DiskStorePrms;
import hydra.DistributedSystemBlackboard;
import hydra.DistributedSystemHelper;
import hydra.FileUtil;
import hydra.GatewayHubBlackboard;
import hydra.GatewayHubHelper;
import hydra.GsRandom;
import hydra.HostHelper;
import hydra.HydraRuntimeException;
import hydra.HydraThreadGroup;
import hydra.HydraThreadLocal;
import hydra.HydraVector;
import hydra.Log;
import hydra.MasterController;
import hydra.ProcessMgr;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.VersionPrms;
import hydra.blackboard.Blackboard;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import management.test.cli.CommandTest;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import parReg.ParRegUtil;
import util.AdminHelper;
import util.BaseValueHolder;
import util.CliHelper;
import util.CliHelperPrms;
import util.NameBB;
import util.NameFactory;
import util.PRObserver;
import util.PersistenceUtil;
import util.RandomValues;
import util.SilenceListener;
import util.StopStartPrms;
import util.StopStartVMs;
import util.SummaryLogListener;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.VHDataSerializable;
import util.VHDataSerializableInstantiator;
import util.VHDataSerializer;
import util.ValueHolder;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlParser;

/**
 * @author lynn
 *
 */
public class RecoveryTest {

  /** instance of this test class */
  public static RecoveryTest testInstance = null;

  /** compaction test steps */
  private static final String STARTING_STEP = "Starting";
  private static final String DESTROY_STEP = "Destroying";
  private static final String COMPACTION_STEP = "Compaction";
  private static final String STOP_START_STEP = "Stopping/Starting";
  private static final String VERIFY_STEP = "Verifying";
  private static final String ADD_STEP = "Adding";

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;
  protected List<Region> allRegions = new ArrayList();
  private int regionNameCounter = 1; // used to create unique region names
  private RandomValues randomValues = new RandomValues(); // used to create payload
  private boolean uniqueKeys = true;
  private boolean extraRegionsCreated = false;

  // static fields
  static Cache theCache = null;

  // fields to help coordinate controller and responder
  private AtomicInteger compactionCoordinator = new AtomicInteger(0);
  private AtomicInteger verifyCoordinator = new AtomicInteger(0);
  private AtomicInteger startingCoordinator = new AtomicInteger(0);
  protected AtomicInteger readyForSnapshot = new AtomicInteger(0);
  private static HydraThreadLocal destroysCompleted = new HydraThreadLocal();
  private static HydraThreadLocal addsCompleted = new HydraThreadLocal();
  protected static HydraThreadLocal threadIsPaused = new HydraThreadLocal();

  // blackboard keys
  private static final String persistentRegionKey = "VmContainsPersistentRegions_";
  private static final String startupStatsForVmKey = "startupStatsForVM_";
  private static final String localInitializationsKey = "localInitializationsForRegion_";
  private static final String remoteInitializationsKey = "remoteInitializationsForRegion_";
  private static final String vmMarkerKey = "markerForVM_";
  private static final String expectDiskRecoveryKey = "expectDiskRecoveryInVM_";
  private static final String allRegionNamesKey = "allRegions";
  private static final String allRegionsSnapshotKey = "allRegionsSnapshot";
  private static final String uniqueKeyIndex = "uniqueKeyIndexForThread_";
  private static final String recoveryCanBeTieKey = "recoveryCanBeTie";
  public static final String oldVersionXmlKey = "oldVersionXmlFile";
  public static final String newVersionXmlKey = "newVersionXmlFile";
  private static final String useXmlKey = "createRegionsWithXml";
  private static final String oldVersionXmlFilesKey = "oldVersionXmlFiles";
  private static final String newVersionXmlFilesKey = "newVersionXmlFiles";
  private static final String singleConvertedVmIdKey = "singleConvertedVmId";

  //operations
  static protected final int ENTRY_ADD_OPERATION = 1;
  static protected final int ENTRY_DESTROY_OPERATION = 2;
  static protected final int ENTRY_INVALIDATE_OPERATION = 3;
  static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
  static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
  static protected final int ENTRY_UPDATE_OPERATION = 6;
  static protected final int ENTRY_GET_OPERATION = 7;
  static protected final int ENTRY_GET_NEW_OPERATION = 8;
  static protected final int ENTRY_PUTALL_OPERATION = 9;
  static protected final int CACHE_OPERATIONS = 10;
  static protected final int PUTS_FOR_KEY_ORDER_POLICY = 11;
  static protected final int CLEAR_REGION = 12;

  //String prefixes for event callback object
  protected static final String getCallbackPrefix = "Get originated in pid ";
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
  protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
  protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
  

  //=================================================
  // initialization methods
  
  public synchronized static void HydraTask_createLocator() throws Throwable {
    hydra.DistributedSystemHelper.createLocator();
  }

  public synchronized static void HydraTask_startLocatorAndDS()
      throws Throwable {
    hydra.DistributedSystemHelper.startLocatorAndDS();
  }

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() throws Throwable {
    if (testInstance == null) {
      testInstance = new RecoveryTest();
      testInstance.initializeInstance();
      theCache = CacheHelper.createCache("cache1");
      List regionConfigNames = RecoveryPrms.getRegionConfigNames();
      testInstance.createRegionHier(regionConfigNames, null);
      Log.getLogWriter().info(regionHierarchyToString());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        boolean registerInterest = RecoveryPrms.getRegisterInterest();
        if (registerInterest) {
          registerInterest();
        }
      }
    }
    testInstance.initializeInstancePerThread();
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() throws Throwable {
    RecoveryTestVersionHelper.initPdxClassLoader();
    if (testInstance == null) {
      testInstance = new RecoveryTest();
      testInstance.initializeInstance();
      theCache = CacheHelper.createCache("cache1");
      List regionConfigNames = RecoveryPrms.getRegionConfigNames();
      testInstance.createRegionHier(regionConfigNames, null);
      Log.getLogWriter().info(regionHierarchyToString());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    testInstance.initializeInstancePerThread();
  }

  /** Initialization for controller for compaction test.
   */
  public synchronized static void HydraTask_initializeController() {
    if (testInstance == null) {
      testInstance = new RecoveryTest();
      testInstance.initializeInstance();
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.firstKeyIndexKey, new Long(1));
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.lastKeyIndexKey, new Long(RecoveryPrms.getMaxNumEntriesPerRegion()));
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, STARTING_STEP);
    }
  }

  /** Initialization for test that run with multi-regions and multi-diskStores
   */
  public synchronized static void HydraTask_multiRegionInitialize() throws Throwable {
    if (testInstance == null) {
      testInstance = new RecoveryTest();
      testInstance.initializeInstance();
      String xmlFileName = getExistingXmlFile();
      RecoveryBB.getBB().getSharedMap().replace(useXmlKey, null, RecoveryPrms.getCreateRegionsWithXml());
      Boolean useXml = (Boolean)RecoveryBB.getBB().getSharedMap().get(useXmlKey);
      if (useXml && (xmlFileName != null)) {
        if (!RecoveryPrms.getConvertWithNewVersionXml()) { // handle conversion tests
          List<String> oldVersionXmlFiles = (List<String>)RecoveryBB.getBB().getSharedMap().get(oldVersionXmlFilesKey);
          List<String> newVersionXmlFiles = (List<String>)RecoveryBB.getBB().getSharedMap().get(newVersionXmlFilesKey);
          if (newVersionXmlFiles != null) { // is null if this is not a conversion test
            int myVmId = RemoteTestModule.getMyVmid();
            for (int i = 0; i < newVersionXmlFiles.size(); i++) {
              String currXmlFileName = newVersionXmlFiles.get(i);
              if (currXmlFileName.startsWith("vm_" + myVmId + "_")) {
                xmlFileName = oldVersionXmlFiles.get(i);
                break;
              }
            }
          }
        }

        Log.getLogWriter().info("Creating regions from " + xmlFileName);
        theCache = CacheHelper.createCacheFromXml(xmlFileName);
        testInstance.allRegions = new ArrayList();
        testInstance.allRegions.addAll(CacheHelper.getCache().rootRegions());
        for (Region aRegion: CacheHelper.getCache().rootRegions()) {
           testInstance.allRegions.addAll(aRegion.subregions(true));
        }
      } else {
        theCache = CacheHelper.createCache("cache1");
        List regionConfigNames = TestConfig.tab().vecAt(RegionPrms.names);
        List diskStoreNames = TestConfig.tab().vecAt(DiskStorePrms.names);
        testInstance.createRegionHier(regionConfigNames, diskStoreNames);
        if (testInstance.allRegions.size() < regionConfigNames.size()) {
          throw new TestException("Test configuration problem; test did not create enough regions " +
              "to create all regions specified in RegionPrms.names; number of regions specified in " +
              "RegionPrms.names: " + regionConfigNames.size() + ", number of regions created: " +
              testInstance.allRegions.size());
        }
      }
      Log.getLogWriter().info(regionHierarchyToString());
    }
    testInstance.initializeInstancePerThread();
  }

  /** Hydra task to create an xml file for all regions currently existing
   * 
   */
  public static void HydraTask_createXmlFile() {
    RecoveryTestVersionHelper.createXmlFile();
  }

  /** Return the xml file name for this vm. 
   *  @return The name of an xml file for this vm, or null if the file does not exist.
   */
  private static String getExistingXmlFile() {
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    int myVmID = RemoteTestModule.getMyVmid();
    String[] dirContents = currDir.list();
    for (String fileName: dirContents) {
      if ((fileName.startsWith("vm_" + myVmID + "_") ||
          (fileName.startsWith("vm_" + myVmID + "."))) &&
          (fileName.endsWith(".xml"))) {
         return fileName;
      }
    }
    return null; 
  }
  
  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    RecoveryTestVersionHelper.installPersistenceObserver();
    uniqueKeys = RecoveryPrms.getUseUniqueKeys();
    RecoveryBB.getBB().getSharedMap().put(RecoveryBB.shutDownAllKey, new Boolean(false));
    RecoveryTestVersionHelper.installPRObserver();
    if (RecoveryPrms.getRegisterSerializer()) {
      Log.getLogWriter().info("Registering " + VHDataSerializer.class.getName());
      DataSerializer.register(VHDataSerializer.class);
    }
    if(RecoveryPrms.setIgnorePreallocate()){
      Log.getLogWriter().info("workaround for #50184, setting DiskStoreImpl.SET_IGNORE_PREALLOCATE = true");
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    }
  }

  /** Initialize an instance of this test class, called for each thread
   * 
   */
  private void initializeInstancePerThread() {
    destroysCompleted.set(new Boolean(false));
    addsCompleted.set(new Boolean(false));
    threadIsPaused.set(new Boolean(false));
    int thisThreadID = RemoteTestModule.getCurrentThread().getThreadId();
    String mapKey = uniqueKeyIndex + thisThreadID;
    Object mapValue = RecoveryBB.getBB().getSharedMap().get(mapKey);
    if (mapValue == null) {
      RecoveryBB.getBB().getSharedMap().put(mapKey, thisThreadID);
    } else {
      RecoveryBB.getBB().getSharedMap().put(mapKey, mapValue);
    }
  }
  
  //=================================================
  // hydra task methods

  /** Hydra task to load the region(s) with keys/values. This is a batched task.
   *   Load the region hierarchy with RecoveryPrms.maxNumEntries across all
   *   regions. 
   * 
   */
  public static void HydraTask_load() {
    int maxNumEntriesPerRegion = RecoveryPrms.getMaxNumEntriesPerRegion();
    boolean allAddsCompleted = testInstance.addToRegions(maxNumEntriesPerRegion);
    if (allAddsCompleted) {
      throw new StopSchedulingTaskOnClientOrder("Put " + maxNumEntriesPerRegion + " entries in each of " +
          testInstance.allRegions.size() + " regions");
    }
  }

  /** Hydra task to run the compaction controller thread. This method controls
   *   what the responder vms do. 
   * 
   */
  public static void HydraTask_compactionController() {
    testInstance.compactionController();
  }

  /** Hydra task to run the responder threads for compaction tests. This method
   *   responds to directives from the compaction controller. 
   */
  public static void HydraTask_compactionResponder() {
    testInstance.compactionResponder();
  }

  /** Hydra task to verify that the latest disk files are recovered.
   */
  public static void HydraTask_latestRecoveryTest() throws ClientVmNotFoundException, InterruptedException {
    latestRecoveryTest();
  }

  /** Record disk startup state to the blackboard for later validation
   * 
   */
  public  static void HydraTask_recordStartupState() {
    Map statsMap = new TreeMap();
    for (Region aRegion: testInstance.allRegions) {
      DiskRegionStats diskStats = ((LocalRegion)aRegion).getDiskRegion().getStats();
      String regionName = aRegion.getFullPath();
      statsMap.put(localInitializationsKey + regionName, diskStats.getLocalInitializations());
      statsMap.put(remoteInitializationsKey + regionName, diskStats.getRemoteInitializations());
    }
    final String key = startupStatsForVmKey + RemoteTestModule.getMyVmid();
    RecoveryBB.getBB().getSharedMap().put(key, statsMap);
    Log.getLogWriter().info("Wrote to blackboard key " +key + ": " + statsMap);
  }

  /** Verify the expected sizes of all regions
   * 
   */
  public  static void HydraTask_verifyRegions() {
    testInstance.verifyRegions();
  }

  /** Verify that each vm got it's data from disk or gii as appropriate
   * 
   */
  public  static void HydraTask_verifyStartupState() {
    verifyStartupState();
  }

  /** Hydra task to mark this vm in the blackboard with a string.
   */
  public  static void HydraTask_markVmInBlackboard() {
    RecoveryBB.getBB().getSharedMap().put(vmMarkerKey + RemoteTestModule.getMyVmid(), 
        RecoveryPrms.getVmMarker());
  }

  /** Verify that all regions are empty
   * 
   */
  public  static void HydraTask_verifyEmptyRegions() {
    for (Region aRegion: testInstance.allRegions) {
      if (aRegion.size() != 0) {
        throw new TestException("Expected " + aRegion.getFullPath() + " to be empty, but its size is " + aRegion.size());
      }
    }
  }

  /** For concurrent recovery tests, control when vms are stopped and restarted
   *   and create blackboard information for verification.
   * 
   */
  public static void HydraTask_concRecoverLatestController() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    recoverLatestController();
  }

  /** For concurrent recovery tests, respond to the concRecoverLatestController
   * 
   */
  public static void HydraTask_concRecoverLatestResponder() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    testInstance.concRecoverLatestResponder();
  }

  /** Verify all regions from the snapshot
   * 
   */
  public static void HydraTask_verifyFromSnapshot() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    testInstance.verifyFromSnapshot();
  }

  public static void HydraTask_writeSnapshot() {
    testInstance.writeSnapshot();
  }
  
  /** Verify all regions from the snapshot and wait for all threads to complete verify
   * 
   */
  public static void HydraTask_verifyFromSnapshotAndSync() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    testInstance.verifyFromSnapshot();
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter", 
        RecoveryBB.doneVerifyingCounter,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads(),
        true, -1, 2000);
  }
  
  /** Verify all regions from the snapshot and wait for all threads to complete verify
   * 
   */
  public static void HydraTask_convertVerifyFromSnapshotAndSync() {
    // the first time we run this on a vm restart, we might have additional extra regions
    // not in the snapshot (because we converted to a new xml file that contained extra
    // persistent regions); add them to the snapshot so verifyFromSnapshot won't complain
    if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
        RecoveryPrms.getConvertWithNewVersionXml()) {
      List[] aList = extractFromXmlFile(getXmlFileName(RemoteTestModule.getMyVmid()));
      List<String> regionNames = aList[0];
      Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
      for (int i = 0; i < regionNames.size(); i++) {
        String rName = regionNames.get(i);
        if (rName.startsWith("/extra")) {
          if (!allRegionsSnapshot.containsKey(rName)) {
            Log.getLogWriter().info("Adding region " + rName + " to snapshot");
            allRegionsSnapshot.put(rName, new HashMap());
          }
        }
      }
      RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    }
    testInstance.verifyFromSnapshot();
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter", 
        RecoveryBB.doneVerifyingCounter,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads(),
        true, -1, 2000);
    HydraTask_prepareForNewVersionOps();
  }

  /** Verify all regions from the snapshot and wait for all threads to complete verify
   * 
   */
  public static void HydraTask_convertVerifyFromSnapshot() {
    // the first time we run this on a vm restart, we might have additional extra regions
    // not in the snapshot (because we converted to a new xml file that contained extra
    // persistent regions); add them to the snapshot so verifyFromSnapshot won't complain
    if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
        RecoveryPrms.getConvertWithNewVersionXml()) {
      List[] aList = extractFromXmlFile(getXmlFileName(RemoteTestModule.getMyVmid()));
      List<String> regionNames = aList[0];
      Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
      for (int i = 0; i < regionNames.size(); i++) {
        String rName = regionNames.get(i);
        if (rName.startsWith("/extra")) {
          if (!allRegionsSnapshot.containsKey(rName)) {
            Log.getLogWriter().info("Adding region " + rName + " to snapshot");
            allRegionsSnapshot.put(rName, new HashMap());
          }
        }
      }
      RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    }
    MasterController.sleepForMs(15000); // allow gateway hub to start delivering from persistent queue
    SummaryLogListener.waitForSilence(30, 2000);
    testInstance.verifyFromSnapshot();
    HydraTask_prepareForNewVersionOps();
  }

  /** For concurrent recovery tests that stop all vms concurrently.
   * 
   */
  public static void HydraTask_concRecoverAllController() {
    concRecoverAllController();
  }

  /** Verify all regions from the snapshot after choosing 1 vm to write its
   * state to the blackboard. 
   * 
   */
  public static void HydraTask_verifyFromLeaderSnapshot() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    testInstance.verifyFromLeaderSnapshot();
  }
  
  /** Task to do random operations
   * 
   */
  public static void HydraTask_doOperations() throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    testInstance.doOperations(minTaskGranularityMS);
  }
  
  /** Task to do random operations and pause then the controller thread signals
   *  pausing by incrementing the pausing counter.
   * 
   */
  public static void HydraTask_doOperationsWithPause() throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    testInstance.doOperations(minTaskGranularityMS);
    if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing) > 0) {
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
      String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
      HydraThreadGroup tg = TestConfig.getInstance().getThreadGroup(tgname);
      int threadCount = tg.getTotalThreads(); 
      int desiredCounterValue = threadCount + 1; // +1 because the controller initiates pausing by incrementing the counter
      TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing", 
          RecoveryBB.pausing, desiredCounterValue, true, -1, 2000);
      // now all the old vm threads have paused
      long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
      if (leader == 1) {
        testInstance.writeSnapshot();
        String stopMode = TestConfig.tab().stringAt(StopStartPrms.stopModes);
        if (stopMode.equalsIgnoreCase("shutDownAll")) {
          // we need to run shutDownAll from the DS that should have the shutDownAll apply to it
          // since the upgradeController method (running with this method) is in the newVersion
          // ds, it can't do shutDownAll, but it will handle other types of shutting down of the
          // old version vms (niceExit, etc)
          AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
          StopStartVMs.shutDownAllMembers(adminDS);
        }
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
      }
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }

  /** Task to do random operations that expect exceptions from shutDownAllMembers
   * 
   */
  public static void HydraTask_doOperationsShutDownAll() throws Exception {
    RecoveryTestVersionHelper.doOperationsShutDownAll(testInstance);
  }

  /** Task to do random operations that expect exceptions from shutDownAllMembers
   * 
   */
  public static void HydraTask_doOperationsHA() throws Exception {
    RecoveryTestVersionHelper.initPdxClassLoader();
    RecoveryTestVersionHelper.doOperationsHA(testInstance);
  }

 /** Task to control the backupResponder vms.
  *
  */
 public static void HydraTask_backupController() {
   new RecoveryTest().backupController();
 }

 /** Task to do random operations and direct other vms to pause and create snapshots.
  *
  */
 public static void HydraTask_backupResponder() throws Exception {
   testInstance.backupResponder();
 }
 
 /** Stop all members and restart to do disk recovery
  * 
  */
 public static void HydraTask_doRecovery() {
   logExecutionNumber();

   // give other vms time to do random ops
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
   MasterController.sleepForMs((int)minTaskGranularityMS);

   // now stop everybody and bring them back for recovery
   List<ClientVmInfo> targetVMs = new ArrayList();
   if (RecoveryPrms.getUseShutDownAll()) {
     AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
     if (adminDS == null) {
       throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
     }
     Object[] anArr = StopStartVMs.shutDownAllMembers(adminDS);
     targetVMs = (List<ClientVmInfo>)(anArr[0]);
   } else {
     List vmIDs = ClientVmMgr.getOtherClientVmids();
     List<String> stopModesList = new ArrayList();
     for (Object vmID: vmIDs) {
       targetVMs.add(new ClientVmInfo((Integer)vmID));
       stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
     }
     StopStartVMs.stopVMs(targetVMs, stopModesList);
   }

   PRObserver.initialize();
   StopStartVMs.startVMs(targetVMs);
 }

 /** Task to control the operationsResponder vms.
   *
   */
  public static void HydraTask_operationsController() {
    new RecoveryTest().operationsController();
  }

  /** Task to do random operations and direct other vms to pause and create snapshots.
   *
   */
  public static void HydraTask_operationsResponder() throws Exception {
    testInstance.operationsResponder();
  }

  /** Task to control the snapshotResponder vms.
   *
   */
  public static void HydraTask_snapshotController() {
    new RecoveryTest().snapshotController();
  }

  /** Task to do random operations, writeSnapshot, export then import snapshot and verify
   *
   */
  public static void HydraTask_snapshotResponder() throws Exception {
    RecoveryTestVersionHelper.snapshotResponder(testInstance);
  }

  /** Task to control the operationsResponder vms.
  *
  */
 public static void HydraTask_diskConvOperationsController() {
   new RecoveryTest().diskConvOperationsController();
 }

 /** Task to do random operations and direct other vms to pause and create snapshots.
  *
  */
 public static void HydraTask_diskConvOperationsResponder() throws Exception {
   if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) <= 1) {
     String clientName = System.getProperty("clientName");
     if (clientName.indexOf("newVersion") >= 0) {
       // this is a newVersion jvm and its the first execution of the
       // controller; wait for the controller to kill this jvm
       Log.getLogWriter().info("Sleeping for 10 seconds...");
       MasterController.sleepForMs(10000);
       return;
     }
   }
   testInstance.operationsResponder();
 }

  /** Serial execution test that steps back and forth between two vms to verify
   *   that the disk files are updated when initialized from gii and can be used for
   *   recovery
   */
  public static void HydraTask_recoverAfterGiiTest() throws Exception {
    RecoveryTestVersionHelper.initPdxClassLoader();
    testInstance.recoverAfterGiiTest();
  }

  /** Task to log the sizes of all regions.
   */
  public static void HydraTask_logRegions() {
    Log.getLogWriter().info(regionHierarchyToString());
  }

  /** Create extra root regions. 
   * 
   */
  public static synchronized void HydraTask_createExtraRegions() {
    if (!testInstance.extraRegionsCreated) {
      final int NUM_EXTRA_REGIONS = 20;
      int regionNameIndex = 1;
      if (System.getProperty("clientName").indexOf("newVersion") >= 0) {
        regionNameIndex = NUM_EXTRA_REGIONS + 1;
      }
      HydraVector aVec = TestConfig.tab().vecAt(RegionPrms.names);
      for (int i = 1; i <= NUM_EXTRA_REGIONS; i++) {
        String regionConfigName = (String)aVec.get(i % aVec.size());
        String regionName = "extra_Region_" + regionNameIndex++;
        AttributesFactory factory = RecoveryTestVersionHelper.setDiskStoreIfNeeded(regionConfigName);
        testInstance.allRegions.add(RegionHelper.createRegion(regionName, factory));
      }
      testInstance.extraRegionsCreated = true;
      Log.getLogWriter().info(regionHierarchyToString());
    }
  }

  /** Create a region for each region defined in hydra.RegionPrms-names
   * 
   */
  public static synchronized void HydraTask_createRegions() {
    if (testInstance == null) {
      testInstance = new RecoveryTest();
      testInstance.initializeInstance();
      int myVmId = RemoteTestModule.getMyVmid();
      String xmlFileName = getXmlFileName(myVmId);
      File xmlFile = new File(xmlFileName);
      boolean postConvert = xmlFile.exists();

      // if we are post-convert, use the appropriate xml file to create cache/regions
      // the appropriate xml file is either the one used for conversion, or a
      // corresponding xml file for those jvms that didn't have their disk files converted
      if (postConvert) { // this is a new version jvm
        if (!RecoveryPrms.getConvertWithNewVersionXml()) {
          List<String> oldVersionXmlFiles = (List<String>)RecoveryBB.getBB().getSharedMap().get(oldVersionXmlFilesKey);
          List<String> newVersionXmlFiles = (List<String>)RecoveryBB.getBB().getSharedMap().get(newVersionXmlFilesKey);
          for (int i = 0; i < newVersionXmlFiles.size(); i++) {
            String currXmlFileName = newVersionXmlFiles.get(i);
            if (currXmlFileName.startsWith("vm_" + myVmId + "_")) {
              xmlFileName = oldVersionXmlFiles.get(i);
              break;
            }
          }
        }
        Log.getLogWriter().info("Creating cache with xml " + xmlFileName);
        theCache = CacheHelper.createCacheFromXml(xmlFileName);
      } else { // this is an old version jvm
        theCache = CacheHelper.createCache("cache1");
        // the following listener is present for disk conversion tests; for other
        // tests it's a noop
        CacheHelper.getCache().getCacheTransactionManager().addListener(new ConverterTxListener());
        testInstance.isBridgeClient = testInstance.isBridgeConfiguration &&
        RecoveryPrms.getRegisterInterest();
        HydraVector aVec = TestConfig.tab().vecAt(RegionPrms.names);
        for (int i = 0; i < aVec.size(); i++) {
          String regionConfigName = (String)aVec.get(i);
          if (!regionConfigName.equals("edgeClientRegion")) {
            String configToUse = regionConfigName;
            if (testInstance.isBridgeClient){
              configToUse = "edgeClientRegion";
            }
            RegionHelper.createRegion("Region_" + (i+1), configToUse);
          }
        }
      }
      Log.getLogWriter().info(regionHierarchyToString());
      testInstance.allRegions = new ArrayList();
      testInstance.allRegions.addAll(CacheHelper.getCache().rootRegions());
      for (Region aRegion: CacheHelper.getCache().rootRegions()) {
         testInstance.allRegions.addAll(aRegion.subregions(true));
      }

      if (testInstance.isBridgeConfiguration) {
        if (testInstance.isBridgeClient) {
          registerInterest();
        }
        if (RecoveryPrms.getStartBridgeServer()) {
          BridgeHelper.startBridgeServer("bridge");
        }
      }
    }
    testInstance.initializeInstancePerThread();
  }
  
  /** Create gateway hub
   * 
   */
  public static void HydraTask_createGatewayHub() {
    String hubConfigName = RecoveryPrms.getHubConfigName();
    Log.getLogWriter().info("Creating gateway hub with hub config: " + hubConfigName);
    GatewayHubHelper.createGatewayHub(hubConfigName);
  }
  
  /** Create gateway hub
   * 
   */
  public static void HydraTask_addGatewayHub() {
    String gatewayConfigName = RecoveryPrms.getGatewayConfigName();
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

  /** Create a region for each region defined in hydra.RegionPrms-names
   * 
   */
  public static synchronized void HydraTask_createRegionsConvertedFirst() {
    if (testInstance == null) {
      Integer convertedVmId = (Integer)RecoveryBB.getBB().getSharedMap().get(singleConvertedVmIdKey);
      int myVmId = RemoteTestModule.getMyVmid();
      if (convertedVmId == myVmId) { // this vm is required to initialize first
        HydraTask_createRegions();
        RecoveryBB.getBB().getSharedMap().remove(singleConvertedVmIdKey);
      } else {
        while (convertedVmId != null) {
          MasterController.sleepForMs(5000);
          convertedVmId = (Integer)RecoveryBB.getBB().getSharedMap().get(singleConvertedVmIdKey);
          if (convertedVmId == null) { // signal for all remaining vms to initialize
            HydraTask_createRegions();
          }
        }
      }
    }
    testInstance.initializeInstancePerThread();
  }

  /** Write a snapshot to the blackboard for all regions except any
   *  extra regions. 
   * 
   */
  public static void HydraTask_writeToBB() {
    // writeSnapshot writes a region snapshot for all regions in 
    // testInstance.allRegions; if this is a conversion test and we
    // are using the oldVersion xml file as the new version xml file
    // during convert, then include all the extra regions, otherwise 
    // remove the extra regions since we don't want them in the snapshot
    if (RecoveryPrms.getConvertWithNewVersionXml()) {
      testInstance.removeExtraRegionsFromAllRegions();
    }
    testInstance.writeSnapshot();
  }

  /** Disconnect from the distributed system
   * 
   */
  public static void HydraTask_disconnect() {
    DistributedSystemHelper.disconnect();
    testInstance = null;
    RecoveryTestVersionHelper.initPRObserver();
  }

  /** Disconnect from the distributed system
   * 
   */
  public static void HydraTask_stopGatewayHub() {
    GatewayHubHelper.stopGatewayHub();
    GatewayHubBlackboard.getInstance().getSharedMap().clear();
    DistributedSystemBlackboard.getInstance().getSharedMap().clear();
  }

  /** Convert disk files for this jvm.
   * 
   */
  public static void HydraTask_doSingleConvert() {
    long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
    if (leader == 1) {
      int convertedIndex = doSingleConvert(true);
      List<String> newVersionXmlFileList = (List<String>)RecoveryBB.getBB().getSharedMap().get(newVersionXmlFilesKey);
      String newVersionXmlFile = newVersionXmlFileList.get(convertedIndex);
      int vmId = getVmIdFromXmlFileName(newVersionXmlFile);
      RecoveryBB.getBB().getSharedMap().put(singleConvertedVmIdKey, vmId);
    }
  }

  /** Verify regions that are PRs 
   *
   */
  public static void HydraTask_verifyPRs() {
    StringBuffer aStr = new StringBuffer();
    for (Region aRegion: testInstance.allRegions) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
        Log.getLogWriter().info("Verifying PR " + aRegion.getFullPath() + " with " + redundantCopies + " redundantCopies");
        // verify PR metadata 
        try {
          ParRegUtil.verifyPRMetaData(aRegion);
        } catch (Exception e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        } catch (TestException e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        }

        // verify primaries
        try {
          ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
        } catch (Exception e) {
          aStr.append(e.toString() + "\n");
        }

        // verify PR data
        try {
          ParRegUtil.verifyBucketCopies(aRegion, redundantCopies);
        } catch (Exception e) {
          aStr.append(e.toString() + "\n");
        } catch (TestException e) {
          aStr.append(e.toString() + "\n");
        }

        if (aStr.length() > 0) {
          // shutdownHook will cause all members to dump partitioned region info
          throw new TestException(aStr.toString());
        }
        Log.getLogWriter().info("Done verifying PR internal consistency for " + aRegion.getFullPath());
      }
    }
  }

  /** For all PRs, wait for startup recovery in this VM. 
   * 
   */
  public static void HydraTask_waitForMyStartupRecovery() {
    int numPRs = 0;
    List prNameList = new ArrayList();
    int myVmID = RemoteTestModule.getMyVmid();
    for (Region aRegion: testInstance.allRegions) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        PartitionAttributes prAttr = aRegion.getAttributes().getPartitionAttributes();
        if ((prAttr.getStartupRecoveryDelay() >= 0) && (prAttr.getRedundantCopies() != 0)) {
          numPRs++;
          prNameList.add(aRegion.getFullPath());
        }
      }
    }
    if (numPRs > 0) {
      PRObserver.waitForRebalRecov(myVmID, 1, numPRs, prNameList, null, false);
    }
  }
  
  public static void HydraTask_convertPre65Controller() {
    // stop the new version vms; they will restart later after the conversion
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    newVersionVMs.removeAll(StopStartVMs.getMatchVMs(newVersionVMs, "newVersionLocator"));
    List<String> stopModesList = new ArrayList();
    for (int i = 1; i <= newVersionVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    StopStartVMs.stopVMs(newVersionVMs, stopModesList);
    
    // sleep to allow ops to run
    int sleepMs = 60000;
    Log.getLogWriter().info("Sleeping " + sleepMs + "ms so ops can run...");
    MasterController.sleepForMs(sleepMs);
    
    // stop the old version vms, one at a time so the ops can continue to 
    // make their disk files different
    List<ClientVmInfo> oldVersionVMs = StopStartVMs.getAllVMs();
    oldVersionVMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion");
    for (ClientVmInfo target: oldVersionVMs) {
      try {
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, target);
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
    
    // convert the disk files
    int convertedIndex = doSingleConvert(true);
    List<String> newVersionXmlFileList = (List<String>)RecoveryBB.getBB().getSharedMap().get(newVersionXmlFilesKey);
    String newVersionXmlFile = newVersionXmlFileList.get(convertedIndex);
    int vmId = getVmIdFromXmlFileName(newVersionXmlFile);
    int startFirstIndex = -1;
    for (int i = 0; i < newVersionVMs.size(); i++) {
      if (newVersionVMs.get(i).getVmid().intValue() == vmId) {
        startFirstIndex = i;
        break;
      }
    }
        
    // start the converted new version jvm first, then the others gii from it
    // starting up one first is a requirement of the conversion tool, otherwise
    // if they all start up together, we will get a ConflictingDataException
    List<ClientVmInfo> startFirstList = new ArrayList();
    startFirstList.add(newVersionVMs.get(startFirstIndex));
    List<Thread> startThread = StopStartVMs.startAsync(startFirstList);
    // when the startThread has written to the blackboard, we can start the rest
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten", 
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);
    newVersionVMs.remove(startFirstIndex);
    StopStartVMs.startVMs(newVersionVMs);
    for (Thread aThread: startThread) {// should just be one thread
      try {
        aThread.join();
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }
  
  /** Controller method to test uprade of disk files one release to another. 
   *  For example, for 6.6 this will test creation of 6.5 disk files to be recovered by 6.6 jvms.
   */
  public static void HydraTask_upgradeController() {
    // stop the new version vms; they will restart later using the disk files created by the old version vms
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    newVersionVMs.removeAll(StopStartVMs.getMatchVMs(newVersionVMs, "newVersionLocator"));
    List<String> stopModesList = new ArrayList();
    for (int i = 1; i <= newVersionVMs.size(); i++) {
      stopModesList.add("nice_exit");
    }
    StopStartVMs.stopVMs(newVersionVMs, stopModesList);
    
    // write xml file names to the blackboard, required for reinitialization in the newVersion vms
    int myVmId = RemoteTestModule.getMyVmid();
    List<String> oldXmlFiles = new ArrayList();
    List<String> newXmlFiles = new ArrayList();
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    String[] dirContents = currDir.list();
    for (String fileName: dirContents) {
      if (fileName.endsWith(".xml")) {
        if (fileName.indexOf("oldVersion") >= 0) {
          oldXmlFiles.add(fileName);
        } else if (fileName.indexOf("newVersion") >= 0) {
          newXmlFiles.add(fileName);
        }
      }
    }
    // write parallel lists to bb; these lists show the mapping of old vmIds
    // to new vmIds for tests that need this information later
    RecoveryBB.getBB().getSharedMap().put(oldVersionXmlFilesKey, oldXmlFiles);
    RecoveryBB.getBB().getSharedMap().put(newVersionXmlFilesKey, newXmlFiles);
    
    // sleep to allow ops to run
    int sleepMs = 60000;
    Log.getLogWriter().info("Sleeping " + sleepMs + "ms so ops can run...");
    MasterController.sleepForMs(sleepMs);
    
    // signal to the old version vms to pause doing ops so they can take a snapshot
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten", 
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);
    // now all vms have paused and they have written a snapshot
    
    // stop the old version vms, using the prescribed strategy
    String stopMode = TestConfig.tab().stringAt(StopStartPrms.stopModes);
    if (stopMode.equalsIgnoreCase("shutDownAll")) {
      // shutDownAll was handled in the doOperationWithPause method because shutDownAll
      // must run in the oldVersion DS; this is the new version ds, but by the time
      // snapshotWritten counter (above) is incremented, the shutdownAll has already
      // occurred
    } else {
      List<ClientVmInfo> oldVersionVMs = StopStartVMs.getAllVMs();
      oldVersionVMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion");
      List<String> stopModes = new ArrayList();
      for (int i = 0; i < oldVersionVMs.size(); i++) {
        stopModes.add(stopMode);
      }
      StopStartVMs.stopVMs(oldVersionVMs, stopModes);
    }
    
    // this start will detect that new version vms are starting up and will
    // create the regions using the old version's xml files; those old version
    // xml files reference the old version disk files. 
    StopStartVMs.startVMs(newVersionVMs);
  }
  
  /** Reset the uniqueKeyIndex to be higher than the current blackboard counter
   *   
   */
  public static void HydraTask_resetUniqueKeyIndex() {
    HydraTask_prepareForNewVersionOps();
  }

  /** set each newVersion uniqueKey blackboard value to a value higher
   *  than was used in the oldVersion ops to ensure unique keys and
   *  that new keys obtained by newVersion vms have never been used
   *  by oldVersion vms
   */
  public static void HydraTask_prepareForNewVersionOps() {
    long max = NameBB.getBB().getSharedCounters().read(NameBB.POSITIVE_NAME_COUNTER);
    
    // set this threads key index to a value higher than max
    int incValue = TestHelper.getNumThreads();
    int myThreadId = RemoteTestModule.getCurrentThread().getThreadId();
    int myKeyIndexValue = myThreadId;
    while (myKeyIndexValue < max) {
      myKeyIndexValue += incValue;
    }
    Log.getLogWriter().info("Setting this threads uniqueKeyIndex to " + myKeyIndexValue);
    RecoveryBB.getBB().getSharedMap().put(uniqueKeyIndex + myThreadId, myKeyIndexValue);
  }
  
  /** Run from a new version locator thread
   *  This code is hardcoded for 3 wan sites because of a time crunch to get
   *  the test running. This could be changed later.
   */
  public static void HydraTask_convertWANTestController() {
    logExecutionNumber();
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    List<ClientVmInfo> oldVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    oldVersionVMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion");
    List<ClientVmInfo> newLocators = StopStartVMs.getMatchVMs(newVersionVMs, "newVersionLocator");
    for (ClientVmInfo info: newLocators) { // remove myself from newLocators
      if (info.getVmid() == RemoteTestModule.getMyVmid()) {
        newLocators.remove(info);
        break;
      }
    }
    List<ClientVmInfo> oldLocators = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersionLocator");
    List<ClientVmInfo> newWanSite1VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_1_");
    List<ClientVmInfo> newWanSite2VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_2_");
    List<ClientVmInfo> newWanSite3VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_3_");
    List<ClientVmInfo> oldWanSite1VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_1_");
    List<ClientVmInfo> oldWanSite2VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_2_");
    List<ClientVmInfo> oldWanSite3VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_3_");
    Log.getLogWriter().info("newVersionVMs: " + newVersionVMs);
    Log.getLogWriter().info("oldVersionVMs: " + oldVersionVMs);
    Log.getLogWriter().info("newWanSite1VMs: " + newWanSite1VMs);
    Log.getLogWriter().info("newWanSite2VMs: " + newWanSite2VMs);
    Log.getLogWriter().info("newWanSite3VMs: " + newWanSite3VMs);
    Log.getLogWriter().info("oldWanSite1VMs: " + oldWanSite1VMs);
    Log.getLogWriter().info("oldWanSite2VMs: " + oldWanSite2VMs);
    Log.getLogWriter().info("oldWanSite3VMs: " + oldWanSite3VMs);

    // stop the new version vms; they will restart later after the conversion
    List<String> stopModesList = new ArrayList();
    List<ClientVmInfo> targetVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_");
    for (int i = 1; i <= targetVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    StopStartVMs.stopVMs(targetVMs, stopModesList);
    StopStartVMs.stopVMs(newLocators, stopModesList);

    // sleep to allow ops to run
    int sleepMs = 60000;
    Log.getLogWriter().info("Sleeping " + sleepMs + "ms so ops can run...");
    MasterController.sleepForMs(sleepMs);

    // stop all other oldVersion vms besides myself in a particular order; this allows WAN queues to grow
    try {
      // stop all but one vm per wan site
      for (int i = 1; i < oldWanSite1VMs.size(); i++) {
        Log.getLogWriter().info("Stopping: " + oldWanSite1VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite1VMs.get(i));
        Log.getLogWriter().info("Stopping: " + oldWanSite2VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite2VMs.get(i));
        Log.getLogWriter().info("Stopping: " + oldWanSite3VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite3VMs.get(i));
      }

      Log.getLogWriter().info("Stopping: " + oldWanSite2VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
          ClientVmMgr.ON_DEMAND, oldWanSite2VMs.get(0));
      Log.getLogWriter().info("Stopping: " + oldWanSite3VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
          ClientVmMgr.ON_DEMAND, oldWanSite3VMs.get(0));

    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    // not only oldWanSite1VMs.get(0) is still running; it needs to pause and write a snapshot
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), 
        "RecoveryBB.snapshotWritten", RecoveryBB.snapshotWritten, 
        1, true, -1, 1000);

    //stop the last vm
    try {
      Log.getLogWriter().info("Stopping: " + oldWanSite1VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.MEAN_KILL,
          ClientVmMgr.ON_DEMAND, oldWanSite1VMs.get(0));
    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    // stop the old version locators
    Log.getLogWriter().info("Stopping the old version locator(s)");
    for (ClientVmInfo info: oldLocators) {
      try {
        ClientVmMgr.stop("Stopping old version locator",
            ClientVmMgr.MEAN_KILL,
            ClientVmMgr.ON_DEMAND, info);
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    // convert 
    convertPre65AndValidate(getXmlFileName(oldWanSite1VMs.get(0).getVmid()), 
        getXmlFileName(newWanSite1VMs.get(0).getVmid()));
    convertPre65AndValidate(getXmlFileName(oldWanSite2VMs.get(0).getVmid()), 
        getXmlFileName(newWanSite2VMs.get(0).getVmid()));
    convertPre65AndValidate(getXmlFileName(oldWanSite3VMs.get(0).getVmid()), 
        getXmlFileName(newWanSite3VMs.get(0).getVmid()));

    // start in a particular order
    // start the locators first
    GatewayHubBlackboard.getInstance().getSharedMap().clear();
    DistributedSystemBlackboard.getInstance().getSharedMap().clear();
    StopStartVMs.startVMs(newLocators);
    // I'm a locator, so I need to initialize also
    //DistributedSystemHelper.createLocator();
    //DistributedSystemHelper.startLocatorAndDS();

    try {
      Log.getLogWriter().info("Starting: " + newWanSite1VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite1VMs.get(0));
      Log.getLogWriter().info("Starting: " + newWanSite2VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite2VMs.get(0));
      Log.getLogWriter().info("Starting: " + newWanSite3VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite3VMs.get(0));
      // @todo lynng starting the above runs validation and confirms the wan queues
      // were converted; starting the below vms is an extra optional step; because
      // this test is running 2 separate versions of 2 distributes systems, the locators
      // are not being sufficiently refreshed by hydra in the above clear() calls
      //for (int i = 1; i < newWanSite2VMs.size(); i++) {
      //  Log.getLogWriter().info("Starting: " + newWanSite1VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite1VMs.get(i));
      //  Log.getLogWriter().info("Starting: " + newWanSite2VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite2VMs.get(i));
      //  Log.getLogWriter().info("Starting: " + newWanSite3VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite3VMs.get(i));
      //}
    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Run from a new version locator thread
   *  This code is hardcoded for 3 wan sites because of a time crunch to get
   *  the test running. This could be changed later.
   */
  public static void HydraTask_convertWANTestResponder() throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    testInstance.doOperations(minTaskGranularityMS);
    if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing) > 0) {
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.Pausing", RecoveryBB.pausing, 
          11, true, -1, 1000);
      long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
      if (leader == 1) {
        HydraTask_writeToBB();
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
      }
      MasterController.sleepForMs(Integer.MAX_VALUE); // wait to be killed by the controller
    }
  }

  /** Hydra task to restore each backup that occurred during the run.
   */
  public static void HydraTask_restoreBackups() throws Exception {
    int numVMs = TestHelper.getNumVMs();
    long counter = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.verifyBackups);
    if (counter == 1) { // this is the restore leader
      Log.getLogWriter().info("This thread is the restore leader");
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.backupRestored);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.Reinitialized);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.FinishedVerify);

      // get a list of the backup directories
      List<File> backupDirList = new ArrayList<File>();
      String currDirName = System.getProperty("user.dir");
      File currDir = new File(currDirName);
      File[] contents = currDir.listFiles();
      for (File aDir: contents) {
        if (aDir.getName().startsWith("backup_")) {
          backupDirList.add(aDir);
        }
      }
      Log.getLogWriter().info("Backup dir list is " + backupDirList);
      
      // run the restore script from each backup directory; expect this to fail
      // because there are existing disk directories
      Log.getLogWriter().info("Restoring backups, expect to fail because disk dirs already exist");
      for (File backupDir: backupDirList) {
        runRestoreScript(backupDir, false);
      }
      Log.getLogWriter().info("Done restoring failed backups");
      
      // remove the existing disk directories and restore each backup
      // successfully 
      for (int i = 0; i < backupDirList.size(); i++) {
        // restore from a backup
        File backupDir = backupDirList.get(i);
        String backupDirName = backupDir.getName();
        long backupNum = Long.valueOf(backupDirName.substring(backupDirName.indexOf("_")+1, backupDirName.length()));
        RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.onlineBackupNumber);
        RecoveryBB.getBB().getSharedCounters().setIfLarger(RecoveryBB.onlineBackupNumber, backupNum);
        PRObserver.initialize();
        Log.getLogWriter().info("Backup number is " + backupNum);
        Log.getLogWriter().info("Restoring from " + backupDir.getName());
        deleteExistingDiskDirs();
        runRestoreScript(backupDir, true);
        Log.getLogWriter().info("Done restoring from " + backupDir.getName());
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.backupRestored);
        
        // initialize and wait for recovery
        Log.getLogWriter().info("Initializing...");
        
        try {
          HydraTask_multiRegionInitialize();
        } catch (Throwable throwable) {
          throw new TestException("Exception during restore backups: ",throwable);
        }
        
        Log.getLogWriter().info("Done initializing");
        
        // wait for everybody else to finish initializing and recover redundancy
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.Reinitialized", 
            RecoveryBB.Reinitialized, 
            numVMs-2, 
            true, 
            -1);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.Reinitialized);
        RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.ReadyToBegin);

        // verify
        testInstance.validateAfterRestore(backupNum);
        
        // wait for everybody else to be done validating
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.FinishedVerify", 
            RecoveryBB.FinishedVerify, 
            numVMs-2, 
            true, 
            -1);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.FinishedVerify);

        // At this point, all other vms are done with this restore; disconnect
        // and get ready for the next restore 
        Log.getLogWriter().info("Preparing for next restore...");
        HydraTask_disconnect();
        RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.backupRestored);
        RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.Reinitialized);
        if (i == backupDirList.size()-1) { // just did the last backup
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.backupsDone);
        }
        
        // wait for everybody else to be disconnected
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.ReadyToBegin", 
            RecoveryBB.ReadyToBegin, 
            numVMs-2, 
            true, 
            -1);
        RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.FinishedVerify);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.ReadyToBegin);
      }
    } else { // not the restore leader
      while (true) {
        // wait for the vm that is restoring files to tell this vm to initialize
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
                                  "RecoveryBB.backupRestored", 
                                  RecoveryBB.backupRestored, 
                                  1, 
                                  true, 
                                  -1);
        
        // initialize
        Log.getLogWriter().info("Initializing...");
        
        try {
          HydraTask_multiRegionInitialize();
        } catch (Throwable throwable) {
          throw new TestException("Exception during restore backups: ",throwable);
        }
        
        Log.getLogWriter().info("Done initializing");
        
        // wait for everybody to initialize and wait for recovery
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.Reinitialized);
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.Reinitialized", 
            RecoveryBB.Reinitialized, 
            numVMs-1, 
            true, 
            -1);

        // verify
        long backupNumber = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.onlineBackupNumber);
        testInstance.validateAfterRestore(backupNumber);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.FinishedVerify);
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.FinishedVerify", 
            RecoveryBB.FinishedVerify, 
            numVMs-1, 
            true, 
            -1);
        
        // now it's safe to disconnect and prepare for next restore
        Log.getLogWriter().info("Preparing for next restore...");
        HydraTask_disconnect();
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.ReadyToBegin);
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.ReadyToBegin", 
            RecoveryBB.ReadyToBegin, 
            numVMs-1, 
            true, 
            -1);
        if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.backupsDone) > 0) {
          Log.getLogWriter().info("All backups have been restored");
          break;
        }
      }
    }
  }

  //=================================================
  // methods to do the work of hydra tasks

  /** After a restore from a backup, validate all region data.
   * 
   * @param backupNum The backup number we restored from (backups are created
   *        in directories called backup_<backupNum>
   */
  protected void validateAfterRestore(long backupNum) throws Exception {
    Map snapshot = null;
    ObjectInputStream ois = null;
    String snapFileName = "snapshotForBackup_" + backupNum + ".ser";

    try {
      ois = new ObjectInputStream(new FileInputStream(snapFileName));
      snapshot = (Map)(ois.readObject());
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } finally {
      if(null != ois) {
        ois.close();
      }
    }

    RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, snapshot);
    verifyFromSnapshot();
  }

    /** This is run by one vm  to control responder vms for compaction tests.
   *   This directs the responder vms to
   *      1) initialize
   *      2) destroy a chunk of keys
   *      3) run compaction for those responder vms that have disk regions
   *      4) stop and restart all vms; one of the vms will initialize from disk regions
   *          and all others will do a gii from the one that did disk recovery
   *      5) direct all vms to verify their contents to ensure compaction ran correctly.
   *      6) add a chunk of new keys equal in number to the chunk destroyed in
   *          step 2
   *   All steps are batched (if possible) to allow this test to scale up.
   */
  private  void compactionController() {
    // initialize
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    String currentTaskStep = (String)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.taskStep));
    Log.getLogWriter().info("In compactionController, currentTaskStep is " + currentTaskStep);
    if (currentTaskStep.equals(STARTING_STEP)) {
      // initialize
      logExecutionNumber();
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneDestroyingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneCompactingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

      // wait for vms to signal they completed starting step
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.doneStartingCounter", RecoveryBB.doneStartingCounter, 
          TestHelper.getNumVMs() -1, true, -1, 1000);

      // prepare for destroys
      NameBB.getBB().getSharedCounters().zero(NameBB.POSITIVE_NAME_COUNTER);
      long firstKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.firstKeyIndexKey));
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, firstKeyIndex-1);

      // change step to destroy
      currentTaskStep = DESTROY_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);

      return; // start next step with fresh maxResultWaitSec
    }

    if (currentTaskStep.equals(DESTROY_STEP)) {
      // wait for destroys to complete; responder threads are done when all 
      // responder threads report they are done
      try {
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.doneDestroyingCounter", RecoveryBB.doneDestroyingCounter, 
            TestHelper.getNumThreads()-1, false, minTaskGranularityMS, 5000);
      } catch (TestException e) {// expired minTaskGranularityMS
        return; // make  batched
      }

      // done with destroys
      long firstKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.firstKeyIndexKey));
      firstKeyIndex = firstKeyIndex + RecoveryPrms.getChunkSize();
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.firstKeyIndexKey, firstKeyIndex);
      currentTaskStep = COMPACTION_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      return; // start next step with fresh maxResultWaitSec
    }

    // control compaction
    if (currentTaskStep.equals(COMPACTION_STEP)) {
      // compaction step is done when all other vms report completion of compaction
      try {
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoverBB.doneCompactingCounter", RecoveryBB.doneCompactingCounter, 
            TestHelper.getNumVMs() - 1, true, minTaskGranularityMS, 5000);
      } catch (TestException e) {// expired minTaskGranularityMS
        return; // make  batched
      }

      // done with compacting
      currentTaskStep = STOP_START_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      return; // start next step with fresh maxResultWaitSec
    }

    // control stop/start
    if (currentTaskStep.equals(STOP_START_STEP)) {
      // stop all vms, then restart (one recovers from disk, the others gii from disk recovery
      Vector otherVmIds = ClientVmMgr.getOtherClientVmids();
      List otherVMs = new ArrayList();
      List replicateVMs = new ArrayList();
      List persistentVMs = new ArrayList();
      List stopModeList = new ArrayList();
      for (int i = 0; i < otherVmIds.size(); i++) {
        Integer otherVmId = (Integer)(otherVmIds.get(i));
        ClientVmInfo info = new ClientVmInfo(otherVmId);
        otherVMs.add(info);
        if (RecoveryBB.getBB().getSharedMap().containsKey(persistentRegionKey + otherVmId)) {
          persistentVMs.add(info);
        } else {
          replicateVMs.add(info);
        }
        String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
        stopModeList.add(choice);
      }
      StopStartVMs.stopVMs(otherVMs, stopModeList); // stop all vms other than this one
      StopStartVMs.startVMs(persistentVMs); // start the persistent vms first
      StopStartVMs.startVMs(replicateVMs); // start the replicate vms next

      // change to next step
      currentTaskStep = VERIFY_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneStartingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneAddingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control verify
    if (currentTaskStep.equals(VERIFY_STEP)) {
      // verify step is done when all other vms report completion of compaction
      try {
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.doneVerifyingCounter", RecoveryBB.doneVerifyingCounter, 
            TestHelper.getNumVMs() - 1, true, minTaskGranularityMS, 5000);
      } catch (TestException e) {// expired minTaskGranularityMS
        return; // make  batched
      }

      // done with verify; prepare for add step
      long lastKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.lastKeyIndexKey));
      NameBB.getBB().getSharedCounters().zero(NameBB.POSITIVE_NAME_COUNTER);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, lastKeyIndex);
      currentTaskStep = ADD_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      return; // start next step with fresh maxResultWaitSec
    }

    // control add
    if (currentTaskStep.equals(ADD_STEP)) {
      // wait for adds to complete
      try {
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.doneAddingCounter", RecoveryBB.doneAddingCounter, 
            TestHelper.getNumThreads()-1, true, minTaskGranularityMS, 10000);
      } catch (TestException e) {// expired minTaskGranularityMS
        return; // make  batched
      }

      // adds are complete
      long lastKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.lastKeyIndexKey));
      lastKeyIndex = lastKeyIndex + RecoveryPrms.getChunkSize();
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.lastKeyIndexKey, lastKeyIndex);
      currentTaskStep = STARTING_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      return; // start next step with fresh maxResultWaitSec
    }
  }

  /** Responds to directives issued by the compaction controller
   * 
   */
  private void compactionResponder() {
    String currentTaskStep = (String)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.taskStep));
    Log.getLogWriter().info("In compactionResponder, currentTaskStep is " + currentTaskStep);
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    if (currentTaskStep.equals(STARTING_STEP)) {
      if (startingCoordinator.incrementAndGet() == 1) { // only do this once per vm
        Log.getLogWriter().info("Initializing for startup...");
        compactionCoordinator = new AtomicInteger(0);
        verifyCoordinator = new AtomicInteger(0);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneStartingCounter);
      } else {
        TestHelper.waitForCounter(RecoveryBB.getBB(), 
            "RecoveryBB.doneStartingCounter", RecoveryBB.doneStartingCounter, 
            TestHelper.getNumVMs() -1, true, -1, 1000);
      }
    } else if (currentTaskStep.equals(DESTROY_STEP)) {
      if ((Boolean)destroysCompleted.get()) {
        try {
          TestHelper.waitForCounter(RecoveryBB.getBB(), 
              "RecoveryBB.doneDestroyingCounter", RecoveryBB.doneDestroyingCounter, 
              TestHelper.getNumThreads()-1, true, minTaskGranularityMS, 1000);
        } catch (TestException e) {
          return; // make batched
        }
      } else {
        long firstKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.firstKeyIndexKey));
        boolean allDestroysDone = doDestroyStep(firstKeyIndex + RecoveryPrms.getChunkSize() - 1);
        if (allDestroysDone) {
          destroysCompleted.set(new Boolean(true));
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneDestroyingCounter);
        }
      }
    } else if (currentTaskStep.equals(COMPACTION_STEP)) {
      startingCoordinator = new AtomicInteger(0);
      doCompactionStep();
    } else if (currentTaskStep.equals(STOP_START_STEP)) { // step handled by controller, not responder
      MasterController.sleepForMs(5000); 
    } else if (currentTaskStep.equals(VERIFY_STEP)) {
      doVerificationStep();
    } else if (currentTaskStep.equals(ADD_STEP)) {
      if ((Boolean)addsCompleted.get()) {
        try {
          TestHelper.waitForCounter(RecoveryBB.getBB(), 
              "RecoveryBB.doneAddingCounter", RecoveryBB.doneAddingCounter, 
              TestHelper.getNumThreads()-1, true, minTaskGranularityMS, 1000);
        } catch (TestException e) {
          return; // make batched
        }
      } else {
        long lastKeyIndex = (Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.lastKeyIndexKey));
        boolean allAddsCompleted = addToRegions(lastKeyIndex + RecoveryPrms.getChunkSize());
        if (allAddsCompleted) {
          addsCompleted.set(new Boolean(true));
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneAddingCounter);
        }
      }
    }
  }

  /** Destroy a chunk of keys deteremined by RecoveryPrms.chunkSize.
   * 
   *  @param lastKeyIndexToDestroy The index of the last key to destroy. Destroys
   *                  start with the next key obtained from NameFactory.
   *  @returns true if all keys have been destroyed, false if there are more destroys
   *                    to do. 
   * 
   */
  private boolean doDestroyStep(long lastKeyIndexToDestroy) {
    int chunkSize = RecoveryPrms.getChunkSize();
    Log.getLogWriter().info("Destroying " + chunkSize + " keys in each of " + 
        allRegions.size() + " regions; last key index to destroy is " + lastKeyIndexToDestroy);
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < minTaskGranularityMS) {
      Object key = NameFactory.getNextPositiveObjectName();
      long nameCounter = NameFactory.getCounterForName(key);
      if (nameCounter <= lastKeyIndexToDestroy) {
        for (Region aRegion: allRegions) {
          Log.getLogWriter().info("Destroying key " + key + " in region " + aRegion.getFullPath());
          aRegion.destroy(key);
        }
      } else {
        Log.getLogWriter().info("Done destroying " + chunkSize + " keys in each of " +
            allRegions.size() + " regions");
        return true; 
      }
    }
    return false;
  }

  /** For those regions that are persistent, force compaction.
   * 
   */
  private synchronized void doCompactionStep() {
    int coord = compactionCoordinator.incrementAndGet();
    if (coord == 1) { // this is the one thread in this vm to do compaction
      for (Region aRegion: allRegions) {
        DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();
        if (dataPolicy.withPersistence() && dataPolicy.withReplication()) {
          Log.getLogWriter().info("Causing compaction of disk files for region " + aRegion.getFullPath());
          long startTime = System.currentTimeMillis();
          boolean compactionResult = ((LocalRegion)aRegion).getDiskStore().forceCompaction();
          long duration = System.currentTimeMillis() - startTime;
          Log.getLogWriter().info("Done with compaction of disk files for region " + aRegion.getFullPath() +", compaction took " +
              duration + " ms");
          if (!compactionResult) {
            throw new TestException("forceCompaction returned " + compactionResult + " for " + aRegion.getFullPath());
          }
        }
      }
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneCompactingCounter);
    } else {
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoverBB.doneCompactingCounter", RecoveryBB.doneCompactingCounter, 
          TestHelper.getNumVMs() - 1, true, -1, 10000);
    }
  }

  /** Verify that all keys expected are present.
   * 
   */
  private void doVerificationStep() {
    int coord = verifyCoordinator.incrementAndGet();
    if (coord == 1) { // this is the one thread in this vm to do verification
      long firstKeyIndex = ((Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.firstKeyIndexKey)));
      long lastKeyIndex = ((Long)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.lastKeyIndexKey)));
      Log.getLogWriter().info("Verifying with firstKeyIndex " + firstKeyIndex + ", lastKeyIndex " + lastKeyIndex);
      StringBuffer errStr = new StringBuffer();
      long cumulativeRegionSizes = 0;
      for (Region aRegion: allRegions) {
        Log.getLogWriter().info("Verifying keys for " + aRegion.getFullPath());
        for (Object key: aRegion.keySet()) {
          // find any unexpected keys in region
          long nameCounter = NameFactory.getCounterForName(key);
          if ((nameCounter < firstKeyIndex) || (nameCounter > lastKeyIndex)) {
            errStr.append("Found unexpected key " + key + " in " + aRegion.getFullPath() + "\n");
          }
        }

        // find any missing keys in region; since we already iterated all the keys
        // in the region above, we only need to iterate the key indexes here if the
        // size of the region is unexpected
        long expectedNumKeys = lastKeyIndex - firstKeyIndex + 1;
        if (aRegion.size() != expectedNumKeys) { // look for missing keys
          for (long i = firstKeyIndex; i <= lastKeyIndex; i++) { 
            Object key = NameFactory.getObjectNameForCounter(i);
            if (!aRegion.containsKey(key)) {
              errStr.append(key + " is missing from " + aRegion.getFullPath() + "\n");
            }
          }
        }
      }
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
      if (errStr.length() > 0) {
        throw new TestException(errStr.toString());
      }
    }
  }

  /** Add a number of keys to all regions defined in this vm. The first key added
   *   is the key obtained by getting the next key from util.NameFactory. The last
   *   key added contains the key index specified by finalKeyIndex.
   *   
   *   @param finalKeyIndex The key index of the last key to add
   *   @return True if all keys were added, false if the method ran for
   *                    TestHelperPrms.minTaskGranularitySec seconds before completing
   *                    all adds.
   * 
   */
  private boolean addToRegions(long finalKeyIndex) {
    Log.getLogWriter().info("Adding to region(s) until biggest key has index " + finalKeyIndex);
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long logIntervalMS = 10000;
    long startTime = System.currentTimeMillis();
    long lastLogTime = startTime;
    int numCreated = 0;
    while (System.currentTimeMillis() - startTime < minTaskGranularityMS) {
      Object key = NameFactory.getNextPositiveObjectName();
      long nameCounter = NameFactory.getCounterForName(key);
      numCreated++;
      if (nameCounter <= finalKeyIndex) {
        for (Region aRegion: allRegions) {
          aRegion.put(key, getValueForKey(key));
          if (System.currentTimeMillis() - lastLogTime > logIntervalMS) {
            Log.getLogWriter().info("Created " + numCreated + " entries per region so far this task; number remaining to create " + (finalKeyIndex - nameCounter));
            lastLogTime = System.currentTimeMillis();
          }
        }
      } else {
        return true;
      }
    }
    return false;
  }

  /** Verify that there are regions present and they have the expected size. 
   */
  private  void verifyRegions() {
    if (allRegions.size() == 0) {
      throw new TestException("No regions in this vm");
    }
    int expectedSize = RecoveryPrms.getMaxNumEntriesPerRegion();
    for (Region aRegion: allRegions) {
      Log.getLogWriter().info("Size of " + aRegion.getFullPath() + " is " + aRegion.size() + "; expected size is " + expectedSize);
      if (aRegion.size() != expectedSize) {
        throw new TestException("Expected region " + aRegion.getFullPath() + " to be size " + expectedSize +
            ", but it is size " + aRegion.size());
      }
    }
  }

  /** Using the blackboard to retrieve information written by each vm, verify
   *   that each vm obtained data either by disk recovery or gii as appropriate.
   * 
   */
  private  static void verifyStartupState() {
    Boolean recoveryCanBeTie = (Boolean)RecoveryBB.getBB().getSharedMap().get(recoveryCanBeTieKey);
    if ((recoveryCanBeTie != null) && (recoveryCanBeTie)) {
      verifyTieStartup();
    } else {
      Map sharedMap = RecoveryBB.getBB().getSharedMap().getMap();
      Iterator it = sharedMap.keySet().iterator();
      StringBuffer logStr = new StringBuffer();
      StringBuffer errStr = new StringBuffer();
      Set<Integer> vmIDsWithDiskRecovery = new HashSet();
      boolean expectAnyVmToRecoverFromDisk = false;
      while (it.hasNext()) {
        Object key = it.next();
        if (key instanceof String) {
          String strKey = (String)key;
          if (strKey.startsWith(startupStatsForVmKey)) { // found start up stats for a vm
            Map<String, Integer> statsMap = (Map)(sharedMap.get(strKey));
            Integer vmID = Integer.valueOf(strKey.substring(startupStatsForVmKey.length(), strKey.length()));
            Boolean expectDiskRecovery = (Boolean)(RecoveryBB.getBB().getSharedMap().get(expectDiskRecoveryKey + vmID));
            if (expectDiskRecovery != null) {
              expectAnyVmToRecoverFromDisk = expectAnyVmToRecoverFromDisk || expectDiskRecovery;
            }
            Log.getLogWriter().info("Found startup stats for vmID " + vmID +  ": " + statsMap + 
                ", vmID " + vmID + " expected disk recovery: " + expectDiskRecovery);
            logStr.append("For vmID " + vmID + ", startup stats are:\n");
            int numDiskRecoveries = 0; // number of regions that did disk recovery in vmID
            int numGiis = 0; // number of regions that did gii in vmID
            for (String statsMapKey: statsMap.keySet()) { 
              if (statsMapKey.startsWith(localInitializationsKey)) {
                String regionName = statsMapKey.substring(localInitializationsKey.length(), statsMapKey.length());
                int localInit = statsMap.get(statsMapKey);
                numDiskRecoveries += localInit;
                if (localInit != 0) {
                  vmIDsWithDiskRecovery.add(vmID);
                }
                logStr.append("   " + regionName + " localInitializations (disk recovery): " + localInit + "\n");
              } else if (statsMapKey.startsWith(remoteInitializationsKey)) {
                String regionName = statsMapKey.substring(remoteInitializationsKey.length(), statsMapKey.length());
                int remoteInit = statsMap.get(statsMapKey);
                numGiis += remoteInit;
                logStr.append("   " + regionName + " remoteInitalizations (gii): " + remoteInit + "\n");
              } else {
                throw new TestException("Unknown key in stats map: " + statsMapKey);
              }
            }

            // check that for member with vmID, all regions either recovered from disk OR from gii (but not both and not neither);
            if ((numGiis != 0) && (numDiskRecoveries != 0) ||
                ((numGiis == 0) && (numDiskRecoveries ==0))) {
              String aStr = "For vmID " + vmID + ", expected all regions to obtain data from either disk or gii, but " + 
              numGiis + " regions obtained data from gii and " + numDiskRecoveries + " regions obtained data from disk\n";
              Log.getLogWriter().info(aStr);
              errStr.append(aStr);
            }

            // check that for member with vmID, it recovered from disk or not as appropriate
            if (expectDiskRecovery != null) { // the test specified disk recovery or gii
              if (expectDiskRecovery) {
                if (numDiskRecoveries <= 0) {
                  String aStr = "For vmID " + vmID + ", expected all regions to recover from " +
                  "disk, but the number of regions that recovered from disk is " + numDiskRecoveries + "\n";
                  Log.getLogWriter().info(aStr);
                  errStr.append(aStr);
                }
              } else { // expect gii
                if (numGiis <= 0) {
                  String aStr = "For vmID " + vmID + ", expected all regions to obtain data from " +
                  "gii, but the number of regions that obtained data from gii is " + numDiskRecoveries + "\n";
                  Log.getLogWriter().info(aStr);
                  errStr.append(aStr);
                }
              }
            }
          }
        }
      }
      if (expectAnyVmToRecoverFromDisk) {// disk recovery was expected in this test
        if (vmIDsWithDiskRecovery.size() != 1) {
          String aStr = "Expected only 1 vm to recover from disk, but the following vmIDs recovered from disk: " +
          vmIDsWithDiskRecovery + "\n";
          Log.getLogWriter().info(aStr);
          errStr.append(aStr);
        }
      }
      Log.getLogWriter().info(logStr.toString());
      if (errStr.length() > 0) {
        throw new TestException(errStr.toString());
      }
    }
  }

  /** Using the blackboard to retrieve information written by each vm, verify
   *   that each vm obtained data either by disk recovery or gii as appropriate
   *   for a tie scenario. 
   */
  private static void verifyTieStartup() {
    // init
    // parallel lists
    List<Integer> vmIdList = new ArrayList(); // list of vm ids
    List<Set<String>> diskRecoveryRegions = new ArrayList(); // list of Sets containing region names
    List<Set<String>> giiRegions = new ArrayList(); // list of Sets containing region names
    Map sharedMap = RecoveryBB.getBB().getSharedMap().getMap();
    Iterator it = sharedMap.keySet().iterator();
    StringBuffer logStr = new StringBuffer();
    StringBuffer errStr = new StringBuffer();

    // extract information from the blackboard into the parallel lists defined above
    while (it.hasNext()) {
      Object key = it.next();
      if (key instanceof String) {
        String strKey = (String)key;
        if (strKey.startsWith(startupStatsForVmKey)) { // found start up stats for a vm
          Map<String, Integer> statsMap = (Map)(sharedMap.get(strKey));
          Integer vmID = Integer.valueOf(strKey.substring(startupStatsForVmKey.length(), strKey.length()));
          Log.getLogWriter().info("Found startup stats for vmID " + vmID +  ": " + statsMap);
          vmIdList.add(vmID);
          logStr.append("For vmID " + vmID + ", startup stats are:\n");
          Set<String> diskRegionsSet = new HashSet();
          Set<String> giiRegionsSet = new HashSet();
          diskRecoveryRegions.add(diskRegionsSet);
          giiRegions.add(giiRegionsSet);
          for (String statsMapKey: statsMap.keySet()) { 
            if (statsMapKey.startsWith(localInitializationsKey)) {
              String regionName = statsMapKey.substring(localInitializationsKey.length(), statsMapKey.length());
              int localInit = statsMap.get(statsMapKey);
              if (localInit > 0) {
                diskRegionsSet.add(regionName);
              }
              logStr.append("   " + regionName + " localInitializations (disk recovery): " + localInit + "\n");
            } else if (statsMapKey.startsWith(remoteInitializationsKey)) {
              String regionName = statsMapKey.substring(remoteInitializationsKey.length(), statsMapKey.length());
              int remoteInit = statsMap.get(statsMapKey);
              if (remoteInit > 0) {
                giiRegionsSet.add(regionName);
              }
              logStr.append("   " + regionName + " remoteInitalizations (gii): " + remoteInit + "\n");
            } else {
              throw new TestException("Unknown key in stats map: " + statsMapKey);
            }
          }
        }
      }
    }

    // now do "tie" validation
    if ((vmIdList.size() != diskRecoveryRegions.size()) ||
        (diskRecoveryRegions.size() != giiRegions.size())) {
      Log.getLogWriter().info(logStr.toString());
      throw new TestException("Test error; vmIdList size is " + vmIdList.size() + ", diskRecoveryRegions size is " +
          diskRecoveryRegions.size() +", giiRegions size is " + giiRegions.size());
    }

    // check that each region in each vmID obtained data from disk or gii but not both
    for (int i = 0; i < vmIdList.size(); i++) {
      Integer vmID = vmIdList.get(i);
      Set<String> diskSet = diskRecoveryRegions.get(i);
      Set<String> giiSet = giiRegions.get(i);
      Set<String> intersection = new HashSet(diskSet);
      intersection.retainAll(giiSet);
      if (intersection.size() != 0) {
        errStr.append("Stats show that region(s) " + intersection + " in vmID " + vmID + 
        " both obtained data from disk and gii\n");
      }
    }

    // check that if a region did disk recovery in one vm, it did gii in all other vms
    // in other words, check that only 1 vm in the system recovered a particular
    // region from disk, while all other vms did a gii
    for (int i = 0; i < vmIdList.size(); i++) {
      Integer vmID = vmIdList.get(i);
      Set<String> diskSet = diskRecoveryRegions.get(i);
      for (int j = 0; j < vmIdList.size(); j++) { // iterator all other vms
        if (i != j) { // want to check OTHER vms
          Integer otherVmID = vmIdList.get(j);
          Set<String> otherGiiSet = giiRegions.get(j);
          if (!otherGiiSet.containsAll(diskSet)) {
            errStr.append("vm with id " + vmID + " recovered the following regions from disk " + diskSet + 
                ", expected those regions in vm id " + otherVmID + " to obtain data via gii, but the regions " +
                " that obtained data from gii are " + otherGiiSet + "\n");
          }
        }
      }
    }

    // check that each region recovered from disk 
    Set<String> diskRegionsUnion = new HashSet();
    for (Set diskRegionsSet: diskRecoveryRegions) {
      diskRegionsUnion.addAll(diskRegionsSet);
    }
    Set testRegions = (Set)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));
    if (!diskRegionsUnion.equals(testRegions)) {
      errStr.append("Expected region(s) " + testRegions + " to all recover from disk, but " +
          diskRegionsUnion + " recovered from disk\n");
    }

    Log.getLogWriter().info(logStr.toString());
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
  }

  /** Depending on the test strategy, stop and restart vms in particular orders and
   *   verify recovery ran in the expected vms.
   * @throws ClientVmNotFoundException
   * @throws InterruptedException
   */
  private static void latestRecoveryTest() throws ClientVmNotFoundException,
  InterruptedException {

    // initialize
    Object[] anArr = StopStartVMs.getOtherVMsDivided(new String[] {"locator"});
    List<ClientVmInfo> allOtherVMsExceptLast = (List)(anArr[0]);
    List<String> stopModesExceptLast = (List)(anArr[1]);
    ClientVmInfo lastVM = (ClientVmInfo)(anArr[2]);
    String lastStopMode = (String)(anArr[3]);
    List<ClientVmInfo> allOtherVMs = (List)(anArr[4]);
    List<String> stopModeList = (List)(anArr[5]);

    // stop and start vms based on the test strategy
    String testStrategy = RecoveryPrms.getTestStrategy();
    Log.getLogWriter().info("Test strategy is " + testStrategy);
    if (testStrategy.equalsIgnoreCase("tie")) {
      // test strategy: Start up vms, load, stop vms, start up all but one and verify 
      //   they wait, start up the last and verify that only of the vms recovered from 
      //   disk and all others did a gii.
      // close task verifies that one vm recovered from disk, all others from gii
      RecoveryBB.getBB().getSharedMap().put(recoveryCanBeTieKey, new Boolean(true));
      for (ClientVmInfo info: allOtherVMs) { // make each vm not record membership info to cause a tie
        int vmID = info.getVmid();
        RecoveryBB.getBB().getSharedMap().put(RecoveryBB.allowPersistedMemberInfo + vmID, new Boolean(false));
      }
      StopStartVMs.stopVMs(allOtherVMs, stopModeList);
      List<Thread> thrList = StopStartVMs.startAsync(allOtherVMsExceptLast);
      if (RecoveryPrms.getConcurrentRegionCreation()) {
        Set<String> allRegionNames = (Set<String>)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));
        RecoveryTestVersionHelper.verifyWaiting(lastVM, allOtherVMsExceptLast);
        verifyNoRegionCreation(allOtherVMsExceptLast, allRegionNames);
      } else {
        RecoveryTestVersionHelper.verifyWaiting(lastVM, allOtherVMsExceptLast);
        Set regNameSet = new HashSet();
        regNameSet.add("/Region_1");
        verifyNoRegionCreation(allOtherVMsExceptLast, regNameSet);
      }
      StopStartVMs.startVM(lastVM);
      for (Thread aThread: thrList) { // wait for all threads to complete
        aThread.join();
      }
      Log.getLogWriter().info("Done starting " + allOtherVMsExceptLast);
    } else if (testStrategy.equalsIgnoreCase("latestStartsFirst")) {
      // test strategy: Start up vms, load, stop all vms except one, stop the last vm, 
      //    restart the last vm and verify it recovers from disk, restart all other vms and 
      //    verify they do a gii
      StopStartVMs.stopVMs(allOtherVMsExceptLast, stopModesExceptLast); // stop all except one
      MasterController.sleepForMs(20000);
      StopStartVMs.stopVM(lastVM, lastStopMode); // stop the last one
      StopStartVMs.startVM(lastVM); // restart the last one
      StopStartVMs.startVMs(allOtherVMsExceptLast);
      markDataSource(lastVM, true);
      markDataSource(allOtherVMsExceptLast, false);
      markDataSource(allOtherVMsExceptLast, false);
    } else if (testStrategy.equalsIgnoreCase("latestStartsLast")) {
      // test strategy: Start up vms, load, stop all vms except one, stop the last vm, 
      //   restart all vms except the last one, verify they are waiting the the last one, 
      // start the last one, verify the last one recovered from disk and all others did gii
      StopStartVMs.stopVMs(allOtherVMsExceptLast, stopModesExceptLast); // stop all except one
      MasterController.sleepForMs(20000);
      StopStartVMs.stopVM(lastVM, lastStopMode); // stop the last one
      List<Thread> thrList = StopStartVMs.startAsync(allOtherVMsExceptLast);
      if (RecoveryPrms.getConcurrentRegionCreation()) {
        Set<String> allRegionNames = (Set<String>)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));
        RecoveryTestVersionHelper.verifyWaiting(lastVM, allOtherVMsExceptLast);
        verifyNoRegionCreation(allOtherVMsExceptLast, allRegionNames);
      } else {
        RecoveryTestVersionHelper.verifyWaiting(lastVM, allOtherVMsExceptLast);
        Set regNameSet = new HashSet();
        regNameSet.add("/Region_1");
        verifyNoRegionCreation(allOtherVMsExceptLast, regNameSet);
      }
      StopStartVMs.startVM(lastVM);
      for (Thread aThread: thrList) { // wait for all the others to complete starting (they will do a gii)
        aThread.join();
      }
      markDataSource(lastVM, true);
      markDataSource(allOtherVMsExceptLast, false);
      markDataSource(allOtherVMsExceptLast, false);
    } else if (testStrategy.equalsIgnoreCase("forceRecovery")) {
      // test strategy: Start up vms, load, stop all vms except one, stop the last vm
      //     start up all vms exception the last one, verify they are waiting for the last 
      //     one, force them to recover without ever starting the last one.
      if (allOtherVMsExceptLast.size() > 1) {
        RecoveryBB.getBB().getSharedMap().put(recoveryCanBeTieKey, new Boolean(true));
      }
      StopStartVMs.stopVMs(allOtherVMsExceptLast, stopModesExceptLast);
      MasterController.sleepForMs(2000);
      StopStartVMs.stopVM(lastVM, lastStopMode);
      List<Thread> thrList = StopStartVMs.startAsync(allOtherVMsExceptLast);
      Set<String> allRegionNames = new TreeSet((Set)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey)));
      RecoveryTestVersionHelper.verifyWaiting(lastVM, allOtherVMsExceptLast);
      verifyNoRegionCreation(allOtherVMsExceptLast, allRegionNames);
      RecoveryTestVersionHelper.forceRecovery(true);
      for (Thread aThread: thrList) { // threads should all complete now
        aThread.join();
      }
    } else if (testStrategy.equalsIgnoreCase("membersNotSimultaneous")) {
      // test strategy: Start up group1 and group2 vms, load, stop group1 vms, start
      // group3 vms, stopgrup2 vms, start group1 vms. Group1 will gii; nobody
      // recovers from disk.
      List <ClientVmInfo> group1VMs = getMarkedVMs("group1");
      List<ClientVmInfo> group2VMs = getMarkedVMs("group2");
      List<ClientVmInfo> group3VMs = getMarkedVMs("group3");
      StopStartVMs.stopVMs(group3VMs, stopModeList.subList(0, group3VMs.size())); // get rid of these uninitialized vms
      StopStartVMs.stopVMs(group1VMs, stopModeList.subList(0, group1VMs.size()));
      StopStartVMs.startVMs(group3VMs); // now they are initialized
      StopStartVMs.stopVMs(group2VMs, stopModeList.subList(0, group2VMs.size()));
      StopStartVMs.startVMs(group1VMs);
      markDataSource(group1VMs, false);
      markDataSource(group3VMs, false);
    } else if (testStrategy.equalsIgnoreCase("refuseRecovery")) {
      // test strategy: Start up one or more vms, load, stop the vms, start up one or more
      //     vms,  restart the first vms, they will refuse to recover.
      NameBB.getBB().getSharedCounters().zero(NameBB.POSITIVE_NAME_COUNTER); // reset for a new load of keys when noInitVMs starts up
      List<ClientVmInfo> fullInitVMs = getMarkedVMs("fullInitialization"); // created cache, regions, data already
      List<ClientVmInfo> noInitVMs = getMarkedVMs("noInitialization"); // no cache, regions or data
      StopStartVMs.stopVMs(allOtherVMs, stopModeList); // stop all
      StopStartVMs.startVMs(noInitVMs); // this creates cache, regions, data
      StopStartVMs.startVMs(fullInitVMs); // this refuses to start; validation occurs in the init task on startup
    } else {
      throw new TestException("Unknown test strategy: " + testStrategy);
    }
  }


  /** Verify that the given vms have not created any regions.
   * @param otherVMInfos The members that should not have created any regions.
   */
  private static void verifyNoRegionCreation(List<ClientVmInfo> members, Set<String> regions) {
    Log.getLogWriter().info("Verifying that vm(s) " + members + " are waiting");
    for (ClientVmInfo info: members) {
      for (String aRegionName: regions) {
        if (RecoveryBB.getBB().getSharedMap().containsKey(getRegionCreatedKey(info.getVmid(), aRegionName))) {
          throw new TestException("Expected vm with id " + info.getVmid() + 
          " to wait for recovery,  it has already created region(s)");
        }
      }
    }
    Log.getLogWriter().info("Vm(s) " + members + " did not create any regions");
  }

  /** Serial execution test that steps back and forth between two vms to verify
   *   that the disk files are updated when initialized from gii and can be used for
   *   recovery
   */
  private void recoverAfterGiiTest() throws Exception {
    // init
    long executionNumber = logExecutionNumber();
    List <Integer> otherVmIDs = ClientVmMgr.getOtherClientVmids();
    if (otherVmIDs.size() != 1) {
      throw new TestException("Expected 2 vms in this test but found " + otherVmIDs.size());
    }
    ClientVmInfo otherVM = new ClientVmInfo(otherVmIDs.get(0));
    List<ClientVmInfo> otherVMList = new ArrayList();
    otherVMList.add(otherVM);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    // If this is the first excution, stop the other vm (after this it will already be stopped when this task runs)
    if (executionNumber == 1) { // first execution
      Log.getLogWriter().info("First execution of task, stopping the other vm");
      StopStartVMs.stopVM(otherVM, ClientVmMgr.NiceExit);
    }

    if ((executionNumber % 2) == 1) { // do step one; this is vm A (vm B is not up); this is the only vm 
      Log.getLogWriter().info("Task is doing test step 1");
      // start the other vm while ops are running
      RecoveryBB.getBB().getSharedMap().remove(allRegionsSnapshotKey); // prevents restarted vm from doing validation
      // when it starts up (done below), ops are occurring concurrently so we can't do validation then in the init task
      long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      List<Thread> thrList = StopStartVMs.startAsync(otherVMList);
      doOperations(minTaskGranularityMS);
      for (Thread aThread: thrList) {
        try {
          aThread.join();
        } catch (InterruptedException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }
      // now both vms are up, vm A and vm B

      // write to the blackboard and stop myself
      SilenceListener.waitForSilence(30, 1000);
      writeSnapshot();
      try {
        ClientVmMgr.stop("Stopping myself", ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND);
        // the above should not return; this vm will be killed while in the above call; restart is onDemand
        throw new TestException("Test error; did not expect to get here");
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } else { // do step 2; this vm is the only one up
      Log.getLogWriter().info("Task is doing test step 2");
      Log.getLogWriter().info("Verifying from snapshot BEFORE stopping this vm and recovering from disk");
      verifyFromSnapshot(); 
      waitForOfflineEvents();

      // after the below stopping of the current vm, the restart is immediate so this vm
      // will then recover from its disk and its init task will verify from the snapshot
      Log.getLogWriter().info("Stopping myself so restarted vm can verify disk files with init task on startup");
      try {
        ClientVmMgr.stop("Stopping myself (followed by init task validation)", ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
        // the above line will not return; when this vm is stopped, no vms are up, but this vm will restart immediately
        // and run its init task which verifies from the snapshot; when this vm restarts, it will recover from disk; after
        // it does the init task, it will then be assigned this task (since it's the only one up) and will become vm A and
        // will do step 1 above
        throw new TestException("Test error; did not expect to get here");
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  /** Wait for this member to recognize that another member has left.
   * 
   */
  private void waitForOfflineEvents() {
    int numRegions = allRegions.size();
    do {
      int counterValue = PersistenceCountObserver.afterPersistedOfflineCount.get();
      if (counterValue == numRegions) {
        Log.getLogWriter().info("Found expected number of afterPersistedOffline calls: " + counterValue);
        return;
      } else {
        Log.getLogWriter().info("Waiting for count of afterPersistedOffline calls to be " + numRegions +
            ", current count is " + counterValue);
        MasterController.sleepForMs(1000);
      }
    } while (true);
  }

  /**
   * Allow the responder vms to do operations and pause them.
   * When paused a responder leader will write a snapshot to disk.
   * The controller will create a backup during the pause that can be compared to the snapshot.
   */
  private void backupController() {
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Pausing...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing",
        RecoveryBB.pausing, TestHelper.getNumThreads(), true, -1, 2000);

    // wait for silence, just to be safe
    SummaryLogListener.waitForSilence(30, 1000);

    // everybody has paused, direct one vm to write to the blackboard
    Log.getLogWriter().info("Directing responder to write to the blackboard...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten",
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // Wait for backup
    RecoveryTestVersionHelper.performBackup();
    Log.getLogWriter().info("Completed backup for iteration " + RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.pausing);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.leader);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);            

    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }    
  }
  
  /**
   * Serializes region snapshot to disk.
   * @param snapshot a multi-region snapshot.
   */
  private void snapshotToDisk(Map snapshot) throws Exception {
    ObjectOutputStream oos = null;

    try {
      long snapshotNumber = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber);
      String snapFileName = "snapshotForBackup_" + snapshotNumber + ".ser";

      oos = new ObjectOutputStream(new FileOutputStream(snapFileName));
      oos.writeObject(snapshot);      
      oos.flush();
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } finally {
      if(null != oos) {
        oos.close();
      }
    }
  }
  
  /**
   * Do operations and pause when directed by the controller.
   * Write a region snapshot to disk when paused (and given leadership).
   */
  private void backupResponder() throws Exception {
    long pausing = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing);
    
    if (pausing > 0) { // controller has signaled to pause
      if ((Boolean)threadIsPaused.get()) { // this thread has already paused, so don't increment counter
        Log.getLogWriter().info("Thread has paused");
      } else {
        Log.getLogWriter().info("This thread is pausing");
        threadIsPaused.set(Boolean.TRUE);
        long pausingValue = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.pausing);
        Log.getLogWriter().info("operationsResponder: pausing is " + pausingValue);
      }      

      long writeSnapshot = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.writeSnapshot);
      
      if (writeSnapshot > 0) {
        long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
        if (leader == 1) { // this is the thread to write the snapshot
          Log.getLogWriter().info("This thread is the leader; it will write the snapshot");
          if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
              RecoveryPrms.getConvertWithNewVersionXml()) {
            removeExtraRegionsFromAllRegions();
          }
          snapshotToDisk(writeSnapshot());
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
        }      
      }
      MasterController.sleepForMs(5000);
    } else { // not pausing
      threadIsPaused.set(Boolean.FALSE);
      long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      doOperations(minTaskGranularityMS);
    }    
  }
  
  /** Allow the responder vms to do operations, pause them, then stop and restart
   *  them allowing them to recover.
   */
  private void operationsController() {
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Pausing...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing",
        RecoveryBB.pausing, TestHelper.getNumThreads(), true, -1, 2000);

    // wait for silence, just to be safe
    SummaryLogListener.waitForSilence(30, 1000);

    // everybody has paused, direct one vm to write to the blackboard
    Log.getLogWriter().info("Directing responder to write to the blackboard...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten",
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // now stop everybody and bring them back for recovery
    List<ClientVmInfo> targetVMs = new ArrayList();
    if (RecoveryPrms.getUseShutDownAll()) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      if (adminDS == null) {
        throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
      }
      Object[] anArr = StopStartVMs.shutDownAllMembers(adminDS);
      targetVMs = (List<ClientVmInfo>)(anArr[0]);
    } else {
      List vmIDs = ClientVmMgr.getOtherClientVmids();
      List<String> stopModesList = new ArrayList();
      for (Object vmID: vmIDs) {
        targetVMs.add(new ClientVmInfo((Integer)vmID));
        stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
      StopStartVMs.stopVMs(targetVMs, stopModesList);
    }
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.pausing);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.leader);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    // before restarting, take a snapshot of the disk files (this is for debug purposes only
    // so will be commented out unless we are debugging)
    // createDiskDirBackup("backupForExeNumber_" + RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));

    // restart all vms; init tasks for all vms compare against the blackboard, so if
    // this returns then all is well
    PRObserver.initialize();
    StopStartVMs.startVMs(targetVMs);
    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

  /** Do operations and pause when directed by the controller
   */
  private void operationsResponder() throws Exception {
    long pausing = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing);
    if (pausing > 0) { // controller has signaled to pause
      if ((Boolean)threadIsPaused.get()) { // this thread has already paused, so don't increment counter
        Log.getLogWriter().info("Thread has paused");
      } else {
        Log.getLogWriter().info("This thread is pausing");
        threadIsPaused.set(new Boolean(true));
        RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.pausing);
      }
      long writeSnapshot = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.writeSnapshot);
      if (writeSnapshot > 0) {
        long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
        if (leader == 1) { // this is the thread to write the snapshot
          Log.getLogWriter().info("This thread is the leader; it will write the snapshot");
          if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
              RecoveryPrms.getConvertWithNewVersionXml()) {
            removeExtraRegionsFromAllRegions();
          }
          writeSnapshot();
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
        }
      }
      MasterController.sleepForMs(5000);
    } else { // not pausing
      long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      doOperations(minTaskGranularityMS);
    }
  }

  /** Direct gemfire members hosting the region to do ops, pause, write the snapshot
   *  and export the entire cache.  Recyle VMs and have responders re-initialize the 
   *  region hierarchy from xml and 1 member import the data.  Everyone validates against
   *  the map of region snapshots in the blackboard.
   */
  private void snapshotController() {
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Pausing...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing",
        RecoveryBB.pausing, TestHelper.getNumThreads(), true, -1, 2000);

    // wait for silence, just to be safe
    SummaryLogListener.waitForSilence(30, 1000);

    // everybody has paused, direct one vm to write to the blackboard
    // and to export the snapshot
    Log.getLogWriter().info("Directing responder to write to the blackboard and export snapshot ...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten",
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // now stop everybody and bring them back for recovery
    List<ClientVmInfo> targetVMs = new ArrayList();
    if (RecoveryPrms.getUseShutDownAll()) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      if (adminDS == null) {
        throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
      }
      Object[] anArr = StopStartVMs.shutDownAllMembers(adminDS);
      targetVMs = (List<ClientVmInfo>)(anArr[0]);
    } else {
      List vmIDs = ClientVmMgr.getOtherClientVmids();
      List<String> stopModesList = new ArrayList();
      for (Object vmID: vmIDs) {
        targetVMs.add(new ClientVmInfo((Integer)vmID));
        stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
      StopStartVMs.stopVMs(targetVMs, stopModesList);
    }
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.pausing);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.importSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotImported);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.leader);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    // before restarting, remove the disk directories

    // restart all vms; one vm will import the snapshot, then 
    // remaining vms compare against the blackboard
    PRObserver.initialize();
    StopStartVMs.startVMs(targetVMs);
    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

  /** Controller for disk conversion test. This is a variant of operationsController.
   *  
   *  There are 2 distributed systems, one for an old version and one for a new
   *  version. This task is run by a thread that does not have a cache or region
   *  for either distributed system, but it is able to stop/start jvms in either
   *  distributed system.
   *  
   *  1 - Kill the jvms in the new version distributed system. These jvms
   *      will be running the diskConversionOperationsResponder task, and will be sleeping
   *      waiting to be killed. Once they are gone, they will not get any more
   *      tasks until they are started, so now only the old version jvms are
   *      running the diskConversionOperationsController task and are populating the region
   *      with data.
   *  2 - Signal to the old jvms to pause; once paused, one is randomly 
   *      chosen to write a snapshot to the blackboard.
   *  3 - Stop all the old jvms. 
   *  4 - Convert the disk files from the old version to the new version.
   *  6 - Start up the new version jvms with the converted disk files. Dynamic
   *      init tasks compare the entries recovered from disk against the
   *      blackboard. 
   *  7 - From this point, the test only uses the new version vms. It will
   *      pause, one jvm will write to the blackboard, restart and verify
   *      against the blackboard in an init task. 
   *  
   */  
  private void diskConvOperationsController() {
    logExecutionNumber();
    checkForLastIteration();

    // if this is the first time to execute this task, do steps 1 through 6
    boolean firstControllerExecution = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1;
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    if (firstControllerExecution) {
      // stop the new version jvms
      List<String> stopModeList = new ArrayList();
      for (int i = 1; i <= newVersionVMs.size(); i++) {
        stopModeList.add("MEAN_KILL");
      }
      StopStartVMs.stopVMs(newVersionVMs, stopModeList);
    }

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Pausing...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing",
        RecoveryBB.pausing, TestHelper.getNumThreads()/2, // either new version or old version running, but not both
        true, -1, 2000);

    // wait for silence, just to be safe
    SummaryLogListener.waitForSilence(30, 1000);

    // everybody has paused, direct one vm to write to the blackboard
    Log.getLogWriter().info("Directing responder to write to the blackboard...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten",
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // stop either 1) all old vms (if this is the first controller execution)
    //          or 2) all new vms (if not the first controller execution)
    List targetVMs = StopStartVMs.getAllVMs();
    if (firstControllerExecution) { // running old version vms
      targetVMs = StopStartVMs.getMatchVMs(targetVMs, "oldVersion");
    } else { // running new version vms
      targetVMs = StopStartVMs.getMatchVMs(targetVMs, "newVersion");
    }
    List<String> stopModesList = new ArrayList();
    for (int i = 1; i <= targetVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    if (RecoveryPrms.getUseShutDownAll()) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      if (adminDS == null) {
        throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
      }
      Object[] anArr = StopStartVMs.shutDownAllMembers(adminDS);
      targetVMs = (List<ClientVmInfo>)(anArr[0]);
    }
    StopStartVMs.stopVMs(targetVMs, stopModesList);

    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.pausing);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.leader);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    if (firstControllerExecution) { // just stopped old version vms, now convert and start up new version vms
      int convertedIndex = doSingleConvert(true);
      List<String> newVersionXmlFileList = (List<String>)RecoveryBB.getBB().getSharedMap().get(newVersionXmlFilesKey);
      String newVersionXmlFile = newVersionXmlFileList.get(convertedIndex);
      int vmId = getVmIdFromXmlFileName(newVersionXmlFile);
      int startFirstIndex = -1;
      for (int i = 0; i < newVersionVMs.size(); i++) {
        if (newVersionVMs.get(i).getVmid().intValue() == vmId) {
          startFirstIndex = i;
          break;
        }
      }
      
      // start the converted new version jvm first, then the others gii from it
      // starting up one first is a requirement of the conversion tool, otherwise
      // if they all start up together, we will get a ConflictingDataException
      List<ClientVmInfo> startFirstList = new ArrayList();
      startFirstList.add(newVersionVMs.get(startFirstIndex));
      List<Thread> startThread = StopStartVMs.startAsync(startFirstList);
      // when the startThread has recovered, we can start the rest
      TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter", 
          RecoveryBB.doneVerifyingCounter,  1,
          false, -1, 2000);
      newVersionVMs.remove(startFirstIndex);
      StopStartVMs.startVMs(newVersionVMs);
      for (Thread aThread: startThread) {// should just be one thread
        try {
          aThread.join();
        } catch (InterruptedException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }
    } else { // just stopped new version vms; they should all startup together
      // restart vms; init tasks for all vms compare against the blackboard, so if
      // this returns then all is well
      StopStartVMs.startVMs(targetVMs);
    }

    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

  //=================================================
  // methods to create a region hierarchy
  /**
   * Create a region hierarchy from hydra parameters.
   * The number of regions is capped at RecoveryPrms.maxRegions.
   * 
   * @param regionConfigNames A List of hydra region config names to choose
   *                      from for each region to be created
   * @param diskStoreNames A List of diskStore names to be used for each
   *                      region if the diskStoreName is not already set. Randomly choose
   *                      one for each region created that needs disk either through 
   *                      persistence or overflow to disk.
   */
  private void createRegionHier(List<String> regionConfigNames,
      List<String> diskStoreNames) throws Throwable {

    // initialize
    int numRootRegions = RecoveryPrms.getNumRootRegions();
    int numSubregions = RecoveryPrms.getNumSubregions();
    int hierDepth = RecoveryPrms.getRegionHierarchyDepth();
    boolean concurrentCreation = RecoveryPrms.getConcurrentRegionCreation();
    Log.getLogWriter().info("In createRegionHier: concurrentRegionCreation is " + concurrentCreation);
    long seed = TestConfig.tab().longAt(hydra.Prms.randomSeed);
    Log.getLogWriter().info("Initializing region generation random seed to " + seed);
    GsRandom rand = new GsRandom(seed);
    List<String> prRegionConfigNames = new ArrayList();
    List<String> nonPrRegionConfigNames = new ArrayList();
    for (String configName: regionConfigNames) {
      if (configName.startsWith("pr")) { // config defines a PR; it can only be a leaf in the hier 
        prRegionConfigNames.add(configName);
      } else {
        nonPrRegionConfigNames.add(configName);
      }
    }
    if ((nonPrRegionConfigNames.size() == 0) && (hierDepth > 1)) {
      // Test has specified only pr regions with a region hierarchy depth; PRs cannot
      // have subregions so this configuration cannot be done
      throw new TestException("Test configuration problem; test has configured only PR regions " +
          " with a hierarchy depth of " + hierDepth + ", PRs cannot have subregions so this configuration " +
      " is invalid");
    }
    List<String> regionConfigNamesToUse = regionConfigNames;
    if (hierDepth > 1) {
      regionConfigNamesToUse = nonPrRegionConfigNames;
    }

    // create the root regions
    final Vector<Region> regions = new Vector();
    regions.setSize(numRootRegions);
    List<Thread> threadList = new ArrayList();
    final List<Throwable> exceptionList = new ArrayList();
    int randomIndex = rand.nextInt(0, regionConfigNamesToUse.size()-1);
    for (int i=1; i <= numRootRegions; i++) {
      final String regionName = "Region_" + regionNameCounter++;
      final String regionConfigName = regionConfigNamesToUse.get(randomIndex);
      randomIndex = (randomIndex + 1) % (regionConfigNamesToUse.size());
      RegionAttributes baseAttr = RegionHelper.getRegionAttributes(regionConfigName);
      AttributesFactory attrFac = RegionHelper.getAttributesFactory(regionConfigName);
      RecoveryTestVersionHelper.setRandomDiskStore(baseAttr, attrFac, diskStoreNames, rand);
      final RegionAttributes attr = attrFac.create();
      final int index = i-1;
      Thread regionCreationThread = new Thread(new Runnable() {
        public void run() {
          try {
            String diskStoreNameLogging = RecoveryTestVersionHelper.createDiskStore(attr);
            Log.getLogWriter().info("Starting creation of root region " + regionName + " using region config name " + 
                regionConfigName + " " + diskStoreNameLogging);
            // do NOT use hydra's RegionHelper here to create the region; the point is to
            // create regions concurrently (when concurrentCreation is true) and hydra
            // synchronizes so we cannot see how the product handles concurrent region
            // creation; here we make calls directly to the product to create the region
            long startTime = System.currentTimeMillis();
            Region aRegion = theCache.createRegion(regionName, attr);
            long duration = System.currentTimeMillis() - startTime;
            Log.getLogWriter().info("Finished creation of region" + aRegion.getFullPath() + ", creation took " + duration + "ms");
            regions.set(index, aRegion);
            RecoveryBB.getBB().getSharedMap().put(getRegionCreatedKey(RemoteTestModule.getMyVmid(), regionName),
                new Boolean(true));
            DataPolicy dataPolicy = attr.getDataPolicy();
            if (dataPolicy.withPersistence() && dataPolicy.withReplication()) {
              RecoveryBB.getBB().getSharedMap().put(persistentRegionKey + RemoteTestModule.getMyVmid(), new Boolean(true));
            }
          } catch (Exception e) {
            String errStr = "Caught " + TestHelper.getStackTrace(e) + " while creating region " + regionName;
            Log.getLogWriter().info(errStr);
            synchronized (exceptionList) {
              exceptionList.add(e);
            }
          } catch (Error e) {
            String errStr = "Caught " + TestHelper.getStackTrace(e) + " while creating region " + regionName;
            Log.getLogWriter().info(errStr);
            synchronized (exceptionList) {
              exceptionList.add(e);
            }
          }

        }
      });
      if (!concurrentCreation) { // create each region serially, so start the thread and wait for it to create a single region
        regionCreationThread.start();
        try {
          regionCreationThread.join();
        } catch (InterruptedException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        if (exceptionList.size() > 0) {
          throw exceptionList.get(0); // throw the first exception we got
        }
      } else { // save the thread to start later
        threadList.add(regionCreationThread);
      }
    } // for

    if (concurrentCreation) { // now execute the threads for concurrent creation
      for (Thread aThread: threadList) { // start all threads
        aThread.start();
      }
      for (int i = 0; i < threadList.size(); i++) { // wait for all threads
        try {
          threadList.get(i).join();
        } catch (InterruptedException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        if (exceptionList.size() > 0) {
          throw exceptionList.get(0); // throw the first exception we got
        }
      }
    }
    allRegions = new ArrayList(regions);

    // create the subregions
    int currentDepth = 1;
    List<Region> currentRegList = regions;
    if (numSubregions > 0) {
      while (currentDepth < hierDepth) {
        if (currentDepth == hierDepth-1) { // creating leaf regions
          if (prRegionConfigNames.size() > 0) { // allow PRs to be leafs
            regionConfigNamesToUse = prRegionConfigNames;
          } 
        }
        List createdRegions = new ArrayList();
        for (int i = 0; i < currentRegList.size(); i++) {
          Region aRegion = (Region)(currentRegList.get(i));
          List newRegions = createSubregions(aRegion, regionConfigNamesToUse, diskStoreNames, seed+i);
          createdRegions.addAll(newRegions);
        }
        currentDepth++;
        currentRegList = createdRegions;
      } // while
    }

    // write  region names to the blackboard
    Set<String> regionNames = new HashSet();
    for (Region aRegion: allRegions) {
      regionNames.add(aRegion.getFullPath());
    }
    RecoveryBB.getBB().getSharedMap().put(allRegionNamesKey, regionNames);
  }

  /** Using hydra parameters, create subregions of region using the given config
   *   name. 
   * @param region The parent region of the subregions to create.
   * @param regionConfigNames A List of hydra region config names to choose
   *                      from for each region to be created
   * @param diskStoreNames A List of diskStore names to be used for each
   *                      region if the diskStoreName is not already set. Randomly choose
   *                      one for each region created that needs disk either through 
   *                      persistence or overflow to disk.
   *  @param randomSeed the random seed to use to create the randm number generator
   *                  this guarantees that each vm creates the same
   *                  sequence of region with the same attributes.
   *                 
   * @return A List of subregions created.
   */
  private List createSubregions(Region region, 
      List<String> regionConfigNames, 
      List<String> diskStoreNames,
      long randomSeed) {
    GsRandom rand = new GsRandom(randomSeed);
    List createdRegions = new ArrayList();
    int numSubregions = RecoveryPrms.getNumSubregions();
    int randomIndex = rand.nextInt(0, regionConfigNames.size()-1);
    for (int i = 1; i <= numSubregions; i++) {
      String regionName = "Region_" + regionNameCounter++;
      String regionConfigName = regionConfigNames.get(randomIndex);
      randomIndex = (randomIndex + 1) % regionConfigNames.size();
      RegionAttributes attr = RegionHelper.getRegionAttributes(regionConfigName);
      AttributesFactory attrFac = RegionHelper.getAttributesFactory(regionConfigName);
      RecoveryTestVersionHelper.setRandomDiskStore(attr, attrFac, diskStoreNames, rand);
      attr = attrFac.create();
      RecoveryTestVersionHelper.createDiskStore(attr);
      long startTime = System.currentTimeMillis();
      Region subregion = region.createSubregion(regionName, attr);
      long duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("Finished creation of region" + subregion.getFullPath() + ", creation took " + duration + "ms");
      RecoveryBB.getBB().getSharedMap().put(getRegionCreatedKey(RemoteTestModule.getMyVmid(), regionName),
          new Boolean(true));
      createdRegions.add(subregion);
      allRegions.add(subregion);
    }
    return createdRegions;
  }

  /** Return a string representation of the region hierarchy for this test. 
   * 
   */
  protected static String regionHierarchyToString() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      return "Cache is null; unable to get region hierarchy";
    }
    Set<Region<?,?>> roots = theCache.rootRegions();
    StringBuffer aStr = new StringBuffer();
    int totalNumRegions = 0;
    for (Region aRegion: roots) {
      aStr.append(aRegion.getFullPath() + "  (size " + aRegion.size() + " dataPolicy " +
          aRegion.getAttributes().getDataPolicy() + " eviction " + aRegion.getAttributes().getEvictionAttributes());
      RecoveryTestVersionHelper.logDiskStore(aRegion, aStr);
      aStr.append(")\n");
      totalNumRegions++;
      Object[] tmp = subregionsToString(aRegion, 1);
      aStr.append(tmp[0]);
      totalNumRegions += (Integer)(tmp[1]);
    }
    return "Region hierarchy with " + totalNumRegions + " regions\n" + aStr.toString();
  }

  /** Given a region and the current level in the region hierarchy, return a String
   *   representation of the region's subregions. 
   *   
   * @param region The parent whose children are return as a String.
   * @param level The current hiearachy level.
   * @return [0] A String representation of region's subregions.
   *                 [1] The number of subregions in [0]
   */
  private static Object[] subregionsToString(Region region, int level) {
    Set<Region> subregionSet = region.subregions(false);
    StringBuffer aStr = new StringBuffer();
    int numRegions = subregionSet.size();
    for (Region reg: subregionSet) {
      for (int i = 1; i <= level; i++) {
        aStr.append("  ");
      }
      aStr.append(reg.getFullPath() + "  (size " + reg.size() + " dataPolicy " +
          reg.getAttributes().getDataPolicy() + " eviction " + reg.getAttributes().getEvictionAttributes());
      RecoveryTestVersionHelper.logDiskStore(reg, aStr);
      aStr.append(")\n");
      Object[] tmp = subregionsToString(reg, level+1);
      aStr.append(tmp[0]);
      numRegions += (Integer)(tmp[1]);
    }
    return new Object[] {aStr.toString(), numRegions};
  }

  //=================================================
  // methods for concurrent recovery tests

  protected static void recoverLatestController() {
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // find all other vms and exclude one to be stopped last
    Object[] anArr = StopStartVMs.getOtherVMsDivided();
    List<ClientVmInfo> otherVMsExceptOne = (List)(anArr[0]);
    List<String> otherStopModesExceptOne = (List)(anArr[1]);
    ClientVmInfo remainingVM = (ClientVmInfo)(anArr[2]);
    String remainingStopMode = (String)(anArr[3]);
    List<ClientVmInfo> otherVMs = (List)(anArr[4]);

    // stop all vms except for one
    StopStartVMs.stopVMs(otherVMsExceptOne, otherStopModesExceptOne);

    // signal to the one remaining responder to write the snapshot to the blackboard
    // and wait for that to complete
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten", 
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // stop the last vm
    StopStartVMs.stopVM(remainingVM, remainingStopMode);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    // restart all vms; init tasks for all vms compare against the blackboard, so if
    // this returns then all is well
    StopStartVMs.startVMs(otherVMs);

    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " + 
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

  /** Do randon operations and wait for the controller to signal it's time to write
   *   a snapshot to the blackboard.
   */
  protected void concRecoverLatestResponder() {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    int numThreadsInThisVM = Integer.getInteger("numThreads");
    Log.getLogWriter().info("number of threads in this vm: " + numThreadsInThisVM);
    do {
      if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.writeSnapshot) > 0) {
        testInstance.readyForSnapshot.addAndGet(1); // wait for all threads in this vm to get to this point
        while (testInstance.readyForSnapshot.get() < numThreadsInThisVM) {
          Log.getLogWriter().info("Waiting for all threads in this vm to pause before writing snapshot, counter is " +
              testInstance.readyForSnapshot.get());
          MasterController.sleepForMs(1000);
        }
        Log.getLogWriter().info("All threads in this vm have paused");
        synchronized (RecoveryTestVersionHelper.class) { // only one thread in this vm writes the snapshot
          if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.snapshotWritten) > 0) {
            // already created snapshot; we are waiting for the controller to stop this vm
            MasterController.sleepForMs(2000);
            return; // in case we are being stopped with a nice_exit
          }
          // this is the single remaining vm; write state to the blackboard signal when done
          testInstance.writeSnapshot();
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
        }
      } else {
        try {
          testInstance.doOperations(5000);
        } catch (Exception e) {
          RecoveryTestVersionHelper.handleOpsException(e);
        }
      }
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }

  /** Run the pre-6.5 to 6.5 (or later) disk file converter. 
   * 
   *  @param oldVersionXmlFilePath The path of the pre-6.5 xml file.
   *  @param newVersionXmlFilePath The path of the 6.5 or later xml file. 
   * 
   */
  private static void convertPre65AndValidate(String oldVersionXmlFilePath,
      String newVersionXmlFilePath) {
    InstantiationHelper.setAllowInstantiation(false);
    List<File> oldVersionDiskFiles = getOldVersionDiskFiles(oldVersionXmlFilePath);
    listDiskFiles();
    String scriptLocation = RecoveryPrms.getScriptLocation();
    String exeName = "DiskFileConverter.sh";
    if (HostHelper.isWindows()) {
      exeName = "DiskFileConverter.bat";
    }
    String command = scriptLocation + File.separator + exeName + 
       " CLASSPATH=" + System.getProperty("JTESTS") +
       " OLD_GEMFIRE=" + getGemFireHome("oldVersion")  +
       " OLD_CACHE_XML=" + oldVersionXmlFilePath + 
       " GEMFIRE=" + getGemFireHome("newVersion") +
       " CACHE_XML=" + newVersionXmlFilePath;
    Log.getLogWriter().info("Executing " + command + "...");
    long startTime = System.currentTimeMillis();
    String result = ProcessMgr.fgexec(command, 0);
    long duration = System.currentTimeMillis() - startTime;
    Log.getLogWriter().info("Done executing " + command + ", result is " + result);
    Log.getLogWriter().info("Disk file conversion tool took " + duration + " millis");
    
    // look for bug 42650
    String gemFireVersion = GemFireVersion.getGemFireVersion();
    for (int i = 0; i < gemFireVersion.length(); i++) { // strip off Beta/Alpha etc
      char ch = gemFireVersion.charAt(i);
      if (!Character.isDigit(ch) && (ch != '.')) {
        gemFireVersion = gemFireVersion.substring(0, i);
        break;
      }
    }
    Log.getLogWriter().info("GemFire version is " + gemFireVersion);
    String[] lines = result.split("\n");
//xxx    for (String aLine: lines) {
//      if ((aLine.indexOf("Warning: Persistent region ") >= 0) &&
//          (aLine.indexOf("is missing") >= 0) &&
//          (aLine.indexOf(", and not converted.") >= 0)) { // is a warning line
//        // look for any bad output in warning line
//        if ((aLine.indexOf("/extra") >= 0) ||
//            (aLine.indexOf("/Region") >= 0) ||
//            (aLine.indexOf("Region/") >= 0) ||
//            (aLine.indexOf("is missing in " + gemFireVersion) >= 0)) {
//          throw new TestException("Unexpected warning output: " + aLine + "\nin converter output: " + result);
//        }
//      }
//    }
    
    // look for bad strings in output; some of the strings we are looking for might
    // be strange, but have been observed in visual inspection of the logs
    if ((result.indexOf("Exception") >= 0) ||
        (result.indexOf("find:") >= 0) || 
        (result.indexOf("rerun the tool") >= 0) ||
        (result.indexOf("Some errors happened")) >= 0) { 
      throw new TestException("Disk file converter contained unexpected output: " + result);
    }
    listDiskFiles(); // the disk files that exist after the convert
    verifyDiskFileConversion(oldVersionXmlFilePath, newVersionXmlFilePath, oldVersionDiskFiles);
    InstantiationHelper.setAllowInstantiation(true);
  }
  
  /** List the existing disk files.
   * 
   */
  private static void listDiskFiles() {
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    File[] dirContents = currDir.listFiles();
    StringBuffer resultStr = new StringBuffer();
    for (File aFile: dirContents) {
      if (aFile.isDirectory()) {
        if (aFile.getName().indexOf("disk_") > 0) {
          // aFile is a diskDirectory
          resultStr.append("Disk directory " + aFile.getAbsolutePath() + "\n");
          File[] diskFiles = aFile.listFiles();
          for (File diskFile: diskFiles) {
            resultStr.append("   " + diskFile.getName() + "\n");
          }
        }
      }
    }
    Log.getLogWriter().info("Existing disk files:\n" + resultStr);
  }

  /** Move files from oldVersion disk-dirs to newVersion disk-dirs
   *
   */
  private static void moveDiskFiles(List oldXmlFiles, List newXmlFiles) {
    for (int i = 0; i < oldXmlFiles.size(); i++) {
      String oldXmlFilename = (String)oldXmlFiles.get(i);
      String newXmlFilename = (String)newXmlFiles.get(i);
      int oldVersionVmId = getVmIdFromXmlFileName(oldXmlFilename);
      int newVersionVmId = getVmIdFromXmlFileName(newXmlFilename);
      Log.getLogWriter().info("Moving disk files from vm_" + oldVersionVmId + " to newVersion disk-dirs for vm_" + newVersionVmId);

      String currDirName = System.getProperty("user.dir");
      File currDir = new File(currDirName);
      List<String> oldDiskDirNames = getDiskDirsForVmId(currDir, oldVersionVmId);
      for (String oldDiskDirName: oldDiskDirNames) {
        String newDiskDirName = getMatchingNewDiskDir(currDir, oldDiskDirName, newVersionVmId);   // match vm_X*disk_Y
        if ((oldDiskDirName != null) && (newDiskDirName != null)) {
          File oldDiskDir = new File(oldDiskDirName);
          File newDiskDir = new File(newDiskDirName);
          File[] diskFiles = oldDiskDir.listFiles();
          for (File diskFile : diskFiles) {
            Log.getLogWriter().info("Working to move file:" + diskFile.getAbsolutePath());
            if (diskFile.exists()) {
              // Before we move the file we need to remove the destination file if it exists (windows #46231)
              File destFile = new File(newDiskDir, diskFile.getName());
              if (HostHelper.isWindows() && destFile.exists()) {
                destFile.delete();
              }
              // Attempt to move the file
              Log.getLogWriter().info("moving (renameTo) " + diskFile.getName() + " from " + oldDiskDir.getAbsolutePath() + " to " + newDiskDir.getAbsolutePath());
              boolean success = diskFile.renameTo(destFile);
              if (!success) {
                throw new TestException("moveDiskFiles failed to move file, " + diskFile.getName() + " from oldDiskDir (" + oldDiskDir.getAbsolutePath() + ") to newDiskDir (" + newDiskDir.getAbsolutePath());
              }
            }
          }
        }
      }
    }
  }

  /**
   *  getMatchingNewDiskDir
   */
  private static String getMatchingNewDiskDir(File dir, String oldDiskDir, int newVmId) {
    class DiskNumFileFilter implements FileFilter {
      String prefix;
      String diskStr;
      public DiskNumFileFilter(int vmId, String diskNum) {  // the newVmId + disk_XXX from oldDiskDir
        this.prefix = "vm_" + vmId + "_";
        this.diskStr = "_disk_" + diskNum;
      }
      public boolean accept(File fn) {
        String fname = fn.getName();
        if (fname.startsWith(prefix) && fname.endsWith(diskStr)) {
          return true;
        } 
        return false;
      }
    };
    // get the diskNum (XXX portion of disk_XXX)
    String fn = FileUtil.filenameFor(oldDiskDir);
    int searchIndex = fn.lastIndexOf('_');
    String diskNum = fn.substring(searchIndex+1);
    List<File> dirs = FileUtil.getFiles(dir, new DiskNumFileFilter(newVmId, diskNum), false);
    if (dirs.size() > 1) {
      throw new TestException("getMatchingNewDiskDir found more than one match for vm_" + newVmId + " and disk_" + diskNum);
    }
    File matchingDiskDir = dirs.get(0);
    return matchingDiskDir.getAbsolutePath();
  }

  /**
   *  getDiskDirsForVmId
   */
  private static List<String> getDiskDirsForVmId (File dir, int vmId) {
    class VmIdFileFilter implements FileFilter {
      String prefix;
      String diskStr;
      public VmIdFileFilter(int vmId) {
        this.prefix = "vm_" + vmId + "_";
        this.diskStr = "_disk_";
      }
      public boolean accept(File fn) {
        String fname = fn.getName();
        if ((fname.startsWith(prefix)) && (fname.indexOf(diskStr) > 0)) {
          return true;
        } 
        return false;
      }
    };
    List<String> diskDirList = new ArrayList();
    List dirs = FileUtil.getFiles(dir, new VmIdFileFilter(vmId), false);
    for (Iterator i = dirs.iterator(); i.hasNext();) {
       File diskFile = (File)i.next();
       diskDirList.add(diskFile.getAbsolutePath());
    }
    return diskDirList;
  }

  /**
   * @param oldVersionXmlFilePath
   * @return
   */
  private static List<File> getOldVersionDiskFiles(String oldVersionXmlFilePath) {
    List<File> oldFileNames = new ArrayList();
    List<List<String>> oldVersionDiskDirs = extractFromXmlFile(oldVersionXmlFilePath)[2];
    for (List<String> diskDirList: oldVersionDiskDirs) {
      for (String diskDir: diskDirList) {
        File aDir = new File(diskDir);
        oldFileNames.addAll(Arrays.asList(aDir.listFiles()));
      }
    }
    return oldFileNames;
  }

  /** Given the xml files used in a pre-6.5 to 6.5 disk file conversion
   *  use the offline validator to validate the new disk files and 
   *  verify the correct regions were converted.
   * @param oldVersionXmlFilePath File path to old version xml file used in conversion.
   * @param newVersionXmlFilePath File path to new version xml file used in conversion.
   * @param oldVersionDiskFiles A List of disk files present prior to converstion (ie
   *        the files to be converted); this is used for validation to ensure they
   *        are still present after conversion.
   */
  private static void verifyDiskFileConversion(String oldVersionXmlFilePath,
      String newVersionXmlFilePath, List<File> oldVersionDiskFiles) {
    List[] oldVersionXmlInfo = extractFromXmlFile(oldVersionXmlFilePath);
    List<String> oldVersionRegionNames = oldVersionXmlInfo[0];
    List[] newVersionXmlInfo = extractFromXmlFile(newVersionXmlFilePath);
    List<String> newVersionRegionNames = newVersionXmlInfo[0];
    List<String> newVersionDiskStoreNames = newVersionXmlInfo[1];
    List<List<String>> newVersionDiskDirs = newVersionXmlInfo[2];
    List<String> newVersionDataPolicy = newVersionXmlInfo[3];

    // use the offline validator to list regions in the converted files
    Set<String> uniqueDiskStoreNames = new HashSet(newVersionDiskStoreNames);
    for (String diskStoreName: uniqueDiskStoreNames) {
      if (diskStoreName == null) {
        // region did not have a diskStore, it's not persistent or overflowToDisk
        // so cannot run the validator
        continue;
      }
      // find 1) region names for persistent regions that use diskStoreName
      //         (regions that use overflow only are not included)
      //      2) all disk directories for regions that use diskStoreName
      Set<String> persistRegNamesForDiskStore = new HashSet();
      Set<String> diskDirsForDiskStore = new HashSet<String>();
      for (int i = 0; i < newVersionDiskStoreNames.size(); i++) {
        if ((newVersionDiskStoreNames.get(i) != null) && (newVersionDiskStoreNames.get(i).equals(diskStoreName))) {
          diskDirsForDiskStore.addAll(newVersionDiskDirs.get(i));
          if (newVersionDataPolicy.get(i) != null) {
            if (newVersionDataPolicy.get(i).indexOf("persistent") >= 0) {
              persistRegNamesForDiskStore.add(newVersionRegionNames.get(i));
            }
          }
        }
      }

      // the expected list of regions that were converted is the intersection
      // of regions in the old version xml and the new version xml for each 
      // diskStore PLUS any extra regions in the new world that are persistent
      Set<String> expectedRegionNames = new HashSet(oldVersionRegionNames);
      expectedRegionNames.retainAll(persistRegNamesForDiskStore);
      for (String regName: persistRegNamesForDiskStore) {
        if (regName.startsWith("/extra_")) {
          expectedRegionNames.add(regName);
        }
      }
      Log.getLogWriter().info("Expected region names for DiskStore " + diskStoreName +
          ": " + expectedRegionNames);

      List diskDirsAsFile = new ArrayList();
      for (String diskDirPath: diskDirsForDiskStore) {
        diskDirsAsFile.add(new File(diskDirPath));
      }
      File[] diskDirArr = new File[diskDirsAsFile.size()];
      diskDirsAsFile.toArray(diskDirArr);
      Object[] valResult = null;
      try {
        valResult = PersistenceUtil.runOfflineValidate(diskStoreName, diskDirArr);
      } catch (HydraRuntimeException e) {
        if (expectedRegionNames.size() == 0) { // there were no persistent regions in diskStoreName, so nothing to convert
          String errStr = e.getCause().toString();
          if ((errStr.indexOf("The init file") >= 0) &&
              (errStr.indexOf(" does not exist") > 0)) {
            // We know we are offline because we are running the offline validator
            // and we know there were no persistent regions in diskStoreName.
            // When the vm was shutdown, if it was a nice_exit the product deletes
            // the disk files for diskStoreName (nothing was persistent), so we
            // would get an error from the offline validator that the disk files
            // were not there; this is expected. But if the vm shutdown with a mean_kill
            // then the disk files would be there and the validator can run
            Log.getLogWriter().info("Got expected exception " + e + " with cause " +
                errStr + " due to no persistent regions in " +
                " DiskStore " + diskStoreName);
            valResult = new Object[] {"", new HashMap()};
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      }
      Map regionNameMap = (Map)(valResult[1]);
      Set<String> convertedRegionNames = regionNameMap.keySet();
      Set missingConvertedRegNames = new HashSet(expectedRegionNames);
      missingConvertedRegNames.removeAll(convertedRegionNames);
      Set extraConvertedRegNames = new HashSet(convertedRegionNames);
      extraConvertedRegNames.removeAll(expectedRegionNames);
      if (missingConvertedRegNames.size() != 0) {
        throw new TestException("After converting disk files, these regions were " +
            "missing from the converted files for DiskStore " + diskStoreName + ": " +
            missingConvertedRegNames + ", result from validator is " + valResult[0]);
      }
      // disk validator will return wan queues as if they were regions; this
      // is documented behavior from the validator, so do not consider
      // wan queues as an extra "region"
      Iterator it = extraConvertedRegNames.iterator();
      while (it.hasNext()) {
        String aStr = (String)it.next();
        if (aStr.endsWith("EVENT_QUEUE")) {
          extraConvertedRegNames.remove(aStr);
          it = extraConvertedRegNames.iterator(); // start iteration over
        }
      }
      if (extraConvertedRegNames.size() != 0) {
        throw new TestException("After converting disk files, these regions were " +
            "extra in the converted files for DiskStore " + diskStoreName + ": " +
            extraConvertedRegNames + ", result from validator is " + valResult[0]);
      }
      Log.getLogWriter().info("Converted disk files " + diskDirsForDiskStore + 
          " for DiskStore " + diskStoreName + " contained expected regions " +
          expectedRegionNames);
    }
    
    // make sure the old version disk files still exist and were not deleted by the tool
    for (File diskFile: oldVersionDiskFiles) {
      if (!diskFile.getName().endsWith(".olg") &&
          !diskFile.getName().endsWith("lk") &&
          !diskFile.getName().startsWith("OVERFLOW")) {
        //Log.getLogWriter().info("Checking that old disk file " + diskFile.getAbsolutePath() + " still exists");
        if (!diskFile.exists()) {
          throw new TestException("old disk file " + diskFile.getAbsolutePath() + " is missing after conversion");
        }
      } // else ignore .olg files; it is possible that conversion using the 
        // snapshot tool could have caused op log rolling followed by removal
        // of .olg files
        // ignore .lk files; vm could have been killed with a lock file open
    }
    Log.getLogWriter().info("Finished verifying disk file conversion");
  }

  /** Given an xml file, return the region names, DiskStore names, and disk directories
   *  for each region contained in it.
   *
   *  @param xmlFilePath The path to an xml file.
   *  @returns Parallel lists:
   *     List 1: (each element is a String) Each element is a region name.
   *     List 2: (each element is a String) Each element is a DiskStore name or null if
   *             no diskStores (because this is a GemFire version prior to DiskStores)
   *     List 3: (each element is a List of Strings) Each element is a List of disk
   *             directories.
   *     List 4: (each element is a String) Each element is the dataPolicy of the region.
   */
  private static List[] extractFromXmlFile(String xmlFilePath) {
    List<String> regionNameList = new ArrayList();
    List<String> diskStoreNameList = new ArrayList();
    List<List<String>> diskDirsList = new ArrayList();
    List<String> dataPolicyList = new ArrayList();
    File file = new File(xmlFilePath);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db;
    try {
      db = dbf.newDocumentBuilder();
      db.setEntityResolver(new CacheXmlParser());
      Document doc = db.parse(file);
      doc.getDocumentElement().normalize();
      NodeList baseNodeList = doc.getElementsByTagName("cache");
      for (int i = 0; i < baseNodeList.getLength(); i++) { // for each cache node (should be just one)
        Node cacheNode = baseNodeList.item(i);
        NodeList cacheNodeList = cacheNode.getChildNodes();
        for (int index2 = 0; index2 < cacheNodeList.getLength(); index2++) {
          Node regionNode = cacheNodeList.item(index2);
          if (regionNode.getNodeName().equals("region")) {
            List[] result = extractFromRegionNode("", regionNode);
            regionNameList.addAll(result[0]);
            diskStoreNameList.addAll(result[1]);
            diskDirsList.addAll(result[2]);
            dataPolicyList.addAll(result[3]);
          }
        }

        // make sure the return Lists are parallel; if any List element was not assigned for this region add null
        // regionNameList size should be >= the others
        if (regionNameList.size() > diskStoreNameList.size()) {
          diskStoreNameList.add(null);
        }
        if (regionNameList.size() > diskDirsList.size()) {
          diskDirsList.add(null);
        }
        if (regionNameList.size() > dataPolicyList.size()) {
          dataPolicyList.add(null);
        }
        if ((regionNameList.size() != diskStoreNameList.size()) ||
            (regionNameList.size() != diskDirsList.size()) ||
            (regionNameList.size() != dataPolicyList.size())) {
          throw new TestException("Test problem; expected equal sizes for parallel Lists, " +
              "regionNameList is size " + regionNameList.size() + " " +
              "diskStoreNameList is size " + diskStoreNameList.size() + " " +
              "diskDirsList is size " + diskDirsList.size() + " " +
              "dataPolicyList is size " + dataPolicyList.size());
        }
      }
    } catch (ParserConfigurationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SAXException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    return new List[] {regionNameList, diskStoreNameList, diskDirsList, dataPolicyList};
  }

  /** Given a Node that represents a region from a parsed xml file, return
   *  return the region names, DiskStore names, and disk directories
   *  for each region and subregion represented by the Node.
   *  
   * @param parentRegionName The name of the parent region, or null if a root region.
   * @param regionNode A Node know to be a region Node
   * @returns Parallel lists:
   *     List 1: (each element is a String) Each element is a region name.
   *     List 2: (each element is a String) Each element is a DiskStore name or null if
   *             no diskStores (because this is a GemFire version prior to DiskStores)
   *     List 3: (each element is a List of Strings) Each element is a List of disk
   *             directories.
   *     List 4: (each element is a String) Each element is the dataPolicy of the region.

   */
  private static List[] extractFromRegionNode(String parentRegionName, Node regionNode) {
    List<String> regionNameList = new ArrayList();
    List<String> diskStoreNameList = new ArrayList();
    List<List<String>> diskDirsList = new ArrayList();
    List<String> dataPolicyList = new ArrayList();

    NamedNodeMap baseNodeAttributes = regionNode.getAttributes();
    Node nameObj = baseNodeAttributes.getNamedItem("name");
    String regionName = nameObj.getNodeValue();
    regionName = parentRegionName + "/" + regionName;
    regionNameList.add(regionName);
    NodeList regionChildNodes = regionNode.getChildNodes();
    for (int index1 = 0; index1 < regionChildNodes.getLength(); index1++) { // for each child of the region node
      Node regionChild = regionChildNodes.item(index1);
      if (regionChild.getNodeName().equals("region-attributes")) { // node is region attributes
        NamedNodeMap regionNodeAttr = regionChild.getAttributes();
        Node diskStoreNode = regionNodeAttr.getNamedItem("disk-store-name");
        if (diskStoreNode == null) {
          diskStoreNameList.add(null);
        } else {
          diskStoreNameList.add(diskStoreNode.getNodeValue());
        }
        dataPolicyList.add(regionNodeAttr.getNamedItem("data-policy").getNodeValue());

        // drill down to get the disk directories
        List<String> diskDirsThisRegion = new ArrayList();
        NodeList attrNodeList = regionChild.getChildNodes();
        for (int index2 = 0; index2 < attrNodeList.getLength(); index2++) {
          Node attrNode = attrNodeList.item(index2);
          if (attrNode.getNodeName().equals("disk-dirs")) {
            NodeList diskDirsNodeList = attrNode.getChildNodes();
            for (int index3 = 0; index3 < diskDirsNodeList.getLength(); index3++) {
              Node diskDirsNode = diskDirsNodeList.item(index3);
              if (diskDirsNode.getNodeName().equals("disk-dir")) {
                NodeList diskDirNodeList = diskDirsNode.getChildNodes();
                for (int index4 = 0; index4 < diskDirNodeList.getLength(); index4++) {
                  Node diskDirNode = diskDirNodeList.item(index4);
                  diskDirsThisRegion.add(diskDirNode.getNodeValue()); // the disk directory
                }
              }
            }
          }
        }
        diskDirsList.add(diskDirsThisRegion);
      } else if (regionChild.getNodeName().equals("region")) {
        List[] result = extractFromRegionNode(regionName, regionChild);
        regionNameList.addAll(result[0]);
        diskStoreNameList.addAll(result[1]);
        diskDirsList.addAll(result[2]);
        dataPolicyList.addAll(result[3]);
      }
    }
    //Log.getLogWriter().info("Returning regionNameList(size " + regionNameList.size() + ") " + regionNameList + " " +
        //" diskStoreNameList(size " + diskStoreNameList.size() + ") " + diskStoreNameList + " " +
        //" diskDirsList(size " + diskDirsList.size() + ") " + diskDirsList + " " +
        //" dataPolicyList(size " + dataPolicyList.size() + ") " + dataPolicyList);
    return new List[] {regionNameList, diskStoreNameList, diskDirsList, dataPolicyList};
  }

  /** Given a String which is contained in hydra.VersionPrms-names, return
   *  a path (String) to the gemfire home (product home).
   * @param versionName A name from hydra.VersionPrms-names
   * @return A path to the GemFire product home directory.
   */
  private static String getGemFireHome(String versionName) {
    HydraVector versionNamesVec = TestConfig.tab().vecAt(VersionPrms.names);
    HydraVector versionHomeVec = TestConfig.tab().vecAt(VersionPrms.gemfireHome);
    int index = versionNamesVec.indexOf(versionName);
    if (index < 0) {
      throw new TestException("Could not find " + versionName + " in VersionPrms.names");
    }
    String gemfireHome = (String)(versionHomeVec.get(index));
    if (gemfireHome.equals("default")) {
      return System.getProperty("gemfire.home");
    } else {
      return gemfireHome;
    }
  }

  /** For existing tests, maintain the behavior of only writing persistent regions to the BB.
   *  For snapshot testing, all regions must be recorded on the BB.
   *
   */
  Map writeSnapshot() {
    return writeSnapshot(false);  // don't include non-persistent regions
  }

  Map writeSnapshot(boolean includeNonPersistentRegions) {
    Log.getLogWriter().info("Preparing to write snapshot for " + allRegions.size() + " regions");
    Map allRegionsSnapshot = new HashMap();
    for (Region aRegion: allRegions) {
      Map regionSnapshot = new HashMap();
      if (includeNonPersistentRegions || RecoveryTestVersionHelper.withPersistence(aRegion)) { // only persistent regions will have values when restarted
        for (Object key: aRegion.keySet()) {
          Object value = null;
          if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
            value = aRegion.get(key);
          }
          if (value instanceof BaseValueHolder) {
            regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
          } else if (RecoveryTestVersionHelper.instanceOfPdxInstance(value)) {
            BaseValueHolder vh = RecoveryTestVersionHelper.toValueHolder(value);
            regionSnapshot.put(key, vh.myValue);
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
    Log.getLogWriter().info("Put snapshot for " + allRegions.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
    
    return allRegionsSnapshot;
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm. 
   * 
   */
  protected void verifyFromSnapshot() {
    Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
    if (allRegionsSnapshot == null) { // recoverFromGii test relies on this being null sometimes
      Log.getLogWriter().info("Not verifying from snapshot because the snapshot in the blackboard is null");
      return;
    }
    Set snapshotRegionNames = allRegionsSnapshot.keySet();
    Set<String> definedRegionNames = new HashSet();
    for (Region aRegion: theCache.rootRegions()) {
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
      ((LocalRegion)aRegion).dumpBackingMap();
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
          } else if (RecoveryTestVersionHelper.instanceOfPdxInstance(actualValue)) {
            actualValue = RecoveryTestVersionHelper.toValueHolder(actualValue);
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else {
            if ((actualValue instanceof byte[]) &&
                (expectedValue instanceof byte[])) {
              byte[] actual = (byte[])actualValue;
              byte[] expected = (byte[])expectedValue;
              if (actual.length != expected.length) {
                throw new TestException("Expected value for key " + key + " to be " +
                    TestHelper.toString(expectedValue) + ", but it is " +
                    TestHelper.toString(actualValue));
              }
            } else {
              throw new TestException("Expected value for key " + key + " to be " +
                  TestHelper.toString(expectedValue) + ", but it is " +
                  TestHelper.toString(actualValue));
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

  /** Stop all vms and restart. Init tasks verify they all have the same data. 
   * 
   */
  private  static void concRecoverAllController() {
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    List<ClientVmInfo> stopVMs = new ArrayList();
    if (RecoveryPrms.getUseShutDownAll()) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.shutDownAllKey, true); // mark that shutdown all is occurring
      stopVMs = (List)(StopStartVMs.shutDownAllMembers(adminDS))[0];
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.shutDownAllKey, false); // mark that shutdown all is done
    } else {
      List vmIDs = ClientVmMgr.getOtherClientVmids();
      List<String> stopModesList = new ArrayList();
      for (Object vmID: vmIDs) {
        stopVMs.add(new ClientVmInfo((Integer)vmID));
        stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
      StopStartVMs.stopVMs(stopVMs, stopModesList);
    }

    // restart all vms; init tasks for all vms compare against the blackboard, so if
    // this returns then all is well
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    StopStartVMs.startVMs(stopVMs);

    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " + 
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

  /** Verify all regions from the snapshot after choosing 1 vm to import the snapshot
   * 
   */
  public static void HydraTask_importAndVerify() {
    RecoveryTestVersionHelper.importAndVerify(testInstance);
  }

  /**
   * 
   */
  private void verifyFromLeaderSnapshot() {
    long counter = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.writeSnapshot);
    if (counter == 1) {
      writeSnapshot();
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
    } else {
      TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten", 
          RecoveryBB.snapshotWritten, 1, true, -1, 2000);
      verifyFromSnapshot();
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
    }
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter", 
        RecoveryBB.doneVerifyingCounter,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads()-1,
        true, -1, 2000);
  }

  /** Do random operations on the cache and any defined regions in this vm.
   * 
   *   @param msToRun The number of milliseconds to run operations before returning.
   */
  protected void doOperations(long msToRun) throws Exception {
    Log.getLogWriter().info("In doOperations, running for " + msToRun + " seconds");
    long startTime = System.currentTimeMillis();
    int numOps = 0;
    do {
      Region aRegion = null;
      try {
        aRegion = allRegions.get(TestConfig.tab().getRandGen().nextInt(0, allRegions.size()-1));
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
          getExistingKey(aRegion);
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
        case ENTRY_PUTALL_OPERATION:
          putAll(aRegion);
          break;
        case CACHE_OPERATIONS:
          cacheRegionOperations();
          break;
        case PUTS_FOR_KEY_ORDER_POLICY:
          putsForKeyOrderPolicy(aRegion);
          break;
        case CLEAR_REGION:
          aRegion = clearRegion();  // clearRegion returns the non-partitioned region it cleared
          break;
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
        }
        numOps++;
        Log.getLogWriter().info("Completed op " + numOps + " for this task");
      } catch (EntryExistsException e) {
        if (uniqueKeys) {
           throw new TestException(TestHelper.getStackTrace(e));
        } else {
           Log.getLogWriter().info("Caught " + e + ", expected with multiple writers (uniqueKeys==false), continuing test");
        }
      } catch (EntryNotFoundException e) {
        if (uniqueKeys) {
          boolean allowENFE = false;
          // if datahosts are being recycled, we could lose the only copy of the data
          long stoppingVMs = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.stoppingVMs);
          if (stoppingVMs > 0) {
            allowENFE = true;
            Log.getLogWriter().info("Caught " + e + ", expected with HA, continuing test");
          } else {
            // This could happen with concurrent execution and Region.clear()
            long clearRegion = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.clears);
            if (clearRegion > 0 && isClearedRegion(aRegion)) {
                allowENFE = true;
                Log.getLogWriter().info("Caught " + e + ", due to concurrent clear on " + aRegion.getFullPath() + ", continuing test");
            }
          }
          if (!allowENFE) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        } else {
           Log.getLogWriter().info("Caught " + e + ", expected with multiple writers (uniqueKeys==false), continuing test");
        }
      } catch (Exception e) {
        boolean undergoingShutDownAll = (Boolean)RecoveryBB.getBB().getSharedMap().get(RecoveryBB.shutDownAllKey);
        if (undergoingShutDownAll) { // shutDownAll disconnects us from the ds so ops could get exceptions
          RecoveryTestVersionHelper.handleOpsExceptionDuringShutDownAll(e);
        } else {
          throw e;
        }
      }
    } while ((System.currentTimeMillis() - startTime < msToRun));
    Log.getLogWriter().info("Done in doOperations");
    Log.getLogWriter().info(regionHierarchyToString());
  }

  /** Get an operation to perform on the region
   *  @param reg The region to get an operation for. 
   *  @returns A random operation.
   */
  protected int getOperation(Region aRegion) {
    Long opsPrm = RecoveryPrms.operations;
    Long upperThresholdOpsPrm = RecoveryPrms.upperThresholdOperations;
    Long lowerThresholdOpsPrm = RecoveryPrms.lowerThresholdOperations;
    int upperThreshold = RecoveryPrms.getUpperThreshold();
    int lowerThreshold = RecoveryPrms.getLowerThreshold();
    if (isBridgeClient) {
      opsPrm = RecoveryPrms.clientOperations; 
      upperThresholdOpsPrm = RecoveryPrms.upperThresholdClientOperations; 
      lowerThresholdOpsPrm = RecoveryPrms.lowerThresholdClientOperations; 
      upperThreshold = RecoveryPrms.getUpperThresholdClient();
      lowerThreshold = RecoveryPrms.getLowerThresholdClient();
    }
    int size = aRegion.size();
    if (size >= upperThreshold) {
      return getOperation(upperThresholdOpsPrm);
    } else if (size <= lowerThreshold) {
      return getOperation(lowerThresholdOpsPrm);
    } else {
      return getOperation(opsPrm);

    }
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
    } else if (operation.equals("putAll")) {
      op = ENTRY_PUTALL_OPERATION;
    } else if (operation.equals("cacheOperations")) {
      op = CACHE_OPERATIONS;
    } else if (operation.equals("putsForKeyOrderPolicy")) {
      op = PUTS_FOR_KEY_ORDER_POLICY;
    } else if (operation.equals("clear")) {
      op = CLEAR_REGION;
    } else {
      throw new TestException("Unknown entry operation: " + operation);
    }
    return op;
  }

  /** Does a get operation on an existing key.
   *
   *  @param aRegion The region to perform the get on.
   *
   */
  protected void getExistingKey(Region aRegion) {
    Object key = obtainExistingKey(aRegion);
    if (key == null) {
      Log.getLogWriter().info("No keys available in " + aRegion.getFullPath() + ", cannot do get on existing key");
      return;
    }
    Log.getLogWriter().info("Getting existing key " + key + " from region " + aRegion.getFullPath());
    Object result = aRegion.get(key);
    Log.getLogWriter().info("Done getting existing key " + key + ", returned value " + result);
  }

  /** Add a new entry to the given region.
   *
   *  @param aRegion The region to use for adding a new entry.
   *
   *  @returns The key that was added.
   */
  protected Object addEntry(Region aRegion) {
    Object key = obtainNewKey();
    addEntry(aRegion, key);
    return key;
  }

  /** Add a new entry to the given region.
   *
   *  @param aRegion The region to use for adding a new entry.
   *  @param key The key to add.
   */
  protected void addEntry(Region aRegion, Object key) {
    Object anObj = getValueForKey(key);
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
      Object returnVal = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
        Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
            TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
        returnVal = aRegion.put(key, anObj, callback);
        Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
      } else {
        Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
            TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
        returnVal = aRegion.put(key, anObj);
        Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
      }
    }
  }

  /** putall a map to the given region.
   *
   *  @param aRegion The region to use for putall a map.
   *
   */
  protected void putAll(Region aRegion) {
    // determine the number of new keys to put in the putAll
    Map mapToPut = getPutAllMap(aRegion);

    // do the putAll
    Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries, region is " + aRegion.getFullPath());
    aRegion.putAll(mapToPut);
    Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");
  }

  /** Get a map to use for putAll for the given region. Make sure the values in the
   *   map  honor unique keys.
   *
   * @param aRegion The region to consider for the putAll.
   * @return Map The map to use for the argument to putAll
   */
  protected Map getPutAllMap(Region aRegion) {
    // determine the number of new keys to put in the putAll
    int numPutAllNewKeys = RecoveryPrms.getNumPutAllNewKeys();

    // get a map to use with putAll; add new keys to it
    Map mapToPut = new HashMap();
    StringBuffer newKeys = new StringBuffer();
    for (int i = 1; i <= numPutAllNewKeys; i++) { // put new keys
      Object key = obtainNewKey();
      Object anObj = getValueForKey(key);
      mapToPut.put(key, anObj);
      newKeys.append(key + " ");
      if ((i % 10) == 0) {
        newKeys.append("\n");
      }
    }
    // add existing keys to the map
    int numPutAllExistingKeys = TestConfig.tab().intAt(RecoveryPrms.numPutAllExistingKeys);
    List<String> existingKeysList = obtainExistingKeys(aRegion, numPutAllExistingKeys);
    StringBuffer existingKeys = new StringBuffer();
    int count = 0;
    for (String key: existingKeysList) { // put existing keys
      Object anObj = getUpdateObject(aRegion, key);
      mapToPut.put(key, anObj);
      existingKeys.append(key + " ");
      count++;
      if ((count % 10) == 0) {
        existingKeys.append("\n");
      }
    }
    Log.getLogWriter().info(aRegion.getFullPath() + " size is " + aRegion.size() + ", map to use as argument to putAll is " +
        mapToPut.getClass().getName() + " containing " + numPutAllNewKeys + " new keys and " +
        numPutAllExistingKeys+ " existing keys (updates); total map size is " + mapToPut.size() +
        "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
    return mapToPut;
  }

  /** Invalidate an entry in the given region.
   *
   *  @param aRegion The region to use for invalidating an entry.
   *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
   */
  protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
    Object key = obtainExistingKey(aRegion);
    if (key == null) {
      Log.getLogWriter().info("invalidateEntry: No keys in region " + aRegion.getFullPath());
      return;
    }
    invalidateEntry(aRegion, key, isLocalInvalidate);
  }

  /** Invalidate an entry in the given region.
   *
   *  @param aRegion The region to use for invalidating an entry.
   *  @param key The key to invalidate.
   *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
   */
  protected void invalidateEntry(Region aRegion, Object key, boolean isLocalInvalidate) {
    String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
    if (isLocalInvalidate) { // do a local invalidate
      if (TestConfig.tab().getRandGen().nextBoolean()) { // local invalidate with callback
        Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback
            + ", region is " + aRegion.getFullPath());
        aRegion.localInvalidate(key, callback);
        Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
      } else { // local invalidate without callback
        Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + ", region is " + aRegion.getFullPath());
        aRegion.localInvalidate(key);
        Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
      }
    } else { // do a distributed invalidate
      if (TestConfig.tab().getRandGen().nextBoolean()) { // invalidate with callback
        Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback
            + ", region is " + aRegion.getFullPath());
        aRegion.invalidate(key, callback);
        Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
      } else { // invalidate without callback
        Log.getLogWriter().info("invalidateEntry: invalidating key " + key + ", aRegion is " + aRegion.getFullPath());
        aRegion.invalidate(key);
        Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
      }
    }
  }

  /** Destroy an entry in the given region.
   *
   *  @param aRegion The region to use for destroying an entry.
   *  @param isLocalDestroy True if the destroy should be local, false otherwise.
   */
  protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
    Object key = obtainExistingKey(aRegion);
    if (key == null) {
      Log.getLogWriter().info("destroyEntry: No keys in region " + aRegion.getFullPath());
      return;
    }
    destroyEntry(aRegion, key, isLocalDestroy);
  }

  /** Destroy an entry in the given region.
   *
   *  @param aRegion The region to use for destroying an entry.
   *  @param key The key to destroy.
   *  @param isLocalDestroy True if the destroy should be local, false otherwise.
   */
  protected void destroyEntry(Region aRegion, Object key, boolean isLocalDestroy) {
    String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
    if (isLocalDestroy) { // do a local destroy
      if (TestConfig.tab().getRandGen().nextBoolean()) { // local destroy with callback
        Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback
            + ", region is " + aRegion.getFullPath());
        aRegion.localDestroy(key, callback);
        Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
      } else { // local destroy without callback
        Log.getLogWriter().info("destroyEntry: local destroy for " + key + ", region is " + aRegion.getFullPath());
        aRegion.localDestroy(key);
        Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
      }
    } else { // do a distributed destroy
      if (TestConfig.tab().getRandGen().nextBoolean()) { // destroy with callback
        Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback
            + ", region is " + aRegion.getFullPath());
        aRegion.destroy(key, callback);
        Log.getLogWriter().info("destroyEntry: done destroying key " + key);
      } else { // destroy without callback
        Log.getLogWriter().info("destroyEntry: destroying key " + key + ", region is " + aRegion.getFullPath());
        aRegion.destroy(key);
        Log.getLogWriter().info("destroyEntry: done destroying key " + key);
      }
    }
  }

  /** Update an existing entry in the given region. If there are
   *  no available keys in the region, then this is a noop.
   *
   *  @param aRegion The region to use for updating an entry.
   */
  protected void updateEntry(Region aRegion) {
    Object key = obtainExistingKey(aRegion);
    if (key == null) {
      Log.getLogWriter().info("updateEntry: No keys in region " + aRegion.getFullPath());
      return;
    }
    updateEntry(aRegion, key);
  }

  /** Update an existing entry in the given region. If there are
   *  no available keys in the region, then this is a noop.
   *
   *  @param aRegion The region to use for updating an entry.
   *  @param key The key to update.
   */
  protected void updateEntry(Region aRegion, Object key) {
    Object anObj = getUpdateObject(aRegion, (String)key);
    String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
    Object returnVal = null;
    if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
          TestHelper.toString(anObj) + ", callback is " + callback
          + ", region is " + aRegion.getFullPath());
      returnVal = aRegion.put(key, anObj, callback);
      Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
    } else { // do a put without callback
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj)
          + ", region is " + aRegion.getFullPath());
      returnVal = aRegion.put(key, anObj, false);
      Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
    }   

  }

  /** Get a new key int the given region.
   *
   *  @param aRegion The region to use for getting an entry.
   */
  protected void getNewKey(Region aRegion) {
    Object key = obtainNewKey();
    getThisNewKey(aRegion, key);
  }

  /** Do a get on the given new key.
   *
   *  @param aRegion The region to use for getting an entry.
   *  @param key A new key never used before.
   */
  protected void getThisNewKey(Region aRegion, Object key) {
    String callback = getCallbackPrefix + ProcessMgr.getProcessId();
    Object anObj;
    if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback
          + ", region is " + aRegion.getFullPath());
      anObj = aRegion.get(key, callback);
    } else { // get without callback
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", region is " + aRegion.getFullPath());
      anObj = aRegion.get(key);
    }
    Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
  }

  /** Return a random key from aRegion
   *
   * @param aRegion The region to get the existing key from.
   * @return A random key from aRegion.
   */
  protected Object obtainExistingKey(Region aRegion) {
    if (aRegion.size() == 0) {
      return null;
    }
    Object[] keyArr = aRegion.keySet().toArray();
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    int numThreads = TestHelper.getNumThreads();
    for (Object key: keyArr) {
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

  /** Return a random key from aRegion
   * 
   * @param aRegion The region to get the existing key from. 
   * @return A List of existing keys.
   */
  protected List<String> obtainExistingKeys(Region aRegion, int numToObtain) {
    List aList = new ArrayList();
    if (aRegion.size() == 0) {
      return aList;
    }
    Object[] keyArr = aRegion.keySet().toArray();
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    int numThreads = TestHelper.getNumThreads();
    for (Object key: keyArr) {
      if (uniqueKeys) {
        long keyIndex = NameFactory.getCounterForName(key);
        if ((keyIndex % numThreads) == myTid) {
          aList.add(key);
          if (aList.size() == numToObtain) {
            return aList;
          }
        }
      } else {
        aList.add(key);
        if (aList.size() == numToObtain) {
          return aList;
        }
      }
    }
    return aList;
  }

  /** Method to exercise Cache.rootRegions and Cache.getRegion(String).
   */
  protected void cacheRegionOperations() {
    Log.getLogWriter().info("Getting rootRegions");
    Set<Region<?,?>> rootRegions = theCache.rootRegions();

    Log.getLogWriter().info("Getting subregions");
    Set<Region> all = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      all.add(aRegion);
      all.addAll(aRegion.subregions(true));
    }   

    Log.getLogWriter().info("Getting individual regions from Cache");
    for (Region aRegion: all) {
      String fullName = aRegion.getFullPath();
      theCache.getRegion(fullName);
    }
  }

  /** Puts ValueHolders with sequentially increasing modVal into the cache 
   *  Does 1000 sequential updates on a single entry in the given region.
   *  These values continually increase (each threads posts its current opNum 
   *  in the BB with a key based on vmId and threadId) for an entry unique to 
   *  this thread (every invocation of this method by a single thread works on the 
   *  same unique entry).
   */
  public static void putsForKeyOrderPolicy(Region aRegion) {
    Blackboard bb = RecoveryBB.getBB();
    int vmId = RemoteTestModule.getMyVmid();
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    String mapKey = "vm_" + vmId + "_thr_" + tid;
    if (bb.getSharedMap().get(mapKey) == null) {  // initialize to 0 on first access only
       bb.getSharedMap().put(mapKey, 0);
    }
    int opNum = ((Integer)(bb.getSharedMap().get(mapKey))).intValue();

    int myUniqueKey = (Integer)(bb.getSharedMap().get(uniqueKeyIndex + tid));
    Log.getLogWriter().info("Updating sequential values for " + myUniqueKey);
    Object key = NameFactory.getObjectNameForCounter(myUniqueKey);
    for (int i = 1; i <= 1000; i++) {
      ValueHolder value = new ValueHolder((String)key, testInstance.randomValues, new Integer(opNum));
      aRegion.put(key, value, mapKey);
      opNum++;
    }
    bb.getSharedMap().put(mapKey, new Integer(opNum));
    Log.getLogWriter().info("Done updating sequential values for " + myUniqueKey);
  }

  /** Invoke region.clear() - only one region is cleared during any test run.
   *  Note that Partitioned Regions do not support clear
   */
  protected Region clearRegion() {
    Object[] regionInfo = getNonPartitionedRegionInfo();
    Region aRegion = (Region)regionInfo[0];
    int numNonPartitionedRegions = ((Integer)regionInfo[1]).intValue();
    long numClears = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.clears);
    if (numClears < numNonPartitionedRegions/10) {  // only clear 10% of the non-partitioned regions
      Log.getLogWriter().info("numClears = " + numClears + " of " + numNonPartitionedRegions/10 + " permitted by this test");
      if (aRegion != null) {
        String fullPathName = aRegion.getFullPath();
        RecoveryBB.getBB().addToClearedRegionList(fullPathName);
        Log.getLogWriter().info("Starting clearRegion on " + fullPathName);
        aRegion.clear();
        Log.getLogWriter().info("Completed clearRegion on " + fullPathName);
      } else {
        Log.getLogWriter().info("clearRegion invoked, but there are no non-PartitionedRegions in this test");
      }
    }
    return aRegion;
  }

  /**  Return an array of nonPartitionedRegionInfo where
   *     Object[0] = selected region (null if there are no non-partitioned regions in the test)
   *     Object[1] = total number of non-partitioned regions.
   *   (Used by clearRegion since clear is unsupported by PartitionedRegions).
   */
  protected Object[] getNonPartitionedRegionInfo() {
    ArrayList nonPartitionedRegions = new ArrayList();
    for (Region aRegion: allRegions) {
      if (!aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        nonPartitionedRegions.add(aRegion);
      }
    }    

    Region aRegion = null;
    int size = nonPartitionedRegions.size();
    if (size > 0) {
      aRegion = (Region)nonPartitionedRegions.get(TestConfig.tab().getRandGen().nextInt(0, size-1));
      Log.getLogWriter().info("getNonPartitionedRegionInfo returning target Region " + aRegion.getFullPath() + " from list of regions of size " + size);
    }
    Object[] info = { aRegion, new Integer(size) };
    return info;
  }

  /** Whether or not the given Region was the target of a Region.clear().
   *
   */
  protected boolean isClearedRegion(Region aRegion) {
    boolean cleared = false;
    List regionList = (List)RecoveryBB.getBB().getSharedMap().get(RecoveryBB.clearedRegionList);
    if (regionList != null) {
      if (regionList.contains(aRegion.getFullPath())) {
        cleared = true;
      }
    } 
    return cleared;
  }

  //=================================================
  // other methods

  /** Find restore scripts for any online backups that were done during
   *  the run and execute the scripts. 
   *  
   * @param backupDir The directory containing a backup.
   * @param expectSuccess If true then the restore scripts should work 
   *        successfully, if false then they should fail because there
   *        are already disk files present. 
   */
  protected static void runRestoreScript(File backupDir,
      boolean expectSuccess) {
    // restore script is (for example) /backup_1/2010-10-14-11-10/bilbo_21339_v1_16273_51816/restore.sh
    // where backup_1 is the argument backupDir
    File[] backupContents = backupDir.listFiles();
    if (backupContents.length != 1) {
      throw new TestException("Expecting backup directory to contain 1 directory, but it contains " + backupContents.length);
    }
    File dateDir = backupContents[0];
    File[] dateDirContents = dateDir.listFiles();
    for (File hostAndPidDir: dateDirContents) {
      File[] hostAndPidContents = hostAndPidDir.listFiles();
      for (File aFile: hostAndPidContents) {
        if (aFile.getName().equals("restore.sh") || aFile.getName().equals("restore.bat")) { // run the restore script
          try {
            String cmd = null;
            if (HostHelper.isWindows()) {
              cmd = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
            } else {
              cmd = "/bin/bash ";
            }
            cmd = cmd + aFile.getCanonicalPath();
            try {
              Log.getLogWriter().info("Running restore scripts");
              String cmdResult = ProcessMgr.fgexec(cmd, 0);
              Log.getLogWriter().info("Result is " + cmdResult);
              if (expectSuccess) {
                Log.getLogWriter().info("Restore script executed successfully");
              } else {
                throw new TestException("Expected restore script to fail, but it succeeded");
              }
            } catch (HydraRuntimeException e) {
              if (expectSuccess) {
                throw e;
              } else {
                String errStr = e.getCause().toString();
                if (errStr.indexOf("Backup not restored. Refusing to overwrite") >= 0) {
                  Log.getLogWriter().info("restore script got expected exception " + e +
                      " " + e.getCause());
                } else {
                  throw e;
                }
              }
            }
          } catch (IOException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
          break; // ran the restore script in this directory
        }
      }
    }
  }

  /** 
   * Recursively delete a directory and its contents
   * @param aDir The directory to delete. 
   */
  protected static void deleteDir(File aDir) {
    try {
      File[] contents = aDir.listFiles();
      for (File aFile: contents) {
        if (aFile.isDirectory()) {
          deleteDir(aFile);
        } else {
          if (!aFile.delete()) {
            throw new TestException("Could not delete " + aFile.getCanonicalPath());
          }
        }
      }
      if (aDir.delete()) {
        Log.getLogWriter().info("Successfully deleted " + aDir.getCanonicalPath());
      } else {
        throw new TestException("Could not delete " + aDir.getCanonicalPath());
      }
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** 
   * Delete existing disk directories if they contain disk files.
   */
  protected static void deleteExistingDiskDirs() {
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    File[] contents = currDir.listFiles();
    for (File aDir: contents) {
      if (aDir.isDirectory() && (aDir.getName().indexOf("_disk_") >= 0)) {
        // this is the check for an empty directory; if it's empty don't
        // delete it. Essentially, this does not delete disk directories
        // that were created based on an endTask vmID. When hydra utilities
        // are called to create a diskStore, it automatically generates disk
        // directories based on the current vmID. In an endTask, this VM ID
        // is different that the vmIDs used by the regular tasks. So hydra
        // creates these directories because the product requires the 
        // directories to exist when setDiskDirs is called on the DiskStoreFactory.
        // So, in order to get passed this check, we leave the endTask disk
        // directories there, even though they are empty and never used to
        // hold disk files
        if (aDir.list().length != 0) {
          deleteDir(aDir);
        }
      }
    }
  }  
  
  /** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
   *  which is equivalent to a full GII.
   */
  protected static void registerInterest() {
    Set<Region<?,?>> rootRegions = theCache.rootRegions();
    for (Region aRegion: rootRegions) {
      Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES for region " + 
          aRegion.getFullPath());
      aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      Log.getLogWriter().info("Done calling registerInterest for all keys, " +
          "result interest policy KEYS_VALUES, " + aRegion.getFullPath() + 
          " size is " + aRegion.size());
    }
  }

  /** Log the execution number of this serial task.
   */
  static protected long  logExecutionNumber() {
    long exeNum = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.executionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    return exeNum;
  }

  /** Returns a List of ClientVmInfos that have the given marker in the blackboard.
   * @return A List of ClientVmInfo's 
   */
  private static List<ClientVmInfo> getMarkedVMs(String targetMarkerValue) {
    Map aMap = RecoveryBB.getBB().getSharedMap().getMap();
    List<ClientVmInfo> aList = new ArrayList();
    for (Object objKey: aMap.keySet()) {
      if (objKey instanceof String) {
        String key = (String)objKey;
        if (key.startsWith(vmMarkerKey)) {
          String markerValue = (String)(aMap.get(key));
          if (markerValue.equals(targetMarkerValue)) {
            Integer vmID = Integer.valueOf(key.substring(vmMarkerKey.length(), key.length()));
            ClientVmInfo info = new ClientVmInfo(vmID);
            aList.add(info);
          }
        }
      }
    }
    return aList;
  }

  /** Mark in the blackboard whether the given vm should obtain data from disk or form a remote vm (gii)
   * @param aVM
   */
  private static void markDataSource(ClientVmInfo aVM, boolean expectDiskRecovery) {
    if (expectDiskRecovery) {
      Log.getLogWriter().info("Marking in the blackboard that vm with ID " + aVM.getVmid() +
      " expects disk recovery");
    } else {
      Log.getLogWriter().info("Marking in the blackboard that vm with ID " + aVM.getVmid() +
      " expects to obtain data from a remote peer (gii)");
    }
    RecoveryBB.getBB().getSharedMap().put(expectDiskRecoveryKey + aVM.getVmid(),
        new Boolean(expectDiskRecovery));
  }

  /** Mark in the blackboard whether the given vms should obtain data from disk or from a remote vm (gii)
   * @param aList A List of ClientVMInfo's
   * @param expectDiskRecovery True if the vms in aList expect disk recovery, false if they expect
   *                 to obtain data from a remote vm (gii)
   */
  private static void markDataSource(List<ClientVmInfo> aList, boolean expectDiskRecovery) {
    for (ClientVmInfo info: aList) {
      markDataSource(info, expectDiskRecovery);
    }
  }

  /** Check if we have run for the desired length of time. We cannot use 
   *  hydra's taskTimeSec parameter because of a small window of opportunity 
   *  for the test to hang due to the test's "concurrent round robin" type 
   *  of strategy. Here we set a blackboard counter if time is up and this
   *  is the last concurrent round.
   */
  protected static void checkForLastIteration() {
    // determine if this is the last iteration
    int secondsToRun = RecoveryPrms.getSecondsToRun();
    long taskStartTime = 0;
    final String bbKey = "taskStartTime";
    Object anObj = RecoveryBB.getBB().getSharedMap().get(bbKey);
    if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      RecoveryBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
    } else {
      taskStartTime = ((Long)anObj).longValue();
    }
    if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.timeToStop);
    } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
          (secondsToRun - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
    }
  }

  /** Return a new key, never before used in the test.
   */
  protected Object obtainNewKey() {
    if (uniqueKeys) {
      int thisThreadID = RemoteTestModule.getCurrentThread().getThreadId();
      int anInt = (Integer)(RecoveryBB.getBB().getSharedMap().get(uniqueKeyIndex + thisThreadID));
      anInt += TestHelper.getNumThreads();
      RecoveryBB.getBB().getSharedMap().put(uniqueKeyIndex + thisThreadID, anInt);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, anInt);
      return NameFactory.getObjectNameForCounter(anInt);
    } else {
      return NameFactory.getNextPositiveObjectName();
    }
  }

  /** Randomly choose between a ValueHolder and the object that is
   *  the extra value in the ValueHolder so the tests can put either
   *  ValueHolders or something else (such as byte[])
   */
  private Object getValueForKey(Object key) {
    String valueClassName = RecoveryPrms.getValueClassName();
    BaseValueHolder vh = null;
    if (valueClassName.equals(ValueHolder.class.getName())) {
      vh = new ValueHolder((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializable.class.getName())) {
      vh = new VHDataSerializable((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializableInstantiator.class.getName())) {
      vh = new VHDataSerializableInstantiator((String)key, randomValues);
    } else if ((valueClassName.equals("util.PdxVersionedValueHolder")) ||
               (valueClassName.equals("util.VersionedValueHolder"))) {
      vh = RecoveryTestVersionHelper.getVersionedValueHolder(valueClassName, (String)key, randomValues);
      
      return vh; // do not allow this method the option of returning the extraObject (below)
    } else {
      throw new TestException("Test does not support RecoveryPrms.valueClassName " + valueClassName);
    }
    if (TestConfig.tab().getRandGen().nextBoolean()) {
      Object anObj = vh.extraObject;
      if (anObj == null) {
        throw new TestException("Test config problem: ValueHolder does not have extraObj");
      }
      return anObj;
    } else {
      return vh;
    }
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
  protected Object getUpdateObject(Region r, String key) {
    Object anObj = r.get(key);
    Object newObj = null;
    if ((anObj == null) || (!(anObj instanceof BaseValueHolder))) {
      newObj = getValueForKey(key);
    } else {
      newObj = ((BaseValueHolder)anObj).getAlternateValueHolder(randomValues);
    }
    if ((newObj instanceof BaseValueHolder) && TestConfig.tab().getRandGen().nextBoolean()) {
      return ((BaseValueHolder)newObj).extraObject;
    } else {
      return newObj;
    }
  }

  /** Return a key to be used in the blackboard to indicate that a member
   *  created a particular region.
   *  @param vmID The vmID that created the region.
   *  @param regionName The name of the region created.
   */
  protected static String getRegionCreatedKey(int vmID, String regionName) {
    return "regionCreatedInVM_" + vmID + "_regionName_" + regionName;
  }

  /** Make a copy of all disk directories in the given directory. 
   * 
   * @param backupDirName The name of a subdirectory to create in the current
   *        directory; a copy of all disk directories are copied to this
   *        subdirectory.
   */
  private void createDiskDirBackup(String backupDirName) {
    Log.getLogWriter().info("Making a copy of all disk directories in " + backupDirName);
    File backupDir = new File(backupDirName);
    if (backupDir.exists()) {
      throw new TestException("Backup directory " + backupDirName + " already exists");
    }
    backupDir.mkdir();

    // Create a script to do the copy
    String scriptFileName = "makeBackup.sh";
    File scriptFile = new File(scriptFileName);
    PrintWriter aFile;
    try {
      aFile = new PrintWriter(new FileOutputStream(scriptFile));
    } catch (FileNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    aFile.print("#! /bin/sh\ncp -r vm*_disk_* $1\n");
    aFile.flush();
    aFile.close();

    // copy the disk directories to the backup directory
    String cmd = "/bin/bash " + scriptFileName + " " + backupDirName;
    String cmdResults = ProcessMgr.fgexec(cmd, 0);
    Log.getLogWriter().info("Results from " + cmd + " is " + cmdResults);
  }

  /** Return the xml file name for the vm with the given ID.
   *  @pararm vmID The vmID to get the xml file name for. O
   * 
   */
  public static String getXmlFileName(int vmID) {
    List<ClientVmInfo> infoList = StopStartVMs.getAllVMs();
    for (ClientVmInfo info: infoList) {
      if (info.getVmid() == vmID) {
        String clientName = info.getClientName();
        String fileName = "vm_" + vmID;
        if (clientName.indexOf("oldVersion") >= 0) {
          fileName = fileName + "_oldVersion";
        } else if (clientName.indexOf("newVersion") >= 0) {
          fileName = fileName + "_newVersion";
        } 
        return fileName + ".xml";
      }
    }
    throw new TestException("Could not determine clientName for vmID " + vmID);
  }
  
  /** Given an xml file name, return its corresponding vmID
   * 
   */
  private static int getVmIdFromXmlFileName(String xmlFileName) {
    String searchStr = "vm_";
    int index = xmlFileName.indexOf(searchStr);
    int index2 = xmlFileName.indexOf("_", index + searchStr.length());
    int vmID = Integer.valueOf(xmlFileName.substring(index + searchStr.length(), index2));
    return vmID;
  }

  /** Convert disk files for a jvm.
   *  @param convertRandom If true, then randomly choose an old version jvm's disk files
   *                       to convert, if false then convert this jvm's disk files.
   *  @return The index into the List<String> in the blackboard for key oldVersionXmlFileKey
   *          of the converted disk files.
   */
  private static int doSingleConvert(boolean convertRandom) throws TestException {
    // get parallel lists: oldXmlFiles, newXmlFiles
    int myVmId = RemoteTestModule.getMyVmid();
    List<String> oldXmlFiles = new ArrayList();
    List<String> newXmlFiles = new ArrayList();
    int indexToConvert = -1;
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    String[] dirContents = currDir.list();
    for (String fileName: dirContents) {
      if (fileName.endsWith(".xml")) {
        if (fileName.indexOf("oldVersion") >= 0) {
          oldXmlFiles.add(fileName);
        } else if (fileName.indexOf("newVersion") >= 0) {
          newXmlFiles.add(fileName);
          if (fileName.startsWith("vm_" + myVmId + "_")) {
            indexToConvert = newXmlFiles.size()-1;
          }
        }
      }
    }
    if (convertRandom) {
       indexToConvert = TestConfig.tab().getRandGen().nextInt(0, oldXmlFiles.size()-1);
    }
    if (oldXmlFiles.size() != newXmlFiles.size()) {
      throw new TestException("Test expects equal number of oldVersion and newVersion *.xml files");
    }
    // write parallel lists to bb; these lists show the mapping of old vmIds
    // to new vmIds for tests that need this information later
    RecoveryBB.getBB().getSharedMap().put(oldVersionXmlFilesKey, oldXmlFiles);
    RecoveryBB.getBB().getSharedMap().put(newVersionXmlFilesKey, newXmlFiles);

    String oldXmlFile = oldXmlFiles.get(indexToConvert);
    String newXmlFile = newXmlFiles.get(indexToConvert);
    if (RecoveryPrms.getConvertWithNewVersionXml()) {
      convertPre65AndValidate(oldXmlFile, newXmlFile);
    } else {
      convertPre65AndValidate(oldXmlFile, oldXmlFile);
    }
    return indexToConvert;
  }
  
  /** Remove all regions whose names begin with "extra" from the global
   *  list of allRegions.
   */
  private void removeExtraRegionsFromAllRegions() {
    List<Region> allRegionsExceptExtra = new ArrayList(); // don't write extra regions to snapshot
    for (Region aRegion: testInstance.allRegions) {
      if (!aRegion.getName().startsWith("extra")) {
        allRegionsExceptExtra.add(aRegion);
      }
    }
    allRegions = allRegionsExceptExtra;
  }

  // 7.0 rvv conversion methods
  /** Controller for rvv (7.0) conversion test. This is a variant of operationsController.
   *  
   *  There are 2 distributed systems, one for an old version and one for a new
   *  version. This task is run by a thread that does not have a cache or region
   *  for either distributed system, but it is able to stop/start jvms in either
   *  distributed system.
   *  
   *  1 - Kill the jvms in the new version distributed system. These jvms
   *      will be running the rvvConvertersionOperationsResponder task, and will be sleeping
   *      waiting to be killed. Once they are gone, they will not get any more
   *      tasks until they are started, so now only the old version jvms are
   *      running the rvvConvertersionOperationsController task and are populating the region
   *      with data.
   *  2 - Signal to the old jvms to pause; once paused, one is randomly 
   *      chosen to write a snapshot to the blackboard.
   *  3 - Stop all the old jvms. 
   *  4 - Convert the disk files from the old version to the new version.
   *  6 - Start up the new version jvms with the converted disk files. Dynamic
   *      init tasks compare the entries recovered from disk against the
   *      blackboard. 
   *  7 - From this point, the test only uses the new version vms. It will
   *      pause, one jvm will write to the blackboard, restart and verify
   *      against the blackboard in an init task. 
   *  
   */  
  public static void HydraTask_rvvConvertOperationsController() {
    new RecoveryTest().rvvConvertOperationsController();
  }

  private void rvvConvertOperationsController() {
    logExecutionNumber();
    checkForLastIteration();

    // if this is the first time to execute this task, do steps 1 through 6
    boolean firstControllerExecution = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1;
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    if (firstControllerExecution) {
      // stop the new version jvms
      List<String> stopModeList = new ArrayList();
      for (int i = 1; i <= newVersionVMs.size(); i++) {
        stopModeList.add("MEAN_KILL");
      }
      StopStartVMs.stopVMs(newVersionVMs, stopModeList);
    }

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Pausing...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.pausing",
        RecoveryBB.pausing, TestHelper.getNumThreads()/2, // either new version or old version running, but not both
        true, -1, 2000);

    // wait for silence, just to be safe
    SummaryLogListener.waitForSilence(30, 1000);

    // everybody has paused, direct one vm to write to the blackboard
    Log.getLogWriter().info("Directing responder to write to the blackboard...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.writeSnapshot);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotWritten",
        RecoveryBB.snapshotWritten, 1, true, -1, 2000);

    // ask oldVersionAdmin (one of the responder vms) to backup the disk files
    Log.getLogWriter().info("Directing oldVersionAdmin to backup disk files ...");
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.backupDiskFiles);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.backupCompleted",
        RecoveryBB.backupCompleted, 1, true, -1, 2000);

    // stop either 1) all old vms (except the oldVersionAdmin, if this is the first controller execution)
    //          or 2) all new vms (if not the first controller execution)
    List targetVMs = StopStartVMs.getAllVMs();
    if (firstControllerExecution) { // running old version vms
      targetVMs = StopStartVMs.getMatchVMs(targetVMs, "oldVersion");
    } else { // running new version vms
      targetVMs = StopStartVMs.getMatchVMs(targetVMs, "newVersion");
    }
    List<String> stopModesList = new ArrayList();
    for (int i = 1; i <= targetVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    if (RecoveryPrms.getUseShutDownAll()) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      if (adminDS == null) {
        throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
      }
      Object[] anArr = StopStartVMs.shutDownAllMembers(adminDS);
      targetVMs = (List<ClientVmInfo>)(anArr[0]);
    }
    StopStartVMs.stopVMs(targetVMs, stopModesList);

    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.pausing);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.writeSnapshot);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.snapshotWritten);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.leader);
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

    if (firstControllerExecution) { 
      // ask oldVersionAdmin (one of the responder vms) to do offline validation and compaction on old files
      Log.getLogWriter().info("Directing oldVersionAdmin to run offline validation and compaction on old disk files ...");
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.compactOldVersionFiles);
      TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.compactionCompleted",
          RecoveryBB.compactionCompleted, 1, true, -1, 2000);

      // now convert and start up new version vms
      rvvConvert();
      StopStartVMs.startVMs(newVersionVMs);
    } else { // just stopped new version vms; they should all startup together
      // restart vms; init tasks for all vms compare against the blackboard, so if
      // this returns then all is well
      StopStartVMs.startVMs(targetVMs);
    }

    // see if it's time to stop the test
    long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    }
  }

 /** Task to do random operations and direct other vms to pause and create snapshots.  Also coordinates an onlineBackup in the oldVersionAdmin
  *
  */
  public static void HydraTask_rvvConvertOperationsResponder() throws Exception {
    String clientName = System.getProperty("clientName");
    if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) <= 1) {
      if (clientName.indexOf("newVersion") >= 0) {
        // this is a newVersion jvm and its the first execution of the
        // controller; wait for the controller to kill this jvm
        Log.getLogWriter().info("Sleeping for 10 seconds...");
        MasterController.sleepForMs(10000);
        return;
      } 

      if (clientName.indexOf("oldVersionAdmin") >= 0) { // run backup/compaction on old version member
        long backupDiskFiles = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.backupDiskFiles);
        long backupCompleted = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.backupCompleted);
        if ((backupDiskFiles > 0) && (backupCompleted == 0)) {
          if (!CliHelperPrms.getUseCli()) {// don't do this backup section of code if we are using the cli on an old version member
            // the new 7.0 cli does not exist on an old member
            // back up oldVersion diskFiles (also do validation and compaction on backed up files)
            PersistenceUtil.doOnlineBackup(false);
          }
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.backupCompleted);
        } else {
          long compactOldVersionFiles = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.compactOldVersionFiles);
          long compactionCompleted = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.compactionCompleted);
          if ((compactOldVersionFiles > 0) && (compactionCompleted == 0)) {
            if (!CliHelperPrms.getUseCli()) {// don't do this offline val and compaction section of code if we are using the cli on an old version member
              // the new 7.0 cli does not exist on an old member
              // back up oldVersion diskFiles (also do validation and compaction on backed up files)
              doOfflineValAndCompaction();
            }
            RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.compactionCompleted);
          } else {
            Log.getLogWriter().info("Sleeping for 10 seconds...");
            MasterController.sleepForMs(10000);
          }
        }
        return;
      }
    }
    // don't assign cache related tasks to the admin vm
    if (clientName.indexOf("oldVersionAdmin") >= 0) {
      MasterController.sleepForMs(10000);
    } else {
      testInstance.operationsResponder();
    }
  }

  public static void HydraTask_convertPre70Controller() {
    // stop the new version vms; they will restart later after the conversion
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    newVersionVMs.removeAll(StopStartVMs.getMatchVMs(newVersionVMs, "newVersionLocator"));
    List<String> stopModesList = new ArrayList();
    for (int i = 1; i <= newVersionVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    StopStartVMs.stopVMs(newVersionVMs, stopModesList);
    
    // sleep to allow ops to run
    int sleepMs = 60000;
    Log.getLogWriter().info("Sleeping " + sleepMs + "ms so ops can run...");
    MasterController.sleepForMs(sleepMs);
    
    // stop the old version vms, one at a time so the ops can continue to 
    // make their disk files different
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.stoppingVMs);
    List<ClientVmInfo> oldVersionVMs = StopStartVMs.getAllVMs();
    oldVersionVMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion");
    for (ClientVmInfo target: oldVersionVMs) {
      try {
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, target);
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
    RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.stoppingVMs);

    // convert the disk files
    rvvConvert();
    StopStartVMs.startVMs(newVersionVMs);
  }

  /** Verify all regions from the snapshot and wait for all threads to complete verify
   * 
   */
  public static void HydraTask_rvvConvertVerifyFromSnapshotAndSync() {
    // the first time we run this on a vm restart, we might have additional extra regions
    // not in the snapshot (because we converted to a new xml file that contained extra
    // persistent regions); add them to the snapshot so verifyFromSnapshot won't complain
    if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
        RecoveryPrms.getConvertWithNewVersionXml()) {
      List[] aList = extractFromXmlFile(getXmlFileName(RemoteTestModule.getMyVmid()));
      List<String> regionNames = aList[0];
      Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
      for (int i = 0; i < regionNames.size(); i++) {
        String rName = regionNames.get(i);
        if (rName.startsWith("/extra")) {
          if (!allRegionsSnapshot.containsKey(rName)) {
            Log.getLogWriter().info("Adding region " + rName + " to snapshot");
            allRegionsSnapshot.put(rName, new HashMap());
          }
        }
      }
      RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    }
    testInstance.verifyFromSnapshot();
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter", 
        RecoveryBB.doneVerifyingCounter,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads(),
        true, -1, 2000);
    HydraTask_prepareForNewVersionOps();
  }

  /** Verify all regions from the snapshot and wait for all threads to complete verify
   * 
   */
  public static void HydraTask_rvvConvertVerifyFromSnapshot() {
    // the first time we run this on a vm restart, we might have additional extra regions
    // not in the snapshot (because we converted to a new xml file that contained extra
    // persistent regions); add them to the snapshot so verifyFromSnapshot won't complain
    if ((RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber) == 1) &&
        RecoveryPrms.getConvertWithNewVersionXml()) {
      List[] aList = extractFromXmlFile(getXmlFileName(RemoteTestModule.getMyVmid()));
      List<String> regionNames = aList[0];
      Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
      for (int i = 0; i < regionNames.size(); i++) {
        String rName = regionNames.get(i);
        if (rName.startsWith("/extra")) {
          if (!allRegionsSnapshot.containsKey(rName)) {
            Log.getLogWriter().info("Adding region " + rName + " to snapshot");
            allRegionsSnapshot.put(rName, new HashMap());
          }
        }
      }
      RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    }
    MasterController.sleepForMs(15000); // allow gateway hub to start delivering from persistent queue
    SummaryLogListener.waitForSilence(30, 2000);
    testInstance.verifyFromSnapshot();
    HydraTask_prepareForNewVersionOps();
  }


  /** Run from a new version locator thread
   *  This code is hardcoded for 3 wan sites because of a time crunch to get
   *  the test running. This could be changed later.
   */
  public static void HydraTask_rvvConvertWANTestController() {
    logExecutionNumber();
    List<ClientVmInfo> newVersionVMs = StopStartVMs.getAllVMs();
    List<ClientVmInfo> oldVersionVMs = StopStartVMs.getAllVMs();
    newVersionVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion");
    oldVersionVMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion");
    List<ClientVmInfo> newLocators = StopStartVMs.getMatchVMs(newVersionVMs, "newVersionLocator");
    for (ClientVmInfo info: newLocators) { // remove myself from newLocators
      if (info.getVmid() == RemoteTestModule.getMyVmid()) {
        newLocators.remove(info);
        break;
      }
    }
    List<ClientVmInfo> oldLocators = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersionLocator");
    List<ClientVmInfo> newWanSite1VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_1_");
    List<ClientVmInfo> newWanSite2VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_2_");
    List<ClientVmInfo> newWanSite3VMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_3_");
    List<ClientVmInfo> oldWanSite1VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_1_");
    List<ClientVmInfo> oldWanSite2VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_2_");
    List<ClientVmInfo> oldWanSite3VMs = StopStartVMs.getMatchVMs(oldVersionVMs, "oldVersion_3_");
    Log.getLogWriter().info("newVersionVMs: " + newVersionVMs);
    Log.getLogWriter().info("oldVersionVMs: " + oldVersionVMs);
    Log.getLogWriter().info("newWanSite1VMs: " + newWanSite1VMs);
    Log.getLogWriter().info("newWanSite2VMs: " + newWanSite2VMs);
    Log.getLogWriter().info("newWanSite3VMs: " + newWanSite3VMs);
    Log.getLogWriter().info("oldWanSite1VMs: " + oldWanSite1VMs);
    Log.getLogWriter().info("oldWanSite2VMs: " + oldWanSite2VMs);
    Log.getLogWriter().info("oldWanSite3VMs: " + oldWanSite3VMs);

    // stop the new version vms; they will restart later after the conversion
    List<String> stopModesList = new ArrayList();
    List<ClientVmInfo> targetVMs = StopStartVMs.getMatchVMs(newVersionVMs, "newVersion_");
    for (int i = 1; i <= targetVMs.size(); i++) {
      stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    StopStartVMs.stopVMs(targetVMs, stopModesList);
    StopStartVMs.stopVMs(newLocators, stopModesList);

    // sleep to allow ops to run
    int sleepMs = 60000;
    Log.getLogWriter().info("Sleeping " + sleepMs + "ms so ops can run...");
    MasterController.sleepForMs(sleepMs);

    // stop all other oldVersion vms besides myself in a particular order; this allows WAN queues to grow
    try {
      // stop all but one vm per wan site
      for (int i = 1; i < oldWanSite1VMs.size(); i++) {
        Log.getLogWriter().info("Stopping: " + oldWanSite1VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite1VMs.get(i));
        Log.getLogWriter().info("Stopping: " + oldWanSite2VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite2VMs.get(i));
        Log.getLogWriter().info("Stopping: " + oldWanSite3VMs.get(i));
        ClientVmMgr.stop("Stopping old version vm", 
            ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
            ClientVmMgr.ON_DEMAND, oldWanSite3VMs.get(i));
      }

      Log.getLogWriter().info("Stopping: " + oldWanSite2VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
          ClientVmMgr.ON_DEMAND, oldWanSite2VMs.get(0));
      Log.getLogWriter().info("Stopping: " + oldWanSite3VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.toStopMode(TestConfig.tab().stringAt(StopStartPrms.stopModes)), 
          ClientVmMgr.ON_DEMAND, oldWanSite3VMs.get(0));

    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    // now only oldWanSite1VMs.get(0) is still running; it needs to pause and write a snapshot
    RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
    TestHelper.waitForCounter(RecoveryBB.getBB(), 
        "RecoveryBB.snapshotWritten", RecoveryBB.snapshotWritten, 
        1, true, -1, 1000);

    //stop the last vm
    try {
      Log.getLogWriter().info("Stopping: " + oldWanSite1VMs.get(0));
      ClientVmMgr.stop("Stopping old version vm", 
          ClientVmMgr.MEAN_KILL,
          ClientVmMgr.ON_DEMAND, oldWanSite1VMs.get(0));
    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    // stop the old version locators
    Log.getLogWriter().info("Stopping the old version locator(s)");
    for (ClientVmInfo info: oldLocators) {
      try {
        ClientVmMgr.stop("Stopping old version locator",
            ClientVmMgr.MEAN_KILL,
            ClientVmMgr.ON_DEMAND, info);
      } catch (ClientVmNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    // todo@lhughes - for now, we only have 1 VM in each WAN Site
    // otherwise, we have to ensure we convert the primary Gateway
    // convert 
    List<String> oldXmlFiles = new ArrayList();
    oldXmlFiles.add(0, getXmlFileName(oldWanSite1VMs.get(0).getVmid()));
    oldXmlFiles.add(1, getXmlFileName(oldWanSite2VMs.get(0).getVmid()));
    oldXmlFiles.add(2, getXmlFileName(oldWanSite3VMs.get(0).getVmid()));

    List<String> newXmlFiles = new ArrayList();
    newXmlFiles.add(0, getXmlFileName(newWanSite1VMs.get(0).getVmid()));
    newXmlFiles.add(1, getXmlFileName(newWanSite2VMs.get(0).getVmid()));
    newXmlFiles.add(2, getXmlFileName(newWanSite3VMs.get(0).getVmid()));
    
    rvvConvert(oldXmlFiles, newXmlFiles);

    // start in a particular order
    // start the locators first
    GatewayHubBlackboard.getInstance().getSharedMap().clear();
    DistributedSystemBlackboard.getInstance().getSharedMap().clear();
    StopStartVMs.startVMs(newLocators);

    try {
      Log.getLogWriter().info("Starting: " + newWanSite1VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite1VMs.get(0));
      Log.getLogWriter().info("Starting: " + newWanSite2VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite2VMs.get(0));
      Log.getLogWriter().info("Starting: " + newWanSite3VMs.get(0));
      ClientVmMgr.start("Starting after conversion", newWanSite3VMs.get(0));
      // @todo lynng starting the above runs validation and confirms the wan queues
      // were converted; starting the below vms is an extra optional step; because
      // this test is running 2 separate versions of 2 distributed systems, the locators
      // are not being sufficiently refreshed by hydra in the above clear() calls
      //for (int i = 1; i < newWanSite2VMs.size(); i++) {
      //  Log.getLogWriter().info("Starting: " + newWanSite1VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite1VMs.get(i));
      //  Log.getLogWriter().info("Starting: " + newWanSite2VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite2VMs.get(i));
      //  Log.getLogWriter().info("Starting: " + newWanSite3VMs.get(i));
      //  ClientVmMgr.start("Starting other vms in site", newWanSite3VMs.get(i));
      //}
    } catch (ClientVmNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Run from the oldVersionPublisher thread 
   *  This code is hardcoded for 3 wan sites because of a time crunch to get
   *  the test running. This could be changed later.
   */
  public static void HydraTask_rvvConvertWANTestResponder() throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    testInstance.doOperations(minTaskGranularityMS);
    if (RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing) > 0) {
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.pausing);
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.Pausing", RecoveryBB.pausing, 
          11, true, -1, 1000);
      long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
      if (leader == 1) {
        HydraTask_writeToBB();
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
      }
      MasterController.sleepForMs(Integer.MAX_VALUE); // wait to be killed by the controller
    }
  }

  /** Execute conversion from pre-7.0 to 7.0 (RVV), but only one
   *  thread in the test can do the conversion.
   */
  public static void HydraTask_rvvConvert() {
    long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
    if (leader == 1) {
      rvvConvert();
    }
  }

  /** Run the 7.0 conversion method.
   */
  public static void rvvConvert() {
    // before converting, take a snapshot of the disk files (this is for debug purposes only
    // so will be commented out unless we are debugging)
    // createDiskDirBackup("backupForExeNumber_" + RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
    List<String> oldXmlFiles = new ArrayList();
    List<String> newXmlFiles = new ArrayList();
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    String[] dirContents = currDir.list();
    for (String fileName: dirContents) {
      if (fileName.endsWith(".xml")) {
        if (fileName.indexOf("oldVersion") >= 0) {
          oldXmlFiles.add(fileName);
        } else if (fileName.indexOf("newVersion") >= 0) {
          newXmlFiles.add(fileName);
        }
      }
    }
    rvvConvert(oldXmlFiles, newXmlFiles);
  }

  public static void rvvConvert(List<String> oldXmlFiles, List<String> newXmlFiles) {

    Log.getLogWriter().info("Converting diskStores to 7.0 (RVV) format ...");

    // write xml file names to the blackboard, required for reinitialization in the newVersion vms
    // write parallel lists to bb; these lists show the mapping of old vmIds
    // to new vmIds for tests that need this information later
    RecoveryBB.getBB().getSharedMap().put(oldVersionXmlFilesKey, oldXmlFiles);
    RecoveryBB.getBB().getSharedMap().put(newVersionXmlFilesKey, newXmlFiles);

    listDiskFiles();

    // convert each oldVersion vms files 
    for (String filename: oldXmlFiles) {
      List[] oldVersionXmlInfo = extractFromXmlFile(filename);
      List<String> oldVersionRegionNames = oldVersionXmlInfo[0];
      List<String> oldVersionDiskStoreNames = oldVersionXmlInfo[1];
      List<List<String>> oldVersionDiskDirs = oldVersionXmlInfo[2];
      List<String> oldVersionDataPolicy = oldVersionXmlInfo[3];

      Set<String> uniqueDiskStoreNames = new HashSet(oldVersionDiskStoreNames);
      // For this vm, build a list of persistent diskDirs for diskStoreName
      for (String diskStoreName: uniqueDiskStoreNames) {
        if (diskStoreName != null) {
          Set<String> persistRegNamesForDiskStore = new HashSet();
          Set<String> diskDirsForDiskStore = new HashSet<String>();
          for (int i = 0; i < oldVersionDiskStoreNames.size(); i++) {
            if ((oldVersionDiskStoreNames.get(i) != null) && (oldVersionDiskStoreNames.get(i).equals(diskStoreName))) {
              diskDirsForDiskStore.addAll(oldVersionDiskDirs.get(i));
              if (oldVersionDataPolicy.get(i) != null) {
                if (oldVersionDataPolicy.get(i).indexOf("persistent") >= 0) {
                  persistRegNamesForDiskStore.add(oldVersionRegionNames.get(i));
                }
              }
            }
          }

          List diskDirsAsFile = new ArrayList();
          for (String diskDirPath: diskDirsForDiskStore) {
            diskDirsAsFile.add(new File(diskDirPath));
          }
          File[] diskDirArr = new File[diskDirsAsFile.size()];
          diskDirsAsFile.toArray(diskDirArr);
          
          if (diskDirArr.length > 0 && hasDiskInitFile(diskStoreName, diskDirArr)) { 
            // there must be at least one persistent region to convert
            try {
              rvvConvert(diskStoreName, diskDirArr);
            } catch (HydraRuntimeException e) {
              if (persistRegNamesForDiskStore.size() == 0) { 
                // there were no persistent regions in diskStoreName, so nothing to convert
                String errStr = e.getCause().toString();
                if ((errStr.indexOf("The init file") >= 0) &&
                    (errStr.indexOf(" does not exist") > 0)) {
                  Log.getLogWriter().info("Got expected exception " + e + " with cause " +
                      errStr + " due to no persistent regions in " +
                      " DiskStore " + diskStoreName);
                } else {
                  throw e;
                }
              } else {
                throw e;
              }
            }
          }  
        }
      }
    }
    listDiskFiles();

    // if configured: for this vm, move the converted files over to newVersion diskDirs
    if (RecoveryPrms.getConvertWithNewVersionXml()) {
      moveDiskFiles(oldXmlFiles, newXmlFiles);
    }
  }
  
  protected static boolean hasDiskInitFile(String diskStoreName, File[] diskDirArr) {
    boolean hasBackupIfFile = false;
    for (File diskDir: diskDirArr) {
      String[] diskDirFileNames = diskDir.list();
      for (String diskFileName: diskDirFileNames) {
        String searchStr1 = "BACKUP";
        String searchStr2 = ".if";
        if ((diskFileName.startsWith(searchStr1)) && 
            ((diskFileName.indexOf(diskStoreName + "_") > 0) ||
             (diskFileName.indexOf(diskStoreName + ".") > 0)) &&
            (diskFileName.endsWith(searchStr2))) {
          Log.getLogWriter().info("diskInitFile " + diskFileName + " found in " + diskDir.getName());
          hasBackupIfFile = true;
        }
      }
    }
    return hasBackupIfFile;
  }

  /** Given a diskStoreName and an array of disk directories:
   *     1) run the offline validator
   *     2) run the offline conversion utility (7.0 RVV)
   *     3) run the offline validator again
   *  then check that the validator returns the same results before and
   *  after the conversion.
   *  
   * @param diskStoreName The name of a diskStore.
   * @param diskDirArr An array of Files, each element being a disk directory.
   */
  protected static void rvvConvert(String diskStoreName, File[] diskDirArr) {
    Vector<String> diskStoreNames = TestConfig.tab().vecAt(DiskStorePrms.names);
    int i = diskStoreNames.indexOf(diskStoreName);
    Vector<String> maxOpLogSizes = TestConfig.tab().vecAt(DiskStorePrms.maxOplogSize, new HydraVector());
    long opLogSize = 0;
    if ((maxOpLogSizes.size() == 0) || (i < 0) || (i >= maxOpLogSizes.size())) {
      opLogSize = DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE;
    } else {
      opLogSize = Long.valueOf(maxOpLogSizes.get(i));
    }

    // convert to 7.0 format (RVV)
    if (hasDiskInitFile(diskStoreName, diskDirArr)) {
      rvvConvert(diskStoreName, diskDirArr, opLogSize);
    } else {
      // No disk init file means there are no persistent regions 
      Log.getLogWriter().info("Not converting files in " + diskDirArr + " for " + diskStoreName + " no BACKUP"+ diskStoreName + ".if file exists" );
    }
  }

  /** Run the offline conversion tool.
   * @param diskStoreName The diskStore name to use for conversion.
   * @param diskDirArr An Array of Files, each file is a disk dir.
   * @param opLogSize The opLog size for the diskStore.
   */
  private static void rvvConvert(String diskStoreName, File[] diskDirArr, long opLogSize) {
    String diskDirStr = "";
    String delimeter = " ";
    if (CliHelperPrms.getUseCli()) {
      delimeter = ",";
    }
    for (int i = 0; i < diskDirArr.length; i++) {
      File diskDir = diskDirArr[i];
      diskDirStr = diskDirStr + diskDir.getAbsolutePath();
      if (i < diskDirArr.length-1){
        diskDirStr = diskDirStr + delimeter;
      }
    }
    diskDirStr = diskDirStr + " ";

    String result = null;
    if (CliHelperPrms.getUseCli()) {
      CommandTest.HydraTask_initAndConnectCLI(); // this is a noop after the first call
      String command = "upgrade offline-disk-store --name=" + diskStoreName + " --disk-dirs=" + diskDirStr + " --max-oplog-size=" + opLogSize;
      result = CliHelper.execCommandLocally(command, true)[1];
    } else {
      String exeName = "gemfire";
      String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
      if (HostHelper.isWindows()) {
        exeName = "gemfire.bat";
        command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
      }
      command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
          File.separator + exeName + " upgrade-disk-store " +
          diskStoreName + " " + diskDirStr +
          "-maxOplogSize=" + opLogSize;
      Log.getLogWriter().info("Calling offline conversion tool with diskStoreName " + diskStoreName +
          ", disk dirs " + diskDirStr + " and opLogSize " + opLogSize);
      long startTime = System.currentTimeMillis();
      result = ProcessMgr.fgexec(command, 0);
      long duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("Done calling offline conversion tool, result is " + result + ", conversion took " + duration + " ms");
    }
    if (result.indexOf("Upgrade disk store") < 0) { // did not return normal result
      throw new TestException("Offline conversion tool returned " + result);
    }
  }

  /* AdminVM to backup disk files (oldversion vms) and do compaction and 
   * validation on oldVersion disk files.
   */
  public static void HydraTask_doOnlineBackup() {
    PersistenceUtil.doOnlineBackup(false);
  }

  /* doOfflineValAndCompaction based on cache.xml definitions for disk-dirs and diskStoreNames
   */
  protected static void doOfflineValAndCompaction() {
    // we cannot use the PersistenceUtil.doOfflineValAndCompact() because it passes in 
    // ALL disk-dirs for a VM.  This allows compacted files to be moved to disk-dirs which are
    // not included in the disk-dirs for a particular diskStore (in the cache.xml).

    // Determine the list of disk-dirs (per vm) based on the cache.xml
    // get the oldVersion cache.xml filenames
    List<String> oldXmlFiles = new ArrayList();
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    String[] dirContents = currDir.list();
    for (String fileName: dirContents) {
      if (fileName.endsWith(".xml")) { 
        if (fileName.indexOf("oldVersion") >= 0) {
          oldXmlFiles.add(fileName);
        } 
      }
    }

    // for each diskStore, validate and compact
    for (String filename: oldXmlFiles) {
      List[] oldVersionXmlInfo = extractFromXmlFile(filename);
      List<String> oldVersionRegionNames = oldVersionXmlInfo[0];
      List<String> oldVersionDiskStoreNames = oldVersionXmlInfo[1];
      List<List<String>> oldVersionDiskDirs = oldVersionXmlInfo[2];
      List<String> oldVersionDataPolicy = oldVersionXmlInfo[3];

      Set<String> uniqueDiskStoreNames = new HashSet(oldVersionDiskStoreNames);
      // For this vm, build a list of persistent diskDirs for diskStoreName
      for (String diskStoreName: uniqueDiskStoreNames) {
        if (diskStoreName != null) {
          Set<String> persistRegNamesForDiskStore = new HashSet();
          Set<String> diskDirsForDiskStore = new HashSet<String>();
          for (int i = 0; i < oldVersionDiskStoreNames.size(); i++) {
            if ((oldVersionDiskStoreNames.get(i) != null) && (oldVersionDiskStoreNames.get(i).equals(diskStoreName))) {
              diskDirsForDiskStore.addAll(oldVersionDiskDirs.get(i));
              if (oldVersionDataPolicy.get(i) != null) {
                if (oldVersionDataPolicy.get(i).indexOf("persistent") >= 0) {
                  persistRegNamesForDiskStore.add(oldVersionRegionNames.get(i));
                }
              }
            }
          }

          List diskDirsAsFile = new ArrayList();
          for (String diskDirPath: diskDirsForDiskStore) {
            diskDirsAsFile.add(new File(diskDirPath));
          }
          File[] diskDirArr = new File[diskDirsAsFile.size()];
          diskDirsAsFile.toArray(diskDirArr);

          // there must be at least one persistent region to validate and compact
          if (diskDirArr.length > 0 && hasDiskInitFile(diskStoreName, diskDirArr)) {
            try {
              PersistenceUtil.runOfflineValAndCompaction(diskStoreName, diskDirArr);
            } catch (TestException e) {
              if (persistRegNamesForDiskStore.size() == 0) { 
                // there were no persistent regions in diskStoreName, so nothing to convert
                // validate-disk-store removes the disk init file in these cases
                // and compact-disk-store then fails with this Exception
                String errStr = e.toString();
                if ((errStr.indexOf("The init file") >= 0) &&
                    (errStr.indexOf(" does not exist") > 0)) {
                  Log.getLogWriter().info("Got expected exception " + e + " with cause " +
                      errStr + " due to no persistent regions in " +
                      " DiskStore " + diskStoreName);
                } else {
                  throw e;
                }
              } else {
                Log.getLogWriter().info("Expected disk files for " + diskStoreName + " with persistent regions " + persistRegNamesForDiskStore);
                throw e;
              }
            }
          }
        }
      }
    }
  }

  /** This is run by one vm to control responder vms for reaping tomstone tests.
   *   This directs the responder vms to
   *      1) initialize (create regions)
   *      2) verify all regions are empty
   *      3) load regions with configured number of entries
   *      4) destroy all entries (via destroy, clear or expiration)
   *      5) stop and restart all vms; one of the vms will initialize from disk regions
   *          and all others will do a gii from the one that did disk recovery
   *   This is repeated until numSecondsToRun is reached 
   */
  public static void HydraTask_tombstoneTestController() {
    testInstance.tombstoneTestController();
  }
  private  void tombstoneTestController() {
    // initialize
    String currentTaskStep = (String)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.taskStep));
    Log.getLogWriter().info("In tombstoneTestController, currentTaskStep is " + currentTaskStep);
    if (currentTaskStep.equals(STARTING_STEP)) {
      // initialize
      logExecutionNumber();
      checkForLastIteration();
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneDestroyingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneAddingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneVerifyingCounter);

      // wait for vms to signal they completed starting step
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.doneStartingCounter", RecoveryBB.doneStartingCounter, 
          TestHelper.getNumVMs()-1, true, -1, 1000);

      // change step to verify (verifying that all regions are empty)
      currentTaskStep = VERIFY_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);

      // let everyone move forward
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneStartingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control verify
    if (currentTaskStep.equals(VERIFY_STEP)) {
      // verify step is done when all other vms report completion of verifyAllRegionsEmtpy
      TestHelper.waitForCounter(RecoveryBB.getBB(),
          "RecoveryBB.doneVerifyingCounter", RecoveryBB.doneVerifyingCounter,
          TestHelper.getNumVMs()-1, true, -1, 5000);

      // done with verify; prepare for add step
      NameBB.getBB().getSharedCounters().zero(NameBB.POSITIVE_NAME_COUNTER);
      currentTaskStep = ADD_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      // let everyone move forward
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control add
    if (currentTaskStep.equals(ADD_STEP)) {
      // wait for adds to complete
      TestHelper.waitForCounter(RecoveryBB.getBB(),
          "RecoveryBB.doneAddingCounter", RecoveryBB.doneAddingCounter,
          TestHelper.getNumThreads()-1, true, -1, 10000);

      // adds are complete
      currentTaskStep = DESTROY_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);

      // let everyone move forward
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneAddingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    if (currentTaskStep.equals(DESTROY_STEP)) {
      // wait for destroys to complete; responder threads are done when all 
      // responder threads report they are done
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.doneDestroyingCounter", RecoveryBB.doneDestroyingCounter, 
          TestHelper.getNumThreads()-1, true, -1, 10000);

      // done with destroys
      currentTaskStep = STOP_START_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);

      // let everyone move forward
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneDestroyingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control stop/start
    if (currentTaskStep.equals(STOP_START_STEP)) {
      // stop all vms, then restart (one recovers from disk, the others gii from disk recovery
      Vector otherVmIds = ClientVmMgr.getOtherClientVmids();
      List otherVMs = new ArrayList();
      List replicateVMs = new ArrayList();
      List persistentVMs = new ArrayList();
      List stopModeList = new ArrayList();
      for (int i = 0; i < otherVmIds.size(); i++) {
        Integer otherVmId = (Integer)(otherVmIds.get(i));
        ClientVmInfo info = new ClientVmInfo(otherVmId);
        otherVMs.add(info);
        if (RecoveryBB.getBB().getSharedMap().containsKey(persistentRegionKey + otherVmId)) {
          persistentVMs.add(info);
        } else {
          replicateVMs.add(info);
        }
        String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
        stopModeList.add(choice);
      }
      StopStartVMs.stopVMs(otherVMs, stopModeList); // stop all vms other than this one

      // wait for tombstone-gc-timeout ... we we gc as soon as we restart (I hope)
      Log.getLogWriter().info("Sleeping for 60 seconds to allow tombstone-gc");
      MasterController.sleepForMs(60000); 
      Log.getLogWriter().info("Finished sleeping for 60 seconds to allow tombstone-gc");

      // change to next step
      currentTaskStep = STARTING_STEP;
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneStartingCounter);
      RecoveryBB.getBB().getSharedCounters().zero(RecoveryBB.doneAddingCounter);
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      RecoveryBB.getBB().getSharedMap().put(RecoveryBB.taskStep, currentTaskStep);
      // see if it's time to stop the test
      // don't start the responder vms 
      long counter = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.timeToStop);
      if (counter >= 1) {
        throw new StopSchedulingOrder("Num controller executions is " +
            RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber));
      }

      StopStartVMs.startVMs(persistentVMs); // start the persistent vms first
      StopStartVMs.startVMs(replicateVMs); // start the replicate vms next
    }
  }

  /** Responds to directives issued by the tombstone test controller
   * 
   */
  public static void HydraTask_tombstoneTestResponder() {
    testInstance.tombstoneTestResponder();
  }
  private void tombstoneTestResponder() {
    String currentTaskStep = (String)(RecoveryBB.getBB().getSharedMap().get(RecoveryBB.taskStep));
    Log.getLogWriter().info("In tombstoneTestResponder, currentTaskStep is " + currentTaskStep);
    if (currentTaskStep.equals(STARTING_STEP)) {
      if (startingCoordinator.incrementAndGet() == 1) { // only do this once per vm
        Log.getLogWriter().info("Initializing for startup...");
        verifyCoordinator = new AtomicInteger(0);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneStartingCounter);
      } 
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.doneStartingCounter", RecoveryBB.doneStartingCounter, 
          TestHelper.getNumVMs(), true, -1, 1000);
    } else if (currentTaskStep.equals(VERIFY_STEP)) {
      int coord = verifyCoordinator.incrementAndGet();
      if (coord == 1) { // this is the one thread in this vm to do verification
        verifyAllRegionsEmpty();
        startingCoordinator = new AtomicInteger(0);
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
      } 
      TestHelper.waitForCounter(RecoveryBB.getBB(), 
          "RecoveryBB.doneVerifyingCounter", RecoveryBB.doneVerifyingCounter, 
          TestHelper.getNumVMs(), true, -1, 1000);
    } else if (currentTaskStep.equals(ADD_STEP)) {
      if ((Boolean)addsCompleted.get()) {
        TestHelper.waitForCounter(RecoveryBB.getBB(),
            "RecoveryBB.doneAddingCounter", RecoveryBB.doneAddingCounter,
            TestHelper.getNumThreads(), true, -1, 1000);
      } else {
        int numKeysToCreate = RecoveryPrms.getMaxNumEntriesPerRegion();
        long nameCounter = 0;
        while (nameCounter < numKeysToCreate) {
          Object key = NameFactory.getNextPositiveObjectName();
          nameCounter = NameFactory.getCounterForName(key);
          for (Region aRegion: allRegions) {
            aRegion.put(key, getValueForKey(key));
          }
        }
        addsCompleted.set(new Boolean(true));
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneAddingCounter);
      }
    } else if (currentTaskStep.equals(DESTROY_STEP)) {
      if ((Boolean)destroysCompleted.get()) {
        TestHelper.waitForCounter(RecoveryBB.getBB(),
            "RecoveryBB.doneDestroyingCounter", RecoveryBB.doneDestroyingCounter,
            TestHelper.getNumThreads(), true, -1, 1000);
      } else {
        String destroyMethod = RecoveryPrms.getDestroyMethod();
        Log.getLogWriter().info("Destroying all entries via " + destroyMethod);
        if (destroyMethod.equals("destroy")) {
          destroyAllEntries();
        } else if (destroyMethod.equals("clearRegion")) {
          clearAllRegions();
        } else if (destroyMethod.equals("expiration")) {
          // allow entries to expire (TTL and IdleTO set to 10 seconds with action DESTROY
          Log.getLogWriter().info("waiting 60 seconds for expiration");
          MasterController.sleepForMs(60000); // allow events to expire (TTL and IdleTO set to 10 seconds)
          Log.getLogWriter().info("Done waiting for expiration");
        } else {
          throw new TestException("Test issue: no destroyMethod support for " + destroyMethod);
        }
        destroysCompleted.set(new Boolean(true));
        RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneDestroyingCounter);
      }
    } else if (currentTaskStep.equals(STOP_START_STEP)) { // step handled by controller, not responder
      MasterController.sleepForMs(5000); 
    }
  }

  protected void verifyAllRegionsEmpty() {
    // get the list of test regions (written to the BB by Hydratask_initialize/createHierRegions
    Set<String> testRegions = (Set<String>)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));

    Cache theCache = CacheHelper.getCache();
    for (String regionName: testRegions) {
      Region aRegion = theCache.getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      } 
      if (aRegion.size() != 0) {
        throw new TestException("Region " + regionName + " is not empty (but should be).  region.size() = " + aRegion.size());
      }
    }

    Log.getLogWriter().info("Successfully verified that all test regions are empty (size=0).  Regions - " + testRegions);
    // reset key name counter (for upcoming load step)
    NameBB.getBB().getSharedCounters().zero(NameBB.POSITIVE_NAME_COUNTER);
  }

  protected void destroyAllEntries() {
    // get the list of test regions (written to the BB by Hydratask_initialize/createHierRegions
    Set<String> testRegions = (Set<String>)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));

    Cache theCache = CacheHelper.getCache();
    for (String regionName: testRegions) {
      Region aRegion = theCache.getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      } 
      for (Object key: aRegion.keySet()) {
        try {
           aRegion.destroy(key);
        } catch (EntryNotFoundException e){
           // quietly allow this exception as we have multple threads destroys entries
        }
      }
      Log.getLogWriter().info("Successfully destroyed all entries in " + regionName);
    }
  }

  protected void clearAllRegions() {
    // get the list of test regions (written to the BB by Hydratask_initialize/createHierRegions
    Set<String> testRegions = (Set<String>)(RecoveryBB.getBB().getSharedMap().get(allRegionNamesKey));

    Cache theCache = CacheHelper.getCache();
    for (String regionName: testRegions) {
      Region aRegion = theCache.getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      } 
      aRegion.clear();
      Log.getLogWriter().info("Successfully cleared " + regionName);
    }
  }

  /**
   * Verify if initialized remotely (via gii) verify that we used deltaGII if expected.
   */
  public static void HydraTask_verifyDeltaGII() {
    testInstance.verifyDeltaGII();
  }

  /**
   * Verify that all regions were gii'ed as expected (delta or full gii).
   * throws a TestException if gii is not as configured via expectDeltaGII.
   */
  private void verifyDeltaGII() {
    boolean expectDeltaGII = true;
    Log.getLogWriter().info("verifyDeltaGII: invoked. expectDeltaGII = " + expectDeltaGII + ". ");
    String regionName;
    DiskRegion diskRegion;
    for (Region aRegion : allRegions) {
      regionName = aRegion.getName();
      diskRegion = ((LocalRegion) aRegion).getDiskRegion();
      if (diskRegion != null && diskRegion.getStats().getRemoteInitializations() == 0) {
        Log.getLogWriter().info("verifyDeltaGII: " + regionName +
                                " was recovered from disk (Remote Initializations = " +
                                diskRegion.getStats().getRemoteInitializations() + ").");
      } else {
        int giisCompleted = TestHelper.getStat_getInitialImagesCompleted(regionName);
        int deltaGiisCompleted = TestHelper.getStat_deltaGetInitialImagesCompleted(regionName);
        if ((expectDeltaGII && (deltaGiisCompleted == 0)) || (!expectDeltaGII && (deltaGiisCompleted == 1))) {
          throw new TestException("Did not perform expected type of GII. expectDeltaGII = " + expectDeltaGII +
                                  " for region " + aRegion.getFullPath() +
                                  " GIIsCompleted = " + giisCompleted +
                                  " DeltaGIIsCompleted = " + deltaGiisCompleted);
        } else {
          Log.getLogWriter().info("verifyDeltaGII: " + regionName +
                                  " Remote Initialization (GII): GIIsCompleted = " + giisCompleted +
                                  " DeltaGIIsCompleted = " + deltaGiisCompleted + ".");
        }
      }
    }
  }
}
