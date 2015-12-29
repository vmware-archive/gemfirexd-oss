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

package snapshot;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.CacheServerHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
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
import hydra.PartitionDescription;
import hydra.ProcessMgr;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.VersionPrms;

import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicBoolean;

import parReg.ParRegUtil;
import pdx.PdxTest;
import rebalance.RebalanceBB;
import rebalance.RebalancePrms;
import rebalance.RebalanceUtil;
import util.CliHelper;
import util.CliHelperPrms;
import util.DeclarativeGenerator;
import util.AdminHelper;
import util.NameBB;
import util.NameFactory;
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
import util.BaseValueHolder;
import util.ValueHolder;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.snapshot.SnapshotIterator;
import com.gemstone.gemfire.cache.snapshot.SnapshotReader;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.cache.snapshot.SnapshotFilter;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * @author lhughes (based on diskRecovery.RecoveryTest written by Lynn Gallinat)
 *
 */
public class SnapshotTest {

  /** instance of this test class */
  public static SnapshotTest testInstance = null;

  /** snapshot test steps */
  private static final String STARTING_STEP = "Starting";
  private static final String EXECUTE_OPS_STEP = "ExecuteOps";
  private static final String EXPORT_STEP = "Exporting";
  private static final String RECYCLE_STEP = "Stopping/Starting";
  private static final String IMPORT_STEP = "Importing";
  private static final String VERIFY_STEP = "Verifying";

  // instance fields to hold information about this test run
  private RandomValues randomValues = new RandomValues(); // used to create payload
  private boolean uniqueKeys = true;
  private String exportStrategy = null;
  private String restrictSnapshotOperationsTo = null;
  private boolean configuredForSnapshotOps = true;  // assume all vms can export, unless we override
                                               // based on restrictSnapshotOperationsTo

  // handle single or multiple regions in the cache
  private List<Region> allRegions = new ArrayList();
  private static final String allRegionNamesKey = "allRegions";
  private static final String allRegionsSnapshotKey = "allRegionsSnapshot";

  private static final String snapshotDirPrefix = "snapshotDir";

  // mark that this vm is the export and/or import leader (at this moment)
  private boolean leaderInThisVM = false;
  private boolean cacheClosed = false;
  private boolean isEdgeClient = false;
  private boolean isBridgeServer = false;
  private boolean isPRAccessor = false;
  private boolean useFilterOnExport = false;
  private boolean useFilterOnImport = false;
  private boolean useSnapshotFilter = false;
  private boolean expirationConfigured = false;

  // vmLists for coordinating stop/start between dataStoreHosts(servers) and accessors (clients)
  List killTargetVMs = null;
  List dataStoreVMs = null;
  List accessorVMs = null;

  // fields to help coordinate controller and responder
  private AtomicInteger verifyCoordinator = new AtomicInteger(0);
  private AtomicInteger startingCoordinator = new AtomicInteger(0);
  private AtomicInteger exportCoordinator = new AtomicInteger(0);
  private AtomicInteger importCoordinator = new AtomicInteger(0);
  private AtomicInteger rebalanceCoordinator = new AtomicInteger(0);
  private static HydraThreadLocal opsCompleted = new HydraThreadLocal();

  // blackboard keys
  private static final String uniqueKeyIndex = "uniqueKeyIndexForThread_";

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

  protected static final String getCallbackPrefix = "Get originated in pid ";
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
  protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
  protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
  

  //=================================================
  // initialization methods
  
  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() throws Throwable {
    //initPdxClassLoader();
    if (testInstance == null) {
      testInstance = new SnapshotTest();
      Cache theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      Region aRegion = RegionHelper.createRegion(ConfigPrms.getRegionConfig());

      if (aRegion.getAttributes().getPoolName() != null) {
        testInstance.isEdgeClient = true;
        aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
        Log.getLogWriter().info("registered interest in ALL_KEYS for " + aRegion.getFullPath());
      }

      String bridgeConfig = ConfigPrms.getBridgeConfig();
      if (bridgeConfig != null) {
        testInstance.isBridgeServer = true;
        BridgeHelper.startBridgeServer(bridgeConfig);
      }

      RegionAttributes attrs = aRegion.getAttributes();
      if (attrs.getDataPolicy().withPartitioning()) {
        int localMaxMemory = attrs.getPartitionAttributes().getLocalMaxMemory();
        if (localMaxMemory == 0) {
          testInstance.isPRAccessor = true;
        }
      }
      if ((attrs.getEntryIdleTimeout() != ExpirationAttributes.DEFAULT) ||
          (attrs.getEntryTimeToLive() != ExpirationAttributes.DEFAULT)) {
        testInstance.expirationConfigured = true;
      }
         

      testInstance.initializeInstance();
      testInstance.allRegions.add(aRegion);
      Log.getLogWriter().info(aRegion.getFullPath() + " containing " + aRegion.size() + " entries has been created");
    }
    testInstance.initializeInstancePerThread();
  }

  /** Creates and initializes colocated regions in a peer vm
   */
  public synchronized static void HydraTask_createColocatedRegions() throws Throwable {
    //initPdxClassLoader();
    if (testInstance == null) {
      testInstance = new SnapshotTest();
      testInstance.createColocatedRegions();
      testInstance.initializeInstance();
    }
    testInstance.initializeInstancePerThread();
  }

  public void createColocatedRegions() {
    // create cache/region
    if (CacheHelper.getCache() == null) {
      Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());

      String regionConfig = ConfigPrms.getRegionConfig();
      AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
      RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
      String regionBase = rd.getRegionName();

      // override colocatedWith in the PartitionAttributes
      PartitionDescription pd = rd.getPartitionDescription();
      PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
      PartitionAttributes prAttrs = null;

      String colocatedWith = null;
      int numRegions = SnapshotPrms.getNumColocatedRegions();
      for (int i = 0; i < numRegions; i++) {
         String regionName = regionBase + "_" + (i+1);
         if (i > 0) {
            colocatedWith = regionBase + "_" + i;
            prFactory.setColocatedWith(colocatedWith);
            prAttrs = prFactory.create();
            aFactory.setPartitionAttributes(prAttrs);
         }
         Region aRegion = RegionHelper.createRegion(regionName, aFactory);
         Log.getLogWriter().info(aRegion.getFullPath() + " containing " + aRegion.size() + " entries has been created");
         testInstance.allRegions.add(aRegion);
      }
    }
  }

  /**
   * Starts the bridge server in this VM.
   */
  public static void HydraTask_startBridgeServer() {
      BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  /** Initialization for snapshot controller.
   */
  public synchronized static void HydraTask_initializeController() {
    if (testInstance == null) {
      testInstance = new SnapshotTest();
      testInstance.initializeInstance();
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, STARTING_STEP);
    }
  }

  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    uniqueKeys = SnapshotPrms.getUseUniqueKeys();
    exportStrategy = SnapshotPrms.getExportStrategy();

    // SnapshotFilter use (if used on export, disable on import and vice versa)
    useFilterOnExport = SnapshotPrms.useFilterOnExport();
    useFilterOnImport = SnapshotPrms.useFilterOnImport();
    if (useFilterOnExport || useFilterOnImport) useSnapshotFilter = true;
    if (useFilterOnExport) useFilterOnImport = false;
    if (useFilterOnImport) useFilterOnExport = false;

    restrictSnapshotOperationsTo = SnapshotPrms.getRestrictSnapshotOperationsTo();
    if (restrictSnapshotOperationsTo.equalsIgnoreCase(SnapshotPrms.PRAccessors)) {
      if (!isPRAccessor) {
        configuredForSnapshotOps = false;
      }
    } else if (restrictSnapshotOperationsTo.equalsIgnoreCase(SnapshotPrms.EdgeClients)) {
      if (!isEdgeClient) {
        configuredForSnapshotOps = false;
      }
    } else if (RemoteTestModule.getMyClientName().indexOf("killTarget") >=0) {
        configuredForSnapshotOps = false;
    }
    SnapshotBB.getBB().getSharedMap().put(SnapshotBB.shutDownAllKey, new Boolean(false));
    if (SnapshotPrms.getRegisterSerializer()) {
      Log.getLogWriter().info("Registering " + VHDataSerializer.class.getName());
      DataSerializer.register(VHDataSerializer.class);
    }
  }

  /** Initialize an instance of this test class, called for each thread
   * 
   */
  private void initializeInstancePerThread() {
    opsCompleted.set(new Boolean(false));
    int thisThreadID = RemoteTestModule.getCurrentThread().getThreadId();
    String mapKey = uniqueKeyIndex + thisThreadID;
    Object mapValue = SnapshotBB.getBB().getSharedMap().get(mapKey);
    if (mapValue == null) {
      SnapshotBB.getBB().getSharedMap().put(mapKey, thisThreadID);
    } else {
      SnapshotBB.getBB().getSharedMap().put(mapKey, mapValue);
    }
  }
  
  //=================================================
  // hydra task methods

  /** Hydra task to load the region with keys/values. This is a batched task.
   *   Load the region with SnapshotPrms.numToLoad entries.
   * 
   */
  public static void HydraTask_loadRegion() {
    initPdxClassLoader();
    int numToLoad = SnapshotPrms.getNumToLoad();
    boolean allAddsCompleted = testInstance.loadRegion(numToLoad);
    if (allAddsCompleted) {
      throw new StopSchedulingTaskOnClientOrder("Put " + numToLoad + " entries in each of " +
          testInstance.allRegions.size() + " regions");
    }
  }

  /** Reset the uniqueKeyIndex to be higher than the current blackboard counter
   *
   */
  public static void HydraTask_resetUniqueKeyIndex() {
    long max = NameBB.getBB().getSharedCounters().read(NameBB.POSITIVE_NAME_COUNTER);

    // set this threads key index to a value higher than max
    int incValue = TestHelper.getNumThreads();
    int myThreadId = RemoteTestModule.getCurrentThread().getThreadId();
    int myKeyIndexValue = myThreadId;
    while (myKeyIndexValue < max) {
      myKeyIndexValue += incValue;
    }
    Log.getLogWriter().info("POSITIVE_NAME_COUNTER = " + max + ", setting this threads uniqueKeyIndex to " + myKeyIndexValue);
    SnapshotBB.getBB().getSharedMap().put(uniqueKeyIndex + myThreadId, myKeyIndexValue);
  }

  /** Hydra task to run the snapshot controller thread. This method controls
   *   what the responder vms do. 
   * 
   */
  public static void HydraTask_snapshotController() {
    testInstance.snapshotController();
    SnapshotBB.getBB().printSharedCounters();
  }

  /** Hydra task to run the responder threads for snapshot tests. This method
   *   responds to directives from the snapshot controller. 
   */
  public static void HydraTask_snapshotResponder() throws Exception {
    initPdxClassLoader();
    testInstance.snapshotResponder();
  }

  /** Verify regions that are PRs 
   *
   */
  private static void verifyPRs() {
    StringBuffer aStr = new StringBuffer();
    for (Region aRegion: testInstance.allRegions) {
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        PartitionAttributes prAttrs = aRegion.getAttributes().getPartitionAttributes();
        int redundantCopies = prAttrs.getRedundantCopies();
 
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
          ParRegUtil.verifyPrimariesWithWait(aRegion, redundantCopies);
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

        // verify colocation
        try {
          verifyColocatedRegions(aRegion);
        } catch (Exception e){
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

  private static void verifyColocatedRegions(Region regionA) {
    Object[] keys = PartitionRegionHelper.getLocalPrimaryData(regionA).keySet().toArray();
    Map colocatedRegionsMap = PartitionRegionHelper.getColocatedRegions(regionA);
    Object[] regionNames = colocatedRegionsMap.keySet().toArray();
    Log.getLogWriter().info("Verifying PR " + regionA.getFullPath() + " with " + colocatedRegionsMap.size() + " colocated regions");
    for (Object regionName: regionNames) {
      Region regionB = (Region)colocatedRegionsMap.get(regionName);
      for (Object aKey: keys) {
        DistributedMember primaryA = PartitionRegionHelper.getPrimaryMemberForKey(regionA, aKey);
        DistributedMember primaryB = PartitionRegionHelper.getPrimaryMemberForKey(regionB, aKey);
        if ((primaryB != null) && !(primaryA.equals(primaryB))) {
          throw new TestException("verifyColocatedRegions reports that primary for " + aKey + " is " + primaryA + " for " + regionA.getName() + ", but the primary for the same entry in " + regionB.getName() + " is " + primaryB);
        } 
      }
      Log.getLogWriter().info("Done verifying consistency in colocated regions");
    }
  }

  //=================================================
  // methods to do the work of hydra tasks

  /** This is run by one vm to control responder vms for snapshot tests.
   *   This directs the responder vms to
   *      1) execute random operations
   *      2) export region to disk snapshot
   *      3) stop and restart all vms
   *      4) import from disk snapshot
   *      5) direct all vms to verify their contents against BB to ensure export/import ran correctly.
   *      6) repeat (start at step 2)
   *   All steps are batched (if possible) to allow this test to scale up.
   */
  private void snapshotController() {
    // initialize
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    String currentTaskStep = (String)(SnapshotBB.getBB().getSharedMap().get(SnapshotBB.taskStep));
    Log.getLogWriter().info("In snapshotController, currentTaskStep is " + currentTaskStep);
    int numTestManagedLocatorThreads = 0;
    if (!TestConfig.tab().booleanAt(hydra.Prms.manageLocatorAgents)) { // have test managed locators
      numTestManagedLocatorThreads = 2; // todo lynn calculate this rather than hard code; this is used only for cli tests
    }

    if (currentTaskStep.equals(STARTING_STEP)) {
      // initialize
      logExecutionNumber();
      checkForLastIteration();

      // wait for worker threads to signal they completed starting step
       TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneStartingCounter", SnapshotBB.doneStartingCounter, 
          TestHelper.getNumThreads()-1-numTestManagedLocatorThreads, true, -1, 1000);

      // clear counters for next round of execution
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.doneExecutingOpsCounter);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.doneExportingCounter);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.doneImportingCounter);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.doneVerifyingCounter);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.exportLeader);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.importLeader);
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.rebalanceLeader);
      SnapshotBB.getBB().printSharedCounters();

      // change step to execute ops
      currentTaskStep = EXECUTE_OPS_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);
      // signal worker threads to continue
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneStartingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    if (currentTaskStep.equals(EXECUTE_OPS_STEP)) {
      // wait for operations to complete; responder threads are done when all 
      // responder threads report they are done
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneExecutingOpsCounter", SnapshotBB.doneExecutingOpsCounter, 
          TestHelper.getNumThreads()-1-numTestManagedLocatorThreads, true, -1, 10000);

      // wait for silence, just to be safe
      SummaryLogListener.waitForSilence(30, 1000);

      currentTaskStep = EXPORT_STEP;
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.doneStartingCounter);
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);
      // signal worker threads to continue
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneExecutingOpsCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control export
    if (currentTaskStep.equals(EXPORT_STEP)) {

      // for ha testing, we will have an extra vm to target for kill during export
      List vmInfoList = StopStartVMs.getAllVMs();
      int myVmID = RemoteTestModule.getMyVmid();
   
      for (Iterator it = vmInfoList.iterator(); it.hasNext();) {
        ClientVmInfo info = (ClientVmInfo)it.next();
        if (info.getVmid().intValue() == myVmID) {       // don't target the current vm
          vmInfoList.remove(info);
        }
      }

      // HA tests will have a VM named killTarget.  If not present, getMatchVMs returns an empty list
      killTargetVMs = StopStartVMs.getMatchVMs(vmInfoList, "killTarget");
      List stopMode = new ArrayList();
      stopMode.add(ClientVmMgr.MeanKill);
      StopStartVMs.stopVMs(killTargetVMs, stopMode); // stop the target vm

      dataStoreVMs = StopStartVMs.getMatchVMs(vmInfoList, "dataStore"); // replicate or PR peers, servers
      accessorVMs = StopStartVMs.getMatchVMs(vmInfoList, "accessor");   // PRAccessor or edge client

      // we always have to wait for a worker threads to write the region snapshot
      // but we can't wait for all (in the case of HA tests)
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
                                "SnapshotBB.doneExportingCounter", SnapshotBB.doneExportingCounter, 
                                1, true, -1, 5000);

      currentTaskStep = RECYCLE_STEP;
      // We will be shutting down all the VMs hosting the region, so with persistent partitioned
      // regions, we can expect some PartitionOfflineExceptions (with concurrent ops)
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.expectOfflineExceptionKey, new Boolean(true));

      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);

      if (exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCmdLineTool)) {
        List vmList = shutDownAll();
        util.PersistenceUtil.doOfflineExport();
        StopStartVMs.startVMs(vmList);
      } 
      return; // start next step with fresh maxResultWaitSec
    }

    // control stop/start
    if (currentTaskStep.equals(RECYCLE_STEP)) {

      // we will want to execute rebalance during import again, so clear rebalanceLeader counter
      SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.rebalanceLeader);

      // see if it's time to stop the test
      long counter = SnapshotBB.getBB().getSharedCounters().read(SnapshotBB.timeToStop);
      if (counter >= 1) {
        throw new StopSchedulingOrder("Num controller executions is " +
            SnapshotBB.getBB().getSharedCounters().read(SnapshotBB.executionNumber));
      }

      if (!exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCmdLineTool)) {
        StopStartVMs.stopVMs(accessorVMs, getStopModes(accessorVMs)); // stop the target vm
        StopStartVMs.stopVMs(dataStoreVMs, getStopModes(dataStoreVMs)); // stop the target vm

        // start all data hosts together (avoid PersistentDataOfflineExceptions)
        for (Iterator it=killTargetVMs.iterator(); it.hasNext();) {
          dataStoreVMs.add((ClientVmInfo)it.next());
        }
        StopStartVMs.startVMs(dataStoreVMs);    // start the dataStore (or servers) first
        StopStartVMs.startVMs(accessorVMs);    // accessors (or edgeClients) next
      }

      // done with recycling caching vms
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.expectOfflineExceptionKey, new Boolean(false));

      // on restart snapshotResponders will recreate empty regions
      // Now that they are up, we need to import the data according to the exportStrategy
      currentTaskStep = IMPORT_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);
      return; // start next step with fresh maxResultWaitSec
    }

    // control import
    if (currentTaskStep.equals(IMPORT_STEP)) {
      // import step is done when all other vms report completion of import
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneImportingCounter", SnapshotBB.doneImportingCounter, 
          TestHelper.getNumThreads()-1-numTestManagedLocatorThreads, true, -1, 5000);

      currentTaskStep = VERIFY_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneImportingCounter);
      return; // start next step with fresh maxResultWaitSec
    }

    // control verify
    if (currentTaskStep.equals(VERIFY_STEP)) {
      // verify step is done when all other threads report completion of verification step
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneVerifyingCounter", SnapshotBB.doneVerifyingCounter, 
          TestHelper.getNumThreads()-1-numTestManagedLocatorThreads, true, -1, 5000);

      // return to STARTING_STEP
      currentTaskStep = STARTING_STEP;
      Log.getLogWriter().info("Changing currentTaskStep to " + currentTaskStep);
      SnapshotBB.getBB().getSharedMap().put(SnapshotBB.taskStep, currentTaskStep);
      // signal worker threads to continue
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneVerifyingCounter);
      return; // start next step with fresh maxResultWaitSec
    }
  }

  /** Task to create a list of stopModes to match the given vmList
   *  
   */
  private List getStopModes(List vmList) {
    List returnList = new ArrayList();
    for (Iterator it = vmList.iterator(); it.hasNext(); it.next()) {
      String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
      returnList.add(choice);
    }
    return returnList;
  }

  /** Task to do a shutDownAll on the worker vms
   *  
   */
  public static List shutDownAll() {
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    if (adminDS == null) {
      throw new TestException("Test is configured to use shutDownAllMembers, but this vm must be an admin vm to use it");
    }
    SnapshotBB.getBB().getSharedMap().put(SnapshotBB.shutDownAllKey, new Boolean(true)); // mark that shutdown all is occurring
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS);  // shutdown with nice_exit to allow CLOSETASK to remove disk files
    SnapshotBB.getBB().getSharedMap().put(SnapshotBB.shutDownAllKey, new Boolean(false)); // mark that shutdown all has completed
    List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
    Set shutDownAllResults = (Set)(tmp[1]);
    if (shutDownAllResults.size() != vmList.size()) { // shutDownAll did not return the expected number of members

      if (SnapshotPrms.executeConcurrentRebalance()) {
        // do nothing, we concurrent rebalance disconnects one member (so we may have closed the cache in one vm)
      } else {
        throw new TestException("Expected shutDownAllMembers to return " + vmList.size() +
          " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
      }
    }
    return vmList;
  }

  /** Initialize import and verify coordinators as these steps occur after
   *  the responder vms have been recycled.
   *
   *  Verify no events processed during import
   */
  public synchronized static void HydraTask_initializeResponder() {
    testInstance.initializeResponder();
  }

  private void initializeResponder() {
     for (Region aRegion: allRegions) {
       aRegion.getAttributesMutator().addCacheListener(new ImportListener());
       aRegion.getAttributesMutator().setCacheWriter(new ImportWriter());
       Log.getLogWriter().info("After adding ImportListener and ImportWriter via AttributesMutator, Region = " + TestHelper.regionToString(aRegion, true));
     }
  }

  /** Responds to directives issued by the snapshot controller
   * 
   */
  private void snapshotResponder() throws Exception {
    String currentTaskStep = (String)(SnapshotBB.getBB().getSharedMap().get(SnapshotBB.taskStep));
    Log.getLogWriter().info("In snapshotResponder, currentTaskStep is " + currentTaskStep);
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    int numTestManagedLocatorThreads = 0;
    if (!TestConfig.tab().booleanAt(hydra.Prms.manageLocatorAgents)) { // have test managed locators
      numTestManagedLocatorThreads = 2; // todo lynn calculate this rather than hard code; this is used only for cli tests
    }

    if (currentTaskStep.equals(STARTING_STEP)) {
      if (startingCoordinator.incrementAndGet() == 1) { // only do this once per vm
        Log.getLogWriter().info("Initializing for startup...");
        // initialize these booleans which control rebalancing and closing the cache in 1 VM during rebalance
        leaderInThisVM = false;
        cacheClosed = false;
      } 
      // wait for all worker threads and controller
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneStartingCounter);
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneStartingCounter", SnapshotBB.doneStartingCounter, 
          TestHelper.getNumThreads()-numTestManagedLocatorThreads, true, -1, 1000);
      return;
    }

    if (currentTaskStep.equals(EXECUTE_OPS_STEP)) {
      if (!(Boolean)opsCompleted.get()) {
        doOperations(minTaskGranularityMS);
        opsCompleted.set(new Boolean(true));
      }

      // wait for all worker threads to complete ops and controller to wait for silence
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneExecutingOpsCounter);
      TestHelper.waitForCounter(SnapshotBB.getBB(), "SnapshotBB.doneExecutingOpsCounter", 
          SnapshotBB.doneExecutingOpsCounter, 
          TestHelper.getNumThreads()-numTestManagedLocatorThreads, true, -1, 10000);
      return;
    } 

    if (currentTaskStep.equals(EXPORT_STEP)) {
      // Some tests restrict which members can do the snapshot operations (export/import)
      // - PRAccessor only
      // - edgeClient only
      if (configuredForSnapshotOps && exportCoordinator.incrementAndGet() == 1) { // only do this once (for exportStrategy.apiFromOneVm)
        long leader = SnapshotBB.getBB().getSharedCounters().incrementAndRead(SnapshotBB.exportLeader);
        if (leader == 1) { // this is the thread to write the snapshot
          leaderInThisVM = true;
          Log.getLogWriter().info("This thread is the leader; it will write the snapshot");
          // todo@lhughes - remove the PdxClassPath (see code in testVersions/version2/ Loader)
          writeSnapshot();  // writes current region contents to blackboard
          if (exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCmdLineTool)) {
            Log.getLogWriter().info("Controller is executing export via the command line tool");
          } else {
            doExportStep();
          }
          SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneExportingCounter);
        }
      } else {  // since the next step is to recycle, we can freely execute concurrent ops or rebalance here
        if (SnapshotPrms.executeConcurrentOps()) {
          doOperations(minTaskGranularityMS);
        } else if (SnapshotPrms.executeConcurrentRebalance()) {
          MasterController.sleepForMs(10);   // allow time leaderInThisVM to be set
          if (!leaderInThisVM && (rebalanceCoordinator.incrementAndGet() == 1)) { 
            long leader = SnapshotBB.getBB().getSharedCounters().incrementAndRead(SnapshotBB.rebalanceLeader);
            if (leader == 1) { 
              // 1 thread in 1 VM can destroy the region, close the cache or disconnect from the DS
              // quickly creating work for the rebalance to handle
              cacheClosed = true;
              CacheHelper.closeCache();
            }
          } else { 
            try {
              doRebalance();
            } catch (TestException e) {
              boolean undergoingShutDownAll = (Boolean)SnapshotBB.getBB().getSharedMap().get(SnapshotBB.shutDownAllKey);
              if (undergoingShutDownAll || cacheClosed) {
                // do nothing, we expect this
              } else {
                throw new TestException(TestHelper.getStackTrace(e));
              }
            } catch (Exception e) {
              handleRebalanceException(e);
            }
          }
        }
      }
      TestHelper.waitForCounter(SnapshotBB.getBB(), "SnapshotBB.doneExportingCounter", 
             SnapshotBB.doneExportingCounter, 1, true, -1, 5000);
      return;
    } 

   if (currentTaskStep.equals(RECYCLE_STEP)) { // step handled by controller, not responder
      leaderInThisVM = false;  // reset this for future use in import
      MasterController.sleepForMs(5000); 
      return;
    } 

    if (currentTaskStep.equals(IMPORT_STEP)) { // determined by exportStrategy!
      if (configuredForSnapshotOps && importCoordinator.incrementAndGet() == 1) { // only do this once per vm
        long leader = SnapshotBB.getBB().getSharedCounters().incrementAndRead(SnapshotBB.importLeader);
        if (leader == 1) { // this is the thread to re-instate the snapshot
          leaderInThisVM = true;
          Log.getLogWriter().info("This thread is the leader; it will import from the snapshot");
          // select SnapshotReader.read() vs. RegionSnapshotService.load() with apis (10% of the time)
          boolean useSnapshotReader = !(useFilterOnImport) && (TestConfig.tab().getRandGen().nextInt(1, 100) < 10);
          doImportStep(useSnapshotReader);
          long numEvents = SnapshotBB.getBB().getSharedCounters().read(SnapshotBB.eventsDuringImport);
          if (!(SnapshotPrms.executeConcurrentOps() || expirationConfigured || useSnapshotReader) && (numEvents > 0)) {
            Log.getLogWriter().info("CacheListener and/or CacheWriter callbacks invoked during import: " + numEvents + " processed");
            throw new TestException("CacheListener and/or CacheWriter callbacks invoked during import: " + numEvents + " processed");
          } else {  // clear counter after SnapshotReader invocations
            SnapshotBB.getBB().getSharedCounters().zero(SnapshotBB.eventsDuringImport);
          }
        } 
      } 

      // wait for import to complete and all worker threads to sync up, then 
      // remove the import Writer and Listener
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneImportingCounter);
      TestHelper.waitForCounter(SnapshotBB.getBB(), "SnapshotBB.doneImportingCounter", 
             SnapshotBB.doneImportingCounter, TestHelper.getNumThreads()-1-numTestManagedLocatorThreads, false, -1, 5000);

      // remove ImportListener, ImportWriter (without disturbing SummaryLogListener)
      for (Region aRegion: allRegions) {
        AttributesMutator mutator = aRegion.getAttributesMutator();
        CacheListener[] listeners = aRegion.getAttributes().getCacheListeners();
        for (CacheListener l: listeners) {
          if (l instanceof ImportListener) {
            mutator.removeCacheListener(l);
          }
        }
        mutator.setCacheWriter(null);
       Log.getLogWriter().info("After removing ImportListener and ImportWriter via AttributesMutator, Region = " + TestHelper.regionToString(aRegion, true));
      }

      // everyone needs to wait for the controller before moving to verifying
      // note that threads may arrive late (after controller has updated, so exact = false
      TestHelper.waitForCounter(SnapshotBB.getBB(), "SnapshotBB.doneImportingCounter", 
             SnapshotBB.doneImportingCounter, TestHelper.getNumThreads()-numTestManagedLocatorThreads, false, -1, 5000);
      return;
    } 

    if (currentTaskStep.equals(VERIFY_STEP)) {
      if (verifyCoordinator.incrementAndGet() == 1) { // only do this once per vm
        doVerificationStep();
      } 
      // wait for all worker and controller vms
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.doneVerifyingCounter);
      TestHelper.waitForCounter(SnapshotBB.getBB(), 
          "SnapshotBB.doneVerifyingCounter", SnapshotBB.doneVerifyingCounter, 
          TestHelper.getNumThreads()-numTestManagedLocatorThreads, true, -1, 3000);
      return;
    }
  }

  /** Randomly select one of the SnapshotFilters (Key, Value or ValueType)
   *  Returns instance of SnapshotFilter.
   */
  protected SnapshotFilter getSnapshotFilter() {
    int i = TestConfig.tab().getRandGen().nextInt(0, 2);
    SnapshotFilter filter = new KeySnapshotFilter();
    if (i == 2) {
      filter = new ValueSnapshotFilter();
    } else if (i == 3) {
      filter = new ValueTypeSnapshotFilter();
    }
    Log.getLogWriter().info("getSnapshotFilter returning instance of " + filter.getClass().getName());
    return filter;
  }

  /**
   * Key SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the key (FilterObject_XXX vs. Object_XXX)
   */
  public static class KeySnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = false;
      String key = (String)entry.getKey();
      if (key.startsWith(NameFactory.OBJECT_NAME_PREFIX)) {
        accepted = true;
      }
      return accepted;
    }
  }

  /**
   * Value SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the value (value should NOT start with this string "object to be filtered").
   */
  public static class ValueSnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = true;
      Object value = entry.getValue();
     
      if ((value instanceof String) && (((String)value).startsWith("object to be filtered"))) {
        accepted = false;
      }
      return accepted;
    }
  }

  /**
   * Value Type SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the value type (ValueHolders are accepted, String values are not).
   */
  public static class ValueTypeSnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = false;
      Object value = entry.getValue();
      
      if (value instanceof ValueHolder) {
        accepted = true;
      }
      return accepted;
    }
  }

  /** Invoke export, right now just use a simple file name ... one per snapshot
   *
   */
  private void doExportStep() {
    // obtain a snapshot (for each region separately)
    for (Region aRegion: testInstance.allRegions) {
      // create some entries to filter out later (regardless of whether filtering is on import or export)
      if (useSnapshotFilter) {  
        int numToCreate = SnapshotPrms.numFilterObjects();
        for (int i=1; i <=numToCreate; i++) {
          String key = "FilterObject_" + i;
          String value = "object to be filtered via snapshot.save() or snapshot.load(): this should never be a value in the cache once snapshot restored";
          aRegion.put(key, value);
        }
        Log.getLogWriter().info("Wrote " + numToCreate + " FilterObject entries to " + aRegion.getFullPath());
      }
      RegionSnapshotService snapshotService = aRegion.getSnapshotService();
      String currDirName = System.getProperty("user.dir");

      String snapshotDirName = snapshotDirPrefix + "_" + RemoteTestModule.getMyVmid();
      snapshotDirName = currDirName + File.separator + snapshotDirName;
      FileUtil.mkdir(snapshotDirName);
      String filename = "snapshot-vm_" + RemoteTestModule.getMyVmid() + "_" + RemoteTestModule.getMyClientName() + aRegion.getFullPath().replace('/', '-')
          + ".gfd";
      File snapshotFile = new File(snapshotDirName, filename);

      long startTime = System.currentTimeMillis();
      if (exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCmdLineTool)) {
         // export the snapshot, every region in the cache will be exported from the offline persistence files
         // do nothing (this is done directly in the snapshotController code)
      } else if (exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingApiFromAllVms)) {
         throw new TestException("Parallel snapshot option not supported in GemFire 7.0");
/*
         SnapshotOptions options = snapshotService.createOptions();
         options.setParallelMode(true);
         try {
            snapshotService.save(snapshotFile, SnapshotFormat.GEMFIRE, options);
         } catch (IOException ioe) {
           throw new TestException("Caught " + ioe + " while exporting region snapshot to " + snapshotFile.getAbsoluteFile() + " " + TestHelper.getStackTrace(ioe));
         }
*/
      } else {  
         // exportUsingApiFromOneVm or exportUsingCli
         Log.getLogWriter().info("Starting export of " + aRegion.getFullPath() + " containing " + aRegion.size() + " entries to " + snapshotFile.getAbsoluteFile());
         SnapshotOptions options = snapshotService.createOptions();
         if (useFilterOnExport) {
           options.setFilter(getSnapshotFilter());
         }
         try {
           if (exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCli)) {
             String command = "export data --region=" + aRegion.getFullPath() + " --file=" +
                    snapshotFile.getAbsolutePath() + " --member=" + DistributedSystemHelper.getDistributedSystem().getDistributedMember().getName();
             CliHelper.execCommandOnRemoteCli(command, true);
           } else {
             Log.getLogWriter().info("Calling snapshotService.save(...)");
             snapshotService.save(snapshotFile, SnapshotOptions.SnapshotFormat.GEMFIRE, options);
           }
         } catch (IOException ioe) {
           String errStr = ioe.toString();
           boolean memberDepartedEvent = (errStr.indexOf("memberDeparted event") >=0);
           if (memberDepartedEvent) {  // retry
             Log.getLogWriter().info("Caught " + ioe + ", retrying export");
             try {
               snapshotService.save(snapshotFile, SnapshotOptions.SnapshotFormat.GEMFIRE, options);
             } catch (IOException e) {
               throw new TestException("Caught " + e + " on retry of export region snapshot to " + snapshotFile.getAbsoluteFile() + " " + TestHelper.getStackTrace(ioe));
             }
           } else {
             throw new TestException("Caught " + ioe + " while exporting region snapshot to " + snapshotFile.getAbsoluteFile() + " " + TestHelper.getStackTrace(ioe));
           }
         }
      }
 
      long endTime = System.currentTimeMillis();
      Log.getLogWriter().info("Export of " + aRegion.getFullPath() + " containing " + aRegion.size() + " entries to " + snapshotFile.getAbsoluteFile() + " took " + (endTime - startTime) + " ms"); 
    }
  }

  /** Invoke import
   *
   */
  private void doImportStep(boolean useSnapshotReader) {
    // obtain a snapshot
 
    String currDirName = System.getProperty("user.dir");
    File snapshotFile = null;

    // import each region individually
    for (Region aRegion: testInstance.allRegions) {
      boolean isPartitioned = aRegion.getAttributes().getDataPolicy().withPartitioning();
      boolean isOfflineExport = exportStrategy.equalsIgnoreCase(SnapshotPrms.exportUsingCmdLineTool);
      String prefix = snapshotDirPrefix;

      // For partitioned regions exported via the command line tool, we must import from each diskStore
      // for all other cases, we only need to import from 1 snapshot
      List dirList = getSnapshotDirs(new File(currDirName), prefix);
      if (!(isOfflineExport && isPartitioned)) {
        dirList = getOneSnapshotDir(dirList, aRegion);
      }

      for (Iterator d = dirList.iterator(); d.hasNext();) {
        File aDir = (File)d.next(); 

        // For each file in snapshotDir which matches our region name
        List fileList = getSnapshotFiles(aDir, aRegion);
        for (Iterator i = fileList.iterator(); i.hasNext();) {
          snapshotFile = (File)i.next();
  
          if (useSnapshotReader) {
            long startTime = System.currentTimeMillis();
            Log.getLogWriter().info("Starting SnapshotReader.read() of " + snapshotFile.getAbsoluteFile() + " into " + aRegion.getFullPath());
            try {
              for (SnapshotIterator it = SnapshotReader.read(snapshotFile); it.hasNext();) {
                Map.Entry entry = (Map.Entry)it.next();
                aRegion.putIfAbsent(entry.getKey(), entry.getValue());
              }
              long endTime = System.currentTimeMillis();
              Log.getLogWriter().info("SnapshotReader.read() of " + aRegion.getFullPath() + " from " + snapshotFile.getAbsoluteFile() + " took " + (endTime - startTime) + " ms.  Region contains " + aRegion.size() + " entries"); 
            } catch (Exception e) {
              throw new TestException("Caught " + e + " while using SnapshotReader.read() to import region snapshot from " + snapshotFile.getAbsoluteFile() + " " + TestHelper.getStackTrace(e));
            }
          } else {
            RegionSnapshotService snapshotService = aRegion.getSnapshotService();
            Log.getLogWriter().info("Starting RegionSnapshotService.load() from " + snapshotFile.getAbsoluteFile() + " into " + aRegion.getFullPath());
            SnapshotOptions options = snapshotService.createOptions();
            if (useFilterOnImport) {
              options.setFilter(getSnapshotFilter()); 
            }
            long startTime = System.currentTimeMillis();
            try {
              // import the snapshot file, updates any existing entries in the region
              if (CliHelperPrms.getUseCli()) {
                String command = "import data --region=" + aRegion.getFullPath() + " --file=" + snapshotFile.getAbsolutePath() +
                    " --member=" + DistributedSystemHelper.getDistributedSystem().getDistributedMember().getName();
                CliHelper.execCommandOnRemoteCli(command, true);
              } else {
                snapshotService.load(snapshotFile, SnapshotOptions.SnapshotFormat.GEMFIRE, options);
              }
              long endTime = System.currentTimeMillis();
              Log.getLogWriter().info("Import of " + aRegion.getFullPath() + " from " + snapshotFile.getAbsoluteFile() + " took " + (endTime - startTime) + " ms.  Region contains " + aRegion.size() + " entries"); 
            } catch (Exception e) {   // IOException or ClassCastException
              throw new TestException("Caught " + e + " while importing region snapshot from " + snapshotFile.getAbsoluteFile() + " " + TestHelper.getStackTrace(e));
            }
          }
          // remove diskSnapshot once imported
          boolean result = snapshotFile.delete();
          if (!result) {
            throw new TestException("Unable to delete file " + snapshotFile.getAbsolutePath());
          }
          Log.getLogWriter().info("Removed " + snapshotFile.getAbsolutePath());
        }
      }
    }
  }

  /**
   *  Given a list of potential snapshot locations (snapshotDir_<vmId>) and a region
   *  whose snapshot we're interested in, return a list containing a single snapshotDir
   *  which contains snapshot files for the given region (avoiding empty snapshotDirs)
   */
  private static List<File> getOneSnapshotDir(List<File> dirList, Region aRegion) {
    List oneDirList = new ArrayList();
    for (Iterator it = dirList.iterator(); it.hasNext();) {
      File dir = (File)it.next();
      List<File> fileList = getSnapshotFiles(dir, aRegion);
      if (fileList.size() > 0) {
        oneDirList.add(dir);
        return oneDirList;
      }
    }
    throw new TestException("No snapshotDir could be found for region " + aRegion.getFullPath() + " in " + dirList.toString());
  }

  /**
   *  Returns a list of all subdirectories of the specified directory whose
   *  names start with the given prefix
   */
  private static List<File> getSnapshotDirs(File aDir, String prefix) {
    class DirFilter implements FileFilter {
      String prefix;
      public DirFilter(String prefix) {
        this.prefix = prefix;
      }
      public boolean accept(File fn) {
       return fn.isDirectory() && fn.getName().startsWith(prefix);
      }
    }
    return FileUtil.getFiles(aDir, new DirFilter(prefix), false);
  }

  /** FileFilter to pick up exported (snapshot) files in given snapshotDir
   *  Filenames start with snapshot and end with fully qualified regionName (with '-' instead of '/')
   */
  private static List<File> getSnapshotFiles(File snapshotDir, Region aRegion) {
    class RegionFilter implements FileFilter {
      String regionName;

      public RegionFilter(Region aRegion) {
        regionName = aRegion.getFullPath().replace('/', '-');
      }

      public boolean accept(File f) {
        String fn = f.getName();
        Log.getLogWriter().info("RegionFilter comparing against filename " + f.getName() + " and regionName " + regionName);
        if (fn.startsWith("snapshot") && fn.endsWith(regionName + ".gfd")) {
          return true;
        }
        return false;
      }
    }
    return FileUtil.getFiles(snapshotDir, new RegionFilter(aRegion), false);
  }

  /** Verify that all expeted keys are present.
   *  Note that this step is skipped if executeConcurrentOps = true or 
   *  if TTL or IdleTimeout (expiration) is configured.
   */
  private void doVerificationStep() {
    if (!(SnapshotPrms.executeConcurrentOps() || expirationConfigured)) {
      verifyPRs();
      verifyFromSnapshot();
    } else {
      Log.getLogWriter().info("Not verifying region contents with concurrentOperations or expiration");
    }
  }

  /** Add SnapshotPrms.numToLoad entries into the test region.
   *  This batched task can be invoked by several threads to accomplish the work.
   *
   */
  private boolean loadRegion(long numKeysToLoad) {
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
      if (nameCounter <= numKeysToLoad) {
        for (Region aRegion: allRegions) {
          aRegion.put(key, getValueForKey(key));
          if (System.currentTimeMillis() - lastLogTime > logIntervalMS) {
            Log.getLogWriter().info("Created " + numCreated + " entries per region so far this task; number remaining to create " + (numKeysToLoad - nameCounter));
            lastLogTime = System.currentTimeMillis();
          }
        }
      } else {
        return true;
      }
    }
    return false;
  }

  /** Record the region snapshot to the BB
   *
   */
  void writeSnapshot() {

    Log.getLogWriter().info("Preparing to write snapshot for " + allRegions.size() + " regions");
    Map allRegionsSnapshot = new HashMap();
    for (Region aRegion: allRegions) {
      Map regionSnapshot = new HashMap();
      for (Object key: aRegion.keySet()) {
        Object value = null;
        if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
          value = aRegion.get(key);
        }
        if (value instanceof BaseValueHolder) {
          regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
        } else if (instanceOfPdxInstance(value)) {
          BaseValueHolder vh = toValueHolder(value);
          regionSnapshot.put(key, vh.myValue);
        } else {
          regionSnapshot.put(key, value);
        }
      }
      allRegionsSnapshot.put(aRegion.getFullPath(), regionSnapshot);
      Log.getLogWriter().info("Region snapshot for " + aRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    } 
    SnapshotBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    Log.getLogWriter().info("Put snapshot for " + allRegions.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm.
   *
   */
  private void verifyFromSnapshot() {
    Cache theCache = CacheHelper.getCache();
    Map<String, Map> allRegionsSnapshot = (Map)(SnapshotBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
    Set snapshotRegionNames = allRegionsSnapshot.keySet();

    // First check to see that all regions are defined in both the snapshot and the cache
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
          } else if (instanceOfPdxInstance(actualValue)) {
            actualValue = toValueHolder(actualValue);
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

  /** do a rebalance operation 
   *
   */
  protected void doRebalance() {

    // VM should have already created PR via initialization task
    Cache myCache = CacheHelper.getCache();
    if (myCache == null) {
      throw new TestException("doRebalance() expects hydra client to have created cache and PR via initialization tasks");
    }
    Log.getLogWriter().info("In doRebalance ... rebalancing PRs in the cache");

    ResourceManager rm = myCache.getResourceManager();
    RebalanceFactory factory = rm.createRebalanceFactory();
    long startTime = System.currentTimeMillis();
    RebalanceOperation rebalanceOp = factory.start();

    // always wait for results (even on killTarget and cancel)
    RebalanceResults rebalanceResults = null;
    try {
       rebalanceResults = rebalanceOp.getResults();
    } catch (InterruptedException ie) {
       Log.getLogWriter().info("getResults() caught exception " + ie);
       throw new TestException("Unexpected exception " + TestHelper.getStackTrace(ie));
    }
    Long stopTime = new Long(System.currentTimeMillis());
    Long rebalanceTime = stopTime - startTime;
    Log.getLogWriter().info("Completed rebalance in " + rebalanceTime + " ms, results = " + rebalanceOp.toString());
    Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "rebalance"));
  }

  /** Do random operations on the cache and any defined regions in this vm.
   * 
   *   @param msToRun The number of milliseconds to run operations before returning.
   */
  protected void doOperations(long msToRun) throws Exception {
    Log.getLogWriter().info("In doOperations, running for " + msToRun + " seconds");
    long startTime = System.currentTimeMillis();
    int numOps = 0;
    Region aRegion = null;
    do {
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
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
        }
        numOps++;
        Log.getLogWriter().info("Completed op " + numOps + " for this task");
      } catch (Exception e)      {
        // handle Exceptions that happen when we kill vms (e.g. shutDownAll, nice_kill)
        handleOperationExceptions(aRegion, e);  
      }
    } while ((System.currentTimeMillis() - startTime < msToRun));
    Log.getLogWriter().info("Done in doOperations");
  }

  /** Get an operation to perform on the region
   *  @param reg The region to get an operation for. 
   *  @returns A random operation.
   */
  protected int getOperation(Region aRegion) {
    Long opsPrm = SnapshotPrms.operations;
    Long upperThresholdOpsPrm = SnapshotPrms.upperThresholdOperations;
    Long lowerThresholdOpsPrm = SnapshotPrms.lowerThresholdOperations;
    int upperThreshold = SnapshotPrms.getUpperThreshold();
    int lowerThreshold = SnapshotPrms.getLowerThreshold();
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
        try {
          aRegion.create(key, anObj, callback);
          Log.getLogWriter().info("addEntry: done creating key " + key);
        } catch (EntryExistsException e) {
          Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
               // concurrentOps with import could generate some EntryExistsExceptions
        }
      } else { // use create with no cacheWriter arg
        Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
            TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
        try {
          aRegion.create(key, anObj);
          Log.getLogWriter().info("addEntry: done creating key " + key);
        } catch (EntryExistsException e) {
          Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
               // concurrentOps with import could generate some EntryExistsExceptions
        }
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
    int numPutAllNewKeys = SnapshotPrms.getNumPutAllNewKeys();

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
    int numPutAllExistingKeys = TestConfig.tab().intAt(SnapshotPrms.numPutAllExistingKeys);
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

  /** Get a new key in the given region.
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

  //=================================================
  // other methods

  /** Log the execution number of this serial task.
   */
  static protected long  logExecutionNumber() {
    long exeNum = SnapshotBB.getBB().getSharedCounters().incrementAndRead(SnapshotBB.executionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    return exeNum;
  }

  /** Check if we have run for the desired length of time. We cannot use
   *  hydra's taskTimeSec parameter because of a small window of opportunity
   *  for the test to hang due to the test's "concurrent round robin" type
   *  of strategy. Here we set a blackboard counter if time is up and this
   *  is the last concurrent round.
   */
  protected static void checkForLastIteration() {
    // determine if this is the last iteration
    int secondsToRun = SnapshotPrms.getSecondsToRun();
    long taskStartTime = 0;
    final String bbKey = "taskStartTime";
    Object anObj = SnapshotBB.getBB().getSharedMap().get(bbKey);
    if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      SnapshotBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
    } else {
      taskStartTime = ((Long)anObj).longValue();
    }
    if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.timeToStop);
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
      int anInt = (Integer)(SnapshotBB.getBB().getSharedMap().get(uniqueKeyIndex + thisThreadID));
      anInt += TestHelper.getNumThreads();
      SnapshotBB.getBB().getSharedMap().put(uniqueKeyIndex + thisThreadID, anInt);
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
    String valueClassName = SnapshotPrms.getValueClassName();
    BaseValueHolder vh = null;
    if (valueClassName.equals(ValueHolder.class.getName())) {
      vh = new ValueHolder((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializable.class.getName())) {
      vh = new VHDataSerializable((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializableInstantiator.class.getName())) {
      vh = new VHDataSerializableInstantiator((String)key, randomValues);
    } else if ((valueClassName.equals("util.PdxVersionedValueHolder")) ||
               (valueClassName.equals("util.VersionedValueHolder"))) {
      vh = getVersionedValueHolder(valueClassName, (String)key, randomValues);
      
      return vh; // do not allow this method the option of returning the extraObject (below)
    } else {
      throw new TestException("Test does not support SnapshotPrms.valueClassName " + valueClassName);
    }

    // Value Type SnapshotFilter depends on original (non-filtered) test objects being ValueHolders.
    if (useSnapshotFilter) {
      return vh;
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

  /** Handle Exceptions thrown during rebalance (which occur from closing the cache, recycling vms)
   *
   *  @param - Exception e
   */
  protected void handleRebalanceException(Exception e) {

    boolean undergoingShutDownAll = (Boolean)SnapshotBB.getBB().getSharedMap().get(SnapshotBB.shutDownAllKey);
    if (undergoingShutDownAll || cacheClosed) {
      // do nothing, we expect this
    } else if ((e instanceof PartitionOfflineException) ||
              ((e instanceof ServerOperationException) && (e.getCause() instanceof PartitionOfflineException))) {
      boolean expectOfflineException = false;
      Object bbValue = SnapshotBB.getBB().getSharedMap().get(SnapshotBB.expectOfflineExceptionKey);
      if (bbValue instanceof Boolean) {
        expectOfflineException = (Boolean)bbValue;
      }

      if (expectOfflineException) {
        Log.getLogWriter().info("handleRebalanceException got expected exception " + e + ", returning normally to allow a possible nice exit, continuing test");
      } else {
        throw new TestException("Unexpected PartitionOfflineException " + e + " " + TestHelper.getStackTrace(e));
      }
    } else {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Handle Exceptions which can be thrown from entry operations during Cache Close/Shutdown
   *
   *  @param - Region involved in failed operation
   *  @param - Exception e
   */
  protected void handleOperationExceptions(Region aRegion, Exception e) {

    boolean expectOfflineException = false;
    Object bbValue = SnapshotBB.getBB().getSharedMap().get(SnapshotBB.expectOfflineExceptionKey);
    if (bbValue instanceof Boolean) {
       expectOfflineException = (Boolean)bbValue;
    }

    boolean persistentPartition = false;
    DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();
    if (dataPolicy.withPartitioning() && dataPolicy.withPersistence()) {
      persistentPartition = true;
    }

    boolean undergoingShutDownAll = (Boolean)SnapshotBB.getBB().getSharedMap().get(SnapshotBB.shutDownAllKey);
    boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
    if (undergoingShutDownAll || thisVMReceivedNiceKill) {
      if ((e instanceof CacheClosedException) || 
          (e instanceof DistributedSystemDisconnectedException) ||
          (e instanceof RegionDestroyedException)) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } else if ((e instanceof PartitionOfflineException) ||
              ((e instanceof ServerOperationException) && (e.getCause() instanceof PartitionOfflineException))) {
        if (isPRAccessor || persistentPartition) {
           Log.getLogWriter().info("handleOperationException got expected exception " + e + ", returning normally to allow a possible nice exit, continuing test");
        } else {
           throw new TestException("PartitionOfflineException processed for region which was not a persistentPartition\n" + TestHelper.getStackTrace(e));
        }
        if (!expectOfflineException) {
           throw new TestException("Unexpected PartitionOfflineException " + e + " " + TestHelper.getStackTrace(e));
        } else {
           Log.getLogWriter().info("handleOperationException got expected exception " + e + ", returning normally to allow a possible nice exit, continuing test");
        }
    } else if (e instanceof PartitionedRegionStorageException) {
        if (isPRAccessor) {
           Log.getLogWriter().info("handleOperationException got expected exception " + e + ", returning normally to allow a possible nice exit, continuing test");
        } else {
           throw new TestException("Unexpected Exception " + e + " " + TestHelper.getStackTrace(e));
        }
    } else {
        throw new TestException("Unexpected Exception " + e + " " + TestHelper.getStackTrace(e));
    }
  }

  /** Close the cache gracefully (so we can remove the underlying disk store files).
   *
   */
  public static synchronized void CloseTask_closeCache( ) {
    CacheHelper.closeCache();
  }

  /** Remove disk files in disk dirs corresponding to this vm's vmID.
   *
   */
  public static synchronized void HydraTask_removeDiskFiles() {
    int myVmID = RemoteTestModule.getMyVmid();
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    File[] listing = currDir.listFiles();
    for (File aFile: listing) {
      String fileName = aFile.getName();
      if (fileName.startsWith("vm_" + myVmID + "_") &&
          (fileName.indexOf("disk_") >= 0) &&
          aFile.isDirectory()) { // found a diskDir for this vm's vmID
        File[] diskFiles = aFile.listFiles();
        for (File diskFile: diskFiles) {
          boolean result = diskFile.delete();
          if (!result) {
            throw new TestException("Unable to delete file " + diskFile.getAbsolutePath());
          }
          Log.getLogWriter().info("Removed " + diskFile.getAbsolutePath());
        }
      }
    }
  }

  /**
   * 
   */
  public static void initPdxClassLoader() {
    PdxTest.initClassLoader();
  }

  /**
   * @param valueClassName
   * @param key
   * @param randomValues
   * @return
   */
  public static BaseValueHolder getVersionedValueHolder(String valueClassName,
      String key, RandomValues randomValues) {
    return PdxTest.getVersionedValueHolder(valueClassName, key, randomValues);
  }

  /**
   * @param value
   * @return
   */
  public static boolean instanceOfPdxInstance(Object value) {
    return value instanceof PdxInstance;
  }

  /**
   * @param value
   * @return
   */
  public static BaseValueHolder toValueHolder(Object value) {
    return PdxTest.toValueHolder(value);
  }
  
}
