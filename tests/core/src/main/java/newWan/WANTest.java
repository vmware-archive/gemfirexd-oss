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

package newWan;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigHashtable;
import hydra.ConfigPrms;
import hydra.DiskStoreHelper;
import hydra.DistributedSystemHelper;
import hydra.GatewayReceiverHelper;
import hydra.GatewaySenderHelper;
import hydra.GemFireDescription;
import hydra.GsRandom;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.TestConfigFcns;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import objects.PSTObject;
import pdx.PdxTest;
import splitBrain.RegionEntryComparator;
import util.BaseValueHolder;
import util.MethodCoordinator;
import util.NameFactory;
import util.PRObserver;
import util.QueryObject;
import util.RandomValues;
import util.SilenceListener;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;
import util.ValueHolderPrms;
import wan.CacheClientPrms;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorDiscovery;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.ha.QueueRemovalMessage;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.pdx.PdxInstance;

import cq.CQUtil;
import cq.CQUtilBB;
import diskReg.DiskRegUtil;
import durableClients.DurableClientsBB;

/**
 * Supports tests for WAN distribution.
 */
public class WANTest {

  protected static final int ITERATIONS = WANTestPrms.getIterations();
  protected static final long maxOperations = WANTestPrms.getMaxOperations();

  protected static List<String> regionDescriptNames = new ArrayList<String>();
  protected static List<String> regionNames = new ArrayList<String>();

  protected boolean isBridgeConfiguration = false;
  protected boolean isBridgeClient = false;
  private static Boolean isValidateDoEntryOperationsCalled = new Boolean(false);

  public static WANTest instance;
  protected static WANOperationsClient opsClient;

  public static volatile MethodCoordinator updateRegionSnapshot = new MethodCoordinator(
      WANOperationsClient.class.getName(), "writeRegionSnapshotToBB");

  static public LogWriter logger = Log.getLogWriter();
  static public GsRandom rand = new GsRandom();
  static public RandomValues rv = new RandomValues();

  WANBlackboard bb = WANBlackboard.getInstance();
  protected static Set<GatewaySender> gatewaySenders = new HashSet<GatewaySender>();
  ConfigHashtable conftab = TestConfig.tab();
  boolean isSenderStarted = false;
  static String SENDER_NAME_PREFIX = "SenderQueue_";

  // boolean for key prefixes for the security tests
  static boolean validPrefix = false;
  static boolean invalidPrefix = false;
  
  protected AtomicBoolean isQueuePrinting = new AtomicBoolean(false);

  public synchronized static void initializeLocator() {
    if (instance == null) {
      instance = new WANTest();
    }
  }

  public synchronized static void initializeDatastore() {
    if (instance == null) {
      instance = new WANTest();
      opsClient = new WANOperationsClient();

      // initialise PRObserver for recovery/rebalance monitoring
      PRObserver.installObserverHook();

      Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
      for (int i = 0; i < regionNames.size(); i++) {
        String regionDescriptName = (String) (regionNames.get(i));
        regionDescriptNames.add(regionDescriptName);
      }

      instance.isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names,
          null) != null;
      if (instance.isBridgeConfiguration) {
        instance.isBridgeClient = false;
      }

      if ((maxOperations % ITERATIONS) != 0) {
        throw new TestException(
            "Incorrect test configuration. maxOperations should be multiple of ITERATIONS. "
                + "newWan.WANTestPrms-maxOperations="
                + maxOperations
                + ", newWan.WANTestPrms-ITERATIONS=" + ITERATIONS);
      }
    }
  }

  public synchronized static void initializeClient() {
    if (instance == null) {
      instance = new WANTest();
      opsClient = new WANOperationsClient();

      Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
      for (int i = 0; i < regionNames.size(); i++) {
        String regionDescriptName = (String) (regionNames.get(i));
        regionDescriptNames.add(regionDescriptName);
      }

      instance.isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names,
          null) != null;
      if (instance.isBridgeConfiguration) {
        instance.isBridgeClient = true;
      }
    }
  }

  // ============================================================================
  // INITTASKS
  // ============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public synchronized static void createLocatorTask() {
    if (instance == null) {
      initializeLocator();
      instance.createLocator();
    }
  }

  void createLocator() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its (non-admin) distributed system.
   */
  public synchronized static void startAndConnectLocatorTask() {
    if (instance == null) {
      initializeLocator();
    }
    instance.startAndConnectLocator();
  }

  void startAndConnectLocator() {
    DistributedSystemHelper.startLocatorAndDS();
    waitForLocatorDiscovery();
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public synchronized static void startAndConnectAndVerifyLocatorTask() {
    if (instance == null) {
      initializeLocator();
    }
    instance.startAndConnectAndVerifyLocator();
  }

  void startAndConnectAndVerifyLocator() {
    Locator l = DistributedSystemHelper.startLocatorAndDS();
    waitForLocatorDiscovery();
    if (l != null) {
      Integer ds = DistributedSystemHelper.getDistributedSystemId();
      String locString = getLocatorAsString(l);
      SharedLock lock = bb.getSharedLock();
      lock.lock();
      Object locatorMap = bb.getSharedMap().get(WANBlackboard.LOCATORS_MAP);
      if (locatorMap == null) {
        Map lMap = new HashMap();
        lMap.put(ds, locString);
        bb.getSharedMap().put(WANBlackboard.LOCATORS_MAP, lMap);
      } else {
        Map lMap = (Map) locatorMap;
        String locators = (String) lMap.get(ds);
        if (locators == null) {
          lMap.put(ds, locString);
        } else if (!locators.contains(locString)) {
          String str = ((Map) bb.getSharedMap().get(WANBlackboard.LOCATORS_MAP))
              .get(ds) + "," + locString;
          lMap.put(ds, str);
        }
        bb.getSharedMap().put(WANBlackboard.LOCATORS_MAP, lMap);
      }
      lock.unlock();
    }
    verifyLocators();
  }

  private String getLocatorAsString(Locator l) {
    String lHost = l.isServerLocator() ? ((InternalLocator) l)
        .getServerLocatorAdvisee().getHostName() : l.getBindAddress()
        .getHostAddress();
    StringBuilder locatorString = new StringBuilder(lHost);
    locatorString.append('[').append(l.getPort()).append(']');
    return locatorString.toString();
  }

  protected void waitForLocatorDiscovery() {
    logger.info("Waititng for locator discovery.");
    boolean isDiscovered;
    do {
      isDiscovered = true;
      List<Locator> locators = Locator.getLocators();
      Map gfLocMap = ((InternalLocator) locators.get(0)).getAllLocatorsInfo();
      List dsList = new ArrayList(); // non discovered ds
      for (GemFireDescription gfd : TestConfig.getInstance()
          .getGemFireDescriptions().values()) {
        Integer ds = gfd.getDistributedSystemId();
        if (!ds.equals(-1) && !dsList.contains(ds) && !gfLocMap.containsKey(ds)) {
          dsList.add(ds);
          isDiscovered = false;
        }
      }
      if (!isDiscovered) {
        logger
            .info("Waiting for locator discovery to complete. Locators not discoverd so far from ds "
                + dsList
                + ". Distribution locator map from gemfire system is "
                + (new ConcurrentHashMap(gfLocMap)).toString());
        MasterController.sleepForMs(5 * 1000); // wait for 5 seconds
      }
    } while (!isDiscovered);
    logger.info("Locator discovery completed.");
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public synchronized static void startLocatorAndAdminDSTask() {
    if (instance == null) {
      initializeLocator();
    }
    instance.startLocatorAndAdminDS();
  }

  void startLocatorAndAdminDS() {
    DistributedSystemHelper.startLocatorAndAdminDS();
    waitForLocatorDiscovery();
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public synchronized static void starAndVerifytLocatorAdminDSTask() {
    if (instance == null) {
      initializeLocator();
    }
    instance.startAndVerifyLocatorAdminDS();
  }

  void startAndVerifyLocatorAdminDS() {
    Locator l = DistributedSystemHelper.startLocatorAndAdminDS();
    waitForLocatorDiscovery();
    if (l != null) {
      Integer ds = DistributedSystemHelper.getDistributedSystemId();
      String locString = getLocatorAsString(l);
      SharedLock lock = bb.getSharedLock();
      lock.lock();
      Object locatorMap = bb.getSharedMap().get(WANBlackboard.LOCATORS_MAP);
      if (locatorMap == null) {
        Map lMap = new HashMap();
        lMap.put(ds, locString);
        bb.getSharedMap().put(WANBlackboard.LOCATORS_MAP, lMap);
      } else {
        Map lMap = (Map) locatorMap;
        String locators = (String) lMap.get(ds);
        if (locators == null) {
          lMap.put(ds, locString);
        } else if (!locators.contains(locString)) {
          String str = ((Map) bb.getSharedMap().get(WANBlackboard.LOCATORS_MAP))
              .get(ds) + "," + locString;
          lMap.put(ds, str);
        }
        bb.getSharedMap().put(WANBlackboard.LOCATORS_MAP, lMap);
      }
      lock.unlock();
    }
    verifyLocators();
  }

  /**
   * Stops a locator.
   */
  public synchronized static void stopLocatorTask() {
    instance.stopLocator();
  }

  void stopLocator() {
    DistributedSystemHelper.stopLocator();
  }

  /**
   * Initializes a peer cache based on the {@link ConfigPrms#cacheConfig}.
   */
  public synchronized static void initPeerCacheTask() {
    if (instance == null) {
      initializeDatastore();
      instance.initPeerCache();
    }
  }

  public void initPeerCache() {
    String cacheConfig = ConfigPrms.getCacheConfig();
    Cache c = CacheHelper.createCache(cacheConfig);
  }

  /**
   * Initializes a server cache based on the {@link ConfigPrms#cacheConfig}.
   */
  public synchronized static void initServerCacheTask() {
    if (instance == null) {
      initializeDatastore();
      instance.initPeerCache();
    }
  }

  public synchronized static void initPeerRegionTask() {
    instance.initDatastoreRegion();
  }

  public void initDatastoreRegion() {
    initPdxDiskStore();
    Log.getLogWriter().info(
        "Creating regions with descriptions :" + regionDescriptNames);
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (!regionDescriptName.startsWith("client")
          && !regionDescriptName.startsWith("accessor")) {
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        RegionHelper.createRegion(regionName, regionDescriptName);
        if (!regionNames.contains(regionName)) {
          regionNames.add(regionName);
        }
        Log.getLogWriter().info(
            "Created datastore region " + regionName
                + " with region descript name " + regionDescriptName);
      }
    }
  }

  public synchronized static void initAccessorRegionTask() {
    List<Region> regions = instance.initAccessorRegion();
    boolean isAllEmpty = true;
    for (Region reg : regions) {
      if (reg.getAttributes().getDataPolicy() != DataPolicy.EMPTY)
        isAllEmpty = false;
    }
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null && !isAllEmpty) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  /*
   * Returns list of all regions created.
   */
  List<Region> initAccessorRegion() {
    initPdxDiskStore();
    List regions = new ArrayList();
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (regionDescriptName.startsWith("accessor")) {
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = RegionHelper.createRegion(regionName,
            regionDescriptName);
        regions.add(region);
        if (!regionNames.contains(regionName)) {
          regionNames.add(regionName);
        }
        Log.getLogWriter().info(
            "Created accessor region " + regionName
                + " with region descript name " + regionDescriptName);
      }
    }
    return regions;
  }

  public synchronized static void initServerRegionTask() {
    instance.initDatastoreRegion();

    String bridgeConfig = ConfigPrms.getBridgeConfig();
    BridgeHelper.startBridgeServer(bridgeConfig);
  }

  /**
   * Initializes a edge cache based on the {@link CacheClientPrms}.
   */
  public synchronized static void initEdgeClientCacheTask() {
    if (instance == null) {
      initializeClient();
      instance.initEdgeClientCache();
    }
  }

  public void initEdgeClientCache() {
    String cacheConfig = WANTestPrms.getClientCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);

    String VmDurableId = ((InternalDistributedSystem) InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    List<Region> regions = new ArrayList<Region>();
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (regionDescriptName.startsWith("client")) {
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = RegionHelper.createRegion(regionName,
            regionDescriptName);
        Log.getLogWriter().info(
            "Created edge client region " + regionName
                + " with region descript name " + regionDescriptName);
        if (!regionNames.contains(regionName)) {
          regionNames.add(regionName);
        }
        PoolImpl mybw = ClientHelper.getPool(region);

        ServerLocation primaryEndpoint = (ServerLocation) mybw.getPrimary();
        Log.getLogWriter().info(
            "The primary server endpoint is " + primaryEndpoint);
        regions.add(region);
      }
    }

    if (!VmDurableId.equals("")) {
      Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
      if (!DurableClientsBB.getBB().getSharedMap().containsKey(VmDurableId)) {
        HashMap map = new HashMap();
        DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      }
      cache.readyForEvents();
    }

    for (int i = 0; i < regions.size(); i++) {
      registerInterest(regions.get(i));
    }
  }

  /**
   * Initializes an edge CQ client based on the {@link CacheClientPrms}.
   */
  public synchronized static void initCQClientTask() {
    if (instance == null) {
      initializeClient();
    }
    instance.initCQClient();
  }

  public void initCQClient() {
    String cacheConfig = WANTestPrms.getClientCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);

    // initialize CQService, if needed
    CQUtil.initialize();
    CQUtil.initializeCQService();

    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (regionDescriptName.startsWith("client")) {
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = RegionHelper.createRegion(regionName,
            regionDescriptName);
        Log.getLogWriter().info(
            "Created edge client region " + regionName
                + " with region descript name " + regionDescriptName);
        if (!regionNames.contains(regionName)) {
          regionNames.add(regionName);
        }
        CQUtil.registerCQ(region);
        bb.getSharedCounters().increment(WANBlackboard.NUM_CQ);
        registerInterest(region);
      }
    }
  }

  /**
   * Registers interest in all keys using the client interest policy.
   */
  private void registerInterest(Region region) {
    InterestResultPolicy interestPolicy = CacheClientPrms.getInterestPolicy();
    LocalRegion localRegion = (LocalRegion) region;
    String VmDurableId = ((InternalDistributedSystem) InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      Log.getLogWriter().info("Doing durable register interest");
      localRegion.registerInterest("ALL_KEYS", interestPolicy, true);
    } else {
      localRegion.registerInterest("ALL_KEYS", interestPolicy);
    }
    Log.getLogWriter().info(
        "Initialized region " + region
            + "\nRegistered interest in ALL_KEYS with InterestResultPolicy = "
            + interestPolicy);
  }

  /**
   * Creates GatewaySender ids based on the
   * {@link ConfigPrms#gatewaySenderConfig}.
   */
  public synchronized static void createGatewaySenderIdsTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.createGatewaySenderIds(senderConfig);
  }

  /**
   * Initializes GatewaySender based on the
   * {@link ConfigPrms#gatewaySenderConfig}.
   */
  public synchronized static void initGatewaySenderTask() {
    instance.initGatewaySender();
  }

  public void initGatewaySender() {
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
    if (senders != null && senders.size() > 0) {
      Log.getLogWriter().info("Sender already created");
      return;
    }

    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    gatewaySenders = GatewaySenderHelper
        .createAndStartGatewaySenders(senderConfig);
  }

  public synchronized static void startGatewaySenderTask() {
    instance.startGatewaySender();
  }

  public void startGatewaySender() {
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
    if (senders == null || senders.size() == 0) {
      throw new HydraConfigException(
          "Test issue: Start gateway sender is called before creaing them.");
    } else if (isSenderStarted) { // senders already started
      Log.getLogWriter().info("Sender already started: " + senders);
      return;
    }

    Set queueRegions = new HashSet();
    GatewaySenderHelper.startGatewaySenders();
    for (GatewaySender sender : senders) {
      opsClient.updateSenderStateToBB(sender);
      addSenderQueueListener(sender);
      if (sender instanceof ParallelGatewaySenderImpl) {
      	ConcurrentParallelGatewaySenderQueue prq = (ConcurrentParallelGatewaySenderQueue) ((ParallelGatewaySenderImpl) sender)
            .getQueues().toArray(new RegionQueue[1])[0];
        if (null == prq.getRegion()) {
          Set prs = prq.getRegions();
          for (Iterator sit = prs.iterator(); sit.hasNext();) {
            queueRegions.add(((PartitionedRegion) sit.next())
                .getRegion().getFullPath());
          }
        } else {
          queueRegions.add(prq.getRegion().getFullPath());
        }
      }
    }
    isSenderStarted = true;

    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String senderkey = SENDER_NAME_PREFIX
        + DistributedSystemHelper.getDistributedSystemId();
    Set queueSet = (Set) bb.getSharedMap().get(senderkey);
    if (queueSet == null) {
      bb.getSharedMap().put(senderkey, queueRegions);
    } else {
      queueSet.addAll(queueRegions);
      bb.getSharedMap().put(senderkey, queueSet);
    }
  }

  private void addSenderQueueListener(GatewaySender sender) {
    String queueListenerClass = WANTestPrms.getSenderQueueRegionListener();
    if (queueListenerClass != null
        && !queueListenerClass.equalsIgnoreCase(BasePrms.NONE)) {
      Object queueListenerInstance = AbstractDescription.getInstance(
          WANTestPrms.senderQueueRegionListener, queueListenerClass);
      if (queueListenerInstance instanceof CacheListener) {
        if (sender instanceof SerialGatewaySenderImpl) {
          Set<RegionQueue> sqSet = ((SerialGatewaySenderImpl) sender)
              .getQueues();
          for (RegionQueue rq : sqSet) {
            rq.addCacheListener((CacheListener) queueListenerInstance);
          }
        } else if (sender instanceof ParallelGatewaySenderImpl) {
          RegionQueue parallelQueue = (RegionQueue) ((ParallelGatewaySenderImpl) sender)
              .getQueues().toArray()[0];
          PartitionedRegion region = (PartitionedRegion) parallelQueue
              .getRegion();
          for (int i = 0; i < region.getTotalNumberOfBuckets(); i++) {
            BucketRegion br = region.getBucketRegion(i);
            if (br != null) {
              AttributesMutator mutator = br.getAttributesMutator();
              mutator.addCacheListener((CacheListener) queueListenerInstance);
            }
          }
        } else {
          throw new HydraConfigException(
              "Test issue: Unknown class of gateway sender: "
                  + GatewaySenderHelper.gatewaySenderToString(sender));
        }
        Log.getLogWriter().info(
            "Configured cache listener " + queueListenerClass
                + " to queue region of sender " + sender.getId());
      } else {
        String s = BasePrms.nameForKey(WANTestPrms.senderQueueRegionListener)
            + " does not implement CacheListener: " + queueListenerClass;
        throw new HydraConfigException(s);
      }
    }
  }

  public synchronized static void HydraTask_stopWANComponents() {
    GatewaySenderHelper.stopGatewaySenders();
    GatewayReceiverHelper.stopGatewayReceivers();
  }

  public synchronized static void stopGatewaySenderTask() {
    instance.stopGatewaySender();
  }

  void stopGatewaySender() {
    GatewaySenderHelper.stopGatewaySenders();
  }

  public synchronized static void stopGatewayReceiverTask() {
    instance.stopGatewayReceiver();
  }

  void stopGatewayReceiver() {
    GatewayReceiverHelper.stopGatewayReceivers();
  }

  /**
   * Initializes GatewayReceiver based on the
   * {@link WANTestPrms#GatewayReceiverConfig}.
   */
  public synchronized static void initGatewayReceiverTask() {
    instance.initGatewayReceiver();
  }

  public void initGatewayReceiver() {
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
  }

  public synchronized static void HydraTask_initPeerTask() {
    if (instance == null) {
      initializeDatastore();
      instance.initPeerCache();
      instance.initGatewaySender();
      instance.initDatastoreRegion();
      instance.initGatewayReceiver();
      instance.startGatewaySender();
    }
  }

  public synchronized static void HydraTask_initServerTask() {
    if (instance == null) {
      initializeDatastore();
      instance.initPeerCache();
      instance.initGatewaySender();
      instance.initServerRegionTask();
      instance.initGatewayReceiver();
      instance.startGatewaySender();
    }
  }

  public synchronized static void HydraTask_initWANComponentsTask() {
    if (instance == null) {
      initializeDatastore();
      instance.initGatewaySender();
      instance.startGatewaySender();
      instance.initGatewayReceiver();
    }
  }

  // ============================================================================
  // CLOSETASKS
  // ============================================================================

  /**
   * Closes an edge cache.
   */
  public static synchronized void closeEdgeClientCacheTask() {
    Cache cache = CacheHelper.getCache();
    if (cache != null) { // another thread has closed the cache
      for (int j = 0; j < regionDescriptNames.size(); j++) {
        String regionDescriptName = (String) (regionDescriptNames.get(j));
        if (regionDescriptName.startsWith("client")) {
          String regionName = RegionHelper.getRegionDescription(
              regionDescriptName).getRegionName();
          Region region = RegionHelper.getRegion(regionName);
          if (region != null) {
            Log.getLogWriter().info(
                "Client's region " + regionName + " contains "
                    + region.keys().size() + " entries");

            // Don't close the cache if we're using partitionedRegions, we need
            // to allow
            // all servers to complete validation prior to cache close so we
            // don't
            // lose any portion of the PR
            if (region.getAttributes().getPartitionAttributes() == null) {
              CacheHelper.closeCache();
              return;
            }
          }
        }
      }
    }
  }

  /** Validates the contents of the local cache. */
  public static void validateSequentialKeysTask() {
    instance.validateSequentialKeysInRegion();
  }

  protected void validateSequentialKeysInRegion() {
    // init class loader for pdx
    PdxTest.initClassLoader();

    Log.getLogWriter().info(
        "validateSequentialKeysInRegion: Validation started");

    // wait for queues to drain
    waitForQueuesToDrain();

    // Wait for Queues to drain
    SilenceListener.waitForSilence(30, 5000);

    boolean onlyParentRegion = WANTestPrms.onlyParentRegion();
    if (onlyParentRegion && !regionNames.isEmpty()) {
      Region aRegion = RegionHelper.getRegion(regionNames.get(0));
      validateRegion(aRegion);
    } else {
      Region aRegion = null;
      Set rootRegions = CacheHelper.getCache().rootRegions();
      for (Iterator rit = rootRegions.iterator(); rit.hasNext();) {
        aRegion = (Region) rit.next();
        validateRegion(aRegion);
        Set subRegions = aRegion.subregions(true);
        for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
          aRegion = (Region) sit.next();
          validateRegion(aRegion);
        }
      }
    }
    Log.getLogWriter().info(
        "validateSequentialKeysInRegion: Validation complete");
  }

  public void validateRegion(Region aRegion) {
    Log.getLogWriter().info("Validating region: " + aRegion.getFullPath());

    if (aRegion == null) {
      throw new TestException("Region is null ");
    }

    StringBuffer aStr = new StringBuffer();
    if (RemoteTestModule.getMyClientName().contains("edge")) {
      // All clients/servers should have all keys
      Set serverKeys = aRegion.keySetOnServer();
      int expectedRegionSize = serverKeys.size();

      Set localKeys = aRegion.keySet();
      // int regionSize = localKeys.size();

      Set myEntries = aRegion.entrySet();
      Object[] entries = myEntries.toArray();
      int localKeySize = entries.length;
      Comparator myComparator = new RegionEntryComparator();
      java.util.Arrays.sort(entries, myComparator);

      // If region is empty (on client & server), there's no need to go further
      if ((entries.length == 0) && (expectedRegionSize == 0)) {
        return;
      }

      Log.getLogWriter().info(
          "Expecting " + expectedRegionSize + " entries from server in Region "
              + aRegion.getFullPath() + ", found " + localKeySize
              + " entries locally\n");
      if (localKeySize != expectedRegionSize) {
        aStr.append("Expected " + expectedRegionSize
            + " keys from  server in Region " + aRegion.getFullPath()
            + " but found " + localKeySize + " entries locally\n");
      }

      Log.getLogWriter().info(
          "Checking for missing or extra keys in client region");
      // Extra keys (not in the server region)?
      List unexpectedKeys = new ArrayList(localKeys);
      unexpectedKeys.removeAll(serverKeys);
      if (unexpectedKeys.size() > 0) {
        aStr.append("Extra keys (not found on server): " + unexpectedKeys
            + "\n");
      }

      // Are we missing keys (found in server region)?
      List missingKeys = new ArrayList(serverKeys);
      missingKeys.removeAll(localKeys);
      if (missingKeys.size() > 0) {
        aStr.append("Missing keys (found on server, but not locally) = "
            + missingKeys + "\n");
      }
    }

    // check the value of each entry in the local cache
    int requiredSizeFromBB = (bb.getSharedMap().get(aRegion.getName()) != null) ? ((Long) bb
        .getSharedMap().get(aRegion.getName())).intValue() : 0;
    int keySize = aRegion.size();

    String str = "SUPPOSED TO HAVE:" + requiredSizeFromBB
        + " FROM BB, DOES HAVE:" + keySize;
    Log.getLogWriter().info(str);
    if (requiredSizeFromBB != keySize) {
      aStr.append(str);
    }

    // Check whether each entry has value as ITERATIONS (default to 1000)
    Log.getLogWriter().info("Entering the validation loop ");
    for (long i = 1; i < requiredSizeFromBB; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      Object val = aRegion.get(key);

      logger.info("Region " + aRegion.getName() + " has entry: " + key + " => "
          + val);
      if (val == null) {
        String s = "No value in cache at " + key + " in Region "
            + aRegion.getFullPath();
        throw new TestException(s);
      } else if (val instanceof Integer) {
        int ival = ((Integer) val).intValue();
        if (ival != ITERATIONS) {
          String s = "Wrong value in cache at " + key + " in Region "
              + aRegion.getFullPath() + ", expected " + ITERATIONS
              + " but got " + ival;
          throw new TestException(s);
        }
      } else if (val instanceof byte[]) {
        if (((byte[]) val).length != ITERATIONS) {
          String s = "Wrong value in cache at " + key
              + ", expected byte[] of length " + ITERATIONS
              + " but got length " + ((byte[]) val).length;
          throw new TestException(s);
        }
      } else if (val instanceof PSTObject) {
        if (((PSTObject) val).getIndex() != ITERATIONS) {
          String s = "Wrong value in cache at " + key
              + ", expected PSTObject with index " + ITERATIONS
              + " but got index " + ((PSTObject) val).getIndex();
          throw new TestException(s);
        }
      } else if (val instanceof BaseValueHolder || (val instanceof PdxInstance)) {
        val = PdxTest.toValueHolder(val);
        BaseValueHolder vh = (BaseValueHolder) val;
        if (!vh.getMyValue().equals(new Long(ITERATIONS))) {
          String s = "Wrong value in cache at " + key
              + ", expected ValueHolder with myValue " + ITERATIONS
              + " but got " + vh.getMyValue();
          throw new TestException(s);
        }
        try {
          vh.verifyMyFields((Long) vh.myValue);
        } catch (TestException e) {
          throw new TestException("While checking key " + key + ": "
              + e.getMessage());
        }
      } else if (val instanceof QueryObject) {
        if (((QueryObject) val).aPrimitiveLong != ITERATIONS) {
          String s = "Wrong value in cache at " + key
              + ", expected QueryObject with aPrimitiveLong=" + ITERATIONS
              + " but got " + ((QueryObject) val).aPrimitiveLong;
          throw new TestException(s);
        }
      } else {
        String s = "Wrong value object type in cache at " + key + ", got "
            + val + " of type " + val.getClass().getName();
        throw new TestException(s);
      }
    }

    if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
    }

    Log.getLogWriter().info("Validated region: " + aRegion.getFullPath());
  }

  /**
   * Validates the CQEvent counters and SelectResults against expected contents
   * of server region
   */
  public static void validateSequentialKeysCQTask() throws Exception {
    for (String regionName : regionNames) {
      instance.validateSequentialKeysCQ(RegionHelper.getRegion(regionName));
    }
  }

  public void validateSequentialKeysCQ(Region region) {
    if (region == null) {
      throw new TestException("Region is null ");
    }

    Log.getLogWriter().info(
        "Validating sequential keys CQ for region: " + region.getName());

    long numCqs = bb.getSharedCounters().read(WANBlackboard.NUM_CQ);

    // wait for queues to drain
    waitForQueuesToDrain();

    // Validate EventCounters collected by CQTestListener
    // Expect WanBB.currentEntry * numCQsCreated CQ Create Events
    // Expect (ITERATIONS-1) * WanBB.currentEntry * numCQsCreated CQ Update
    // Events
    // 0 invalidates, destroys and onError invocations
    SharedCounters sc = CQUtilBB.getBB().getSharedCounters();
    long requiredSize = ((Long) bb.getSharedMap().get(region.getName()))
        .longValue();
    StringBuffer errMsg = new StringBuffer();

    Log.getLogWriter().info(
        "Expected " + requiredSize * numCqs + " (" + requiredSize
            + " regionEntries * " + numCqs + " numCQs) for region "
            + region.getFullPath());

    try {
      TestHelper.waitForCounter(CQUtilBB.getBB(), "NUM_CREATE",
          CQUtilBB.NUM_CREATE, (requiredSize * numCqs), true, 300000);
      TestHelper.waitForCounter(CQUtilBB.getBB(), "NUM_UPDATE",
          CQUtilBB.NUM_UPDATE, ((ITERATIONS - 1) * (requiredSize * numCqs)),
          true, 180000);
    } catch (TestException e) {
      errMsg.append(e.toString() + "\n");
    }

    // Our WANCQListener tracks the missing events
    // The first of which will be included in the TestException;
    // all will be logged in the system logs. It also tracks
    // the number of skipped updates.
    hydra.blackboard.SharedMap aMap = CQUtilBB.getBB().getSharedMap();
    String exceptionStr = (String) aMap.get(TestHelper.EVENT_ERROR_KEY);
    if (exceptionStr != null) {
      errMsg.append("\nFirst Exception encountered by listener: \n");
      errMsg.append("\t" + exceptionStr + "\n");
      // Log the number of missing and late updates from the listeners
      long missing = sc.read(CQUtilBB.MISSING_UPDATES);
      long late = sc.read(CQUtilBB.LATE_UPDATES);
      errMsg.append("\tTotal count of missingUpdates = " + missing + "\n");
      errMsg.append("\tTotal count of late (OutOfOrder) Updates = " + late
          + "\n");
    }

    long numCQInvalidateEvents = sc.read(CQUtilBB.NUM_INVALIDATE);
    if (numCQInvalidateEvents != 0) {
      errMsg.append("Expected 0 CQ Invalidate Events, but found "
          + numCQInvalidateEvents + "\n");
    }

    long numCQDestroyEvents = sc.read(CQUtilBB.NUM_DESTROY);
    if (numCQDestroyEvents != 0) {
      errMsg.append("Expected 0 CQ Destroy Events, but found "
          + numCQDestroyEvents + "\n");
    }

    long numOnErrorInvocations = sc.read(CQUtilBB.NUM_ERRORS);
    if (numOnErrorInvocations != 0) {
      errMsg.append("Expected 0 onError() invocations, but found "
          + numOnErrorInvocations + "\n");
    }

    if (errMsg.length() > 0) {
      String errHdr = "Failures found while verifying CQEvents\n";
      throw new TestException(errHdr + errMsg.toString()
          + util.TestHelper.getStackTrace());
    }

    long numCQCreateEvents = sc.read(CQUtilBB.NUM_CREATE);
    long numCQUpdateEvents = sc.read(CQUtilBB.NUM_UPDATE);

    Log.getLogWriter().info("Successfully validated CQEvents in client");
    StringBuffer aStr = new StringBuffer("CQEvent Summary\n");
    aStr.append("   numCQCreateEvents = " + numCQCreateEvents + "\n");
    aStr.append("   numCQUpdateEvents = " + numCQUpdateEvents + "\n");
    aStr.append("   numCQDestroyEvents = " + numCQDestroyEvents + "\n");
    aStr.append("   numCQInvalidateEvents = " + numCQInvalidateEvents + "\n");
    aStr.append("   numOnErrorInvocations = " + numOnErrorInvocations + "\n");
    Log.getLogWriter().info(aStr.toString());

    // Now that we've finished processing events, it is safe to get the
    // SelectResults (otherwise, we stop the CQ in order to get the latest
    // SelectResults)
    // check the value of each entry in the SelectResults
    SelectResults results = CQUtil.getCQResults();
    logger.info("SelectResults SUPPOSED TO HAVE:" + requiredSize
        + " DOES HAVE:" + results.size() + " numCqs = " + numCqs);

    if (results.size() != requiredSize) {
      throw new TestException("Expected SelectResults to be size "
          + requiredSize + " but found " + results.size() + " elements");
    }

    // traverse Select Results to validate region values are at ITERATOR
    Iterator it = results.iterator();
    while (it.hasNext()) {
      Object val = it.next();
      if (val == null) {
        String s = "Unexpected value " + val + " in SelectResults";
        throw new TestException(s);
      } else if (val instanceof ValueHolder) {
        ValueHolder v = (ValueHolder) val;
        if (!v.getMyValue().equals(new Long(ITERATIONS))) {
          String s = "Wrong value in SelectResults, expected myValue to be "
              + ITERATIONS + " but got " + v.getMyValue() + " in value " + v;
          throw new TestException(s);
        }
      } else if (val instanceof QueryObject) {
        QueryObject v = (QueryObject) val;
        if (!v.aLong.equals(new Long(ITERATIONS))) {
          String s = "Wrong value in SelectResults, expected aLong to be "
              + ITERATIONS + " but got " + v.aLong + " in QueryObject value "
              + v;
          throw new TestException(s);
        }
      } else {
        String s = "Wrong type in SelectResults, expected ValueHolder "
            + "but got " + val + " of type " + val.getClass().getName();
        throw new TestException(s);
      }
    }
    Log.getLogWriter().info(
        "Successful validation of SelectResults (" + results.size() + "): "
            + results.toString());

  }

  /**
   * 
   */
  public synchronized static void HydraTask_verifyQueueEviction() {
    instance.verifyQueueEviction();
  }

  /**
   * Task to pause all senders in vm
   */
  public synchronized static void HydraTask_pauseSenders() {
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
    for (GatewaySender sender : senders) {
      logger.info("pausing sender: " + sender.getId());
      sender.pause();
    }
  }

  /**
   * Task to resume all senders in vm
   */
  public synchronized static void HydraTask_resumeSenders() {
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
    for (GatewaySender sender : senders) {
      logger.info("resuming sender: " + sender.getId());
      sender.resume();
    }
  }

  protected void verifyQueueEviction() {
    Cache cache = CacheHelper.getCache();
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();

    for (GatewaySender sender : senders) {
      logger.info("verifyQueueEviction on sender " + sender.getId());

      // verify queue region memory size is not more than maximumQueueMemoery
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) sender)
          .getQueues();
      for (RegionQueue rq : regionQueues) {
        Region queueRegion = rq.getRegion();

        long entriesInVm = getDiskStatNumEntriesInVM(queueRegion);
        long entriesInDisk = getDiskStatNumOverflowOnDisk(queueRegion);
        Set entries = queueRegion.entrySet();
        long entriesInRegion = entries.size();

        if (queueRegion instanceof PartitionedRegion) {
          long totalEntriesInBuckets = 0;
          Set bucketList = ((PartitionedRegion) queueRegion).getDataStore()
              .getAllLocalBuckets();
          Iterator iterator = bucketList.iterator();
          while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            BucketRegion localBucket = (BucketRegion) entry.getValue();
            if (localBucket != null) {
              totalEntriesInBuckets = totalEntriesInBuckets
                  + localBucket.entryCount();
            }
          }
          entriesInRegion = totalEntriesInBuckets;
        }

        // check for eviction
        if (entriesInDisk == 0) {
          throw new TestException(
              "Test tuning required: no eviction happen for gateway sender: "
                  + GatewaySenderHelper.gatewaySenderToString(sender));
        }

        // calculate per entry size
        int approxBytesPerEntry = 4;
        if (!entries.isEmpty()) {
          Region.Entry entry = (Region.Entry) entries.iterator().next();
          com.gemstone.gemfire.cache.util.ObjectSizer sizer = new com.gemstone.gemfire.cache.util.ObjectSizerImpl();
          approxBytesPerEntry = sizer.sizeof(entry.getKey())
              + sizer.sizeof(entry.getValue());
        }
        logger.info("Approximate size of per entry is " + approxBytesPerEntry
            + " bytes");

        long approxBytesInVm = entriesInVm * approxBytesPerEntry;
        long maxPermissibleBytes = sender.getMaximumQueueMemory() * 1024 * 1024
            / ((AbstractGatewaySender) sender).getDispatcherThreads(); // convert
        // to
        // byte
        long allowedOverhead = maxPermissibleBytes / 10; // allowed 10% overload
        // of queque memory
        StringBuffer str = new StringBuffer().append(
            "Permissible Bytes for sender region " + queueRegion.getFullPath()
                + " is " + (maxPermissibleBytes + allowedOverhead)
                + " bytes (Configured maxPermissibleBytes="
                + maxPermissibleBytes + " + 10% more for overhead)"
                + " but approx bytes in VM is " + approxBytesInVm).append(
            " (totalQueueRegionEntries= " + entriesInRegion
                + ", numEntriesInVm=" + entriesInVm + ", NumEntriesInDisk="
                + entriesInDisk + ")");
        if (approxBytesInVm > (maxPermissibleBytes + allowedOverhead)) {
          throw new TestException(str.toString());
        } else {
          Log.getLogWriter().info(str.toString());
        }

        // Verify region entries with vm entries and overflowed entries
        logger.info("Queue region entries for sender region "
            + queueRegion.getFullPath() + ". totalQueueRegionEntries= "
            + entriesInRegion + ", numEntriesInVm=" + entriesInVm
            + ", NumEntriesInDisk=" + entriesInDisk);
        if (entriesInRegion != (entriesInVm + entriesInDisk)) {
          throw new TestException(
              "Queue region entries does not matched with entries in VM and Disk for sender region "
                  + queueRegion.getFullPath()
                  + ". totalQueueRegionEntries= "
                  + entriesInRegion
                  + ", numEntriesInVm="
                  + entriesInVm
                  + ", NumEntriesInDisk=" + entriesInDisk);
        }
      }
    }
  }

  private long getDiskStatNumEntriesInVM(Region queueRegion) {
    long numEntriesInVm = 0;
    if (queueRegion instanceof PartitionedRegion) {
      PartitionedRegion shadowPR = ((PartitionedRegion) queueRegion);
      numEntriesInVm = shadowPR.getDiskRegionStats().getNumEntriesInVM();
    } else if (queueRegion instanceof LocalRegion) {
      DiskRegionStats diskStat = ((LocalRegion) queueRegion).getDiskRegion()
          .getStats();
      numEntriesInVm = diskStat.getNumEntriesInVM();
    } else {
      throw new TestException("Test issue: Unknown class of queueRegion : "
          + RegionHelper.regionAttributesToString(queueRegion.getAttributes()));
    }
    return numEntriesInVm;
  }

  private long getDiskStatNumOverflowOnDisk(Region queueRegion) {
    long numOverflowObDisk = 0;
    if (queueRegion instanceof PartitionedRegion) {
      PartitionedRegion shadowPR = ((PartitionedRegion) queueRegion);
      numOverflowObDisk = shadowPR.getDiskRegionStats().getNumOverflowOnDisk();
    } else if (queueRegion instanceof LocalRegion) {
      DiskRegionStats diskStat = ((LocalRegion) queueRegion).getDiskRegion()
          .getStats();
      numOverflowObDisk = diskStat.getNumOverflowOnDisk();
    } else {
      throw new TestException("Test issue: Unknown class of queueRegion : "
          + RegionHelper.regionAttributesToString(queueRegion.getAttributes()));
    }
    return numOverflowObDisk;
  }

  /** Prints the contents of the local queue. */
  public static void printQueueContentsTask() throws Exception {
    instance.printQueueContents();
  }

  public void printQueueContents() {
    
    //only one thread to print queue at a time
    if(!isQueuePrinting.get()){
     isQueuePrinting.set(true);
      
    for (GatewaySender gs : gatewaySenders) {
      Log.getLogWriter().info("Printing queue contents for sender : " + gs.getId());
      
      //Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gs).getQueues();
      //for (RegionQueue rq : regionQueues) 
      //Region region = rq.getRegion();
      Set queueRegions = new HashSet();
      if (gs instanceof ParallelGatewaySenderImpl) {
    	  ConcurrentParallelGatewaySenderQueue prq = (ConcurrentParallelGatewaySenderQueue) ((ParallelGatewaySenderImpl) gs)
          .getQueues().toArray(new RegionQueue[1])[0];
      
          if (null == prq.getRegion()) {
            Set prs = prq.getRegions();
            
            for (Iterator sit = prs.iterator(); sit.hasNext();) {
              queueRegions.add(((PartitionedRegion) sit.next())
                  .getRegion());
            }
            
            for (Iterator sit = prs.iterator(); sit.hasNext();){
              printRegionQueue(((PartitionedRegion) sit.next())
              .getRegion(),queueRegions);
            }
          } else {
            queueRegions.add(prq.getRegion());
            printRegionQueue(prq.getRegion(),queueRegions);
          }
        }
      } 
    isQueuePrinting.set(false);
    }else{
      Log.getLogWriter().info("Some other thread is already printing queue content in this vm, thus skiping...");
    }
  }
  
  public void printRegionQueue(Region region, Set rq) {

    int totalCnt = region.size();
    int primCnt = 0;
    String primary = "";
    Region localPrimaryRegion = null;
    boolean isPartition = false;
    if (region instanceof PartitionedRegion) {
      localPrimaryRegion = PartitionRegionHelper.getLocalPrimaryData(region);
      primary = ", " + localPrimaryRegion.size() + " primary entries.";
      primCnt = localPrimaryRegion.size();
      isPartition = true;
    }

    Log.getLogWriter().info("Gateway sender queue " + region.getFullPath() + " has " + totalCnt
            + " total entries " + primary);
    StringBuilder aStr = new StringBuilder();
    
    //check for primary entries, 
    // do not print huge entries to avoid OOME
    if(isPartition && primCnt < 20000){ 
      try{
        Object[] myEntries = localPrimaryRegion.entrySet().toArray();
        aStr.append("Queue containts (only primary entries) are : Total entries=" + myEntries.length + "\n");
        for (int i = 0; i < myEntries.length && i < 5; i++) { // print only 5 entries
          try {
            Region.Entry entry = (Region.Entry)myEntries[i];
            if (entry != null) {
              Object key = entry.getKey();
              Object val = entry.getValue();
              aStr.append("\t" + key + " -> " + ((val == null) ? "null" : val) + "\n");
            }
          }
          catch (EntryDestroyedException ed) {
          } // expected for RegionQueue
        }
        if (myEntries.length > 5) {
          aStr.append("\n The queue (only primary entries) contains more than 5 entries, ignoring others...\n");
        }
      }catch (RuntimeException tde) {
        if (tde.toString().contains("ToDataException"))
          Log.getLogWriter().info("Observed ToDataException while iterating queue region, expected in case of OffHeap region, continue...");
        else 
          throw tde;
      }
    }
    
    // do not print huge entries to avoid OOME
    if (totalCnt < 20000){
      //check for all entries
      try{
        Object[] myEntries = region.entrySet().toArray();
        aStr.append("Queue containts for all entries are : Total entries=" + myEntries.length + "\n");
        for (int i = 0; i < myEntries.length && i < 5; i++) { // print only 5 entries
          try {
            Region.Entry entry = (Region.Entry)myEntries[i];
            if (entry != null) {
              Object key = entry.getKey();
              Object val = entry.getValue();
              aStr.append("\t" + key + " -> " + ((val == null) ? "null" : val) + "\n");
            }
          }
          catch (EntryDestroyedException ed) {
          } // expected for RegionQueue
        }
        if (myEntries.length > 5) {
          aStr.append("\n The queue contains for all entries has more than 5 entries, ignoring others... ");
        }
      }catch (RuntimeException tde) {
        if (tde.toString().contains("ToDataException"))
          Log.getLogWriter().info("Observed ToDataException while iterating queue region, expected in case of OffHeap region, continue...");
        else 
          throw tde;
      }
    }else{
      aStr.append(" \n Queue contains total " + totalCnt + " entries, skipping printing.");
    }
    
    Log.getLogWriter().info(aStr.toString());
  }
  
  public static void waitForQueuesToDrainTask() {
    instance.waitForQueuesToDrain();
  }

  /**
   * Waits {@link newWan.WANTestPrms#secToWaitForQueue} for queues to drain.
   * Suitable for use as a helper method or hydra task.
   */
  public void waitForQueuesToDrain() {
    final SharedMap bb = WANBlackboard.getInstance().getSharedMap();
    // Lets check to make sure the queues are empty
    long startTime = System.currentTimeMillis();
    long maxWait = WANTestPrms.getSecToWaitForQueue();
    long entriesLeft = 0;
    Map queueEntryMap = new HashMap();
    while ((System.currentTimeMillis() - startTime) < (maxWait * 1000)) {
      boolean pass = true;
      entriesLeft = 0;

      for (GatewaySender gs : gatewaySenders) {
        if (gs instanceof SerialGatewaySenderImpl) {
          int queuesize = 0;
          Set<RegionQueue> qrs = ((SerialGatewaySenderImpl) gs).getQueues();
          for (RegionQueue rq : qrs) {
            queuesize += rq.size();
          }
          entriesLeft += queuesize;
          queueEntryMap.put(gs.getId(), new Integer(queuesize));
          if (queuesize > 0) {
            logger
                .warning("Still waiting for queue to drain. SerialGatewaySender "
                    + gs + " has " + queuesize + " entries in it.");
            pass = false;
          }
        } else if (gs instanceof ParallelGatewaySenderImpl) {
          RegionQueue rq = (RegionQueue) ((ParallelGatewaySenderImpl) gs)
              .getQueues().toArray()[0];
          int queuesize = rq.size();
          entriesLeft += queuesize;
          queueEntryMap.put(gs.getId(), new Integer(queuesize));
          if (queuesize > 0) {
            logger
                .warning("Still waiting for queue to drain. ParallelGatewaySender "
                    + gs + " has " + queuesize + " entries in it.");
            pass = false;
          }
        } else {
          throw new TestException("Unknown class of gateway sender: "
              + GatewaySenderHelper.gatewaySenderToString(gs));
        }
      }

      if (pass) {
        entriesLeft = 0;
        break;
      } else {
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      printQueueContents();
    }
    if (entriesLeft > 0) {
      throw new TestException(
          "Timed out waiting for queue to drain. Waited for " + maxWait
              + " sec, total entries left in all queues are " + entriesLeft
              + ". queueEntryMap=" + queueEntryMap);
    } else {
      logger.info("SENDER QUEUES ARE DRAINED");
    }
  }

  /**
   * Task to wait for the listener silence before doing validation tasks.
   */
  public static void waitForListenerSilence() {
    Log.getLogWriter().info("Silence Listeenr waiting for 30 seconds ...");
    SilenceListener.waitForSilence(30, 5000);
  }

  /** Prints the contents of the local cache. */
  public static void printSequentialKeysTask() throws Exception {
    for (String rName : regionNames) {
      instance.printSequentialKeys(RegionHelper.getRegion(rName));
    }
  }

  public void printSequentialKeys(Region region) throws Exception {
    if (region == null) {
      throw new TestException("Region in printSequentialKeys is null");
    }

    // print the value of each entry in the local cache
    StringBuffer buffer = new StringBuffer();
    buffer.append("Contents of region");
    buffer.append(region.getFullPath());
    buffer.append(":");

    Set keys = region.keys();
    Iterator kI = keys.iterator();
    while (kI.hasNext()) {
      Object key = kI.next();
      Object val = DiskRegUtil.getValueInVM(region, key);
      if (val != null) {
        buffer.append("\n\tENTRY ");
        buffer.append(key);
        buffer.append(":");
        buffer.append(val);
      }
    }
    Log.getLogWriter().info(buffer.toString());
  }

  public static void doHAEntryOperationTask() {
    Log.getLogWriter().info("In doHAEntryOperationTask");

    long cycleCounter = WANOperationsClientBB.getBB().getSharedCounters()
        .read(WANOperationsClientBB.NumCycle);
    int numThreadsForTask = RemoteTestModule.getCurrentThread()
        .getCurrentTask().getTotalThreads();
    long numDoingOps = WANOperationsClientBB.getBB().getSharedCounters()
        .read(WANOperationsClientBB.NumStartedDoingOps);
    if (numDoingOps >= numThreadsForTask - 1) {
      Log.getLogWriter()
          .info(
              "Returning from doHAEntryOperationTask "
                  + "with noops as WANOperationsClientBB.NumStartedDoingOps reached to "
                  + numDoingOps);
      MasterController.sleepForMs(10000);
      return;
    }
    WANOperationsClientBB.getBB().getSharedCounters()
        .increment(WANOperationsClientBB.NumStartedDoingOps);
    logger.info("doHAEntryOperationTask: started doing operations counter is "
        + numDoingOps + " in current cycle, cycleCounter=" + cycleCounter);

    doEntryOperationTask();

    // increment numDoingOps counter
    long numDoneOps = WANOperationsClientBB.getBB().getSharedCounters()
        .incrementAndRead(WANOperationsClientBB.NumFinishedDoingOps);
    logger.info("doHAEntryOperationTask: finished doing operation counter is "
        + numDoneOps + " in current cycle, cycleCounter=" + cycleCounter);
    Log.getLogWriter().info("Done doHAEntryOperationTask");
  }

  public static void doEntryOperationTask() {
	  try {
          instance.doEntryOperationAllRegions();
	  } catch (CacheClosedException except) {   
	       if (StopStartVMs.niceKillInProgress()) {
	           // a thread in this VM closed the cache or disconnected from the dist system
	           // or we are undergoing a nice_kill; all is OK
	        } else { // no reason to get this error
	           throw new TestException(TestHelper.getStackTrace(except));
	        }
	    }
  }

  protected void doEntryOperationAllRegions() {
    Region aRegion = null;
    Set regionVisited = new HashSet();
    Set rootRegions = CacheHelper.getCache().rootRegions();
    logger.info("doEntryOperationUniqueKeys: on regions " + rootRegions);
    for (Iterator rit = rootRegions.iterator(); rit.hasNext();) {
      aRegion = (Region) rit.next();
      if (!regionVisited.contains(aRegion.getName())) {
        opsClient.doEntryOperations(aRegion);
        regionVisited.add(aRegion.getName());
      }
      Set subRegions = aRegion.subregions(true);
      for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
        aRegion = (Region) sit.next();
        if (!regionVisited.contains(aRegion.getName())) {
          opsClient.doEntryOperations(aRegion);
          regionVisited.add(aRegion.getName());
        }
      }
    }
  }

  /**
   * Validates the contents of the local cache after running
   * doEntryOperationTask task
   */
  public static void validateDoEntryOperationTask() {
    // workaround for #44840, now only one thread from a vm will do validation,
    // as multiple threads iterating over keyset causes the hang.
    synchronized (isValidateDoEntryOperationsCalled) {
      if (!isValidateDoEntryOperationsCalled.booleanValue()) {
        instance.validateDoEntryOperationUniqueKeys();
        isValidateDoEntryOperationsCalled = new Boolean(true);
      }
    }
  }

  protected void validateDoEntryOperationUniqueKeys() {
    Log.getLogWriter().info("validateDoEntryOperation: Validation started");

    // wait for queues to drain
    waitForQueuesToDrain();

    // Wait for Queues to drain
    SilenceListener.waitForSilence(30, 5000);

    boolean onlyParentRegion = WANTestPrms.onlyParentRegion();
    if (onlyParentRegion && !regionNames.isEmpty()) {
      Region aRegion = RegionHelper.getRegion(regionNames.get(0));
      opsClient.verifyRegionContents(aRegion,
          opsClient.getBBSnapshotForUniqueKeyMap(aRegion));
    } else {
      Region aRegion = null;
      Set rootRegions = CacheHelper.getCache().rootRegions();
      for (Iterator rit = rootRegions.iterator(); rit.hasNext();) {
        aRegion = (Region) rit.next();
        opsClient.verifyRegionContents(aRegion,
            opsClient.getBBSnapshotForUniqueKeyMap(aRegion));
        Set subRegions = aRegion.subregions(true);
        for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
          aRegion = (Region) sit.next();
          opsClient.verifyRegionContents(aRegion,
              opsClient.getBBSnapshotForUniqueKeyMap(aRegion));
        }
      }
    }
    Log.getLogWriter().info("validateDoEntryOperation: Validation complete");
  }

  public static void updateRegionSnapshotTask() {
    instance.updateRegionSnapshot.executeOnce(opsClient, new Object[0]);
  }

  /**
   * Validates the contents of the local cache after running
   * doEntryOperationTask task
   */
  public static void validateDoEntryOperationTxTask() {
    // workaround for #44840, now only one thread from a vm will do validation,
    // as multiple threads iterating over keyset causes the hang.
    synchronized (isValidateDoEntryOperationsCalled) {
      if (!isValidateDoEntryOperationsCalled.booleanValue()) {
        instance.validateDoEntryOperationUniqueKeysTransactions();
        isValidateDoEntryOperationsCalled = new Boolean(true);
      }
    }
  }

  protected void validateDoEntryOperationUniqueKeysTransactions() {
    Log.getLogWriter().info("validateDoEntryOperation: Validation started");

    // wait for queues to drain
    WANTest.waitForQueuesToDrainTask();
    // Wait for Queues to drain
    SilenceListener.waitForSilence(30, 5000);

    Region aRegion = null;
    Set rootRegions = CacheHelper.getCache().rootRegions();
    for (Iterator rit = rootRegions.iterator(); rit.hasNext();) {
      aRegion = (Region) rit.next();
      opsClient.verifyRegionContents(aRegion, opsClient.getBBSnapshot(aRegion));
      Set subRegions = aRegion.subregions(true);
      for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
        aRegion = (Region) sit.next();
        opsClient.verifyRegionContents(aRegion,
            opsClient.getBBSnapshot(aRegion));
      }
    }
    Log.getLogWriter().info("validateDoEntryOperation: Validation complete");
  }

  /**
   * Puts integers into the cache with a globally increasing shared counter as
   * the key. Does 1000 updates on each key.
   */
  public static void putSequentialKeysTask() throws Exception {
    Set regionVisited = new HashSet();
    for (String regionName : regionNames) {
      if (!regionVisited.contains(regionName)) {
        instance.putSequentialKeys(RegionHelper.getRegion(regionName));
        regionVisited.add(regionName);
      }
    }
  }

  public void putSequentialKeys(Region region) throws Exception {
    if (region == null) {
      throw new TestException("Region in putSequentialKeys is " + region);
    }

    // init class loader if pdx is enabled
    PdxTest.initClassLoader();

    int putAllOps = 5; // 1 out of 5 chances to perform putAll operation

    // add putAll task
    int sleepMs = WANTestPrms.getSleepSec() * 1000;

    for (int j = 0; j < WANTestPrms.getBatchSize(); j++) {
      int doPutAll = rand.nextInt(putAllOps);
      if (doPutAll == 1) {
        putSequentialKeyUsingPutAll(region, sleepMs);
      } else {
        long opsCounter = bb.getSharedCounters().read(
            WANBlackboard.operation_counter);
        if ((opsCounter + ITERATIONS) > maxOperations) {
          // stop scheduling task
          throw new StopSchedulingOrder(
              "Time to stop as max number of operations reached. Operations="
                  + opsCounter);
        }

        opsCounter = bb.getSharedCounters().add(
            WANBlackboard.operation_counter, ITERATIONS);
        Long keyCounter = bb.increamentAndReadKeyCounter(region.getName(), 1);
        Object key = getKeyForCounter(keyCounter.longValue());

        logger.info("Working on key :" + key + " for region "
            + region.getFullPath() + " opsCounter=" + opsCounter);
        for (long i = 1; i <= ITERATIONS; i++) {
          MasterController.sleepForMs(sleepMs);

          BaseValueHolder val = getValueForCounter(i);
          Object v = region.replace(key, val);
          if (v == null) { // force replacement
            region.put(key, val);
          }
        }
      }
    }
  }

  /**
   * Puts integers into the cache with a globally increasing shared counter as
   * the key. Does 1000 updates on each key.
   */
  public static void putSequentialKeysAndHATask() throws Exception {
    instance.putSequentialKeysAndHA();
  }

  public synchronized void putSequentialKeysAndHA() throws Exception {
    int numVMsToStop = TestConfig.tasktab().intAt(StopStartPrms.numVMsToStop,
        TestConfig.tab().intAt(StopStartPrms.numVMsToStop));
    if (numVMsToStop <= 0) {
      logger
          .warning("putSequentialKeysAndHA: Not doing anything as number of vm requested to kill is "
              + numVMsToStop);
      return;
    }

    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String peerName;
    if (isBridgeConfiguration) {
      String vmName = RemoteTestModule.getMyClientName();
      peerName = "bridge"
          + vmName.substring(vmName.indexOf("_"),
              vmName.indexOf("_", vmName.indexOf("_") + 1));
    } else {
      peerName = "peer" + dsName.substring(dsName.indexOf("_"));
    }

    logger.info("putSequentialKeysAndHA: Looking for " + numVMsToStop
        + " vm with client name " + peerName);
    PRObserver.initialize();
    Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, peerName);
    List vmList = (List) (tmpArr[0]);
    List stopModeList = (List) (tmpArr[1]);
    List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);
    // this asychronously starts the thread to stop and start
    // meanwhile, we will do operations
    for (String regionName : regionNames) {
      putSequentialKeys(RegionHelper.getRegion(regionName));
    }
    StopStartVMs.joinStopStart(vmList, threads);
  }

  /**
   * Puts integers into the cache with a globally increasing shared counter as
   * the key. Does 1000 updates on each key.
   */
  public static void putSequentialKeysCQTask() throws Exception {
    for (String regionName : regionNames) {
      instance.putSequentialKeysCQ(RegionHelper.getRegion(regionName));
    }
  }

  public void putSequentialKeysCQ(Region region) throws Exception {
    if (region == null) {
      throw new TestException("Region in putSequentialKeys is " + region);
    }

    int putAllOps = 5; // 1 out of 5 chances to perform putAll operation
    // add putAll task
    int sleepMs = WANTestPrms.getSleepSec() * 1000;

    for (int j = 0; j < WANTestPrms.getBatchSize(); j++) {
      int doPutAll = rand.nextInt(putAllOps);
      if (doPutAll == 1) {
        int mapSize = rand.nextInt(4) + 1;

        Long entryCounter = bb.increamentAndReadKeyCounter(region.getName(),
            mapSize);
        logger.info("Working on key :"
            + getKeyForCounter(entryCounter - mapSize + 1) + " for region "
            + region.getFullPath() + ". PutAll with the map size of " + mapSize
            + " from key " + getKeyForCounter(entryCounter - mapSize + 1));

        HashMap aMap = new HashMap();
        for (long i = 1; i <= ITERATIONS; i++) {
          MasterController.sleepForMs(sleepMs);
          for (long k = entryCounter - mapSize + 1; k <= entryCounter; k++) {
            String key = NameFactory.getObjectNameForCounter(k);
            QueryObject val = getQueryObjectForCounter(i);
            aMap.put(key, val);
          }
          region.putAll(aMap);
        }
      } else {
        Long keyCounter = bb.increamentAndReadKeyCounter(region.getName(), 1);
        String key = NameFactory
            .getObjectNameForCounter(keyCounter.longValue());

        logger.info("Working on key :" + key + " for region "
            + region.getFullPath());
        for (long i = 1; i <= ITERATIONS; i++) {
          MasterController.sleepForMs(sleepMs);
          QueryObject val = getQueryObjectForCounter(i);
          Object v = region.replace(key, val);
          if (v == null) { // force replacement
            region.put(key, val);
          }
        }
      }
    }
  }

  /**
   * Mention the reference vm in the blackboard
   */

  public static void mentionReferenceInBlackboard() {

    String VmDurableId = ((InternalDistributedSystem) InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    Log.getLogWriter().info("Reference vm is : " + VmDurableId);

    DurableClientsBB.getBB().getSharedMap().put("REFERENCE VM:", VmDurableId);
  }

  /**
   * Validate the events received by the clients with the reference Vm
   */
  public static void validateEventsReceived() {
    // hydra.MasterController.sleepForMs(150000);
    DurableClientsBB.getBB().printSharedMap();
    String ReferenceVm = (String) DurableClientsBB.getBB().getSharedMap()
        .get("REFERENCE VM:");
    String VmDurableId = ((InternalDistributedSystem) InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    HashMap currentVmMap = (HashMap) DurableClientsBB.getBB().getSharedMap()
        .get(VmDurableId);
    HashMap referenceMap = (HashMap) DurableClientsBB.getBB().getSharedMap()
        .get(ReferenceVm);
    if (currentVmMap.isEmpty()) {
      throw new TestException(" The map of threads is empty for the Vm "
          + VmDurableId);
    }
    if (referenceMap.isEmpty()) {
      throw new TestException(
          " The map of threads is empty for the Reference Vm " + ReferenceVm);
    }
    Iterator iterator = referenceMap.entrySet().iterator();
    Map.Entry entry = null;
    Object key = null;
    Object value = null;
    while (iterator.hasNext()) {
      entry = (Map.Entry) iterator.next();
      key = entry.getKey();
      value = entry.getValue();
      HashMap referenceThreadMap = (HashMap) value;
      Iterator iterator1 = referenceThreadMap.entrySet().iterator();
      Map.Entry entry1 = null;
      Object key1 = null;
      Object value1 = null;

      while (iterator1.hasNext()) {
        entry1 = (Map.Entry) iterator1.next();
        key1 = entry1.getKey();
        value1 = entry1.getValue();
        if (((String) key1).startsWith("EVENT SR. No : ")) {
          if (currentVmMap.get(key) == null) {
            throw new TestException("Map is null");
          }
          if (((HashMap) currentVmMap.get(key)).get(key1) == null) {

            throw new TestException(
                "Event not received by the client for the key " + key
                    + " for the event no " + key1);
          }
          if (!value1.equals(((HashMap) currentVmMap.get(key)).get(key1))) {
            throw new TestException(" For the key " + key + " the event no "
                + key1 + " has different events");
          }
        }

      }
    }

  }

  /**
   * Kill the clients
   */
  public synchronized static void killClient() {
    try {
      hydra.MasterController.sleepForMs(1000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    } catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }

  private Object getKeyForCounter(long key) {
    return NameFactory.getObjectNameForCounter(key);
  }

  private BaseValueHolder getValueForCounter(long counter) {
    String objectType = TestConfig.tab().stringAt(ValueHolderPrms.objectType,
        "util.ValueHolder");

    Object vh = opsClient.getValueForKey(NameFactory
        .getObjectNameForCounter(counter));
    if ((objectType.equals("util.PdxVersionedValueHolder"))
        || (objectType.equals("util.VersionedValueHolder"))) {
      return PdxTest.getVersionedValueHolder(objectType,
          ((ValueHolder) vh).getMyValue(), rv);
    } else {
      return (ValueHolder) vh;
    }
  }

  /**
   * Return a value for the given key
   */
  public QueryObject getQueryObjectForCounter(long key) {
    return new QueryObject(key, QueryObject.EQUAL_VALUES, 0, 0);
  }

  public void putSequentialKeyUsingPutAll(Region region, int sleepMs)
      throws Exception {
    int mapSize = 1;
    long opsCounter = bb.getSharedCounters().read(
        WANBlackboard.operation_counter);
    if ((opsCounter + ITERATIONS * mapSize) > maxOperations) {
      // time to stop scheduling
      throw new StopSchedulingOrder(
          "Time to stop as max number of operations reached. Operations="
              + opsCounter);
    }

    opsCounter = bb.getSharedCounters().add(WANBlackboard.operation_counter,
        (ITERATIONS * mapSize));
    Long entryCounter = bb.increamentAndReadKeyCounter(region.getName(),
        mapSize);
    Set keyMap = new TreeSet();
    for (long j = entryCounter - mapSize + 1; j <= entryCounter; j++) {
      keyMap.add(getKeyForCounter(j));
    }
    logger.info("Working on key :" + keyMap + " for region "
        + region.getFullPath() + ". PutAll with the map size of " + mapSize
        + " opsCounter=" + opsCounter);

    HashMap aMap = new HashMap();
    for (long i = 1; i <= ITERATIONS; i++) {
      MasterController.sleepForMs(sleepMs);
      for (long j = entryCounter - mapSize + 1; j <= entryCounter; j++) {
        Object key = getKeyForCounter(j);
        BaseValueHolder val = getValueForCounter(i);
        aMap.put(key, val);
      }
      region.putAll(aMap);
    }
    logger.info("Done putall on keys " + aMap.keySet());
  }

  public static void waitTask() {
    MasterController.sleepForMs(2 * 60 * 1000);
  }

  /**
   * Generates comma-separated lists of remote distributed system names.
   */
  public static String generateAllRemoteDSNameLists(String prefix, int n, int m) {
    return TestConfigFcns.generateNameListsRepeatedlyShift(prefix, n, m);
  }

  /**
   * Generates comma-separated lists of remote distributed system names in
   * random topology.
   */
  public static String generateRingRemoteDSName(String prefix, int n, int m) {
    return TestConfigFcns.generateNamesRepeatedlyShift(prefix, n, m, false,
        true);
  }

  /**
   * Generates comma-separated lists of remote distributed system names in
   * random topology.
   */
  public static String generateRandomRemoteDSName(String prefix, int n, int m) {
    String v = "";
    for (int i = 1; i <= n; i++) {
      for (int j = 1; j <= m; j++) {
        int suffix = i;
        do {
          suffix = rand.nextInt(1, n);
        } while (suffix == i);
        v += prefix + suffix;
        if (i * j < m * n) {
          v += ", ";
        }
      }
    }
    return v;
  }

  public static void killAndVerifyLocatorsTask() {
    instance.killAndVerifyLocators();
  }

  public void killAndVerifyLocators() {
    List<Locator> locators = Locator.getLocators();
    Map lMap = ((InternalLocator) locators.get(0)).getAllLocatorsInfo();

    logger.info("Distribution locator map before kill: " + lMap);

    int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String locatorName = "locator" + dsName.substring(dsName.indexOf("_"));
    logger.info("killAndVerifyLocators: Looking for " + numVMsToStop
        + " with matching client name " + locatorName);

    Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, locatorName);
    List vmList = (List) (tmpArr[0]);
    List stopModeList = (List) (tmpArr[1]);
    StopStartVMs.stopStartVMs(vmList, stopModeList);

    verifyLocators();
  }

  public static void restartVerifyLocatorsTask() {
    instance.startAndConnectLocator();
  }

  public static void printDistributionLocatorMapTask() {
    List<Locator> locators = Locator.getLocators();
    Map gfLocMap = ((InternalLocator) locators.get(0)).getAllLocatorsInfo();

    logger.info("Distribution locator map from gemfire system is " + gfLocMap);
  }

  public static void verifyLocatorsTask() {
    instance.verifyLocators();
  }

  public void verifyLocators() {
    logger.info("Verify locators status. Sleeping for "
        + LocatorDiscovery.WAN_LOCATOR_CONNECTION_TIMEOUT
        + " ms for locator to exchange locator metadata");
    MasterController
        .sleepForMs(LocatorDiscovery.WAN_LOCATOR_CONNECTION_TIMEOUT);

    Map gfLocMap = null;
    try {
      List<Locator> locators = Locator.getLocators();
      gfLocMap = ((InternalLocator) locators.get(0)).getAllLocatorsInfo();
    } catch (Exception e) {
      throw new TestException(
          "Unexpected Exception occurred while reading Distribution locator map."
              + TestHelper.getStackTrace(e));
    }

    logger.info("Distribution locator map from gemfire system is " + gfLocMap);

    Object omap = bb.getSharedMap().get(WANBlackboard.LOCATORS_MAP);
    logger.info("Distribution locator map from blackboard is " + omap);

    if (omap != null) {
      Map bbLocMap = (Map) omap;
      Set keys = bbLocMap.keySet();
      for (Object ds : keys) {
        String bbLocList = (String) bbLocMap.get(ds);
        String[] bbLocArray = bbLocList.split(",");

        if (gfLocMap.get(ds) == null) {
          throw new TestException("No locators found for distributed system "
              + ds + ", expected locators are :" + bbLocList);
        }

        Set<DistributionLocatorId> gfLocSet = (Set<DistributionLocatorId>) gfLocMap
            .get(ds);

        for (int i = 0; i < bbLocArray.length; i++) {
          String loc = bbLocArray[i];
          DistributionLocatorId dls = new DistributionLocatorId(loc);
          if (!gfLocSet.contains(dls)) {
            throw new TestException("Locator " + loc
                + " not found in distributed system " + ds);
          }
        }
        logger.info("locators are available in distribution list for " + ds
            + ". locators are " + bbLocArray.toString());
      }
    } else {
      throw new TestException("No locator found in blackboard.");
    }
  }

  /**
   * kill other peer and simultaneously do entry operations. This TASK should be
   * run with maxThreads = 1. It coordinates with other entry operations task
   * (ex doHAEntryOperationTask).
   */
  public synchronized static void killPeerAndDoOpsTask() {
    instance.killPeerAndDoOps();
  }

  public synchronized void killPeerAndDoOps() {
    // wait till one cycle of operation is done.
    long cycleCounter = WANOperationsClientBB.getBB().getSharedCounters()
        .read(WANOperationsClientBB.NumCycle);
    logger.info("HA cycle counter = " + cycleCounter);
    int numThreadsForTask = RemoteTestModule.getCurrentThread()
        .getCurrentTask().getTotalThreads();
    try {
      TestHelper.waitForCounter(WANOperationsClientBB.getBB(),
          "WANOperationsClientBB.NumFinishedDoingOps",
          WANOperationsClientBB.NumFinishedDoingOps, (numThreadsForTask - 1),
          false, 120000);
    } catch (TestException e) {
      // simply return
      logger
          .info("Returning as timeout while waiting for WANOperationsClientBB.NumStartedDoingOps");
      return;
    }

    // restart vms
    int numVMsToStop = TestConfig.tasktab().intAt(StopStartPrms.numVMsToStop,
        TestConfig.tab().intAt(StopStartPrms.numVMsToStop));
    if (numVMsToStop <= 0) {
      logger
          .warning("killPeerAndDoOps: Not doing anything as number of vm requested to kill is "
              + numVMsToStop);
      return;
    }

    // get target vm prefix
    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String peerName;
    if (isBridgeConfiguration) {
      String vmName = RemoteTestModule.getMyClientName();
      peerName = "bridge"
          + vmName.substring(vmName.indexOf("_"),
              vmName.indexOf("_", vmName.indexOf("_") + 1));
    } else {
      peerName = "peer" + dsName.substring(dsName.indexOf("_"));
    }

    PRObserver.initialize();
    logger.info("killPeerAndDoOps: Looking for " + numVMsToStop
        + " vm with client name " + peerName);
    Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, peerName);
    List vmList = (List) (tmpArr[0]);
    List stopModeList = (List) (tmpArr[1]);
    List otherVms = (List) (tmpArr[2]);
    // this asychronously starts the thread to stop and start
    List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);

    // meanwhile, we will do operations
    doEntryOperationAllRegions();

    // wait for restarted vm to join
    StopStartVMs.joinStopStart(vmList, threads);
    logger.info("killPeerAndDoOps: done restarting " + vmList);

    // reset numDoingOps counter, and increment cycle counter
    WANOperationsClientBB.getBB().getSharedCounters()
        .zero(WANOperationsClientBB.NumStartedDoingOps);
    WANOperationsClientBB.getBB().getSharedCounters()
        .zero(WANOperationsClientBB.NumFinishedDoingOps);
    WANOperationsClientBB.getBB().getSharedCounters()
        .increment(WANOperationsClientBB.NumCycle);

    // wait to recovery after vm restart
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (!regionDescriptName.startsWith("client")
          && !regionDescriptName.startsWith("accessor")) {
        RegionDescription rd = RegionHelper
            .getRegionDescription(regionDescriptName);
        DataPolicy policy = rd.getDataPolicy();
        if (policy.equals(DataPolicy.PARTITION)
            || policy.equals(DataPolicy.PERSISTENT_PARTITION)) {
          waitForRecovery(vmList, otherVms);
          break;
        }
      }
    }

    // simply wait in case of bridge clients
    if (isBridgeClient) {
      MasterController.sleepForMs(10 * 1000);
    }
    logger.info("killPeerAndDoOps: done");
  }

  /**
   * kill other peer and simultaneously do entry operations. This TASK should be
   * run with maxThreads = 1. No coordination is provided with other entry
   * operations task.
   */
  public synchronized static void killPeerAndDoNonCoordinatedOpsTask() {
    instance.killPeerAndDoNonCoordinatedOps();
  }

  public synchronized void killPeerAndDoNonCoordinatedOps() { // restart vms
    int numVMsToStop = TestConfig.tasktab().intAt(StopStartPrms.numVMsToStop,
        TestConfig.tab().intAt(StopStartPrms.numVMsToStop));
    if (numVMsToStop <= 0) {
      logger
          .warning("killPeerAndDoOps: Not doing anything as number of vm requested to kill is "
              + numVMsToStop);
      return;
    }

    // get target vm prefix
    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String peerName;
    if (isBridgeConfiguration) {
      String vmName = RemoteTestModule.getMyClientName();
      peerName = "bridge"
          + vmName.substring(vmName.indexOf("_"),
              vmName.indexOf("_", vmName.indexOf("_") + 1));
    } else {
      peerName = "peer" + dsName.substring(dsName.indexOf("_"));
    }

    PRObserver.initialize();
    logger.info("killPeerAndDoOps: Looking for " + numVMsToStop
        + " vm with client name " + peerName);
    Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, peerName);
    List vmList = (List) (tmpArr[0]);
    List stopModeList = (List) (tmpArr[1]);
    List otherVms = (List) (tmpArr[2]);
    // this asychronously starts the thread to stop and start
    List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);

    // meanwhile, we will do operations
    doEntryOperationAllRegions();

    // wait for restarted vm to join
    StopStartVMs.joinStopStart(vmList, threads);
    logger.info("killPeerAndDoOps: done restarting " + vmList);

    for (int j = 0; j < regionDescriptNames.size(); j++) {
      String regionDescriptName = (String) (regionDescriptNames.get(j));
      if (!regionDescriptName.startsWith("client")
          && !regionDescriptName.startsWith("accessor")) {
        RegionDescription rd = RegionHelper
            .getRegionDescription(regionDescriptName);
        DataPolicy policy = rd.getDataPolicy();
        if (policy.equals(DataPolicy.PARTITION)
            || policy.equals(DataPolicy.PERSISTENT_PARTITION)) {
          waitForRecovery(vmList, otherVms);
          break;
        }
      }
    }

    // simply wait in case of bridge clients
    if (isBridgeClient) {
      MasterController.sleepForMs(10 * 1000);
    }
    logger.info("killPeerAndDoOps: done");
  }

  public void waitForRecovery(List startupRecoveryVms, List departedRecoveryVMs) {
    // wait to recovery after vm restart
    List prnames = new ArrayList();
    Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
    for (Region r : regions) {
      prnames.add(r.getFullPath());
    }

    for (GatewaySender s : gatewaySenders) {
      /*if (s instanceof ParallelGatewaySenderImpl) {
        RegionQueue queue = ((ParallelGatewaySenderImpl) s).getQueues()
            .toArray(new RegionQueue[1])[0];
        prnames.add(queue.getRegion().getFullPath());
      }*/
      if (s instanceof ParallelGatewaySenderImpl) {
        Set<RegionQueue> queues = ((ParallelGatewaySenderImpl) s).getQueues();
        if (queues == null) continue;
        ConcurrentParallelGatewaySenderQueue prq = (ConcurrentParallelGatewaySenderQueue) queues.toArray(new RegionQueue[1])[0];
        if (null == prq.getRegion()) {
          Set prs = prq.getRegions();
          for (Iterator sit = prs.iterator(); sit.hasNext();) {
            prnames.add(((PartitionedRegion) sit.next())
                .getRegion().getFullPath());
          }
        } else {
          prnames.add(prq.getRegion().getFullPath());
        }
      }
    }

    if (isBridgeClient) {
      String vmName = RemoteTestModule.getMyClientName();
      String dsid = (vmName.split("_"))[1];
      String senderKey = SENDER_NAME_PREFIX + dsid;
      Set queueRegions = (Set) bb.getSharedMap().get(senderKey);
      prnames.addAll(queueRegions);
    }

    logger.info("waiting for startup recovery in " + startupRecoveryVms
        + ", for regions " + prnames);
    PRObserver.waitForRebalRecov(startupRecoveryVms, 1, prnames.size(), null,
        null, false);
    logger.info("done waiting for recovery activity for " + startupRecoveryVms
        + " for regions " + prnames);
  }

  public static void restartMembersTask() {
    instance.restartMembers();
  }

  public void restartMembers() {
    String clientVmName = WANTestPrms.getMemberClientName();

    int numVMsToStop = TestConfig.tasktab().intAt(StopStartPrms.numVMsToStop,
        TestConfig.tab().intAt(StopStartPrms.numVMsToStop));
    logger.info("Inside restartWanSite method. Looking for " + numVMsToStop
        + " vms with client name " + clientVmName);

    Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, clientVmName);
    List vmList = (List) (tmpArr[0]);
    List stopModeList = (List) (tmpArr[1]);
    StopStartVMs.stopStartVMs(vmList, stopModeList);
  }

  public static void doSenderOperationTask() {
    opsClient.doSenderOperations();
  }

  public static void doHASenderOperationAndVerifyTask() {
    opsClient.doHASenderOperationsAndVerify();
  }

  public static void verifySenderOperationTask() {
    opsClient.verifySenderOperations();
  }

  public static void startResumeSenderTask() throws Exception {
    instance.startResumeSenders();
  }

  public void startResumeSenders() throws Exception {
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
    for (GatewaySender sender : senders) {
      if (!sender.isRunning()) {
        logger.info("startResumeSenders: starting sender " + sender.getId());
        sender.start();
        logger.info("startResumeSenders: started sender " + sender.getId());
      } else if (sender.isPaused()) {
        logger.info("startResumeSenders: resuming sender " + sender.getId());
        sender.resume();
        logger.info("startResumeSenders: resumed sender " + sender.getId());
      } else {
        logger
            .info("startResumeSenders: sender already running, no operation done on sender "
                + sender.getId());
      }
    }
  }

  /**
   * Create the pdx disk store if one was specified.
   */
  private void initPdxDiskStore() {
    if (CacheHelper.getCache().getPdxPersistent()) {
      String pdxDiskStoreName = TestConfig.tab().stringAt(
          CachePrms.pdxDiskStoreName, null);
      if (pdxDiskStoreName != null) {// pdx disk store name was specified
        if (CacheHelper.getCache().findDiskStore(pdxDiskStoreName) == null) {
          DiskStoreHelper.createDiskStore(pdxDiskStoreName);
        }
      }
    }
  }

  /**
   * Generates a list of strings with the specified prefix and suffixed x_y,
   * where x varies start_n,...,n and y varies start_m,...,m, and x varies first
   * if varyFirst is true, else y. For example:
   * <p>
   * generateDoubleSuffixedNames("frip", 3, 2, 1, 3, true, false) yields:
   * frip_1_3 frip_2_3 frip_3_3 frip_1_4 frip_2_4 frip_3_4.
   * <p>
   * generateDoubleSuffixedNames("frip", 3, 2, 1, 3, false, false) yields:
   * frip_1_3 frip_1_4 frip_2_3 frip_2_4 frip_3_3 frip_3_4.
   */
  public static String generateDoubleSuffixedNames(String prefix, int n, int m,
      int start_n, int start_m, boolean varyFirst, boolean useComma) {
    String v = "";
    if (varyFirst) {
      for (int j = start_m; j < m + start_m; j++) {
        for (int i = start_n; i < n + start_n; i++) {
          v += prefix + "_" + i + "_" + j;
          if (i * j < (m + start_m - 1) * (n + start_n - 1)) {
            if (useComma)
              v += ",";
            v += " ";
          }
        }
      }
    } else {
      for (int i = start_n; i < n + start_n; i++) {
        for (int j = start_m; j < m + start_m; j++) {
          v += prefix + "_" + i + "_" + j;
          if (i * j < (m + start_m - 1) * (n + start_n - 1)) {
            if (useComma)
              v += ",";
            v += " ";
          }
        }
      }
    }
    return v;
  }
}
