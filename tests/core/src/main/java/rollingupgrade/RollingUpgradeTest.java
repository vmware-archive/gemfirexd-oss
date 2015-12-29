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
package rollingupgrade;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ConfigPrms;
import hydra.DiskStoreDescription;
import hydra.HydraThreadGroup;
import hydra.Log;
import hydra.MasterController;
import hydra.PoolDescription;
import hydra.Prms;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.shared.Version;

import parReg.ParRegUtil;
import util.BaseValueHolder;
import util.DeclarativeGenerator;
import util.NameFactory;
import util.OperationsClient;
import util.SilenceListener;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import rollingupgrade.RollingUpgradeBB;
import rollingupgrade.RollingUpgradePrms;

public class RollingUpgradeTest extends OperationsClient {
  
  private static RollingUpgradeTest testInstance = new RollingUpgradeTest();
  protected List<Region> allRegions = new ArrayList();
  // static fields
  static Cache theCache = null;
  // bb keys
  private static final String xmlFileNameKeyPrefix = "xmlForVmId_";
  private static final String allRegionsSnapshotKey = "allRegionsSnapshot";
  
  public synchronized static void HydraTask_createLocator() throws Throwable {
    hydra.DistributedSystemHelper.createLocator();
  }

  public synchronized static void HydraTask_startLocatorAndDS()
      throws Throwable {
    hydra.DistributedSystemHelper.startLocatorAndDS();
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() {
    testInstance = new RollingUpgradeTest();
    theCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
    
    Log.getLogWriter().info("The cache: "+ theCache);
    Log.getLogWriter().info("Current gemfire version: " + Version.CURRENT);
    createRegions();

    if (ConfigPrms.getBridgeConfig() != null) {
      BridgeHelper.startBridgeServer("bridge");
    } else {//it is an edge client
      
      registerInterest();
    }
    
    Log.getLogWriter().info("Use cache xml :" + TestConfig.tab().booleanAt(RollingUpgradePrms.useCacheXml, false));
    
    // Generate cache xml file reboot
    if (TestConfig.tab().booleanAt(RollingUpgradePrms.useCacheXml, false)) {
      Log.getLogWriter().info("Generating cache xml");
      if (ConfigPrms.getBridgeConfig() != null) {
        HydraTask_createXmlFile();
      } else {//it is an edge client
        createXmlFile_Client(ConfigPrms.getRegionConfig(), ConfigPrms.getCacheConfig());
      }
    }
  }
  
  /** Creates and initializes a server or peer using xml from an oldVersion jvm. 
   */
  public synchronized static void HydraTask_initializeWithXml() {
    createRegionsWithXml();
    
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    Log.getLogWriter().info("Current thread groip name is: "+ tgname);
    if (tgname.equals("edge")) {
      registerInterest();
    }
  }
  
  /** Initialize the compatibility test controller
   * 
   */
  public synchronized static void HydraTask_initController() {
    testInstance = new RollingUpgradeTest();
  }
  
   
  /** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
   *  which is equivalent to a full GII.
   */
  protected static void registerInterest() {
    Log.getLogWriter().info("Calling registerInterest for different regions");
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

  /** Create region(s) in hydra.RegionPrms.names, using the hydra param
   *  to determine which regions specified in hdyra.RegionPrms.names to create.
   */
  private static void createRegions() {
    //List<String> regionConfigNamesList = TestConfig.tab().vecAt(RegionPrms.names);
    String regionConfig = ConfigPrms.getRegionConfig();
    Region aRegion = RegionHelper.createRegion(regionConfig);
    Log.getLogWriter().info("Root regions in the cache: " + CacheHelper.getCache().rootRegions().toString());
    testInstance.allRegions.add(aRegion);
  }
  
  /** Given a vmId, return the xml file name for it
   * 
   * @param vmId The vmId of a test jvm.
   * @return The xml file name (not the complete path) of the xml file for the vmId.
   */
  protected static String getXmlFileName(int vmId) {
    final String xmlFileNamePrefix = "vmId_";
    String fileName = xmlFileNamePrefix + vmId + ".xml";
    return fileName;
  }
  
  /** Create an xml file for the current cache and all its regions and write it
   *  to the blackboard for later use. 
   * 
   */
  public static synchronized void HydraTask_createXmlFile() {
    // create the xml file
    String fileName = getXmlFileName(RemoteTestModule.getMyVmid());
    File aFile = new File(fileName);
    if (aFile.exists()) {
       return;
    }
    DeclarativeGenerator.createDeclarativeXml(fileName, CacheHelper.getCache(), false, true);
    
    // write the xml file name to the blackboard
    RollingUpgradeBB.getBB().getSharedMap().put(xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid(), aFile.getAbsolutePath());
  }
  
  /** Create an xml file for the current cache and all its regions and write it
   *  to the blackboard for later use. 
   * 
   */
  public static synchronized void createXmlFile_Client(String regionDescriptName, String cacheConfig) {
    // create the xml file
    String fileName = getXmlFileName(RemoteTestModule.getMyVmid());
    File aFile = new File(fileName);
    if (aFile.exists()) {
       return;
    }
    PoolDescription poolDescr = RegionHelper.getRegionDescription(regionDescriptName).getPoolDescription();
    DiskStoreDescription desc = RegionHelper.getRegionDescription(regionDescriptName).getDiskStoreDescription();
    String diskStoreName = null;
    if (desc != null) {
        diskStoreName = desc.getName();
    }
    Log.getLogWriter().info("About to generate xml, diskStoreName is " + diskStoreName);
    Log.getLogWriter().info("About to generate xml, poolDescr is " + poolDescr);
    if(poolDescr != null) {
      CacheHelper.generateCacheXmlFile(cacheConfig, null, regionDescriptName, null, null, poolDescr.getName(), diskStoreName, null, fileName);
    }

    
   // DeclarativeGenerator.createDeclarativeXml(fileName, CacheHelper.getCache(), false, true);
    
    // write the xml file name to the blackboard
    RollingUpgradeBB.getBB().getSharedMap().put(xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid(), aFile.getAbsolutePath());
  }
  
  
  /** Create cache/regions with an xml file retrieved from the blackboard
   * 
   */
  private static void createRegionsWithXml() {
    String bbKey = xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid();
    String xmlFilePath = (String) RollingUpgradeBB.getBB().getSharedMap().get(bbKey);
    theCache = CacheHelper.createCacheFromXml(xmlFilePath);
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  public static void HydraTask_doOperationsAndPauseVerify() {
    testInstance.doOperationsAndPauseVerify();
  }
  
  public static void HydraTask_doOperations() {
    testInstance.doOperations();
  }
  
  private void doOperations() {
    long opsTaskGranularitySec = TestConfig.tab().longAt(RollingUpgradePrms.opsTaskGranularitySec);
    long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    performOps(minTaskGranularityMS);
    if (RollingUpgradeBB.getBB().getSharedCounters().read(RollingUpgradeBB.recycledAllVMs) != 0) {
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }
  
  private void doOperationsAndPauseVerify() {
    int numTotalVMs = StopStartVMs.getAllVMs().size();
    long opsTaskGranularitySec = TestConfig.tab().longAt(RollingUpgradePrms.opsTaskGranularitySec);
    long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    if (RollingUpgradeBB.getBB().getSharedCounters().read(RollingUpgradeBB.pausing) == 0) {
      performOps(minTaskGranularityMS);
    }
    
    Log.getLogWriter().info("Done performing one invocation of performOps()");
    if (RollingUpgradeBB.getBB().getSharedCounters().read(RollingUpgradeBB.pausing) > 0) {
      // Controller has restarted one VM and we are ready for validation.
      RollingUpgradeBB.getBB().getSharedCounters().increment(RollingUpgradeBB.pausing);
      HydraThreadGroup tg1 = TestConfig.getInstance().getThreadGroup("bridge");
      HydraThreadGroup tg2 = TestConfig.getInstance().getThreadGroup("edge");
      int threadCount = tg1.getTotalThreads() + tg2.getTotalThreads(); 
      int desiredCounterValue = threadCount + 1; // +1 because the controller initiates pausing by incrementing the counter
      TestHelper.waitForCounter(RollingUpgradeBB.getBB(), "RollingUpgradeBB.pausing", 
          RollingUpgradeBB.pausing, desiredCounterValue, true, -1, 2000);
      // now all the vm threads have paused
      SilenceListener.waitForSilence(30, 2000);

      
      // elect leader
      long leader = RollingUpgradeBB.getBB().getSharedCounters().incrementAndRead(RollingUpgradeBB.leader);
      if (leader == 1) {
        writeSnapshot();
        // notify all the waiting threads that the snapshot has been written.
        RollingUpgradeBB.getBB().getSharedCounters().increment(RollingUpgradeBB.snapshotWritten);
        RollingUpgradeBB.getBB().getSharedCounters().decrement(RollingUpgradeBB.pausing);
      } else {
        TestHelper.waitForCounter(RollingUpgradeBB.getBB(), "RollingUpgradeBB.snapshotWritten", 
            RollingUpgradeBB.snapshotWritten, 1, true, -1, 2000);
        verifyFromSnapshot();
        // Ops thread is here means the controller is waiting for threads to finish verification
        // So its  a safe time to reset pausing to 0 here.
        RollingUpgradeBB.getBB().getSharedCounters().decrement(RollingUpgradeBB.pausing);
      }
      if (RollingUpgradeBB.getBB().getSharedCounters().read(RollingUpgradeBB.pausing) == 1) {
        // This is the last thread, so make it zero so that controller gets unblocked. 
        RollingUpgradeBB.getBB().getSharedCounters().setIfSmaller(RollingUpgradeBB.pausing, 0);
      }
      TestHelper.waitForCounter(RollingUpgradeBB.getBB(), "RollingUpgradeBB.pausing", 
          RollingUpgradeBB.pausing, 0, true, -1, 2000);
    }
    if (RollingUpgradeBB.getBB().getSharedCounters().read(RollingUpgradeBB.recycledAllVMs) != 0) {
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }
  
  private void performOps(long taskTimeMS) {
    super.initializeOperationsClient();
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {  // can happen during HA
      return; 
    }
    long startTime = System.currentTimeMillis();
    do {
      Set<Region<?, ?>> rootRegions = theCache.rootRegions();
      for (Region aRegion: rootRegions) {
        super.doEntryOperations(aRegion);
      }
      Log.getLogWriter().info("Done performing one batch of operations");
    } while((System.currentTimeMillis() - startTime < taskTimeMS));
  }
  
  public static void HydraTask_UpgradeController() {
    testInstance.rollUpgradeVMs(true);
  }
  
  public static void HydraTask_UpgradeControllerNoVerify() {
    testInstance.rollUpgradeVMs(false);
  }
  
  public static void HydraTask_verifySnapshot() {
    testInstance.verifySnapshot();
  }
  
  private void rollUpgradeVMs(boolean waitForVerification) {
    List<ClientVmInfo> locatorVMs = StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), "locator");
    List<ClientVmInfo> bridgeVMs = StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), "bridge");
    List<ClientVmInfo> edgeVMs = StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), "edge");
    do {
      
      // This is check to make sure no thread is in its verification state.
      if(waitForVerification) {
        TestHelper.waitForCounter(RollingUpgradeBB.getBB(), "RollingUpgradeBB.pausing", 
            RollingUpgradeBB.pausing, 0, true, -1, 2000);
      }
      
      ClientVmInfo vmInfo = null;
      // For upgrade we will follow a sequence of locators -> bridge servers -> edge clients.
      // This order is predefined and being recommended to customers for rolling upgrade.
      if (locatorVMs.size() != 0) {
        vmInfo = locatorVMs.get(0);
        locatorVMs.remove(0);
      } else if (bridgeVMs.size() != 0) {
        vmInfo = bridgeVMs.get(0);
        bridgeVMs.remove(0);
      } else if (edgeVMs.size() != 0) {
        vmInfo = edgeVMs.get(0);
        edgeVMs.remove(0);
      }
      
      MasterController.sleepForMs(15000);
      
      StopStartVMs.stopVM(vmInfo, "nice_exit");
      Log.getLogWriter().info("Sleeping for " + 2 + " seconds to allow ops to run...");
      MasterController.sleepForMs(2 * 1000);
      StopStartVMs.startVM(vmInfo);
      
      MasterController.sleepForMs(15000);
      if (waitForVerification) {
        // Controller is here means that none of the threads are pausing or verifying.
        RollingUpgradeBB.getBB().getSharedCounters().setIfSmaller(RollingUpgradeBB.snapshotWritten, 0);
        RollingUpgradeBB.getBB().getSharedCounters().setIfSmaller(RollingUpgradeBB.leader, 0);
        // Notify the threads that they can start pausing now for verification.
        RollingUpgradeBB.getBB().getSharedCounters().increment(RollingUpgradeBB.pausing);
      }
      Log.getLogWriter().info("Locator VMs size: " + locatorVMs.size());
      Log.getLogWriter().info("Bridge VMs size: " + bridgeVMs.size());
      Log.getLogWriter().info("Edge VMs size: " + edgeVMs.size());
    } while (locatorVMs.size() != 0 || bridgeVMs.size() != 0 || edgeVMs.size() != 0);
    RollingUpgradeBB.getBB().getSharedCounters().increment(RollingUpgradeBB.recycledAllVMs);
  }

  public static void HydraTask_UpgradeLocators() throws Exception {
    if (testInstance == null) {
      testInstance = new RollingUpgradeTest();      
    }
    testInstance.upgradeLocators();
  }
  
  private void upgradeLocators() throws Exception {
    List vmInfoList = StopStartVMs.getAllVMs();
    int myVmID = RemoteTestModule.getMyVmid();
    List<ClientVmInfo> locatorVMs = new ArrayList();
    Log.getLogWriter().info("VMInfo list" + vmInfoList);
    for (int i = 0; i < vmInfoList.size(); i++) {
      
      Object anObj = vmInfoList.get(i);
      Log.getLogWriter().info("VM info obj :" + anObj);
      if (anObj instanceof ClientVmInfo) {
        ClientVmInfo info = (ClientVmInfo) (anObj);
        Log.getLogWriter().info("info.getClientName()" + info.getClientName());
        if (info.getClientName().indexOf("locator") >= 0) { // its a match
          if(info.getVmid().intValue() != myVmID) {
            locatorVMs.add(info);            
          }
        }
      }
    }

    Log.getLogWriter().info("locatorVMs" + locatorVMs);
    while (locatorVMs.size() != 0) {
      ClientVmInfo vmInfo = null;
      if (locatorVMs.size() != 0) {
        vmInfo = locatorVMs.get(0);
        locatorVMs.remove(0);
      }
      MasterController.sleepForMs(15000);

      StopStartVMs.stopVM(vmInfo, "nice_exit");
      Log.getLogWriter().info(
          "Sleeping for " + 2 + " seconds to allow ops to run...");
      MasterController.sleepForMs(2 * 1000);
      StopStartVMs.startVM(vmInfo);

      MasterController.sleepForMs(15000);
      Log.getLogWriter().info("Locator VMs size: " + locatorVMs.size());

    }
    MasterController.sleepForMs(15000);
    ClientVmMgr.stopAsync("Killing self at version: " + GemFireVersion.getGemFireVersion(), ClientVmMgr.NICE_KILL, ClientVmMgr.IMMEDIATE);
  }
  
  private void verifySnapshot() {
    SilenceListener.waitForSilence(60, 2000);
    // elect leader
    long leader = RollingUpgradeBB.getBB().getSharedCounters().incrementAndRead(RollingUpgradeBB.leader);
    if (leader == 1) {
      writeSnapshot();
      // notify all the waiting threads that the snapshot has been written.
      RollingUpgradeBB.getBB().getSharedCounters().increment(RollingUpgradeBB.snapshotWritten);
    } else {
      TestHelper.waitForCounter(RollingUpgradeBB.getBB(), "RollingUpgradeBB.snapshotWritten", 
          RollingUpgradeBB.snapshotWritten, 1, true, -1, 2000);
      verifyFromSnapshot();
    }
  }
  
  
  /**
   * Write snapshot of all the regions into BB
   *
   */
  Map writeSnapshot() {
    Map allRegionsSnapshot = new HashMap();
    Set<Region<?,?>>  regions = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info("Preparing to write snapshot for " + regions.size() + " regions");
    for (Region aRegion: regions) {
      Map regionSnapshot = new HashMap();
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
      allRegionsSnapshot.put(aRegion.getFullPath(), regionSnapshot);
      Log.getLogWriter().info("Region snapshot for " + aRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    }
    RollingUpgradeBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    Log.getLogWriter().info("Put snapshot for " + regions.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
    
    return allRegionsSnapshot;
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm. 
   * 
   */
  protected void verifyFromSnapshot() {
    Map<String, Map> allRegionsSnapshot = (Map)(RollingUpgradeBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
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
  
  public static void HydraTask_loadRegions() {
    testInstance.loadRegions();
  }
  
  /** Load each region in this vm with PdxPrms.numToLoadPerRegion entries.
   * 
   */
  public void loadRegions() {
    Log.getLogWriter().info("cache : " + CacheHelper.getCache());
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    long numToLoadPerRegion = RollingUpgradePrms.getNumToLoad();
    Map putAllMap = new HashMap();
    final int putAllMapMaximum = 500;
    Log.getLogWriter().info("rootRegions : " + rootRegions);
    
    Log.getLogWriter().info("Creating " + numToLoadPerRegion + " entries in each of " + rootRegions.size() + " regions...");

    long loadCoordinator = RollingUpgradeBB.getBB().getSharedCounters().incrementAndRead(RollingUpgradeBB.loadCoordinator);
    
    while (loadCoordinator <= numToLoadPerRegion) {
      String key = NameFactory.getNextPositiveObjectName();
      Object value = getValueForKey(key);
      putAllMap.put(key, value);
      if (putAllMap.size() >= putAllMapMaximum) {
        for (Region aRegion: rootRegions) {
          Log.getLogWriter().info("Putting(1) " + putAllMap.size() + " entries into " + aRegion.getFullPath());
          aRegion.putAll(putAllMap);
        }
        putAllMap = new HashMap();
      }
      loadCoordinator = RollingUpgradeBB.getBB().getSharedCounters().incrementAndRead(RollingUpgradeBB.loadCoordinator);
    }
    if (putAllMap.size() > 0) {
      for (Region aRegion: rootRegions) {
        Log.getLogWriter().info("Putting(2) " + putAllMap.size() + " entries into " + aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    }

    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }
  
  
}
