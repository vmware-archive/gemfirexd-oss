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
package pdx.compat;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.ClientVmInfo;
import hydra.DiskStoreHelper;
import hydra.DiskStorePrms;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parReg.ParRegUtil;
import pdx.PdxBB;
import pdx.PdxPrms;
import pdx.PdxTest;
import util.AdminHelper;
import util.BaseValueHolder;
import util.CacheUtil;
import util.DeclarativeGenerator;
import util.NameFactory;
import util.OperationsClientPrms;
import util.RandomValues;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.VHDataSerializable;
import util.ValueHolder;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;

import diskRecovery.RecoveryBB;
import diskRecovery.RecoveryPrms;
import diskRecovery.RecoveryTest;
import diskRecovery.RecoveryTestVersionHelper;

/* Class to test pdx compatibility between minor GemFire version numbers
 * 
 */
public class PdxCompatTest {

  /** instance of this test class */
  public static PdxCompatTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;
  private boolean useShutDownAll;
  private static RandomValues randomValues = new RandomValues();

  // backwards compatibility fields
  private List<ClientVmInfo> upVMs = new ArrayList();
  private List<ClientVmInfo> downVMs = new ArrayList();

  // used to keep track of controller task progress so it can be batched
  // and resume where it left off
  private int currentControllerStep = controllerStep1;
  private static final int controllerStep1 = 1;
  private static final int controllerStep2 = 2;
  private static final int controllerStep3 = 3;
  
  // bb keys
  private static final String xmlFileNameKeyPrefix = "xmlForVmId_";
  
  //=================================================
  // initialization methods

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new PdxCompatTest();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        boolean registerInterest = RecoveryPrms.getRegisterInterest();
        if (registerInterest) {
          registerInterest();
        }
      }
    }
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    if (testInstance == null) {
      testInstance = new PdxCompatTest();
      CacheHelper.createCache("cache1");
      createRegions();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }
  
  /** Creates and initializes a server or peer using xml from an oldVersion jvm. 
   */
  public synchronized static void HydraTask_initializeWithXml() {
    RecoveryTestVersionHelper.initPdxClassLoader();
    if (testInstance == null) {
      testInstance = new PdxCompatTest();
      createRegionsWithXml();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }
  
  /** Initialize the compatibility test controller
   * 
   */
  public synchronized static void HydraTask_initController() {
    testInstance = new PdxCompatTest();
    testInstance.useShutDownAll = PdxPrms.getShutDownAll();
  }
  
  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    List<String> regionConfigNames = TestConfig.tab().vecAt(RegionPrms.names);
    useShutDownAll = PdxPrms.getShutDownAll();
    initPdxDiskStore();
    Log.getLogWriter().info("isBridgeConfiguration is " + isBridgeConfiguration);
    Log.getLogWriter().info("useShutDownAll is " + useShutDownAll);
  }

  /** Create region(s) in hydra.RegionPrms.names, using the hydra param
   *  PdxPrms.createProxyRegions to determine which regions specified in
   *  hdyra.RegionPrms.names to create.
   */
  private static void createRegions() {
    boolean createProxyRegions = PdxPrms.getCreateProxyRegions();
    List<String> regionConfigNamesList = TestConfig.tab().vecAt(RegionPrms.names);
    for (String regionConfigName: regionConfigNamesList) {
      String configNameLC = regionConfigName.toLowerCase();
      boolean isProxyRegion = ((configNameLC.indexOf("empty") >= 0) ||
          (configNameLC.indexOf("accessor") >= 0));
      if (isProxyRegion == createProxyRegions) {
        Log.getLogWriter().info("Creating region with config name " + regionConfigName);
        Region aRegion = RegionHelper.createRegion(regionConfigName);
        Log.getLogWriter().info("Done creating region with config name " + regionConfigName + ", region name is " + aRegion.getFullPath());
      }
    }
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }
  
  /** Create cache/regions with an xml file retrieved from the blackboard
   * 
   */
  private static void createRegionsWithXml() {
    String bbKey = xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid();
    String xmlFilePath = (String) PdxBB.getBB().getSharedMap().get(bbKey);
    CacheHelper.createCacheFromXml(xmlFilePath);
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  //=================================================
  // hydra task methods

  /** Load each region in this vm with PdxPrms.numToLoadPerRegion entries.
   * 
   */
  public static void HydraTask_loadRegions() {
    PdxTest.initClassLoader();
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    RandomValues rv = new RandomValues();
    long numToLoadPerRegion = PdxPrms.getNumToLoadPerRegion();
    Map putAllMap = new HashMap();
    final int putAllMapMaximum = 2000;
    final int logIntervalMS = 10000;
    Log.getLogWriter().info("Creating " + numToLoadPerRegion + " entries in each of " + rootRegions.size() + " regions...");

    long loadCoordinator = PdxBB.getBB().getSharedCounters().incrementAndRead(PdxBB.loadCoordinator);
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
      loadCoordinator = PdxBB.getBB().getSharedCounters().incrementAndRead(PdxBB.loadCoordinator);
    }
    if (putAllMap.size() > 0) {
      for (Region aRegion: rootRegions) {
        Log.getLogWriter().info("Putting(2) " + putAllMap.size() + " entries into " + aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    }

    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  /** Controller task for pdx enum rolling upgrade test
   * 
   */
  public static void HydraTask_compatWithEnumsController() {
    testInstance.compatWithEnumsController();
  }
 
  /** Controller task for pdx rolling upgrade test
   * 
   */
  public static void HydraTask_compatController() {
    testInstance.compatController();
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
    PdxBB.getBB().getSharedMap().put(xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid(), aFile.getAbsolutePath());
  }
  
  /** Log the blackboard shared map 
   * 
   */
  public static void HydraTask_printBBMap() {
    PdxBB.getBB().printSharedMap();
  }
  
  /** Verify the state of the PRs defined in the cache
   * 
   */
  public static void HydraTask_verifyPRs() {
    testInstance.verifyPRs();
  }
 
  //=================================================
  // other methods

  private void compatWithEnumsController() {
    Log.getLogWriter().info("In compatWithEnumsController with controller step " + currentControllerStep);
    final int secToSleep = 10;
    final int numToStop = 1;

    if (currentControllerStep == controllerStep1) { // stop new version jvms and sleep for ops to run
      // first do a test consistency check to make sure the number of old version jvms is equal to the
      // number of newVerisonWithFlag jvms is equal to the number of newVersionWithoutFlag jvms
      List allVMs = StopStartVMs.getAllVMs();
      List withFlagVMs = StopStartVMs.getMatchVMs(allVMs, "newVersionWithFlag");
      List withoutFlagVMs = StopStartVMs.getMatchVMs(allVMs, "newVersionWithoutFlag");
      List oldVersionVMs = StopStartVMs.getMatchVMs(allVMs, "oldVersion");
      if ((oldVersionVMs.size() != withFlagVMs.size()) || 
          (oldVersionVMs.size() != withoutFlagVMs.size())) {
        throw new TestException("Test problem; expect the number of oldVersion jvms " + oldVersionVMs.size() +
            " to be equal to the number of newVersionWithFlag jvms " + withFlagVMs.size() +
            " and to be equal to the number of newVersionWithoutFlag jvms " + withoutFlagVMs.size());
      }

      // stop all the new version vms (both with and without flag)
      List newVersionVMs = StopStartVMs.getMatchVMs(allVMs, "newVersion"); // include both with and without flag jvms
      List stopModeList = new ArrayList();
      for (int i = 0; i < newVersionVMs.size(); i++) {
        stopModeList.add("nice_exit");
      }
      StopStartVMs.stopVMs(newVersionVMs, stopModeList);

      // sleep for a while so the old version vms can run ops
      Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow ops to run...");
      MasterController.sleepForMs(secToSleep * 1000);

      // setup and advance to next step
      currentControllerStep++; 
      upVMs = oldVersionVMs;
      downVMs = withFlagVMs;
    } else if ((currentControllerStep == controllerStep2) ||
               (currentControllerStep == controllerStep3)) {
      // phase2 and phase3 are similar:
      //    phase2: stop some number of oldVersion jvms and replace with newVersionWithFlag jvms
      //    phase3: stop some number of newVersionWtihFlag jvms and replace with newVerisonWithoutFlag jvms
      // In either case, the jvms remaining to stop are in upVMs and the jvms remaining to start are in downVMs
      // Note: if the enum flag is used, it tells the new version jvms to use the 6.6 (and 6.6.1) pdx enum handling; if the flag is not
      //       used then the jvm uses the 6.6.2 new and improved pdx enum handling
      int numToStopThisTime = Math.min(upVMs.size(), numToStop);
      List<String> stopModeList = new ArrayList();
      List<ClientVmInfo> vmsToStop = new ArrayList();
      List<ClientVmInfo> vmsToStart = new ArrayList();
      SharedMap bbMap = PdxBB.getBB().getSharedMap();
      for (int i = 1; i <= numToStopThisTime; i++) {
        ClientVmInfo stopVmInfo = upVMs.remove(0);
        vmsToStop.add(stopVmInfo);
        stopModeList.add("nice_exit");
        ClientVmInfo startVmInfo = downVMs.remove(0);
        vmsToStart.add(startVmInfo);
        
        // write to the blackboard to tell the start vm which xml file to use for initialization
        bbMap.put(xmlFileNameKeyPrefix+startVmInfo.getVmid(), bbMap.get(xmlFileNameKeyPrefix+stopVmInfo.getVmid()));
      }
      PdxBB.getBB().printSharedMap();
      StopStartVMs.stopVMs(vmsToStop, stopModeList);
      StopStartVMs.startVMs(vmsToStart);

      // sleep for a while so the current set of jvms can run ops together
      Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow ops to run...");
      MasterController.sleepForMs(secToSleep * 1000);
      
      // for a persistence test, do a shutdownAll, then restart to do disk recovery with the current mix of jvms
      if (useShutDownAll) {
        AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
        PdxBB.getBB().getSharedCounters().increment(PdxBB.shutDownAllInProgress);
        Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS);
        PdxBB.getBB().getSharedCounters().decrement(PdxBB.shutDownAllInProgress);
        List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
        Set shutDownAllResults = (Set)(tmp[1]);
        List allVMs = StopStartVMs.getAllVMs();
        int expectedNumVMs = StopStartVMs.getMatchVMs(allVMs, "newVersionWithFlag").size();
        if (shutDownAllResults.size() != expectedNumVMs) { // shutdownAll did not return the expected number of members
          throw new TestException("Expected shutDownAllMembers to return " + expectedNumVMs +
              " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
        }
        PdxBB.getBB().getSharedCounters().increment(PdxBB.restartingAfterShutDownAll);
        StopStartVMs.startVMs(shutDownAllVMs);
        PdxBB.getBB().getSharedCounters().decrement(PdxBB.restartingAfterShutDownAll);
      }

      // determine if we are done with the test or need to advance to the next phase
      if (upVMs.size() == 0) { // all jvms have been stopped and replaced
        if (upVMs.size() != downVMs.size()) { //consistency check
          throw new TestException("Test problem; expected number of upVMs and number of downVMs to both be 0, num upVMs is " + upVMs.size() +
              ", num downVMs is " + downVMs.size());
        }
        currentControllerStep++;
        if (currentControllerStep == controllerStep3) {
          List allVMs = StopStartVMs.getAllVMs();
          upVMs = StopStartVMs.getMatchVMs(allVMs, "newVersionWithFlag");
          downVMs = StopStartVMs.getMatchVMs(allVMs, "newVersionWithoutFlag");
        } else if (currentControllerStep > controllerStep3) {
          Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow final ops to run...");
          MasterController.sleepForMs(secToSleep * 1000);
          throw new StopSchedulingOrder("Rolling upgrade has completed");
        }
      }
    }
  }
  
  private void compatController() {
    Log.getLogWriter().info("In compatController");
    final int secToSleep = 10;
    final int numToStop = 1;

    if (upVMs.size() == 0) { // this is the first time this method has executed; do initialization
      // first do a test consistency check to make sure the number of old version jvms is equal to the
      // number of newVerison jvms
      List allVMs = StopStartVMs.getAllVMs();
      List newVersionVMs = StopStartVMs.getMatchVMs(allVMs, "newVersion");
      List oldVersionVMs = StopStartVMs.getMatchVMs(allVMs, "oldVersion");
      if ((oldVersionVMs.size() != newVersionVMs.size())) {
        throw new TestException("Test problem; expect the number of oldVersion jvms " + oldVersionVMs.size() +
            " to be equal to the number of newVersion jvms " + newVersionVMs.size());
      }

      // stop all the new version vms
      List stopModeList = new ArrayList();
      for (int i = 0; i < newVersionVMs.size(); i++) {
        stopModeList.add("nice_exit");
      }
      StopStartVMs.stopVMs(newVersionVMs, stopModeList);
      upVMs = oldVersionVMs;
      downVMs = newVersionVMs;
    }

    // sleep for a while so the current vms can run ops
    Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow ops to run...");
    MasterController.sleepForMs(secToSleep * 1000);

    // stop some number of oldVersion jvms and replace with newVersion jvms
    // the jvms remaining to stop are in upVMs and the jvms remaining to start are in downVMs
    int numToStopThisTime = Math.min(upVMs.size(), numToStop);
    List stopModeList = new ArrayList();
    List<ClientVmInfo> vmsToStop = new ArrayList();
    List<ClientVmInfo> vmsToStart = new ArrayList();
    SharedMap bbMap = PdxBB.getBB().getSharedMap();
    for (int i = 1; i <= numToStopThisTime; i++) {
      ClientVmInfo stopVmInfo = upVMs.remove(0);
      vmsToStop.add(stopVmInfo);
      stopModeList.add("nice_exit");
      ClientVmInfo startVmInfo = downVMs.remove(0);
      vmsToStart.add(startVmInfo);

      // write to the blackboard to tell the start vm which xml file to use for initialization
      bbMap.put(xmlFileNameKeyPrefix+startVmInfo.getVmid(), bbMap.get(xmlFileNameKeyPrefix+stopVmInfo.getVmid()));
    }
    PdxBB.getBB().printSharedMap();
    StopStartVMs.stopVMs(vmsToStop, stopModeList);
    StopStartVMs.startVMs(vmsToStart);

    // sleep for a while so the current set of jvms can run ops together
    Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow ops to run...");
    MasterController.sleepForMs(secToSleep * 1000);

    // for a persistence test, do a shutdownAll, then restart to do disk recovery with the current mix of jvms
    if (useShutDownAll) {
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      PdxBB.getBB().getSharedCounters().increment(PdxBB.shutDownAllInProgress);
      Object[] tmp = StopStartVMs.shutDownAllMembers(adminDS);
      PdxBB.getBB().getSharedCounters().decrement(PdxBB.shutDownAllInProgress);
      List<ClientVmInfo> shutDownAllVMs = (List<ClientVmInfo>)tmp[0];
      Set shutDownAllResults = (Set)(tmp[1]);
      List allVMs = StopStartVMs.getAllVMs();
      // expectedNumVMs is the number of vms that are always up; this is equal
      // to the total number of newVersion OR oldVersion vms; we have already
      // established with a consistency check that the number of newVersion
      // and oldVersion vms are equal
      int expectedNumVMs = StopStartVMs.getMatchVMs(allVMs, "newVersion").size();
      if (shutDownAllResults.size() != expectedNumVMs) { // shutdownAll did not return the expected number of members
        throw new TestException("Expected shutDownAllMembers to return " + expectedNumVMs +
            " members in its result, but it returned " + shutDownAllResults.size() + ": " + shutDownAllResults);
      }
      PdxBB.getBB().getSharedCounters().increment(PdxBB.restartingAfterShutDownAll);
      StopStartVMs.startVMs(shutDownAllVMs);
      PdxBB.getBB().getSharedCounters().decrement(PdxBB.restartingAfterShutDownAll);
    }

    // determine if we are done with the test or need to advance to the next phase
    if (upVMs.size() == 0) { // all jvms have been stopped and replaced
      if (upVMs.size() != downVMs.size()) { //consistency check
        throw new TestException("Test problem; expected number of upVMs and number of downVMs to both be 0, num upVMs is " + upVMs.size() +
            ", num downVMs is " + downVMs.size());
      }
      Log.getLogWriter().info("Sleeping for " + secToSleep + " seconds to allow final ops to run...");
      MasterController.sleepForMs(secToSleep * 1000);
      throw new StopSchedulingOrder("Rolling upgrade has completed");
    }
  }

  /** Register interest with ALL_KEYS, and InterestPolicyR
  /** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
   *  which is equivalent to a full GII.
   */
  protected static void registerInterest() {
    Set<Region<?,?>> rootRegions = CacheUtil.getCache().rootRegions();
    for (Region aRegion: rootRegions) {
      Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES for region " + 
          aRegion.getFullPath());
      aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      Log.getLogWriter().info("Done calling registerInterest for all keys, " +
          "result interest policy KEYS_VALUES, " + aRegion.getFullPath() + 
          " size is " + aRegion.size());
    }
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
  
  /** Return a value for the given key
   */
  public static BaseValueHolder getValueForKey(Object key) {
    List<String> objectTypes = OperationsClientPrms.getObjectTypes();
    String objType = objectTypes.get(TestConfig.tab().getRandGen().nextInt(0, objectTypes.size()-1));
    if ((objType.equals("util.PdxVersionedValueHolder")) ||
        (objType.equals("util.VersionedValueHolder"))) {
      BaseValueHolder vh = PdxTest.getVersionedValueHolder(objType, (String)key, randomValues);
      return vh;
    } else if (objType.equals("util.VHDataSerializable")) {
      return new VHDataSerializable((String)key, randomValues);
    } else if (objType.equals("util.ValueHolder")) {
      return new ValueHolder((String)key, randomValues);
    } else {
      throw new TestException("Don't know how to handle " + objType);
    }
  }
  
  /** Create the pdx disk store if one was specified.
   * 
   */
  private void initPdxDiskStore() {
    if (CacheHelper.getCache().getPdxPersistent()) {
      String pdxDiskStoreName = TestConfig.tab().stringAt(CachePrms.pdxDiskStoreName, null);
      if (pdxDiskStoreName != null) {// pdx disk store name was specified
        if (CacheHelper.getCache().findDiskStore(pdxDiskStoreName) == null) {
          DiskStoreHelper.createDiskStore(pdxDiskStoreName);
        }
      }
    }
  }
  
  private void verifyPRs() {
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region> regionSet = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    
    for (Region aRegion: regionSet) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        verifyPR(aRegion);
      }
    }
  }
  
  private void verifyPR(Region aRegion) {
  StringBuffer aStr = new StringBuffer();

  // verify PR metadata 
  try {
    ParRegUtil.verifyPRMetaData(aRegion);
  } catch (Exception e) {
    aStr.append(TestHelper.getStackTrace(e) + "\n");
  } catch (TestException e) {
    aStr.append(TestHelper.getStackTrace(e) + "\n");
  }

  int redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
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
    aStr.append(TestHelper.getStackTrace(e) + "\n");
  } catch (TestException e) {
    aStr.append(e.toString() + "\n");
  }

  if (aStr.length() > 0) {
    // shutdownHook will cause all members to dump partitioned region info
    throw new TestException(aStr.toString());
  }
  Log.getLogWriter().info("Done verifying PR internal consistency");
}
}
