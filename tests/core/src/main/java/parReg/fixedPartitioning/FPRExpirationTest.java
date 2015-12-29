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
package parReg.fixedPartitioning;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.FixedPartitionBlackboard;
import hydra.PartitionPrms;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import parReg.ParRegBB;
import parReg.eviction.ParRegExpirationTest;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.Region;

public class FPRExpirationTest extends ParRegExpirationTest {

  protected static Long dataStoreSequenceId;

  public final static String DATASTORE_SEQUENCE_ID = "dataStore_sequence_id_";
  
  protected static Cache theCache;

  // ------------------- STARTTASKS -----------------------------------------//
  public static void StartTask_initialize() {
    List<FixedPartitionAttributes> fixedPartitions = TestConfig.tab().vecAt(
        hydra.FixedPartitionPrms.partitionNames);
    int redundantCopies = TestConfig.tab().intAt(PartitionPrms.redundantCopies);
    List<FixedPartitionAttributes> secondaryFixedPartitions = new ArrayList<FixedPartitionAttributes>();
    for (int i = 0; i < redundantCopies; i++) {
      secondaryFixedPartitions.addAll(fixedPartitions);
    }
    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "PrimaryPartitions", fixedPartitions);
    FixedPartitionBlackboard.getInstance().getSharedMap().put(
        "SecondaryPartitions", secondaryFixedPartitions);
  }

  /**
   * Creates and initializes the singleton instance of FPRExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initializeControlThread() {
    if (testInstance == null) {
      testInstance = new FPRExpirationTest();
      setDataStoreSequenceId();
      ((FPRExpirationTest)testInstance).initializeControlThread();
    }
  }
  
  /**
   * Creates and initializes the singleton instance of FPRExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initServers() {
    if (testInstance == null) {
      testInstance = new FPRExpirationTest();
      setDataStoreSequenceId();
      ((FPRExpirationTest)testInstance).initializeServers();
      BridgeHelper.startBridgeServer("bridge");
    }
  }
  
  /**
   * Creates and initializes the singleton instance of FPRExpirationTest. All
   * regions are created.
   */
  public synchronized static void HydraTask_initClients() {
    if (testInstance == null) {
      testInstance = new FPRExpirationTest();
      setDataStoreSequenceId();
      ((FPRExpirationTest)testInstance).initializeClients();
    }
  }

  /**
   * Create the regions specified
   */
  protected void initializeControlThread() {

    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }
    Vector regionNames = (Vector)TestConfig.tab().vecAt(RegionPrms.names, null);
    for (Iterator iter = regionNames.iterator(); iter.hasNext();) {
      String rName = (String)iter.next();
      String regionName = RegionHelper.getRegionDescription(rName)
          .getRegionName();
      Region aRegion = RegionHelper.createRegion(regionName, RegionHelper
          .getRegionAttributes(rName));
      hydra.Log.getLogWriter().info(
          "Created region " + aRegion.getName() + " with attributes "
              + aRegion.getAttributes());
    }
  }

  /**
   * Create the regions specified
   */
  protected void initializeServers() {
    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }
    Vector regionNames = (Vector)TestConfig.tab().vecAt(RegionPrms.names, null);
    for (Iterator iter = regionNames.iterator(); iter.hasNext();) {
      String rName = (String)iter.next();
      if (rName.startsWith("bridge")) {
        String regionName = RegionHelper.getRegionDescription(rName)
            .getRegionName();
        Region aRegion = RegionHelper.createRegion(regionName, RegionHelper
            .getRegionAttributes(rName));
        hydra.Log.getLogWriter().info(
            "Created region " + aRegion.getName() + " with attributes "
                + aRegion.getAttributes());
      }
    }
  }

  /**
   * Create the regions specified
   */
  public void initializeClients() {
    if (theCache == null) {
      theCache = CacheHelper.createCache("cache1");
    }
    Vector regionNames = (Vector)TestConfig.tab().vecAt(RegionPrms.names, null);
    for (Iterator iter = regionNames.iterator(); iter.hasNext();) {
      String rName = (String)iter.next();
      if (rName.startsWith("edge")) {
        String regionName = RegionHelper.getRegionDescription(rName)
            .getRegionName();
        Region aRegion = RegionHelper.createRegion(regionName, RegionHelper
            .getRegionAttributes(rName));
        aRegion.registerInterest("ALL_KEYS");
        hydra.Log.getLogWriter().info(
            "Created region " + aRegion.getName() + " with attributes "
                + aRegion.getAttributes());     
      }
    }
  }

  /**
   * Utility method that get the datastore sequence id. Used to get the correct
   * region definition for the member during HA scenarios
   */
  public static void setDataStoreSequenceId() {
    if (ParRegBB.getBB().getSharedMap().get(RemoteTestModule.getMyVmid()) == null) {
      Map vmInfo = new HashMap();
      dataStoreSequenceId = ParRegBB.getBB().getSharedCounters()
          .incrementAndRead(ParRegBB.numOfDataStores);
      vmInfo.put(DATASTORE_SEQUENCE_ID, dataStoreSequenceId);
      ParRegBB.getBB().getSharedMap().put(RemoteTestModule.getMyVmid(), vmInfo);
    }
    else {
      Map vmInfo = (HashMap)ParRegBB.getBB().getSharedMap().get(
          RemoteTestModule.getMyVmid());
      dataStoreSequenceId = (Long)vmInfo.get(DATASTORE_SEQUENCE_ID);
    }
  }

}
