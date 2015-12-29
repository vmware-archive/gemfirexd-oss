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

package admin;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import util.AdminHelper;
import util.TestException;
import util.TestHelper;

/**
 * @author lynn
 *
 */
public class ShutDownAllMembersTest {

  /** instance of this test class */
  public static ShutDownAllMembersTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;
  private Cache theCache;
  private DistributedSystem ds;
  private List<Region> regionList = new ArrayList();

  //=================================================
  // initialization methods

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() throws Throwable {
    if (testInstance == null) {
      testInstance = new ShutDownAllMembersTest();
      testInstance.initializeInstance();
      testInstance.theCache = CacheHelper.createCache("cache1");
      testInstance.ds = DistributedSystemHelper.getDistributedSystem();
      String regionConfigName = ShutDownAllMembersPrms.getRegionConfigName();
      testInstance.regionList.add(RegionHelper.createRegion(regionConfigName));
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        registerInterest();
      }
    }
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() throws Throwable {
    if (testInstance == null) {
      testInstance = new ShutDownAllMembersTest();
      testInstance.initializeInstance();
      testInstance.theCache = CacheHelper.createCache("cache1");
      testInstance.ds = DistributedSystemHelper.getDistributedSystem();
      String regionConfigName = ShutDownAllMembersPrms.getRegionConfigName();
      testInstance.regionList.add(RegionHelper.createRegion(regionConfigName));
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }

  /** Task to create and start a locator
   * 
   */
  public synchronized static void HydraTask_initLocator() {
    DistributedSystemHelper.createLocator();
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
  }

  //=================================================
  // hydra tasks

  /** Task to call shutDownAllMembers
   * 
   */
  public static void HydraTask_shutDownAllMembers() {
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    Log.getLogWriter().info("AdminDS " + adminDS + " is shutting down all members...");
    Set<DistributedMember> memberSet;
    try {
      memberSet = adminDS.shutDownAllMembers();
    } catch (AdminException e1) {
      throw new TestException(TestHelper.getStackTrace(e1));
    }
    Log.getLogWriter().info("AdminDS " + adminDS + " shut down (disconnected) the following members " +
        "(vms remain up): " + memberSet);
  }

  /** Verify whether or not this vm has been shut down with shutDownAllMembers
   * 
   */
  public static void HydraTask_verifyVmStatus() {
    boolean expectVmShutDown = ShutDownAllMembersPrms.getExpectVmShutDown();
    boolean cacheClosed = testInstance.theCache.isClosed();
    boolean dsConnected = testInstance.ds.isConnected();
    if (expectVmShutDown) {
      if (!cacheClosed) {
        throw new TestException("Expected cache to be closed but " + testInstance.theCache + 
            " isClosed() returned " + cacheClosed);
      } else {
        Log.getLogWriter().info("Cache is closed as expected");
      }
      if (dsConnected) {
        throw new TestException("Expected to be disconnected from the DS, but " + testInstance.ds + 
            " isConnected() returned " + dsConnected);
      } else {
        Log.getLogWriter().info("This vm is disconnected from the ds as expected");
      }
      for (Region aRegion: testInstance.regionList) {
        try {
          aRegion.size();
          throw new TestException("Was able to call size() on region, but we expected this vm to be disconnected from the ds");
        } catch (DistributedSystemDisconnectedException e) {
          Log.getLogWriter().info(aRegion.getFullPath() + " is not accessible as expected");
        } catch (CacheClosedException cce) {
          Log.getLogWriter().info(aRegion.getFullPath() + " is not accessible as expected");
        }
      }
    } else { // expect vm to be alive
      if (cacheClosed) {
        throw new TestException("Expect cache to not be closed but " + testInstance.theCache +
            " is " + cacheClosed);
      } else {
        Log.getLogWriter().info("Cache is not closed as expected");
      }
      if (!dsConnected) {
        throw new TestException("Expected to be connected to the DS, but " + testInstance.ds + 
            " isConnected() returned " + dsConnected);
      } else {
        Log.getLogWriter().info("This vm is connected to the ds as expected");
      }
      for (Region aRegion: testInstance.regionList) {
        try {
          aRegion.size();
          Log.getLogWriter().info(aRegion + " is accessible as expected");
        } catch (DistributedSystemDisconnectedException e) {
          throw new TestException("Was unable to call size() on region, but we expected this vm to be connected from the ds " +
              TestHelper.getStackTrace(e));
        }
      }
    }
  }

  /** Verify whether or not this admin vm has been shut down with shutDownAllMembers
   * 
   */
  public static void HydraTask_verifyAdminVmStatus() {
    boolean expectVmShutDown = ShutDownAllMembersPrms.getExpectVmShutDown();
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    boolean isConnected = adminDS.isConnected();
    if (expectVmShutDown) {
      if (!isConnected) {
        throw new TestException("Expected this admin vm to not be connected but " + adminDS + 
            " isConnected() returned " + isConnected);
      } else {
        Log.getLogWriter().info("admin vm is connected as expected");
      }
    }
  }

  //=================================================
  // other methods

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

}
