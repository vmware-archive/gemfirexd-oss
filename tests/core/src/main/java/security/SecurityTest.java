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

package security;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientCacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.MasterController;
import hydra.PoolPrms;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import util.TestException;
import util.TestHelper;
import util.TxHelper;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.NotAuthorizedException;

import cq.CQUtil;

/**
 * Contains the hydra task for the Security framework test
 * 
 * @author Rajesh Kumar
 * @since 5.5
 * 
 */

public class SecurityTest {

  // private static Cache cache = null;
  // test region name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      SecurityClientsPrms.regionName);

  static protected final String NON_SECURE_REGION_NAME = "nonSecureRegion";

//  static protected final Boolean IS_MULTI_USER_MODE = TestConfig.tab()
//      .booleanAt(PoolPrms.multiuserAuthentication, false);

  static LogWriter logger = Log.getLogWriter();

  static long killInterval = TestConfig.tab().longAt(
      SecurityClientsPrms.killInterval);

  static Random rand = new Random();

  public static synchronized void openCacheTask() {
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    try {
      String cacheConfig = ConfigPrms.getCacheConfig();
      CacheHelper.createCache(cacheConfig);

      String bridgeConfig = ConfigPrms.getBridgeConfig();

      int numOfRegion = TestConfig.tab().intAt(
          SecurityClientsPrms.numberOfRegions, 1);
      if (numOfRegion == 1)
        RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
      else {
        for (int i = 1; i <= numOfRegion; i++) {
          RegionHelper.createRegion(REGION_NAME + i, ConfigPrms
              .getRegionConfig());
        }
      }

      if (SecurityClientsPrms.useTransactions()) {
         TxHelper.setTransactionManager();
      }

      if (bridgeConfig != null) {
        BridgeHelper.startBridgeServer(bridgeConfig);
      }
      else {
        CQUtil.initialize();
        CQUtil.initializeCQService();
      }

      if (expectedFail) {
        throw new TestException("Expected this to throw AuthFailException");
      }
    }
    catch (AuthenticationFailedException e) {
      if (expectedFail) {
        Log.getLogWriter().info(
            "Got expected AuthenticationFailedException: " + e.getMessage());
      }
      else {
        throw new TestException(
            "AuthenticationFailedException while openCacheTask :"
                + e.getMessage());
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openCacheTask :"
          + TestHelper.getStackTrace(e));
    }
    FunctionService.registerFunction(new SecurityFunction());
  }

  public static synchronized void initTaskSecureRegions() {
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    try {
      String cacheConfig = ConfigPrms.getCacheConfig();
      CacheHelper.createCache(cacheConfig);
      String bridgeConfig = ConfigPrms.getBridgeConfig();

      // Create Secure region
      RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());

      // Create Non Secure region
      RegionHelper.createRegion(NON_SECURE_REGION_NAME, ConfigPrms
          .getRegionConfig());

      if (bridgeConfig != null) {
        BridgeHelper.startBridgeServer(bridgeConfig);
      }
      else {
        CQUtil.initialize();
        CQUtil.initializeCQService();
      }

      if (expectedFail) {
        throw new TestException("Expected this to throw AuthFailException");
      }
    }
    catch (AuthenticationFailedException e) {
      if (expectedFail) {
        Log.getLogWriter().info(
            "Got expected AuthenticationFailedException: " + e.getMessage());
      }
      else {
        throw new TestException(
            "AuthenticationFailedException while openCacheTask :"
                + e.getMessage());
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openCacheTask :"
          + e.getMessage());
    }
    FunctionService.registerFunction(new SecurityFunction());
  }

  public static void performOpToTestCredentials() {
    Region region = RegionHelper.getRegion(REGION_NAME);
    try {
      region.get("A_KEY");
      throw new TestException("Expected this to throw CacheWriterException");
    }
    catch (Exception ex) {
      if ((ex instanceof CacheWriterException
          || ex instanceof ServerConnectivityException || ex instanceof CacheLoaderException)
          && (ex.getCause() instanceof AuthenticationFailedException)) {
        Log.getLogWriter().info(
            "Got expected CacheWriterException: " + ex.getMessage());
      }
      else {
        throw new TestException("Exception while performOpToTestCredentials :"
            + ex.getMessage(), ex);
      }
    }
  }

  /**
   * Task to perform putAll operations.
   */
  public static void putAllTask() {
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    int numOfRegion = TestConfig.tab().intAt(
        SecurityClientsPrms.numberOfRegions, 1);
    Region region = null;
    HashMap aMap = getMapForPutAll();
    try {
      if (numOfRegion == 1) {
        region = RegionHelper.getRegion(REGION_NAME);
        region.putAll(aMap);
      }
      else {
        for (int i = 0; i < numOfRegion; i++) {
          region = RegionHelper.getRegion(REGION_NAME + i);
          region.putAll(aMap);
        }
      }
      Log.getLogWriter().info("performed a PutAll operation in the region");
      if (expectedFail) {
        throw new TestException(
            "Expected this to throw CacheWriterException for putAll operation");
      }

    }
    catch (Exception ex) {
      if ((ex instanceof CacheWriterException || ex instanceof ServerConnectivityException)
          && ex.getCause() instanceof NotAuthorizedException) {
        if (expectedFail) {
          Log.getLogWriter().info(
              "Got expected CacheWriterException: " + ex.getMessage());
        }
        else {
          throw new TestException(
              "CacheWriterException while performing putAll Task :"
                  + ex.getMessage());
        }
      }
      else {
        throw new TestException("Exception while performing putAllTask :"
            + ex.getMessage());
      }

    }
  }

  protected static HashMap getMapForPutAll() {
    HashMap aMap = new HashMap();
    int maxMapSize = 20;
    int mapSize = rand.nextInt(maxMapSize) + 1;
    for (int i = 0; i < mapSize; i++) {
      Object key = "key" + i;
      Object value = key + "_Value";
      aMap.put(key, value);
    }
    return aMap;
  }

  /**
   * Task to register interest in registerInterest
   */
  public static void registerInterestAllKeys() {
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    int numOfRegion = TestConfig.tab().intAt(
        SecurityClientsPrms.numberOfRegions, 1);
    Region region = null;
    try {
      if (numOfRegion == 1) {
        region = RegionHelper.getRegion(REGION_NAME);
        region.registerInterest("ALL_KEYS");
      }
      else {
        for (int i = 0; i < numOfRegion; i++) {
          region = RegionHelper.getRegion(REGION_NAME + i);
          region.registerInterest("ALL_KEYS");
        }
      }
      Log.getLogWriter().info(
          "Registered Interest for all keys for region " + region);
      if (expectedFail) {
        throw new TestException("Expected this to throw CacheWriterException");
      }

    }
    catch (Exception ex) {
      if ((ex instanceof CacheWriterException || ex instanceof ServerConnectivityException)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        if (expectedFail) {
          Log.getLogWriter().info(
              "Got expected CacheWriterException: " + ex.getMessage());
        }
        else {
          throw new TestException(
              "CacheWriterException while registerInterest Task :"
                  + ex.getMessage(), ex);
        }
      }
      else {
        throw new TestException("Exception while registerInterestTask :"
            + ex.getMessage(), ex);
      }

    }
  }

  /**
   * Updates the blackboard with the events from the writer
   */
  public static void updateBlackoard() {
    Log.getLogWriter().info("Sleeping for some time!!!");
    hydra.MasterController.sleepForMs(50000);
    SecurityClientBB.getBB().getSharedMap().put("Create_events",
        new Integer(SecurityListener.create));
    SecurityClientBB.getBB().getSharedMap().put("Update_events",
        new Integer(SecurityListener.update));
    SecurityClientBB.getBB().getSharedMap().put("Destroy_events",
        new Integer(SecurityListener.destroy));
    SecurityClientBB.getBB().getSharedMap().put("Invalidate_events",
        new Integer(SecurityListener.invalidate));
    Log.getLogWriter().info(
        "Writer writes Create_events " + SecurityListener.create
            + " Update_events " + SecurityListener.update + " Destory_events "
            + SecurityListener.destroy + " Invalidate_events "
            + SecurityListener.invalidate);
  }

  /**
   * Validating the event counters for the vm
   */
  public static void validateEventCounters() {
    Log.getLogWriter().info("Sleeping for some time!!!");
    hydra.MasterController.sleepForMs(50000);
    Log.getLogWriter()
        .info("Create events received " + SecurityListener.create);
    Log.getLogWriter()
        .info("Update events received " + SecurityListener.update);
    Log.getLogWriter().info(
        "Destroy events received " + SecurityListener.destroy);
    Log.getLogWriter().info(
        "Invalidate events received " + SecurityListener.invalidate);

    int writer_create = ((Integer)(SecurityClientBB.getBB().getSharedMap()
        .get("Create_events"))).intValue();
    int writer_update = ((Integer)(SecurityClientBB.getBB().getSharedMap()
        .get("Update_events"))).intValue();
    int writer_destroy = ((Integer)(SecurityClientBB.getBB().getSharedMap()
        .get("Destroy_events"))).intValue();
    int writer_invalidate = ((Integer)(SecurityClientBB.getBB().getSharedMap()
        .get("Invalidate_events"))).intValue();

    if (SecurityClientsPrms.isExpectedPass()) {
      if ((writer_create == SecurityListener.create)
          && (writer_update == SecurityListener.update)
          && (writer_destroy == SecurityListener.destroy)
          && (writer_invalidate == SecurityListener.invalidate)) {
        Log.getLogWriter().info("Validation successfull");
      }
      else {
        throw new TestException(
            "Validation failed because of wrong event counter values. "
                + "Expected create events = " + writer_create
                + " received create events = " + SecurityListener.create
                + " Expected update events = " + writer_update
                + " received update events = " + SecurityListener.update
                + " Expected destroy events = " + writer_destroy
                + " received destroy events = " + SecurityListener.destroy
                + " Expected invalidate events = " + writer_invalidate
                + " received invalidate events = "
                + SecurityListener.invalidate);
      }

    }
    else {
      if ((SecurityListener.create == 0) && (SecurityListener.update == 0)
          && (SecurityListener.destroy == 0)
          && (SecurityListener.invalidate == 0)) {
        Log.getLogWriter().info("Validation successfull");
      }
      else {
        throw new TestException(
            "Number of events supposed to be zero but that is not the case");
      }
    }
  }

  public static synchronized void closeCacheTask() {
    if (ConfigPrms.getBridgeConfig() != null) {
      BridgeHelper.stopBridgeServer();
    }
    CacheHelper.closeCache();
    ClientCacheHelper.closeCache();
  }

  /**
   * A INITTASK that initializes the blackboard to keep track of the last time
   * something was killed.
   * 
   */
  public static void initBlackboard() {
    SecurityClientBB.getBB().getSharedMap().put("lastKillTime", new Long(0));

  }

  /**
   * Killing the Stable server
   * 
   */

  public static void killStableServer() throws ClientVmNotFoundException {
    try {
      killServer();
    }
    catch (AuthenticationFailedException e) {
      throw new TestException(
          "AuthenticationFailedException while killStableServer :"
              + e.getMessage());
    }
    catch (Exception e) {
      throw new TestException("Exception while killStableServer:"
          + e.toString());
    }

  }

  /**
   * A Hydra TASK that kills a random server
   */
  public synchronized static void killServer() throws ClientVmNotFoundException {
    Set dead;
    Set active;
    Region aRegion = null;

    // int numRegion = TestConfig.tab().intAt(SecurityClientsPrms.regionRange,
    // 1) - 1;
    // aRegion = RegionHelper.getRegion(REGION_NAME + numRegion);
    aRegion = RegionHelper.getRegion(REGION_NAME);

    active = ClientHelper.getActiveServers(aRegion);

    // keep at least 2 servers alive
    // TODO : get this from conf file
    int minServersRequiredAlive = TestConfig.tab().intAt(
        SecurityClientsPrms.minServersRequiredAlive, 3);
    if (active.size() < minServersRequiredAlive) {
      Log.getLogWriter().info(
          "No kill executed , a minimum of " + minServersRequiredAlive
              + " servers have to be kept alive");
      return;
    }

    long now = System.currentTimeMillis();
    Long lastKill = (Long)SecurityClientBB.getBB().getSharedMap().get(
        "lastKillTime");
    long diff = now - lastKill.longValue();
    if (diff < killInterval) {
      Log.getLogWriter().info("No kill executed");
      return;
    }
    else {
      SecurityClientBB.getBB().getSharedMap()
          .put("lastKillTime", new Long(now));
    }

    List endpoints = BridgeHelper.getEndpoints();
    int index = rand.nextInt(endpoints.size() - 1);
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints
        .get(index);
    ClientVmInfo target = new ClientVmInfo(endpoint);
    target = ClientVmMgr.stop("Killing random cache server",
        ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, target);
    Log.getLogWriter().info("Server Killed : " + target);

    int sleepSec = TestConfig.tab().intAt(SecurityClientsPrms.restartWaitSec);
    Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds");
    MasterController.sleepForMs(sleepSec * 1000);

    active = ClientHelper.getActiveServers(aRegion);

    ServerLocation server = new ServerLocation(endpoint.getHost(), endpoint
        .getPort());
    if (active.contains(server)) {
      Log.getLogWriter().info(
          "ERROR: Killed server " + server + " found in Active Server List: "
              + active);
    }

    ClientVmMgr.start("Restarting the cache server", target);

    int sleepMs = ClientHelper.getRetryInterval(aRegion) + 1000;
    Log.getLogWriter().info("Sleeping for " + sleepMs + " ms");
    MasterController.sleepForMs(sleepMs);

    active = ClientHelper.getActiveServers(aRegion);
    if (!active.contains(server)) {
      Log.getLogWriter().info(
          "ERROR: Restarted server " + server + " not in Active Server List: "
              + active);
    }

    return;

  }

  /**
   * Tasks to create, start and stop locator
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  public static void startLocatorAndAdminDSTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  public static void startLocatorAndDSTask() {
    DistributedSystemHelper.startLocatorAndDS();
  }

  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }

  public static void stopLocatorAndDSTask() {
    DistributedSystemHelper.stopLocator();
    DistributedSystemHelper.disconnect();
  }

  /**
   * TASK to validate the expected number of members.
   */
  public static void validateExpectedMembersTask() {
    InternalDistributedSystem ds = (InternalDistributedSystem)DistributedSystemHelper
        .getDistributedSystem();
    int actual = ds == null ? 0 : ds.getDistributionManager()
        .getNormalDistributionManagerIds().size();
    int expected = SecurityClientsPrms.getExpectedMembers();
    if (actual == expected) {
      String s = "Have expected " + expected + " members";
      Log.getLogWriter().info(s);
    }
    else {
      String s = "Expected " + expected + " members, found " + actual;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * validate task for various entry operations
   */
  public static void validateFailedOrPassed() {
    Vector operations = SecurityClientsPrms
        .getSuccessOrFailureEntryOperationsList();
    for (int i = 0; i < operations.size(); i++) {
      Log.getLogWriter().info(
          "validateFailedOrPassed successOrFailureEntryOperations = "
              + operations.elementAt(i));
      Integer totalOperation = (Integer)security.EntryOperations.operationsMap
          .get(operations.elementAt(i));
      Integer notAuthzCount = (Integer)security.EntryOperations.exceptionMap
          .get(operations.elementAt(i));

      if (totalOperation == null) {
        continue;
      }
      if (SecurityClientsPrms.isExpectedPass()) {
        if ((totalOperation.intValue() != 0) && (notAuthzCount == null)) {

          Log.getLogWriter().info(
              "Task passed sucessfully with total operation = "
                  + totalOperation.intValue());
        }
        else {

          throw new TestException(notAuthzCount
              + " NotAuthorizedException found for operation "
              + operations.elementAt(i) + " while expected 0");
        }
      }
      else {
        if (totalOperation != null && notAuthzCount != null) {
          if (totalOperation.intValue() == notAuthzCount.intValue()) {
            Log
                .getLogWriter()
                .info(
                    "Task passed sucessfully and got the expected number of not authorize exception: "
                        + notAuthzCount
                        + " with total number of operation "
                        + totalOperation);
          }
          else {
            throw new TestException(
                "Expected NotAuthorizedException with total number of "
                    + operations.elementAt(i) + " operation " + totalOperation
                    + " but found " + notAuthzCount);
          }
        }
      }
    }
  }
}
