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
import hydra.EnvHelper;
import hydra.GemFirePrms;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.ClientRegionHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import util.TestException;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * Contains the hydra task for the perUsersRequsetSecurity framework test
 * 
 * @author Swati Bandal
 * @since 6.5
 * 
 */

public class PerUserRequestSecurityTest {

  static protected final String SECURITY_SCHEME = TestConfig.tab().stringAt(
      GemFirePrms.securityName, "dummyScheme");

  public static final String USER_NAME = "security-username";

  public static final String PASSWORD = "security-password";

  public static Map<String, Region> proxyRegionMap = new HashMap<String, Region>();
  public static Map<String, RegionService> proxyAuthenticatedCacheMap = new HashMap<String, RegionService>();
  

  public static Map<String, Map<String, ArrayList<String>>> userToRolesMap = new HashMap<String, Map<String, ArrayList<String>>>();

  public static Boolean isFailOver = false;
  
  public static ClientCache clientCache = null;
  
  /**
   * Used 
   * 1. To decide if listener should write the event value into the BB.
   * 2. To decide if a Long or a String to be put into region
   * 3. To register durable/non-durable CQ
   */
  public static Boolean writeToBB = false;
 
  static {
    ArrayList<String> ops = new ArrayList<String>();
    Map<String, ArrayList<String>> globalRolesToOpsMap = new HashMap<String, ArrayList<String>>();
    // ///////// ROLE 1
    ops.add(0, "GET");
    ops.add(1, "CONTAINS_KEY");
    ops.add(2, "KEY_SET");
    ops.add(3, "EXECUTE_FUNCTION");
    globalRolesToOpsMap.put("reader", ops);

    // ///////// ROLE 2
    ops = new ArrayList<String>();
    ops.add(0, "PUT");
    ops.add(1, "PUTALL");
    ops.add(2, "DESTROY");
    ops.add(1, "INVALIDATE");
    ops.add(3, "REGION_CLEAR");
    globalRolesToOpsMap.put("writer", ops);

    // ///////// ROLE 3
    ops = new ArrayList<String>();
    ops.add(0, "QUERY");
    ops.add(1, "EXECUTE_CQ");
    ops.add(2, "STOP_CQ");
    ops.add(3, "CLOSE_CQ");
    globalRolesToOpsMap.put("queryRegions", ops);

    // ///////// ROLE 4
    ops = new ArrayList<String>();
    ops.add(0, "QUERY");
    ops.add(1, "EXECUTE_CQ");
    ops.add(2, "STOP_CQ");
    ops.add(3, "CLOSE_CQ");
    globalRolesToOpsMap.put("cacheOps", ops);

    // ///////////////////////////////////////////
    Map<String, ArrayList<String>> rolesToOpsMap = new HashMap<String, ArrayList<String>>();

    rolesToOpsMap.put("reader", globalRolesToOpsMap.get("reader"));
    userToRolesMap.put("gemfire3", rolesToOpsMap);
    userToRolesMap.put("gemfire4", rolesToOpsMap);
    userToRolesMap.put("gemfire5", rolesToOpsMap);

    rolesToOpsMap = new HashMap<String, ArrayList<String>>();
    rolesToOpsMap.put("writer", globalRolesToOpsMap.get("writer"));
    userToRolesMap.put("user3", rolesToOpsMap);
    userToRolesMap.put("user4", rolesToOpsMap);
    userToRolesMap.put("gemfire6", rolesToOpsMap);
    userToRolesMap.put("gemfire7", rolesToOpsMap);
    userToRolesMap.put("gemfire8", rolesToOpsMap);

    rolesToOpsMap = new HashMap<String, ArrayList<String>>();
    rolesToOpsMap.put("queryRegions", globalRolesToOpsMap.get("queryRegions"));
    userToRolesMap.put("user5", rolesToOpsMap);
    userToRolesMap.put("user6", rolesToOpsMap);
    userToRolesMap.put("gemfire9", rolesToOpsMap);
    userToRolesMap.put("gemfire10", rolesToOpsMap);

    rolesToOpsMap = new HashMap<String, ArrayList<String>>();
    rolesToOpsMap.put("reader", globalRolesToOpsMap.get("reader"));
    rolesToOpsMap.put("cacheOps", globalRolesToOpsMap.get("cacheOps"));
    userToRolesMap.put("user1", rolesToOpsMap);
    userToRolesMap.put("user2", rolesToOpsMap);

    rolesToOpsMap = new HashMap<String, ArrayList<String>>();
    rolesToOpsMap.put("writer", globalRolesToOpsMap.get("writer"));
    rolesToOpsMap.put("reader", globalRolesToOpsMap.get("reader"));
    rolesToOpsMap.put("cacheOps", globalRolesToOpsMap.get("cacheOps"));
    userToRolesMap.put("root", rolesToOpsMap);
    userToRolesMap.put("admin", rolesToOpsMap);
    userToRolesMap.put("administrator", rolesToOpsMap);
    userToRolesMap.put("gemfire1", rolesToOpsMap);
    userToRolesMap.put("gemfire2", rolesToOpsMap);
  }

  static LogWriter logger = Log.getLogWriter();

  static long killInterval = TestConfig.tab().longAt(
      SecurityClientsPrms.killInterval);

  static Random rand = new Random();

  public static synchronized void openServerCacheTask() {
    try {
      String cacheConfig = ConfigPrms.getCacheConfig();
      CacheHelper.createCache(cacheConfig);

      String bridgeConfig = ConfigPrms.getBridgeConfig();

      int numOfRegion = TestConfig.tab().intAt(
          SecurityClientsPrms.numberOfRegions, 1);
      if (numOfRegion == 1)
        RegionHelper.createRegion(SecurityTest.REGION_NAME, ConfigPrms
            .getRegionConfig());
      else {
        for (int i = 1; i <= numOfRegion; i++) {
          RegionHelper.createRegion(SecurityTest.REGION_NAME + i, ConfigPrms
              .getRegionConfig());
        }
      }

      if (bridgeConfig != null) {
        BridgeHelper.startBridgeServer(bridgeConfig);
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openServerCacheTask :"
          + e.getMessage(), e.getCause());
    }
  }

  public static synchronized void openClientCacheTask() {
    try {
      String cacheConfig = ConfigPrms.getClientCacheConfig();
      clientCache = ClientCacheHelper.createCache(cacheConfig);
      Log.getLogWriter().info("The client cache has been created.");
      Region aRegion = ClientRegionHelper.createRegion(SecurityTest.REGION_NAME, ConfigPrms.getClientRegionConfig());
      Log.getLogWriter().info("The client region has been created.");
      Log.getLogWriter().info(
          "Entries have been put into the region ::" + SecurityTest.REGION_NAME);
      Pool pool = PoolHelper.getPool("brloader");
      if (pool.getMultiuserAuthentication()) {
        Vector usersPerVMList = null;
        Vector usersKeyStorePathPerVMList = null;
        Vector usersKeyStoreAliasPerVMList = null;

        String schemeName = DistributedSystemHelper.getGemFireDescription()
            .getSecurityDescription().getName();
        Log.getLogWriter().info("Security Name is :: " + schemeName);

        if (schemeName.equals("dummyScheme")) {
          usersPerVMList = SecurityClientsPrms
              .getUserCredentialsListForDummyScheme();
        }
        else if (schemeName.equals("ldapScheme")) {
          usersPerVMList = SecurityClientsPrms
              .getUserCredentialsListForLdapScheme();
        }

        if (schemeName.equals("dummyScheme") || schemeName.equals("ldapScheme")) {

          for (int i = 0; i < usersPerVMList.size(); i++) {
            String userCred = (String)usersPerVMList.elementAt(i);
            Log.getLogWriter().info(
                "userCredentials are :: " + userCred.toString());
            String userName = userCred.substring(0, userCred.indexOf("-"));
            String password = userCred.substring(userCred.indexOf("-") + 1);
            Properties userProps = new Properties();
            userProps.setProperty("security-username", userName);
            userProps.setProperty("security-password", password);
            ProxyCache cache = (ProxyCache)clientCache.createAuthenticatedView(userProps, pool.getName());
            Log.getLogWriter()
                .info("Got the proxyCache for user ::" + userName);
            proxyRegionMap.put(userName, cache
                .getRegion(SecurityTest.REGION_NAME));
            proxyAuthenticatedCacheMap.put(userName, cache);
            Log.getLogWriter().info(
                "Got the proxyRegion for user ::" + userName);
          }
        }

        else if (schemeName.equals("pkcsScheme")) {

          usersKeyStorePathPerVMList = SecurityClientsPrms
              .getKeyStorePathListForPkcsScheme();
          usersKeyStoreAliasPerVMList = SecurityClientsPrms
              .getKeyStoreAliasListForPkcsScheme();
          String kestorePass = TestConfig.tab().stringAt(
              PKCSUserPasswordPrms.keystorepass, "gemfire");

          for (int i = 0; i < usersKeyStorePathPerVMList.size(); i++) {
            String keystorePath = (String)usersKeyStorePathPerVMList
                .elementAt(i);
            String keystoreAlias = (String)usersKeyStoreAliasPerVMList
                .elementAt(i);
            keystorePath = EnvHelper.expandPath("$JTESTS" + keystorePath);
            Properties userProps = new Properties();
            userProps.setProperty("security-keystorepath", keystorePath);
            userProps.setProperty("security-alias", keystoreAlias);
            userProps.setProperty("security-keystorepass", kestorePass);
            ProxyCache cache = (ProxyCache)clientCache.createAuthenticatedView(userProps, pool.getName());
            Log.getLogWriter().info(
                "Got the proxyCache for user ::" + keystoreAlias);
            proxyRegionMap.put(keystoreAlias, cache
                .getRegion(SecurityTest.REGION_NAME));
            proxyAuthenticatedCacheMap.put(keystoreAlias, cache);
            Log.getLogWriter().info(
                "Got the proxyRegion for user ::" + keystoreAlias);
          }
        }
        EntryOperations.initCQTask(writeToBB);
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openClientCacheTask :"
          + e.getMessage(), e);
    }

  }
 
  public static synchronized void openDurableClientCacheTask() {
    try {
      writeToBB = true;
      openClientCacheTask();
      String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig().getDurableClientId();
      Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
      if (!SecurityClientBB.getBB().getSharedMap().containsKey(VmDurableId)) {
        Iterator<String> iter = proxyRegionMap.keySet().iterator();
        HashMap<String , Map<String, Long>[]> userMap = new HashMap<String , Map<String, Long>[]>();
        while (iter.hasNext()) {
          String userName = (String)iter.next();
          HashMap<String, Long>[] mapArray = new HashMap[2];
          mapArray[0] = new HashMap<String, Long>();
          mapArray[1] = new HashMap<String, Long>();
          userMap.put(userName, mapArray);
        }
        SecurityClientBB.getBB().getSharedMap().put(VmDurableId, userMap);
      }
      clientCache.readyForEvents();
    }
    catch (Exception e) {
      throw new TestException("Exception while openDurableClientCacheTask :"
          + e.getMessage(), e);
    }
  }
  
  public static void mentionReferenceInBlackboard() {
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    Log.getLogWriter().info("Reference VM is : " + VmDurableId);
    SecurityClientBB.getBB().getSharedMap().put("REFERENCE VM:", VmDurableId);
  }

  public static void killClient() {
    try {
      hydra.MasterController.sleepForMs(10000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }
  
  /**
   * validate task for various entry operations in case of MultiUser Mode
   */
  public static void validateFailedOrPassedEntryOpsForMultiUserMode() {
    int totalOperation = 0;
    int notAuthzCount = 0;
    Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
        .iterator();
    Boolean opFoundInOpList = false;
    Boolean hasCQExecOp = false;
    Boolean hasGETOp = false;
    String opName = null;
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      hasCQExecOp = false;
      hasGETOp = false;
      Map operations = EntryOperations.userOperationMap.get(userName);
      Map exceptions = EntryOperations.userExceptionMap.get(userName);
      Iterator opIter = operations.keySet().iterator();
      while (opIter.hasNext()) {
        opName = (String)opIter.next();
        Log.getLogWriter().fine(
            "validateFailedOrPassed successOrFailureEntryOperations = "
                + operations.get(opName));
        Log.getLogWriter().fine("OperationName ::" + opName);
        Integer tmpTotalOperation = (Integer)operations.get(opName);
        Integer tmpNotAuthzCount = (Integer)exceptions.get(opName);
        totalOperation = (tmpTotalOperation == null) ? 0 : tmpTotalOperation;
        notAuthzCount = (tmpNotAuthzCount == null) ? 0 : tmpNotAuthzCount;
        Log.getLogWriter().info("Total Operations Count ::" + totalOperation);
        Log.getLogWriter().info("Total exceptions Count ::" + notAuthzCount);
        Map<String, ArrayList<String>> userRoleOpMap = userToRolesMap
            .get(userName);
        Iterator userRoleOpIter = userRoleOpMap.keySet().iterator();
        while (userRoleOpIter.hasNext()) {
          opFoundInOpList = false;

          String roleName = (String)userRoleOpIter.next();
          ArrayList<String> roleOpNames = userRoleOpMap.get(roleName);

          for (int i = 0; i < roleOpNames.size(); i++) {
            if (roleOpNames.get(i).equalsIgnoreCase("EXECUTE_CQ")) {
              hasCQExecOp = true;
            }
            if (roleOpNames.get(i).equalsIgnoreCase("GET")) {
              hasGETOp = true;
            }
            if (opName.equalsIgnoreCase(roleOpNames.get(i))) {
              opFoundInOpList = true;
            }
            else if (opName.equalsIgnoreCase("executeQuery")
                && roleOpNames.get(i).equalsIgnoreCase("QUERY")) {
              opFoundInOpList = true;
            }
          }
          if (opFoundInOpList)
            break;
        }
        if (opFoundInOpList) {
          if ((totalOperation != 0) && (notAuthzCount == 0)) {
            Log.getLogWriter().info(
                "Task passed sucessfully with total operation = "
                    + totalOperation + " for the user ::" + userName);
          }
          else {
            Log.getLogWriter().info("Task failed for the user :: " + userName);
            throw new TestException(notAuthzCount
                + " NotAuthorizedException found for operation " + opName
                + " while expected 0");
          }
        }
        else {
          if (totalOperation == notAuthzCount) {
            Log
                .getLogWriter()
                .info(
                    "Task passed sucessfully and got the expected number of not authorize exception: "
                        + notAuthzCount
                        + " with total number of operation "
                        + totalOperation);
            Log.getLogWriter().info(
                "Task Passed successfully for the user ::" + userName);
          }
          else {
            Log.getLogWriter().info("Task failed for the user :: " + userName);
            throw new TestException(
                "Expected NotAuthorizedException with total number of "
                    + opName + " operation " + totalOperation + " but found "
                    + notAuthzCount);
          }
        }
      }
      /* validation of CQ */
      long numOfEventsForCQ1 = 0, numOfEventsForCQ2 = 0;
      //QueryService qs = proxyRegionMap.get(userName).getCache().getQueryService();
      QueryService qs = proxyAuthenticatedCacheMap.get(userName).getQueryService();
      
      if (qs.getCq("CQ1_" + userName) != null) {
        numOfEventsForCQ1 = ((CqQueryTestListener)qs.getCq("CQ1_" + userName)
            .getCqAttributes().getCqListeners()[0]).getTotalEventCount();
      }
      else {
        Log.getLogWriter().info(" CQ1 is null for user :" + userName);
      }
      if (qs.getCq("CQ2_" + userName) != null) {
        numOfEventsForCQ2 = ((CqQueryTestListener)qs.getCq("CQ2_" + userName)
            .getCqAttributes().getCqListeners()[0]).getTotalEventCount();
      }
      else {
        Log.getLogWriter().info(" CQ2 is null for user :" + userName);
      }

      if (hasCQExecOp && hasGETOp) {
        if (numOfEventsForCQ1 > 0 && numOfEventsForCQ2 > 0) {
          Log.getLogWriter().info(
              "CQ Task Passed sucessfully for user :: "  + userName + " with total number of CQ events for CQ1 : "
                  + numOfEventsForCQ1 + " and for CQ2 : " + numOfEventsForCQ2);
        }
        else {
          Log.getLogWriter().info("CQ Task failed for the user :: " + userName);
          throw new TestException(
              "Expected total CQ events for CQ1 and CQ2 to be > 0 but found CQ1 : "
                  + numOfEventsForCQ1 + " and CQ2 : " + numOfEventsForCQ2);
        }
      }
      else {
        if (numOfEventsForCQ1 == 0 && numOfEventsForCQ2 == 0) {
          Log.getLogWriter().info(
              "CQ Task Passed sucessfully for user :: " + userName + " with 0 CQ events for CQ1 and CQ2");
        }
        else {
          Log.getLogWriter().info("CQ Task failed for the user :: " + userName);
          throw new TestException(
              "Expected total CQ events for CQ1 and CQ2 to be 0, but found "
                  + numOfEventsForCQ1 + " and " + numOfEventsForCQ2
                  + " respectively.");
        }
      }
    }
  }

  public static void setFailOverTest() {
    isFailOver = true;
  }
  
  public static void validateDurableCQEvents() {

    String ReferenceVm = (String)SecurityClientBB.getBB().getSharedMap().get(
        "REFERENCE VM:");
    Log.getLogWriter().info("Reference VM Client Id is " + ReferenceVm);
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
    HashMap currentVmMap = (HashMap)SecurityClientBB.getBB().getSharedMap()
        .get(VmDurableId);
    HashMap referenceMap = (HashMap)SecurityClientBB.getBB().getSharedMap()
        .get(ReferenceVm);

    if (currentVmMap.isEmpty()) {
      throw new TestException(" The map is empty for " + VmDurableId);
    }

    if (referenceMap.isEmpty()) {
      throw new TestException("The map is empty for Reference Vm "
          + ReferenceVm);
    }

    Iterator iterator = referenceMap.entrySet().iterator();
    Map.Entry entry = null;
    Object userName = null;
    Object mapArray = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      userName = entry.getKey();
      mapArray = entry.getValue();

      HashMap<String, Long>[] referenceMapArray = (HashMap<String, Long>[])mapArray;
      HashMap<String, Long> opEventCntMap = (HashMap<String, Long>)referenceMapArray[0];
      Iterator iterator1 = opEventCntMap.entrySet().iterator();
      Map.Entry entry1 = null;
      String operation = null;
      long eventCnt = 0;

      Log.getLogWriter().info("Event Map for UserName :: " + userName);
      Log.getLogWriter().info("Operation - ReferenceVM   CurrentVM ");
      while (iterator1.hasNext()) {
        entry1 = (Map.Entry)iterator1.next();
        operation = (String)entry1.getKey();
        eventCnt = (Long)entry1.getValue();
        if (currentVmMap.get(userName) == null) {
          throw new TestException("No event map for " + userName
              + " because client didnot receive any events for this user");
        }
        if (((HashMap[])currentVmMap.get(userName))[0].get(operation) == null) {
          throw new TestException(operation
              + " event not received by the client for " + userName);
        }
        Log.getLogWriter().info(
            operation + "  -  " + eventCnt + "       "
                + ((HashMap[])currentVmMap.get(userName))[0].get(operation));
        long currentVmOpCount = (Long)((HashMap[])currentVmMap.get(userName))[0]
            .get(operation);
        if (eventCnt != currentVmOpCount) {
          throw new TestException("For the user " + userName + ", expected "
              + eventCnt + " " + operation + "s but found " + currentVmOpCount);
        }
      }
    }
  }
  
  public static void closeProxyCacheAndCQTask() {
    Region region = null;
    Iterator iter = proxyRegionMap.keySet().iterator();
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      region = proxyRegionMap.get(userName);
      proxyAuthenticatedCacheMap.get(userName).getQueryService().closeCqs();
      //region.getCache().getQueryService().closeCqs();
      //region.getCache().close();
      proxyAuthenticatedCacheMap.get(userName).close();
      
    }
  }

}
