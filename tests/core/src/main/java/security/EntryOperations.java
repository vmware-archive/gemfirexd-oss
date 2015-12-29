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

import hydra.HydraThread;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import parReg.execute.ArrayListResultCollector;
import query.QueryPrms;
import util.TestException;
import util.TxHelper;

import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionInDoubtException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.security.NotAuthorizedException;

import cq.CQUtil;
import cq.CQUtilPrms;

/**
 * A EntryOperations class for Security framework testing. It creates a
 * configurable ( passed through bt/conf) number of threads for doing various
 * entry operations and each thread does a operations on fixed set of keys with
 * range specified by configuration.
 * 
 * @author Rajesh Kumar
 * @author Swati Bandal
 * @since 5.5
 */
public class EntryOperations {

  /** range of keys to be used by each thread */
  final static int PUT_KEY_RANGE = TestConfig.tab().intAt(
      SecurityClientsPrms.numKeyRangePerThread);

  /** A regionName to which index is attached to create multiple regions */
  // test region name
  final static String regionName = TestConfig.tab().stringAt(
      SecurityClientsPrms.regionName);

  static protected final String NON_SECURE_REGION_NAME = "nonSecureRegion";

  static protected final String KEY_SET = "KEY_SET";

  /** Total number of regions in the tests. Default is one */
  static int numOfRegion = 1;

  private static volatile Integer totalOperation;

  private static volatile Integer notAuthzCount;

  public static Map operationsMap = new HashMap();

  public static Map exceptionMap = new HashMap();

  public static Map<String, Map> userOperationMap = new HashMap<String, Map>();

  public static Map<String, Map> userExceptionMap = new HashMap<String, Map>();

  static AtomicInteger cqCounter = new AtomicInteger(0);

  private static ThreadLocal thrId = new ThreadLocal();

  public synchronized static void doPut() {
    Region region = null;
    if (PoolHelper.getPool("brloader").getMultiuserAuthentication()) {
      Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
          .iterator();
      while (iter.hasNext()) {
        String userName = (String)iter.next();
        region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);
        updateKeysInRegion(region);
      }
    }
    else {
      region = RegionHelper.getRegion(regionName);
      updateKeysInRegion(region);
    }
  }

  
  public static void updateKeysInRegion(Region aRegion) {
    long thrdNum = SecurityClientBB.getBB().getSharedCounters()
        .incrementAndRead(SecurityClientBB.threadCount);
    for (int keyRng = 0; keyRng < PUT_KEY_RANGE; keyRng++) {
      String Key = getKeyForOperation();
      String Value = Key + "_VALUE";
      Log.getLogWriter().info("key is: " + Key);
      if (TestConfig.tab().getRandGen().nextBoolean()) {
      aRegion.put(Key, Value);
      } else {
        Object v = aRegion.putIfAbsent(Key, Value);
        if (v != null) {
          aRegion.replace(Key, v, Value);
    }
  }
    }
  }

  public synchronized static void doInitialFeed() {
    Boolean putOpFoundInOpList = false;
    Region region = null;
    String userName = null;
    if (PoolHelper.getPool("brloader").getMultiuserAuthentication()) {
      Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
          .iterator();
      while (iter.hasNext()) {
        putOpFoundInOpList = false;
        userName = (String)iter.next();
        Map<String, ArrayList<String>> userRoleOpMap = PerUserRequestSecurityTest.userToRolesMap
            .get(userName);
        Iterator userRoleOpIter = userRoleOpMap.keySet().iterator();
        while (userRoleOpIter.hasNext()) {
          String roleName = (String)userRoleOpIter.next();
          ArrayList<String> roleOpNames = userRoleOpMap.get(roleName);
          for (int i = 0; i < roleOpNames.size(); i++) {
            if (roleOpNames.get(i).equalsIgnoreCase("PUT")) {
              putOpFoundInOpList = true;

            }
          }
          if (putOpFoundInOpList)
            break;
        }
        if (putOpFoundInOpList) {
          break;
        }
      }
      if(putOpFoundInOpList){
      region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);
      Log.getLogWriter().info("Got the proxyRegion for user :: " + userName);
      }
      else {
        Log.getLogWriter().info("No user found with PUT permission.");
      }
    }

    else {
      region = RegionHelper.getRegion(regionName);
    }
    for (int keyRng = 0; keyRng < PUT_KEY_RANGE; keyRng++) {
      String Key = getKeyForOperation();
      Long Value = new Long(1);
      if (TestConfig.tab().getRandGen().nextBoolean()) {
      region.put(Key, Value);
      } else {
        Object v = region.putIfAbsent(Key, Value);
        if (v != null) {
           region.replace(Key, Value);
    }
  }
    }
  }
  
  public static void putKeysInBB() {
    Region region = RegionHelper.getRegion(regionName);
    HashSet keySet = new HashSet(region.keySet());
    if (SecurityClientBB.getBB().getSharedMap().get(KEY_SET) == null) {
      SecurityClientBB.getBB().getSharedMap().put(KEY_SET, keySet);
    }
  }

  public synchronized static void putAllKeysInRange() {
    Region region = RegionHelper.getRegion(regionName);
    for (int keyRng = 0; keyRng < PUT_KEY_RANGE; keyRng++) {
      String Key = "KEY-" + keyRng;
      String Value = Key + "_VALUE";
      Log.getLogWriter().info("key is: " + Key);
      if (TestConfig.tab().getRandGen().nextBoolean()) {
      region.put(Key, Value);
      } else {
        Object v = region.putIfAbsent(Key, Value);
        if (v != null) {
          region.replace(Key, Value);
    }
  }
    }
  }

  public synchronized static void populateNonSecureRegion() {
    Region nonSecureRegion = RegionHelper.getRegion(NON_SECURE_REGION_NAME);
    if (nonSecureRegion == null)
      return;
    for (int keyRng = 0; keyRng < PUT_KEY_RANGE; keyRng++) {
      String Key = "KEY-" + keyRng;
      String Value = Key + "_VALUE";
      Log.getLogWriter().info("key is: " + Key);
      if (TestConfig.tab().getRandGen().nextBoolean()) {
      nonSecureRegion.put(Key, Value);
      } else {
        Object v = nonSecureRegion.putIfAbsent(Key, Value);
        if (v != null) {
          nonSecureRegion.replace(Key, Value);
    }
  }
    }
  }
  
  /**
   * This method tests queryService functionality in multiuserMode.
   */

  public static void startQueryExecuteOperationForMultiUserMode() {
    Region region = null;
    Map[] maps = new HashMap[2];
    Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
        .iterator();
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);
      synchronized (userOperationMap) {
        if (userOperationMap.get(userName) == null) {
          userOperationMap.put(userName, new HashMap());
        }
      }
      synchronized (userExceptionMap) {
        if (userExceptionMap.get(userName) == null) {
          userExceptionMap.put(userName, new HashMap());
        }
      }
      maps[0] = userOperationMap.get(userName);
      maps[1] = userExceptionMap.get(userName);

      doQueryExecute(region, userName, maps);
    }
  }

  public static void doQueryExecute(Region region, String userName, Map[] maps) {
    Map opMap = maps[0];
    Map exMap = maps[1];
    String key = getKeyForOperation();
    boolean doAddOperation = true;
    String operation = "executeQuery";
    try {
      String queryString = "SELECT DISTINCT * FROM " + region.getFullPath();
      //Query query = region.getCache().getQueryService().newQuery(queryString);
      Query query = PerUserRequestSecurityTest.proxyAuthenticatedCacheMap.get(userName).getQueryService().newQuery(queryString); 
      SelectResults result = (SelectResults)query.execute();

    }
    catch (Exception ex) {
      if ((ex instanceof CacheLoaderException
          || ex instanceof CacheWriterException
          || ex instanceof ServerConnectivityException
          || ex instanceof QueryInvocationTargetException || ex instanceof ServerOperationException)
          && ex.getCause() instanceof NotAuthorizedException) {

        Log.getLogWriter().info(
            "Got expected NotAuthorizedException when doing " + operation
                + " operation for " + userName + " : " + ex.getCause());
        synchronized (exMap) {
          Integer tmpNotAuthzCount = (Integer)exMap.get(operation);

          if (tmpNotAuthzCount == null) {
            exMap.put(operation, new Integer(1));
          }
          else {
            exMap.put(operation, new Integer(tmpNotAuthzCount.intValue() + 1));
          }
        }
      }
      else {
        doAddOperation = false;
        throw new TestException("Exception while Query Operation for user : "
            + userName, ex);

      }
    }
    finally {
      synchronized (opMap) {
        if (doAddOperation) {
          Integer tmpTotalOperation = (Integer)opMap.get(operation);

          if (tmpTotalOperation == null) {
            // 1st time = " + totalOperation.intValue());
            opMap.put(operation, new Integer(1));
          }
          else {
            opMap.put(operation, new Integer(tmpTotalOperation.intValue() + 1));
          }
        }
        Log.getLogWriter().fine(
            "For user :: " + userName + " operation : " + operation
                + " operation count is " + (Integer)opMap.get(operation)
                + " And Exception count is :: " + (Integer)exMap.get(operation)
                + " key is " + key);
      }
    }
  }
  
  /**
   * This method tests CQ functionality in multiuserMode.
   */

  public static void initCQTask(Boolean isDurable) {
    Region region = null;
    Map[] maps = new HashMap[2];
    Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
        .iterator();
    CqQueryTestListener.usedForUnitTests = false;
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);

      synchronized (userOperationMap) {
        if (userOperationMap.get(userName) == null) {
          userOperationMap.put(userName, new HashMap());
        }
      }
      synchronized (userExceptionMap) {
        if (userExceptionMap.get(userName) == null) {
          userExceptionMap.put(userName, new HashMap());
        }
      }
      maps[0] = userOperationMap.get(userName);
      maps[1] = userExceptionMap.get(userName);

      doExecuteCQ(region, userName, maps, isDurable);
    }
  }

  private static void doExecuteCQ(Region region, String userName, Map[] maps,
      Boolean isDurable) {
    Map opMap = maps[0];
    Map exMap = maps[1];
    String key = getKeyForOperation();
    boolean doAddOperation = true;

    String operation = "EXECUTE_CQ";
    String cqName1 = "CQ1_" + userName;
    String cqName2 = "CQ2_" + userName;
    SelectResults cqResults = null;
    String queryString = "SELECT * FROM " + region.getFullPath();
    //QueryService qs = region.getCache().getQueryService();
    QueryService qs = PerUserRequestSecurityTest.proxyAuthenticatedCacheMap.get(userName).getQueryService();

    try {
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners1 = { new CqQueryTestListener(Log.getLogWriter()) };
      ((CqQueryTestListener)cqListeners1[0]).cqName = cqName1;
      ((CqQueryTestListener)cqListeners1[0]).userName = userName;
      cqf.initCqListeners(cqListeners1);
      CqAttributes cqAttributes = cqf.create();
      CqQuery cq1 = qs.newCq(cqName1, queryString, cqAttributes, isDurable);
      cq1.execute();

      CqListener[] cqListeners2 = { new CqQueryTestListener(Log.getLogWriter()) };
      ((CqQueryTestListener)cqListeners2[0]).cqName = cqName2;
      ((CqQueryTestListener)cqListeners2[0]).userName = userName;
      cqf.initCqListeners(cqListeners2);
      cqAttributes = cqf.create();
      CqQuery cq2 = qs.newCq(cqName2, queryString, cqAttributes, isDurable);
      cqResults = cq2.executeWithInitialResults();
    }

    catch (Exception ex) {
      if (ex instanceof CacheLoaderException
          || ex instanceof CacheWriterException
          || (ex instanceof ServerConnectivityException && ex.getCause() instanceof NotAuthorizedException)
          || (ex instanceof CqException && ex.getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException && ex.getCause() instanceof NotAuthorizedException)) {

        Log.getLogWriter().info(
            "Got expected NotAuthorizedException when doing " + operation
                + " operation for " + userName + " : " + ex.getCause());
        synchronized (exMap) {
          Integer tmpNotAuthzCount = (Integer)exMap.get(operation);

          if (tmpNotAuthzCount == null) {
            exMap.put(operation, new Integer(1));
          }
          else {
            exMap.put(operation, new Integer(tmpNotAuthzCount.intValue() + 1));
          }
        }
      }
      else {
        doAddOperation = false;
        throw new TestException("Exception while CQ Operation for user :"
            + userName + " ", ex);

      }
    }
    finally {
      synchronized (opMap) {
        if (doAddOperation) {
          Integer tmpTotalOperation = (Integer)opMap.get(operation);

          if (tmpTotalOperation == null) {
            opMap.put(operation, new Integer(1));
          }
          else {
            opMap.put(operation, new Integer(tmpTotalOperation.intValue() + 1));
          }
        }
        Log.getLogWriter().fine(
            "For user :: " + userName + " operation : " + operation
                + " operation count is " + (Integer)opMap.get(operation)
                + " And Exception count is :: " + (Integer)exMap.get(operation)
                + " key is " + key);
      }
    }
  }

  /**
   * This method tests functionService in multiuserMode.
   */

  public static void startFunctionExecuteOperationForMultiUserMode() {
    Region region = null;
    Map[] maps = new HashMap[2];
    Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
        .iterator();
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);
      synchronized (userOperationMap) {
        if (userOperationMap.get(userName) == null) {
          userOperationMap.put(userName, new HashMap());
        }
      }
      synchronized (userExceptionMap) {
        if (userExceptionMap.get(userName) == null) {
          userExceptionMap.put(userName, new HashMap());
        }
      }
      maps[0] = userOperationMap.get(userName);
      maps[1] = userExceptionMap.get(userName);

      SecurityFunction function = new SecurityFunction(
          PerUserRequestSecurityTest.isFailOver);
      Random randomGen = new Random();
      int randomInt = randomGen.nextInt(3);
      if (PerUserRequestSecurityTest.isFailOver || randomInt == 0) {
        doFunctionExecute(region, userName, maps, function, "region");
      }
      else if (randomInt == 1) {
        doFunctionExecute(region, userName, maps, function, "server");
      }
      else if (randomInt == 2) {
        doFunctionExecute(region, userName, maps, function, "servers");
      }
    }
  }

  public static void doFunctionExecute(Region region, String userName,
      Map[] maps, Function function, String method) {
    Map opMap = maps[0];
    Map exMap = maps[1];
    String key = getKeyForOperation();
    String operation = "EXECUTE_FUNCTION";
    Execution dataSet = null;
    boolean doAddOperation = true;
    FunctionService.registerFunction(function);
    try {
      Pool pool = PoolHelper.getPool("brloader");
      if (pool == null) {
        pool = PoolHelper.createPool("brloader");
      }
      if ("servers".equals(method)) {
        RegionService cache = PerUserRequestSecurityTest.proxyAuthenticatedCacheMap.get(userName);
        dataSet = FunctionService.onServers(cache)
            .withCollector(new ArrayListResultCollector());
      }
      else if ("server".equals(method)) {
        RegionService cache = PerUserRequestSecurityTest.proxyAuthenticatedCacheMap.get(userName);
        dataSet = FunctionService.onServer(cache)
            .withCollector(new ArrayListResultCollector());
      }
      else if ("region".equals(method)) {
        dataSet = FunctionService.onRegion(region).withCollector(
            new ArrayListResultCollector());
      }
      ArrayList<Object> list = (ArrayList)dataSet.execute(function).getResult();
    }
    catch (Exception ex) {
      if (((ex instanceof CacheLoaderException
          || ex instanceof CacheWriterException || ex instanceof ServerConnectivityException) && ex
          .getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException && ex.getCause() instanceof NotAuthorizedException)
          || (ex instanceof FunctionException
              && ex.getCause() instanceof ServerOperationException && (ex
              .getCause()).getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException
              && ex.getCause() instanceof FunctionException && (ex.getCause())
              .getCause() instanceof NotAuthorizedException)) {
        Log.getLogWriter().info(
            "Got expected NotAuthorizedException when doing " + operation + "."
                + method + "() operation for " + userName + " : "
                + ex.getCause());
        synchronized (exMap) {
          Integer tmpNotAuthzCount = (Integer)exMap.get(operation);

          if (tmpNotAuthzCount == null) {
            exMap.put(operation, new Integer(1));
          }
          else {
            exMap.put(operation, new Integer(tmpNotAuthzCount.intValue() + 1));
          }
        }
      }
      else {
        doAddOperation = false;
        Log.getLogWriter().info(
            "Exception while Function Execution for method " + method + " :"
                + ex.getCause() + " for " + userName);
        throw new TestException("Exception while Function Execution:", ex);
      }
    }
    finally {
      synchronized (opMap) {
        if (doAddOperation) {
          Integer tmpTotalOperation = (Integer)opMap.get(operation);

          if (tmpTotalOperation == null) {
            opMap.put(operation, new Integer(1));
          }
          else {
            opMap.put(operation, new Integer(tmpTotalOperation.intValue() + 1));
          }
        }
        Log.getLogWriter().fine(
            "For user :: " + userName + " operation : " + operation
                + " operation count is " + (Integer)opMap.get(operation)
                + " And Exception count is :: " + (Integer)exMap.get(operation)
                + " key is " + key);
      }
    }
  }
  
  /**
   * This method selects one entry operation randomly in multiuserMode.
   */
  public static void startRandomOperationsForMultiUserMode() {
    Region region = null;

    Map[] maps = new HashMap[2];
    Iterator iter = PerUserRequestSecurityTest.proxyRegionMap.keySet()
        .iterator();
    while (iter.hasNext()) {
      String userName = (String)iter.next();
      region = PerUserRequestSecurityTest.proxyRegionMap.get(userName);
      synchronized (userOperationMap) {
        if (userOperationMap.get(userName) == null) {
          userOperationMap.put(userName, new HashMap());
        }
      }
      synchronized (userExceptionMap) {
        if (userExceptionMap.get(userName) == null) {
          userExceptionMap.put(userName, new HashMap());
        }
      }
      maps[0] = userOperationMap.get(userName);
      maps[1] = userExceptionMap.get(userName);
      startRandomOperationsOnRegion(region, userName, maps);

    }
  }
  
  

  public static void startRandomOperationsOnRegion(Region region,
      String userName, Map[] maps) {
    Map opMap = maps[0];
    Map exMap = maps[1];
    String key = getKeyForOperation();
    String operation = TestConfig.tab().stringAt(
        SecurityClientsPrms.entryOperations, "put");
    if (Log.getLogWriter().fineEnabled())
      Log.getLogWriter().info("Operation :" + operation + " and Key " + key);
    boolean doAddOperation = true;
    try {

      if (operation.equals("create")) {
        try {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.create(key, key + "_VALUE");
          } else {
            Object v = region.putIfAbsent(key, key + "_VALUE");
            if (v != null) {
               doAddOperation = false;
        }
          }
        }
        catch (EntryExistsException ex) {
          doAddOperation = false;
        }

      }
      else if (operation.equals("put")) {
        if (!PerUserRequestSecurityTest.writeToBB) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.put(key, key + "_Value");
          } else {
            Object v = region.putIfAbsent(key, key + "_VALUE");
            if (v != null) {
               region.replace(key, key + "_VALUE");
        }
          }
        }
        else {
          // This is to test durable CQ in multiuser mode.
          // The feeder will put the key value incrementing it by 1.
          if (SecurityClientBB.getBB().getSharedMap().containsKey(key)) {
            Long valueFromBB = (Long)SecurityClientBB.getBB().getSharedMap().get(key);
            if (TestConfig.tab().getRandGen().nextBoolean()) {
            region.put(key, valueFromBB + 1);
          } else {
              Object v = region.putIfAbsent(key, valueFromBB + 1);
              if (v != null) {
                 region.replace(key, valueFromBB + 1);
              }
            }
          } else {
            if (TestConfig.tab().getRandGen().nextBoolean()) {
            region.put(key, Long.valueOf(1));
            } else {
              region.putIfAbsent(key, Long.valueOf(1));
            }
            SecurityClientBB.getBB().getSharedMap().put(key, Long.valueOf(1));
          }
        }
      }
      else if (operation.equals("update")) {
        if (TestConfig.tab().getRandGen().nextBoolean()) {
        region.put(key, key + "_VALUE");
        } else {
            Object v = region.putIfAbsent(key, key + "_VALUE");
            if (v != null) {
               region.replace(key, key + "_VALUE");
      }
        }
      }
      else if (operation.equals("putAll")) {
        HashMap aMap = getMapForPutAll();

        Log.getLogWriter().info(
            "Putting into region with mapSize " + aMap.size());
        region.putAll(aMap);
      }
      else if (operation.equals("invalidate")) {
//        if (!region.containsKey(key)) {
//          Log.getLogWriter().info("Region does not have the key");
//        }
//        else {
          region.invalidate(key);
//        }

      }
      else if (operation.equals("destroy")) {
        try {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.destroy(key);
          } else {
            boolean removed = region.remove(key, key + "_VALUE");
            if (!removed) {
              doAddOperation = false;
        }
          }
        }
        catch (EntryNotFoundException ex) {
          doAddOperation = false;
        }
      }
      else if (operation.equals("get")) {

        /*
         * try { region.localInvalidate(key); } catch (Exception e) { }
         */
        region.get(key);
      }

      else if (operation.equals("query")) {

        String querystring = "select distinct * from " + region.getFullPath()
            + " where FALSE";
        SelectResults queryResults = region.query(querystring);

      }

      else {
        throw new TestException("Unknown entry operation: " + operation);
      }
    }
    catch (Exception ex) {
      if (((ex instanceof CacheLoaderException
          || ex instanceof CacheWriterException
          || ex instanceof ServerConnectivityException || ex instanceof QueryInvocationTargetException) && ex
          .getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException && ex.getCause() instanceof NotAuthorizedException)) {

        Log.getLogWriter().info(
            "Got expected NotAuthorizedException when doing " + operation
                + " operation :" + ex.getCause());
        synchronized (exMap) {
          Integer tmpNotAuthzCount = (Integer)exMap.get(operation);

          if (tmpNotAuthzCount == null) {
            exMap.put(operation, new Integer(1));
          }
          else {
            exMap.put(operation, new Integer(tmpNotAuthzCount.intValue() + 1));
          }
        }
      }
      else if (ex instanceof QueryInvocationTargetException) {
        if (QueryPrms.allowQueryInvocationTargetException()) {
          // ignore for PR HA tests
          Log
              .getLogWriter()
              .info(
                  "Caught "
                      + ex
                      + " (expected with concurrent execution); continuing with test");
        }
        else {
          doAddOperation = false;
          throw new TestException("Exception while EntryOperation:", ex);
        }
      }
      else {
        doAddOperation = false;
        throw new TestException("Exception while EntryOperation " + operation
            + " for user : " + userName + " : ", ex);
      }
    }
    finally {
      synchronized (opMap) {
        if (doAddOperation) {
          Integer tmpTotalOperation = (Integer)opMap.get(operation);

          if (tmpTotalOperation == null) {
            // 1st time = " + totalOperation.intValue());
            opMap.put(operation, new Integer(1));
          }
          else {
            opMap.put(operation, new Integer(tmpTotalOperation.intValue() + 1));
          }
        }
        Log.getLogWriter().info(
            "For user :: " + userName + " operation : " + operation
                + " operation count is " + (Integer)opMap.get(operation)
                + " And Exception count is :: " + (Integer)exMap.get(operation)
                + " key is " + key);
      }
    }
  }

  /** 
   *  Returns true if operation is an entry operation (create, put, update, destroy, get, or putAll)
   */
  private static boolean isEntryOp(String op) {
    boolean entryOp = false;
    if (op.equals("create")   || op.equals("put")        || 
        op.equals("update")   || op.equals("invalidate") ||
        op.equals("destroy")  || op.equals("get")        || 
        op.equals("putAll")) {
       entryOp = true;
    }
    return entryOp;
  }

  /**
   * This method is an select one entry operation randomly.
   */
  public synchronized static void startRandomOperations() {

    Region region = RegionHelper.getRegion(regionName);
    String key = getKeyForOperation();
    String operation = TestConfig.tab().stringAt(
        SecurityClientsPrms.entryOperations, "put");
    if (Log.getLogWriter().fineEnabled())
      Log.getLogWriter().info("Operation :" + operation + " and Key " + key);
    boolean doAddOperation = true;
    boolean useTransactions = SecurityClientsPrms.useTransactions();
    try {

      if (useTransactions && isEntryOp(operation)) {
        TxHelper.begin();
      }

      if (operation.equals("create")) {
        try {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.create(key, key + "_VALUE");
          } else {
            Object v = region.putIfAbsent(key, key + "_VALUE");
            if (v != null) {
              doAddOperation = false;
        }
          }
        } catch (EntryExistsException ex) {
          doAddOperation = false;
        } catch (TransactionDataNodeHasDepartedException e) {
          Log.getLogWriter().info("Caught " + e + ".  Expected, continuing test");
          doAddOperation = false;
        }
      }
      else if (operation.equals("put")) {
        if (TestConfig.tab().getRandGen().nextBoolean()) {
        region.put(key, key + "_VALUE");
        } else {
          Object v = region.putIfAbsent(key, key + "_VALUE");
          if (v != null) {
             region.replace(key, key + "_VALUE");
      }
        }
      }
      else if (operation.equals("update")) {
        if (TestConfig.tab().getRandGen().nextBoolean()) {
        region.put(key, key + "_VALUE");
        } else {
            Object v = region.putIfAbsent(key, key + "_VALUE");
            if (v != null) {
              region.replace(key, key + "_VALUE");
      }
        }
      }
      else if (operation.equals("putAll")) {
        HashMap aMap = getMapForPutAll();
        Log.getLogWriter().info(
            "Putting into region with mapSize " + aMap.size());
        region.putAll(aMap);
      }
      else if (operation.equals("invalidate")) {
//        if (!region.containsKey(key)) {
//          Log.getLogWriter().info("Region does not have the key");
//        }
//        else {
          region.invalidate(key);
//        }

      }
      else if (operation.equals("destroy")) {
        try {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.destroy(key);
          } else {
            boolean removed = region.remove(key, key + "_VALUE");
            if (!removed) {
               doAddOperation = false;
        }
          }
        } catch (EntryNotFoundException ex) {
          doAddOperation = false;
        } catch (TransactionDataNodeHasDepartedException e) {
          Log.getLogWriter().info("Caught " + e + ".  Expected, continuing test");
          doAddOperation = false;
        }
      }
      else if (operation.equals("get")) {
        // To ensure that we always get from server, use BridgeLoader
        if (useTransactions) {
          // See BUG 43263: for server affinity to work properly in tx, we cannot use
          // internal apis (like ServerProxy.get()).  In addition, within a tx, operations
          // are always on the server side.
          region.get(key);
        } else {
          LocalRegion lregion = (LocalRegion)region;
          LoaderHelper lhelper = lregion.createLoaderHelper(key, null, false, true, null);
          lregion.getServerProxy().get(key, null, null);
        }
      }
      else if (operation.equals("regNUnregInterest")) {
        region.registerInterest(key);
        region.unregisterInterest(key);
      }
      else if (operation.equals("query")) {
        String querystring = "select distinct * from " + region.getFullPath()
            + " where FALSE";
        SelectResults queryResults = region.query(querystring);

      }
      else if (operation.equals("cq")) {
        String querystring = "SELECT * FROM " + region.getFullPath();
        String cqName = "cq" + cqCounter.incrementAndGet() + "_" + key;
        CQUtil.registerAndCloseCQ(cqName, querystring, region);
      }
      else if (operation.equals("serverFunctionExecute")) {
        SecurityFunction function = new SecurityFunction();
        Pool pool = PoolHelper.getPool("brloader");
        if (pool == null) {
          Log.getLogWriter().info("Pool is null");
          pool = PoolHelper.createPool("brloader");
        }
        Execution dataSet;
        if (TestConfig.tab().getRandGen().nextBoolean()) {
          dataSet = FunctionService.onServers(pool).withCollector(
              new ArrayListResultCollector());
        }
        else {
          dataSet = FunctionService.onServer(pool).withCollector(
              new ArrayListResultCollector());
        }
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("serverExecuteWrongFunction")) {
        SecurityFunction function = new SecurityFunction();
        Pool pool = PoolHelper.getPool("brloader");
        if (pool == null) {
          Log.getLogWriter().info("Pool is null");
          pool = PoolHelper.createPool("brloader");
        }
        Execution dataSet;

        if (TestConfig.tab().getRandGen().nextBoolean()) {
          dataSet = FunctionService.onServers(pool).withCollector(
              new ArrayListResultCollector());
        }
        else {
          dataSet = FunctionService.onServer(pool).withCollector(
              new ArrayListResultCollector());
        }
        ArrayList list = (ArrayList)dataSet.execute(new FunctionAdapter() {

          public void execute(FunctionContext context) {
            DistributionConfig dc = ((InternalDistributedSystem)InternalDistributedSystem
                .getAnyInstance()).getConfig();
            ArrayList<Object> list = new ArrayList<Object>();
            list.add(dc.getCacheXmlFile());
            list.add(dc.getName());
            context.getResultSender().lastResult(list);
          }

          public String getId() {
            return "NonSecureFunction";
          }

        }).getResult();
      }
      else if (operation.equals("serverExecuteWrongPostAuthz")) {
        SecurityFunction function = new SecurityFunction();
        Pool pool = PoolHelper.getPool("brloader");
        if (pool == null) {
          Log.getLogWriter().info("Pool is null");
          pool = PoolHelper.createPool("brloader");
        }
        Execution dataSet;
        if (TestConfig.tab().getRandGen().nextBoolean()) {
          dataSet = FunctionService.onServers(pool).withArgs("Args")
              .withCollector(new ArrayListResultCollector());
        }
        else {
          dataSet = FunctionService.onServer(pool).withArgs("Args")
              .withCollector(new ArrayListResultCollector());
        }
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("functionExecute")) {
        SecurityFunction function = new SecurityFunction();
        Set<Object> keySet = (Set)SecurityClientBB.getBB().getSharedMap().get(
            KEY_SET);
        if (keySet == null || keySet.size() == 0) {
          throw new TestException(
              "Test issue: keySet for filter cannot be null");
        }
        HashSet<Object> args = new HashSet<Object>();
        args.add("KEY-0");
        args.add("KEY-1");
        keySet.removeAll(args);
        Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
            .withArgs(args).withCollector(new ArrayListResultCollector());
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("executeWrongFilter")) {
        SecurityFunction function = new SecurityFunction();
        Set keySet = region.keySet();
        HashSet<Object> args = new HashSet<Object>();
        args.add("KEY-0");
        args.add("KEY-1");
        Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
            .withArgs(args).withCollector(new ArrayListResultCollector());
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("executeWrongPostAuthz")) {
        SecurityFunction function = new SecurityFunction();
        Execution dataSet = FunctionService.onRegion(region).withCollector(
            new ArrayListResultCollector());
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("executeWrongRegion")) {
        SecurityFunction function = new SecurityFunction();
        HashSet<Object> args = new HashSet<Object>();
        args.add("KEY-0");
        args.add("KEY-1");
        Region nonSecureRegion = RegionHelper.getRegion(NON_SECURE_REGION_NAME);
        Execution dataSet = FunctionService.onRegion(nonSecureRegion).withArgs(
            args).withCollector(new ArrayListResultCollector());
        ArrayList<Object> list = (ArrayList)dataSet.execute(function)
            .getResult();
      }
      else if (operation.equals("executeWrongFunction")) {
        HashSet<Object> args = new HashSet<Object>();
        args.add("KEY-0");
        args.add("KEY-1");
        Execution dataSet = FunctionService.onRegion(region).withArgs(args)
            .withCollector(new ArrayListResultCollector());
        ArrayList list = (ArrayList)dataSet.execute(new FunctionAdapter() {
          public void execute(FunctionContext context) {

            final boolean isRegionContext = context instanceof RegionFunctionContext;
            RegionFunctionContext regionContext = null;
            if (isRegionContext) {
              regionContext = (RegionFunctionContext)context;
            }

            Region region = regionContext.getDataSet();
            ArrayList<Object> list = new ArrayList<Object>();

            Set<Object> keys = region.keySet();
            Iterator<Object> iterator = keys.iterator();
            while (iterator.hasNext()) {
              list.add(iterator.next());
            }

            if (regionContext.getArguments() != null) {
              HashSet args = (HashSet)regionContext.getArguments();
              list.removeAll(args);
            }

            context.getResultSender().lastResult(list);

          }

          public boolean hasResult() {
            return true;
          }

          public String getId() {
            return "NonSecureFunction";
          }

        }).getResult();
      }
      else if (operation.equals("executeWrongOptimization")) {
        HashSet<Object> args = new HashSet<Object>();
        args.add("KEY-0");
        args.add("KEY-1");
        Execution dataSet = FunctionService.onRegion(region).withArgs(args)
            .withCollector(new ArrayListResultCollector());
        ArrayList list = (ArrayList)dataSet.execute(new FunctionAdapter() {
          public void execute(FunctionContext context) {

            final boolean isRegionContext = context instanceof RegionFunctionContext;
            RegionFunctionContext regionContext = null;
            if (isRegionContext) {
              regionContext = (RegionFunctionContext)context;
            }

            Region region = regionContext.getDataSet();
            ArrayList<Object> list = new ArrayList<Object>();

            Set<Object> keys = region.keySet();
            Iterator<Object> iterator = keys.iterator();
            while (iterator.hasNext()) {
              list.add(iterator.next());
            }

            if (regionContext.getArguments() != null) {
              HashSet args = (HashSet)regionContext.getArguments();
              list.removeAll(args);
            }

            context.getResultSender().lastResult(list);

          }

          public String getId() {
            return "OptimizationFunction";
          }

          public boolean hasResult() {
            return true;
          }

          public boolean optimizeForWrite() {
            return true;
          }

        }).getResult();
      }
      else {
        throw new TestException("Unknown entry operation: " + operation);
      }
    }
    catch (Exception ex) {
      if (((ex instanceof CacheLoaderException
          || ex instanceof CacheWriterException
          || ex instanceof ServerConnectivityException
          || ex instanceof QueryInvocationTargetException || ex instanceof CqException) && ex
          .getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException && ex.getCause() instanceof NotAuthorizedException)
          || (ex instanceof FunctionException
              && ex.getCause() instanceof ServerOperationException && (ex
              .getCause()).getCause() instanceof NotAuthorizedException)
          || (ex instanceof ServerOperationException
              && ex.getCause() instanceof FunctionException && (ex.getCause())
              .getCause() instanceof NotAuthorizedException)) {

        Log.getLogWriter().info(
            "Got expected NotAuthorizedException when doing " + operation
                + " operation :" + ex.getCause());
        synchronized (exceptionMap) {
          notAuthzCount = (Integer)exceptionMap.get(operation);

          if (notAuthzCount == null) {
            exceptionMap.put(operation, new Integer(1));

          }
          else {
            exceptionMap.put(operation, new Integer(
                notAuthzCount.intValue() + 1));
          }
        }
      }
      else {
        if (ex instanceof CqException) {
          doAddOperation = false;
        }
        else if (ex instanceof QueryInvocationTargetException) {
          if (QueryPrms.allowQueryInvocationTargetException()) {
            // ignore for PR HA tests
            Log
                .getLogWriter()
                .info(
                    "Caught "
                        + ex
                        + " (expected with concurrent execution); continuing with test");
          }
          else {
            doAddOperation = false;
            throw new TestException("Exception while EntryOperation:", ex);
          }
        }
        else if (ex instanceof TransactionDataRebalancedException) {
           Log.getLogWriter().info("Caught " + ex + "  (expected with concurrent execution); continuing with test");
           doAddOperation = false;
        }
        else if (ex instanceof TransactionDataNodeHasDepartedException) {
           Log.getLogWriter().info("Caught " + ex + "  (expected with concurrent execution); continuing with test");
           doAddOperation = false;
        }
        else {
          doAddOperation = false;
          throw new TestException("Exception while EntryOperation:", ex);
        }
      }
    }
    finally {

      try {
         if (useTransactions && isEntryOp(operation)) {
           TxHelper.commit();
         }
      } catch (TransactionDataNodeHasDepartedException e) {
         Log.getLogWriter().info("Caught " + e + "  (expected with concurrent execution); continuing with test");
         doAddOperation = true;  
      } catch (TransactionDataRebalancedException e) {
         Log.getLogWriter().info("Caught " + e + "  (expected with concurrent execution); continuing with test");
         doAddOperation = true;  
      } catch (TransactionInDoubtException e) {
         Log.getLogWriter().info("Caught " + e + "  (expected with concurrent execution); continuing with test");
         doAddOperation = true;  
      }

      synchronized (operationsMap) {
        if (doAddOperation) {
          totalOperation = (Integer)operationsMap.get(operation);

          if (totalOperation == null) {
            // 1st time = " + totalOperation.intValue());
            operationsMap.put(operation, new Integer(1));

          }
          else {

            operationsMap.put(operation, new Integer(
                totalOperation.intValue() + 1));

          }
        }
        if (Log.getLogWriter().fineEnabled())
          Log.getLogWriter().fine(
              "For operation : " + operation + " operation count is "
                  + (Integer)operationsMap.get(operation)
                  + " Exception count is "
                  + (Integer)exceptionMap.get(operation) + " key is " + key);
      }
    }

  }

  // get aMap for putAll
  protected static HashMap getMapForPutAll() {
    HashMap aMap = new HashMap();
    int mapSize = TestConfig.tab()
        .intAt(SecurityClientsPrms.putAllMapSize, 100);
    for (int i = 0; i < mapSize; i++) {
      Object key = getKeyForOperation();
      Object value = key + "_VALUE";
      aMap.put(key, value);
      // if (Log.getLogWriter().fineEnabled())
      Log.getLogWriter().info("Putting into putAll mpa for Key " + key);
    }
    return aMap;
  }

  /*
   * Return the CQ attributes
   */
  private static CqAttributes getCQAttributes() {
    CqAttributesFactory factory = new CqAttributesFactory();
    factory.addCqListener(CQUtilPrms.getCQListener());
    CqAttributes cqAttrs = factory.create();
    return cqAttrs;
  }

  /**
   * Randomly pick a key within the PUT_KEY_RANGE for doing operation.
   * 
   * @return string key
   */
  private static String getKeyForOperation() {
    Random r = new Random();
    int randint = r.nextInt(PUT_KEY_RANGE);
    long numOfThread = SecurityClientBB.getBB().getSharedCounters().read(
        SecurityClientBB.threadCount);
    int randint2 = r.nextInt(PUT_KEY_RANGE);
    Integer randthrdId = (Integer)thrId.get();
    if (randthrdId == null) {
      thrId.set(getUniqueThreadId());
      randthrdId = (Integer)thrId.get();
    }
    String Key = "KEY-" + randthrdId + "-" + randint + "-" + randint2;
    return (Key);
  }

  private static synchronized Integer getUniqueThreadId() {
    String thrName = HydraThread.currentThread().getName();
    int startIndex = thrName.indexOf("thr") + 4;
    int endIndex = thrName.indexOf("_", startIndex);
    String thrId = thrName.substring(startIndex, endIndex);
    return new Integer(thrId);
  }

}
