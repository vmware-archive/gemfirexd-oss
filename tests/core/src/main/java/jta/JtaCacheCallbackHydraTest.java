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

package jta;

import java.util.*;

import java.sql.*;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.datasource.DataSourceFactory;
import com.gemstone.gemfire.internal.jta.GlobalTransaction;

import dunit.DistributedTestCase;

import hydra.*;
import util.*;

/**
 * @author nandk
 * 
 * Reworked by Lynn Hughes-Godfrey (6.1)
 */
public class JtaCacheCallbackHydraTest {

  static final String TABLENAME = "CacheTest";
  static boolean isSerialExecution;

  /**
   * Start Task (to create database)
   * 
   * @throws Exception
   */
  public static void createDatabase() {
    try {

      if (JtaPrms.useDerbyNetworkServer()) {
        GlobalTransaction.DISABLE_TRANSACTION_TIMEOUT_SETTING = true;
        DataSourceFactory.setTestConnectionUrl(getDerbyURL());
        DataSourceFactory.setTestConnectionHost(DerbyServerHelper.getEndpoint().getHost());
        DataSourceFactory.setTestConnectionPort(String.valueOf(DerbyServerHelper.getEndpoint().getPort()));
      }

      Cache cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      Log.getLogWriter().info("Creating database ...");
      DBUtil.createTable(TABLENAME);
      Log.getLogWriter().info("Created database.");
      CacheHelper.closeCache();
    } catch (Exception e) {
      throw new TestException("Error in createDatabase()" + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Init task for bridgeServers working with JTA edge clients
   */
  public synchronized static void HydraTask_initializeBridgeServer() {
     if (CacheHelper.getCache() == null) {
        Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());

        // Install the TransactionListener, if configured
        TransactionListener txListener = JtaPrms.getTxListener();
        if (txListener != null) {
          c.getCacheTransactionManager().setListener(txListener);
          Log.getLogWriter().info("Installed TransactionListener " + txListener);
        }

        // Install the TransactionWriter, if configured
        TransactionWriter txWriter = JtaPrms.getTxWriter();
        if (txWriter != null) {
          ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(txWriter);
          Log.getLogWriter().info("Installed TransactionWriter " + txWriter);
        }

        try {
           // create root region (baseed on RegionPrms)
           Region root = RegionHelper.createRegion("root", ConfigPrms.getRegionConfig());

           // create employee subregion
           RegionAttributes ratts = RegionHelper.getRegionAttributes("bridgeSubregion");
  
           Log.getLogWriter().info("Creating employee subregion ...");
           Region employee = root.createSubregion("employee", ratts);
           Log.getLogWriter().info("Created employee subregion.");
        } catch (Exception e) {
           throw new TestException("Error in initTask() " + TestHelper.getStackTrace(e));
        }
     }
     BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  /**
   * EndTask (to display final values in database)
   * 
   * @throws Exception
   */
  public static void dumpDatabase() {

    try {
      if (JtaPrms.useDerbyNetworkServer()) {
        GlobalTransaction.DISABLE_TRANSACTION_TIMEOUT_SETTING = true;
        DataSourceFactory.setTestConnectionUrl(getDerbyURL());
        DataSourceFactory.setTestConnectionHost(DerbyServerHelper.getEndpoint().getHost());
        DataSourceFactory.setTestConnectionPort(String.valueOf(DerbyServerHelper.getEndpoint().getPort()));
      }

      Cache cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      DBUtil.displayData(TABLENAME);
      CacheHelper.closeCache();
    } catch (Exception e) {
      throw new TestException("Error in dumpDatabase()" + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Use a loader to get the entries from the database into the cache
   */
  public synchronized static void loadCache() {
    Cache cache = CacheHelper.getCache();
    Region employee = RegionHelper.getRegion("/root/employee");

    if (employee.isEmpty()) {
      Log.getLogWriter().info("Loading database entries into cache ....");
      for (int i=1; i <=4; i++) {
        String key = "key"+i;
        employee.get(key);
      }
    }
    Log.getLogWriter().info("Loaded database entries into cache.");
  }

  /**
   * Initialize Cache, region and database table
   */
  public synchronized static void initTask() {

    // initialize cache, regions and database
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      try {
        System.setProperty("derby.locks.waitTimeout","180");

        if (JtaPrms.useDerbyNetworkServer()) {
          GlobalTransaction.DISABLE_TRANSACTION_TIMEOUT_SETTING = true;
          DataSourceFactory.setTestConnectionUrl(getDerbyURL());
          DataSourceFactory.setTestConnectionHost(DerbyServerHelper.getEndpoint().getHost());
          DataSourceFactory.setTestConnectionPort(String.valueOf(DerbyServerHelper.getEndpoint().getPort()));
        }

        cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());

        // Install the TransactionListener, if configured
        TransactionListener txListener = JtaPrms.getTxListener();
        if (txListener != null) {
          cache.getCacheTransactionManager().setListener(txListener);
          Log.getLogWriter().info("Installed TransactionListener " + txListener);
        }
 
        // Install the TransactionWriter, if configured
        TransactionWriter txWriter = JtaPrms.getTxWriter();
        if (txWriter != null) {
          ((CacheTransactionManager)cache.getCacheTransactionManager()).setWriter(txWriter);
          Log.getLogWriter().info("Installed TransactionWriter " + txWriter);
        } 

        // create employee subregion 
        Region root = cache.getRegion("root");
        RegionAttributes ratts = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
        // For tests with edgeClients, create the pool before creating the subregion
        String poolConfig = ratts.getPoolName();
        if (poolConfig != null) {
          PoolHelper.createPool(poolConfig);
        }

        Log.getLogWriter().info("Creating employee subregion ...");
        Region employee = root.createSubregion("employee", ratts);
        if (employee.getAttributes().getPoolName() != null) {
           employee.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
           Log.getLogWriter().info("registered interest in ALL_KEYS for " + employee.getFullPath());
         }
        Log.getLogWriter().info("Created employee subregion.");
      } catch (Exception e) {
        throw new TestException("Error in initTask() " + TestHelper.getStackTrace(e));
      }
      isSerialExecution = TestConfig.tab().booleanAt(Prms.serialExecution, false);
    }
  }
    
  /**
   *  All threads update values transactionally.  
   */
  public static void testTask() {

    long localCounter = 0;

    String key = null;
    String dbValue = null;
    String newValue = null;
    String regionVal = null;

    boolean isRollBack = false;

    // check for any listener exceptions thrown by previous method execution
    TestHelper.checkForEventError(JtaBB.getBB());

    Cache cache = CacheHelper.getCache();
    Region employee = cache.getRegion("root/employee");
    ((DBLoader)employee.getAttributes().getCacheLoader()).setTxLoad(true);
    
    try {
      	
      // Step 1: begin a transaction and update an entry in via region.put()
      Context ctx = null;
      UserTransaction utx = null;
      ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      boolean rolledback = false;

      utx.begin();
      
      localCounter = JtaBB.getBB().getSharedCounters().incrementAndRead(JtaBB.COUNTER);
      key = "key" + ((localCounter % 4) + 1);  // key1 ... key4
      newValue = "value" + localCounter;
      dbValue = DBUtil.getDBValue(TABLENAME, key);

      if (TestConfig.tab().getRandGen().nextBoolean()) {
        regionVal = (String)employee.get(key);
        Log.getLogWriter().info("region value for key (" + key + ") = " + regionVal);
      }

      Log.getLogWriter().info("Selected " + key + " for update, original dbValue is " + dbValue + " new value will be " + newValue);

      employee.put(key, newValue);

      // With PRs (aswith bridgeServers), the data could be hosted in a remote VM which would cause the writer
      // to also be invoked in the remote VM, which means we will not have our DB connection
      // (since associated with the utx thread), so we cannot do the db update/commit in 
      // a callback in these configurations.  We can only use the callbacks to update the db
      // with a single VM OR with replicate peers (since this is the only time we can guarantee
      // that the writer will be invoked locally in the same thread that established the db connection.
      if (JtaPrms.executeDBOpsInline()) {
        try {
          Connection conn = DBUtil.getXADSConnection();
          String sql = "UPDATE " + TABLENAME + " SET name = '" + newValue + "' WHERE id = ('" + key + "')";
          int i = DBUtil.executeUpdate(sql, conn);
          Log.getLogWriter().info("rows updated = " + i);
          conn.close();
        } catch(Exception e) {
          synchronized(JtaCacheCallbackHydraTest.class) { // synchronize to prevent interleaving
            Log.getLogWriter().info("testTask caught " + e, e);
            if (e.getMessage().indexOf("A lock could not be obtained") >= 0) {
              DistributedTestCase.dumpMyThreads(Log.getLogWriter());
            }
          }
          throw new TestException("testTask caught " + e);
        }
      }
      
      try {
        
        if (TestConfig.tab().getRandGen().nextInt(1,100) < 25) {
          Log.getLogWriter().info("ROLLING BACK transaction with " + key + "(" + newValue + ")"); 
          utx.rollback();
          Log.getLogWriter().info("ROLLED BACK.");
          isRollBack = true;
        } else {
          Log.getLogWriter().info("COMMITTING transaction with " + key + "(" + newValue + ")"); 
          utx.commit();
          Log.getLogWriter().info("COMMITTED.");
        }
      } catch (RollbackException r) {
        Throwable causedBy = r.getCause();
        String errStr = causedBy.toString();
        boolean isCausedByTransactionWriterException = errStr.indexOf("intentionally throw") >= 0;
        if (isSerialExecution && !isCausedByTransactionWriterException) {
          throw new TestException("Unexpected exception " + r + " caught in serialExecution test");
        } else {
          Log.getLogWriter().info("Caught RolledbackException " + r + " for " + key + "(" + newValue + "): expected with concurrent operations, continuing test");
          isRollBack = true;
        }
      } catch (Exception e) {
        Log.getLogWriter().error("testTask caught exception during commit/rollback ", e);
        throw new TestException("Error in testTask during commit/rollback " + TestHelper.getStackTrace(e));
      }

      // Verify region <-> database consistency (only for serialExecution tests)
      if (isSerialExecution) {
        try {
          Log.getLogWriter().info("Validating data consistency for " + (isRollBack?"Rollback":"Commit") + " with " + key + "(" + newValue + ")");
          if (!checkData(employee, key, newValue, !isRollBack)) {
            String s = "Region/DataBase inconsistent for " +(isRollBack?"Rollback":"Commit") + " with " + key + "(" + newValue + ")";
            Log.getLogWriter().info(s);
            throw new TestException(s + " " + TestHelper.getStackTrace());
          }
          Log.getLogWriter().info("data consistency verified after " + (isRollBack?"Rollback":"Commit") + " for " + key + "(" + newValue + ")");
        } catch(Exception e) {
          String s = "Exception while retrieving the Data for validation of commit for " + key + "(" + newValue + ")";
          Log.getLogWriter().info(s, e);
          throw new TestException(s + " " + TestHelper.getStackTrace(e));
        }
      }
    } catch (TransactionDataNodeHasDepartedException e) { 
       Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
    } catch (TransactionDataRebalancedException e) { 
       Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTask() for " + key + "(" + newValue + ")", e);
      throw new TestException("Error in testTask() " + TestHelper.getStackTrace(e));
    } 
  }   
      
  /** Validate value for key in region 
   *
   *  @param region - GemFire region
   *  @param key - entry key in region, database and valuesMap
   *  @param value - expected value
   *  @param isCommit - true if tx committed (false if rolled back)
   */
  public static boolean checkData(Region region, String key, String value, boolean isCommit) throws Exception {
    String regionVal = (String)region.get(key);

    Log.getLogWriter().info("checkData (" + ((isCommit)?"commit":"rollback") + ") for " + key + " and newValue " + value + " found region value " + regionVal);
    if (isCommit) {
      if (!(regionVal.equals(value))) {
        Log.getLogWriter().info("checkData (on commit): region value for " + key + " is " + regionVal + ", expected newValue " + value);
        return false;
      }
    } else {  // rollback
      if (regionVal.equals(value)) {
        Log.getLogWriter().info("checkData (on rollback): region value for " + key + " is " + regionVal + ", did not expect it to be updated to newValue " + value + " after rollback");
        return false;
      }
    }
    return true;
  }

  /**
   * CloseTask to verify that all values in the database are consistent with the GemFire cache
   */
  public static void closeTask()  {
    validateFinally();
  }
  
  /** verify that all validates in the database and consistent with the values 
   *  in the Cache.
   */
  public static void validateFinally() {
    Cache cache = CacheHelper.getCache();
    Region employee = cache.getRegion("root/employee");
    Set keySet = employee.keySet();
    StringBuffer aStr = new StringBuffer();

    Map map = null;
    try {
      map = DBUtil.getData(TABLENAME);
    } catch(Exception e){
      throw new TestException("Caught unexpected Exception in validateFinally " + e);
    }

    Set dbKeys = map.keySet();
    if (dbKeys.size() != keySet.size()) {
      aStr.append("Inconsistency detected with database size = " + dbKeys.size() + " and cache size = " + keySet.size());
    }

    // What keys are missing from keySet?
    for (Iterator ks = keySet.iterator(); ks.hasNext(); ) {
      String key = (String)ks.next();
      if (!dbKeys.contains(key)) {
        aStr.append(key + " missing from database\n");
      }
    }

    // What keys are missing from the database?
    for (Iterator db = dbKeys.iterator(); db.hasNext(); ) {
      String key = (String)db.next();
      if (!keySet.contains(key)) {
        aStr.append(key + " missing from GemFire cache\n");
      }
    }
 
    StringBuffer displayStr = new StringBuffer();
    try {
      Log.getLogWriter().info("In validateFinally(), verifying " + map.size() +  " entries");
      for (Iterator ks = keySet.iterator(); ks.hasNext(); ) {
        String key = (String)ks.next();
        String regionValue = (String)employee.get(key);
        String dbValue = DBUtil.getDBValue(TABLENAME, key);
        displayStr.append("  " + key + " dbValue = " + dbValue + " regionValue = " + regionValue + "\n");
        if(!dbValue.equals(regionValue)){
          aStr.append("Data inconsistency detected for " + key + " dbValue = " + dbValue + " and regionValue " + regionValue);
        }
      }
    } catch(Exception e){
      throw new TestException("Caught unexpected Exception in validateFinally " + e);
    }

    if (aStr.length() > 0) {
      Log.getLogWriter().info("Validation FAILED\n" + displayStr.toString());
      throw new TestException(aStr.toString() + TestHelper.getStackTrace());
    } else {
      Log.getLogWriter().info("Validation SUCCESSFUL\n" + displayStr.toString());
    }
  }

  /** For multiVM JTA tests (which require the derby network server)
   *  create a derby network server URL to look like:
   *  "jdbc:derby://<host>:<port>/<dbName>;create=true"
   */
  private static String getDerbyURL() {
    String protocol = "jdbc:derby:";
    String dbName = "newDB";

    String host = DerbyServerHelper.getEndpoint().getHost();
    int port = DerbyServerHelper.getEndpoint().getPort();

    StringBuffer url = new StringBuffer();
    url.append(protocol);
    url.append("//" + host + ":" + port + "/");
    url.append(dbName);
    url.append(";create=true");
    Log.getLogWriter().info("Derby URL = " + url.toString());
    return (url.toString());
  }

  /**
   * Randomly stop and restart vms which are not rebalancing (rebalance must 
   * be part of the clientName).  Tests which invoke this must also call
   * util.StopStartVMs.StopStart_init as a INITTASK (to write clientNames
   * to the Blackboard).
  */
  public static void HydraTask_stopStartServerVM() {
     int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
     int randInt =  TestConfig.tab().getRandGen().nextInt(1, numVMsToStop);
     Object[] objArr = StopStartVMs.getOtherVMsWithExclude(randInt, "edge");
     List clientVmInfoList = (List)(objArr[0]);
     List stopModeList = (List)(objArr[1]);
     StopStartVMs.stopStartVMs(clientVmInfoList, stopModeList);
  }
}
