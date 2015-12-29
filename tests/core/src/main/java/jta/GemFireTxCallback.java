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

import com.gemstone.gemfire.cache.*;
import hydra.*;
import util.*;

/**
 * @author nandk
 * 
 * Reworked by Lynn Hughes-Godfrey (6.1)
 */
public class GemFireTxCallback {

  static boolean isSerialExecution;

  // each thread has a dbConnection for each task invocation
  private static ThreadLocal dbConnection = new ThreadLocal();

  public static Connection getDBConnection() throws Exception {
    Connection conn = (Connection)dbConnection.get();
    if (conn == null) {
      conn = DBUtil.getXADSConnection();
      setDBConnection(conn);
    }
    return (conn);
  }

  private static void setDBConnection(Connection conn) {
     dbConnection.set(conn);
  }

  /**
   * create database table
   */
  public synchronized static void createDatabase() {

    try {
      Cache cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());

      // create and populate database
      Log.getLogWriter().info("Creating database table and entries ...");

      // create a unique name for the oracle database table (since multiple
      // tests/regressions could be running concurrently).
      // we cannot have '-' in the table name (Wenatchee data center hosts)
      String hostName = RemoteTestModule.getMyHost().replace('-', '_');
      setDBTableName("t_" + hostName + "_" + RemoteTestModule.getMyPid());

      DBUtil.createTable(getDBTableName());
      Log.getLogWriter().info("Created database table " + getDBTableName() + " with entries.");
      CacheHelper.closeCache();
    } catch (Exception e) {
      throw new TestException("Error in createDatabase() " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * drop database table (CLOSETASK)
   */
  public synchronized static void dropOracleTable() {
    try {
      // drop table for this test (since tables are now uniquely named)
      Log.getLogWriter().info("Dropping oracle database table " + getDBTableName());
      DBUtil.dropTable(getDBTableName());
      Log.getLogWriter().info("Dropped oracle database table");
    } catch (Exception e) {
      throw new TestException("Unexpected exception caught in dropTable() " + e);
    }
  }

  /** accessor methods for oracle tableName (dynamically created for oracle tests)
   */
  public static void setDBTableName(String aName) {
    Log.getLogWriter().info("setDBTableName " + aName);
    JtaBB.getBB().getSharedMap().put(JtaBB.dbTableName, aName);
  }

  public static String getDBTableName() {
    String tableName = (String)JtaBB.getBB().getSharedMap().get(JtaBB.dbTableName);
    return tableName;
  }

  /**
   * dump database table
   */
  public synchronized static void dumpDatabase() {
    try {
      Cache cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());

      // create and populate database
      DBUtil.displayData(getDBTableName());
      CacheHelper.closeCache();
    } catch (Exception e) {
      throw new TestException("Error in dumpDatabase() " + TestHelper.getStackTrace(e));
    }
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

        Log.getLogWriter().info("Creating employee subregion ...");
        Region employee = root.createSubregion("employee", ratts);
        Log.getLogWriter().info("Created employee subregion.");
        
        // populate the local map with database values (via DBLoader)
        // only one thread in the tset needs to load the entries into the cache
        if (employee.isEmpty()) {
          Log.getLogWriter().info("Loading database entries into cache ....");
          for (int i=1; i <=4; i++) {
            String key = "key"+i;
            employee.get(key);
          }
          Log.getLogWriter().info("Loaded database entries into cache.");
        }
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

    boolean isRollBack = false;

    // check for any listener exceptions thrown by previous method execution
    TestHelper.checkForEventError(JtaBB.getBB());

    Cache cache = CacheHelper.getCache();
    Region employee = cache.getRegion("root/employee");
    String tableName = getDBTableName();
    
    try {
      	
      // Step 1: begin a transaction and update an entry in via region.put()
      TxHelper.begin();
      
      localCounter = JtaBB.getBB().getSharedCounters().incrementAndRead(JtaBB.COUNTER);

      key = "key" + ((localCounter % 4) + 1);  // key1 ... key4
      newValue = "value" + localCounter;
      dbValue = DBUtil.getDBValue(tableName, key);
      Log.getLogWriter().info("Selected " + key + " for update, original dbValue is " + dbValue + " new value will be " + newValue);

      employee.put(key, newValue);
      
      try {
        if (TestConfig.tab().getRandGen().nextInt(1,100) < 25) {
          Log.getLogWriter().info("ROLLING BACK transaction with " + key + "(" + newValue + ")"); 
          TxHelper.rollback();
          Log.getLogWriter().info("ROLLED BACK.");
          isRollBack = true;
        } else {
          Log.getLogWriter().info("COMMITTING transaction with " + key + "(" + newValue + ")"); 
          TxHelper.commit();
          Log.getLogWriter().info("COMMITTED.");
        }
      } catch (ConflictException cce) {
        String errStr = cce.toString();
        boolean isCausedByTransactionWriterException = errStr.indexOf("intentionally throw") >= 0;
        if (isSerialExecution && !isCausedByTransactionWriterException) {
          throw new TestException("Unexpected ConflictException " + cce + " caught in serialExecution test");
        } else {
          Log.getLogWriter().info("testTask caught expected " + cce + ", continuing with test");
          isRollBack = true;
        }
      } catch (Exception e) {
        Log.getLogWriter().error("testTask caught exception ", e);
        throw new TestException("Error in testTask during commit/rollback " + TestHelper.getStackTrace(e));
      }

      if (isSerialExecution) {
        // Verify region <-> database consistency
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
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTask() for " + key + "(" + newValue + ")", e);
      throw new TestException("Error in testTask() " + TestHelper.getStackTrace(e));
    } finally {
        try {
          Connection conn = getDBConnection();
          if (conn != null) conn.close();
        } catch (Exception e) {
          Log.getLogWriter().info("conn.close() threw " + e);
          throw new TestException("conn.close() threw " + e + " " + TestHelper.getStackTrace(e));
        }
      setDBConnection(null);
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
    String dbValue = DBUtil.getDBValue(getDBTableName(), key);
 
    Log.getLogWriter().info("checkData (" + ((isCommit)?"commit":"rollback") + ") for " + key + " and newValue " + value + " found region value " + regionVal + ", database value " + dbValue);
    if (isCommit) {
      if (!(regionVal.equals(value))) {
        Log.getLogWriter().info("checkData (on commit): region value for " + key + " is " + regionVal + ", expected " + value);
        return false;
      }
      if (!(dbValue.equals(value))) {
        Log.getLogWriter().info("checkData (on commit): dbValue for " + key + " is " + dbValue + ", expected " + value);
        return false;
      }
    } else {  // rollback
      if (regionVal.equals(value)) {
        Log.getLogWriter().info("checkData (on rollback): region value for " + key + " is " + regionVal + ", did not expect it to be updated to dbValue " + value + " after rollback");
        return false;
      }
      if (dbValue.equals(value)) {
        Log.getLogWriter().info("checkData (on rollback): dbValue for " + key + " is " + dbValue + ", did not expect it to be updated to newValue " + value + " after rollback");
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
      map = DBUtil.getData(getDBTableName());
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
        String dbValue = DBUtil.getDBValue(getDBTableName(), key);
        displayStr.append("  " + key + " dbValue = " + dbValue + " regionValue = " + regionValue + "\n");
        if(!dbValue.equals(regionValue)){
          aStr.append("Data inconsistency detected for " + key + " dbValue = " + dbValue + " and regionValue " + regionValue);
        }
      }
    } catch(Exception e){
      throw new TestException("Exception in validateFinally ", e);
    }

    if (aStr.length() > 0) {
      Log.getLogWriter().info("Validation FAILED\n" + displayStr.toString());
      throw new TestException(aStr.toString() + TestHelper.getStackTrace());
    } else {
      Log.getLogWriter().info("Validation SUCCESSFUL\n" + displayStr.toString());
    }
  }
}
