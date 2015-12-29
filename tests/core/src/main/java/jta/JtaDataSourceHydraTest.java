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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import java.util.*;

import com.gemstone.gemfire.cache.*;

import hydra.*;
import util.*;

/**  This test performs operations on an external database (testTask) or
 *   optionally on both the cache and the database.
 *  
 * @author nandk
 */
public class JtaDataSourceHydraTest {

  private static final int PUT_OPERATION = 1;
  private static final int GET_OPERATION = 2;
  private static final int UPDATE_OPERATION = 3;
  
  /**
   * Start Task for DataSource Hydra test
   * 
   * @throws Exception
   */
  public static void startTask() {
    try {
      Cache cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      JTAUtil.createTable("JtaDataSourceTest");
      JTAUtil.listTableData("JtaDataSourceTest");
      Log.getLogWriter().info("Created table ");
      CacheHelper.closeCache();
    } catch (Exception e) {
      throw new TestException("Error in Cache Initialization in StartTask " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Init Task for DataSource Hydra test
   */
  public synchronized static void initTask() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      try {
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
      } catch (Exception e) {
        throw new TestException("Error in Cache Initialization in initTask " + TestHelper.getStackTrace(e));
      }
    }
    JtaCacheTestUtil.createRootRegions();
  }

  /**
   * Test Task for DataSource Hydra test
   */
  public static void testTask() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Log.getLogWriter().info("Starting Transaction ");
      utx.begin();
      (new JtaDataSourceHydraTest()).doRandomOperation();
      utx.commit();
      Log.getLogWriter().info("Committed Transaction ");
    } catch (RollbackException re) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ", re);
    } catch (Exception e) {
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("explicitly Rolledback Transaction ");
        } catch (Exception e1) {
          throw new TestException("Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(e1));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Test Task for DataSource And Cache Hydra test
   */
  public static void testCacheAndDBTask() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Log.getLogWriter().info("Starting Transaction ");
      utx.begin();
      (new JtaDataSourceHydraTest()).doRandomCacheAndDBOperation();
      utx.commit();
      Log.getLogWriter().info("Committed Transaction ");
    } catch (RollbackException re) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ", re);
    } catch (Exception e) {
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("Explicitly Rolledback Transaction ");
        } catch (Exception e1) {
          throw new TestException("Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(e1));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }
  
  /**
   * Close Task for DataSource Hydra test
   */
  public static void closeTask() {
    JtaBB.getBB().printSharedCounters();
  }

  /**
   * End Task for DataSource Hydra test
   */
  public static void endTask() {
    Cache cache = CacheHelper.getCache();
    try {
      if (cache == null) {
        cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      }
      JTAUtil.listTableData("JtaDataSourceTest");
      JTAUtil.destroyTable("JtaDataSourceTest");
      Log.getLogWriter().info("Destroyed table ");
    }
    catch (Exception e) {
      throw new TestException("Error in endTask " + TestHelper.getStackTrace(e));
    }
  }

  /*
   * Does the random operation ( put/get/update ) for DataSource (only)
   */
  private void doRandomOperation() throws Exception {
    Context ctx = (CacheHelper.getCache()).getJNDIContext();
    DataSource ds = null;
    try {
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    } catch (Exception e) {
      Log.getLogWriter().info("Failed to get DataSource, The Exception is " + e);
      throw e;
    }

    int total = TestConfig.tab().intAt(JtaPrms.numberOfEvents);
    int count = 0;
    while (count < total) {
      int whichOp = JtaCacheTestUtil.getRandomOperation(JtaPrms.entryOperations);
      switch (whichOp) {
        case PUT_OPERATION:
          doPutOperation(ds);
          break;
        case GET_OPERATION:
          doGetOperation(ds);
          break;
        case UPDATE_OPERATION:
          doUpdateOperation(ds);
          break;
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
      }
      count++;
    }
  }

  /*
   * Does either put operation on the cache or update operation in the DataBase.
   */
  
  private void doRandomCacheAndDBOperation() throws Exception {
    Cache cache = CacheHelper.getCache();
    Context ctx = cache.getJNDIContext();
    String regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
    Region region = RegionHelper.getRegion(regionName);

    DataSource ds = null;
    try {
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    } catch (Exception e) {
      Log.getLogWriter().info("Failed to get DataSource, The Exception is " + e);
      throw e;
    }

    int total = TestConfig.tab().intAt(JtaPrms.numberOfEvents);
    int count = 0;
    while (count < total) {
      int whichOp = JtaCacheTestUtil.getRandomOperation(JtaPrms.entryOperations);
      switch (whichOp) {
        case PUT_OPERATION:
          (new JtaCacheTestUtil()).doPutOperation(region);
          break;
        case UPDATE_OPERATION:
          doUpdateOperation(ds);
          break;
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
      }
      count++;
    }
  }
  
  /*
   * Does an update operation.
   */
  private void doUpdateOperation(DataSource ds) throws SQLException {
    Connection conn = null;
    Statement sm = null;
    ResultSet rs = null;
    try {
      conn = ds.getConnection();
      sm = conn.createStatement();
      int i = (new Random()).nextInt(1000);
      String newName = "" + (new Random()).nextLong();
      String sql = "update JtaDataSourceTest set name = '" + newName + "' where id = " + i;
      sm.execute(sql);
      Log.getLogWriter().info("doUpdateOperation: " + sql);
      JtaBB.incrementCounter("DoUpdateOperation", JtaBB.NUM_UPDATE);
    } catch (SQLException e) {
      Log.getLogWriter().info("Failed in doUpdateOperation, The exception is " + e);
      throw e;
    } finally {
      try {
        if (rs != null) rs.close();
        if (sm != null) sm.close();
        if (conn != null) conn.close();
      } catch (SQLException e1) {
        Log.getLogWriter().info("Failed in doUpdateOperation");
        throw e1;
      }
      rs = null;
      sm = null;
      conn = null;
    }
  }

  /*
   * perform a get operation on datasource. @param ds @throws RollbackException
   */
  private void doGetOperation(DataSource ds) throws SQLException {
    Connection conn = null;
    Statement sm = null;
    ResultSet rs = null;
    try {
      conn = ds.getConnection();
      sm = conn.createStatement();
      int i = (new Random()).nextInt(1000);
      String sql = "select name from JtaDataSourceTest where id = " + i;
      rs = sm.executeQuery(sql);
      if (rs.next()) {
          Log.getLogWriter().info("Got value for id " + i + " as " + rs.getString(1));
      }
      JtaBB.incrementCounter("DoGetOperation", JtaBB.NUM_GET);
    } catch (SQLException e) {
      Log.getLogWriter().info("doGetOperation failed ");
      throw e;
    } finally {
      try {
        if (rs != null) rs.close();
        if (sm != null) sm.close();
        if (conn != null) conn.close();
      } catch (SQLException e1) {
        Log.getLogWriter().info("doGetOperation failed, The Exception is  " + e1);
        throw e1;
      }
      rs = null;
      sm = null;
      conn = null;
    }
  }

  /*
   * perform a put operation on datasource. Its only symbolic. In fact we are
   * not doing anything except calling this method. @param ds DataSource.
   */
  private void doPutOperation(DataSource ds) {
    JtaBB.incrementCounter("DoPutOperation", JtaBB.NUM_CREATE);
  }
}
