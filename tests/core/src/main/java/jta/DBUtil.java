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
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.naming.Context;
import javax.sql.DataSource;

import com.gemstone.gemfire.cache.Cache;
import hydra.CacheHelper;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.TestConfig;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;

/**
 * Utilies to provide access to derby and oracle databases.
 */
public class DBUtil {

  public static DBUtil dbUtilInstance;

  /**
   * the oracle connection pool (for GemFire tests)
   */
  private OracleDataSource cachedDS;

  // Constructor (to setup oracle connection pool)
  DBUtil() {
    this.cachedDS = createConnectionPool();
  }

  /**
   * Creates a database connection pool object using JDBC 2.0.
   */
  private OracleDataSource createConnectionPool() {
    try {
      // Create a OracleConnectionPoolDataSource instance.
      OracleDataSource ds = new OracleDataSource();
      ds.setConnectionCacheName("oraCache");
      ds.setConnectionCachingEnabled(true);
      ds.setURL(TestConfig.tab().stringAt(JtaPrms.jdbcUrl));
      ds.setUser(TestConfig.tab().stringAt(JtaPrms.rdbUser));
      ds.setPassword(TestConfig.tab().stringAt(JtaPrms.rdbPassword));

      Properties cacheProps = new Properties();
      cacheProps.setProperty("MinLimit",
          String.valueOf(TestConfig.tab().intAt(JtaPrms.poolMinLimit)));
      ds.setConnectionCacheProperties(cacheProps);
      return ds;
    } catch (Exception ex) {
      Log.getLogWriter().info("Unable to create connection pool: " + ex, ex);
      throw new HydraRuntimeException("Problem creating Oracle connection pool", ex);
    }
  }

  public static DBUtil getDBUtil() {
    if (dbUtilInstance == null) {
      synchronized (DBUtil.class) {
        if (dbUtilInstance == null)
          dbUtilInstance = new DBUtil();
      }
    }
    return dbUtilInstance;
  }

  /**
   * get a DataSource connection (for query)
   */
  public static Connection getSimpleDSConnection() throws Exception {
    DataSource ds;
    Connection conn;
    if (JtaPrms.useGemFireTransactionManager()) {
      // GemFire Transaction Manager
      DBUtil dbUtil = getDBUtil();
      conn = dbUtil.cachedDS.getConnection();
    } else {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      ds = (DataSource)ctx.lookup("java:/SimpleDataSource");
      conn = ds.getConnection();
    }
    return conn;
  }

  /**
   * get a DataSource connection (for updates)
   */
  public static Connection getXADSConnection() throws Exception {
    Connection conn;
    if (JtaPrms.useGemFireTransactionManager()) {
      // GemFire Transaction Manager, get Oracle Pooled Data Source
      DBUtil dbUtil = getDBUtil();
      conn = dbUtil.cachedDS.getConnection();
    } else {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource)ctx.lookup("java:/XAHydraPooledDataSource");
      conn = ds.getConnection();
    }
    conn.setAutoCommit(false);
    //conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    return conn;
  }

  public static void displayData(String tableName) throws Exception {
    Log.getLogWriter().info("display rows from " + tableName);
    Log.getLogWriter().info("DB state is " + getData(tableName));
  }

  public static Map getData(String tableName) throws Exception {
    Connection conn = getSimpleDSConnection();
    String sql = "SELECT * FROM " + tableName;
    ResultSet rs = executeResultSet(sql, conn);
    Map map = new TreeMap();
    while (rs.next()) {
      String key = rs.getString(1);
      String value = rs.getString(2);
      map.put(key, value);
    }
    if (rs != null) rs.close();
    if (conn != null) conn.close();
    return map;
  }

  public static String getDBValue(String tableName, String key) throws Exception {
    Connection conn = getXADSConnection();
    String sql = "SELECT name FROM " + tableName + " WHERE id = '" + key + "'";
    String value = "-1";
    try {
      Log.getLogWriter().info("Executing " + sql + " on connection " + conn.toString());
      ResultSet rs = executeResultSet(sql, conn);
      rs.next();
      value = rs.getString(1);
      if (value == null || value.equals("")) {
        throw new SQLException("Null ResultSet in getDBValue ");
      }
      if (rs != null) rs.close();
      if (conn != null) conn.close();
    } catch (Exception e) {
      conn.close();
      Log.getLogWriter().info("DBUtil.getDBValue() caught exception " + e);
      throw e;
    }
    return value;
  }

  static void dropTable(String tableName) throws Exception {
    Connection conn = getSimpleDSConnection();
    Statement stm = conn.createStatement();

    String dropTableSql = "DROP TABLE " + tableName;
    Log.getLogWriter().info("Executing " + dropTableSql);
    stm.execute(dropTableSql);
    if (stm != null) stm.close();
  }

  static void createTable(String tableName) throws Exception {
    Connection conn = getSimpleDSConnection();
    Statement stm = conn.createStatement();

    String sql = "CREATE TABLE " + tableName
        + " (id varchar(5) NOT NULL, name varchar(50), PRIMARY KEY(id))";

    Log.getLogWriter().info("create table statement = " + sql);
    stm = conn.createStatement();
    stm.executeUpdate(sql);
    String key = null;
    for (int i = 1; i <= 4; i++) {
      key = "key" + i;
      sql = "INSERT INTO " + tableName + " VALUES ('" + key + "','value0')";
      Log.getLogWriter().info("insert row = " + sql);
      stm.addBatch(sql);
    }
    stm.executeBatch();
    conn.commit();
    if (stm != null) stm.close();
    if (conn != null) conn.close();
    Log.getLogWriter().info("Created table " + tableName + " in database");
    displayData(tableName);
  }

  public static ResultSet executeResultSet(String sql, Connection conn) throws Exception {
    ResultSet rs = null;
    Statement stm = conn.createStatement();
    rs = stm.executeQuery(sql);
    return rs;
  }

  public static int executeUpdate(String sql, Connection conn) throws Exception {
    int count;
    Statement stm = conn.createStatement();
    count = stm.executeUpdate(sql);
    if (stm != null) stm.close();
    return count;
  }

  /*------------------------------------------------------
   * For GemFire Transaction using the Oracle database
   *----------------------------------------------------*/
  public static DataSource getOraclePooledDataSource() throws SQLException {
    // Create an OracleDataSource instance explicitly
    OracleConnectionPoolDataSource ds = new OracleConnectionPoolDataSource();
    ds.setURL(TestConfig.tab().stringAt(JtaPrms.jdbcUrl));
    ds.setUser(TestConfig.tab().stringAt(JtaPrms.rdbUser));
    ds.setPassword(TestConfig.tab().stringAt(JtaPrms.rdbPassword));
    return ds;
  }
}
