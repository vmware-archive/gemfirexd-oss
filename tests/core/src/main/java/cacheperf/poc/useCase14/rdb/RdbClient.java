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

package cacheperf.poc.useCase14.rdb;

import cacheperf.*;
import hydra.HydraRuntimeException;
import java.sql.*;
import objects.ObjectHelper;

/**
 * Client for relational data feed.
 */

public class RdbClient extends CachePerfClient {

  private String tableName, driverName, dbUrl, userName, password;

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   *  TASK to delete a relational table.
   */
  public static void deleteRelationalDataTask() {
    RdbClient c = new RdbClient();
    c.initialize();
    c.initRdbConfiguration();
    c.deleteTable();
  }
  private void deleteTable() {
    loadDriver(this.driverName);
    Connection conn = getConnection(this.dbUrl, this.userName, this.password);
    Statement stmt = createStatement(conn);
    try {
      stmt.executeUpdate("DROP TABLE " + this.tableName);
      log().info("Deleted table " + this.tableName);
    } catch (SQLException e) {
      log().info("Table " + this.tableName + " already deleted");
    }
  }

  /**
   *  TASK to put relational objects into a database.
   */
  public static void createRelationalDataTask() {
    RdbClient c = new RdbClient();
    c.initialize(CREATES);
    c.initRdbConfiguration();
    c.deleteTable();
    c.createAndPopulateTable();
  }
  private void createAndPopulateTable() {
    loadDriver(this.driverName);
    Connection conn = getConnection(this.dbUrl, this.userName, this.password);
    Statement stmt = createStatement(conn);
    try {
      stmt.executeUpdate(
        "CREATE TABLE " + this.tableName + " " +
        "(SYMBOL VARCHAR2(50) NOT NULL , ORGANIZATION VARCHAR2(50) NOT NULL )"
      );
      log().info("Created table " + this.tableName);
    } catch (SQLException e) {
      throw new HydraRuntimeException("Table " + this.tableName + " already created");
    }
    log().info("Populating table " + this.tableName + " with " + this.maxKeys + " entries...");
    for (int i = 0; i < this.maxKeys; i++) {
      long start = this.statistics.startCreate();
      try {
        stmt.executeUpdate("Insert into " + this.tableName + " values ('SYM" + i
            + "', 'ORGANIZATION" + i + "')");
      } catch (SQLException e) {
        throw new HydraRuntimeException("Problem executing update " + i, e);
      }
      this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
    }
    log().info("Populated table " + this.tableName);
  }

  /**
   *  TASK to load relational objects into a region.
   */
  public static void loadRelationalDataTask() {
    RdbClient c = new RdbClient();
    c.initialize(PUTS);
    c.initRdbConfiguration();
    c.loadRelationalData();
  }
  private void loadRelationalData() {
    loadDriver(this.driverName);
    Connection conn = getConnection(this.dbUrl, this.userName, this.password);
    Statement stmt = createStatement(conn);
    try {
      log().info("Populating region from database...");
      int entries = 0;
      ResultSet resultSet = stmt.executeQuery("select * from " + this.tableName);
      while (resultSet.next()) {
        String key = resultSet.getString(1);
        String value = resultSet.getString(2);
        long start = this.statistics.startPut();
        this.cache.put(key, value);
        this.statistics.endPut(start, this.isMainWorkload, this.histogram);
        ++entries;
      }
      log().info("Populated region with " + entries + " entries from database");
    } catch (SQLException e) {
      throw new HydraRuntimeException("Problem reading data", e);
    }
  }

  private void initRdbConfiguration() {
    this.tableName = RdbPrms.getTableName();
    this.driverName = RdbPrms.getDriverName();
    this.dbUrl = RdbPrms.getDbUrl();
    this.userName = RdbPrms.getUserName();
    this.password = RdbPrms.getPassword();
    log().info("Configured RDB with driverName=" + this.driverName
         + " dbUrl=" + this.dbUrl
         + " userName=" + this.userName + " password=" + this.password);
  }
  private void loadDriver(String driverName) {
    try {
      Driver driver = (Driver)Class.forName(driverName).newInstance();
      log().info("Loaded driver: " + driver);
    } catch (ClassNotFoundException e) {
      throw new HydraRuntimeException("Problem instantiating driver", e);
    } catch (IllegalAccessException e) {
      throw new HydraRuntimeException("Problem instantiating driver", e);
    } catch (InstantiationException e) {
      throw new HydraRuntimeException("Problem instantiating driver", e);
    }
  }
  private Connection getConnection(String dbUrl,
                                   String userName, String password) {
    try {
      Connection conn = DriverManager.getConnection(dbUrl, userName, password);
      log().info("Got connection: " + conn);
      return conn;
    } catch (SQLException e) {
      throw new HydraRuntimeException("Problem obtaining connection", e);
    }
  }
  private Statement createStatement(Connection conn) {
    try {
      return conn.createStatement();
    } catch (SQLException e) {
      throw new HydraRuntimeException("Problem creating statement", e);
    }
  }

  /**
   * TASK to register interest in keys.
   */
  public static void registerInterestTask() {
    RdbClient c = new RdbClient();
    c.initialize();
    c.registerInterest();
  }
  private void registerInterest() {
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      int n = this.rng.nextInt(1, 100);
      register(key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }
  protected void register( int i ) {
    if (this.cache instanceof distcache.gemfire.GemFireCacheTestImpl) {
      Object key = ObjectHelper.createName( this.keyType, i );
      distcache.gemfire.GemFireCacheTestImpl impl =
              (distcache.gemfire.GemFireCacheTestImpl)this.cache;
      impl.registerInterest("SYM" + key, this.registerDurableInterest);
    }
  }

  /**
   * TASK to register interest in all keys.
   */
  public static void registerInterestInAllKeysTask() {
    RdbClient c = new RdbClient();
    c.initialize();
    c.registerInterestInAllKeys();
  }
  private void registerInterestInAllKeys() {
    if (this.cache instanceof distcache.gemfire.GemFireCacheTestImpl) {
      distcache.gemfire.GemFireCacheTestImpl impl =
              (distcache.gemfire.GemFireCacheTestImpl)this.cache;
      impl.registerInterest("ALL_KEYS", this.registerDurableInterest);
    }
  }
}
