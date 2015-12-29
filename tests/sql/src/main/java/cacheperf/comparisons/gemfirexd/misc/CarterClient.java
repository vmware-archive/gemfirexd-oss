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
package cacheperf.comparisons.gemfirexd.misc;

import cacheperf.comparisons.gemfirexd.QueryPerfClient;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import java.io.*;
import java.sql.*;
import objects.query.QueryPrms;

public class CarterClient extends QueryPerfClient
{
  private static String PRODUCT_INSERT = "insert into product values (?, ?, ?, ?)";
  private static String STORE_INSERT = "insert into store values (?, ?, ?)";
  private static String SALE_INSERT = "insert into sale (timestamp, storeid, productid, units, price) values (?, ?, ?, ?, ?)";
  private static String PRODUCT_UPDATE = "update product set inventory = inventory - 1 where id = ?";

  private static final boolean logQueries = QueryPrms.logQueries();
  private static final boolean logQueryResults = QueryPrms.logQueryResults();
  private static final int numProducts = CarterPrms.getNumProducts();
  private static final int numStores = CarterPrms.getNumStores();
  private static final int numSalesRecords = CarterPrms.getNumSalesRecords();
  private static float[] Prices = new float[CarterPrms.getNumProducts()];
  private static final String CONFLICT_STATE = "X0Z02";

  private static HydraThreadLocal localcarterstats = new HydraThreadLocal();

  protected CarterStats carterstats; // statistics

//------------------------------------------------------------------------------
// statistics task
//------------------------------------------------------------------------------

  /**
   *  TASK to register the performance statistics object.
   */
  public static void openStatisticsTask() {
    CarterClient c = new CarterClient();
    c.openStatistics();
  }
  private void openStatistics() {
    this.carterstats = getCarterStats();
    if (this.carterstats == null) {
      this.carterstats = CarterStats.getInstance();
    }
    setCarterStats(this.carterstats);
  }
  
  /** 
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    CarterClient c = new CarterClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.carterstats != null) {
      this.carterstats.close();
    }
  }

//------------------------------------------------------------------------------
// DDL
//------------------------------------------------------------------------------

  public static void dropTablesTask()
  throws SQLException {
    CarterClient c = new CarterClient();
    c.initialize();
    if (c.ttgid == 0) {
      c.dropTables();
    }
  }
  private void dropTables()
  throws SQLException {
    dropTable("sale");
    dropTable("store");
    dropTable("product");
  }
  private void dropTable(String table)
  throws SQLException {
    String ddl = "drop table if exists " + table;
    executeDDL(ddl);
  }

  public static void createTablesTask()
  throws FileNotFoundException, IOException, SQLException {
    CarterClient c = new CarterClient();
    c.initialize();
    if (c.ttgid == 0) {
      c.createTables();
    }
  }
  private void createTables()
  throws SQLException {
    createTable("sale (timestamp timestamp, storeid int, productid int, units int, price float) partition by column (productid)");
    createTable("store (id int, city varchar(80), state char(2))");
    createTable("product (id int, name varchar(80), msrp float, inventory int)");
  }
  private void createTable(String table)
  throws SQLException {
    String ddl = "create table " + table;
    executeDDL(ddl);
  }

  private void executeDDL(String ddl) throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing DDL: " + ddl);
    }
    Statement stmt = this.connection.createStatement();
    stmt.executeUpdate(ddl);
    stmt.close();
    if (logQueries) {
      Log.getLogWriter().info("Executed DDL: " + ddl);
    }
  }

//------------------------------------------------------------------------------
// populate
//------------------------------------------------------------------------------

  public static void populateProductsTask()
  throws SQLException {
    CarterClient c = new CarterClient();
    c.initialize(-1);
    if (c.ttgid == 0) {
      c.populateProducts();
    }
  }
  private void populateProducts()
  throws SQLException {
    PreparedStatement stmt = this.connection.prepareStatement(PRODUCT_INSERT);
    for (int i = 1; i <= numProducts; i++) {
      stmt.setInt(1, i);                // id int
      stmt.setString(2, "frazzled");    // name varchar(80)
      float basePrice = this.rng.nextFloat();
      Prices[i-1] = basePrice;
      stmt.setFloat(3, basePrice);      // msrp float
      stmt.setInt(4, 100000);           // inventory int
      stmt.addBatch();
    }
    stmt.executeBatch();
    stmt.close();
    this.connection.commit();
  }

  public static void populateStoresTask()
  throws SQLException {
    CarterClient c = new CarterClient();
    c.initialize(-1);
    if (c.ttgid == 0) {
      c.populateStores();
    }
  }
  private void populateStores()
  throws SQLException {
    PreparedStatement stmt = this.connection.prepareStatement(STORE_INSERT);
    for (int i = 1; i <= numStores; i++) {
      stmt.setInt(1, i);              // id int
      stmt.setString(2, "lockhart");  // city varchar(80)
      stmt.setString(3, "tx");        // state char(2)
      stmt.addBatch();
    }
    stmt.executeBatch();
    stmt.close();
    this.connection.commit();
  }

  public static void populateSaleRecordsTask() throws SQLException {
    CarterClient c = new CarterClient();
    c.initialize(-1);
    c.populateSaleRecords();
  }
  private void populateSaleRecords() throws SQLException {
    PreparedStatement saleStmt = this.connection.prepareStatement(SALE_INSERT);
    PreparedStatement inventoryStmt =
                      this.connection.prepareStatement(PRODUCT_UPDATE);
    for (int i = 1; i <= numSalesRecords; i++) {
      int retries = 0;
      boolean committed = false;
      int productNumber = this.rng.nextInt(1, numProducts);
      int storeNumber = this.rng.nextInt(1, numStores);
      Timestamp currentDate = new Timestamp(System.currentTimeMillis());
      while (!committed && retries++ < 50) {
        long start = this.carterstats.startSale();
        saleStmt.setTimestamp(1, currentDate);         // timestamp timestamp
        saleStmt.setInt(2, storeNumber);               // storeid int
        saleStmt.setInt(3, productNumber);             // productid int
        saleStmt.setInt(4, this.rng.nextInt(1, 10));   // units int
        saleStmt.setFloat(5, Prices[productNumber-1]); // price float

        inventoryStmt.setInt(1, productNumber);        // inventory int
 
        try {
          saleStmt.execute();
          inventoryStmt.execute();
          this.connection.commit();
          committed = true;
        } catch (SQLException e) {
          checkConflict(e);
        }
        this.carterstats.endSale(start, committed);
      }
    }
  }

  private void checkConflict(SQLException e) throws SQLException {
    if (e instanceof BatchUpdateException) {
      SQLException ex = e;
      while (ex != null) {
        if (!ex.getSQLState().equals(CONFLICT_STATE)) {
          throw e;
        }
        ex = ex.getNextException();
      }
    } else if (!e.getSQLState().equals(CONFLICT_STATE)) {
      throw e;
    }
  }

//------------------------------------------------------------------------------
// hydra thread locals
//------------------------------------------------------------------------------

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.carterstats = getCarterStats();
  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setCarterStats(this.carterstats);
  }

  /**
   * Gets the per-thread CarterStats wrapper instance.
   */
  protected CarterStats getCarterStats() {
    CarterStats cstats = (CarterStats)localcarterstats.get();
    return cstats;
  }

  /**
   * Sets the per-thread CarterStats wrapper instance.
   */
  protected void setCarterStats(CarterStats cstats) {
    localcarterstats.set(cstats);
  }
}
