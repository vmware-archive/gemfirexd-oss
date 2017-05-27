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

package com.pivotal.gemfirexd.internal.engine.management;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import javax.management.MBeanServer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 * If a SQL select on a PR table with primary key lookup is done it goes by
 * GetExecutorMessage? to peer members. As this message bypasses the
 * Region.get() API and directly access InternalDataView?.getLocally() the gets
 * stats are not getting updated.
 * 
 * 
 * Moreover GFE & GemFireXD not updating "putsCompleted", "createsCompleted" in
 * case of a transaction . After committing stats are not getting updated
 * properly for PR regions
 * 
 * Tests to verify the fix for #48947.
 * 
 * 
 * 
 * 
 * @author rishim
 * 
 */
public class PRRegionReadWriteStatsDUnit extends DistributedSQLTestBase {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** The <code>MBeanServer</code> for this application */
  public static MBeanServer mbeanServer = ManagementFactory
      .getPlatformMBeanServer();

  public PRRegionReadWriteStatsDUnit(String name) {
    super(name);
  }

  public void testCreateStatsNonTxn() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      serverInfo.setProperty("server-groups", "MYGROUP");

      startServerVMs(1, 0, null, serverInfo);

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      conn.setAutoCommit(false);
      createTable(conn);
      insertRow(conn);
      validatePutStats(2);
      conn.close();

      for (VM vm : this.serverVMs) {
        validatePutStats(vm, false, 1);
      }

      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  public void testCreateStatsTxn() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      serverInfo.setProperty("server-groups", "MYGROUP");

      startServerVMs(1, 0, null, serverInfo);

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      conn.setAutoCommit(false);
      createTable(conn);
      insertRowWithTxn(conn);
      Thread.sleep(2000);
      validatePutStats(0);

      for (VM vm : this.serverVMs) {
        validatePutStats(vm, true, 1);
      }

      conn.close();
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  public void testUpdateStatsTxn() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      serverInfo.setProperty("server-groups", "MYGROUP");

      startServerVMs(1, 0, null, serverInfo);

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      conn.setAutoCommit(false);
      createTable(conn);
      insertRowWithTxn(conn);
      updateRowWithTxn(conn);
      Thread.sleep(2000);
      validatePutStats(0);

      for (VM vm : this.serverVMs) {
        validatePutStats(vm, true, 2);
      }

      conn.close();
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  public void testGetConvertibleQueryStats() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      serverInfo.setProperty("server-groups", "MYGROUP");

      startServerVMs(1, 0, null, serverInfo);

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      createTable(conn);
      insertRowWithTxn(conn);
      queryRowWithPrimaryKey(conn);
      Thread.sleep(2000);
      validateGetStats(2);
      conn.close();
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  /**
   * test for all basic statement stats gathered during dml execution.
   * 
   * Remove IGNORE when getAll Stats are added to CacheperfStat &
   * PartitionedRegionStats. In that case we need to override executeFunction()
   * in GetAllExecutorMessage and update the stats as in GetExecutorMessage.
   * 
   */
  public void IGNORE_testGetAllConvertibleQueryStats() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      serverInfo.setProperty("server-groups", "MYGROUP");

      startServerVMs(1, 0, null, serverInfo);

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      // conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      createTable(conn);
      insertRowWithTxn(conn);
      queryRowWithPrimaryKeyWithIN(conn);
      Thread.sleep(2000);
      validateGetStats(2);
      conn.close();
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  protected static void validatePutStats(int expectedVal) {

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Region r = cache.getRegion("/TRADE/CUSTOMERS");
    assertNotNull(r);
    assert (r instanceof PartitionedRegion);

    PartitionedRegion parRegion = (PartitionedRegion) r;
    int val = parRegion.getPrStats().getStats().get("createsCompleted")
        .intValue();
    assertEquals(val, expectedVal);

  }

  protected static void validateGetStats(int expectedVal) {

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Region r = cache.getRegion("/TRADE/CUSTOMERS");
    assertNotNull(r);
    assert (r instanceof PartitionedRegion);

    PartitionedRegion parRegion = (PartitionedRegion) r;
    int val = parRegion.getPrStats().getStats().get("getsCompleted").intValue();
    assertEquals(expectedVal, val);
    
    Region r1 = cache.getRegion("/TRADE/PROSPECTIVES");
    
    assert (r1 instanceof DistributedRegion);
    
    LocalRegion lRegion = (LocalRegion)r1;
    val = lRegion.getCachePerfStats().getStats().get("gets").intValue();
    assertEquals(expectedVal, val);

  }

  protected void validatePutStats(final VM vm, final boolean isTxn,
      final int expectedVal) {
    SerializableRunnable validatePutStats = new SerializableRunnable(
        "Validate Put Stats") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Region r = cache.getRegion("/TRADE/CUSTOMERS");
        assertNotNull(r);
        assert (r instanceof PartitionedRegion);

        PartitionedRegion parRegion = (PartitionedRegion) r;
        int actualVal = parRegion.getPrStats().getStats().get("putsCompleted")
            .intValue();

        if (isTxn) {
          assertEquals(expectedVal, actualVal);
        } else {
          assertEquals(0, actualVal);
        }

      }
    };
    vm.invoke(validatePutStats);
  }

  /**
   * Creates a partition Region
   * 
   * @param vm
   */
  protected void validateGetStats(final VM vm, final int expectedVal) {
    SerializableRunnable validatePutStats = new SerializableRunnable(
        "Validate Put Stats") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        Region r = cache.getRegion("/TRADE/CUSTOMERS");
        assertNotNull(r);
        assert (r instanceof PartitionedRegion);

        PartitionedRegion parRegion = (PartitionedRegion) r;
        int actualVal = parRegion.getPrStats().getStats().get("getsCompleted")
            .intValue();
        assertEquals(expectedVal, actualVal);

      }
    };
    vm.invoke(validatePutStats);
  }

  private void createTable(Connection conn) throws Exception {

    final Statement stmt = conn.createStatement();

    final String createSchemaOrder = "create schema trade";
    stmt.execute(createSchemaOrder);

    stmt
        .execute("create table trade.customers (cid int not null, "
            + "cust_name varchar(100), since date, addr varchar(100), tid int, "
            + "primary key (cid))  PARTITION BY primary key SERVER GROUPS (MYGROUP)");
    

    stmt
        .execute("create table trade.prospectives (cid int not null, "
            + "cust_name varchar(100), since date, addr varchar(100), tid int, "
            + "primary key (cid))  REPLICATE SERVER GROUPS (MYGROUP)");

    stmt.close();

  }

  private void insertRow(Connection conn) throws Exception {

    PreparedStatement psInsertCust = conn.prepareStatement("insert into "
        + "trade.customers values (?,?,?,?,?)");

    java.sql.Date since = new java.sql.Date(System.currentTimeMillis());

    for (int i = 0; i < 2; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX" + i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX" + i);
      psInsertCust.setInt(5, i);
      psInsertCust.executeUpdate();
    }

    psInsertCust = conn.prepareStatement("insert into "
        + "trade.prospectives values (?,?,?,?,?)");
    
    for (int i = 0; i < 2; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX" + i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX" + i);
      psInsertCust.setInt(5, i);
      psInsertCust.executeUpdate();
    }
    
    psInsertCust.close();

  }

  private void insertRowWithTxn(Connection conn) throws Exception {

    PreparedStatement psInsertCust = conn.prepareStatement("insert into "
        + "trade.customers values (?,?,?,?,?)");

    java.sql.Date since = new java.sql.Date(System.currentTimeMillis());
    // Insert 0-10.
    for (int i = 0; i < 2; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX" + i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX" + i);
      psInsertCust.setInt(5, i);
      psInsertCust.executeUpdate();
    }
    conn.commit();

    psInsertCust.close();

  }

  private void updateRowWithTxn(Connection conn) throws Exception {

    PreparedStatement psUpdateCust = conn
        .prepareStatement("update trade.customers set addr=? where cid=?");

    for (int i = 0; i < 2; i++) {
      psUpdateCust.setString(1, "YYYY" + i);
      psUpdateCust.setInt(2, i);
      psUpdateCust.executeUpdate();
    }
    conn.commit();

    psUpdateCust.close();

  }

  private void queryRowWithPrimaryKey(Connection conn) throws Exception {

    PreparedStatement psSelectCust = conn
        .prepareStatement("select addr from trade.customers where cid=?");
    for (int i = 0; i < 2; i++) {
      psSelectCust.setInt(1, i);
      ResultSet rs = psSelectCust.executeQuery();
      rs.next();
    }
    
    psSelectCust = conn
        .prepareStatement("select addr from trade.prospectives where cid=?");
    for (int i = 0; i < 2; i++) {
      psSelectCust.setInt(1, i);
      ResultSet rs = psSelectCust.executeQuery();
      rs.next();
    }

    conn.commit();
    psSelectCust.close();

  }

  private void queryRowWithPrimaryKeyWithIN(Connection conn) throws Exception {

    PreparedStatement psSelectCust = conn
        .prepareStatement("select addr from trade.customers where cid in(?,?)");
    psSelectCust.setInt(1, 1);
    psSelectCust.setInt(2, 2);
    ResultSet rs = psSelectCust.executeQuery();
    rs.next();

    psSelectCust.close();

  }
}
