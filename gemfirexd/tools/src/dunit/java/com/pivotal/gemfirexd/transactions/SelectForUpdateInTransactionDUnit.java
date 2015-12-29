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
package com.pivotal.gemfirexd.transactions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.locks.LockMode;
import com.gemstone.gemfire.internal.cache.locks.LockingPolicy;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

@SuppressWarnings("serial")
public class SelectForUpdateInTransactionDUnit extends DistributedSQLTestBase {

  public SelectForUpdateInTransactionDUnit(String name) {
    super(name);
  }

  private final String goldenTextFile = TestUtil.getResourcesDir()
      + "/lib/checkQuery.xml";

  public static void executeSelectForUpdateQuery(final String sql,
      final boolean exception, final int isolationLevel) {
    try {
      final java.sql.Connection conn = TestUtil.getConnection();
      Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
      if (isolationLevel != Connection.TRANSACTION_NONE) {
        conn.setTransactionIsolation(isolationLevel);
      }
      stmt.execute(sql);
      ResultSet rs = stmt.getResultSet();
      while(rs.next()) {
      }
      if (exception) {
        stmt.close();
        conn.commit();
        fail("The execution should have thrown exception");
      }
      stmt.close();
      conn.commit();
    } catch (Exception e) {
      if (!exception) {
        fail("should have got exception");
      }
    }
  }

  static class SelectForUpdateObserver extends GemFireXDQueryObserverAdapter {

    private HashMap<Object, Object> keysLocked;

    private HashMap<Object, Object> keysInfoAttached;

    @Override
    public void lockingRowForTX(TXStateProxy tx, GemFireContainer container,
        RegionEntry entry, boolean writeLock) {
      if (writeLock) {
        if (this.keysLocked == null) {
          this.keysLocked = new HashMap<Object, Object>();
        }
        this.keysLocked.put(entry.getKeyCopy(), null);
      }
    }

    @Override
    public void attachingKeyInfoForUpdate(GemFireContainer container,
        RegionEntry entry) {
      if (this.keysInfoAttached == null) {
        this.keysInfoAttached = new HashMap<Object, Object>();
      }
      this.keysInfoAttached.put(entry.getKeyCopy(), null);
    }

    public boolean checkIfTheseKeysLocked(Object[] keys) {
      for (int i = 0; i < keys.length; i++) {
        if (!this.keysLocked.containsKey(keys[i])) {
          return false;
        }
      }
      return true;
    }

    public boolean checkIfTheseKeysSent(Object[] keys) {
      for (int i = 0; i < keys.length; i++) {
        if (!this.keysInfoAttached.containsKey(keys[i])) {
          return false;
        }
      }
      return true;
    }
  }

  public static void installObserver() {
    GemFireXDQueryObserverHolder.setInstance(new SelectForUpdateObserver());
  }

  public static void checkIfTheseKeysLocked(Object[] keys) {
    SelectForUpdateObserver observer = (SelectForUpdateObserver)GemFireXDQueryObserverHolder
        .getInstance();
    if (!observer.checkIfTheseKeysLocked(keys)) {
      fail("Keys not locked as expected");
    }
  }

  public static void checkIfTheseKeysSent(Object[] keys) {
    SelectForUpdateObserver observer = (SelectForUpdateObserver)GemFireXDQueryObserverHolder
        .getInstance();
    if (!observer.checkIfTheseKeysSent(keys)) {
      fail("Keys not sent as expected");
    }
  }

  public void testSelectForUpdate_PR_key_not_in_projection_and_whereClause()
      throws Exception {
    startVMs(2, 1);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname))";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";
    
    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT workdept, bonus "
      + "FROM EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    
    sql = "select * from employee where lastname = 'kumar'";
    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(1, cnt);
    
    conn.commit();
  }

  public void testSelectForUpdate_PR_key_not_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname))";

    clientSQLExecute(1, jdbcSQL);

    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "installObserver");

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while (rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);

    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1 }, sql, goldenTextFile,
        "equal_bonus");
  }

  public void testSelectForUpdate_PR_key_in_projection() throws Exception {
    startVMs(2, 2);
    runSelectForUpdate_PR_key_in_projection(TestUtil.getConnection());
  }

  public void testSelectForUpdate_directDelete() throws Exception {
    startVMs(2, 1);
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    conn.setAutoCommit(false);
    
    Statement stmtForTableAndInsert = conn.createStatement();
    stmtForTableAndInsert.execute("create table Employee"
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus decimal(10,4), "
        + "primary key (firstname, lastname))");
    stmtForTableAndInsert
        .execute("insert into employee values('neeraj', 'kumar', 'rnd', 0.0), "
            + "('asif', 'shahid', 'rnd', 1.0), "
            + "('dada', 'ji', 'rnd', 2.0), ('sum', 'wale', 'rnd', 3.0)");
    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    ResultSet uprs = stmt.executeQuery("SELECT workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS");

    while (uprs.next()) {
      uprs.deleteRow();
    }
    
    SQLWarning w = uprs.getWarnings();
    assertTrue("0A000".equals(w.getSQLState()));
    uprs.close();
    Statement stmt2 = conn.createStatement();
    ResultSet rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    rs = stmt2.executeQuery("select * from employee");
    assertFalse(rs.next());

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    
    stmt.execute("insert into employee values('asif', 'shahid', 'rnd', 1.0), ('dada', 'ji', 'rnd', 2.0), ('sum', 'wale', 'rnd', 3.0)");
    
    uprs = stmt.executeQuery("SELECT * "
        + "FROM EMPLOYEE FOR UPDATE of BONUS");

    while (uprs.next()) {
      if (uprs.getString(1).equalsIgnoreCase("asif")) {
        uprs.deleteRow();
      }
    }

    w = uprs.getWarnings();
    assertNull(w);
    uprs.close();
    conn.commit();
    
    stmt2 = conn.createStatement();
    rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    assertTrue(rs.next());
    assertTrue((rs.getString(1).equalsIgnoreCase("sum") || rs.getString(1)
        .equalsIgnoreCase("dada")));
    
    assertTrue(rs.next());
    
    assertTrue((rs.getString(1).equalsIgnoreCase("sum") || rs.getString(1)
        .equalsIgnoreCase("dada")));
    
    assertFalse(rs.next());
  }
  
  public void testSelectForUpdate_directDeleteNoPK() throws Exception {
    startVMs(2, 1);
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    conn.setAutoCommit(false);
    Statement stmtForTableAndInsert = conn.createStatement();
    stmtForTableAndInsert.execute("create table Employee"
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus decimal(10,4))");
    stmtForTableAndInsert
        .execute("insert into employee values('neeraj', 'kumar', 'rnd', 0.0), "
            + "('asif', 'shahid', 'rnd', 1.0), "
            + "('dada', 'ji', 'rnd', 2.0), ('sum', 'wale', 'rnd', 3.0)");
    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    ResultSet uprs = stmt.executeQuery("SELECT workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS");

    while (uprs.next()) {
      uprs.deleteRow();
    }
    
    SQLWarning w = uprs.getWarnings();
    assertTrue("0A000".equals(w.getSQLState()));
    uprs.close();
    Statement stmt2 = conn.createStatement();
    ResultSet rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    rs = stmt2.executeQuery("select * from employee");
    assertFalse(rs.next());
    
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    
    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    
    stmt.execute("insert into employee values('asif', 'shahid', 'rnd', 1.0), ('dada', 'ji', 'rnd', 2.0), ('sum', 'wale', 'rnd', 3.0)");
    
    uprs = stmt.executeQuery("SELECT * "
        + "FROM EMPLOYEE FOR UPDATE of BONUS");

    while (uprs.next()) {
      if (uprs.getString(1).equalsIgnoreCase("asif")) {
        uprs.deleteRow();
      }
    }

    w = uprs.getWarnings();
    assertNull(w);
    uprs.close();
    conn.commit();
    
    stmt2 = conn.createStatement();
    rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    assertTrue(rs.next());
    assertTrue((rs.getString(1).equalsIgnoreCase("sum") || rs.getString(1)
        .equalsIgnoreCase("dada")));
    
    assertTrue(rs.next());
    
    assertTrue((rs.getString(1).equalsIgnoreCase("sum") || rs.getString(1)
        .equalsIgnoreCase("dada")));
    
    assertFalse(rs.next());
  }
  
  public void DISABLED_43188_testSelectForUpdate_PR_key_in_projection_client()
      throws Exception {
    startVMs(2, 2);
    final int netPort = startNetworkServer(2, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort, null, null);
    runSelectForUpdate_PR_key_in_projection(conn);
  }

  protected void runSelectForUpdate_PR_key_in_projection(final Connection conn)
      throws Exception {
    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        //+ "workdept varchar(50), bonus int not null, primary key (firstname, lastname))";
    + "workdept varchar(50), bonus int not null, primary key (firstname))";
    
    clientSQLExecute(1, jdbcSQL);

    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_PR_composite_key_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname))";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_PR_composite_key_partially_in_projection()
      throws Exception {
    startVMs(2, 2);
    runSelectForUpdate_PR_composite_key_partially_in_projection(TestUtil
        .getConnection());
  }

  public void DISABLED_43188_testSelectForUpdate_PR_composite_key_partially_in_projection_client()
      throws Exception {
    startVMs(2, 2);
    final int netPort = startNetworkServer(2, null, null);
    final Connection conn = TestUtil.getNetConnection(netPort, null, null);
    runSelectForUpdate_PR_composite_key_partially_in_projection(conn);
  }

  protected void runSelectForUpdate_PR_composite_key_partially_in_projection(
      final Connection conn) throws Exception {
    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname))";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    
    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_PR_composite_key_not_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname))";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }
  
  public void testSelectForUpdate_PR_no_primary_key() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null)";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();
    conn.setAutoCommit(false);

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());

    stmt.execute(sql);

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }
  //////////////////////////// Below this same test with replicated ///////////////////

  public static void verifyUpdateConflict(final String sql, Boolean exception,
      int isolationLevel) throws SQLException {
    try {
      final java.sql.Connection conn = TestUtil.getConnection();
      conn.setTransactionIsolation(isolationLevel);
      conn.setAutoCommit(false);
      
      Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

      int updateCnt = stmt.executeUpdate(sql);

      conn.commit();
      conn.close();

      if (exception.booleanValue()) {
        fail("The execution should have thrown exception instead it updated "
            + updateCnt + " rows");
      }
    } catch (Exception e) {
      SQLException sqle = (SQLException)e;
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
  }

  public static void verifyLocalConflict(final String regionPath,
      final String key, Boolean exception) throws SQLException,
      StandardException {
    LocalRegion reg = (LocalRegion)Misc.getRegion(regionPath, true, false);
    Object sqlkey = GemFireXDUtils.convertIntoGemfireRegionKey(
        new SQLVarchar(key), (GemFireContainer)reg.getUserAttribute(), false);
    RegionEntry entry = reg.basicGetEntryForLock(reg, sqlkey);
    boolean locked = entry.attemptLock(LockMode.EX_SH, 0,
        LockingPolicy.FAIL_FAST_TX, 0, Thread.currentThread(), reg);
    if (exception) {
      assertFalse(locked);
    }
    else {
      assertTrue(locked);
    }
  }

  public void testSelectForUpdate_RR_key_not_in_projection_and_whereClause() throws Exception {
    startVMs(2, 2);

    clientSQLExecute(1, "create schema myapp");
    
    String jdbcSQL = "create table myapp.Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname)) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into myapp.employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT workdept, bonus "
        + "FROM myapp.EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update myapp.employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyLocalConflict",
        new Object[] { "/MYAPP/EMPLOYEE", "neeraj", Boolean.TRUE });

    this.serverVMs.get(1).invoke(getClass(), "verifyLocalConflict",
        new Object[] { "/MYAPP/EMPLOYEE", "neeraj", Boolean.TRUE });

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update myapp.employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.FALSE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.FALSE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    
    sql = "select * from myapp.employee where lastname = 'kumar'";
    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(1, cnt);
    
    conn.commit();
  }
  
  public void testSelectForUpdate_RR_key_not_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname)) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "installObserver");

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_RR_key_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        //+ "workdept varchar(50), bonus int not null, primary key (firstname, lastname))";
    + "workdept varchar(50), bonus int not null, primary key (firstname)) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_RR_composite_key_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname)) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_RR_composite_key_partially_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname)) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdate_RR_composite_key_not_in_projection() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null, primary key (firstname, lastname)) replicate";

    clientSQLExecute(1, jdbcSQL);

    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }
  
  public void testSelectForUpdate_RR_no_primary_key() throws Exception {
    startVMs(2, 2);

    String jdbcSQL = "create table Employee "
        + "(firstname varchar(50) not null, lastname varchar(50) not null, "
        + "workdept varchar(50), bonus int not null) replicate";
    
    clientSQLExecute(1, jdbcSQL);
    
    jdbcSQL = "insert into employee values('neeraj', 'kumar', 'rnd', 0), "
        + "('asif', 'shahid', 'rnd', 0), "
        + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)";

    clientSQLExecute(1, jdbcSQL);

    String sql = "SELECT firstname, workdept, bonus "
        + "FROM EMPLOYEE FOR UPDATE of BONUS";

    final java.sql.Connection conn = TestUtil.getConnection();

    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);

    stmt.execute(sql);

    String conflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'kumar'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { conflictSql, Boolean.TRUE, getIsolationLevel() });

    String noConflictSql = "update employee set workdept = 'xxx' "
        + "where lastname = 'wale'";

    this.serverVMs.get(0).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(1).invoke(getClass(), "verifyUpdateConflict",
        new Object[] { noConflictSql, Boolean.TRUE, getIsolationLevel() });

    this.serverVMs.get(0).invoke(getClass(), "executeSelectForUpdateQuery",
        new Object[] { sql, Boolean.TRUE, getIsolationLevel() });

    ResultSet uprs = stmt.getResultSet();

    String theDept = "rnd";
    while (uprs.next()) {
      String workDept = uprs.getString("WORKDEPT");
      if (workDept.equals(theDept)) {
        uprs.updateInt("bonus", 10);
        uprs.updateRow();
      }
    }
    conn.commit();
    sql = "select * from employee";

    stmt.execute(sql);
    ResultSet rs = stmt.getResultSet();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      int bonus = rs.getInt(4);
      assertEquals(10, bonus);
    }
    assertEquals(4, cnt);
    
    conn.commit();
    sqlExecuteVerify(new int[] { 1 }, new int[] {1}, sql, goldenTextFile, "equal_bonus");
  }

  public void testSelectForUpdateWarnings() throws Exception {
    startVMs(2, 2);
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    conn.setAutoCommit(false);
    Statement stmtForTableAndInsert = conn.createStatement();
    stmtForTableAndInsert
        .execute("create table Employee"
            + "(firstname varchar(50) not null, lastname varchar(50) not null, "
            + "workdept varchar(50), bonus int not null, primary key (firstname, lastname))");
    stmtForTableAndInsert
        .execute("insert into employee values('neeraj', 'kumar', 'rnd', 0), "
            + "('asif', 'shahid', 'rnd', 0), "
            + "('dada', 'ji', 'rnd', 0), ('sum', 'wale', 'rnd', 0)");
    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    ResultSet uprs = stmt.executeQuery("SELECT workdept, bonus "
        + "FROM EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS");
    while (uprs.next()) {
      uprs.getString("WORKDEPT");
      int bonus = uprs.getInt("BONUS");
      uprs.updateInt("BONUS", bonus+250);
      uprs.updateRow();
    }

    SQLWarning w = uprs.getWarnings();
    assertTrue("0A000".equals(w.getSQLState()));
    uprs.close();
    Statement stmt2 = conn.createStatement();
    ResultSet rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    conn.commit();
    stmt.executeUpdate("update Employee set bonus = 1000 where lastname <> 'kumar'");

    w = stmt.getWarnings();
    assertNull(rs.getWarnings());
    
    stmt2.executeUpdate("update Employee set bonus = bonus + 1000 where lastname <> 'kumar'");
    w = stmt2.getWarnings();
    assertNull(w);
    
    // Prepared Statement
    
    PreparedStatement ps = conn.prepareStatement("SELECT workdept, bonus "
        + "FROM EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS", ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE);
    
    ps.execute();
    
    ResultSet prs = ps.getResultSet();
    
    while(prs.next()) {
      prs.getString("WORKDEPT");
      int bonus = prs.getInt("BONUS");
      prs.updateInt("BONUS", bonus + 1250);
      prs.updateRow();
    }
    
    w = prs.getWarnings();
    assertTrue("0A000".equals(w.getSQLState()));
    
    // The above thing repeated but with transaction set so no warnings
    // expected
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    
    conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    uprs = stmt.executeQuery("SELECT workdept, bonus "
        + "FROM EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS");
    while (uprs.next()) {
      uprs.getString("WORKDEPT");
      int bonus = uprs.getInt("BONUS");
      uprs.updateInt("BONUS", bonus+250);
      uprs.updateRow();
    }

    w = uprs.getWarnings();
    assertNull(w);
    
    uprs.close();
    stmt2 = conn.createStatement();
    rs = stmt2.executeQuery("select * from employee");
    w = rs.getWarnings();
    assertNull(w);

    conn.commit();
    stmt.executeUpdate("update Employee set bonus = 1000 where lastname <> 'kumar'");

    w = stmt.getWarnings();
    assertNull(rs.getWarnings());
    
    stmt2.executeUpdate("update Employee set bonus = bonus + 1000 where lastname <> 'kumar'");
    w = stmt2.getWarnings();
    assertNull(w);
    
    // Prepared Statement
    ps = conn.prepareStatement("SELECT workdept, bonus "
        + "FROM EMPLOYEE where lastname = 'kumar' FOR UPDATE of BONUS", ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_UPDATABLE);
    
    ps.execute();
    
    prs = ps.getResultSet();
    
    while(prs.next()) {
      prs.getString("WORKDEPT");
      int bonus = prs.getInt("BONUS");
      prs.updateInt("BONUS", bonus+1250);
      prs.updateRow();
    }
    
    conn.commit();
    w = prs.getWarnings();
    assertNull(w);

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1 }, "select * from employee", goldenTextFile,
    "non_equal_bonus");
  }

  protected int getIsolationLevel() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }
}
