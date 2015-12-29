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
package com.pivotal.gemfirexd.ddl;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

@SuppressWarnings("serial")
public class FKOnPrimaryKeyDUnit extends DistributedSQLTestBase {

  public FKOnPrimaryKeyDUnit(String name) {
    super(name);
  }
  
  protected String reduceLogging() {
    return "config";
  }
  
  public void basicTest(boolean localIndex, boolean isTx) throws SQLException {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    String partitionByClause = null;
    
    if (localIndex) {
      partitionByClause = " partition by list "
          + "(col1) (VALUES (1), VALUES (2), VALUES (3), values (4))";
    } else {
      partitionByClause = " partition by list "
          + "(col3) (VALUES (1), VALUES (2), VALUES (3), values (4))";
    }

    // create tables
    st.execute("create table parent (col1 int, col2 int, col3 int not null, "
        + "constraint pk1 primary key (col1))" + partitionByClause );

    st.execute("create table child (col1 int, col2 int, col3 int not null, "
        + "constraint pk2 primary key (col1), constraint fk1 foreign key "
        + "(col2) references parent (col1)) partition by list "
        + "(col3) (VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    if (isTx) {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    
    st.execute("insert into parent values (1, 1, 1), (2, 2, 2), "
        + "(3, 3, 3), (4, 4, 4)");
    
    // NULL column values do not cause constraint violations 
    // and are ignored during constraint checks
    st.execute("insert into child values (1, 1, 1), (2, 2, 2), (3, NULL, 3)");
    conn.commit();
    
    ResultSet rs2 = st.executeQuery("select count(*) from child");
    assertTrue(rs2.next());
    assertEquals(3, rs2.getInt(1));
    rs2.close();
    ResultSet rs1 = st.executeQuery("select col1 from child order by col1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(3, rs1.getInt(1));
    assertFalse(rs1.next());
    rs1.close();
  }
  
  public void testBasicBatchInsert_PK_global() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    basicTest(false, false);
 }
  
  public void testBasicBatchInsert_PK_global_TX() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    basicTest(false, true);
 }
  
  public void testBasicBatchInsert_PK_local() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    basicTest(true, false);
 }
  
  public void testBasicBatchInsert_PK_local_TX() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    basicTest(true, true);
 }
  
  public void testBatchInsertMultipleFKs() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    
    // create tables
    // global PK index
    st.execute("create table parent1 (col1 int, col2 int, col3 int not null, "
        + "constraint pk1 primary key (col1)) partition by list "
        + "(col3) (VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    // local PK index
    st.execute("create table parent2 (col1 int, col2 int, col3 int not null, "
        + "constraint pk2 primary key (col1)) partition by list "
        + "(col1) (VALUES (1), VALUES (2), VALUES (3), values (4))");

    st.execute("create table child (col1 int, col2 int, col3 int, col4 int, "
        + "constraint pk3 primary key (col1), " 
        + "constraint fk1 foreign key (col2) references parent1 (col1), "
        + "constraint fk2 foreign key (col3) references parent2 (col1)) "
        + "partition by list (col4) "
        + "(VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    
    st.execute("insert into parent1 values (1, 1, 1), (2, 2, 2), "
        + "(3, 3, 3), (4, 4, 4)");
    
    st.execute("insert into parent2 values (1, 1, 1), (2, 2, 2), "
        + "(3, 3, 3), (4, 4, 4)");
    
    // NULL column values do not cause constraint violations 
    // and are ignored during constraint checks
    st.execute("insert into child values (1, 1, 1, 1), (2, 2, 2, 2), (3, NULL, NULL, 3)");
    conn.commit();
    
    ResultSet rs2 = st.executeQuery("select count(*) from child");
    assertTrue(rs2.next());
    assertEquals(3, rs2.getInt(1));
    rs2.close();
    ResultSet rs1 = st.executeQuery("select col1 from child order by col1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(3, rs1.getInt(1));
    assertFalse(rs1.next());
    rs1.close();
  }
  
  public void testCompositeFKonPK_global() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    
    // create tables
    // global PK index
    st.execute("create table base1 (col1 int, col2 int, col3 int not null, " 
        + "col4 int, constraint pk1 primary key (col1, col4)) " 
        + "partition by list (col3) (VALUES (1), VALUES (2), " 
        + "VALUES (3), values (4))");
    
    // local PK index
    st.execute("create table base2 (col1 int, col2 int, col3 int not null, " 
        + "col4 int, constraint pk2 primary key (col1, col4)) " 
        + "partition by column (col1, col4)");
    
    st.execute("create table child (col1 int, col2 int, col3 int, col4 int, "
        + "col5 int, col6 int,"
        + "constraint pk3 primary key (col1), " 
        + "constraint fk1 foreign key (col2, col4) references base1 (col1, col4), "
        + "constraint fk2 foreign key (col5, col6) references base2 (col1, col4)) "
        + "partition by list (col4) "
        + "(VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    
    st.execute("insert into base1 values (1, 1, 1, 1), (2, 2, 2, 2), "
        + "(3, 3, 3, 3), (4, 4, 4, 4)");
    
    st.execute("insert into base2 values (10, 10, 10, 10), (20, 20, 20, 20), "
        + "(30, 30, 30, 30), (40, 40, 40, 40)");
    
    st.execute("insert into child values " 
    		+ "(1, 1, 1, 1, 10, 10), " 
    		+ "(2, 2, 2, 2, 20, 20), "
        // NULL column values do not cause constraint violations 
        // and are ignored during constraint checks
    		+ "(3, NULL, NULL, 3, 30, NULL)");
    
    conn.commit();
    ResultSet rs2 = st.executeQuery("select count(*) from child");
    assertTrue(rs2.next());
    assertEquals(3, rs2.getInt(1));
    rs2.close();
    ResultSet rs1 = st.executeQuery("select col1 from child order by col1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(3, rs1.getInt(1));
    assertFalse(rs1.next());
    rs1.close();
  }
  
  public void testBatchInsert_FkOnPkViolation() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    // create tables
    st.execute("create table parent (col1 int, col2 int, col3 int not null, "
        + "constraint pk1 primary key (col1)) partition by list "
        + "(col3) (VALUES (1), VALUES (2), VALUES (3), values (4))");

    st.execute("create table child (col1 int, col2 int, col3 int not null, "
        + "constraint pk2 primary key (col1), constraint fk1 foreign key "
        + "(col2) references parent (col1)) partition by list "
        + "(col3) (VALUES (1), VALUES (2), VALUES (3), values (4))");

    st.execute("insert into parent values (1, 1, 1), (2, 2, 2), "
        + "(3, 3, 3), (4, 4, 4)");
    conn.commit();
    PreparedStatement pstmt = conn.prepareStatement("insert into child "
        + "values (?, ?, ?)");

    for (int i = 1; i <= 3; i++) {
      pstmt.setInt(1, i);
      pstmt.setInt(2, i);
      pstmt.setInt(3, i);
      pstmt.addBatch();
    }
    
    // this row to cause an FK violation 
    pstmt.setInt(1, 4);
    pstmt.setInt(2, 100); // FK violation
    pstmt.setInt(3, 4);
    pstmt.addBatch();
    // one more row with no error
    pstmt.setInt(1, 5);
    pstmt.setInt(2, 3);
    pstmt.setInt(3, 4);
    pstmt.addBatch();
    
    try {
      int[] ret = pstmt.executeBatch();
      fail("This statement should have failed due to FK violation");
    } catch (java.sql.BatchUpdateException be) {
      assertEquals("23503", be.getSQLState());
    }
    
    // no rows should be inserted
    ResultSet rs = st.executeQuery("select count(*) from child");
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
  }
  
  public void testSelfReferentialConstraints() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    
    // create table with self referential FK constraint
    // local index
    st.execute("create table base1 (col1 int, col2 int, col3 int, "
        + "constraint pk1 primary key (col1), constraint fk1 foreign key " +
        "(col2) references base1 (col1))" + " partition by list "
        + "(col1) (VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    // should not cause FK violation
    // the row being inserted itself satisfies the FK constraint
    st.execute("insert into base1 values (1, 1, 1), (2, 2, 2), "
        + "(3, 3, 3)");
    
    ResultSet rs2 = st.executeQuery("select count(*) from base1");
    assertTrue(rs2.next());
    assertEquals(3, rs2.getInt(1));
    rs2.close();
    ResultSet rs1 = st.executeQuery("select col1 from base1 order by col1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(3, rs1.getInt(1));
    assertFalse(rs1.next());
    rs1.close();
    
    // create table with self referential FK constraint
    // global index
    st.execute("create table base2 (col1 int, col2 int, col3 int, col4 int," +
    		" col5 int, constraint pk2 unique (col1, col2), constraint " +
    		"fk2 foreign key (col3, col4) references base2 (col1, col2))" + 
    		" partition by list "
        + "(col5) (VALUES (1), VALUES (2), VALUES (3), values (4))");
    
    // should not cause FK violation
    // the row being inserted itself satisfies the FK constraint
    st.execute("insert into base2 values (1, 1, 1, 1, 1), (2, 2, 2, 2, 2), "
        + "(3, 3, 3, 3, 3)");
    
    rs2 = st.executeQuery("select count(*) from base2");
    assertTrue(rs2.next());
    assertEquals(3, rs2.getInt(1));
    rs2.close();
    rs1 = st.executeQuery("select col1 from base2 order by col1");
    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
    assertTrue(rs1.next());
    assertEquals(3, rs1.getInt(1));
    assertFalse(rs1.next());
    rs1.close();
    
  }
  
  public void doSomeWarmUpOps(boolean isTx, Statement st, Connection conn)
    throws SQLException {
    basicPerfTest(isTx, st, conn, 300000, true);
    truncateTablesForPerfTest(st);
  }
  
  public void createTablesForPerfTest(boolean useLocalIndex, Statement st, 
    boolean usePrimaryKey) throws SQLException {
    String partitionByClause = null;
    String primaryOrUniqeClause = null;
    
    if (useLocalIndex) {
      partitionByClause = " PARTITION BY RANGE (col1) " ;
    } else {
      // global index
      partitionByClause = " PARTITION BY RANGE (col3) " ;
    }
    
    if (usePrimaryKey) {
      primaryOrUniqeClause = " constraint pk1 primary key (col1)";
    } else {
      primaryOrUniqeClause = " constraint pk1 unique (col1)";
    }
    
    // create tables
    st.execute("create table parent (col1 int, col2 int, col3 int not null, "
        + primaryOrUniqeClause + ")" + partitionByClause  
        + "(VALUES BETWEEN 1 AND 10000, VALUES BETWEEN 100001 AND 200000," 
        + " VALUES BETWEEN 200001 AND 300000)");

    st.execute("create table child (col1 int, col2 int, col3 int not null, "
        + "constraint pk2 primary key (col1), constraint fk1 foreign key "
        + "(col2) references parent (col1)) PARTITION BY RANGE "
        + "(col3) (VALUES BETWEEN 1 AND 10000, VALUES BETWEEN 100001 AND 200000," +
        " VALUES BETWEEN 200001 AND 300000)");
  }
  
  public void truncateTablesForPerfTest(Statement st) throws SQLException {
    st.execute("truncate table child");
    st.execute("truncate table parent");
  }
  
  public void basicPerfTest(boolean isTx, Statement st, Connection conn, 
    int numRowsToBeInserted, boolean forWarmUpOps) throws SQLException {
    if (isTx) {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    getLogWriter().info("forWarmUpOps=" + forWarmUpOps);
    
    getLogWriter().info("inserting into a parent table");
    PreparedStatement ps1 = conn.prepareStatement("insert into parent values" +
        " (?, ?, ?)");
    for (int i = 1; i <= numRowsToBeInserted; i++) {
      ps1.setInt(1, i);
      ps1.setInt(2, i);
      ps1.setInt(3, i);
      ps1.addBatch();
    }
    ps1.executeBatch();
    getLogWriter().info("DONE inserting into parent table");
    
    getLogWriter().info("inserting into a child table");
    long startTime = System.nanoTime();
    PreparedStatement ps2 = conn.prepareStatement("insert into child values" +
        " (?, ?, ?)");
    int val = numRowsToBeInserted + 1;
    for (int i = 1; i <= numRowsToBeInserted; i++) {
      ps2.setInt(1, i);
      ps2.setInt(2, val - i);
      ps2.setInt(3, i);
      ps2.addBatch();
    }
    ps2.executeBatch();
    getLogWriter().info("DONE insert into child table");
    long endTime = System.nanoTime();
    if (!forWarmUpOps) {
      long elpasedTime = endTime - startTime;
      long seconds = TimeUnit.SECONDS.convert(elpasedTime,
        TimeUnit.NANOSECONDS);
      getLogWriter().info("time required to insert into child table="
        + elpasedTime + "ns");
    }
    conn.commit();
 }
  
 // global index bulk fk checks disabled
 public void __testBasicBatchInsert_PK_global_TX_perf_disabled() throws Exception {
   Properties props = new Properties();
   System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
   props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
   startVMs(1, 3, 0, null, props);
   Connection conn = TestUtil.getConnection(props);
   Statement st = conn.createStatement();
   boolean isTx = true;
   boolean useLocalIndex = false;
   createTablesForPerfTest(useLocalIndex, st, true);
   doSomeWarmUpOps(isTx, st, conn);
   basicPerfTest(isTx, st, conn, 300000, false);
}
  
  // global index bulk fk checks enabled
  public void __testBasicBatchInsert_PK_global_TX_perf_enabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = true;
    boolean useLocalIndex = false;
    createTablesForPerfTest(useLocalIndex, st, true);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }
  
//global index bulk fk checks enabled
 public void __testBasicBatchInsert_PK_global_perf_disabled() throws Exception {
   Properties props = new Properties();
   System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
   props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
   startVMs(1, 3, 0, null, props);
   Connection conn = TestUtil.getConnection(props);
   Statement st = conn.createStatement();
   boolean isTx = false;
   boolean useLocalIndex = false;
   createTablesForPerfTest(useLocalIndex, st, true);
   doSomeWarmUpOps(isTx, st, conn);
   basicPerfTest(isTx, st, conn, 300000, false);
 }
 
//global index bulk fk checks enabled
public void __testBasicBatchInsert_PK_global_perf_enabled() throws Exception {
  Properties props = new Properties();
  System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
  props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
  startVMs(1, 3, 0, null, props);
  Connection conn = TestUtil.getConnection(props);
  Statement st = conn.createStatement();
  boolean isTx = false;
  boolean useLocalIndex = false;
  createTablesForPerfTest(useLocalIndex, st, true);
  doSomeWarmUpOps(isTx, st, conn);
  basicPerfTest(isTx, st, conn, 300000, false);
}
  
  // fk local index bulk fk checks disabled
  public void __testBasicBatchInsert_PK_local_TX_perf_disabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = true;
    boolean useLocalIndex = true;
    createTablesForPerfTest(useLocalIndex, st, true);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }
  
  // fk local index bulk fk checks enabled
  public void __testBasicBatchInsert_PK_local_TX_perf_enabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = true;
    boolean useLocalIndex = true;
    createTablesForPerfTest(useLocalIndex, st, true);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }
  
//fk local index bulk fk checks disabled
 public void __testBasicBatchInsert_PK_local_perf_disabled() throws Exception {
   Properties props = new Properties();
   System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
   props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
   startVMs(1, 3, 0, null, props);
   Connection conn = TestUtil.getConnection(props);
   Statement st = conn.createStatement();
   boolean isTx = false;
   boolean useLocalIndex = true;
   createTablesForPerfTest(useLocalIndex, st, true);
   doSomeWarmUpOps(isTx, st, conn);
   basicPerfTest(isTx, st, conn, 300000, false);
 }
  
//fk local index bulk fk checks enabled
 public void __testBasicBatchInsert_PK_local_perf_enabled() throws Exception {
   Properties props = new Properties();
   System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
   props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
   startVMs(1, 3, 0, null, props);
   Connection conn = TestUtil.getConnection(props);
   Statement st = conn.createStatement();
   boolean isTx = false;
   boolean useLocalIndex = true;
   createTablesForPerfTest(useLocalIndex, st, true);
   doSomeWarmUpOps(isTx, st, conn);
   basicPerfTest(isTx, st, conn, 300000, false);
 }
  
  // global index bulk fk checks disabled
  public void __testBasicBatchInsert_UK_global_perf_disabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = false;
    boolean useLocalIndex = false;
    createTablesForPerfTest(useLocalIndex, st, false);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }

  // global index bulk fk checks enabled
 public void __testBasicBatchInsert_UK_global_perf_enabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = false;
    boolean useLocalIndex = false;
    createTablesForPerfTest(useLocalIndex, st, false);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }
  
  // local index bulk fk checks disabled
  public void __testBasicBatchInsert_UK_local_perf_disabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "false");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = false;
    boolean useLocalIndex = true;
    createTablesForPerfTest(useLocalIndex, st, false);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }

  // local index bulk fk checks enabled
  public void __testBasicBatchInsert_UK_local_perf_enabled() throws Exception {
    Properties props = new Properties();
    System.clearProperty(GfxdConstants.GFXD_ENABLE_BULK_FK_CHECKS);
    props.setProperty(Attribute.ENABLE_BULK_FK_CHECKS, "true");
    startVMs(1, 3, 0, null, props);
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    boolean isTx = false;
    boolean useLocalIndex = false;
    createTablesForPerfTest(useLocalIndex, st, false);
    doSomeWarmUpOps(isTx, st, conn);
    basicPerfTest(isTx, st, conn, 300000, false);
  }
}