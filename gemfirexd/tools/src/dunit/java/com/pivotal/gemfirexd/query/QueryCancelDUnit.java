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

package com.pivotal.gemfirexd.query;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Tests for query cancel functionality
 * @author shirishd
 */
@SuppressWarnings("serial")
public class QueryCancelDUnit extends QueryCancelTestHelper {

  public QueryCancelDUnit(String name) {
    super(name);
  }
  
  public void createTables(Connection cxn, int numRows) 
      throws Exception {
    final Statement stmt = cxn.createStatement();

    stmt.execute("create table MyTable(x int, y int) partition by column(x)");
    final PreparedStatement pstmt1 = cxn
        .prepareStatement("insert into MyTable values " + "(?, ?)");
    for (int i = 1; i <= numRows; i++) {
      pstmt1.setInt(1, i);
      pstmt1.setInt(2, i);
      pstmt1.execute();
    }
    
    pstmt1.close();
    stmt.close();
  }
  
  public void selectQueryTest(boolean usePrepStatement, boolean useCancelSProc,
      boolean useThinClient, String testName) throws Throwable {
    final int numServers = 2;
    int tableSize = 40;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);

    this.createTables(cxn, tableSize);

    Statement stmt = null;
    String testQueryString = "select * from MyTable where x > 0";
    if (usePrepStatement) {
      final PreparedStatement pstmt2 = cxn
          .prepareStatement("select * from MyTable where x > ?");
      pstmt2.setInt(1, 0);
      stmt = pstmt2;
    } else {
      stmt = cxn.createStatement();
    }

    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(
        testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation(); 
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };

    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(stmt,
        usePrepStatement ? StatementType.PREPARED_STATEMENT
            : StatementType.STATEMENT, 
            numServers, key, testQueryString, useCancelSProc);

    // execute the query again just to make sure that there are
    // no other issues after the cancellation
    ResultSet rs = null;
    if (usePrepStatement) {
      rs = ((PreparedStatement)stmt).executeQuery();
    } else {
      rs = stmt.executeQuery(testQueryString);
    }
    int numRows = 0;
    while (rs.next()) {
      numRows++;
    }
    assertEquals(tableSize, numRows);
  }
  
  public void testSelectQuery_prepStmt() throws Throwable {
    selectQueryTest(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        false /*use thin client?*/, 
        "testSelectQuery_prepStmt");
  }
  
  public void testSelectQuery_prepStmt_thinClient() throws Throwable {
    selectQueryTest(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        true /*use thin client*/, 
        "testSelectQuery_prepStmt_thinClient");
  }
  
  public void testSelectQuery() throws Throwable {
    selectQueryTest(false, /*cancel a prepared statement?*/ 
        false/*don't use system proc to cancel the query*/,
        false /*use thin client?*/, 
        "testSelectQuery");
  }
  
  public void testSelectQuery_thinClient() throws Throwable {
    selectQueryTest(false, /*cancel a prepared statement?*/ 
        false/*don't use system proc to cancel the query*/,
        true /*use thin client?*/, 
        "testSelectQuery_thinClient");
  }

  public void testSelectQueryUsingCancelSproc_thin() throws Throwable {
    selectQueryTest(false,  /*cancel a prepared statement?*/
        true /*use system proc to cancel the query*/,
        true /*use thin client?*/, 
        "testSelectQueryUsingCancelSproc_thin");
  }

  /*
   * Verifies that a transaction is rolled back if a delete DML being executed
   * inside it is cancelled.
   */
  public void testTxRollbackOnQueryCancellation() throws Throwable {
    deleteQueryTestWithTx(true, /*cancel a prep statement?*/
        false /*don't use system proc to cancel the query*/,
        false /*don't use thin client*/, 
        "testTxRollbackOnQueryCancellation");
  }
  
  public void deleteQueryTestWithTx(boolean usePrepStatement, boolean useCancelProc,
      boolean useThinClient, String testName) throws Throwable {
    final int numServers = 2;
    int tableSize = 40;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);
 
    this.createTables(cxn, tableSize);
    
    cxn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    cxn.setAutoCommit(false);
    
    Statement st = cxn.createStatement();
    st.execute("insert into MyTable values(1, 1)");
    st.execute("insert into MyTable values(2, 2)");
    st.execute("insert into MyTable values(3, 3)");
    st.execute("insert into MyTable values(4, 4)");
    
    ResultSet rs = st.executeQuery("select count(*) from MyTable");
    assertTrue(rs.next());
    int count = rs.getInt(1);
    // 40 committed earlier and 4 added above not yet committed
    assertEquals(44, count);
    
    Statement stmt = null;
    String testQueryString = "delete from MyTable where x > 0";
    if (usePrepStatement) {
      final PreparedStatement pstmt2 = cxn
          .prepareStatement("delete from MyTable where x > ?");
      pstmt2.setInt(1, 0);
      stmt = pstmt2;
    } else {
      stmt = cxn.createStatement();
    }

    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(
        testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onDeleteResultSetOpen(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onDeleteResultSetOpen called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation(); 
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    
    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in 
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(stmt,
        usePrepStatement ? StatementType.PREPARED_STATEMENT
            : StatementType.STATEMENT, 
            numServers, key, testQueryString, useCancelProc);
    

    rs = st.executeQuery("select count(*) from MyTable");
    assertTrue(rs.next());
    count = rs.getInt(1);
    // Tx should have rolled back because of delete query cancellation
    // and so the previously added 4 rows should not show up.
    assertEquals(40, count);

  }
  
  
  public void deleteQueryTest1(boolean usePrepStatement, boolean useCancelProc,
      boolean useThinClient, String testName) throws Throwable {
    final int numServers = 2;
    int tableSize = 40;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);
 
    this.createTables(cxn, tableSize);

    Statement stmt = null;
    String testQueryString = "delete from MyTable where x > 0";
    if (usePrepStatement) {
      final PreparedStatement pstmt2 = cxn
          .prepareStatement("delete from MyTable where x > ?");
      pstmt2.setInt(1, 0);
      stmt = pstmt2;
    } else {
      stmt = cxn.createStatement();
    }

    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(
        testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onDeleteResultSetOpen(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onDeleteResultSetOpen called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation(); 
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    
    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in 
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(stmt,
        usePrepStatement ? StatementType.PREPARED_STATEMENT
            : StatementType.STATEMENT, 
            numServers, key, testQueryString, useCancelProc);
  }
  
  // simple deletes
  
  //1. cancelling un-prepared delete statement over thin client (not supported)
  public void testDeleteQuery1_thinClient() throws Throwable {
    try {
      deleteQueryTest1(false, /*cancel a prep statement?*/
          false /*don't use system proc to cancel the query*/,
          true /*use thin client*/, 
          "testDeleteQuery1_thinClient");
    } catch (java.sql.SQLFeatureNotSupportedException se) {
      //ignore
    }
  }
  
  //2. cancelling a un-prepared delete statement over peer client
  public void testDeleteQuery1() throws Throwable {
      deleteQueryTest1(true, /*cancel a prep statement?*/
          false /*don't use system proc to cancel the query*/,
          false /*don't use thin client*/, 
          "testDeleteQuery1");
  }
  
  // prepared statement peer client
  public void testDeleteQuery1_prepStmt() throws Throwable {
    deleteQueryTest1(true, /*cancel a prep statement?*/
        false /*don't use system proc to cancel the query*/, 
        false /*don't use thin client*/, 
        "testDeleteQuery1_prepStmt");
  }
  
  // prepared statement thin client
  public void testDeleteQuery1_prepStmt_thinClient() throws Throwable {
    deleteQueryTest1(true, /*cancel a prep statement?*/
        false /*don't use system proc to cancel the query*/, 
        true /*don't use thin client*/, 
        "testDeleteQuery1_prepStmt_thinClient");
  }
  
  // cancel by calling system proc (sys.cancel_statement)
  public void testDeleteQuery1UsingCancelProc_prepStmt_thinClient()
      throws Throwable {
    deleteQueryTest1(true, /*cancel a prep statement?*/
        true /*use system proc to cancel the query*/,
        true /*use thin client*/,
        "testDeleteQuery1UsingCancelProc_prepStmt_thinClient");
  }
  
  public void deleteQueryTest2(boolean useCancelProc, boolean useThinClient,
      String testName) throws Throwable {
    final int numServers = 2;
    int tableSize = 40;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);
    
    final Statement stmt = cxn.createStatement();

    stmt.execute("create table MyTable_parent(x int, y int primary key) " +
                "partition by column(x)");
    stmt.execute("create table MyTable_child(a int, b int, foreign key " +
                "(b) references MyTable_parent(y)) partition by column(a)");
    
    PreparedStatement pstmt1 = cxn
        .prepareStatement("insert into MyTable_parent values " + "(?, ?)");
    for (int i = 1; i <= tableSize*2; i++) {
      pstmt1.setInt(1, i);
      pstmt1.setInt(2, i);
      pstmt1.execute();
    }
    
    pstmt1 = cxn
        .prepareStatement("insert into MyTable_child values " + "(?, ?)");
    for (int i = 1; i <= tableSize; i++) {
      pstmt1.setInt(1, i);
      pstmt1.setInt(2, i);
      pstmt1.execute();
    }
    pstmt1.close();
    stmt.close();
    
    final PreparedStatement pstmt2 = cxn
        .prepareStatement("delete from MyTable_parent where x > ?");
    pstmt2.setInt(1, tableSize);

    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(
        testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onDeleteResultSetOpenAfterRefChecks (
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onDeleteResultSetOpenAfterRefChecks called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation(); 
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    
    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in 
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(pstmt2, StatementType.PREPARED_STATEMENT,
        numServers, key, null, useCancelProc);
  }
  
  //delete from parent so that ref key checks will be triggered on the child
  public void testDeleteQuery2() throws Throwable {
    deleteQueryTest2(false /*don't use system proc to cancel the query*/,
        false /*don't use thin client*/, "testDeleteQuery2");
  }
  
  public void testDeleteQuery2UsingCancelProc_thin() throws Throwable {
    deleteQueryTest2(true /*use system proc to cancel the query*/,
        true /*use thin client*/, "testDeleteQuery2UsingCancelProc_thin");
  }

  public void updateQueryTest(boolean useCancelProc, boolean useThinClient,
      String testName) throws Throwable {
    final int numServers = 2;
    int tableSize = 40;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);

    this.createTables(cxn, tableSize);

    final PreparedStatement pstmt2 = cxn
        .prepareStatement("select * from MyTable where x > ?");
    pstmt2.setInt(1, 0);

    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(
        testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation(); 
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    
    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in 
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(pstmt2, StatementType.PREPARED_STATEMENT,
        numServers, key, null, useCancelProc);
  }
  
  public void testUpdateQuery() throws Throwable {
    updateQueryTest(false /*don't use system proc to cancel the query*/,
        false /*don't use thin client*/, "testUpdateQuery");
  }
  
  public void testUpdateQueryUsingCancelProc_thin() throws Throwable {
    updateQueryTest(true /*use system proc to cancel the query*/,
        true /*use thin client*/, "testUpdateQueryUsingCancelProc_thin");
  }
  
  // cancel a batch of different statements
  // TODO: as this cannot be tested using a PreparedStatement
  public void __testExecuteBatch() throws Throwable {

  }
  
  // cancel a batched insert
  public void testBatchInsert() throws Throwable {
    final int numServers = 2;
    final int numClients = 1;
    int tableSize = 10;

    startVMs(numClients, numServers);
    Connection cxn = TestUtil.getConnection();

    this.createTables(cxn, 0);

    final PreparedStatement pstmt2 = cxn
        .prepareStatement("insert into mytable values (?, ?)");
    for (int i = 1; i < tableSize; i++) {
      pstmt2.setInt(1, i);
      pstmt2.setInt(2, i);
      pstmt2.addBatch();
    }

    final String key = "testBatchInsert";
    SerializableRunnable csr1 = new SerializableRunnable(
        "_testBatchInsert_") {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void beforeFlushBatch(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet,
                  LanguageConnectionContext lcc) {
                getLogWriter().info("beforeFlushBatch called");
                if (!flag) {
                  incrementValueInBBMap(key, numClients);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    
    // set up the above runnable object
      clientExecute(1, csr1);

    // execute the query in a different thread, this will result in 
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(pstmt2, StatementType.BATCH_STATEMENT,
        numClients, key, null, false);
  }
  
  public static void proc2(int[] count, String testName,
      ResultSet[] resultSet1, ResultSet[] resultSet2,
      ProcedureExecutionContext ctx) 
          throws SQLException, InterruptedException {
    Connection conn = ctx.getConnection();
    // Connection conn = DriverManager.getConnection("jdbc:default:connection");

    PreparedStatement ps1 = conn.prepareStatement("select * from mytable");
    ps1.execute();
    resultSet1[0] = ps1.getResultSet();

    QueryCancelDUnit.incrementValueInBBMap(testName, 2);
    Thread.sleep(3000);
    // check whether the outer callable statement is cancelled
    ctx.checkQueryCancelled();
    fail("The procedure should have failed due to "
        + "a user initiated cancellation (SQLState:XCL56)");

    PreparedStatement ps2 = conn
        .prepareStatement("select count(*) from mytable");
    ps2.execute();
    ps2.getResultSet().next();
    Integer cnt = ps2.getResultSet().getInt(1);
    count[0] = cnt;

    PreparedStatement ps3 = conn
        .prepareStatement("select count(*) from mytable");
    ps3.execute();
    resultSet2[0] = ps3.getResultSet();
  }
 
  public void storedProcTest(boolean useCancelProc, String testName)
      throws Throwable {
    final int numServers = 2;
    startVMs(1, numServers);
    Connection cxn = TestUtil.getConnection();
    Statement stmt = cxn.createStatement();

    // create a procedure
    stmt.execute("CREATE PROCEDURE Proc2 " + "(OUT count INTEGER, " 
       + "IN testName varchar(50))"
        + "LANGUAGE JAVA PARAMETER STYLE JAVA " + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 2 " + "EXTERNAL NAME '"
        + QueryCancelDUnit.class.getName() + ".proc2'");

    stmt.execute("create table MyTable(x int, y int) partition by column(x)");
    stmt.execute("insert into MyTable values (1, 1), (2, 2), (3, 3), "
        + "(4, 4), (5, 5), (6, 6), (7, 7)");

    CallableStatement callableStmt = cxn
        .prepareCall("{CALL Proc2(?, ?) ON TABLE MyTable}");
    callableStmt.registerOutParameter(1, Types.INTEGER);
    callableStmt.setString(2, testName);

    executeAndCancelQuery(callableStmt, StatementType.PREPARED_STATEMENT,
        numServers, testName, null, useCancelProc);
  }
  
  public void testStoredProc() throws Throwable {
    storedProcTest(false, "testStoredProc");
  }
  
  public void DISABLED__testStoredProcUsingCancelProc() throws Throwable {
    storedProcTest(true, "testStoredProcUsingCancelProc");
  }
  
  public void TSMCSubSelectQueryTest(boolean useThinClient, String testName,
      boolean useCancelProc) throws Throwable {
    final int numServers = 2;
    startVMs(1, numServers);
    Connection conn = _getConnection(useThinClient);

    Statement st = conn.createStatement();

    st.execute("create table FDC_NRT_CNTXT_HIST (EQP_ID VARCHAR(40) NOT NULL, "
        + "CNTXT_ID INTEGER NOT NULL, STOP_DT TIMESTAMP NOT NULL, "
        + " primary key(eqp_id, cntxt_id, stop_dt) "
        + ") partition by column (eqp_id, cntxt_id, stop_dt)");

    st.execute("insert into FDC_NRT_CNTXT_HIST values "
        + " ('1', 1, '2014-01-24 18:48:00')"
        + ",('2', 1, '2014-01-24 18:48:00')"
        + ",('3', 1, '2014-01-24 18:48:00')"
        + ",('4', 1, '2014-01-24 18:48:00')"
        + ",('5', 1, '2014-01-24 18:48:00')"
        + ",('6', 1, '2014-01-24 18:48:00')"
        + ",('7', 1, '2014-01-24 18:48:00')"
        + ",('8', 1, '2014-01-24 18:48:00')"
        + ",('9', 1, '2014-01-24 18:48:00')"
        + ",('10', 1, '2014-01-24 18:48:00')"
        + ",('11', 1, '2014-01-24 18:48:00')"
        + ",('12', 1, '2014-01-24 18:48:00')"
        + ",('13', 1, '2014-01-24 18:48:00')");
    
    st.execute("create table FDC_NRT_TCHART_HIST (EQP_ID VARCHAR(40) NOT NULL, "
        + "CNTXT_ID INTEGER NOT NULL, STOP_DT TIMESTAMP NOT NULL, "
        + "SVID_NAME VARCHAR(64) NOT NULL, "
        + " primary key(eqp_id, cntxt_id, stop_dt, svid_name) "
        + ") partition by column (eqp_id, cntxt_id, stop_dt)"
        + " COLOCATE WITH (FDC_NRT_CNTXT_HIST)");

    st.execute("insert into FDC_NRT_TCHART_HIST values "
        + " ('1', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('2', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('3', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('4', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('5', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('6', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('7', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('8', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('9', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('10', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('11', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('12', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')"
        + ",('13', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59')");
    
    st.execute("create table FDC_NRT_PUMPER_HIST_LOG ( EQP_ID VARCHAR(40) "
        + "NOT NULL, CNTXT_ID INTEGER NOT NULL, "
        + "STOP_DT TIMESTAMP NOT NULL, UPDATE_DT TIMESTAMP NOT NULL, "
        + "EXEC_TIME DOUBLE, primary key (eqp_id, cntxt_id, stop_dt, update_dt) "
        + ") partition by primary key");

    st.execute("insert into FDC_NRT_PUMPER_HIST_LOG values "
        + " ('1', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('2', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('3', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('4', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('5', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('6', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('7', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('8', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('9', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('10', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('11', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('12', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)"
        + ",('13', 1, '2014-01-24 18:48:00', '2014-01-24 18:47:59', 41)");
    
    PreparedStatement pstmt = conn
        .prepareStatement("select a.eqp_id, a.cntxt_id, "
            + "a.stop_dt, dsid() as datanode_id "
            + "from FDC_NRT_CNTXT_HIST a " + "left join FDC_NRT_TCHART_HIST b "
            + "on (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and "
            + "a.stop_dt=b.stop_dt) "
            + "where a.eqp_id||cast(a.cntxt_id as char(100)) " + "in " + "( "
            + "select eqp_id||cast(t.cntxt_id as char(100)) "
            + "from FDC_NRT_PUMPER_HIST_LOG t where 1=1 "
            + "and exec_time > 40 " + "and stop_dt > '2014-01-24 18:47:59' "
            + "and stop_dt < '2014-01-24 18:49:59'" + ")");
    
    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation();
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };

    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);

    // execute the query in a different thread, this will result in
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(pstmt, StatementType.PREPARED_STATEMENT, numServers,
        key, null, useCancelProc);

  }

  public void testTSMCSubSelectQuery() throws Throwable {
    TSMCSubSelectQueryTest(true, "testTSMCSubSelectQuery", false);
  }
  
  public void selectQueryWithSubSelectTest(boolean useCancelProc,
      boolean useThinClient, String testName) throws Throwable {
    final int numServers = 2;
    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);
    Statement st = cxn.createStatement();

    final String subquery = "Select SUM(ID2) from Testtable2 where " +
               "description2 = 'desc2_'";
    String query = "select ID1, DESCRIPTION1 from TESTTABLE1 where ID1 IN ("
        + subquery + ")";

    st.execute("create table TESTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) " 
        + "not null, primary key (ID1))"
        + "PARTITION BY COLUMN ( ID1 )");
    
    st.execute("create table TESTTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) " 
        + "not null, primary key (ID2)) " 
        + "partition by column( ID2)  colocate with (TESTTABLE1) ");
    
    for (int i = 1; i < 40; i++) {
      st.execute("Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
          + "', 'ADD_1" + i + "')");
      st.execute("Insert into  TESTTABLE2 values(" + i + ",'desc2_"
          + "', 'ADD_2" + i + "')");
    }
    
    PreparedStatement pstmt = cxn.prepareStatement(query);
    
    final String key = testName;
    SerializableRunnable csr1 = new SerializableRunnable(testName) {

      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private boolean flag = false;

              @Override
              public void onGetNextRowCoreOfGfxdSubQueryResultSet(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                if (!flag) {
                  incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation();
                  long connId = a.getConnectionID();
                  long stmtId = a.getStatementID();
                  long execId = a.getExecutionID();
                  putStatementUUIDinBBMap(key, connId, stmtId, execId);
                  flag = true;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };

    // set up the above runnable object
    serverExecute(1, csr1);
    serverExecute(2, csr1);
    
    // execute the query in a different thread, this will result in
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(pstmt, StatementType.PREPARED_STATEMENT, numServers,
        key, null, useCancelProc);
  }
  
  public void testSubQuery() throws Throwable {
    selectQueryWithSubSelectTest(
        false /*don't use system proc to cancel the query*/,
        false /*don't use thin client*/, "testSubQuery");
  }
  
  public void testSubQueryUsingCancelProc_thin() throws Throwable {
    selectQueryWithSubSelectTest(
        true /*use system proc to cancel the query*/,
        true /*use thin client*/, 
        "testSubQueryUsingCancelProc_thin");
  }
}
