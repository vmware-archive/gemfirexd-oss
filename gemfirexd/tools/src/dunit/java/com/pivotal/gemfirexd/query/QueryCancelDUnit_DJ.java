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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericActivationHolder;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Query cancellation tests for distributed join queries
 * @author shirishd
 */
public class QueryCancelDUnit_DJ extends QueryCancelTestHelper {
  
  public QueryCancelDUnit_DJ(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }  
  
  public void selectQueryTest_PR_PR(boolean usePrepStatement, boolean useCancelSProc,
      boolean useThinClient, String testName) throws Throwable {
    final int numServers = 3;
    Properties props = new Properties();
//    props.setProperty("gemfirexd.debug.true", "QueryDistribution,TraceExecution");
    
    startVMs(1, numServers, 0, null, props);
    Connection cxn = _getConnection(useThinClient);
    
    Statement st = cxn.createStatement();
    st.execute("create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    cxn.commit();
    
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
    }

    Statement stmt = null;
    String testQueryString = "Select A.ID, A.VID, B.ID, B.VID from "
        + " tglobalindex A inner join tdriver B "
        + " on A.ID = B.ID "
        + " where A.SID > 16 and B.SID < 16 ";
//        + " where A.SID > ? " + "and B.SID < ? ";
    if (usePrepStatement) {
      final PreparedStatement pstmt2 = cxn
          .prepareStatement(testQueryString);
//      pstmt2.setInt(1, 6);
//      pstmt2.setInt(2, 16);
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
              public void beforeQueryExecution(EmbedStatement stmt,
                  Activation activation) {
                long connId = ((GenericActivationHolder) activation)
                    .getActivation().getLanguageConnectionContext()
                    .getConnectionId();
                if (connId != -2) {
                  long rootID = ((GenericActivationHolder) activation)
                      .getActivation().getStatementID();
                  //long execId = stmt.getExecutionID();
                  long execId = 1; //cancelling the first execution 
                  QueryCancelDUnit.putStatementUUIDinBBMap(key, connId, rootID,
                      execId);
                }
                getLogWriter().info(
                    "beforeQueryExecution called connId=" + connId);
              }
              
              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                if (!flag) {
                  QueryCancelDUnit.incrementValueInBBMap(key, numServers);
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
    serverExecute(3, csr1);

    // execute the query in a different thread, this will result in
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(stmt,
        usePrepStatement ? StatementType.PREPARED_STATEMENT
            : StatementType.STATEMENT, 
            numServers, key, testQueryString, useCancelSProc);
  }
  
  public void testPRPRQuery_prepStmt() throws Throwable {
    selectQueryTest_PR_PR(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        false /*use thin client?*/, 
        "testPRPRQuery_prepStmt");
  }
  
  public void testPRPRQuery_prepStmt_thin() throws Throwable {
    selectQueryTest_PR_PR(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        true /*use thin client?*/, 
        "testPRPRQuery_prepStmt_thin");
  }
  
  public void testPRPRQueryUsingCancelSproc_thin() throws Throwable {
    selectQueryTest_PR_PR(false,  /*cancel a prepared statement?*/
        true /*use system proc to cancel the query*/,
        true /*use thin client?*/, 
        "testPRPRQueryUsingCancelSproc_thin");
  }
  
  public void testPRPRQuery() throws Throwable {
    selectQueryTest_PR_PR(false, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        false /*use thin client?*/, 
        "testPRPRQuery");
  }

  
  public void selectQueryTest_PRpk_COLoth_COLoth_COLoth_REP(
      boolean usePrepStatement, boolean useCancelSProc, boolean useThinClient,
      String testName) throws Throwable {
    final int numServers = 3;

    startVMs(1, numServers);
    Connection cxn = _getConnection(useThinClient);

    Statement st = cxn.createStatement();
    st.execute("create table tpk ( id int not null, "
        + "vid varchar(10) primary key, sid int not null) partition by primary key");
    st.execute("create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    st.execute("create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");
    cxn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tpk values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
      st.execute("Insert into trep values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
      st.execute("Insert into tcol values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
      st.execute("Insert into tcol2 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
      st.execute("Insert into tcol3 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }

    Statement stmt = null;
    String testQueryString = "Select A.ID, A.VID, B.ID, B.VID, C.ID," 
    		+ " C.VID, D.ID, D.VID, E.ID, E.VID from "
        + " tpk A inner join tcol B "
        + "on A.VID = B.VID and A.ID = B.ID"
        + " inner join tcol2 C "
        + "on B.ID = C.ID and B.VID = C.VID"
        + " inner join tcol3 D "
        + "on C.ID = D.ID and D.VID = C.VID"
        + " inner join trep E "
        + "on E.ID = D.ID and E.VID = D.VID"
        + " where A.SID < 450 " + "and B.SID != 200 ";
    
    if (usePrepStatement) {
      final PreparedStatement pstmt2 = cxn.prepareStatement(testQueryString);
      stmt = pstmt2;
    } else {
      stmt = cxn.createStatement();
    }

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
                  QueryCancelDUnit.incrementValueInBBMap(key, numServers);
                  Activation a = resultSet.getActivation();
                  long connId = a.getConnectionID();
                  // long stmtId = a.getStatementID();
                  long rootID = a.getRootID();
                  long execId = a.getExecutionID();
                  QueryCancelDUnit.putStatementUUIDinBBMap(key, connId, rootID,
                      execId);
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
    serverExecute(3, csr1);

    // execute the query in a different thread, this will result in
    // data nodes incrementing a value in BB map in observer
    // callback as an indication to cancel the query
    executeAndCancelQuery(stmt,
        usePrepStatement ? StatementType.PREPARED_STATEMENT
            : StatementType.STATEMENT, numServers, key, testQueryString,
        useCancelSProc);
  }
  
  public void testPRpk_COLoth_COLoth_COLoth_REP_prepStmt() throws Throwable {
    selectQueryTest_PRpk_COLoth_COLoth_COLoth_REP(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        false /*use thin client?*/, 
        "testPRpk_COLoth_COLoth_COLoth_REP_prepStmt");
  }
  
  public void testPRpk_COLoth_COLoth_COLoth_REP_prepStmt_thin() throws Throwable {
    selectQueryTest_PRpk_COLoth_COLoth_COLoth_REP(true, /*is it a prepared statement?*/ 
        false /*use system proc to cancel the query?*/,
        true /*use thin client?*/, 
        "testPRpk_COLoth_COLoth_COLoth_REP_prepStmt_thin");
  }

}