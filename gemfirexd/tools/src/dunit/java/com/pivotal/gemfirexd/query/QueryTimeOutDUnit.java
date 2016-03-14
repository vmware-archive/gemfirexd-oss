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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.Properties;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Tests for query timeout functionality
 * @author shirishd
 *
 */
@SuppressWarnings("serial")
public class QueryTimeOutDUnit extends DistributedSQLTestBase {

  public QueryTimeOutDUnit(String name) {
    super(name);
  }
  
  public void testSimpleSelectQuery() throws Exception {
    startVMs(1, 2);
    Connection cxn = TestUtil.getConnection();
    final Statement stmt = cxn.createStatement();

    stmt.execute("create table MyTable(x int, y int) partition by column(x)");
    final PreparedStatement pstmt1 = cxn
        .prepareStatement("insert into MyTable values " + "(?, ?)");
    for (int i = 1; i <= 10; i++) {
      pstmt1.setInt(1, i);
      pstmt1.setInt(2, i);
      pstmt1.execute();
    }
    final PreparedStatement pstmt2 = cxn
        .prepareStatement("select * from MyTable where x > ?");
    pstmt2.setInt(1, 0);

    SerializableRunnable csr2 = new SerializableRunnable(
        "_testSimpleSelectQuery_") {
      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                try {
                  getLogWriter().info("onGetNextRowCoreOfBulkTableScan called");
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };

    // delay the execution so that the query times out
    clientExecute(1, csr2);
    serverExecute(1, csr2);
    serverExecute(2, csr2);

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 },
        SQLException.class);
    try {
      pstmt2.setQueryTimeout(1);
      ResultSet rs = pstmt2.executeQuery();
      while (rs.next()) {
        System.out.println(rs.getInt(1));
        System.out.println(rs.getInt(2));
      }
      fail("This test should have thrown exception "
          + "due to query cancellation (exception state XCL52)");
    } catch (SQLException se) {
       if (!se.getSQLState().equals("XCL52")) {
       throw se;
       } // else ignore
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          SQLException.class);
    }
  }
  
  public void queryTimeOutTestTSMC(boolean useThinClient) throws Exception {
    startVMs(1, 3);
    Connection conn = null;
    
    if (useThinClient) {
//      Properties props = new Properties();
//       props.setProperty("log-level", "fine");
      int clientPort = startNetworkServer(1, null, null);
      conn = TestUtil.getNetConnection(clientPort, null, null);
    } else {
      conn = TestUtil.getConnection();
    }
    
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

    // suspend the execution to allow query get timed out
    SerializableRunnable csr = new SerializableRunnable(
        "_testTimeOut_") {
      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              @Override
              public void onGetNextRowCore(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };

    clientExecute(1, csr);
    serverExecute(1, csr);
    serverExecute(2, csr);
    serverExecute(3, csr);

    try {
      addExpectedException(new int[] { 1 }, new int[] { 1, 2, 3 },
          SQLException.class);
      st.setQueryTimeout(1);
      ResultSet r = st
          .executeQuery("select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id "
              + "from FDC_NRT_CNTXT_HIST a "
              + "left join FDC_NRT_TCHART_HIST b "
              + "on (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) "
              + "where a.eqp_id||cast(a.cntxt_id as char(100)) "
              + "in "
              + "( "
              + "select eqp_id||cast(t.cntxt_id as char(100)) "
              + "from FDC_NRT_PUMPER_HIST_LOG t where 1=1 "
              + "and exec_time > 40 "
              + "and stop_dt > '2014-01-24 18:47:59' "
              + "and stop_dt < '2014-01-24 18:49:59'" + ")");

      while (r.next()) {
        // System.out.println(r.getString(1));
      }
      fail("This test should have thrown exception "
          + "due to query timeout (exception state XCL52)");
    } catch (SQLException se) {
      if (!se.getSQLState().equals("XCL52")) {
        throw se;
      }
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2, 3 },
          SQLException.class);
    }
  }

  // test query time out for TSMC reported query
  public void testQueryTimeOutTSMC() throws Exception {
    queryTimeOutTestTSMC(false /*don't use thin client*/);
  }
  
  public void testQueryTimeOutTSMC_thin() throws Exception {
	  if(isTransactional) {
		  return;
	  }
    queryTimeOutTestTSMC(true /*use thin client*/);
  }
  
  /**
   * 
   * @param timOutOnCallableStmt - time out (in seconds) set on the outer 
   * callable stmt
   */
  public static void myProc1(int timOutOnCallableStmt, 
      int[] count, ResultSet[] resultSet1,
      ResultSet[] resultSet2,
      ProcedureExecutionContext ctx) 
          throws SQLException, InterruptedException {
    Connection conn = ctx.getConnection();
//    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    Statement stmt = conn.createStatement();
    
    // make sure that the timeout for statement in proc is same as 
    // that of outer callable statement's time out
    assertEquals(timOutOnCallableStmt, stmt.getQueryTimeout());
    
    // introduce delay so that the procedure times out
    Thread.sleep(1500);
    // check whether the outer callable statement is cancelled
    ctx.checkQueryCancelled();
    fail("The procedure should have failed due to " +
    		"query time out (SQLState:XCL52");
    
    stmt.execute("select * from mytable");
    resultSet1[0] = stmt.getResultSet();
    
    Statement stmt3 = conn.createStatement();
    stmt3 .execute("select count(*) from mytable");
    stmt3.getResultSet().next();
    Integer cnt = stmt3.getResultSet().getInt(1);
    count[0] = cnt;
    
    Statement stmt2 = conn.createStatement();
    stmt2.execute("select count(*) from mytable");
    resultSet2[0] = stmt2.getResultSet();
  }
  
  // tests query timeout when time out is set on (outer) callable statement
  // only
  public void testQueryTimeOutStoredProc_1() throws Exception {
    startVMs(1, 2);
    Connection cxn = TestUtil.getConnection() ;
    Statement stmt = cxn.createStatement();
    
    // create a procedure
    stmt.execute("CREATE PROCEDURE myProc1 " 
        + "(IN timOutOnCallableStmt INTEGER, "
        + " OUT count INTEGER)" 
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA " + "DYNAMIC RESULT SETS 2 " + "EXTERNAL NAME '"
        + QueryTimeOutDUnit.class.getName() + ".myProc1'");
    
    stmt.execute("create table MyTable(x int, y int) partition by column(x)");
    stmt.execute("insert into MyTable values (1, 1), (2, 2), (3, 3), " +
    		"(4, 4), (5, 5), (6, 6), (7, 7)");
    
    int timOutOnCallableStmt = 1; // seconds
    // introduce an artificial delay in sproc to timeout the callable stmt
    CallableStatement callableStmt = cxn
         // first param to myProc1- timeOut to be set on callable stmt
        // CALL myProc(1, ?) ON TABLE MyTable 
        .prepareCall("{CALL myProc1(" + timOutOnCallableStmt +  
        		", ?) ON TABLE MyTable}");
    callableStmt.registerOutParameter(1, Types.INTEGER);

    callableStmt.setQueryTimeout(timOutOnCallableStmt);
    addExpectedException(new int[] { 1 }, new int[] { 1, 2 },
        SQLException.class);
    try {
      callableStmt.execute();
      fail("This test should have thrown exception "
          + "due to query timeout (exception state XCL52)");
    } catch (SQLException se) {
      if (!se.getSQLState().equals("XCL52")) {
        throw se;
      }
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2, 3 },
          SQLException.class);
    }
    
// process the result
//    boolean moreResults = true;
//    ResultSet rs = null;
//    do {
//      rs = callableStmt.getResultSet();
//      while (rs.next());
//      rs.close();
//      moreResults = callableStmt.getMoreResults();
//    } while (moreResults);
  }
  
  /**
   * @param timOutOnCallableStmt - time out (in seconds) set on the outer 
   * callable stmt
   */
  public static void myProc2(int timOutOnCallableStmt, 
      int[] count, ResultSet[] resultSet1,
      ResultSet[] resultSet2,
      ProcedureExecutionContext ctx) 
          throws SQLException, InterruptedException {
    Connection conn = ctx.getConnection();
//    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    
    Statement stmt = conn.createStatement();
    
    // make sure that by default the timeout for statement in proc is same as 
    // that of outer callable statement's time out
    assertEquals(timOutOnCallableStmt, stmt.getQueryTimeout());
    
    // timeout cannot be more than the outer callable stmt's timeout, so the 
    // time out will be set to outer statement's timeout
    stmt.setQueryTimeout(timOutOnCallableStmt + 1);
    SQLWarning sw = stmt.getWarnings();
    if (sw != null) {
      if (!sw.getSQLState().equals("01509")) {
//        fail("Expected warning state 01509. Received warning:" + sw.getSQLState());
        throw sw;
      } // else ignore
    }
    else {
      fail("This test should have thrown a warning(01509) as query time out "
          + "for statement in stored procedure can not be more "
          + "than outer callable statement's time out");
    }
    assertEquals(timOutOnCallableStmt, stmt.getQueryTimeout());

    // set different(lesser) timeout for stmt in sproc
    stmt.setQueryTimeout(1); 
    assertEquals(1, stmt.getQueryTimeout());
    
    stmt.execute("select * from mytable");
    resultSet1[0] = stmt.getResultSet();
    
    Statement stmt3 = conn.createStatement();
    stmt3 .execute("select count(*) from mytable");
    stmt3.getResultSet().next();
    Integer cnt = stmt3.getResultSet().getInt(1);
    count[0] = cnt;
    
    Statement stmt2 = conn.createStatement();
    stmt2.execute("select count(*) from mytable");
    resultSet2[0] = stmt2.getResultSet();
  }
  
 
  // set time out on the outer callabale statement and set lesser time out
  // inner statements in sproc.  The statements in in sproc should
  // time out
  public void testQueryTimeOutStoredProc_2() throws Exception {
    startVMs(1, 2);
    Connection cxn = TestUtil.getConnection() ;
    Statement stmt = cxn.createStatement();
    
    // create a procedure
    stmt.execute("CREATE PROCEDURE myProc2 " 
        + "(IN timOutOnCallableStmt INTEGER, "
        + " OUT count INTEGER)" 
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA " + "DYNAMIC RESULT SETS 2 " + "EXTERNAL NAME '"
        + QueryTimeOutDUnit.class.getName() + ".myProc2'");
    
    stmt.execute("create table MyTable(x int, y int) partition by column(x)");
    stmt.execute("insert into MyTable values (1, 1), (2, 2), (3, 3), " +
            "(4, 4), (5, 5), (6, 6), (7, 7)");
    
    // set a large timeout on outer callable stmt so that it does not timeout
    int timOutOnCallableStmt = 10; // seconds
    CallableStatement callableStmt = cxn
         // first param to myProc2- timeOut to be set on callable stmt
        // CALL myProc2(10, ?) ON TABLE MyTable 
        .prepareCall("{CALL myProc2(" + timOutOnCallableStmt +  
                ", ?) ON TABLE MyTable}");
    callableStmt.registerOutParameter(1, Types.INTEGER);

    callableStmt.setQueryTimeout(timOutOnCallableStmt);
    
    // suspend the execution to allow query get timed out
    SerializableRunnable csr = new SerializableRunnable(
        "_testTimeOut_") {
      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              @Override
              public void onGetNextRowCoreOfBulkTableScan(
                  com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                try {
                  Thread.sleep(1500);
                } catch (InterruptedException e) {
                }
              }
            });
      }
    };
    // clientExecute(1, csr);
    serverExecute(1, csr);
    serverExecute(2, csr);
    
    addExpectedException(new int[] { 1 }, new int[] { 1, 2 },
        SQLException.class);
    try {
      callableStmt.execute();
      fail("This test should have thrown exception "
          + "due to query timeout (exception state XCL52)");
    } catch (SQLException se) {
      if (!se.getSQLState().equals("XCL52")) {
        throw se;
      }
      assertEquals(10, callableStmt.getQueryTimeout());
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          SQLException.class);
    }
  }
  
    
  final String PROP_FILE_NAME = "gfxd-querytimeout.properties";

  private File createPropertyFile() throws Exception {
    final File file = new File(PROP_FILE_NAME);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      Properties props = new Properties();
      props.setProperty("gemfirexd.query-timeout", "2");
      props.store(fos,
          "-- gfxd properties file for testQueryTimeOutThruPropertiesFile");
      return file;
    }
  }

  // gemfirexd.query-timeout setting in properties file should apply to all
  // statements
  public void testQueryTimeOutThruPropertiesFile() throws Exception {
    // create a properties file and set it so that gfxd read props thru it
    File propFile = createPropertyFile();
    try {
      Properties p1 = new Properties();
      p1.setProperty(com.pivotal.gemfirexd.Property.PROPERTIES_FILE,
          propFile.getAbsolutePath());
      startVMs(1, 2, 0, null, p1);

      Connection cxn = TestUtil.getConnection();
      final Statement stmt = cxn.createStatement();

      stmt.execute("create table MyTable(x int, y int) partition by column(x)");
      final PreparedStatement pstmt1 = cxn
          .prepareStatement("insert into MyTable values " + "(?, ?)");
      for (int i = 1; i <= 10; i++) {
        pstmt1.setInt(1, i);
        pstmt1.setInt(2, i);
        pstmt1.execute();
      }
      final PreparedStatement pstmt2 = cxn
          .prepareStatement("select * from MyTable where x > ?");
      pstmt2.setInt(1, 0);

      // make sure that the query time out set in properties
      // file is applied to the statement
      assertEquals(2, pstmt2.getQueryTimeout());

      SerializableRunnable csr2 = new SerializableRunnable(
          "testQueryTimeOutThruPropertiesFile") {
        @Override
        public void run() {
          GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter() {
                @Override
                public void onGetNextRowCoreOfBulkTableScan(
                    com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
                  try {
                    getLogWriter().info(
                        "onGetNextRowCoreOfBulkTableScan called");
                    Thread.sleep(3000);
                  } catch (InterruptedException e) {
                  }
                }
              });
        }
      };

      // delay the execution so that the query times out
      clientExecute(1, csr2);
      serverExecute(1, csr2);
      serverExecute(2, csr2);

      addExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          SQLException.class);
      try {
        ResultSet rs = pstmt2.executeQuery();
        while (rs.next()) {
          System.out.println(rs.getInt(1));
          System.out.println(rs.getInt(2));
        }
        fail("This test should have thrown exception "
            + "due to query cancellation (exception state XCL52)");
      } catch (SQLException se) {
        if (!se.getSQLState().equals("XCL52")) {
          throw se;
        } // else ignore
      } finally {
        removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
            SQLException.class);
      }
    } finally {
      //noinspection ResultOfMethodCallIgnored
      propFile.delete();
    }
  }
}
