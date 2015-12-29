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
package com.pivotal.gemfirexd.jdbc;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.dataawareprocedure.MergeSortDUnit;
import com.pivotal.gemfirexd.dataawareprocedure.MergeSortDUnit.MergeSortProcessor;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

import junit.framework.AssertionFailedError;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * This junit test is used to test procedure
 * @author yjing
 *
 */

public class Procedure2Test extends JdbcTestBase {

  private List<Statement> statements;

  public Procedure2Test(String name) {
    super(name);    
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(Procedure2Test.class));
  }

  // TESTS

  public static void selectOnlyIn(int cid1, int cid2, int sid, int tid, 
      ResultSet[] rs, ResultSet[] rs2, ResultSet[] rs3, ProcedureExecutionContext context) 
      throws SQLException {
    getLogger().info("method executed");
  }

  public static void selectInAsWellASOut(int cid1, int cid2, int sid, int tid, int[] data, 
      ResultSet[] rs, ResultSet[] rs2, ResultSet[] rs3, ProcedureExecutionContext context) 
      throws SQLException {
    getLogger().info("method executed");
  }
  
  public void testProperExceptionInvalidProcClassName() throws SQLException {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    try {
      s.execute("CREATE PROCEDURE MergeSort () "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA "
          + "READS SQL DATA DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
          + MergeSortDUnit.class.getName() + "garbage.mergeSort' ");

      s.execute("CREATE ALIAS MergeSortProcessor FOR '"
          + MergeSortProcessor.class.getName() + "garbage'");

      s.execute("CALL MergeSort() WITH RESULT PROCESSOR MergeSortProcessor");
      fail("should not proceed");
    } catch (SQLException e) {
      assertEquals("42X51", e.getSQLState());
    }
  }
  
  public void testProperExceptionInvalidProcessorClassName() throws SQLException {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    try {
      s.execute("CREATE PROCEDURE MergeSort () "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA "
          + "READS SQL DATA DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
          + MergeSortDUnit.class.getName() + ".mergeSort' ");

      s.execute("CREATE ALIAS MergeSortProcessor FOR '"
          + MergeSortProcessor.class.getName() + "garbage'");

      s.execute("CALL MergeSort() WITH RESULT PROCESSOR MergeSortProcessor");
      fail("should not proceed");
    } catch (SQLException e) {
      assertEquals("42X51", e.getSQLState());
    }
  }
  
  public void testBug42875_OUT() throws SQLException {
    String showGfxdPortfolio = "create procedure trade.showGfxdPortfolio(IN DP1 Integer, " +
    "IN DP2 Integer, IN DP3 Integer, IN DP4 Integer, OUT DP5 Integer) " +
    "PARAMETER STYLE JAVA " +
    "LANGUAGE JAVA " +
    "READS SQL DATA " +
    "DYNAMIC RESULT SETS 3 " +
    "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Procedure2Test.selectInAsWellASOut'";
    
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s.execute("create table trade.portfolio " +
        "(cid int not null, tid int not null)");
    
    s.execute(showGfxdPortfolio);
    
    CallableStatement cs = conn.prepareCall(" call trade.showGfxdPortfolio(?, ?, ?, ?, ?) " +
                "ON Table trade.portfolio where cid > 258 and cid< 1113 and tid=2");
    cs.setInt(1, 1);
    cs.setInt(2, 2);
    cs.setInt(3, 3);
    cs.setInt(4, 4);
    cs.registerOutParameter(5, Types.INTEGER);
    
    cs.execute();
  }

  public void testBug42875_onlyIN() throws SQLException {
    String showGfxdPortfolio = "create procedure trade.showGfxdPortfolio(IN DP1 Integer, " +
    "IN DP2 Integer, IN DP3 Integer, IN DP4 Integer) " +
    "PARAMETER STYLE JAVA " +
    "LANGUAGE JAVA " +
    "READS SQL DATA " +
    "DYNAMIC RESULT SETS 3 " +
    "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Procedure2Test.selectOnlyIn'";
    
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s.execute("create table trade.portfolio " +
        "(cid int not null, tid int not null)");
    
    s.execute(showGfxdPortfolio);
    
    CallableStatement cs = conn.prepareCall(" call trade.showGfxdPortfolio(?, ?, ?, ?) " +
                "ON Table trade.portfolio where cid > 258 and cid< 1113 and tid=2");
    cs.setInt(1, 1);
    cs.setInt(2, 2);
    cs.setInt(3, 3);
    cs.setInt(4, 4);

    
    cs.execute();
  }

  static int tidGot = -1;
  static BigDecimal subtotalGot = new BigDecimal(-1);
  
  public static void updateGfxdPortfolioByCidRange(BigDecimal subTotal, int tid, 
      ProcedureExecutionContext context) throws SQLException {
    tidGot = tid;
    subtotalGot = subTotal;
  }

  public void testBug42963_NPEInBCMethod() throws SQLException {
    String updateGfxdPortfolioByCidRange = "create procedure trade.updateGfxdPortfolioByCidRange(IN DP1 DECIMAL(30, 20), " +
    "IN DP2 Integer) " +
    "PARAMETER STYLE JAVA " +
    "LANGUAGE JAVA " +
    "Modifies SQL DATA " +
    "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Procedure2Test.updateGfxdPortfolioByCidRange'";
    
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s.execute("create table trade.portfolio " +
        "(cid int not null, tid int not null)");
    
    s.execute(updateGfxdPortfolioByCidRange);
    
    CallableStatement cs = conn.prepareCall("{call trade.updateGfxdPortfolioByCidRange(?, ?) " +
    		"ON Table trade.portfolio where cid > 474 and cid< 2055 and tid=17}");
    cs.setInt(1, 1);
    cs.setInt(2, 2);
    cs.execute();
    assertEquals(2, tidGot);
    //assertTrue(subtotalGot.equals(new BigDecimal(1)));
  }
  
  /**
   * Tests that <code>PreparedStatement.executeQuery()</code> fails
   * when no result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithNoDynamicResultSets_prepared()
  throws SQLException {
    setup();
    PreparedStatement ps = prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    ps.setInt(1, 0);
    try {
      ps.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }
  }

  /**
   * Tests that <code>PreparedStatement.executeQuery()</code>
   * succeeds when one result set is returned from a stored
   * procedure.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithOneDynamicResultSet_prepared()
  throws SQLException {
    setup();
    PreparedStatement ps = prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    ps.setInt(1, 1);
    ResultSet rs = ps.executeQuery();
    assertNotNull("executeQuery() returned null.", rs);
    assertSame(ps, rs.getStatement());
    JDBC.assertDrainResultsHasData(rs);

  }

  /**
   * Tests that <code>PreparedStatement.executeQuery()</code> fails
   * when multiple result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithMoreThanOneDynamicResultSet_prepared()
  throws SQLException {
    setup();
    PreparedStatement ps = prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    ps.setInt(1, 2);
    try {
      ps.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertMultipleResultsFromExecuteQuery(sqle);
    }
  }

  /**
   * Tests that <code>PreparedStatement.executeUpdate()</code>
   * succeeds when no result sets are returned.
   *
   * <p>Currently, this test fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  public void testExecuteUpdateWithNoDynamicResultSets_prepared()
  throws SQLException {
    setup();
    PreparedStatement ps = prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    ps.setInt(1, 0);
    //   assertUpdateCount(ps, 0);
    JDBC.assertNoMoreResults(ps);
  }

  /**
   * Tests that <code>PreparedStatement.executeUpdate()</code> fails
   * when a result set is returned from a stored procedure.
   *
   * <p>Currently, this test fails with
   * JCC. However, the corresponding tests for
   * <code>Statement</code> and <code>CallableStatement</code>
   * succeed. Strange...
   *
   * @exception SQLException if a database error occurs
   */
  public void testExecuteUpdateWithOneDynamicResultSet_prepared()
  throws SQLException {
    setup();
    PreparedStatement ps = prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    ps.setInt(1, 1);
    try {
      ps.executeUpdate();
      fail("executeUpdate() didn't fail.");
    }
    catch (SQLException sqle) {
      assertResultsFromExecuteUpdate(sqle);
    }
  }

  /**
   * Tests that <code>CallableStatement.executeQuery()</code> fails
   * when no result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithNoDynamicResultSets_callable()
  throws SQLException {
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    cs.setInt(1, 0);
    try {
      cs.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }
  }

  /**
   * Tests that <code>CallableStatement.executeQuery()</code>
   * succeeds when one result set is returned from a stored
   * procedure.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithOneDynamicResultSet_callable()
  throws SQLException {
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    cs.setInt(1, 1);
    ResultSet rs = cs.executeQuery();
    assertNotNull("executeQuery() returned null.", rs);
    assertSame(cs, rs.getStatement());
    JDBC.assertDrainResultsHasData(rs);
  }

  
  /**
   * Tests that <code>CallableStatement.executeQuery()</code> fails
   * when multiple result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithMoreThanOneDynamicResultSet_callable()
      throws SQLException {
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    cs.setInt(1, 2);        
    try {
      cs.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertMultipleResultsFromExecuteQuery(sqle);
    }
  }
  
//******************************************************
  public void testCreateProcedureResultProcessorAlias() throws SQLException {
    setup();
    Statement s = getConnection().createStatement();
    s.execute("CREATE ALIAS MergeSortProcessor FOR '"
        + com.pivotal.gemfirexd.dataawareprocedure.MergeSortDUnit.class
            .getName() + ".MergeSortProcessor'");
   /*
   s.execute("Create alias aaa FOR ddd");
   s.execute("Create alias aaa FOR BBB");
   */
  }

  public void testAlias() throws Exception {
    Statement s = getConnection().createStatement();
    s.execute("CREATE PROCEDURE MergeSort () "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
        + MergeSortDUnit.class.getName() + ".justanothermergeSort' ");

    // create custom processor
    s.execute("CREATE ALIAS MergeSortProcessor FOR '"
        + MergeSortProcessor.class.getName()+"'");
    /*
    "CALL GFXD.CreateResultProcessor('MergeSortProcessor', "
    + MergeSortDUnit.class.getName()
    + ".MergeSortProcessor)");
    */

    CallableStatement cs = prepareCall("CALL MergeSort() "
        + "WITH RESULT PROCESSOR MergeSortProcessor");
    cs.execute();
    cs.getResultSet();
  }
  // !!!:ezoerner:20090825 
  // fails with:
  // Caused by: com.gemstone.gemfire.cache.execute.FunctionException: No target node found for KEY = 2
  //Neeraj::20100317
  // This test is invalid because optimizedForWrite has been made as true which
  // means procedure will be routed to a node irrespective of whether data is there
  // or not in the system.
  public void _testDataAwareProcedureWithOutgoingResultSetsWithoutNodesToExecute()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON TABLE "
        + "EMP.PARTITIONEMPTYTESTTABLE WHERE ID in (?, ?, ?)");
    int number=2;
    cs.setInt(1, number);  
    cs.setInt(2, 2);
    cs.setInt(3, 3);
    cs.setInt(4, 4);
    
    cs.execute();
        
    do {          
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      if(rowCount!=0 || rs.next()) {
         fail("The result set is supposed to be empty!");
      }
    } while (cs.getMoreResults());           
  }
  
  /***
   * Note test for no out parameters
   */
  
  /***
   * Note test for one of parameters is null
   * 
   */
  
  /***
   * normal test for out parameter
   */
  
  /**
   * Tests that <code>CallableStatement.executeUpdate()</code> fails
   * when a result set is returned from a stored procedure.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteUpdateWithOneDynamicResultSet_callable()
  throws SQLException {
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
    cs.setInt(1, 1);
    try {
      cs.executeUpdate();
      fail("executeUpdate() didn't fail.");
    }
    catch (SQLException sqle) {
      assertResultsFromExecuteUpdate(sqle);
    }
  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeQuery()</code> are correctly rolled back when
   * <code>Connection.rollback()</code> is called.
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWithExecuteQuery() throws SQLException {
    setup();
    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(1)");
    rs.close();
    // rollback();

    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeUpdate()</code> are correctly rolled back when
   * <code>Connection.rollback()</code> is called.
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWithExecuteUpdate() throws SQLException {
    setup();
    Statement stmt = createStatement();
    stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(0)");
    //  rollback();

    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeQuery()</code> are correctly rolled back when the
   * query fails because the number of returned result sets is zero.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteQueryReturnsNothing()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    Statement stmt = createStatement();
    try {
      stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(0)");
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }

    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeQuery()</code> are correctly rolled back when the
   * query fails because the number of returned result sets is more
   * than one.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteQueryReturnsTooMuch()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    Statement stmt = createStatement();
    try {
      stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(2)");
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertMultipleResultsFromExecuteQuery(sqle);
    }
    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeUpdate()</code> are correctly rolled back when the
   * query fails because the stored procedure returned a result set.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteUpdateReturnsResults()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    Statement stmt = createStatement();
    try {
      stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(1)");
      fail("executeUpdate() didn't fail.");
    }
    catch (SQLException sqle) {
      assertResultsFromExecuteUpdate(sqle);
    }
    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeQuery()</code> are correctly rolled back when the
   * query fails because the number of returned result sets is zero.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteQueryReturnsNothing_prepared()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    PreparedStatement ps = prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
    ps.setInt(1, 0);
    try {
      ps.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }
    Statement stmt = createStatement();
    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeQuery()</code> are correctly rolled back when the
   * query fails because the number of returned result sets is more
   * than one.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteQueryReturnsTooMuch_prepared()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    PreparedStatement ps = prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
    ps.setInt(1, 2);
    try {
      ps.executeQuery();
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertMultipleResultsFromExecuteQuery(sqle);
    }
    Statement stmt = createStatement();
    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
  }

  /**
   * Tests that the effects of executing a stored procedure with
   * <code>executeUpdate()</code> are correctly rolled back when the
   * query fails because the stored procedure returned a result set.
   *
   * <p> This test case fails with JCC.
   *
   * @exception SQLException if a database error occurs
   */
  // !!!:ezoerner:20090825 
  // this test is not yet working
  public void _testRollbackStoredProcWhenExecuteUpdateReturnsResults_prepared()
      throws SQLException {
    Connection conn = getConnection();
    conn.setAutoCommit(true);
    PreparedStatement ps = prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
    ps.setInt(1, 1);
    try {
      ps.executeUpdate();
      fail("executeUpdate() didn't fail.");
    }
    catch (SQLException sqle) {
      assertResultsFromExecuteUpdate(sqle);
    }
    Statement stmt = createStatement();
    // Expect Side effects from stored procedure to be rolled back.
    JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));

  }

  /**
   * Tests that closed result sets are not returned when calling
   * <code>executeQuery()</code>.
   * @exception SQLException if a database error occurs
   */
  public void testClosedDynamicResultSetsFromExecuteQuery() throws SQLException {
    setup();
    Statement stmt = createStatement();
    try {
      stmt.executeQuery("CALL RETRIEVE_CLOSED_RESULT()");
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }
  }

  private static int delInvokeCnt = 0;

  public static void procreadssql(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {
    delInvokeCnt++;
  }

  public void testDeleteTriggerWhichHasProc() throws SQLException {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table t2(col1 int not null, col2 int not null primary key)");
    s.execute("CREATE PROCEDURE proc_reads_sql () "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
        + Procedure2Test.class.getName() + ".procreadssql' ");

    try {
      s.execute("create trigger before_row_trig_reads_sql "
          + "no cascade BEFORE delete on t2 for each ROW call proc_reads_sql()");
      s.execute("insert into t2 values(1, 11), (2, 22)");
      // --- delete 2 rows. check that trigger is fired - procedure should be
      // called twice
      s.execute("delete from t2");
      // checkAndResetSelectRowsCount(2);
      // --- check delete is successful
      ResultSet rs = s.executeQuery("select * from t2");
      while (rs.next()) {
        fail("table should have been empty");
      }
    } finally {
      s.execute("drop trigger before_row_trig_reads_sql");
    }
    s.execute("insert into t2 values(1, 11), (2, 22)");
    ResultSet rs = s.executeQuery("select * from t2");
    int cnt = 0;
    while (rs.next()) {
      cnt++;
    }
    assertEquals(2, cnt);
    assertEquals(2, delInvokeCnt);
  }
  
  
  /**
   * Tests that closed result sets are ignored when calling
   * <code>executeUpdate()</code>.
   * @exception SQLException if a database error occurs
   */
  public void testClosedDynamicResultSetsFromExecuteUpdate()
  throws SQLException {
    setup();
    Statement stmt = createStatement();
    stmt.executeUpdate("CALL RETRIEVE_CLOSED_RESULT()");
    JDBC.assertNoMoreResults(stmt);
  }

  // UTILITY METHODS

  /**
   * Raises an exception if the exception is not caused by
   * <code>executeQuery()</code> returning no result set.
   *
   * @param sqle a <code>SQLException</code> value
   */
  private void assertNoResultSetFromExecuteQuery(SQLException sqle) {
    //  if (usingDB2Client()) {
    //     assertNull("Unexpected SQL state.", sqle.getSQLState());
    // } else {
    assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
    // }
  }

  /**
   * Raises an exception if the exception is not caused by
   * <code>executeQuery()</code> returning multiple result sets.
   *
   * @param sqle a <code>SQLException</code> value
   */
  private void assertMultipleResultsFromExecuteQuery(SQLException sqle) {
    // if (usingDB2Client()) {
    //     assertNull("Unexpected SQL state.", sqle.getSQLState());
    // } else {
    assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
    // }
  }

  /**
   * Raises an exception if the exception is not caused by
   * <code>executeUpdate()</code> returning result sets.
   *
   * @param sqle a <code>SQLException</code> value
   */
  private void assertResultsFromExecuteUpdate(SQLException sqle) {
    // if (usingDB2Client()) {
    //     assertNull("Unexpected SQL state.", sqle.getSQLState());
    // } else {
    assertSQLState("Unexpected SQL state.", "X0Y79", sqle);
    // }

  }

  // SETUP

  /**
   * Creates the test suite and wraps it in a <code>TestSetup</code>
   * instance which sets up and tears down the test environment.
   * @return test suite
   */
  private static void setup() throws SQLException {

    Connection conn = getConnection();
    //conn.setAutoCommit(false);
    Statement s = conn.createStatement();
    
    
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, PRIMARY KEY (SECONDID, THIRDID))");
     //   + " PARTITION BY COLUMN (ID)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (2, 2, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (3, 3, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (4, 4, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (5, 5, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (2, 2, '2') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE VALUES (2, 2, '4') ");
    
    s.execute("SELECT * FROM EMP.PARTITIONTESTTABLE " +
    		"WHERE (SECONDID=10 AND THIRDID='20') OR (SECONDID=10 AND THIRDID='20')");

    s.execute("create table EMP.PARTITIONTESTTABLE1 (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, " +
        		"PRIMARY KEY (SECONDID, THIRDID)) PARTITION BY COLUMN (ID)");
     //   + " PARTITION BY COLUMN (ID)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (3, 3, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (4, 4, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (5, 5, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '2') ");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '4') ");

    s.execute("create table EMP.PARTITIONRANGETESTTABLE (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, PRIMARY KEY (ID)) PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 10, VALUES BETWEEN 10 and 20, "
            + "VALUES BETWEEN 20 and 30, VALUES BETWEEN 30 and 40 )");
     //   + " PARTITION BY COLUMN (ID)");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (1, 2, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (10, 3, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (15, 4, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (21, 5, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (30, 2, '2') ");
    s.execute("INSERT INTO EMP.PARTITIONRANGETESTTABLE VALUES (35, 2, '4') ");

    s.execute("create table EMP.PARTITIONLISTTESTTABLE (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, PRIMARY KEY (ID)) PARTITION BY LIST ( ID )"
        + " ( VALUES (0, 5) , VALUES (10, 15), "
        + "VALUES (20, 23), VALUES(25, 30), VALUES(31, 100))");
     //   + " PARTITION BY COLUMN (ID)");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (0, 2, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (5, 3, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (15, 4, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (30, 5, '3') ");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (31, 2, '2') ");
    s.execute("INSERT INTO EMP.PARTITIONLISTTESTTABLE VALUES (25, 2, '4') ");

    s.execute("create table EMP.PARTITIONEMPTYTESTTABLE (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, PRIMARY KEY (ID)) PARTITION BY COLUMN ( ID )");
    /**
     * Creates the tables and the stored procedures used in the test cases.
     * 
     * @exception SQLException
     *                    if a database error occurs
     */

    
    for (int i = 0; i < PROCEDURES.length; i++) {
      s.execute(PROCEDURES[i]);
    }
    for (int i = 0; i < TABLES.length; i++) {
      s.execute(TABLES[i][1]);
    }
    conn.close();

  }

  /**
   * Procedures that should be created before the tests are run and dropped when
   * the tests have finished. First element in each row is the name of the
   * procedure, second element is SQL which creates it.
   */
  private static final String[] PROCEDURES = {

      "CREATE PROCEDURE RETRIEVE_DYNAMIC_RESULTS(number INT) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".retrieveDynamicResults' "
          + "DYNAMIC RESULT SETS 4",
          
      "CREATE PROCEDURE RETRIEVE_OUTGOING_RESULTS(number INT) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".retrieveDynamicResultsWithOutgoingResultSet'"
          + "DYNAMIC RESULT SETS 4",    

      "CREATE PROCEDURE PROCEDURE_INOUT_PARAMETERS(number INT, INOUT name VARCHAR(25), OUT total INT) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".procedureWithInAndOutParameters'"
          + "DYNAMIC RESULT SETS 4",        
        
      "CREATE PROCEDURE PROCEDURE_WITHOUT_RESULTSET(number INT, INOUT name VARCHAR(20) ) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".procedureWithoutDynamicResultSets'",       
          
      "CREATE PROCEDURE PROCEDURE_WITHOUT_RESULTSET_OUTPARAMETER(number INT, name VARCHAR(20) ) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".procedureWithoutDynamicResultSetsAndOutParameters'",     
           
      "CREATE PROCEDURE RETRIEVE_CLOSED_RESULT() LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".retrieveClosedResult' "
          + "DYNAMIC RESULT SETS 1",

      "CREATE PROCEDURE RETRIEVE_EXTERNAL_RESULT("
          + "DBNAME VARCHAR(128), DBUSER VARCHAR(128), DBPWD VARCHAR(128)) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".retrieveExternalResult' "
          + "DYNAMIC RESULT SETS 1",

      "CREATE PROCEDURE PROC_WITH_SIDE_EFFECTS(ret INT) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".procWithSideEffects' "
          + "DYNAMIC RESULT SETS 2",

      "CREATE PROCEDURE NESTED_RESULT_SETS(proctext VARCHAR(128)) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + Procedure2Test.class.getName() + ".nestedDynamicResultSets' "
          + "DYNAMIC RESULT SETS 6"

  };

  /**
   * Tables that should be created before the tests are run and
   * dropped when the tests have finished. The tables will be
   * cleared before each test case is run. First element in each row
   * is the name of the table, second element is the SQL text which
   * creates it.
   */
  private static final String[][] TABLES = {
  // SIMPLE_TABLE is used by PROC_WITH_SIDE_EFFECTS
  { "SIMPLE_TABLE", "CREATE TABLE SIMPLE_TABLE (id INT)" }, };

  // PROCEDURES

  /**
   * Stored procedure which returns 0, 1, 2, 3 or 4 <code>ResultSet</code>s.
   *
   * @param number the number of <code>ResultSet</code>s to return
   * @param rs1 first <code>ResultSet</code>
   * @param rs2 second <code>ResultSet</code>
   * @param rs3 third <code>ResultSet</code>
   * @param rs4 fourth <code>ResultSet</code>
   * @exception SQLException if a database error occurs
   */
  public static void retrieveDynamicResults(int number, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4) throws SQLException {
      Connection c = DriverManager.getConnection("jdbc:default:connection");

    if (number > 0) {
      rs1[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 1) {
      rs2[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 2) {
      rs3[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 3) {
      rs4[0] = c.createStatement().executeQuery("VALUES(1)");
    }
  }
  
  
  public static void procedureWithInAndOutParameters(int number,
      String[] name,
      int[]    total,
      ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4) throws SQLException {
      Connection c = DriverManager.getConnection("jdbc:default:connection");
    
    name[0]=name[0]+"Modified";
    total[0]=number;
    
    if (number > 0) {
      rs1[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 1) {
      rs2[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 2) {
      rs3[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 3) {
      rs4[0] = c.createStatement().executeQuery("VALUES(1)");
    }
  }
  
  public static void procedureWithoutDynamicResultSets(int number,
      String[] name) throws SQLException {        
    name[0]=name[0]+number;
        
  }
  
  public static void procedureWithoutDynamicResultSetsAndOutParameters(int number,
      String name) throws SQLException {
      
  }
  /**
   * Stored procedure which returns 0, 1, 2, 3 or 4 <code>ResultSet</code>s.
   *
   * @param number the number of <code>ResultSet</code>s to return
   * @param rs1 first <code>ResultSet</code>
   * @param rs2 second <code>ResultSet</code>
   * @param rs3 third <code>ResultSet</code>
   * @param rs4 fourth <code>ResultSet</code>
   * @exception SQLException if a database error occurs
   */
  public static void retrieveDynamicResultsWithOutgoingResultSet(int number, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4, ProcedureExecutionContext pec) 
      throws SQLException {
      Connection c = DriverManager.getConnection("jdbc:default:connection");

    if (number > 0) {
      rs1[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 1) {
      OutgoingResultSet ors=pec.getOutgoingResultSet(2);
      for(int i=0; i<10; ++i) {
        List<Object> row=new ArrayList<Object>();
        row.add(new Integer(i));
        row.add("String"+ i);        
        ors.addRow(row);
      }
      ors.endResults();      
    }
    if (number > 2) {
      rs3[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (number > 3) {
      rs4[0] = c.createStatement().executeQuery("VALUES(1)");
    }
  }

  /**
   * Stored procedure which produces a closed result set.
   *
   * @param closed holder for the closed result set
   * @exception SQLException if a database error occurs
   */
  public static void retrieveClosedResult(ResultSet[] closed)
      throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    closed[0] = c.createStatement().executeQuery("VALUES(1)");
    closed[0].close();
  }

  /**
   * Stored procedure which produces a result set in another
   * connection.
   *
   * @param external result set from another connection
   * @exception SQLException if a database error occurs
   */
  public static void retrieveExternalResult(String dbName, String user,
      String password, ResultSet[] external) throws SQLException {
    // Use a server-side connection to the same database.
    String url = "jdbc:derby:" + dbName;

    Connection conn = DriverManager.getConnection(url, user, password);

    external[0] = conn.createStatement().executeQuery("VALUES(1)");
  }

  /**
   * Stored procedure which inserts a row into SIMPLE_TABLE and
   * optionally returns result sets.
   *
   * @param returnResults if one, return one result set; if greater
   * than one, return two result sets; otherwise, return no result
   * set
   * @param rs1 first result set to return
   * @param rs2 second result set to return
   * @exception SQLException if a database error occurs
   */
  public static void procWithSideEffects(int returnResults, ResultSet[] rs1,
      ResultSet[] rs2) throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    Statement stmt = c.createStatement();
    stmt.executeUpdate("INSERT INTO SIMPLE_TABLE VALUES (42)");
    if (returnResults > 0) {
      rs1[0] = c.createStatement().executeQuery("VALUES(1)");
    }
    if (returnResults > 1) {
      rs2[0] = c.createStatement().executeQuery("VALUES(1)");
    }
  }

  /**
   * Method for a Java procedure that calls another procedure
   * and just passes on the dynamic results from that call.
   */
  public static void nestedDynamicResultSets(String procedureText,
      ResultSet[] rs1, ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
      ResultSet[] rs5, ResultSet[] rs6) throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:default:connection");

    CallableStatement cs = c.prepareCall("CALL " + procedureText);

    cs.execute();

    // Mix up the order of the result sets in the returned
    // parameters, ensures order is defined by creation
    // and not parameter order.
    rs6[0] = cs.getResultSet();
    if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
      return;
    rs3[0] = cs.getResultSet();
    if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
      return;
    rs4[0] = cs.getResultSet();
    if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
      return;
    rs2[0] = cs.getResultSet();
    if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
      return;
    rs1[0] = cs.getResultSet();
    if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
      return;
    rs5[0] = cs.getResultSet();

  }

  /**
   * Test various combinations of getMoreResults
   * 
   * @throws SQLException
   */
  // !!!:ezoerner:20090825 
  // fails with Caused by: java.lang.ClassCastException: com.pivotal.gemfirexd.internal.engine.locks.GfxdLockSet
  public void _testGetMoreResults() throws SQLException {

    Statement s = createStatement();

    s.executeUpdate("create table MRS.FIVERS(i integer)");
    PreparedStatement ps = prepareStatement("insert into MRS.FIVERS values (?)");
    for (int i = 1; i <= 20; i++) {
      ps.setInt(1, i);
      ps.executeUpdate();
    }

    // create a procedure that returns 5 result sets.

    s.executeUpdate("create procedure MRS.FIVEJP() " +
    		"parameter style JAVA READS SQL DATA dynamic result sets 5 " +
    		"language java " +
    		"external name 'org.apache.derbyTesting.functionTests.util.Procedure2Test.fivejp'");

    CallableStatement cs = prepareCall("CALL MRS.FIVEJP()");
    ResultSet[] allRS = new ResultSet[5];

    defaultGetMoreResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);
    closeCurrentGetMoreResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);
    keepCurrentGetMoreResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);
    mixedGetMoreResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);
    checkExecuteClosesResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);
    checkCSCloseClosesResults(cs, allRS);
    java.util.Arrays.fill(allRS, null);

    // a procedure that calls another procedure that returns
    // dynamic result sets, see if the result sets are handled
    // correctly through the nesting.
    CallableStatement nestedCs = prepareCall("CALL NESTED_RESULT_SETS('MRS.FIVEJP()')");
    defaultGetMoreResults(nestedCs, allRS);

  }

  /**
   * Check that CallableStatement.execute() closes results
   * @param cs
   * @param allRS
   * @throws SQLException
   */
  private void checkExecuteClosesResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    //Fetching result sets with 
    // getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.execute() closes them");          
    cs.execute();
    int pass = 0;
    do {

      allRS[pass++] = cs.getResultSet();
      assertSame(cs, allRS[pass - 1].getStatement());
      // expect everything to stay open.                        

    } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    //fetched all results
    // All should still be open.
    for (int i = 0; i < 5; i++)
      JDBC.assertDrainResults(allRS[i]);

    cs.execute();
    // all should be closed.
    for (int i = 0; i < 5; i++)
      JDBC.assertClosed(allRS[i]);
  }

  /**
   * Check that CallableStatement.close() closes results
   * @param cs
   * @param allRS
   * @throws SQLException
   */
  private void checkCSCloseClosesResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    cs.execute();
    int pass = 0;
    do {

      allRS[pass++] = cs.getResultSet();
      assertSame(cs, allRS[pass - 1].getStatement());
      // expect everything to stay open.                        

    } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    //fetched all results
    // All should still be open.
    for (int i = 0; i < 5; i++)
      JDBC.assertDrainResults(allRS[i]);

    cs.close();
    // all should be closed.
    for (int i = 0; i < 5; i++)
      JDBC.assertClosed(allRS[i]);
  }

  private void mixedGetMoreResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    //Fetching result sets with getMoreResults(<mixture>)"
    cs.execute();

    //first two with KEEP_CURRENT_RESULT"
    allRS[0] = cs.getResultSet();
    assertSame(cs, allRS[0].getStatement());
    boolean moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
    if (!moreRS)
      fail("FAIL - no second result set");
    allRS[1] = cs.getResultSet();
    assertSame(cs, allRS[1].getStatement());
    // two open
    allRS[0].next();
    assertEquals(2, allRS[0].getInt(1));
    allRS[1].next();
    assertEquals(3, allRS[1].getInt(1));

    //third with CLOSE_CURRENT_RESULT"
    moreRS = cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    if (!moreRS)
      fail("FAIL - no third result set");
    // first and third open
    allRS[2] = cs.getResultSet();
    assertSame(cs, allRS[2].getStatement());
    assertEquals(2, allRS[0].getInt(1));
    JDBC.assertClosed(allRS[1]);
    allRS[2].next();
    assertEquals(4, allRS[2].getInt(1));

    //fourth with KEEP_CURRENT_RESULT"
    moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
    if (!moreRS)
      fail("FAIL - no fourth result set");
    allRS[3] = cs.getResultSet();
    assertSame(cs, allRS[3].getStatement());
    allRS[3].next();
    // first, third and fourth open, second closed
    assertEquals(2, allRS[0].getInt(1));
    JDBC.assertClosed(allRS[1]);
    assertEquals(4, allRS[2].getInt(1));
    assertEquals(5, allRS[3].getInt(1));

    //fifth with CLOSE_ALL_RESULTS"
    moreRS = cs.getMoreResults(Statement.CLOSE_ALL_RESULTS);
    if (!moreRS)
      fail("FAIL - no fifth result set");
    allRS[4] = cs.getResultSet();
    assertSame(cs, allRS[4].getStatement());
    allRS[4].next();
    // only fifth open
    JDBC.assertClosed(allRS[0]);
    JDBC.assertClosed(allRS[1]);
    JDBC.assertClosed(allRS[2]);
    JDBC.assertClosed(allRS[3]);
    assertEquals(6, allRS[4].getInt(1));

    //no more results with with KEEP_CURRENT_RESULT"
    moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
    if (moreRS)
      fail("FAIL - too many result sets");
    // only fifth open
    JDBC.assertClosed(allRS[0]);
    JDBC.assertClosed(allRS[1]);
    JDBC.assertClosed(allRS[2]);
    JDBC.assertClosed(allRS[3]);
    assertEquals(6, allRS[4].getInt(1));

    allRS[4].close();
  }

  /**
   * Check getMoreResults(Statement.KEEP_CURRENT_RESULT)  
   * 
   * @param cs
   * @param allRS
   * @throws SQLException
   */
  private void keepCurrentGetMoreResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    cs.execute();

    for (int i = 0; i < 5; i++) {
      allRS[i] = cs.getResultSet();
      assertSame(cs, allRS[i].getStatement());
      allRS[i].next();
      assertEquals(2 + i, allRS[i].getInt(1));

      if (i < 4)
        assertTrue(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
      else
        assertFalse(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    }

    // resultSets should still be open
    for (int i = 0; i < 5; i++)
      JDBC.assertDrainResults(allRS[i]);
  }

  private void closeCurrentGetMoreResults(CallableStatement cs,
      ResultSet[] allRS) throws SQLException {
    cs.execute();

    for (int i = 0; i < 5; i++) {
      allRS[i] = cs.getResultSet();
      assertSame(cs, allRS[i].getStatement());
      allRS[i].next();
      assertEquals(2 + i, allRS[i].getInt(1));

      if (i < 4)
        assertTrue(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
      else
        assertFalse(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
    }

    // verify resultSets are closed
    for (int i = 0; i < 5; i++)
      JDBC.assertClosed(allRS[i]);
  }

  /**
   * Test default getMoreResults() closes result set.
   * @param cs
   * @param allRS
   * @throws SQLException
   */
  private void defaultGetMoreResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    // execute the procedure that returns 5 result sets and then use the various
    // options of getMoreResults().

    cs.execute();

    for (int i = 0; i < 5; i++) {
      allRS[i] = cs.getResultSet();
      assertSame(cs, allRS[i].getStatement());
      allRS[i].next();
      assertEquals(2 + i, allRS[i].getInt(1));

      if (i < 4)
        assertTrue(cs.getMoreResults());
      else
        assertFalse(cs.getMoreResults());
    }

    // verify resultSets are closed
    for (int i = 0; i < 5; i++)
      JDBC.assertClosed(allRS[i]);
  }

  public Statement createStatement() throws SQLException {
    Statement s = getConnection().createStatement();
    addStatement(s);
    return s;
  }

  /**
   * Add a statement into the list we will close
   * at tearDown.
   */
  private void addStatement(Statement s) {
    if (statements == null)
      statements = new ArrayList<Statement>();
    statements.add(s);
  }

  /**
   * Utility method to create a PreparedStatement using the connection
   * returned by getConnection.
   * The returned statement object will be closed automatically
   * at tearDown() but may be closed earlier by the test if required.
   * @return Statement object from
   * getConnection.prepareStatement(sql)
   * @throws SQLException
   */
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    PreparedStatement ps = getConnection().prepareStatement(sql);
    addStatement(ps);
    return ps;
  }

  /**
   * Utility method to create a CallableStatement using the connection
   * returned by getConnection.
   * The returned statement object will be closed automatically
   * at tearDown() but may be closed earlier by the test if required.
   * @return Statement object from
   * getConnection().prepareCall(sql)
   * @throws SQLException
   */
  public CallableStatement prepareCall(String sql) throws SQLException {
    CallableStatement cs = getConnection().prepareCall(sql);
    addStatement(cs);
    return cs;

  }

  /**
   * Assert that SQLState is as expected.  If the SQLState for
   * the top-level exception doesn't match, look for nested
   * exceptions and, if there are any, see if they have the
   * desired SQLState.
   *
   * @param message message to print on failure.
   * @param expected the expected SQLState.
   * @param exception the exception to check the SQLState of.
   */
  public static void assertSQLState(String message, String expected,
      SQLException exception) {
    // Make sure exception is not null. We want to separate between a
    // null-exception object, and a null-SQLState.
    assertNotNull("Exception cannot be null when asserting on SQLState",
        exception);

    try {
      String state = exception.getSQLState();

      if (state != null)
        assertTrue("The exception's SQL state must be five characters long",
            state.length() == 5);

      if (expected != null)
        assertTrue("The expected SQL state must be five characters long",
            expected.length() == 5);

      assertEquals(message, expected, state);
    }
    catch (AssertionFailedError e) {

      // Save the SQLException
      try {
        Method m = Throwable.class.getMethod("initCause",
            new Class[] { Throwable.class });
        m.invoke(e, new Object[] { exception });
      }
      catch (Throwable t) {
        // Some VMs don't support initCause(). It is OK if they fail.
      }

      //     if (usingDB2Client())
      //     {
      /* For JCC the error message is a series of tokens representing
       * different things like SQLSTATE, SQLCODE, nested SQL error
       * message, and nested SQL state.  Based on observation it
       * appears that the last token in the message is the SQLSTATE
       * of the nested exception, and it's preceded by a colon.
       * So using that (hopefully consistent?) rule, try to find
       * the target SQLSTATE.
       */
      //        String msg = exception.getMessage();
      //        if (!msg.substring(msg.lastIndexOf(":")+1)
      //            .trim().equals(expected))
      //        {
      //            throw e;
      //        }
      //    }
      //    else
      //    {
      // Check nested exceptions to see if any of them is
      // the one we're looking for.
      exception = exception.getNextException();
      if (exception != null)
        assertSQLState(message, expected, exception);
      else
        throw e;
      // }
    }
  }

  public static void assertErrorCode(String message, int expected,
      SQLException exception) {
    while (exception != null) {
      try {
        assertEquals(message, expected, exception.getErrorCode());
      }
      catch (AssertionFailedError e) {
        // check and see if our error code is in a chained exception
        exception = exception.getNextException();
      }
    }
  }

}
