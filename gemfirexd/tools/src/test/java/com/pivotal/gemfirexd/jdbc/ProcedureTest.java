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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.TestUtil;
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

public class ProcedureTest extends JdbcTestBase {

  private List statements;

  public ProcedureTest(String name) {
    super(name);    
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ProcedureTest.class));
  }

  // TESTS

  public static class ExampleObj implements Serializable {
    private static final long serialVersionUID = 1L;
    private int val;

    public ExampleObj() {
      
    }
    
    public void setValue(int val) {
      this.
      val = val;
    }
    
    public int getValue() {
      return this.val;
    }
  }
  
  public void testDocExample() throws Exception {
    
    Connection cxn = TestUtil.getConnection();//DriverManager.getConnection("jdbc:gemfirexd:");
    Statement stmt = cxn.createStatement();

    stmt.execute("create type ExampleObjType external name '"
        + ExampleObj.class.getName() + "' language java");
    
    stmt.execute("CREATE PROCEDURE myProc " + "(IN inParam1 VARCHAR(10), "
        + " OUT outParam2 INTEGER, " + " INOUT example ExampleObjType, OUT count INTEGER)" 
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA " + "DYNAMIC RESULT SETS 2 " + "EXTERNAL NAME '"
        + ProcedureTest.class.getName() + ".myProc'");
    
    stmt.execute("create table MyTable(x int not null, y int not null)");
    stmt.execute("insert into MyTable values (1, 10), (2, 20), (3, 30), (4, 40)");
    CallableStatement callableStmt = cxn
        .prepareCall("{CALL myProc('abc', ?, ?, ?) ON TABLE MyTable WHERE x BETWEEN 5 AND 10}");
    callableStmt.registerOutParameter(1, Types.INTEGER);
    callableStmt.registerOutParameter(2, Types.JAVA_OBJECT);
    callableStmt.registerOutParameter(3, Types.INTEGER);

    callableStmt.setObject(2, new ExampleObj());
    
    callableStmt.execute();
    
    int outParam2 = callableStmt.getInt(1);
    ExampleObj example = (ExampleObj)callableStmt.getObject(2);

    assert example.getValue() == 100;
    assert outParam2 == 200;
    
    ResultSet thisResultSet;
    boolean moreResults = true;
    int cnt = 0;
    int rowCount = 0;
    do {
      thisResultSet = callableStmt.getResultSet();
      int colCnt = thisResultSet.getMetaData().getColumnCount();
      if(cnt == 0) {
        System.out.println("Result Set 1 starts");
        while(thisResultSet.next()) {
          for(int i=1; i<colCnt+1; i++) {
            System.out.print(thisResultSet.getObject(i));
            if (i==1) {
              System.out.print(',');
            }
          }
          System.out.println();
          rowCount++;
        }
        System.out.println("ResultSet 1 ends\n");
        cnt++;
      }
      else {
        thisResultSet.next();
        System.out.println("ResultSet 2 starts");
        for(int i=1; i<colCnt+1; i++) {
          cnt = thisResultSet.getInt(1);
          System.out.print(cnt);
          System.out.println();
          break;
        }
        System.out.println("ResultSet 2 ends");
      }
      moreResults = callableStmt.getMoreResults();
    } while (moreResults);
    assert rowCount ==  cnt;
    assert rowCount == 4;
  }

  public static void myProc(String inParam1, int[] outParam2,
      ExampleObj[] example, int[] count, ResultSet[] resultSet1,
      ResultSet[] resultSet2,
      ProcedureExecutionContext ctx) throws SQLException {
    Connection conn = ctx.getConnection();
    ExampleObj obj = new ExampleObj();
    obj.setValue(100);
    example[0] = obj;
    
    outParam2[0] = 200;
    
    Statement stmt = conn.createStatement();
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

  public void testDocExample_1() throws Exception {

    Connection cxn = TestUtil.getConnection() ;//DriverManager.getConnection("jdbc:gemfirexd:");
    Statement stmt = cxn.createStatement();

    stmt.execute("create type ExampleObjType external name '"
        + ExampleObj.class.getName() + "' language java");
    
    stmt.execute("CREATE PROCEDURE myProc " + "(IN inParam1 ExampleObjType) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA " + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
        + ProcedureTest.class.getName() + ".myProc_1'");
    
    stmt.execute("create table MyTable(x int not null)");
    CallableStatement callableStmt = cxn
        .prepareCall("{CALL myProc(?) ON TABLE MyTable WHERE x BETWEEN 5 AND 10}");
    callableStmt.setObject(1, new ExampleObj());    
    callableStmt.execute();

    boolean moreResults = true;
    do {
      callableStmt.getResultSet();
      moreResults = callableStmt.getMoreResults();
    } while (moreResults);
  }

  public static void myProc_1(ExampleObj example, ResultSet[] resultSet1) {
  }
  
  public void testReadWriteModifiesSQLDataProcedure() throws Exception {
    String createProcNoSQLStr = "CREATE PROCEDURE SQL_DATA_OPTIONS_PROC_NO_SQL(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL EXTERNAL NAME '"
        + ProcedureTest.class.getName()
        + ".sqlDataOptionsProc' "
        + "DYNAMIC RESULT SETS 4";
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute(createProcNoSQLStr);
    try {
      s.execute("call SQL_DATA_OPTIONS_PROC_NO_SQL(1)");
      fail("should fail as the proc is actually executing sql data");
    } catch (SQLException e) {
      if (!"38001".equals(e.getSQLState())) {
        throw e;
      }
    }

    String createProcSQLStr = "CREATE PROCEDURE SQL_DATA_OPTIONS_PROC_SQL(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest.class.getName()
        + ".sqlDataOptionsProc' "
        + "DYNAMIC RESULT SETS 4";

    s.execute(createProcSQLStr);
    s.execute("call SQL_DATA_OPTIONS_PROC_SQL(1)");

    String createProcUpdateSQLStr = "CREATE PROCEDURE SQL_DATA_OPTIONS_PROC_UPDATE_SQL(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA EXTERNAL NAME '"
        + ProcedureTest.class.getName()
        + ".sqlDataOptionsProc' "
        + "DYNAMIC RESULT SETS 4";

    s.execute("create table app.t1(col1 int not null)");
    s.execute("insert into app.t1 values(1), (2)");
    s.execute(createProcUpdateSQLStr);
    try {
      s.execute("call SQL_DATA_OPTIONS_PROC_UPDATE_SQL(2)");
      fail("should fail as updating should not be allowed");
    } catch (SQLException e) {
      if (!"38002".equals(e.getSQLState())) {
        throw e;
      }
    }

    String createProcUpdateSQLStr_WITH_MODIFIES = "CREATE PROCEDURE SQL_DATA_OPTIONS_PROC_UPDATE_SQL_WITH_MODIFIES(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA EXTERNAL NAME '"
        + ProcedureTest.class.getName()
        + ".sqlDataOptionsProc' "
        + "DYNAMIC RESULT SETS 4";

    s.execute(createProcUpdateSQLStr_WITH_MODIFIES);

    s.execute("call SQL_DATA_OPTIONS_PROC_UPDATE_SQL_WITH_MODIFIES(2)");

    String createProcUpdateSQLStr_WITH_DEFAULT = "CREATE PROCEDURE SQL_DATA_OPTIONS_PROC_UPDATE_SQL_WITH_DEFAULT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest.class.getName()
        + ".sqlDataOptionsProc' "
        + "DYNAMIC RESULT SETS 4";

    s.execute(createProcUpdateSQLStr_WITH_DEFAULT);

    s.execute("call SQL_DATA_OPTIONS_PROC_UPDATE_SQL_WITH_DEFAULT(2)");
  }

  public static void sqlDataOptionsProc(int number, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
      ProcedureExecutionContext ctx) throws SQLException {
    Connection c = ctx.getConnection();

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

    if (number > 1) {
      c.createStatement().executeUpdate("update app.t1 set col1 = 100");
    }
  }
  
  /**
   * Tests that <code>Statement.executeQuery()</code> fails when no
   * result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithNoDynamicResultSets() throws SQLException {
    setup();
    Statement stmt = createStatement();
    try {
      stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(0)");
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertNoResultSetFromExecuteQuery(sqle);
    }
  }

  /**
   * Tests that <code>Statement.executeQuery()</code> succeeds when
   * one result set is returned from a stored procedure.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithOneDynamicResultSet() throws SQLException {
    setup();
    Statement stmt = createStatement();
    ResultSet rs = stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
    assertNotNull("executeQuery() returned null.", rs);
    assertSame(stmt, rs.getStatement());
    JDBC.assertDrainResultsHasData(rs);
  }

  /**
   * Tests that <code>Statement.executeQuery()</code> fails when
   * multiple result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithMoreThanOneDynamicResultSet()
  throws SQLException {
    setup();
    Statement stmt = createStatement();
    try {
      stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(2)");
      fail("executeQuery() didn't fail.");
    }
    catch (SQLException sqle) {
      assertMultipleResultsFromExecuteQuery(sqle);
    }
  }

  /**
   * Tests that <code>Statement.executeUpdate()</code> fails when a
   * result set is returned from a stored procedure.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteUpdateWithOneDynamicResultSet() throws SQLException {
    setup();
    Statement stmt = createStatement();
    try {
      stmt.executeUpdate("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
      fail("executeUpdate() didn't fail.");
    }
    catch (SQLException sqle) {
      assertResultsFromExecuteUpdate(sqle);
    }
  }

  public void testExecuteQueryWithDataAwareProcedureCall()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?) "
        + "ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID in (?,?,?) AND THIRDID='3'");
    cs.setInt(1, 2);
    cs.setInt(2, 3);
    cs.setInt(3, 4);
    cs.setInt(4, 5);
    cs.execute();
    
    String[][] results=new String[2][1];
    results[0][0]="1";
    results[1][0]="1";
             
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>1) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());    
    
  }
  
  // !!!:ezoerner:20090922  fails with
  // AssertionError: Alias name resolution not yet implemented
  //	at com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProcessorNode.bindExpression(ProcedureProcessorNode.java:50)
  public void _testExecuteQueryWithMissingResultProcessorClass()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall(
      "CALL RETRIEVE_DYNAMIC_RESULTS(?) WITH RESULT PROCESSOR Fubar ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID in (?,?,?) AND THIRDID='3'");
    cs.setInt(1, 2);
    cs.setInt(2, 3);
    cs.setInt(3, 4);
    cs.setInt(4, 5);
    cs.execute();
    
    String[][] results=new String[2][1];
    results[0][0]="1";
    results[1][0]="1";
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>1) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());    
    
  }
  
  
  public void testDataAwareProcedureCallUsingGlobalIndex()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?) ON TABLE EMP.PARTITIONTESTTABLE1 WHERE SECONDID in (?,?,?) AND THIRDID='3'");
    cs.setInt(1, 2);
    cs.setInt(2, 3);
    cs.setInt(3, 4);
    cs.setInt(4, 5);
    cs.execute();
    
    String[][] results=new String[2][1];
    results[0][0]="1";
    results[1][0]="1";
             
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>1) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());    
    
  }
  
  // !!!:ezoerner:20090826 
  // disabled due NPE similar to that in #40924
  // Note: Since there is an OUT parameter, this is not supposed to be
  // "fire-n-forget". This is now (after merge with fn_ha_admin branch)
  // causing a NullPointerException in
  // a DistributionManager thread
  public void testDataAwareProcedureWithoutResultSets()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL PROCEDURE_WITHOUT_RESULTSET(?, ?) ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID=4 AND THIRDID='3'");
    cs.registerOutParameter(2, java.sql.Types.VARCHAR, 20);
    int number=2;
    String name="INOUTPARAMETER";
    cs.setInt(1, number);
    cs.setString(2, name);
   
    
    cs.execute();
    
    ResultSet rs=cs.getResultSet();
    if(rs!=null || cs.getMoreResults()) {
      fail("no dynamic result set for the procedure!");
    }
    
    ParameterMetaData pmd=cs.getParameterMetaData();  
    int numParameters=pmd.getParameterCount();
    assertTrue(" the number of parameter is 2", numParameters==2);
    try {
      cs.getInt(1);
      fail("the in parameteter cannot be read!");
    } catch (Exception e) {
      
    }
    Object parameter2=cs.getObject(2);    
    assertTrue("the second inout parameter is "+name+number, parameter2.equals(name+number));
      
    

  }
  
  // !!!:ezoerner:20090826 
  // disabled due NPE similar to that in #40924
  // Note: Since there is an OUT parameter, this is not supposed to be
  // "fire-n-forget". This is now (after merge with fn_ha_admin branch)
  // causing a NullPointerException in
  // a DistributionManager thread
  public void testDataAwareProcedureWithoutResultSetsUsingGlobalIndex()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL PROCEDURE_WITHOUT_RESULTSET(?, ?) ON TABLE EMP.PARTITIONTESTTABLE1 WHERE SECONDID=4 AND THIRDID='3'");
    cs.registerOutParameter(2, java.sql.Types.VARCHAR, 20);
    int number=2;
    String name="INOUTPARAMETER";
    cs.setInt(1, number);
    cs.setString(2, name);
   
    
    cs.execute();
    
    ResultSet rs=cs.getResultSet();
    if(rs!=null || cs.getMoreResults()) {
      fail("no dynamic result set for the procedure!");
    }
    
    ParameterMetaData pmd=cs.getParameterMetaData();  
    int numParameters=pmd.getParameterCount();
    assertTrue(" the number of parameter is 2", numParameters==2);
    try {
      cs.getInt(1);
      fail("the in parameteter cannot be read!");
    } catch (Exception e) {
      
    }
    Object parameter2=cs.getObject(2);    
    assertTrue("the second inout parameter is "+name+number, parameter2.equals(name+number));
      
    

  }

  // !!!:ezoerner:20090826 
  // disabled due to suspect strings: #40924
  public void testDataAwareProcedureWithoutResultSetsAndOutParameters()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL PROCEDURE_WITHOUT_RESULTSET_OUTPARAMETER(?, ?) " +
    		"ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID=4 AND THIRDID='3'");    
    int number=2;
    String name="INOUTPARAMETER";
    cs.setInt(1, number);
    cs.setString(2, name);
   
    
    cs.execute();
    
    ResultSet rs=cs.getResultSet();
    if(rs!=null || cs.getMoreResults()) {
      fail("no dynamic result set for the procedure!");
    }

    ParameterMetaData pmd=cs.getParameterMetaData();  
    int numParameters=pmd.getParameterCount();
    assertTrue(" the number of parameter is 2", numParameters==2);
    try {
      cs.getInt(1);
      fail("the in parameter should not be used for output!");
    } catch (SQLException e) {
      if (!"XCL26".equals(e.getSQLState())) {
        throw e;
      }
    }

    try {
      cs.getObject(2);
      fail("the in parameter should not be used for output!");
    } catch (SQLException e) {
      if (!"XCL26".equals(e.getSQLState())) {
        throw e;
      }
    }
  }

  // !!!:ezoerner:20090826 
  // disabled due to suspect strings: #40924
  public void testDataAwareProcedureWithoutResultSetsAndOutParametersAndQuestionMark()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL PROCEDURE_WITHOUT_RESULTSET_OUTPARAMETER(2, 'INOUTPARAMETER')" +
    		" ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID=4 AND THIRDID='3'");            
    cs.execute();
    
    ResultSet rs=cs.getResultSet();
    if(rs!=null || cs.getMoreResults()) {
      fail("no dynamic result set for the procedure!");
    }

    ParameterMetaData pmd=cs.getParameterMetaData();  
    int numParameters=pmd.getParameterCount();
    assertTrue(" the number of parameter is 0", numParameters==0);
    try {
      cs.getInt(1);
      fail("no input parameters!");
    } catch (SQLException e) {
      if (!"07009".equals(e.getSQLState())) {
        throw e;
      }
    }

    try {
      cs.getObject(2);
      fail("no input parameters!");
    } catch (SQLException e) {
      if (!"07009".equals(e.getSQLState())) {
        throw e;
      }
    }
  }

  public void testProcedureWithoutResultSetsAndOutParameters()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL PROCEDURE_WITHOUT_RESULTSET_OUTPARAMETER(?, ?)");    
    int number=2;
    String name="INOUTPARAMETER";
    cs.setInt(1, number);
    cs.setString(2, name);
   
    
    cs.execute();
    
    ResultSet rs=cs.getResultSet();
    if(rs!=null || cs.getMoreResults()) {
      fail("no dynamic result set for the procedure!");
    }
    
    ParameterMetaData pmd=cs.getParameterMetaData();  
    int numParameters=pmd.getParameterCount();
    assertTrue(" the number of parameter is 2", numParameters==2);
    try {
      cs.getInt(1);
      fail("the in parameteter cannot be read!");
    } catch (Exception e) {
      
    }
    
    try {
      cs.getObject(2);
      fail("the in parameteter cannot be read!");
    } catch (Exception e) {
      
    }                   
  }
  
  // !!!:ezoerner:20090930 
  // after the merge with fn_ha_admin branch, this test is now producing
  // suspect strings like you get with #40924, even though this has
  // result sets so execution should not be "fire-n-forget"
  public void testDataAwareProcedureWithOutgoingResultSets()
  throws SQLException {
    
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID=4 AND THIRDID='3'");
    int number=2;
    cs.setInt(1, number);   
    cs.execute();
    
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());           
  }
  
  
  public void testDataAwareProcedureWithOutgoingResultSetsAndNoRoutingObjects()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON TABLE EMP.PARTITIONRANGETESTTABLE WHERE SECONDID in (?, ?, ?)");
    int number=2;
    cs.setInt(1, number);   
    
    cs.setInt(2, 2);
    cs.setInt(3, 3);
    cs.setInt(4, 4);
    
    cs.execute();
    
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());           
  }

  
  public void testDataAwareProcedureWithOutgoingResultSetsOnAll()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON ALL");
    int number=2;
    cs.setInt(1, number);   
    cs.execute();
    
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());           
  }
    
  public void testDataAwareProcedureWithOutgoingResultSetsOnRangePartitionTable()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON TABLE EMP.PARTITIONRANGETESTTABLE WHERE ID>1 and ID<15");
    int number=2;
    cs.setInt(1, number);   
    cs.execute();
    
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());           
  }

  
  public void testDataAwareProcedureWithOutgoingResultSetsOnListPartitionTable()
  throws SQLException {

    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?) ON TABLE EMP.PARTITIONLISTTESTTABLE WHERE ID in (?, ?, ?)");
    int number=2;
    cs.setInt(1, number);   
    cs.setInt(2, 10);
    cs.setInt(3, 15);
    cs.setInt(4, 31);
    cs.execute();
    
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());           
  }

  
  /**
   * Tests that <code>CallableStatement.executeQuery()</code> fails
   * when multiple result sets are returned.
   * @exception SQLException if a database error occurs
   */
  public void testExecuteQueryWithOutgoingResultSetInDerbyCall()
      throws SQLException {
    setup();
    CallableStatement cs = prepareCall("CALL RETRIEVE_OUTGOING_RESULTS(?)");
    cs.setInt(1, 2);
    cs.execute();
    String[][] results=new String[2][10];
    results[0][0]="1";
    for(int i=0; i<10; i++) {
      results[1][i]=i+"String"+i;
    }  
    
    int[] numRows={0,9};
    
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>numRows[rsIndex]) {
          fail("the result is not correct!");
        }
        assertEquals(results[rsIndex][rowIndex], row);
        ++rowIndex;
      }
    } while (cs.getMoreResults());           

  }
  
  public void testCallProcedureWithInAndOutParameter()
     throws SQLException  {
    
    setup();
    int number=2;
    CallableStatement cs = prepareCall("CALL PROCEDURE_INOUT_PARAMETERS(?, ?, ?)");
    cs.setInt(1, number);
    cs.registerOutParameter(2, java.sql.Types.VARCHAR);
    cs.setString(2, "INOUT_PARAMETER");
    cs.registerOutParameter(3, java.sql.Types.INTEGER);
    cs.execute();
    
    String[][] results=new String[2][1];
    results[0][0]="1";
    results[1][0]="1";
             
    int rsIndex=-1;
    do {
      ++rsIndex;
      int rowIndex=0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row="";
        for (int i = 1; i <=rowCount; ++i) {
          Object value = rs.getObject(i);
          row+=value.toString();          
        }
        if(rsIndex>1 || rowIndex>1) {
          fail("the result is not correct!");
        }
        if(!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());    
    
    String outValue=cs.getString(2);
    String outParameter="INOUT_PARAMETER"+"Modified";
    if(!outValue.equals(outParameter)) {
      fail("the out parameter is supposed to "+outParameter+" but "+outValue);
    }
    
    int parameter3=cs.getInt(3);    
    if(parameter3!=number) {
      fail("the out parameter is supposed to "+number+" but "+parameter3);
    }
             
  }
  
  
  // !!!:ezoerner:20090930 
  // after the merge with fn_ha_admin branch, this test is now producing
  // suspect strings like you get with #40924, even though this has out
  // parameters so execution should not be "fire-n-forget"
  public void testDataAwareProcedureWithInAndOutParameter() throws SQLException {

    setup();
    int number = 2;
    CallableStatement cs = prepareCall("CALL PROCEDURE_INOUT_PARAMETERS(?, ?, ?) ON TABLE EMP.PARTITIONTESTTABLE WHERE SECONDID=4 AND THIRDID='3'");
    cs.setInt(1, number);
    cs.registerOutParameter(2, java.sql.Types.VARCHAR);
    cs.setString(2, "INOUT_PARAMETER");
    cs.registerOutParameter(3, java.sql.Types.INTEGER);
    cs.execute();

    String[][] results = new String[2][1];
    results[0][0] = "1";
    results[1][0] = "1";

    int rsIndex = -1;
    do {
      ++rsIndex;
      int rowIndex = 0;
      ResultSet rs = cs.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int rowCount = metaData.getColumnCount();
      while (rs.next()) {
        String row = "";
        for (int i = 1; i <= rowCount; ++i) {
          Object value = rs.getObject(i);
          row += value.toString();
        }
        if (rsIndex > 1 || rowIndex > 1) {
          fail("the result is not correct!");
        }
        if (!row.equals(results[rsIndex][rowIndex])) {
          fail("the result is not correct!");
        }
        ++rowIndex;
      }
    } while (cs.getMoreResults());

    String outValue = cs.getString(2);
    String outParameter = "INOUT_PARAMETER" + "Modified";
    if (!outValue.equals(outParameter)) {
      fail("the out parameter is supposed to " + outParameter + " but "
          + outValue);
    }

    int parameter3 = cs.getInt(3);
    if (parameter3 != number) {
      fail("the out parameter is supposed to " + number + " but " + parameter3);
    }

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

    setupConnection();
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
    
    s.execute("SELECT * FROM EMP.PARTITIONTESTTABLE WHERE (SECONDID=10 AND THIRDID='20') OR (SECONDID=10 AND THIRDID='20')");

    s.execute("create table EMP.PARTITIONTESTTABLE1 (ID int NOT NULL,"
        + " SECONDID int not null, THIRDID varchar(10) not null, PRIMARY KEY (SECONDID, THIRDID)) PARTITION BY COLUMN (ID)");
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
          + ProcedureTest.class.getName() + ".retrieveDynamicResults' "
          + "DYNAMIC RESULT SETS 4",
          
      "CREATE PROCEDURE RETRIEVE_OUTGOING_RESULTS(number INT) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".retrieveDynamicResultsWithOutgoingResultSet'"
          + "DYNAMIC RESULT SETS 4",    

      "CREATE PROCEDURE PROCEDURE_INOUT_PARAMETERS(number INT, INOUT name VARCHAR(25), OUT total INT) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".procedureWithInAndOutParameters'"
          + "DYNAMIC RESULT SETS 4",        
        
      "CREATE PROCEDURE PROCEDURE_WITHOUT_RESULTSET(number INT, INOUT name VARCHAR(20) ) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".procedureWithoutDynamicResultSets'",       
          
      "CREATE PROCEDURE PROCEDURE_WITHOUT_RESULTSET_OUTPARAMETER(number INT, name VARCHAR(20) ) "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".procedureWithoutDynamicResultSetsAndOutParameters'",     
           
      "CREATE PROCEDURE RETRIEVE_CLOSED_RESULT() LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".retrieveClosedResult' "
          + "DYNAMIC RESULT SETS 1",

      "CREATE PROCEDURE RETRIEVE_EXTERNAL_RESULT("
          + "DBNAME VARCHAR(128), DBUSER VARCHAR(128), DBPWD VARCHAR(128)) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".retrieveExternalResult' "
          + "DYNAMIC RESULT SETS 1",

      "CREATE PROCEDURE PROC_WITH_SIDE_EFFECTS(ret INT) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".procWithSideEffects' "
          + "DYNAMIC RESULT SETS 2",

      "CREATE PROCEDURE NESTED_RESULT_SETS(proctext VARCHAR(128)) LANGUAGE JAVA "
          + "PARAMETER STYLE JAVA EXTERNAL NAME '"
          + ProcedureTest.class.getName() + ".nestedDynamicResultSets' "
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
   * Check that CallableStatement.execute() closes results
   * @param cs
   * @param allRS
   * @throws SQLException
   */
  private void checkExecuteClosesResults(CallableStatement cs, ResultSet[] allRS)
      throws SQLException {
    //Fetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.execute() closes them");          
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
    Statement s = getStatement();
    addStatement(s);
    return s;
  }

  /**
   * Add a statement into the list we will close
   * at tearDown.
   */
  private void addStatement(Statement s) {
    if (statements == null)
      statements = new ArrayList();
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
    PreparedStatement ps = getPreparedStatement(sql);
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
    CallableStatement cs = jdbcConn.prepareCall(sql);
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

  // Try out invalid ON clauses
  // Each should throw sqlstate 0A000 :
  // ON <view>
  // ON <VTI>
  // ON <SYSDUMMY>
  public void testInvalidCallOnClause()
  throws SQLException {
    
    setup();
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("CREATE TABLE INVALID_CALL_TABLE (COL1 INTEGER)");
    s.execute("CREATE VIEW INVALID_CALL_VIEW AS SELECT * FROM INVALID_CALL_TABLE");

    // Try a VTI 
    try {
      CallableStatement cs = prepareCall("CALL SYSCS_UTIL.EMPTY_STATEMENT_CACHE() ON TABLE SYS.MEMBERS");            
      cs.execute();
    } catch (SQLException e) {
      if (!"0A000".equals(e.getSQLState())) {
        throw e;
      }
    }
    // Try a view
    try {
      CallableStatement cs = prepareCall("CALL SYSCS_UTIL.EMPTY_STATEMENT_CACHE() ON TABLE INVALID_CALL_VIEW");            
      cs.execute();
    } catch (SQLException e) {
      if (!"0A000".equals(e.getSQLState())) {
        throw e;
      }
    }
    // Try the sysdummy table
    try {
      CallableStatement cs = prepareCall("CALL SYSCS_UTIL.EMPTY_STATEMENT_CACHE() ON TABLE SYSIBM.SYSDUMMY1");            
      cs.execute();
    } catch (SQLException e) {
      if (!"0A000".equals(e.getSQLState())) {
        throw e;
      }
    }
  }

  // This is the DDL Unit Test for the CALL statement
  // Test handling of NULL parameters to built-in call statements
  // Test incorrect or illegal parameters for CALL ON... clause
  // These tests will ensure that the connection is not dropped and no
  // Java or NullPointer exception gets thrown back to the user 
  // Other buckets will test if ATTACH/REMOVE calls return valid errors if the
  // gatewaysender/listener is nonexistent
  // This also tests the fix for bug 45815
  public void testDDLUnitTestCallStmt()
  throws SQLException {
    
    setup();
    Connection conn = getConnection();
    Statement s = conn.createStatement();

    // Setup objects
    s.execute("CREATE TABLE CALLUT1 (COL1 INTEGER, COL2 INTEGER)");
    s.execute("CREATE TABLE CALLUT2 (COL1 INTEGER, COL2 INTEGER) PARTITION BY COLUMN(COL1)");

    // Pass in NULL for all non-integer values
    // TODO : try empty strings, invalid integers, etc (although Derby does not catch all of these)
    String[] sqlstate_xie06_calls = { 
       "SYSCS_UTIL.EXPORT_QUERY(NULL,NULL,NULL,NULL,NULL)",
       "SYSCS_UTIL.EXPORT_TABLE(NULL,NULL,NULL,NULL,NULL,NULL)",
       "SYSCS_UTIL.IMPORT_DATA(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0)",
       "SYSCS_UTIL.IMPORT_TABLE(NULL,NULL,NULL,NULL,NULL,NULL,0)",
       "SQLJ.INSTALL_JAR(NULL,NULL,0)",
       "SQLJ.INSTALL_JAR_BYTES(NULL,NULL)",
       "SQLJ.REMOVE_JAR(NULL,0)",
       "SQLJ.REPLACE_JAR(NULL,NULL)",
       "SQLJ.REPLACE_JAR_BYTES(NULL,NULL)",
       "SYS.ADD_LISTENER(NULL,NULL,NULL,NULL,NULL,NULL)",
       "SYS.ATTACH_LOADER(NULL,NULL,NULL,NULL)",
       "SYS.ATTACH_WRITER(NULL,NULL,NULL,NULL,NULL)",
       "SYS.REMOVE_LISTENER(NULL,NULL,NULL)", 
       "SYS.REMOVE_LOADER(NULL,NULL)",
       "SYS.REMOVE_WRITER(NULL,NULL)",
       "SYS.START_ASYNC_EVENT_LISTENER(NULL)",
       "SYS.STOP_ASYNC_EVENT_LISTENER(NULL)",
       "SYS.STOP_GATEWAYSENDER(NULL)",
       "SYS.START_GATEWAYSENDER(NULL)",
       "SYS.SET_QUERYSTATS(NULL)",
       "SYS.CREATE_ALL_BUCKETS(NULL)"
    };

    // Try built-in procs with NULL passed in paramters
    // Should receive sqlstate XIE06 (Entity was null) to match Derby built-ins handling of nulls
    for (String myCall : sqlstate_xie06_calls)
    {
      try {
        CallableStatement cs = prepareCall("CALL " + myCall);      
        cs.execute();
        fail("should fail since nulls not allowed for these calls");
      } catch (SQLException e) {
        if (!"XIE06".equals(e.getSQLState())) {
          throw e;
        }
      }
    }

    // Try illegal values for heap percentages
    // TODO : this actually throws an IllegalStateException, should throw SQL error
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_EVICTION_HEAP_PERCENTAGE(-5)");      
      cs.execute();
      fail("should fail as percentage is negative");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_EVICTION_HEAP_PERCENTAGE(444)");      
      cs.execute();
      fail("should fail as percentage is greater than 100");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_CRITICAL_HEAP_PERCENTAGE(-5)");      
      cs.execute();
      fail("should fail as percentage is negative");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_CRITICAL_HEAP_PERCENTAGE(444)");      
      cs.execute();
      fail("should fail as percentage is greater than 100");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal values for off heap percentages
    // TODO : this actually throws an IllegalStateException, should throw SQL error
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_EVICTION_OFFHEAP_PERCENTAGE(-5)");      
      cs.execute();
      fail("should fail as percentage is negative");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_EVICTION_OFFHEAP_PERCENTAGE(444)");      
      cs.execute();
      fail("should fail as percentage is greater than 100");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE(-5)");      
      cs.execute();
      fail("should fail as percentage is negative");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.SET_CRITICAL_OFFHEAP_PERCENTAGE(444)");      
      cs.execute();
      fail("should fail as percentage is greater than 100");
    } catch (SQLException e) {
      if (!"38000".equals(e.getSQLState())) {
        throw e;
      }
    }
    
    // Try CREATE/DROP_USER will null username (previously it killed the connection)
    try {
      CallableStatement cs = prepareCall("CALL SYS.CREATE_USER(NULL,NULL)");      
      cs.execute();
      fail("should fail as userid is null");
    } catch (SQLException e) {
      if (!"28502".equals(e.getSQLState())) {
        throw e;
      }
    }
    try {
      CallableStatement cs = prepareCall("CALL SYS.DROP_USER(NULL)");      
      cs.execute();
      fail("should fail as userid is null");
    } catch (SQLException e) {
      if (!"28502".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal CREATE_ALL_BUCKETS call
    try {
      CallableStatement cs = prepareCall("CALL SYS.CREATE_ALL_BUCKETS('SYS.SYSTABLES')");      
      cs.execute();
      fail("should fail as table is not partitioned");
    } catch (SQLException e) {
      if (!"X0Z15".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Now let's try the CALL ... ON... syntax
    // Using SYS.REBALANCE_ALL_BUCKETS() since we're just checking syntax and don't care
    // about the execution
    // Try with result processing that doesn't exist
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() WITH RESULT PROCESSOR X");      
      cs.execute();
      fail("should fail as result processor class is missing");
    } catch (SQLException e) {
      if (!"42X51".equals(e.getSQLState())) {
        throw e;
      }
    }
   
    // Try with missing table 
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE MISSINGTABLE");      
      cs.execute();
      fail("should fail as table is missing");
    } catch (SQLException e) {
      if (!"42X05".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal target
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE SYS.SYSTABLES");      
      cs.execute();
      fail("should fail as table is not legal target");
    } catch (SQLException e) {
      if (!"0A000".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal WHERE clause (with subquery)
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() "
          + "ON TABLE CALLUT2 WHERE COL1 IN (SELECT COL1 FROM CALLUT1)");
      cs.execute();
      fail("should fail as where clause is not scalar");
    } catch (SQLException e) {
      if (!"42904".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal WHERE clause (column doesn't exist in table)
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 WHERE COL99 = 4");   
      cs.execute();
      fail("should fail as where clause references missing column");
    } catch (SQLException e) {
      if (!"42X04".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try illegal WHERE clause (aggregate)
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 WHERE MAX(COL1) = 5");   
      cs.execute();
      fail("should fail as where clause contains an aggregate");
    } catch (SQLException e) {
      if (!"42903".equals(e.getSQLState())) {
        throw e;
      }
    }
    // Try illegal WHERE clause (constant)
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 WHERE 5");   
      cs.execute();
      fail("should fail as where clause is a constant");
    } catch (SQLException e) {
      if (!"42X19".equals(e.getSQLState())) {
        throw e;
      }
    }
    // Try illegal WHERE clause (NULL)
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 WHERE NULL");   
      cs.execute();
      fail("should fail as where clause is a null - syntax error");
    } catch (SQLException e) {
      if (!"42X01".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Try bad syntax
    // ON TABLE and ALL
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 ON ALL");   
      cs.execute();
      fail("should fail with syntax error");
    } catch (SQLException e) {
      if (!"42X01".equals(e.getSQLState())) {
        throw e;
      }
    }
    // ON TABLE and ON SERVER GROUPS
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT2 ON SERVER GROUPS()");   
      cs.execute();
      fail("should fail with syntax error");
    } catch (SQLException e) {
      if (!"42X01".equals(e.getSQLState())) {
        throw e;
      }
    }

    // Empty server groups
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON SERVER GROUPS ()");   
      cs.execute();
      fail("should fail with syntax error");
    } catch (SQLException e) {
      if (!"42X01".equals(e.getSQLState())) {
        throw e;
      }
    }
    // Two tables
    try {
      CallableStatement cs = prepareCall("CALL SYS.BALANCE_ALL_BUCKETS() ON TABLE CALLUT1 ON TABLE CALLUT2");   
      cs.execute();
      fail("should fail with syntax error");
    } catch (SQLException e) {
      if (!"42X01".equals(e.getSQLState())) {
        throw e;
      }
    }
  }
  public static void procSecuritasBug(Timestamp time) throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    PreparedStatement stmt = c.prepareStatement("update test set time2= ?");
    stmt.setTimestamp(1, time);
    stmt.executeUpdate();
  }
  
  public static void procSecuritasBug_2(ResultSet[] external) throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    Statement stmt = c.createStatement();
    external[0] = stmt.executeQuery("select time2 from test");
  }
}
