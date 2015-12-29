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
package tests;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.ProcedureExecutionContextImpl;

/**
 * This class defines the procedures used by C# tests.
 *
 * @author swale
 */
public class TestProcedures {
  private static String nestedProtocol = "jdbc:default:connection";
  // no instances allowed
  private TestProcedures() { }
  
  public static int times(int value) {	
     return value * 2;
  }
  
  public static ResultSet subset(String tableName, int low, int high)
      throws SQLException {
    Connection conn = DriverManager.getConnection(nestedProtocol);
    String query = "SELECT ID, SECURITY_ID FROM " + tableName + " where id>"
        + low + " and id<" + high;
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(query);
    return rs;
  }

  public static void inOutParams(int p1, int[] p2, String[] p3,
      ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      p2[0] = p1 * p2[0];
      p3[0] = "output value changed";
      rs1[0] = conn.createStatement().executeQuery("values('first column of " +
        "first rowset', 2, 'third column of first rowset')");
    } finally {
      conn.close();
    }
  }

  public static void multipleResultSets(int maxId, ResultSet[] rs1,
      ResultSet[] rs2) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      Statement stmt = conn.createStatement();
      rs1[0] = stmt.executeQuery("select id from numeric_family where" +
          " id <=" + maxId + " order by id asc");
      stmt = conn.createStatement();
      rs2[0] = stmt.executeQuery("select type_int from numeric_family where" +
          " id <=" + maxId + " order by id asc");
    } finally {
      conn.close();
    }
  }

  public static void longParams(String p1, String p2, String p3, String[] p4,
      int[] p5) throws SQLException {
    p4[0] = "Hello";
    p5[0] = 2;
  }

  public static void noParam(ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      rs1[0] = conn.createStatement().executeQuery("values ('data')");
    } finally {
      conn.close();
    }
  }

  public static void monoBug66630(short status, ResultSet[] rs1)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      rs1[0] = conn.createStatement().executeQuery("values (5, cast(" +
        status + " as smallint))");
    } finally {
      conn.close();
    }
  }

  public static void testSize(String[] p1) throws SQLException {
    if (p1[0] == null) {
      p1[0] = "1234567890";
    }
  }

  public static void decimalTest(BigDecimal in, BigDecimal[] out, int[] di)
      throws SQLException {
    out[0] = in;
    di[0] = in.intValue();
  }

  public static void paramTest(String type, int param1, int[] param2,
      int[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      pstmt.setObject(1, param1);
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, short param1, short[] param2,
      short[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      pstmt.setObject(1, param1);
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, long param1, long[] param2,
      long[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      pstmt.setObject(1, param1);
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, float param1, float[] param2,
      float[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      pstmt.setObject(1, param1);
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, double param1, double[] param2,
      double[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      pstmt.setObject(1, param1);
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, BigDecimal param1,
      BigDecimal[] param2, BigDecimal[] param3, int[] out, ResultSet[] rs1)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      if (param1 != null) {
        pstmt.setObject(1, param1);
      }
      else {
        pstmt.setNull(1, pstmt.getMetaData().getColumnType(1));
      }
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    param2[0] = param1;
    out[0] = 5;
  }

  public static void paramTest(String type, Object param1, Object[] param2,
      Object[] param3, int[] out, ResultSet[] rs1) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "values(cast(? as " + type + "))");
      if (param1 != null) {
        pstmt.setObject(1, param1);
      }
      else {
        pstmt.setNull(1, pstmt.getMetaData().getColumnType(1));
      }
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
    //java.lang.reflect.Array.set(param2, 0, param1);
    param2[0] = param1;
    out[0] = 5;
  }

  public static void insertEmployee(String fname, Timestamp dob,
      Timestamp[] doj, int[] id) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "select max(id) from employee");
      ResultSet rs = pstmt.executeQuery();
      rs.next();
      id[0] = rs.getInt(1) + 6000 + 1;
      rs.close();
      doj[0] = new Timestamp(System.currentTimeMillis());
      pstmt = conn.prepareStatement(
        "insert into employee (id, fname, dob, doj) values (?, ?, ?, ?)");
      pstmt.setInt(1, id[0]);
      pstmt.setString(2, fname);
      pstmt.setTimestamp(3, dob);
      pstmt.setTimestamp(4, doj[0]);
      pstmt.execute();
    } finally {
      conn.close();
    }
  }

  public static void bug382635(String desc) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "UPDATE Bug382635 SET description=?");
      pstmt.setString(1, desc);
      pstmt.executeUpdate();
    } finally {
      conn.close();
    }
  }

  public static void bug382539(BigDecimal[] ts) throws SQLException {
    ts[0] = new BigDecimal("102.34");
  }

  public static void multipleResultSets(ResultSet[] rs1, ResultSet[] rs2)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "Select id, type_int from numeric_family");
      rs1[0] = pstmt.executeQuery();
      pstmt = conn.prepareStatement("Select type_varchar from numeric_family");
      rs2[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
  }

  public static void multipleNoColumnResults(ResultSet[] rs1, ResultSet[] rs2)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "select 10, 20 from numeric_family");
      rs1[0] = pstmt.executeQuery();
      pstmt = conn.prepareStatement("values (10, 30)");
      rs2[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
  }

  public static void updateAndQuery(ResultSet[] rs1)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      conn.createStatement().execute(
        "update numeric_family set type_int=100 where id=50");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from numeric_family");
      rs1[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
  }

  public static void multiplePKResultSets(ResultSet[] rs1, ResultSet[] rs2)
      throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    try {
      PreparedStatement pstmt = conn.prepareStatement(
        "Select id, type_int from numeric_family");
      rs1[0] = pstmt.executeQuery();
      pstmt = conn.prepareStatement(
        "Select type_varchar, id, type_int from numeric_family");
      rs2[0] = pstmt.executeQuery();
    } finally {
      conn.close();
    }
  }

  public static void testProc(BigDecimal[] decval, int id, String[] name)
      throws SQLException {
    decval[0] = new BigDecimal("102.34");
    name[0] = "gemfire1";
  }
  
  private static final int onTable = 0;
  private static final int onAll = 1;
  private static final int onServerGroups = 2;
  private static final int noOnClause = 3;
  public static final int onTableNoWhereClause = 5;
  
    private static String getInvocationType(int invocationType) {
    switch(invocationType) {
      case onTable:
        return "onTable";
        
      case onAll:
        return "onAll";
        
      case onServerGroups:
        return "onServerGroups";
        
      case noOnClause:
        return "noOnClause";
        
      case onTableNoWhereClause:
        return "onTableNoWhereClause";
        
        default:
          return "unknown";
    }
  }
  
  public static void select_proc(int invocationType, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
      ProcedureExecutionContext pec) throws SQLException {
	  System.out.println("select_proc called with invocation type: "
            + getInvocationType(invocationType) + " and pec: " + pec);    
    ProcedureExecutionContextImpl cimpl = (ProcedureExecutionContextImpl)pec;
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    System.out.println("from pec tablename is: " + pec.getTableName());
    if (cimpl.getTableName() != null) {
      //assertTrue(cimpl.getTableName().equalsIgnoreCase("partitiontestTable"));
    }

    String filter = " where " + cimpl.getFilter();
    String sql = "select * from emp.partitiontestTable";
    String local_sql = "";
    String global_sql = "";
    String default_sql = "";

    switch (invocationType) {
      case onTable:
        filter = " where " + cimpl.getFilter();
        sql = sql + " " + filter;
        local_sql = "<LOCAL> " + sql;
        global_sql = "<GLOBAL> " + sql;
        default_sql = sql;
        break;

      case onAll:
      case onServerGroups:
      case noOnClause:
      case onTableNoWhereClause:
        local_sql = "<LOCAL> " + sql;
        global_sql = "<GLOBAL> " + sql;
        default_sql = sql;
        break;
        
      default:
        break;
    }
    System.out.println(
        "select_proc def sql is: " + sql + " local sql is: " + local_sql
            + " and global sql is: " + global_sql);

    rs1[0] = c.createStatement().executeQuery(default_sql);
    rs2[0] = c.createStatement().executeQuery(local_sql);
    rs3[0] = c.createStatement().executeQuery(global_sql);
  }

}
