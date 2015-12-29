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

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/*
 * Author: sjigyasu
 * Verifies that SQL queries are authorized when called from procedures.
 */
public class ProcedureAuthorizationTest extends JdbcTestBase {

  Properties props = new Properties();
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ProcedureAuthorizationTest.class));
  }
  
  public ProcedureAuthorizationTest(String name) {
    super(name);
  }
  public void verifyUnauthorizedExecution(CallableStatement tCallableSt, boolean withContextConn) throws SQLException{
    int param2 = withContextConn ? 0 : 1;
    // // Test SELECT
    tCallableSt.setInt(1, 1);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();
    }
    catch(SQLException e){
      // AUTH_NO_COLUMN_PERMISSION
      if(!e.getSQLState().equals("42502") && !e.getSQLState().equals("38002")){
        throw e;
      }
    }
    // Test insert
    tCallableSt.setInt(1, 2);
    tCallableSt.setInt(2, param2);
    
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      // AUTH_NO_TABLE_PERMISSION
      if(!e.getSQLState().equals("42500") && !e.getSQLState().equals("38002")){
        throw e;
      }
    }
    // Test update
    tCallableSt.setInt(1, 3);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      //AUTH_NO_COLUMN_PERMISSION
      if(!e.getSQLState().equals("42502") && !e.getSQLState().equals("38002")){
        throw e;
      }
    }
    // Test delete
    tCallableSt.setInt(1, 4);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      // AUTH_NO_TABLE_PERMISSION
      if(!e.getSQLState().equals("42500") && !e.getSQLState().equals("38002")){
        throw e;
      }
    }
    
    // Test drop
/*    tCallableSt.setInt(1, 5);
    tCallableSt.setInt(2, 0);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      if(!e.getSQLState().equals("XCL21"))
      {
        throw e;
      }
    }    
*/    
  }
  public void verifyUnauthorizedExecutionForReadOnlyProc_(CallableStatement tCallableSt, boolean withContextConn) throws SQLException{
    int param2 = withContextConn ? 0 : 1;
    // // Test SELECT
    tCallableSt.setInt(1, 1);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();
    }
    catch(SQLException e){
      // AUTH_NO_COLUMN_PERMISSION
      if(!e.getSQLState().equals("42502")){
        throw e;
      }
    }
    // Test insert
    tCallableSt.setInt(1, 2);
    tCallableSt.setInt(2, param2);
    
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      // EXTERNAL_ROUTINE_NO_MODIFIES_SQL
      if(!e.getSQLState().equals("38002"))
      {
        throw e;
      }
    }
    // Test update
    tCallableSt.setInt(1, 3);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      //EXTERNAL_ROUTINE_NO_MODIFIES_SQL
      if(!e.getSQLState().equals("38002")){
        throw e;
      }
    }
    // Test delete
    tCallableSt.setInt(1, 4);
    tCallableSt.setInt(2, param2);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      // EXTERNAL_ROUTINE_NO_MODIFIES_SQL
      if(!e.getSQLState().equals("38002"))
      {
        throw e;
      }
    }
    
    // Test drop
/*    tCallableSt.setInt(1, 5);
    tCallableSt.setInt(2, 0);
    try{
      tCallableSt.execute();  
    }
    catch(SQLException e){
      if(!e.getSQLState().equals("XCL21"))
      {
        throw e;
      }
    }    
*/    
  }
  
  public void testProcedureAuthorization_ModifiesSQLData() throws SQLException {

    //System.setProperty("gemfirexd.debug.true", "DumpClassFile");
    
    
    props.put("auth-provider", "BUILTIN");
    props.put("gemfirexd.user.APP", "APP");
    props.put("gemfirexd.sql-authorization", "TRUE");
    
    props.put("user", "APP");
    props.put("password","APP");
    props.setProperty("mcast-port", "0");
    loadDriverClass(TestUtil.getDriver());
    props = doCommonSetup(props);
    Connection conn = DriverManager
        .getConnection(TestUtil.getProtocol(), props);

    Statement st = conn.createStatement();
    st.execute("DROP TABLE IF EXISTS TESTSCHEMA.T");
    st.execute("DROP SCHEMA IF EXISTS TESTSCHEMA RESTRICT");
    st.execute("CREATE SCHEMA TESTSCHEMA");
    st.execute("CREATE TABLE TESTSCHEMA.T(C1 INT, C2 INT)");
    st.execute("CALL SYS.CREATE_USER('gemfirexd.user.TUSER','TPASSWORD')");
    
    Properties props2 = new Properties();
    props2.put("user", "TUSER");
    props2.put("password","TPASSWORD");
    Connection tconn = DriverManager.getConnection(TestUtil.getProtocol(),
        props2);
    Statement tstatement = tconn.createStatement();

    tstatement.execute("DROP PROCEDURE IF EXISTS TUSER.EXAMPLE_PROC");
    String createProc = "CREATE PROCEDURE TUSER.EXAMPLE_PROC(IN S_MONTH INTEGER," +
        "IN S_YEAR INTEGER) " +
         "LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA EXTERNAL NAME " + 
         "'com.pivotal.gemfirexd.jdbc.ProcedureAuthorizationTest.exampleProc'" ;

    tstatement.execute(createProc);
    CallableStatement tCallableSt = tconn.prepareCall("CALL TUSER.EXAMPLE_PROC(?,?)");
    
    // Verify with context connection
    verify(st, tCallableSt, true, false);
    
    // Verify with new connection taken within the procedure
    verify(st, tCallableSt, false, false);

  }
  public void testProcedureAuthorization_ReadsSQLData() throws SQLException{

    props.put("auth-provider", "BUILTIN");
    props.put("gemfirexd.user.APP", "APP");
    props.put("gemfirexd.sql-authorization", "TRUE");
    
    props.put("user", "APP");
    props.put("password","APP");
    props.setProperty("mcast-port", "0");
    loadDriverClass(TestUtil.getDriver());
    props = doCommonSetup(props);
    Connection conn = DriverManager
        .getConnection(TestUtil.getProtocol(), props);

    Statement st = conn.createStatement();
    st.execute("DROP TABLE IF EXISTS TESTSCHEMA.T");
    st.execute("DROP SCHEMA IF EXISTS TESTSCHEMA RESTRICT");
    st.execute("CREATE SCHEMA TESTSCHEMA");
    st.execute("CREATE TABLE TESTSCHEMA.T(C1 INT, C2 INT)");
    st.execute("CALL SYS.CREATE_USER('gemfirexd.user.TUSER','TPASSWORD')");

    Properties props2 = new Properties();
    props2.put("user", "TUSER");
    props2.put("password","TPASSWORD");
    Connection tconn = DriverManager.getConnection(TestUtil.getProtocol(),
        props2);
    Statement tstatement = tconn.createStatement();

    tstatement.execute("DROP PROCEDURE IF EXISTS TUSER.EXAMPLE_PROC");
    String createProc = "CREATE PROCEDURE TUSER.EXAMPLE_PROC(IN S_MONTH INTEGER," +
        "IN S_YEAR INTEGER) " +
         "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA EXTERNAL NAME " + 
         "'com.pivotal.gemfirexd.jdbc.ProcedureAuthorizationTest.exampleProc'" ;

    tstatement.execute(createProc);
    CallableStatement tCallableSt = tconn.prepareCall("CALL TUSER.EXAMPLE_PROC(?,?)");
    
    // Verify with context connection
    verify(st, tCallableSt, true, true);
    
    // Verify with new connection taken within the procedure
    verify(st, tCallableSt, false, true);
    
  }
  public void testProcedureAuthorization_NoSQL() throws SQLException{

    props.put("auth-provider", "BUILTIN");
    props.put("gemfirexd.user.APP", "APP");
    props.put("gemfirexd.sql-authorization", "TRUE");
    
    props.put("user", "APP");
    props.put("password","APP");
    props.setProperty("mcast-port", "0");
    loadDriverClass(TestUtil.getDriver());
    props = doCommonSetup(props);
    Connection conn = DriverManager
        .getConnection(TestUtil.getProtocol(), props);

    Statement st = conn.createStatement();
    st.execute("DROP TABLE IF EXISTS TESTSCHEMA.T");
    st.execute("DROP SCHEMA IF EXISTS TESTSCHEMA RESTRICT");
    st.execute("CREATE SCHEMA TESTSCHEMA");
    st.execute("CREATE TABLE TESTSCHEMA.T(C1 INT, C2 INT)");
    st.execute("CALL SYS.CREATE_USER('gemfirexd.user.TUSER','TPASSWORD')");

    Properties props2 = new Properties();
    props2.put("user", "TUSER");
    props2.put("password", "TPASSWORD");
    Connection tconn = DriverManager.getConnection(TestUtil.getProtocol(),
        props2);
    Statement tstatement = tconn.createStatement();

    tstatement.execute("DROP PROCEDURE IF EXISTS TUSER.EXAMPLE_PROC");
    String createProc = "CREATE PROCEDURE TUSER.EXAMPLE_PROC(IN S_MONTH INTEGER," +
        "IN S_YEAR INTEGER) " +
         "LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL EXTERNAL NAME " + 
         "'com.pivotal.gemfirexd.jdbc.ProcedureAuthorizationTest.exampleProc'" ;

    tstatement.execute(createProc);
    CallableStatement tCallableSt = tconn.prepareCall("CALL TUSER.EXAMPLE_PROC(?,?)");
    
    // Procedures declared with NO SQL should throw error 
    try{
      // Verify with context connection
      verify(st, tCallableSt, true, true);
    }
    catch(SQLException e){
      // EXTERNAL_ROUTINE_NO_SQL
      if(!e.getSQLState().equals("38001")){
        throw e;
      }
    }
    // Procedures declared with NO SQL should throw error 
    try{
      // Verify with new connection taken within the procedure
      verify(st, tCallableSt, false, true);
    }
    catch(SQLException e){
      // EXTERNAL_ROUTINE_NO_SQL
      if(!e.getSQLState().equals("38001")){
        throw e;
      }
    }
  }
  public void assertExecutionException(CallableStatement tCallableSt, String errorCode) throws SQLException{
    boolean thrown = false;
      try{
        tCallableSt.execute();
      }
      catch(SQLException e){
        if(e.getSQLState().equals(errorCode)){
          thrown = true;
        }
        else{
          throw e;
        }
      }
      assertTrue(thrown);
  }
  public void verify(Statement first, CallableStatement second, boolean withContextConnection, boolean readOnlyProc) throws SQLException{
    Statement st = first;
    CallableStatement tCallableSt = second;
    
    verifyUnauthorizedExecution(tCallableSt, withContextConnection);
    
    st.execute("GRANT SELECT ON TESTSCHEMA.T TO TUSER");
    tCallableSt.setInt(1, 1);
    tCallableSt.setInt(2, 0);
    tCallableSt.execute();
    
    st.execute("GRANT INSERT ON TESTSCHEMA.T TO TUSER");
    
    tCallableSt.setInt(1, 2);
    tCallableSt.setInt(2, 0);
    // Read only procedures should still throw Exception.
    if(readOnlyProc){
      assertExecutionException(tCallableSt, "38002");
    }
    else{
      tCallableSt.execute();  
    }

    st.execute("GRANT UPDATE ON TESTSCHEMA.T TO TUSER");
    tCallableSt.setInt(1, 3);
    tCallableSt.setInt(2, 0);
    
    if(readOnlyProc){
      assertExecutionException(tCallableSt, "38002");
    }
    else{
      tCallableSt.execute();
    }
    st.execute("GRANT DELETE ON TESTSCHEMA.T TO TUSER");
    tCallableSt.setInt(1, 4);
    tCallableSt.setInt(2, 0);
    
    if(readOnlyProc){
      assertExecutionException(tCallableSt, "38002");
    }
    else{
      tCallableSt.execute();
    }
    
    st.execute("REVOKE SELECT ON TESTSCHEMA.T FROM TUSER");
    st.execute("REVOKE INSERT ON TESTSCHEMA.T FROM TUSER");
    st.execute("REVOKE UPDATE ON TESTSCHEMA.T FROM TUSER");
    st.execute("REVOKE DELETE ON TESTSCHEMA.T FROM TUSER");

    verifyUnauthorizedExecution(tCallableSt, withContextConnection);
  }
  public static void exampleProc(int inParam1, int inParam2, ProcedureExecutionContext ctx) throws SQLException {

    Statement st = null;
    if(inParam2 == 0){
      Connection conn = ctx.getConnection();
      st = conn.createStatement();
    }
    else{
      Properties props2 = new Properties();
      props2.put("user", "TUSER");
      props2.put("password","TPASSWORD");
      // Embedded connection
      Connection tconn = DriverManager.getConnection(TestUtil.getProtocol(),
          props2);
      st = tconn.createStatement();
    }

    if(inParam1 == 1){
      st.execute("SELECT * FROM TESTSCHEMA.T");
    }
    else if(inParam1 == 2){
      st.execute("INSERT INTO TESTSCHEMA.T VALUES(4,4)");
    }
    else if(inParam1 == 3){
      st.execute("UPDATE TESTSCHEMA.T SET C1 = 3 WHERE C1 = 4");
    }
    else if(inParam1 == 4){
      st.execute("DELETE FROM TESTSCHEMA.T WHERE C1 = 3");
    }
    /*    else if(inParam1 == 5){
      st.execute("DROP TABLE TESTSCHEMA.T");
    }
*/   

  }
  
  @Override
  protected void tearDown() throws Exception {
    try {
      shutDown(props);
      currentUserName = null;
      currentUserPassword = null;
    } finally {
      super.tearDown();
      final String[] dirs = testSpecificDirectoriesForDeletion();
      if (dirs != null) {
        for (String dir : dirs) {
          deleteDir(new File(dir));
        }
      }
      clearAllOplogFiles();
    }
  }
  
}
