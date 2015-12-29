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
package sql.ddlStatements;

import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.sql.*;
import java.util.*;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.ddlStatements.DDLStmtIF;
import sql.security.SQLSecurityTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;
import hydra.Log;

@SuppressWarnings("unchecked")
public class AuthorizationDDLStmt implements DDLStmtIF {
  //static int totalAccessorThreads = TestConfig.tab().intAt(SQLPrms.totalAccessorThreads);
  //static int totalStoreThreads = TestConfig.tab().intAt(SQLPrms.totalStoreThreads);
  private static String[] tableNames = SQLPrms.getTableNames();
  private static HashMap<String, List<String>>tableCols = new HashMap<String, List<String>>();
  protected static ArrayList<String>  procedureNames = null;
  protected static ArrayList<String>  functionNames = null;
  protected static boolean usePublic = TestConfig.tab().booleanAt(SQLPrms.usePublicAsGrantees, false);
  
  static {
    tableCols.putAll((Map)SQLBB.getBB().getSharedMap().get("tableCols")); //need to invoke setTable first
  }
  
  private static String[] tablePriv = {
       "delete", "insert", "trigger", "References ",
      /*"References ", only ddl thread could create reference */ "select", "update",
  };
  
  String[] action = {"grant ", "revoke " };
  String withGrantOption = " WITH GRANT OPTION "; //with grant is not supported yet
  private static boolean hasRoutine = TestConfig.tab().
    booleanAt(SQLPrms.hasRoutineInSecurityTest, false);
  
  
  public void doDDLOp(Connection dConn, Connection gConn) {
    boolean success = false;
    int maxNumOfTries = 1;
    ArrayList<SQLException> exList = new ArrayList<SQLException>();    
    String tableName = tableNames[SQLTest.random.nextInt(tableNames.length)];
    String routineName = (hasRoutine) ? getRoutineNames(dConn, gConn) : null;
    StringBuffer aStr = new StringBuffer();
    
    //grant or revoke
    String act = action[SQLTest.random.nextInt(action.length)];
    aStr.append(act); 
    
    //rest of the authorization stmt
    if (routineName == null) 
      aStr.append(getAuthForTables(tableName, act));
    else
      aStr.append(SQLTest.random.nextBoolean()? getAuthForTables(tableName, act) 
        : getAuthForRoutines(routineName, act)); 
    
    if (dConn != null) {
        try {
          success = applySecurityToDerby(dConn, aStr.toString(), exList);  //insert to derby table 
          int count = 0;
          while (!success) {
            if (count>=maxNumOfTries) {
              Log.getLogWriter().info("Could not get the lock to finish grant/revoke stmt, abort this operation");
              return;
            }
            exList .clear();
            success = applySecurityToDerby(dConn, aStr.toString(), exList);  //retry insert to derby table         
            count++;
          }

          applySecurityToGFE(gConn, aStr.toString(), exList);    //insert to gfe table 
          SQLHelper.handleMissedSQLException(exList);
          gConn.commit();
          dConn.commit();         
        } catch (SQLException se) {
          SQLHelper.printSQLException(se);
          throw new TestException ("execute security statement fails " + TestHelper.getStackTrace(se));
        }  //for verification
      }
      else {
        try {
          applySecurityToGFE(gConn, aStr.toString());    //insert to gfe table 
          gConn.commit();
        } catch (SQLException se) {
          SQLHelper.printSQLException(se);
          throw new TestException ("execute security statement fails " + TestHelper.getStackTrace(se));
        }
      } //no verification  
    
  }
  
  public void assignSelectToMe(Connection dConn, Connection gConn, int tid) {
    boolean success = false;
    int maxNumOfTries = 1;
    ArrayList<SQLException> exList = new ArrayList<SQLException>();    

    //for load test
    String sql = "grant select on trade.buyorders to thr_" + tid;
    
    
    if (dConn != null) {
        try {
          success = applySecurityToDerby(dConn, sql, exList);  //insert to derby table 
          int count = 0;
          while (!success) {
            if (count>=maxNumOfTries) {
              Log.getLogWriter().info("Could not get the lock to finish grant/revoke stmt, abort this operation");
              return;
            }
            exList .clear();
            success = applySecurityToDerby(dConn, sql, exList);  //retry insert to derby table         
            count++;
          }

          applySecurityToGFE(gConn, sql, exList);    //insert to gfe table 
          SQLHelper.handleMissedSQLException(exList);
          gConn.commit();
          dConn.commit();         
        } catch (SQLException se) {
          SQLHelper.printSQLException(se);
          throw new TestException ("execute security statement fails " + TestHelper.getStackTrace(se));
        }  //for verification
      }
      else {
        try {
          applySecurityToGFE(gConn, sql);    //insert to gfe table 
          gConn.commit();
        } catch (SQLException se) {
          SQLHelper.printSQLException(se);
          throw new TestException ("execute security statement fails " + TestHelper.getStackTrace(se));
        }
      } //no verification  
    
  }

  public void createDDLs(Connection dConn, Connection gConn) {
    // TODO Auto-generated method stub

  }
  
  protected String getRoutineNames(Connection dConn, Connection gConn) {
    int n = 10;
    if (SQLTest.random.nextInt(n) == 0) 
      return "function " + getFunctionName(dConn, gConn);
    else 
      return "procedure " + getProcedureName(dConn, gConn);
  }
  
  protected synchronized String getProcedureName(Connection dConn, Connection gConn) {
    /// avoid #42569
    ResultSet rs = null;
    
    try {
      rs = gConn.getMetaData().getProcedures(null, null, null);
      List procNames = ResultSetHelper.asList(rs, false);
      Log.getLogWriter().info("procedure names are " + ResultSetHelper.listToString(procNames));
      rs.close();
    } catch (SQLException se){
      SQLHelper.handleSQLException(se);
    }
    
    try {
      rs = gConn.getMetaData().getFunctions(null, null, null);      
    } catch (SQLException se){
      SQLHelper.handleSQLException(se);
    }   
    List funcNames = ResultSetHelper.asList(rs, false);
    Log.getLogWriter().info("function names are " + ResultSetHelper.listToString(funcNames)); 
    
    if (procedureNames == null) {
      ArrayList<String> procs = new ArrayList<String>();
      procs.addAll(ProcedureDDLStmt.modifyProcNameList);
      procs.addAll(ProcedureDDLStmt.nonModifyProcNameList);
      procedureNames = new ArrayList<String>(procs);
    }
    
    return procedureNames.get(SQLTest.random.nextInt(procedureNames.size()));
  }
  
  protected String getFunctionName(Connection dConn, Connection gConn) {
    if (functionNames == null) functionNames = 
      new ArrayList<String>(FunctionDDLStmt.functionNameList);
    return functionNames.get(SQLTest.random.nextInt(functionNames.size()));
  }
  
  //All privileges or priviege list for a table 
  private String getAuthForTables(String tableName, String act) {
    StringBuffer aStr = new StringBuffer();
    //privilege type
    if (SQLTest.random.nextBoolean()) {
    aStr.append( "ALL PRIVILEGES " );
    } else {
    String privList = getPrivilegeLists(tableName);
    if (privList.equals(" ")) {
      Log.getLogWriter().info("did not get the privilege list");
      aStr.append( "ALL PRIVILEGES " );
    } else {
      aStr.append(privList);
    }
    } //type
    
    //on table tablename to
    if (act.contains("grant "))
      aStr.append(" on " + tableName + " to ");
    else
      aStr.append(" on " + tableName + " from ");  
    
    //grantees
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = null;
    if (usePublic) {
      grantees = (SQLTest.random.nextInt(100) == 0) ? getGrantees(num): " public ";
    } else{
      grantees = getGrantees(num);
    } 
    aStr.append((grantees.equals(" "))? "thr_0" : grantees );    
    
    return aStr.toString();
  }
  
  //return optionally any insert, delete, trigger, select [col list], update [col list]
  private String getPrivilegeLists(String tableName) {
      StringBuffer aStr = new StringBuffer("  ");  
    for (int i=0; i<tablePriv.length; i++) {
    if (SQLTest.random.nextBoolean()) {
      aStr.append(tablePriv[i]); //insert delete etc
      if (i>2) {
      aStr.append(getColumnLists(tableName)); //column list for the table
      } //may include column list
      aStr.append(", ");
    }      
    }
    if (aStr.charAt(aStr.length()-2) == ',') {
    aStr.deleteCharAt(aStr.length()-2);
    }
    aStr.deleteCharAt(0); //delete the leading space
    return aStr.toString();
  }
  
  //returns column list: column-id, column-id ...
  private String getColumnLists(String tableName) {
    List<String> colNames = (List<String>)tableCols.get(tableName);
    StringBuffer aStr = new StringBuffer("  ");
    for (int i=0; i<colNames.size(); i++) {
      if (SQLTest.random.nextBoolean()) {
        aStr.append(colNames.get(i) + ", ");
      }
    }
    if (aStr.charAt(aStr.length()-2) == ',') {
      aStr.deleteCharAt(aStr.length()-2);
    }   
    aStr.deleteCharAt(0); //delete the leading space
    if (aStr.length() != 1) {
      aStr.insert(1, '(');
      aStr.append(')');
    } //has column
    return aStr.toString();
  } 
  
  private String getAuthForRoutines(String routineName, String act) {
    StringBuffer aStr = new StringBuffer();
    //privilege type

    aStr.append( "EXECUTE ON " );

    if (act.contains("grant "))
      aStr.append(routineName + " to ");
    else
      aStr.append(routineName + " from ");  
    
    //grantees
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = null;
    if (usePublic) {
      grantees = (SQLTest.random.nextInt() == 0) ? getGrantees(num): " public ";
    } else{
      grantees = getGrantees(num);
    } 
    aStr.append((grantees.equals(" "))? "thr_0" : grantees );    
    
    if (act.contains("revoke ")) 
      aStr.append(" restrict ");  //revoke privilege for routine must have restrict keyword
    
    return aStr.toString();
   }
  
  protected boolean applySecurityToDerby(Connection dConn, String sql, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("execute authorization statement in derby");
    Log.getLogWriter().info("security statement is: " + sql);
    try {
      Statement stmt = dConn.createStatement();
      stmt.execute(sql); //execute authorization stmt on derby
    } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(dConn, se)) { //handles the deadlock of aborting
          Log.getLogWriter().info("detected the deadlock, will try it again");
          return false;
        } else {
          SQLHelper.handleDerbySQLException(se, exList);
        }
    }
    return true;
  }
  
  protected void applySecurityToGFE(Connection gConn, String sql, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("execute authorization statement in GFE");
    Log.getLogWriter().info("security statement is: " + sql);
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql); //execute authorization stmt 
    } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
    }
  }
  
  
  protected void applySecurityToGFE(Connection gConn, String sql) {
    Log.getLogWriter().info("execute authorization statement in GFE");
    Log.getLogWriter().info("security statement is: " + sql);
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql); //execute authorization 
    } catch (SQLException se) {
      if (se.getSQLState().equals("42506") && SQLTest.testSecurity) 
         Log.getLogWriter().info("Got the expected exception for authorization," +
            " continuing tests");  
      else if (se.getSQLState().equals("42509") && SQLTest.testSecurity) 
        Log.getLogWriter().info("Got the expected grant or revoke operation " +
           "is not allowed exception for authorization," +
           " continuing tests"); 
      else if (se.getSQLState().equals("42Y03") && hasRoutine) 
        Log.getLogWriter().info("Got the expected not recognized as " +
           "a function or procedure exception for authorization," +
           " continuing tests"); 
      else 
        SQLHelper.handleSQLException(se);
    }
  }
  
  public static void turnOnAuthorization(Connection dConn, Connection gConn) {
    String sql = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " +
        "'derby.database.sqlAuthorization', 'true')";
    ArrayList<SQLException> exList = new ArrayList<SQLException>();
    if (dConn != null) {
    turnOnDerbyAuthorization(dConn, sql, exList);
    turnOnGFEAuthorization(gConn, sql, exList);
    SQLHelper.handleMissedSQLException(exList);
    } 
    else turnOnGFEAuthorization(gConn, sql);
  }
  
  public static void turnOnDerbyAuthorization(Connection conn, String sql, 
       ArrayList<SQLException> exList) {
    Log.getLogWriter().info("turn on authorization statement in derby");
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
      ResultSet rs = stmt.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
          "'derby.database.sqlAuthorization')");
      rs.next();
      Log.getLogWriter().info("Value of sqlAuthorization is " + rs.getString(1));
      rs = stmt.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
          "'derby.database.defaultConnectionMode')");
      rs.next();
      Log.getLogWriter().info("Value of defaultConnectionMode is " + rs.getString(1));
      rs.close();
    } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
    }    
  }
  
  public static void turnOnGFEAuthorization(Connection conn, String sql, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("turn on authorization statement in GFE");
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
    }    
  }
  
  public static void turnOnGFEAuthorization(Connection conn, String sql) {
    Log.getLogWriter().info("turn on authorization statement in GFE");
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
    }    
  }
  
  public void provideAllPrivToAll(Connection dConn, Connection gConn) {    
    ArrayList<SQLException> exList = new ArrayList<SQLException>();
    for (int i=0; i<tableNames.length; i++) {
      StringBuffer sql = new StringBuffer();
      sql.append("grant all privileges on " + tableNames[i] + " to " + getAllGrantees());
      Log.getLogWriter().info("security statement is " + sql.toString());
      if (dConn != null) {
        try {
          Statement stmt = dConn.createStatement();
          stmt.execute(sql.toString()); //execute authorization 
          dConn.commit();
          stmt.close();
        } catch (SQLException se) {
          SQLHelper.handleDerbySQLException(se, exList);
        }
        try {
          Statement stmt = gConn.createStatement();
          stmt.execute(sql.toString()); //execute authorization 
          gConn.commit();
          stmt.close();
        } catch (SQLException se) {
          SQLHelper.handleGFGFXDException(se, exList);
        }
      } else {
        try {
          Statement stmt = gConn.createStatement();
          stmt.execute(sql.toString()); //execute authorization 
          gConn.commit();
          stmt.close();
        } catch (SQLException se) {
          SQLHelper.handleSQLException(se);
        } 
      }
    }
  }

  
  //"with grant option" not supported in derby yet
  public void delegateGrantOption(Connection dConn, Connection gConn) {
    if (dConn != null) { 
      for (int i=0; i<tableNames.length; i++) {
        delegateGrantOption(dConn, gConn, tableNames[i]);
      }  
    } else {
      for (int i=0; i<tableNames.length; i++) {
        delegateGrantOption(gConn, tableNames[i]);
      }  
    }
  }
  
  private void delegateGrantOption(Connection dConn, Connection gConn, String tableName) {    
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = getGrantees(num);
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    StringBuffer sql = new StringBuffer();
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    sql.append("grant " + tablePriv[whichPriv] + " on " + tableName + " to " 
        + grantees + withGrantOption);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = dConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      dConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleDerbySQLException(se, exceptionList);
    } 
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptionList);
    }
    SQLHelper.handleMissedSQLException(exceptionList);
  }
  
  private void delegateGrantOption(Connection gConn, String tableName) {    
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = getGrantees(num);
    StringBuffer sql = new StringBuffer();
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    sql.append("grant " + tablePriv[whichPriv] + " on " + tableName + " to " 
        + grantees + withGrantOption);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      if (se.getSQLState().equals("42X01")) {
        Log.getLogWriter().info("Got expected exception as WITH GRANT OPTION " +
            "is not supported yet, continuing test");
      } else {
        SQLHelper.handleSQLException(se);
      }
    }
  }
  

  /* currently only the object owner/or system owner has the right to grant/revoke
   * once With GRANT OPTION is supported, all connections with privileges could grant
   * or revoke the privileges
   */
  public void useDelegatedPrivilege(Connection dConn, Connection gConn) {
    if (dConn != null) { 
      for (int i=0; i<tableNames.length; i++) {
        useDelegatedPrivilege(dConn, gConn, tableNames[i]);
      }  
    } else {
      for (int i=0; i<tableNames.length; i++) {
        useDelegatedPrivilege(gConn, tableNames[i]);
      }  
    }
  }
  
  private void useDelegatedPrivilege(Connection dConn, Connection gConn, String tableName) {
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = getGrantees(num);
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    StringBuffer sql = new StringBuffer();
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    sql.append("grant " + tablePriv[whichPriv] + " on " + tableName + " to " 
        + grantees);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = dConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      dConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleDerbySQLException(se, exceptionList);
    } 
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptionList);
    } 
    SQLHelper.handleMissedSQLException(exceptionList);
  }
  
  private void useDelegatedPrivilege(Connection gConn, String tableName) {
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = getGrantees(num);
    StringBuffer sql = new StringBuffer();
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    sql.append("grant " + tablePriv[whichPriv] + " on " + tableName + " to " 
        + grantees);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } 
  }

  
  public void revokeDelegatedPrivilege(Connection dConn, Connection gConn) { 
    if (dConn != null) { 
      for (int i=0; i<tableNames.length; i++) {
        revokeDelegatedPrivilege(dConn, gConn, tableNames[i]);
      }  
    } else {
      for (int i=0; i<tableNames.length; i++) {
        revokeDelegatedPrivilege(gConn, tableNames[i]);
      }  
    }
  }
  
  private void revokeDelegatedPrivilege(Connection dConn, Connection gConn, String tableName) {
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    String grantees = getGrantees(num);
    StringBuffer sql = new StringBuffer();
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    sql.append("revoke " + tablePriv[whichPriv] + " on " + tableName + " from " 
        + grantees);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = dConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      dConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleDerbySQLException(se, exceptionList);
    } 
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptionList);
    } 
  }
  
  private void revokeDelegatedPrivilege(Connection gConn, String tableName) {
    int num = SQLTest.random.nextInt(SQLTest.numOfWorkers) + 1;
    String grantees = getGrantees(num);
    int whichPriv = SQLTest.random.nextInt(tablePriv.length);
    StringBuffer sql = new StringBuffer();
    sql.append("revoke " + tablePriv[whichPriv] + " on " + tableName + " from " 
        + grantees);
    Log.getLogWriter().info("security statement is " + sql.toString());
    try {
      Statement stmt = gConn.createStatement();
      stmt.execute(sql.toString()); //execute authorization 
      gConn.commit();
      stmt.close();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } 
  }
  
  //return all grantees: thr_0, thr_1, thr_2 ..., excluding itself
  private static String getAllGrantees() {
    StringBuffer aStr = new StringBuffer("  ");
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB()
        .getSharedMap().get(SQLSecurityTest.userPasswdMap);
    for (Map.Entry<String, String> e : userPasswd.entrySet()) {
      if (!e.getKey().equalsIgnoreCase("thr_"+ RemoteTestModule.getCurrentThread().getThreadId()))
        aStr.append(e.getKey() + ", ");
    } 
    if (aStr.charAt(aStr.length()-2) == ',') {
      aStr.deleteCharAt(aStr.length()-2);
    }  
    aStr.deleteCharAt(0); //delete the leading space
    return aStr.toString();
  }
  
  //any number of users excluding itself
  private static String getGrantees(int num) {
    StringBuffer aStr = new StringBuffer("  ");
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB()
        .getSharedMap().get(SQLSecurityTest.userPasswdMap);
    userPasswd.remove("thr_"+ RemoteTestModule.getCurrentThread().getThreadId());
    String[] users = new String[userPasswd.size()];
    userPasswd.keySet().toArray(users);
    int i =0;
    while (i<num) {
      int x = SQLTest.random.nextInt(users.length);
      aStr.append(users[x] + ", ");
      i++;
    }
    
    if (aStr.charAt(aStr.length()-2) == ',') {
      aStr.deleteCharAt(aStr.length()-2);
    }  
    aStr.deleteCharAt(0); //delete the leading space
    return aStr.toString();
  }
  
  private static String getGrantee() {
    return getGrantees(1);
  }

}
