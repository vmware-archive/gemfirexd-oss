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

import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.sql.SQLException;

import sql.SQLHelper;
import sql.SQLTest;
import util.TestException;

public class FunctionDDLStmt implements DDLStmtIF {
  static String portfolio = "create function trade.funcPortf(DP1 Integer) " +
  "RETURNS TABLE " +
  "( " +
  "  cid int, sid int, qty int, availQty int, subTotal decimal(30,20), tid int " +  
  ") " +
  "PARAMETER STYLE DERBY_JDBC_RESULT_SET " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "EXTERNAL NAME 'sql.FunctionTest.readPortfolioDerby'";
  
  static String multiply = "create function trade.multiply(DP1 Decimal (30, 20) ) " + // comment out (30, 20)
  "RETURNS DECIMAL (30, 20) " + //comment out scale and precision (30, 20) to reproduce the bug
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "NO SQL " +
  "EXTERNAL NAME 'sql.FunctionTest.multiply'";
  
  static String maxCid = "create function trade.maxCid(DP1 Integer) " +
  "RETURNS INTEGER " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "EXTERNAL NAME 'sql.FunctionTest.getMaxCid'";
  
  public static HashMap<String, String> functionMap = new HashMap<String, String>(); //key: function name, value: function statement
  public static ArrayList<String>  functionNameList = new ArrayList<String>();
  protected static boolean hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
  protected boolean dropFunc = SQLTest.dropFunc;
  
  static {
    functionNameList.add("trade.multiply");
    functionMap.put("trade.multiply", multiply);
  }
  
  //find out which procedure using the function
  private String findProcedureName(String funcName) {
  	if (funcName.equals("trade.multiply")) {
  		return "trade.addInterest";
  	} else 
  		throw new TestException("incorrect funcName is used");
  }
  
  
  //functions are used in procedures/other operations
  public void doDDLOp(Connection dConn, Connection gConn) {
    int chance = 1000; //reduce the rate of concurrent DDL op to not hit #47362 frequently
    if (!dropFunc || SQLTest.random.nextInt(chance) != 1 ) {
      Log.getLogWriter().info("will not perform drop function");
      return;
    }
    int whichFunction = 0;
    String function = null; //function statement
    String functionName = null; //function name 
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    //get which functions will be created/dropped

    whichFunction = SQLTest.random.nextInt(functionNameList.size());
    function = (String)functionMap.get(functionNameList.get(whichFunction));
    functionName = (String) functionNameList.get(whichFunction); 
    
    //to see if able to perfrom the task on creating/dropping the function
    if (hasDerbyServer){  
      if (!Procedures.doOp(findProcedureName(functionName))) {
        Log.getLogWriter().info("Other threads are performing op using the function " 
            + functionName + ", abort this operation");
        return; //other threads performing operation on the procedure, abort      
      }
    } 
    
    //some operations will drop the function, some will create; some will do drop and create
    int choice = 3; //3 choices, 0 do drop only, 1 both drop and create, 2 create only
    int op = SQLTest.random.nextInt(choice);
    if (op == 0 || op ==1) {
      String dropStatement = "drop function " + functionName;
      if (dConn != null) {
        if (!exeDerbyFunction(dConn, dropStatement, exceptionList)) {
          Procedures.zeroCounter(findProcedureName(functionName));
          return; //drop function in derby     
        }
        if (!exeDerbyFunction(dConn, function, exceptionList)) {
          Procedures.zeroCounter(findProcedureName(functionName));
          try {
            dConn.rollback(); 
            Log.getLogWriter().info("roll back the ops");
          } catch (SQLException se) {
            SQLHelper.handleSQLException(se);
          }
          return; //create function in derby, does not proceed further if derby failed caused by lock issue    
        }
        exeGFEFunction(gConn, dropStatement, exceptionList); //drop function in GFE
        exeGFEFunction(gConn, function, exceptionList); //create Function in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGFEFunction(gConn, dropStatement); //drop functions in GFE
      }
    }

    if (op ==2) {
      if (dConn != null) {
        if (!exeDerbyFunction(dConn, function, exceptionList)) {
          Procedures.zeroCounter(findProcedureName(functionName));
          return; //create function in derby, does not proceed further if derby failed caused by lock issue    
        }
        exeGFEFunction(gConn, function, exceptionList); //create Function in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGFEFunction(gConn, function); //create function in GFE
      }
    }
    
    if (hasDerbyServer){ 
    	Procedures.zeroCounter(findProcedureName(functionName));
    } //zero the counter after op is finished
  }
  
  //execute function statement in derby
  protected boolean exeDerbyFunction(Connection dConn, String function, 
  		ArrayList<SQLException> exList) {
    Log.getLogWriter().info("try to " + function + " in derby");
    try {
      exeFunction(dConn, function);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  protected void exeGFEFunction(Connection gConn, String function, 
  		ArrayList<SQLException> exList) {
    Log.getLogWriter().info("try to " + function + " in gfe");
    try {
      exeFunction(gConn, function);
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  protected void exeGFEFunction(Connection gConn, String function) {
    Log.getLogWriter().info("try to " + function + " in gfe if exists");
    try {
      exeFunction(gConn, function);
    } catch (SQLException se) { 
      if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94"))
        Log.getLogWriter().info("expected function does not exist exception, continuing test");
      else if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected function already exist exception, continuing test");
      else if (se.getSQLState().equals("42507") && SQLTest.testSecurity)
        Log.getLogWriter().info("expected authorization exception, continuing test");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  protected void exeFunction(Connection conn, String function) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(function);    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  

  public void createDDLs(Connection dConn, Connection gConn) {
 		// not used yet
  }
  
  public static void createFuncMultiply (Connection conn) throws SQLException {
	if (conn== null) return;
	
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(multiply);   
    Log.getLogWriter().info("executed " + multiply);
    /*
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning

    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    }   
    */
  }
  
  public static void createFuncMaxCid (Connection conn) throws SQLException {
	if (conn== null) return;
	
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(maxCid);    
    Log.getLogWriter().info("executed " + maxCid);
    /*
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning

    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    }   
    */
	  }

  public static void createFuncPortf(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(portfolio);  
    Log.getLogWriter().info("executed " + portfolio);
    /*
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning

    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    }   
    */
  }
  
  public static void dropFuncPortf(Connection conn) throws SQLException {
	  String dropFuncPortf = "drop function trade.funcPortf";
	  Statement stmt = conn.createStatement();
    stmt.executeUpdate(dropFuncPortf); 
    Log.getLogWriter().info("executed " + dropFuncPortf);
    /*
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning

    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    }   
    */
  }
}
