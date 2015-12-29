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
/**
 * 
 */
package sql.ddlStatements;

import java.sql.Connection;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;

import sql.*;
import sql.wan.SQLWanPrms;
import sql.wan.WanTest;
import hydra.*;

/**
 * @author eshu
 *
 */
public class ProcedureDDLStmt implements DDLStmtIF {
  protected boolean dropProc = SQLTest.dropProc;
  
  static String showCustomers = "create procedure trade.show_customers(DP1 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 1 " +
  "EXTERNAL NAME 'sql.ProcedureTest.selectCustomers'";
  
  static String addInterest = "create procedure trade.addInterest(DP1 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.ProcedureTest.addInterest'";
  
  static String testInOutParam = "create procedure trade.testInOutParam(DP1 Integer, " +
      "OUT DP2 DECIMAL (30, 20), INOUT DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 2 " +
  "EXTERNAL NAME 'sql.ProcedureTest.testInOutParam'";
  
  static String queryCustomers = "create procedure trade.query_customers(DP1 Integer , DP2 varchar(500) , OUT DP3 varchar(500)) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "EXTERNAL NAME 'sql.ProcedureTest.queryCustomers'";
  
  static String[] showCustomersByWanSite = null;
  static String[] addInterestByWanSite = null;
  static String[] testInOutParamByWanSite = null;
  
  public static HashMap<String, String> nonModifyProcMap = new HashMap<String, String>(); //key: procedure name, value: procedure statement
  public static HashMap<String, String> modifyProcMap = new HashMap<String, String>(); 
  public static ArrayList<String>  modifyProcNameList = new ArrayList<String>();
  public static ArrayList<String>  nonModifyProcNameList = new ArrayList<String>();
  protected static boolean hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
  static boolean isSingleSitePublisher = TestConfig.tab().
    booleanAt(sql.wan.SQLWanPrms.isSingleSitePublisher, false);
  static boolean isWanTest = TestConfig.tab().booleanAt(SQLWanPrms.isWanTest, false);
  static int numOfWanSites = TestConfig.tab().intAt(SQLWanPrms.numOfWanSites, 1);
  static {
    if (isWanTest && !isSingleSitePublisher) {
      showCustomersByWanSite = new String[numOfWanSites];
      addInterestByWanSite = new String[numOfWanSites];
      testInOutParamByWanSite = new String[numOfWanSites];
      
      for (int i = 0; i< numOfWanSites; i++) {
        showCustomersByWanSite[i] = "create procedure trade.show_customers" + (i+1) +
        "(DP1 Integer) " +
        "PARAMETER STYLE JAVA " +
        "LANGUAGE JAVA " +
        "READS SQL DATA " +
        "DYNAMIC RESULT SETS 1 " +
        "EXTERNAL NAME 'sql.ProcedureTest.selectCustomers'";
        
        addInterestByWanSite[i] = "create procedure trade.addInterest" + (i+1) +
        "(DP1 Integer) " +
        "PARAMETER STYLE JAVA " +
        "LANGUAGE JAVA " +
        "MODIFIES SQL DATA " +
        "EXTERNAL NAME 'sql.ProcedureTest.addInterest'";
        
        testInOutParamByWanSite[i] = "create procedure trade.testInOutParam" + (i+1) +
        "(DP1 Integer, " +
        "OUT DP2 DECIMAL (30, 20), INOUT DP3 Integer) " +
        "PARAMETER STYLE JAVA " +
        "LANGUAGE JAVA " +
        "READS SQL DATA " +
        "DYNAMIC RESULT SETS 2 " +
        "EXTERNAL NAME 'sql.ProcedureTest.testInOutParam'";
      }
    }
  }
  static {   
    if (isWanTest && !isSingleSitePublisher) {
      //need to understand that wan site write behind nature, some rows may not
      //show in the other site yet, unique key by own tid testings for multiple wan publishers
      //are acceptable, except for the unique key issue      
      int myWanSite = WanTest.myWanSite;
      Log.getLogWriter().info("in Procedure, my wan site is " + myWanSite);
      //nonModifyProc
      nonModifyProcNameList.add("trade.show_customers"+myWanSite);
      nonModifyProcMap.put("trade.show_customers"+ myWanSite, showCustomersByWanSite[myWanSite-1]);
      
      nonModifyProcNameList.add("trade.testInOutParam"+myWanSite);
      nonModifyProcMap.put("trade.testInOutParam"+ myWanSite, testInOutParamByWanSite[myWanSite-1] );
      
      //modifyProc
      modifyProcNameList.add("trade.addInterest"+myWanSite);
      modifyProcMap.put("trade.addInterest"+ myWanSite, addInterestByWanSite[myWanSite-1]);  
      
    } else {
      //nonModifyProc
      nonModifyProcNameList.add("trade.show_customers");
      nonModifyProcMap.put("trade.show_customers", showCustomers);
      
      nonModifyProcNameList.add("trade.testInOutParam");
      nonModifyProcMap.put("trade.testInOutParam", testInOutParam );
      
      //modifyProc
      modifyProcNameList.add("trade.addInterest");
      modifyProcMap.put("trade.addInterest", addInterest);    
    }
  }
  private static long lastDDLOpTime = 0;
  private static int waitPeriod = 3; //3 minutes
  private static String lastProcOpTime = "lastProcOpTime";

  /* (non-Javadoc)
   * @see sql.ddlStatements.DDLStmtIF#doDDLOp(java.sql.Connection, java.sql.Connection)
   * this should be invoked in the concurrent dml and ddl statement execution
   * when there is derby server for verification, only one operation on a particular procedure is 
   * allowed (drop, create and the call procedure) due to the limitation
   */
  public void doDDLOp(Connection dConn, Connection gConn) {
    //logic wheter allow concurrent ddl ops in test and whether limit 
    //concurrent ddl ops in the test run
    if (!RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString().
        equalsIgnoreCase("INITTASK")) {
      if (!SQLTest.allowConcDDLDMLOps) {
        Log.getLogWriter().info("This test does not run with concurrent ddl with dml ops, " +
            "abort the op");
        return;
      } else {
        if (SQLTest.limitConcDDLOps) {
          //this is test default setting, only one concurrent procedure op in 3 minutes
          if (! perfDDLOp()) {
            Log.getLogWriter().info("Does not meet criteria to perform concurrent ddl right now, " +
            "abort the op");
            return;
          } else {
            Long lastUpdateTime = (Long) SQLBB.getBB().getSharedMap().get(lastProcOpTime);
            if (lastUpdateTime != null) {
              long now = System.currentTimeMillis();
              lastDDLOpTime = lastUpdateTime;
              //Log.getLogWriter().info(lastProcOpTime + " currently is set to " + lastDDLOpTime);
              //avoid synchronization in the method, so need to check one more time
              if (now - lastDDLOpTime < waitPeriod * 60 * 1000)  {
                SQLBB.getBB().getSharedCounters().zero(SQLBB.perfLimitedProcDDL);
                Log.getLogWriter().info("Does not meet criteria to perform concurrent ddl abort");
                return;
              } 
            }
          }
          
        } else {
          //without limiting concurrent DDL ops
          //this needs to be specifically set to run from now on
        }
        Log.getLogWriter().info("performing conuccrent procedure op in main task");
      }
      
    }
    
    int whichProc = 0;
    String proc = null; //procedure statement
    String procName = null; //procedure name 
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    //get which procedure will be created/dropped

    if (SQLTest.random.nextBoolean()) {
      whichProc = SQLTest.random.nextInt(nonModifyProcNameList.size());
      proc = (String)nonModifyProcMap.get(nonModifyProcNameList.get(whichProc)); 
      procName = (String) nonModifyProcNameList.get(whichProc);
    } else {
      whichProc = SQLTest.random.nextInt(modifyProcNameList.size());
      proc = (String)modifyProcMap.get(modifyProcNameList.get(whichProc));
      procName = (String) modifyProcNameList.get(whichProc); 
    }
    
    //to see if able to perfrom the task on creating/dropping the procedure
    if (hasDerbyServer){  
      if (!Procedures.doOp(procName)) {
        Log.getLogWriter().info("Other threads are performing op on the procedure " 
            + procName + ", abort this operation");
        return; //other threads performing operation on the procedure, abort      
      }
    } 
    
    //some operatioins will drop the procedure, some will create; some will do drop and create
    int choice = 3; //3 choices, 0 do drop only, 1 both drop and create, 2 create only
    int op = SQLTest.random.nextInt(choice);
    //int op = 1; //TODO, comment out this line, so it will be random.
      if ((op == 0 || op ==1) && dropProc) {
      String dropStatement = "drop procedure " + procName;
      if (dConn != null) {
        if (!exeDerbyProcedure(dConn, dropStatement, exceptionList)) {
          Procedures.zeroCounter(procName);
          return; //drop procedure in derby     
        }
        exeGFEProcedure(gConn, dropStatement, exceptionList); //drop procedure in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGFEProcedure(gConn, dropStatement); //create procedure in GFE
      }
    }

    if (op == 1 || op ==2) {
      if (dConn != null) {
        if (!exeDerbyProcedure(dConn, proc, exceptionList)) {
          Procedures.zeroCounter(procName);
          return; //create procedure in derby, does not proceed further if derby failed caused by lock issue    
        }
        exeGFEProcedure(gConn, proc, exceptionList); //create procedure in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGFEProcedure(gConn, proc); //create procedure in GFE
      }
    }
    
    if (hasDerbyServer){ 
      Procedures.zeroCounter(procName);
    } //zero the counter after op is finished
    
    //reset flag/timer for next ddl op
    if (!RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString().
        equalsIgnoreCase("INITTASK")) {
      if (SQLTest.limitConcDDLOps) {
        long now = System.currentTimeMillis();
        SQLBB.getBB().getSharedMap().put(lastProcOpTime, now);
        lastDDLOpTime = now;
        SQLBB.getBB().getSharedCounters().zero(SQLBB.perfLimitedProcDDL);
      }
    }
  }
  
  private boolean perfDDLOp() {
    if (lastDDLOpTime == 0) {
      Long lastUpdateTime = (Long) SQLBB.getBB().getSharedMap().get(lastProcOpTime);
      if (lastUpdateTime == null) return checkBBForDDLOp();
      else lastDDLOpTime = lastUpdateTime;
    }
    
    long now = System.currentTimeMillis();
    if (now - lastDDLOpTime < waitPeriod * 60 * 1000)  {
      return false;
    } else {
      lastDDLOpTime =  (Long) SQLBB.getBB().getSharedMap().get(lastProcOpTime);
      //avoid synchronization in the method, so need to check one more time
      if (now - lastDDLOpTime < waitPeriod * 60 * 1000)  {
        return false;
      } else return checkBBForDDLOp();
    }       
  }
  
  private boolean checkBBForDDLOp() {
    int perfLimitedConcDDL = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(
        SQLBB.perfLimitedProcDDL);
    return perfLimitedConcDDL == 1;
  }
  
  //execute procedure statement in derby
  protected boolean exeDerbyProcedure(Connection dConn, String procedure, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("try to " + procedure + " in derby");
    try {
      exeProcedure(dConn, procedure);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  //create or drop procedures
  protected void exeGFEProcedure(Connection gConn, String procedure, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("try to " + procedure + " in gfe");
    try {
      exeProcedure(gConn, procedure);
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //create or drop procedures
  protected void exeGFEProcedure(Connection gConn, String procedure) {
    Log.getLogWriter().info("try to " + procedure + " in gfe if exists");
    try {
      exeProcedure(gConn, procedure);
    } catch (SQLException se) { 
      if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94"))
        Log.getLogWriter().info("expected procedrue does not exist exception, continuing test");
      else if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected procedrue already exist exception, continuing test");
      else if (se.getSQLState().equals("42507") && SQLTest.testSecurity)
        Log.getLogWriter().info("expected authorization exception, continuing test");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  //create or drop procedures
  protected void exeProcedure(Connection conn, String procedure) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(procedure);    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  
  //created in the init task
  public void createDDLs(Connection dConn, Connection gConn) {
    ArrayList<SQLException> exList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    //modify procedures
    for (int i=0; i<modifyProcNameList.size(); i++) {
      String proc = (String)modifyProcMap.get(modifyProcNameList.get(i));
      if (dConn != null) {     
        exeDerbyProcedure(dConn, proc, exList);
        exeGFEProcedure(gConn, proc, exList);
        SQLHelper.handleMissedSQLException(exList);
      } //create procedures on derby
      else { 
        exeGFEProcedure(gConn, proc); //create procedure1 in GFE
      }   
    }

    
    //non modifiy procedures
    for (int i=0; i<nonModifyProcNameList.size(); i++) {
      String proc = (String)nonModifyProcMap.get(nonModifyProcNameList.get(i));
      if (dConn != null) {     
        exeDerbyProcedure(dConn, proc, exList);
        exeGFEProcedure(gConn, proc, exList);
        SQLHelper.handleMissedSQLException(exList);
      } //create procedures on derby
      else { 
        exeGFEProcedure(gConn, proc); //create procedure1 in GFE
      }   
    }   
  }
  
  
  public void createJSONDDL( Connection gConn) {
    exeGFEProcedure(gConn, queryCustomers); //create procedure1 in GFE         
    }

}
