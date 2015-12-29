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
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlDAP.SQLDAPPrms;
import sql.sqlDAP.SQLDAPTest;
import sql.sqlDAP.DAProcedures;

public class DAPDDLStmt implements DDLStmtIF {
  public static HashMap<String, String> derbyNonModifyProcMap = new HashMap<String, String>(); //key: procedure name, value: procedure statement
  public static HashMap<String, String> gfxdNonModifyProcMap = new HashMap<String, String>(); 
  public static ArrayList<String>  derbyNonModifyProcNameList = new ArrayList<String>();
  public static ArrayList<String>  gfxdNonModifyProcNameList = new ArrayList<String>();
  
  public static HashMap<String, String> derbyModifyProcMap = new HashMap<String, String>(); 
  public static ArrayList<String>  derbyModifyProcNameList = new ArrayList<String>();
  public static HashMap<String, String> gfxdModifyProcMap = new HashMap<String, String>(); 
  public static ArrayList<String>  gfxdModifyProcNameList = new ArrayList<String>();
  
  protected static boolean hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
  protected static boolean cidByRange = TestConfig.tab().booleanAt(SQLDAPPrms.cidByRange, true);
  protected static boolean testServerGroupsInheritence = TestConfig.tab().booleanAt(SQLPrms.testServerGroupsInheritence,false);
  public static boolean updateWrongSG = TestConfig.tab().booleanAt(SQLDAPPrms.updateWrongSG,false);
  public static boolean testCustomProcessor = TestConfig.tab().booleanAt(SQLDAPPrms.testCustomProcessor, false);
  static boolean concurrentDropDAPOp = SQLDAPTest.concurrentDropDAPOp;
  
  static String showDerbyBuyorders = "create procedure trade.showDerbyBuyorders(IN DP1 Integer, " +
      "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 1 " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.selectDerbyBuyordersByTidList'";
  
  static String showGfxdBuyorders = "create procedure trade.showGfxdBuyorders(IN DP1 Integer, " +
      "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 1 " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.selectGfxdBuyordersByTidList'";
  
  static String getListOfDerbyBuyorders = "create procedure trade.getListOfDerbybuyorders(IN DP1 Integer, " +
  "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 1 " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.selectDerbyBuyordersByTidList'";
  
  static String getListOfGfxdBuyorders = "create procedure trade.getListOfGfxdbuyorders(IN DP1 Integer, " +
      "IN DP2 Integer, OUT DP3 Object) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.getListOfGfxdBuyordersByTidList'";
  
  static String showDerbyPortfolio = "create procedure trade.showDerbyPortfolio(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 Integer, IN DP4 Integer, OUT DP5 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 4 " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.selectDerbyPortfolioByCidRange'";

  static String showGfxdPortfolio = "create procedure trade.showGfxdPortfolio(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 Integer, IN DP4 Integer, OUT DP5 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 4 " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.selectGfxdPortfolioByCidRange'";

  static String updateGfxdPortfolioWrongSG = "create procedure trade.updateGfxdPortfolioWrongSG(IN DP1 DECIMAL, " +
  "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.updateGfxdPortfolioWrongSG'";

  static String updateDerbyPortfolioWrongSG = "create procedure trade.updateDerbyPortfolioWrongSG(IN DP1 DECIMAL, " +
  "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.updateDerbyPortfolioWrongSG'";
  
  
  static String updateDerbyPortfolioByCidRange = "create procedure trade.updateDerbyPortfolioByCidRange(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 DECIMAL(30, 20), " +
  "IN DP4 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "Modifies SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.updateDerbyPortfolioByCidRange'";

  static String updateGfxdPortfolioByCidRange = "create procedure trade.updateGfxdPortfolioByCidRange(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 DECIMAL(30, 20), " +
  "IN DP4 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "Modifies SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.updateGfxdPortfolioByCidRange'";
  
  static String updateDerbySellordersSG = "create procedure trade.updateDerbySellordersSG(IN DP1 TIMESTAMP, " +
  "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "Modifies SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.updateDerbySellordersSG'";

  static String updateGfxdSellordersSG = "create procedure trade.updateGfxdSellordersSG(IN DP1 TIMESTAMP, " +
  "IN DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "Modifies SQL DATA " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.updateGfxdSellordersSG'";
  
  static String customDerbyProc = "create procedure trade.customDerbyProc(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 2 " +
  "EXTERNAL NAME 'sql.sqlDAP.DerbyDAPTest.customDerbyProc'";

  static String customGfxdProc = "create procedure trade.customGfxdProc(IN DP1 Integer, " +
  "IN DP2 Integer, IN DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 3 " +
  "EXTERNAL NAME 'sql.sqlDAP.DAPTest.customGfxdProc'";
  
  public static final String SHOWDERBYBUYORDERS = "trade.showDerbyBuyorders";
  public static final String SHOWGFXDBUYORDERS = "trade.showGfxdBuyorders";
  public static final String GETLISTOFGFXDBUYORDERS = "trade.getListOfGfxdbuyorders";
  public static final String GETLISTOFDERBYBUYORDERS = "trade.getListOfDerbybuyorders";
  public static final String SHOWDERBYPORTFOLIO = "trade.showDerbyPortfolio";
  public static final String SHOWGFXDPORTFOLIO = "trade.showGfxdPortfolio";
  
  public static final String UPDATGFXDFPORTFOLIOWRONGSG = "trade.updateGfxdPortfolioWrongSG";
  public static final String UPDATEDERBYPORTFOLIOWRONGSG = "trade.updateDerbyPortfolioWrongSG";
  public static final String UPDATEDERBYPORTFOLIO = "trade.updateDerbyPortfolioByCidRange";
  public static final String UPDATGFXDFPORTFOLIO = "trade.updateGfxdPortfolioByCidRange";
  
  public static final String UPDATEDERBYSELLORDERSSG = "trade.updateDerbySellordersSG";
  public static final String UPDATGFXDFSELLORDERSSG = "trade.updateGfxdSellordersSG";
  
  public static final String CUSTOMGFXDPROC = "trade.customGfxdProc";
  public static final String CUSTOMDERBYPROC = "trade.customDerbyProc";
  
  static {
    //nonModifyProc
    derbyNonModifyProcNameList.add(SHOWDERBYBUYORDERS);
    derbyNonModifyProcMap.put(SHOWDERBYBUYORDERS, showDerbyBuyorders);
    
    gfxdNonModifyProcNameList.add(SHOWGFXDBUYORDERS);
    gfxdNonModifyProcMap.put(SHOWGFXDBUYORDERS, showGfxdBuyorders);
    
    //derbyNonModifyProcNameList.add(GETLISTOFGFXDBUYORDERS); //ticket #42772
    derbyNonModifyProcMap.put(GETLISTOFGFXDBUYORDERS, getListOfDerbyBuyorders);
    
    //gfxdNonModifyProcNameList.add(GETLISTOFGFXDBUYORDERS); //ticket #42772
    gfxdNonModifyProcMap.put(GETLISTOFGFXDBUYORDERS, getListOfGfxdBuyorders);
    
    if (cidByRange) {
      derbyNonModifyProcNameList.add(SHOWDERBYPORTFOLIO);
      derbyNonModifyProcMap.put(SHOWDERBYPORTFOLIO, showDerbyPortfolio);
    
      gfxdNonModifyProcNameList.add(SHOWGFXDPORTFOLIO);
      gfxdNonModifyProcMap.put(SHOWGFXDPORTFOLIO, showGfxdPortfolio);
    }
    
    if (testCustomProcessor) {
      gfxdNonModifyProcNameList.add(CUSTOMGFXDPROC);
      gfxdNonModifyProcMap.put(CUSTOMGFXDPROC, customGfxdProc);
      derbyNonModifyProcNameList.add(CUSTOMDERBYPROC);
      derbyNonModifyProcMap.put(CUSTOMDERBYPROC, customDerbyProc);
    }
    
    //modifyProc
    if (testServerGroupsInheritence) {
      if (updateWrongSG) {
        gfxdModifyProcNameList.add(UPDATGFXDFPORTFOLIOWRONGSG);
        gfxdModifyProcMap.put(UPDATGFXDFPORTFOLIOWRONGSG, updateGfxdPortfolioWrongSG); 
        derbyModifyProcNameList.add(UPDATEDERBYPORTFOLIOWRONGSG);
        derbyModifyProcMap.put(UPDATEDERBYPORTFOLIOWRONGSG, updateDerbyPortfolioWrongSG); 
      }
      
      gfxdModifyProcNameList.add(UPDATGFXDFSELLORDERSSG);
      gfxdModifyProcMap.put(UPDATGFXDFSELLORDERSSG, updateGfxdSellordersSG); 
      derbyModifyProcNameList.add(UPDATEDERBYSELLORDERSSG);
      derbyModifyProcMap.put(UPDATEDERBYSELLORDERSSG, updateDerbySellordersSG); 
    }
    
    derbyModifyProcNameList.add(UPDATEDERBYPORTFOLIO);
    derbyModifyProcMap.put(UPDATEDERBYPORTFOLIO, updateDerbyPortfolioByCidRange); 
    gfxdModifyProcNameList.add(UPDATGFXDFPORTFOLIO);
    gfxdModifyProcMap.put(UPDATGFXDFPORTFOLIO, updateGfxdPortfolioByCidRange); 
    
  }
  
  @Override
  public void createDDLs(Connection dConn, Connection gConn) {
    ArrayList<SQLException> exList = new ArrayList<SQLException>(); 
    //modify procedures
    for (int i=0; i<gfxdModifyProcNameList.size(); i++) {
      String derbyProc = derbyModifyProcMap.get(derbyModifyProcNameList.get(i));
      String gfxdProc = gfxdModifyProcMap.get(gfxdModifyProcNameList.get(i));
      if (dConn != null) {     
        if (derbyProc != null) exeDerbyProcedure(dConn, derbyProc, exList);
        exeGfxdProcedure(gConn, gfxdProc, exList);
        SQLHelper.handleMissedSQLException(exList);
      } //create procedures on derby
      else { 
        exeGfxdProcedure(gConn, gfxdProc); //create procedure1 in GFE
      }   
    }

    //non modifiy procedures
    for (int i=0; i<derbyNonModifyProcNameList.size(); i++) {
      String derbyProc = derbyNonModifyProcMap.get(derbyNonModifyProcNameList.get(i));
      String gfxdProc = gfxdNonModifyProcMap.get(gfxdNonModifyProcNameList.get(i));
      if (dConn != null) {     
        exeDerbyProcedure(dConn, derbyProc, exList);
        exeGfxdProcedure(gConn, gfxdProc, exList);
        SQLHelper.handleMissedSQLException(exList);
      } //create procedures on derby
      else { 
        exeGfxdProcedure(gConn, gfxdProc); //create procedure in gfxd
      }   
    } 
    
    //add the getListOfGfxdBuyorders
  }
  
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
  protected void exeGfxdProcedure(Connection gConn, String procedure, 
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("try to " + procedure + " in gfxd");
    try {
      exeProcedure(gConn, procedure);
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //create or drop procedures
  protected void exeGfxdProcedure(Connection gConn, String procedure) {
    Log.getLogWriter().info("try to " + procedure + " in gfxd if exists");
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

  @Override
  public void doDDLOp(Connection dConn, Connection gConn) {
    int whichProc = 0;
    String derbyProc = null; //derby procedure statement
    String derbyProcName = null; //derby procedure name 
    String gfxdProc = null; //gfxd procedure statement
    String gfxdProcName = null; //gfxd procedure name 

    whichProc = SQLTest.random.nextInt(derbyNonModifyProcNameList.size());
    derbyProc = derbyNonModifyProcMap.get(derbyNonModifyProcNameList.get(whichProc)); 
    derbyProcName = derbyNonModifyProcNameList.get(whichProc);
    gfxdProc = gfxdNonModifyProcMap.get(gfxdNonModifyProcNameList.get(whichProc)); 
    gfxdProcName = gfxdNonModifyProcNameList.get(whichProc);
    
    //to see if able to perfrom the task on creating/dropping the procedure
    if (hasDerbyServer && concurrentDropDAPOp){  
      if (!DAProcedures.doOp(gfxdProcName)) {
        Log.getLogWriter().info("Other threads are performing op on the procedure " 
            + gfxdProcName + ", abort this operation");
        return; //other threads performing operation on the procedure, abort      
      }
   
      doConcurrentDAPOp(dConn, gConn, derbyProc, derbyProcName, gfxdProc, gfxdProcName); 
    
      DAProcedures.zeroCounter(gfxdProcName);
    } //zero the counter after op is finished
    else {
      doConcurrentDAPOp(dConn, gConn, derbyProc, derbyProcName, gfxdProc, gfxdProcName);
    }
  }
  
  protected void doConcurrentDAPOp(Connection dConn, Connection gConn, String derbyProc, 
      String derbyProcName, String gfxdProc, String gfxdProcName) {
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    //get which procedure will be created/dropped
    //some operatioins will drop the procedure, some will create; some will do drop and create
    int choice = 3; //3 choices, 0 do drop only, 1 both drop and create, 2 create only
    int op = SQLTest.random.nextInt(choice);
      if (op == 0 || op ==1) {
      String dropDerbyStatement = "drop procedure " + derbyProcName;
      String dropGfxdStatement = "drop procedure " + gfxdProcName;
      if (dConn != null) {
        if (!exeDerbyProcedure(dConn, dropDerbyStatement, exceptionList) && concurrentDropDAPOp) {
          DAProcedures.zeroCounter(gfxdProcName);
          return; //drop procedure in derby     
        }
        exeGfxdProcedure(gConn, dropGfxdStatement, exceptionList); //drop procedure in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGfxdProcedure(gConn, dropGfxdStatement); //create procedure in GFE
      }
    }

    if (op == 1 || op ==2) {
      if (dConn != null) {
        if (!exeDerbyProcedure(dConn, derbyProc, exceptionList) && concurrentDropDAPOp) {
          DAProcedures.zeroCounter(gfxdProcName);
          return; //create procedure in derby, does not proceed further if derby failed caused by lock issue    
        }
        exeGfxdProcedure(gConn, gfxdProc, exceptionList); //create procedure in GFE
        SQLHelper.handleMissedSQLException(exceptionList);  
      }
      else {
        exeGfxdProcedure(gConn, gfxdProc); //create procedure in GFE
      }
    }
  }
}
