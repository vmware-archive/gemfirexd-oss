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

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import util.TestException;
import hydra.Log;
import hydra.MasterController;
import hydra.Prms;
import hydra.TestConfig;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.AbstractDMLStmt;
import sql.sqlutil.ResultSetHelper;
import sql.wan.SQLWanBB;
import sql.wan.SQLWanPrms;
import sql.wan.WanTest;

/**
 * @author eshu
 *
 */
public class Procedures {
  protected static int maxNumOfTries = 1;
  protected static boolean hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
  static boolean isWanTest = TestConfig.tab().booleanAt(SQLWanPrms.isWanTest, false);
  static boolean isSingleSitePublisher = TestConfig.tab().
		booleanAt(sql.wan.SQLWanPrms.isSingleSitePublisher, false);
  static boolean testPartitionBy = TestConfig.tab().booleanAt(SQLPrms.testPartitionBy,false);
  static boolean modifyPartitionKey = modifyPartitionedKey();
  static boolean isHATest = SQLTest.isHATest;
  protected static boolean dropProc = SQLTest.dropProc;
  
  public static void callProcedures(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    String whichProc = null;
    boolean[] success = new boolean[1];
    Object[] inOut = new Object[2];
    if (dConn!=null) {
      if (SQLTest.random.nextBoolean()) {
      	if (modifyPartitionKey) return; //do not execute when procedure modify partitioned key
      	if (isHATest) return; //do not execute procedures with f1= f1+x (in addIntest)
        whichProc = getModifyProcedure();
        callModifyProcedures(dConn, gConn, whichProc, inOut, exList);
      } else {
        whichProc = getNonModifyProcedure();
        callNonModifyProcedures(dConn, gConn, whichProc, inOut, exList);
      }
    }    
    else {
      if (SQLTest.random.nextBoolean()) {
      	if (modifyPartitionKey) return; //do not execute when procedure modify partitioned key
      	if (isHATest) return; //do not execute procedures with f1= f1+x
        whichProc = getModifyProcedure();
        callGFEProcedure(gConn,whichProc, inOut, success);
      } else {
        whichProc = getNonModifyProcedure();
        ResultSet[] rs = callGFEProcedure(gConn,whichProc, inOut, success);
        if (success[0])
          processRS(rs);
      }
    } //no verification
  }
  
  //modify procedure does not return result sets
  protected static void callModifyProcedures(Connection dConn, Connection gConn, 
		  String whichProc, Object[] inOut, List<SQLException> exList) {
    //to see if able to perfrom the task on calling the procedure, needed for concurrent dml, ddl 
	//as the nested connection on the server will release the lock for the procedure in conn.close()

  	if (hasDerbyServer){  
      if (!doOp(whichProc)) {
        Log.getLogWriter().info("Other threads are performing op on the procedure " 
            + whichProc + ", abort this operation");
        return; //other threads performing operation on the procedure, abort      
      }
    }
    
    int count = 0;
    boolean success = callDerbyModifyProcedure(dConn,whichProc, inOut, exList);
    while (!success) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("could not call the derby procedure due to issues, abort this operation");
        zeroCounter(whichProc);
        return; //not able to call derby procedure
      }
      
      exList.clear();
      count++;
      success = callDerbyModifyProcedure(dConn,whichProc, inOut, exList);
    }
    boolean[] successForHA = new boolean[1];
    callGFEProcedure(gConn,whichProc, inOut, exList, successForHA);
    while (!successForHA[0]) {
    	callGFEProcedure(gConn,whichProc, inOut, exList, successForHA); //retry
    	//TODO current only procedure modifies data as f1=f1+x type, 
    	//which does not run in HA test due to limitation
    	//need to add modify procedure that modify data and could be retried
    }
    SQLHelper.handleMissedSQLException(exList); 
    
    if (hasDerbyServer){ 
      zeroCounter(whichProc);
    } //zero the counter after op is finished
  }
  
  //if derby not able to call procedure due to lock issues (did not hold lock) return false
  protected static boolean callDerbyModifyProcedure(Connection conn, String whichProc, 
		  Object[] inOut, List<SQLException> exList) {
    int tid = AbstractDMLStmt.getMyTid();
    Log.getLogWriter().info("call Derby procedure " + whichProc + ", myTid is " + tid);
    int numOfRS = getNumOfRS(whichProc);
    try {
      callProcedure(conn, whichProc, numOfRS, tid, inOut);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return false;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return true;
  }
  
//non-modify procedure returns result sets
  protected static void callNonModifyProcedures(Connection dConn, Connection gConn, 
		  String whichProc, Object[] inOut, List<SQLException> exList) {
    //to see if able to perfrom the task on calling the procedure, needed for concurrent dml, ddl
    
    if (hasDerbyServer){  
      if (!doOp(whichProc)) {
        Log.getLogWriter().info("Other threads are performing op on the procedure " 
            + whichProc + ", abort this operation");        
        return; //other threads performing operation on the procedure, abort      
      }
    }
    
    Object[] gfxdInOut = new Object[2];
    ResultSet[] derbyRS= callDerbyProcedure(dConn,whichProc, inOut, exList);
    if (derbyRS == null && exList.size()==0) {
      zeroCounter(whichProc);
      return;  //could not get rs from derby, therefore can not verify  
    }
    boolean[] success = new boolean[1];
    ResultSet[] gfeRS = callGFEProcedure(gConn,whichProc, gfxdInOut, exList, success);
    while (!success[0]) {
    	gfeRS = callGFEProcedure(gConn,whichProc, gfxdInOut, exList, success); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    if (derbyRS != null) compareResultSets(derbyRS, gfeRS);
    else {
      zeroCounter(whichProc);
      return;
    }
    
    if (whichProc.startsWith("trade.testInOutParam")) {
      if (gfxdInOut[0] == null && inOut[0] == null)	 {
    	Log.getLogWriter().info("No max cash found, possible no data " +
    			"inserted by this thread or data deleted");
      } else if (gfxdInOut[0] != null && inOut[0] != null) {
        if (!gfxdInOut[0].getClass().equals(BigDecimal.class)) {
          throw new TestException("supposed to get BigDecimal but got " + gfxdInOut[0].getClass());
        } else if (((BigDecimal)inOut[0]).subtract(((BigDecimal)gfxdInOut[0])).longValue() != 0) {
      	  throw new TestException("Got different out parameter value, derby is " 
      			  + ((BigDecimal)inOut[0]).longValue()  + " gfxd is " 
      			  + ((BigDecimal)gfxdInOut[0]).longValue());      	  
        } else {
          Log.getLogWriter().info("maxCash is " + ((BigDecimal)inOut[0]).longValue());
        }
      } else {
    	throw new TestException("Only one outparam has data, derby maxCash is "
    			+ inOut[0] + "gemfirexd maxCash is " + gfxdInOut[0]);
      }
      
      if (!gfxdInOut[1].getClass().equals(Integer.class)) {
        throw new TestException("supposed to get Integer but got " + gfxdInOut[1].getClass());
      }

	  if (((Integer)inOut[1]).intValue()!= ((Integer)gfxdInOut[1]).intValue()) {
	  	  throw new TestException("Got different out parameter value, derby is " 
	  			  + ((Integer)inOut[1]).intValue()  + " gfxd is " 
	  			  + ((Integer)gfxdInOut[1]).intValue());
      } else {
    	Log.getLogWriter().info("out paramet int is " + ((Integer)inOut[1]).intValue());
      }
    }
    
    if (hasDerbyServer){ 
      zeroCounter(whichProc);
    } //zero the counter after op is finished
  }
  
  //this returns result set
  protected static ResultSet[] callDerbyProcedure(Connection conn, 
		  String whichProc, Object[] inOut, List<SQLException> exList) {
    ResultSet[] rs = null;
    int tid = AbstractDMLStmt.getMyTid();
    Log.getLogWriter().info("call Derby procedure " + whichProc + ", myTid is " + tid);
    int numOfRS = getNumOfRS(whichProc);
    try {
      rs = callProcedure(conn, whichProc, numOfRS, tid, inOut);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return null;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return rs;
  }
  
  //returns result set
  protected static ResultSet[] callGFEProcedure(Connection conn, 
		  String whichProc, Object[] inOut, List<SQLException> exList, boolean[] success) {
    ResultSet[] rs = null;
    int tid = AbstractDMLStmt.getMyTid();
    Log.getLogWriter().info("call GFE procedure " + whichProc + ", myTid is " + tid);
    int numOfRS = getNumOfRS(whichProc);
    success[0] = true;
    try {
      rs = callProcedure(conn, whichProc, numOfRS, tid, inOut);
    } catch (SQLException se) {
    	if (se.getSQLState().equals("X0Z01")) {
      	//node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
      	SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
    return rs; 
  }
  
  //returns result set
  protected static ResultSet[] callGFEProcedure(Connection conn, String whichProc,
		  Object[] inOut, boolean[] success) {
    int tid = AbstractDMLStmt.getMyTid();
    Log.getLogWriter().info("call GFE procedure " + whichProc + ", myTid is " + tid);
    int numOfRS = getNumOfRS(whichProc);
    success[0] = true;
    try {
      return callProcedure(conn, whichProc, numOfRS,tid, inOut);
    } catch (SQLException se) {
      if (se.getSQLState().equals("42Y03")) {
      	//procedure does not exists in concurrent testing
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else if (se.getSQLState().equals("38000")) {
      	//function used in procedure does not exists in concurrent testing
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else if (se.getSQLState().equals("X0Z01") && SQLTest.isHATest) {
      	//node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else if (se.getSQLState().equals("42504") && SQLTest.testSecurity) {
        Log.getLogWriter().info("expected authorization exception, continuing test");
        success[0] = false;
      } else if (se.getSQLState().equals("X0Z02") && SQLTest.hasTx) {
        Log.getLogWriter().info("expected conflict exception, continuing test");
        success[0] = false;
      } else
        SQLHelper.handleSQLException(se);
    }  
    return null; // this line won't be executed, either test failed with TestException or resultSet[] is returned
  }
  
  //based on num of param to prepare call.
  protected static ResultSet[] callProcedure(Connection conn, String proc, 
		  int numOfRS, int tid, Object[] inOut) throws SQLException { 
    ResultSet[] rs = new ResultSet[numOfRS];
    int num = getNumOfParams(proc);
    CallableStatement cs = null;
    if (num == 1) {      
      cs = conn.prepareCall("{call " + proc + "(?)}");
      cs.setInt(1, tid);
    } //one paramet will always be tid
    
    else if (num == 0) {      
      cs = conn.prepareCall("{call " + proc + " }");
    } //no params
    
    else if (num == 3) {
      if (proc.startsWith("trade.testInOutParam")) {
    	cs = conn.prepareCall("{call " + proc + " (?, ?, ?)}");
    	cs.registerOutParameter(2, Types.DECIMAL);
    	cs.registerOutParameter(3, Types.INTEGER);    
    	cs.setInt(1, tid);
    	cs.setInt(3, tid);    	
      }
    }
    
    cs.execute(); 
    Log.getLogWriter().info("executed " + proc);
    if (numOfRS>0) {
  	  rs[0] = cs.getResultSet();
  	  int i=1;
  	  while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
  	    Log.getLogWriter().info("has more results");
  		rs[i] = cs.getResultSet();
  		i++;
  	  }
  	  Log.getLogWriter().info("finished setting outgoing result set");
    }
    
    if (proc.startsWith("trade.testInOutParam")) {
      inOut[0] = cs.getBigDecimal(2);
      inOut[1] = new Integer(cs.getInt(3));
      Log.getLogWriter().info("finished setting output value");
    }
    
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rs;
  }
  
  //get the procedures that modify SQL data
  protected static String getModifyProcedure() {
    int whichOne = SQLTest.random.nextInt(ProcedureDDLStmt.modifyProcNameList.size());
    return (String)ProcedureDDLStmt.modifyProcNameList.get(whichOne);    
  }
  
  
  //get the procedures that does not modify SQL data
  protected static String getNonModifyProcedure() {
    int whichOne = SQLTest.random.nextInt(ProcedureDDLStmt.nonModifyProcNameList.size());
    return (String)ProcedureDDLStmt.nonModifyProcNameList.get(whichOne);  
  }
  
  //get how many params so appropriate call method can be used.
  protected static int getNumOfParams(String proc) {
    if (proc.startsWith("trade.show_customers") || proc.startsWith("trade.addInterest") ) 
      return 1;
    if (proc.startsWith("trade.testInOutParam")) {
      Log.getLogWriter().info("param is 3");
      return 3;
    }
      
    else
      return 0;
  }
  
  //get how many results sets will be returned.
  protected static int getNumOfRS(String proc) {
    if (proc.startsWith("trade.show_customers")) 
      return 1;
    else if (proc.startsWith("trade.testInOutParam")) 
      return 2;
    else
      return 0; //include all modify sql data procedures
  }
  
  //return false when not able to get one of the derby result set (most likely could not hold lock 
  //when process result set
  protected static boolean compareResultSets(ResultSet[] derbyRS, ResultSet[] gfeRS) {
    boolean success = true;
    if (derbyRS.length != gfeRS.length) {
      throw new TestException("got different number of resultSets: there are" +
          derbyRS.length + "result sets from derby and " + gfeRS.length + "result sets from gfe");
    }
    for (int i=0; i<derbyRS.length; i++) {
      success = compareResultSets(derbyRS[i], gfeRS[i]);
      if (!success) return success;
    }
    return success;
  }
  
  protected static boolean compareResultSets(ResultSet derbyRS, ResultSet gfeRS) {
    return ResultSetHelper.compareResultSets(derbyRS, gfeRS);
  }
  
  protected static void processRS(ResultSet[] rs){
    for (int i=0; i<rs.length; i++) {
      List<com.gemstone.gemfire.cache.query.Struct> list = ResultSetHelper.asList(rs[i], false);
      if (list == null & isHATest) return; //may encounter node down in HA tests
    }
  }
  
  //only one thread can do operation on the procedure, to see if any other thread is 
  //operating on the procedure
  public static boolean doOp(String procName) {
    int doOp = getCounter(procName);
    int count =0;
    while (doOp != 1) {
      if (count >maxNumOfTries) return false;  //try to operation, if not do not operate
      count ++;
      MasterController.sleepForMs(100 * SQLTest.random.nextInt(30)); //sleep from 0 - 2900 ms
      doOp = getCounter(procName);
    }  
    return true;
  }
  //get counter to see if there is thread operation on the procedure
  public static int getCounter(String procName) {
    if (!dropProc) return 1; 
    //no concurrent dropping procedure, so allow concurrent procedure calls 
    
    if (procName.startsWith("trade.show_customers")) {
      int counter = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.show_customers);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if  (procName.startsWith("trade.addInterest")) {
      int counter = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.addInterest);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    }  else if  (procName.startsWith("trade.testInOutParam")) {
       int counter = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.testInOutParam);
       Log.getLogWriter().info(procName + " counter is " + counter + " ");
       return counter;
    } else 
      return -1;
  }
  
  //zero counter after finished the operation
  public static void zeroCounter(String procName) {
    if (procName.startsWith("trade.show_customers")) {
      Log.getLogWriter().info("zeros counter SQLBB.show_customers");
      SQLBB.getBB().getSharedCounters().zero(SQLBB.show_customers);
    } else if  (procName.startsWith("trade.addInterest")) {
      Log.getLogWriter().info("zeros counter SQLBB.addInterest");
      SQLBB.getBB().getSharedCounters().zero(SQLBB.addInterest);
    } else if  (procName.startsWith("trade.testInOutParam")) {
      Log.getLogWriter().info("zeros counter SQLBB.testInOutParam");
      SQLBB.getBB().getSharedCounters().zero(SQLBB.testInOutParam);;
    }
  }
  
  //currently only cash in networth may be modified
  @SuppressWarnings("unchecked")
  protected static boolean modifyPartitionedKey() {
  	if (testPartitionBy && isWanTest && !isSingleSitePublisher ) {
  		int myWanSite = WanTest.myWanSite;
  		List<String> partitionKey = (List<String>) SQLWanBB.getBB().
  			getSharedMap().get(myWanSite+"_networthPartition");
  		return partitionKey.contains("cash");
  	} else if (testPartitionBy) {
  		List<String> partitionKey = (List<String>) SQLBB.getBB().
  			getSharedMap().get("networthPartition");
  		return partitionKey.contains("cash");
  	} else return false;
  }
  
 public static void callJsonProcedure(Connection conn, 
      int tid) throws SQLException { 
CallableStatement cs = null;
String networth_json =null;
ResultSet rs = conn.createStatement().executeQuery("select  json_details from trade.networth where tid = " + tid  + " and json_details is not null");
if (rs.next() ) {
  networth_json = rs.getString(1);
}
cs = conn.prepareCall("{call  trade.query_Customers(?,?,?)}");
cs.setInt(1, tid);  
cs.setString(2,networth_json);
cs.registerOutParameter(3, Types.VARCHAR);
cs.execute(); 
String networthOut = cs.getString(3);


if (!networth_json.equalsIgnoreCase(networthOut)) {
  throw new TestException("Inout Parameter Mismatch. Input param is: " +  networth_json + " outputParam is " + networthOut );
} 
Log.getLogWriter().info("executed trade.query_Customers ");
}
}
