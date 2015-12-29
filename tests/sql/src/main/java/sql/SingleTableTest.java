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
package sql;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import hydra.Log;
import hydra.RemoteTestModule;
import sql.sqlutil.ResultSetHelper;
import util.PRObserver;
import util.TestException;

public class SingleTableTest extends SQLTest {
	protected static SingleTableTest stTest = new SingleTableTest();
	
	public static synchronized void HydraTask_initialize() {
		if (stTest == null) {
			stTest = new SingleTableTest();
			
			PRObserver.installObserverHook();
			PRObserver.initialize(RemoteTestModule.getMyVmid());
		    
		  stTest.initialize();
		}
  }
	
  public static synchronized void HydraTask_createGFXDDB() {
    stTest.createGFXDDB();
  }
  
  public static synchronized void HydraTask_createGFXDDBForAccessors() {
    stTest.createGFXDDBForAccessors();
  }
  
  public static synchronized void HydraTask_createGFXDSchemas() {
    stTest.createGFESchemas();
  }
  
  public static void HydraTask_createGFXDTables(){
    stTest.createGFXDTables();
  }
  
  protected void createGFXDTables() {
    Connection conn = getGFEConnection();
    createTables(conn);
    closeGFEConnection(conn);
  }

  protected void createTables(Connection conn) {
  	String sql = 	"create table trade.networth " +
  			"(cid int not null, cash decimal (30, 15), " +
  			"securities decimal (30, 15), " +
  			"loanlimit int, availloan decimal (30, 15), " +
  			"tid int)";
  	if (SQLTest.isOffheap) {
  	  sql = sql + SQLTest.OFFHEAPCLAUSE;
  	}
  	try {
	  	Statement stmt = conn.createStatement();
	  	stmt.execute(sql);
	  } catch (SQLException se) {
	  	SQLHelper.handleSQLException(se);
	  }
  }
  
  public static void HydraTask_populateTables(){
    stTest.populateTables();
  }
  
  protected void populateTables() {
    Connection gConn = getGFEConnection();
    populateTables(gConn); 
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  protected void populateTables(Connection conn) {
  	insertTable(conn, 100);
  }
  
  protected void insertTable(Connection conn, int size) {
  	int[] cid = new int[size];
    int key = (int) SQLBB.getBB().getSharedCounters().add(SQLBB.tradeCustomersPrimary, size);
    int counter;
    for (int i = 0 ; i <size ; i++) {
      counter = key - i;
      cid[i]= counter;
    }
    
    int initialAmount = 1000;
    BigDecimal cash = new BigDecimal(initialAmount);
    int loanLimit = 1000;
    BigDecimal availLoan = new BigDecimal(loanLimit);
    BigDecimal securities = new BigDecimal(Integer.toString(0));     
    
    //every record has the same initial value
    insertToTable(conn, cid, cash, securities, loanLimit, availLoan, size);
    
  }
  
  protected void insertToTable (Connection conn, int[] cid, BigDecimal cash, 
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int size)  {
    String insert = "insert into trade.networth values (?,?,?,?,?,?)";  
    PreparedStatement stmt = null;
    try {
    	stmt = conn.prepareStatement(insert);
    } catch (SQLException se) {
    	SQLHelper.handleSQLException(se);
    }   
    int tid = getMyTid();
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    for (int i = 0; i < size; i++) {
      try {
        insertToTable(stmt, cid[i], cash, securities, loanLimit, availLoan, tid);
      }  catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }    
    }
  }
  
  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, int cid, BigDecimal cash,
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid)
      throws SQLException {
    Log.getLogWriter().info("inserting into table trade.networth cid is "
        + cid + " cash is " + cash + " securities is " + securities
        + " loanLimit is " + loanLimit + " availLoan is " + loanLimit
        + " tid is " + tid);
    stmt.setInt(1, cid);
    stmt.setBigDecimal(2, cash);
    stmt.setBigDecimal(3, securities);  
    stmt.setInt(4, loanLimit); 
    stmt.setBigDecimal(5, availLoan);  
    stmt.setInt(6, tid);
    int rowCount = stmt.executeUpdate();
    return rowCount;
  }

  protected void createProcedure(Connection conn) {
  	String procedure = "create procedure trade.addInterest2 () " +
	    "PARAMETER STYLE JAVA " +
	    "LANGUAGE JAVA " +
	    "MODIFIES SQL DATA " +
	    "EXTERNAL NAME 'sql.ProcedureTest.addInterest2'";
    try {
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(procedure);    
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected procedrue already exist exception, continuing test");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  protected void exeProcedure(Connection conn) {
  	String procedure =null;    
    if (random.nextBoolean()) {
	    procedure= "create procedure trade.addInterest2 () " +
	    "PARAMETER STYLE JAVA " +
	    "LANGUAGE JAVA " +
	    "MODIFIES SQL DATA " +
	    "EXTERNAL NAME 'sql.ProcedureTest.addInterest2'";
    } else {
    	procedure = "drop procedure trade.addInterest2";
    } 
    try {
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(procedure);    
    } catch (SQLException se) {
      if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94"))
        Log.getLogWriter().info("expected procedrue does not exist exception, continuing test");
      else if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected procedrue already exist exception, continuing test");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  protected void createFunction(Connection conn) {
    String function = "create function multiply(DP1 Decimal (30, 15) ) " + 
	    "RETURNS DECIMAL (30, 15) " + 
	    "PARAMETER STYLE JAVA " +
	    "LANGUAGE JAVA " +
	    "NO SQL " +
	    "EXTERNAL NAME 'sql.FunctionTest.multiply2'";
    
    Log.getLogWriter().info("try to " + function );
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(function);  
    } catch (SQLException se) { 
      if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected function already exist exception, continuing test");
      else
        SQLHelper.handleSQLException(se); 
    }
  }
  
  protected void exeFunction(Connection conn) {
    String function ;
    if (random.nextBoolean()) {
	    function = "create function multiply(DP1 Decimal (30, 15) ) " + 
	    "RETURNS DECIMAL (30, 15) " + 
	    "PARAMETER STYLE JAVA " +
	    "LANGUAGE JAVA " +
	    "NO SQL " +
	    "EXTERNAL NAME 'sql.FunctionTest.multiply2'";
    } else {
    	function = "drop function multiply";
    }
    
    Log.getLogWriter().info("try to " + function + " in gfe if exists");
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(function);  
    } catch (SQLException se) { 
      if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94"))
        Log.getLogWriter().info("expected function does not exist exception, continuing test");
      else if (se.getSQLState().equals("X0Y68"))
        Log.getLogWriter().info("expected function already exist exception, continuing test");
      else
        SQLHelper.handleSQLException(se); 
    }
  }
  
  public static void HydraTask_doOps() {
  	stTest.doOps();
  }
  
  public static void HydraTask_createDDL() {
  	stTest.createDDL();
  }
  
  protected void createDDL() {
  	Connection conn = getGFEConnection();
  	createFunction(conn);
  	createProcedure(conn);
  	closeGFEConnection(conn);
  }
  
  //about 1 in 10 chances
  protected void doDDLOp() { 
  	Connection conn = getGFEConnection();
  	//if (random.nextBoolean()) exeProcedure(conn);
  	//else exeFunction(conn);
  	closeGFEConnection(conn);
  }
  
  protected void doDMLOp() {
    Connection conn = getGFEConnection();
    callProcedure(conn);
    commit(conn);
    closeGFEConnection(conn);
  }
  
  
  protected void callProcedure(Connection conn) {
  	String procedure = "trade.addInterest2";	
  	callProcedure(conn, procedure);
  }
  
  protected static void callProcedure(Connection conn, String whichProc) {
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    Log.getLogWriter().info("call GFE procedure " + whichProc + ", myTid is " + tid);
    try {
      callProcedure(conn, whichProc, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("42Y03")) {
      	//procedure does not exists in concurrent testing
        Log.getLogWriter().info("got expected exception, continuing test");
      } else if (se.getSQLState().equals("38000")) {
      	//function used in procedure does not exists in concurrent testing
        Log.getLogWriter().info("got expected exception, continuing test");
      } else if (se.getSQLState().equals("X0Z01")) {
      	//node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
      } 
      else
        SQLHelper.handleSQLException(se);
    }  
  }
  
  protected static void callProcedure(Connection conn, String proc, 
		  int tid) throws SQLException { 
    CallableStatement cs = null;
    cs = conn.prepareCall("{call " + proc + " ()}");
    Log.getLogWriter().info("call " + proc);
   
    cs.execute();    
    
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  
  public static void HydraTask_verify() {
  	stTest.verify();
  }
  
  protected void verify() {
  	Connection conn = getGFEConnection();
  	String sql = "select cash from trade.networth";
  	List<Struct> results = null;
  	try {
  		Statement stmt = conn.createStatement();
  		ResultSet rs = stmt.executeQuery(sql);  	
  		results = ResultSetHelper.asList(rs, false);  		
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
  	
  	Struct aStruct = results.get(0);
  	for (Struct result: results) {
  		if (!aStruct.equals(result)) {
  			throw new TestException("The cash are not the same for all records : " 
  					+ ResultSetHelper.listToString(results));
  		}
  	}
  	
  }
  

}
