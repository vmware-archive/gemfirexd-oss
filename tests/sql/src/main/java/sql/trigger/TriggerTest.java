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
package sql.trigger;

import java.sql.*;
import java.util.*;
import util.*;
import hydra.*;
import hydra.gemfirexd.GfxdHelper;
import hydra.gemfirexd.GfxdHelperPrms;
import sql.sqlutil.*;
import sql.ddlStatements.*;
import sql.dmlStatements.*;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLBB;
import sql.SQLTest;
import sql.trigger.TriggerTestBB;
import sql.DBType;

public class TriggerTest extends SQLTest {
  
	protected static TriggerTest triggerTest;   
	DMLStmtIF dmlStmt;	
	long pauseSec = TestConfig.tab().longAt(TriggerPrms.pauseSec,10);
	boolean audit = TestConfig.tab().booleanAt(TriggerPrms.audit, true);
	boolean manageDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, true);
	boolean createRecursiveTrigger = TestConfig.tab().booleanAt(SQLPrms.createRecursiveTrigger, false);
	
	public static synchronized void HydraTask_initialize() {
		if (triggerTest == null){
			triggerTest = new TriggerTest();    
			triggerTest.initialize();	
		}		
	  }
	
	protected void initialize(){
		super.initialize();
	}
	
	public static void HydraTask_createTriggers() {
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.createProcedures();
		triggerTest.createTriggers();
	}	
	
	protected void createProcedures(){
		createProcedure(DBType.DERBY);
		createProcedure(DBType.GFXD);
	}
	
	protected void createProcedure(DBType dbType) {
		Connection conn = getConnection(dbType);	
		String[] procedureStmts = TriggerPrms.getProcedureStmts();
		List<SQLException> exList = new ArrayList<SQLException>();		 
	    try {
	    	Log.getLogWriter().info("Creating Procedures on " + dbType);
	        Statement stmt = conn.createStatement();	        
	        for (String procedureStmt:procedureStmts){
	          try {
	            stmt.execute(procedureStmt); 
	            Log.getLogWriter().info("Created Java Procedure: " + procedureStmt);
	            commit(conn);
	          } catch (SQLException se) {
	            SQLHelper.handleSQLException(dbType, se, exList);
	          }
	        }
	        stmt.close();
	      } catch (SQLException se) {
	        SQLHelper.handleSQLException(dbType, se, exList);
	        throw new TestException("Create Procedure on " + dbType + " Failed");
	      }	    
	    closeConnection(conn, dbType);  	   
	}
	
	protected void createTriggers(){	
		List<SQLException> exList = new ArrayList<SQLException>();
		if(!hasDerbyServer){
			createTrigger(DBType.GFXD, exList);
			return;
		}
		createTrigger(DBType.DERBY, exList);
		createTrigger(DBType.GFXD, exList);
	}
	
	protected void createTrigger(DBType dbType, List<SQLException> exList){
		Connection conn = getConnection(dbType);
		String[] triggerStmts = TriggerPrms.getTriggerStmts();		
	    try {
	    	Log.getLogWriter().info("Creating Triggers on " + dbType);
	        Statement stmt = conn.createStatement();
	        for (String triggerStmt:triggerStmts) {
	          if (dbType == DBType.DERBY && triggerStmt.contains("CALL readProc")) {
	            Log.getLogWriter().info("do not create this trigger in derby as the procedure should not be run on derby");
	            continue; //this is a work around for #47452, may need to be modified if test changes.
	          }
	          try {
	            Log.getLogWriter().info("Executing: " + triggerStmt + " on " + dbType);
	            stmt.execute(triggerStmt); 
	            Log.getLogWriter().info("Created Trigger: " + triggerStmt + " on " + dbType);
	            commit(conn);
	          } catch (SQLException se) {
	            if (se.getSQLState().equals("0A000") && createRecursiveTrigger) {
	              Log.getLogWriter().info("got expected unsupported exception when creating recursive trigger");
	            }
	            else SQLHelper.handleSQLException(dbType, se, exList);
	          }
	        }
	        stmt.close();
	        commit(conn);
	      } catch (SQLException se) {
	        SQLHelper.handleSQLException(dbType, se, exList);
	      }	 
	      closeConnection(conn, dbType);      
	}

	public static void HydraTask_dropTriggers(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.dropTriggers();
	}
	
	protected void dropTriggers(){	
		List<SQLException> exList = new ArrayList<SQLException>();
		if(!hasDerbyServer){
			dropTrigger(DBType.GFXD, exList);
			return;
		}
		dropTrigger(DBType.DERBY, exList);
		dropTrigger(DBType.GFXD, exList);
	}
	
	protected void dropTrigger(DBType dbType, List<SQLException> exList){	
		Connection conn = getConnection(dbType);
		String[] dropTriggerStmts = TriggerPrms.getDropTriggerStmts();
	    try {
	    	Log.getLogWriter().info("Dropping Triggers on " + dbType);
	        Statement stmt = conn.createStatement();
	        for (String dropTriggerStmt:dropTriggerStmts) {
	          try {
	            stmt.execute(dropTriggerStmt); 
	            Log.getLogWriter().info("Dropped Trigger: " + dropTriggerStmt + " on " + dbType);
	            commit(conn);
	          } catch (SQLException se) {
	            SQLHelper.handleSQLException(dbType, se, exList);
	          }
	        }
	        stmt.close();
	        commit(conn);
	    } catch (SQLException se) {
	        SQLHelper.handleSQLException(dbType, se, exList);  
	    }	  
	    closeConnection(conn, dbType);
	}
	
	public static void HydraTask_createAndDropTriggers(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.dropTriggers();
		triggerTest.pause();
		triggerTest.createTriggers();
		triggerTest.pause();
	}
	
	protected void pause(){
		try{
			Thread.sleep(pauseSec * 1000);
		}catch(InterruptedException ie){
		}
	}
	
	public static void HydraTask_testInsertActions(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testInsertActions();
	}
	
	protected void testInsertActions(){	
		Connection gConn = getGFEConnection();
		if (!hasDerbyServer){
			testInsertActions(null,gConn);
			commit(gConn);
			closeGFEConnection(gConn);
		}else{
			Connection dConn = getDiscConnection();
			testInsertActions(dConn,gConn);
			commit(dConn);
			commit(gConn);
			closeDiscConnection(dConn);
			closeGFEConnection(gConn);
		}
	}
	
	protected void testInsertActions(Connection dConn,Connection gConn){
		// NPE workaround
		dmlStmt = new TradeCustomersInsertStmt();
		dmlStmt.insert(dConn, gConn, 2);	
	}
	
	public static void HydraTask_testDeleteActions(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testDeleteActions();
	}
	
	protected void testDeleteActions(){
		Connection dConn = getDiscConnection();
		Connection gConn = getGFEConnection();
		testDeleteActions(dConn,gConn);
	}
	
	// bug42467 repro, test will be changed after bug fix
	protected void testDeleteActions(Connection dConn,Connection gConn){		
		try{
			String sql = "delete from trade.customers where cid = 1";		
			int dRow = dConn.createStatement().executeUpdate(sql);
			Log.getLogWriter().info("Deleted " + dRow + " row from Derby");			
			int gRow = gConn.createStatement().executeUpdate(sql);			
			Log.getLogWriter().info("Deleted " + gRow + " row from GFXD");	
			commit(dConn);
			commit(gConn);
			closeDiscConnection(dConn);
			closeGFEConnection(gConn);
		} catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
	}
	
	public static void HydraTask_testUpdateActions(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testUpdateActions();
	}
	
	protected void testUpdateActions(){
		Connection dConn = getDiscConnection();
		Connection gConn = getGFEConnection();
		testUpdateActions(dConn,gConn);
	}
	
	// bug42467 repro, test will be changed after bug fix
	protected void testUpdateActions(Connection dConn,Connection gConn){			
		try{
			String sql = "update trade.customers set addr = 'NEWADDR2' where cid = 2";		
			int dRow = dConn.createStatement().executeUpdate(sql);
			Log.getLogWriter().info("Updated " + dRow + " row on DERBY");			
			int gRow = gConn.createStatement().executeUpdate(sql);			
			Log.getLogWriter().info("Updated " + gRow + " row on GFXD");		
			commit(dConn);
			commit(gConn);
			closeDiscConnection(dConn);
			closeGFEConnection(gConn);
		} catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
	}
	
	public static void HydraTask_testDeleteAll(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testDeleteAll();
	}
	
	protected void testDeleteAll(){
		testDeleteAll(DBType.DERBY);
		testDeleteAll(DBType.GFXD);
	}
	
	protected void testDeleteAll(DBType dbType){		
		try{
			Connection conn = getConnection(dbType);
			String sql = "DELETE FROM trade.customers";
			int row = conn.createStatement().executeUpdate(sql);
			Log.getLogWriter().info("Deleted " + row + " row on " + dbType);	
			commit(conn);
			// temporary verification
			String query = "SELECT COUNT(*) FROM trade.customers_audit";
			PreparedStatement stmt = conn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			int num_row = rs.getInt(1);
			Log.getLogWriter().info("Number of rows inserted in audit table : " + num_row);
			rs.close();
			stmt.close();	
			commit(conn);
			// above temporary verification will be removed after bug fix
			closeConnection(conn, dbType);
		} catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
	}
	
	public static void HydraTask_testReadProc(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testReadProc();
	}
	
	// No need to verify before trigger on derby
	protected void testReadProc(){
		Connection gConn = getGFEConnection();
		testReadProc(gConn);
	}
	
	protected void testReadProc(Connection gConn){
		dmlStmt = new TradeCustomersInsertStmt();		
		dmlStmt.insert(null, gConn, 1);	
		long num_insert = TriggerTestBB.getBB().getSharedCounters().incrementAndRead(TriggerTestBB.NUM_INSERT);		
		Log.getLogWriter().info("Num of before insert called :  " + num_insert);
		commit(gConn);
		closeGFEConnection(gConn);
	}
	
	public static void HydraTask_testInsertWriter(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testInsertWriter();
	}
	
	protected void testInsertWriter(){			
		addWriter("trade","customers","sql.trigger.TriggerWriter","insert writer","");
		dmlStmt = new TradeCustomersInsertStmt();
		Connection conn = getGFEConnection();
		dmlStmt.insert(null, conn, 1);
		commit(conn);
		closeGFEConnection(conn);
	}
	
	public static void HydraTask_testDeleteWriter(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testDeleteWriter();
	}
	
	protected void testDeleteWriter(){	
		addWriter("trade","customers","sql.trigger.TriggerWriter","delete writer","");
		Connection conn = getGFEConnection();
		String sql = "delete from trade.customers where cid = 1";	
		try{
			int row = conn.createStatement().executeUpdate(sql);
			Log.getLogWriter().info("Deleted " + row + " row from customers table");
		}catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}			
		commit(conn);
		closeGFEConnection(conn);
	}
	 
	public static void HydraTask_testUpdateWriter(){
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.testUpdateWriter();
	}
	
	protected void testUpdateWriter(){				
		addWriter("trade","customers","sql.trigger.TriggerWriter","update writer","");
		Connection conn = getGFEConnection();
		String sql = "update trade.customers set addr = 'NEWADDR1' where cid = 1";
		try{
			int row = conn.createStatement().executeUpdate(sql);
			Log.getLogWriter().info("Updated " + row + " row on customers table");	
		}catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
		commit(conn);
		closeGFEConnection(conn);
	}
	
	private void addWriter(String schemaName, String tableName,  
			String functionStr, String initInfoStr, String serverGroups){
		Connection conn = getGFEConnection();
		try{
			CallableStatement cs = conn.prepareCall("CALL SYS.ATTACH_WRITER(?,?,?,?,?)");	    
			cs.setString(1, schemaName);
			cs.setString(2, tableName);    
			cs.setString(3, functionStr);   
			cs.setString(4, initInfoStr); 
			cs.setString(5, serverGroups);
			cs.execute();
		}catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
		commit(conn);
		closeGFEConnection(conn);
	}
	
	
	public static void HydraTask_verifyTriggerResults() {
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.verifyTriggerResults();
	}
	
	protected void verifyTriggerResults(){
		if (!manageDerbyServer){
			Connection conn = getGFEConnection();
			Log.getLogWriter().info("Comparing tables on GFXD ..." );
			verifyResultSets(conn, "trade", "customers", "customers_audit");	
		} else {
			Connection dConn = getDiscConnection();
			Connection gConn = getGFEConnection();
			Log.getLogWriter().info("Verifying customers tables on both DBs ..." );
			verifyResultSets(dConn, gConn, "trade", "customers");
		if(audit){
			Log.getLogWriter().info("Verifying customers_audit tables on both DBs ...");
			verifyResultSets(dConn, gConn, "trade", "customers_audit");
		}
		}
	}
  
	protected void verifyResultSets(Connection conn, String schema, String table1, String table2) {
		  	try {
		  		String select1 = "select * from " + schema + "." + table1; 	
		  		String select2 = "select * from " + schema + "." + table2;
		  		ResultSet RS1 = conn.createStatement().executeQuery(select1);
		  		ResultSet RS2 = conn.createStatement().executeQuery(select2);
		  		ResultSetHelper.compareResultSets(RS1, RS2);  
		  	} catch (SQLException se) {
		  		SQLHelper.handleSQLException(se);
		  	} finally {
		      commit(conn);
		  	}
		  }
	
	public static void HydraTask_verifyProcTriggerResults() {
		if (triggerTest == null){
			triggerTest = new TriggerTest(); 
		}
		triggerTest.verifyProcTriggerResults();
	}
	
	protected void verifyProcTriggerResults(){
		long insertCounter = TriggerTestBB.getBB().getSharedCounters().read(TriggerTestBB.NUM_INSERT);
		long procCounter = TriggerTestBB.getBB().getSharedCounters().read(TriggerTestBB.NUM_PROC);
		if (insertCounter != procCounter) {
			Log.getLogWriter().error("Test Failed : Number of procedure calls " + procCounter + "  does not equal to number of insert actions - " + insertCounter );		
		  	throw new TestException ("verify results Failed");	 
		}
	}
		
	public static void readProc() {
		triggerTest.getInsertCount();
		long num_proc = TriggerTestBB.getBB().getSharedCounters().incrementAndRead(TriggerTestBB.NUM_PROC);
		Log.getLogWriter().info("JAVA Stored Procedure is called " + num_proc + "times");	
	}
	
	// Read only SQL 
	protected void getInsertCount() {
		Connection gConn = getGFEConnection();
		String query = "SELECT COUNT(*) FROM trade.customers";
		try{	
			PreparedStatement stmt = gConn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			int num_row = rs.getInt(1);
			Log.getLogWriter().info("Number of rows inserted : " + num_row);
			rs.close();
			stmt.close();	
			closeGFEConnection(gConn);
		}catch (SQLException se){
			SQLHelper.handleSQLException(se);
			throw new TestException("Read SQL Data Failed");
		}
	}
}
