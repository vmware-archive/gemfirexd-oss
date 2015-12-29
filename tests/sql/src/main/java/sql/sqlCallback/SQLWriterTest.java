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
package sql.sqlCallback;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.DMLStmtIF;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class SQLWriterTest extends SQLTest {
	protected static SQLWriterTest sqlWriterTest;
	public static String backendDB_url = TestConfig.tab().stringAt(SQLPrms.backendDB_url, "jdbc:derby:test");
	public static synchronized void HydraTask_initialize() {
		if (sqlWriterTest == null) {
			sqlWriterTest = new SQLWriterTest();
		}
  }

  public static void HydraTask_doDMLOp() {
    sqlWriterTest.doDMLOp();
  }
  
  protected void doDMLOp(Connection dConn, Connection gConn) {
    Log.getLogWriter().info("performing dmlOp, myTid is " + getMyTid());
    int table = dmlTables[random.nextInt(dmlTables.length)]; //get random table to perform dml
    DMLStmtIF dmlStmt= dmlFactory.createWriterDMLStmt(table); //dmlStmt of a table
    int upto = 10; //maxim records to be manipulated in one op
    int size = random.nextInt(upto);
    if (setTx) size = 1; //avoid #43725, partial txn ops sync to derby but txn failed afterwards
   
    if (setCriticalHeap) resetCanceledFlag();
    if (setTx && isHATest) resetNodeFailureFlag();
    if (setTx && testEviction) resetEvictionConflictFlag();
    
    //perform the opeartions
    String operation = TestConfig.tab().stringAt(SQLPrms.dmlOperations);
    if (operation.equals("insert"))
      dmlStmt.insert(dConn, gConn, size);
    else if (operation.equals("update"))
      dmlStmt.update(dConn, gConn, size);
    else if (operation.equals("delete"))
      dmlStmt.delete(dConn, gConn);
    else if (operation.equals("query")) {
    	if (testUniqueKeys)
    		dmlStmt.query(dConn, gConn); //query derby database throughout the test
    	else
    		dmlStmt.query(null, gConn);  //verify at the end of the test, only process the gemfirexd resultSet
    }      
    else
      throw new TestException("Unknown entry operation: " + operation);
    try {
      if (dConn!=null) {
        dConn.commit(); //derby connection is not null;        
        closeDiscConnection(dConn);
        Log.getLogWriter().info("closed the disc connection");
      }
      gConn.commit();
      closeGFEConnection(gConn);      
    } catch (SQLException se) {
      SQLHelper.handleSQLException (se);      
    }      
   
    Log.getLogWriter().info("done dmlOp");
  }
  
  public static void HydraTask_populateTables(){
    sqlWriterTest.populateTables();
  }
  
  protected void populateTables(Connection dConn, Connection gConn) {
    //int initSize = random.nextInt(10)+1;
    //int initSize = 10;
    for (int i=0; i<dmlTables.length; i++) {
      DMLStmtIF dmlStmt= dmlFactory.createWriterDMLStmt(dmlTables[i]);
      dmlStmt.populate(null, gConn);
      try {
        gConn.commit();
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      } 
    }  
  }
  
  public static void HydraTask_createWriter() {
  	sqlWriterTest.createWriters();
  }
  
  protected void createWriters() {
  	try {
  		Connection conn = getGFEConnection();
  		String[] tableNames  = SQLPrms.getTableNames();
  		for (String tableName: tableNames) {
  			createWriter(conn, tableName);
  			conn.commit();
  		}
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
  }
  
  protected void createWriter(Connection conn, String tableName) throws SQLException {
  	String[] str = tableName.split("\\.");
  	String schema = str[0];
  	String table = str[1];
  	String tableWriter = table.substring(0, 1).toUpperCase() + table.substring(1) + "Writer";
		CallableStatement cs = conn.prepareCall("CALL SYS.ATTACH_WRITER(?,?,?,?,?)");
		cs.setString(1, schema);
		cs.setString(2, table);
		cs.setString(3, "sql.sqlCallback.writer." + tableWriter);
		cs.setString(4, backendDB_url);
		cs.setString(5, "");
		cs.execute();  	
  	Log.getLogWriter().info("attach the writer " + tableWriter 
  			+ " for " + tableName);
  }  
  
  public static void HydraTask_clearTables() {
  	sqlWriterTest.clearTables();  	
  }
  
  protected void clearTables() {
  	if (!hasDerbyServer) return;
  	//if (getMyTid() > 6) return; /*work around #42237, which need performance test to track*/
    Connection gConn = getGFEConnection();  
    clearTablesInOrder(null, gConn);
    commit(gConn);
    closeGFEConnection(gConn);   
    
    if (getMyTid() == ddlThread)
      verifyResultSets();
  }
  
  protected void clearTablesInOrder(Connection dConn, Connection gConn) {
  	clearTables(dConn, gConn, "trade", "buyorders");	
  	clearTables(dConn, gConn, "trade", "txhistory");	
  	clearTables(dConn, gConn, "trade", "sellorders");	
  	clearTables(dConn, gConn, "trade", "portfolio");	
  	clearTables(dConn, gConn, "trade", "networth");	
  	clearTables(dConn, gConn, "trade", "customers");	
  	clearTables(dConn, gConn, "trade", "securities");	
  }
  
  //delete all records in the tables
	protected void  clearTables(Connection dConn, Connection gConn) {
  	/*try {
      ResultSet rs = gConn.createStatement().executeQuery("select tableschemaname, tablename "
	  	        + "from sys.systables where tabletype = 'T' ");
	    while (rs.next()) {
	      String schemaName = rs.getString(1);
	      String tableName = rs.getString(2);
	      	            
	      clearTables(dConn, gConn, schemaName, tableName);	
	    }
	    */
  	try {
  	  String sql = "select tableschemaname, tablename "
        + "from sys.systables where tabletype = 'T' and tableschemaname not like 'SYS%'";
  		 ResultSet rs = gConn.createStatement().executeQuery(sql);
  		 Log.getLogWriter().info(sql);
  		 List<Struct> list = ResultSetHelper.asList(rs, false);  	
  		 Log.getLogWriter().info(ResultSetHelper.listToString(list));
  		 for (Struct e: list) {
  			 Object[] table = e.getFieldValues();
  			 clearTables(dConn, gConn, (String)table[0], (String)table[1]);
  		 }  		   		 
  	} catch (SQLException se) {  	  
  		SQLHelper.handleSQLException(se);
  	} 
  	/* to reproduce #42307
  	if (RemoteTestModule.getCurrentThread().getThreadId() == 0)
  		sqlWriterTest.verifyResultSets();
  	*/
  }
  
  protected void clearTables(Connection dConn, Connection gConn, String schema, String table) {
  	boolean testTruncate = false; //due to #42307
  	int gCount = 0;
  	String delete = "delete from " + schema + "." + table; 	
  	String truncate = "truncate table " + schema + "." + table;
  	try { 	
  		//work around truncate table issue such as #42377, #43272 etc
  	  if (setTx) {
  	    if (getMyTid() == ddlThread) {
  	      Log.getLogWriter().info(delete);
	        gCount = gConn.createStatement().executeUpdate(delete); 
	        Log.getLogWriter().info("gemfirexd deletes " + gCount + " rows from " + table );
  	      //single thread execution to avoid conflict exception which could lead to #43725
  	    }
  	    return;
  	  } 
  	  
  	  //for original non txn case
  		if (RemoteTestModule.getCurrentThread().getThreadId() != 0) {
  			if (testTruncate) return;
  			Log.getLogWriter().info(delete);
	  		gCount = gConn.createStatement().executeUpdate(delete); 
	  		Log.getLogWriter().info("gemfirexd deletes " + gCount + " rows from " + table );
  		}
  		else {
  		  if (testTruncate) {
    			Log.getLogWriter().info(truncate);
      		gCount = gConn.createStatement().executeUpdate(truncate); 
      		Log.getLogWriter().info("gemfirexd truncate table returns " + gCount );
  		  }
  		} 
  	} catch (SQLException se) {
  		if (se.getSQLState().equalsIgnoreCase("23503")) {
  			Log.getLogWriter().info("could not delete due to delete restrict in gfxd");
  		} else if (se.getSQLState().equalsIgnoreCase("XCL48")) {
  			Log.getLogWriter().info("could not truncate due to foreign key reference in gfxd");
  		} else if (setTx && se.getSQLState().equalsIgnoreCase("X0Z02")) {
        Log.getLogWriter().info("Got expected conflict exception using txn");
        //TODO do not compare exception here -- 
        //derby may delete the rows but gfxd not
        //may need to add rollback derby op here.
        return;
      } else
  			SQLHelper.handleSQLException(se);
  	} 
    commit(gConn);
  	
  }
}
