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
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import hydra.DerbyServerHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.DMLStmtIF;
import sql.sqlutil.ResultSetHelper;
import sql.ClientDiscDBManager;
import util.TestException;
import util.TestHelper;

public class SQLAsyncEventListenerTest extends SQLTest {
	protected static SQLAsyncEventListenerTest sqlAsyncEventListenerTest;
	public static String backendDB_url = TestConfig.tab().stringAt(SQLPrms.backendDB_url, "jdbc:derby:test");
	public static volatile TestException testException = null;
	
	public static synchronized void HydraTask_initialize() {
		if (sqlAsyncEventListenerTest == null) {
			sqlAsyncEventListenerTest = new SQLAsyncEventListenerTest();
		}
  }

  public static void HydraTask_doDMLOp() {
    sqlAsyncEventListenerTest.doDMLOp();
  }
  
  protected void doDMLOp() {    
    Connection gConn = getGFEConnection();
    
    if (setCriticalHeap) resetCanceledFlag();
    if (setTx && isHATest) resetNodeFailureFlag();
    if (setTx && testEviction) resetEvictionConflictFlag();
    
    doDMLOp(null, gConn); //use callbacks to write to back end

    Log.getLogWriter().info("done dmlOp");
  }
  
  protected void doDMLOp(Connection dConn, Connection gConn) {
    Log.getLogWriter().info("performing dmlOp, myTid is " + getMyTid());
    int table = dmlTables[random.nextInt(dmlTables.length)]; //get random table to perform dml
    DMLStmtIF dmlStmt= dmlFactory.createWriterDMLStmt(table); //dmlStmt of a table
    int upto = 10; //maxim records to be manipulated in one op
    int size = random.nextInt(upto);
   
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
    sqlAsyncEventListenerTest.populateTables();
  }
  
  protected void populateTables(Connection dConn, Connection gConn) {
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
  
  public static void HydraTask_createAsyncEventListener() {
  	sqlAsyncEventListenerTest.createAsyncEventListeners();
  }

  protected void createAsyncEventListeners() {
  	try {
  		Connection conn = getGFEConnection();
      if (!useMultipleAsyncEventListener) {
        String basicListener = "BasicAsyncListener";
        createNewAsyncEventTableListener(conn, basicListener);
        return;
      }
      
  		String[] tableNames  = SQLPrms.getTableNames();
  		for (String tableName: tableNames) {
  		    createNewAsyncEventListener(conn, tableName);
  		    conn.commit();
  		}
  		//add employees table
  		createNewAsyncEventListener(conn, "default1.Employees");
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
  }

  protected void createNewAsyncEventListener(Connection conn, String tableName) 
    throws SQLException{
    String[] tableStr = tableName.split("\\.");
    //String schema = tableStr[0];
    String table = tableStr[1];
    String tableListener = table.substring(0, 1).toUpperCase() + 
      table.substring(1) + "AsyncListener";
    
    createNewAsyncEventTableListener(conn, tableListener);        
  }
  
  protected void createNewAsyncEventTableListener(Connection conn, String tableListener)
    throws SQLException {
    boolean isTicket44274Fixed = false;
    int batchSize = random.nextInt(1000) + 100;   
    int batchTimeInterval = random.nextInt(1000) + 1000;
    boolean enableBatchConflation = TestConfig.tab().booleanAt(SQLPrms.enableQueueConflation, false);
    int maxQueueMem = random.nextInt(100) + 50;
    boolean enablePersistence = TestConfig.tab().booleanAt(SQLPrms.enableQueuePersistence, false);

    int alert_Threshold = random.nextInt(1000) + 1000;
    String driverClass =  "org.apache.derby.jdbc.ClientDriver";
    String dbUrl = "jdbc:derby://" + DerbyServerHelper.getEndpoint().getHost() +
    ":" + DerbyServerHelper.getEndpoint().getPort() + "/" + ClientDiscDBManager.getDBName() +
    ";create=true";
    String INIT_PARAM_STR = "org.apache.derby.jdbc.ClientDriver," +
    "jdbc:derby://" + DerbyServerHelper.getEndpoint().getHost() +
    ":" + DerbyServerHelper.getEndpoint().getPort() + "/" + ClientDiscDBManager.getDBName() +
    ";create=true";
    boolean manualStart = random.nextBoolean();
    if (!isTicket44274Fixed) manualStart = true; //work around #44274
    manualStart = false;
    boolean useDefaultManualStart = false;
    
    StringBuilder str = new StringBuilder();    
    str.append("CREATE asyncEventListener ");
    str.append(tableListener);  //id name
    str.append(" ( listenerclass '");
    str.append("sql.sqlCallback.asyncEventListener." + tableListener);
    
    str.append("' initparams '" + INIT_PARAM_STR + "'");
    
    if (random.nextBoolean()) { 
      str.append(" MANUALSTART " + manualStart);      
      Log.getLogWriter().info("asyncEventListener MANUALSTART is " + manualStart);
      
      if (random.nextBoolean()) {
        str.append(" ENABLEBATCHCONFLATION " + enableBatchConflation);
        Log.getLogWriter().info("asyncEventListener ENABLEBATCHCONFLATION is " 
            + enableBatchConflation);
      } else {
        Log.getLogWriter().info("asyncEventListener ENABLEBATCHCONFLATION uses default");
      }
      if (random.nextBoolean()) {
        str.append(" BATCHSIZE " + batchSize);
        Log.getLogWriter().info("asyncEventListener BATCHSIZE is " 
            + batchSize);
      } else {
        Log.getLogWriter().info("asyncEventListener BATCHSIZE uses default");
      }
      if (random.nextBoolean()) {
        str.append(" BATCHTIMEINTERVAL " + batchTimeInterval);
        Log.getLogWriter().info("asyncEventListener BATCHTIMEINTERVAL is " 
            + batchTimeInterval);
      } else {
        Log.getLogWriter().info("asyncEventListener BATCHTIMEINTERVAL uses default");
      }
      if (random.nextBoolean()) {
        str.append(" ENABLEPERSISTENCE " + enablePersistence);
        Log.getLogWriter().info("asyncEventListener ENABLEPERSISTENCE is " 
            + enablePersistence);
      } else {
        Log.getLogWriter().info("asyncEventListener ENABLEPERSISTENCE uses default");
      }
      if (random.nextBoolean()) {
        str.append(" DISKSTORENAME " + dbSynchStore);
        Log.getLogWriter().info("asyncEventListener DISKSTORENAME is " 
            + dbSynchStore);
      } else {
        Log.getLogWriter().info("asyncEventListener DISKSTORENAME  uses default");
      }
      if (random.nextBoolean()) {
        str.append(" MAXQUEUEMEMORY " + maxQueueMem);
        Log.getLogWriter().info("asyncEventListener MAXQUEUEMEMORY is " 
            + maxQueueMem);
      } else {
        Log.getLogWriter().info("asyncEventListener MAXQUEUEMEMORY uses default");
      }
      if (random.nextBoolean()) {
        str.append(" ALERTTHRESHOLD " + alert_Threshold );
        Log.getLogWriter().info("asyncEventListener ALERTTHRESHOLD is " 
            + alert_Threshold);
      } else {
        Log.getLogWriter().info("asyncEventListener ALERTTHRESHOLD uses default");
      }
    } else {
      useDefaultManualStart = true; //default manual start is true
      Log.getLogWriter().info("DBSynchronizer uses default settings");
    }
    
    str.append( " ) ");
  
    str.append("SERVER GROUPS ( " + sgDBSync + " )");
    
    try {
      Statement s = conn.createStatement();
      s.execute(str.toString());
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    
    Log.getLogWriter().info(str.toString());
    
    if (manualStart || useDefaultManualStart)
      startAsynchEvent(conn, tableListener);    
    else 
      startAlreadyStartedAsynchEvent(conn, tableListener); 
        
  }
  
  public static void HydraTask_startAsyncEventListener() {
    sqlAsyncEventListenerTest.startAsyncEventListeners();
  }

  protected void startAsyncEventListeners() {
    try {
      Connection conn = getGFEConnection();
      if (!useMultipleAsyncEventListener) {
        String basicListener = "BasicAsyncListener";
        startAsynchEvent(conn, basicListener);
        return;
      }
      
      String[] tableNames  = SQLPrms.getTableNames();
      for (String tableName: tableNames) {
          startAsynchEvent(conn, tableName);
          conn.commit();
      }
      //add employees table
      startAsynchEvent(conn, "default1.Employees");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void startAsynchEvent(Connection conn, String id) throws SQLException {
    String sql = "CALL SYS.START_ASYNC_EVENT_LISTENER (?)";
    CallableStatement cs1 = conn.prepareCall(sql);
    cs1.setString(1, id);          
    cs1.execute();
    Log.getLogWriter().info(sql);
    Log.getLogWriter().info("manually started the async eventlistener " + id );
  }
  
  public static void HydraTask_stopAsyncEventListener() {
    sqlAsyncEventListenerTest.stopAsyncEventListeners();
  }

  protected void stopAsyncEventListeners() {
    try {
      Connection conn = getGFEConnection();
      if (!useMultipleAsyncEventListener) {
        String basicListener = "BasicAsyncListener";
        stopAsynchEvent(conn, basicListener);
        return;
      }
      
      String[] tableNames  = SQLPrms.getTableNames();
      for (String tableName: tableNames) {
          stopAsynchEvent(conn, tableName);
          conn.commit();
      }
      //add employees table
      stopAsynchEvent(conn, "default1.Employees");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void stopAsynchEvent(Connection conn, String id) throws SQLException {
    String sql = "CALL SYS.STOP_ASYNC_EVENT_LISTENER (?)";
    CallableStatement cs1 = conn.prepareCall(sql);
    cs1.setString(1, id);          
    cs1.execute();
    Log.getLogWriter().info(sql);
    Log.getLogWriter().info("stopped the async eventlistener " + id );
  }
  
  protected void startAlreadyStartedAsynchEvent(Connection conn, String id) {
    boolean noWarning = false;
    boolean noException = false;
    try {
      CallableStatement cs1 = conn.prepareCall("CALL SYS.START_ASYNC_EVENT_LISTENER (?)");
      cs1.setString(1, id);          
      cs1.execute();
      SQLWarning w = cs1.getWarnings();
      if (w != null) 
        SQLHelper.printSQLWarning(w);
      else noWarning = true;
      
      noException = true;
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }

    if (noWarning && noException) {
      throw new TestException ("call SYS.START_ASYNC_EVENT_LISTENER on an already" +
      		" started async event listener does not get warning or exception");
    } else      
      Log.getLogWriter().info("async eventlistener " + id + " has already been started");
  }

  public static void HydraTask_clearTables() {
  	sqlAsyncEventListenerTest.clearTables();  	
  }
  
  protected void clearTables() {
  	if (!hasDerbyServer) return;
  	if (getMyTid() > 6) return; /*work around #42237, which need performance test to track*/
    Connection gConn = getGFEConnection();  
    if (random.nextBoolean() && hasNetworth) clearTablesInOrder(null, gConn);
    else clearTables(null, gConn);
    commit(gConn);
    closeGFEConnection(gConn);   
  }
  
  protected void clearTablesInOrder(Connection dConn, Connection gConn) {
  	clearTables(dConn, gConn, "trade", "buyorders");	
  	//clearTables(dConn, gConn, "trade", "txhistory");	
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
  		 ResultSet rs = gConn.createStatement().executeQuery("select tableschemaname, tablename "
 	        + "from sys.systables where tabletype = 'T' and tableschemaname not like 'SYS%'");
  		 List<Struct> list = ResultSetHelper.asList(rs, false);
  		 for (Struct e: list) {
  			 Object[] table = e.getFieldValues();
  			 clearTables(dConn, gConn, (String)table[0], (String)table[1]);
  		 }  		   		 
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	} 
  	/* to reproduce #42307
  	if (RemoteTestModule.getCurrentThread().getThreadId() == 0)
  		sqlAsyncEventListenerTest.verifyResultSets();
  	*/
  }
  
  protected void clearTables(Connection dConn, Connection gConn, String schema, String table) {
  	//boolean testTruncate = true;
  	int gCount = 0;
  	String delete = "delete from " + schema + "." + table; 	
  	String truncate = "truncate table " + schema + "." + table;
  	try { 	
  		/* work around truncate table issue such as #42377 etc
  		if (RemoteTestModule.getCurrentThread().getThreadId() != 0) {
  			//if (testTruncate) return;
  			Log.getLogWriter().info(delete);
	  		gCount = gConn.createStatement().executeUpdate(delete); 
	  		Log.getLogWriter().info("gemfirexd deletes " + gCount + " rows from " + table );
  		} else {
  			Log.getLogWriter().info(truncate);
    		gCount = gConn.createStatement().executeUpdate(truncate); 
    		Log.getLogWriter().info("gemfirexd truncate table returns " + gCount );
  		} */
			Log.getLogWriter().info(delete);
  		gCount = gConn.createStatement().executeUpdate(delete); 
  		Log.getLogWriter().info("gemfirexd deletes " + gCount + " rows from " + table );
  	} catch (SQLException se) {
  		if (se.getSQLState().equalsIgnoreCase("23503")) {
  			Log.getLogWriter().info("could not delete due to delete restrict in gfxd");
  		} else if (se.getSQLState().equalsIgnoreCase("XCL48")) {
  			Log.getLogWriter().info("could not truncate due to foreign key reference in gfxd");
  		} else
  			SQLHelper.handleSQLException(se);
  	} 
    commit(gConn);
  	
  }
  
  public static void HydraTask_checkTestFalure() {
    long start_time = System.currentTimeMillis();
    while (System.currentTimeMillis() - start_time < maxResultWaitSec * 1000 / 5) {
      if (testException != null) {
        if (!useMultipleAsyncEventListener) {
          Log.getLogWriter().warning("Got AsyncListner test exception.\n" + TestHelper.getStackTrace(testException));
          //Tests allow out of order queuing of parent/child table by same thread, so there might be 
          //retries of the same event -- causing delete or update 0 row
          //will just log the issue and verify after the test run
        }
        else 
          throw new TestException ("AsyncListner test failed.\n" + TestHelper.getStackTrace(testException));
      }
      MasterController.sleepForMs(1000);
    }
  }
  
  public static void HydraTask_alterTableAddListener() {
    sqlAsyncEventListenerTest.alterTableAddListener();
  }
  
  protected void alterTableAddListener() {
    if (addListenerUsingAlterTable && !useMultipleAsyncEventListener) {  
      Connection gConn = getGFEConnection();
      alterTableAddListner(gConn);
      
      commit(gConn);
    }
  }
  
  protected void alterTableAddListner(Connection conn) {    
    try {
      ResultSet rs = conn
          .createStatement()
          .executeQuery(
              "select tableschemaname, tablename "
                  + "from sys.systables where tabletype = 'T' and tableschemaname != '"
                  + GfxdConstants.PLAN_SCHEMA + "' ");
      while (rs.next()) {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);

        String sql = "alter table " + schemaName + "." + tableName + " set AsyncEventListener(BasicAsyncListener)";        
        conn.createStatement().execute(sql);
        Log.getLogWriter().info("executed " + sql );
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
}
