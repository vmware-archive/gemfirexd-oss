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
package sql.sqlTx;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import sql.GFEDBManager;
import sql.sqlTx.SQLTxBB;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlTxStatements.*;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import sql.sqlutil.GFXDTxHelper;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import hydra.blackboard.AnyCyclicBarrier;

public class SQLTxTest extends SQLTest {
	protected static SQLTxTest sqlTxTest;
	public static boolean isTxTest = true; //default to true
	protected static DMLTxStmtsFactory dmlTxFactory = new DMLTxStmtsFactory();
	private static String thisTxId = "thisTxId";  
	private static boolean oneOpPerTx = TestConfig.tab().booleanAt(SQLPrms.oneOpPerTx, false);
	public static HydraThreadLocal curTxId = new HydraThreadLocal();
	
  public static synchronized void HydraTask_initializeGFXD() {
  	if (sqlTxTest == null) {
  		sqlTxTest = new SQLTxTest();
  		PRObserver.installObserverHook();
  		PRObserver.initialize(RemoteTestModule.getMyVmid());
  	  
  		sqlTxTest.initialize();
    }
  }
	
  protected void initialize() {
  	super.initialize();
  	
  	isTxTest = TestConfig.tab().booleanAt(SQLPrms.hasTx, true);
  }
  
  public static synchronized void HydraTask_createDiscDB() {
    sqlTxTest.createDiscDB();
  }
  
  public static synchronized void HydraTask_createDiscSchemas() {
    sqlTxTest.createDiscSchemas();
  }
  
  public static synchronized void HydraTask_createDiscTables(){
    sqlTxTest.createDiscTables();
  }
  
  //create gfxd database (through connection) without ds
  public static synchronized void HydraTask_createGFXDDB() {
    sqlTxTest.createGFXDDB();
  }
  
  public static synchronized void HydraTask_createGFXDDBForAccessors() {
    sqlTxTest.createGFXDDBForAccessors();
  }
  
  public static void HydraTask_createGFESchemas() {
    sqlTxTest.createGFESchemas();
  }
  
  public static void HydraTask_createGFETables(){
    sqlTxTest.createGFETables();
  }
  
  public static void HydraTask_populateTxTables(){
    sqlTxTest.populateTxTables();
  }
  
  public static void HydraTask_insertCustomersTxConflict() {
  	sqlTxTest.insertCustomersTxConflict();
  }
  
  public static void HydraTask_updateCustomersTxConflict() {
  	sqlTxTest.updateCustomersTxConflict();
  }
  
  public static void HydraTask_doTxDMLOps(){
    sqlTxTest.doTxDMLOps();
  }
  
  public static void HydraTask_doTxOps(){
    sqlTxTest.doTxOps();
  }
  
  public static void HydraTask_testDerby(){ 	
    sqlTxTest.testDerbyForeignKey();
    //sqlTxTest.testDerbyUniqKey();
  	//sqlTxTest.testDerbyConcUpdate();
  	//sqlTxTest.testDerbyRepeatableRead();
  }
  protected void testDerbyUniqKey() {
  	sqlTxTest.createTableUniqKey();  
  	waitForBarrier();
    sqlTxTest.testUniqKey();
  }
  
  protected void createTableUniqKey() {
  	Connection dConn = getDiscConnection();
  	if (getMyTid() == 1) {
  		String t1 = "create table trade.t1 (id int not null, f1 int, " +
  				"constraint t1_pk primary key (id), constraint t1_uniq unique (f1))";
  	 try {	
	  	 dConn.createStatement().execute(t1);
	  	 dConn.commit();
	  	 Log.getLogWriter().info("uniq table created");
  	 } catch (SQLException se) {
  		 SQLHelper.handleSQLException(se);
  	 }
  	}
  }
  
  protected void testUniqKey() {
  	Log.getLogWriter().info("into uniq key testing");
  	Connection dConn = getDiscConnection();
  	int count = 0;
  	try {
  		if (getMyTid() == 1 ) {
  			Log.getLogWriter().info("inserting into t1");
	  		String sql = "insert into trade.t1 values (1, 1)";
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("insert into t1 of row: " + count);
	  	} 
  		if (getMyTid() == 0 ) {
  			MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("inserting into t1");
  			try {
		  		String sql = "insert into trade.t1 values (?, ?)";
		  		java.sql.PreparedStatement stmt = dConn.prepareStatement(sql);
		  		stmt.setInt(1, 2);
		  		stmt.setInt(2, 1);
		  		count = stmt.executeUpdate();
		  		Log.getLogWriter().info("insert into t1 of row:" + count);
  			} catch (SQLException e) {
  				Log.getLogWriter().info("insert into t1 failed");
  				SQLHelper.printSQLException(e);
  			}
	  	}
  		waitForBarrier();
  		getLock();
  		if (getMyTid() == 0 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t2");
	  	}
  		else if (getMyTid() == 1 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t1");
	  	} 
  		releaseLock();
  		waitForBarrier();
  		
  		
  		//concurrent delete and insert
  		if (getMyTid() == 1 ) {
  			MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("deleting from t1 ");
	  		String sql = "delete from trade.t1 where id = 1 ";
  		  //Log.getLogWriter().info("updating t1 ");
	  		//String sql = "update trade.t1 set id = 2 where id = 1 ";
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("delete from t1 of row: " + count);
	  		//Log.getLogWriter().info("update from t1 of row: " + count);
	  	} 
  		if (getMyTid() == 0 ) {
  			//MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("inserting into t2");
  			try {
		  		String sql = "insert into trade.t2 values (?, ?)";
		  		java.sql.PreparedStatement stmt = dConn.prepareStatement(sql);
		  		stmt.setInt(1, 1);
		  		stmt.setInt(2, 1);
		  		count = stmt.executeUpdate();
		  		Log.getLogWriter().info("insert into t2 of row:" + count);
  			} catch (SQLException e) {
  				Log.getLogWriter().info("insert into t2 failed");
  				SQLHelper.printSQLException(e);
  			}
	  	}
  		waitForBarrier();
  		getLock();
  		if (getMyTid() == 0 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t2");
	  	}
  		else if (getMyTid() == 1 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t1");
	  	} 
  		releaseLock();
  		waitForBarrier();
  		
  	}catch (SQLException se) {
 		 SQLHelper.handleSQLException(se);
 	 }
  }
  
  protected void testDerbyForeignKey() {
  	sqlTxTest.createTableForeignKey();  
  	waitForBarrier();
    sqlTxTest.testForeignKey();
  }
  
  protected void testDerbyConcUpdate() {
  	sqlTxTest.createTableUpdate();  
  	waitForBarrier();
    sqlTxTest.testConcUpdate();
  }
  
  protected void testDerbyRepeatableRead() {
  	sqlTxTest.createTableUpdate();  
  	waitForBarrier();
    sqlTxTest.testRepeatableRead();
  }
  
  protected void createTableUpdate() {
  	//Connection dConn = getDiscConnection();
  	Connection dConn = getGFEConnection();
  	if (getMyTid() == 1) {
  		String t3 = "create table trade.t3 (id int not null, f1 varchar(10), f2 int, " +
  				"constraint t3_pk primary key (id))";
  	 try {	
	  	 dConn.createStatement().execute(t3);
	  	 dConn.commit();
	  	 Log.getLogWriter().info("tables are created");
	  	
	  	int count =0;
 			Log.getLogWriter().info("inserting into t3");
  		String sql = "insert into trade.t3 values (1, 'red', 2)";
  		count = dConn.createStatement().executeUpdate(sql);
  		Log.getLogWriter().info("insert into t3 for " + count + " row: " + sql);
  		
  		sql = "insert into trade.t3 values (2, 'yellow', 201)";
  		count = dConn.createStatement().executeUpdate(sql);
  		Log.getLogWriter().info("insert into t3 for " + count + " row: " + sql);
	  	 
  		dConn.commit();
  	 } catch (SQLException se) {
  		 SQLHelper.handleSQLException(se);
  	 }
  	}
  }
  
  protected void testConcUpdate() {
  	Log.getLogWriter().info("into concurrent update testing");
  	//Connection dConn = getDiscConnection();
    Connection dConn = getGFEConnection();
  	int count = 0;
  	String sql = null;
  	try {
  		dConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  		//dConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
  		//dConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  		printIsolationLevel(dConn);
  		if (getMyTid() == 1 ) {
	  		sql = "update trade.t3 set f1='green' where f2>100";
	  		Log.getLogWriter().info("updating t3 " + sql );
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("update t3 number of row: " + count);
	  	} 
  		waitForBarrier();
  		if (getMyTid() == 0 ) {
  			try {
		  		//sql = "update trade.t3 set f2=101 where id =1";
  				sql = "update trade.t3 set f2=1 where id =2";
		  		Log.getLogWriter().info("updating t3 " + sql );
		  		count = dConn.createStatement().executeUpdate(sql);
		  		Log.getLogWriter().info("update t3 number of row: " + count);
		  		dConn.commit();
		  		Log.getLogWriter().info(sql + " is committed");
  			} catch (SQLException e) {
  				Log.getLogWriter().info("update to t3 failed " + sql);
  				SQLHelper.printSQLException(e);
  			}
  		}
  		waitForBarrier();
  		if (getMyTid() == 1 ) {
  			/*
	  		sql = "update trade.t3 set f1='green' where id=2";
	  		Log.getLogWriter().info("updating t3 " + sql );
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("update t3 number of row: " + count);
	  		*/
  			dConn.commit();
  			sql = "select * from trade.t3";
  			ResultSet rs = dConn.createStatement().executeQuery(sql);
  			List<Struct> list = ResultSetHelper.asList(rs, true);
  			Log.getLogWriter().info("table list is " + ResultSetHelper.listToString(list));
	  	} 
  		
  	}catch (SQLException se) {
 		 SQLHelper.handleSQLException(se);
 	 }
  }
  
  
  protected void testRepeatableRead() {
  	Log.getLogWriter().info("into derby repeatable read testing");
  	Connection dConn = getDiscConnection();
  	int count = 0;
  	String sql = null;
  	try {
  		dConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
  		printIsolationLevel(dConn);
  		if (getMyTid() == 1 ) {
	  		sql = "select * from trade.t3 where f2>100";
	  		Log.getLogWriter().info("querying t3: " + sql );
	  		ResultSet rs = dConn.createStatement().executeQuery(sql);
	  		Log.getLogWriter().info("query results are: " + 
	  				ResultSetHelper.listToString(ResultSetHelper.asList(rs, true)));
	  	} 
  		waitForBarrier();
  		if (getMyTid() == 0 ) {
  			try {
		  		sql = "update trade.t3 set f1='green' where id =1";
		  		Log.getLogWriter().info("updating t3 " + sql );
		  		count = dConn.createStatement().executeUpdate(sql);
		  		Log.getLogWriter().info("update t3 number of row: " + count);
		  		dConn.commit();
		  		Log.getLogWriter().info(sql + " is committed");
  			} catch (SQLException e) {
  				Log.getLogWriter().info("update to t3 failed " + sql);
  				SQLHelper.printSQLException(e);
  			}
  		}
  		waitForBarrier();
  		if (getMyTid() == 1 ) {
  			dConn.commit();
  			sql = "select * from trade.t3";
  			ResultSet rs = dConn.createStatement().executeQuery(sql);
  			List<Struct> list = ResultSetHelper.asList(rs, true);
  			Log.getLogWriter().info("table list is " + ResultSetHelper.listToString(list));
	  	} 
  		
  	}catch (SQLException se) {
 		 SQLHelper.handleSQLException(se);
 	 }
  }
  
  protected void createTableForeignKey() {
  	Connection dConn = getDiscConnection();
  	if (getMyTid() == 1) {
  		String t1 = "create table trade.t1 (id int not null, f1 int, " +
  				"constraint t1_pk primary key (id))";
  		String t2 = "create table trade.t2 (id int not null, t1id int, " +
  				"constraint t2_pk primary key (id), constraint t2_fk foreign key (t1id) " +
  				"references trade.t1 (id) on delete restrict)";  		
  	 try {	
	  	 dConn.createStatement().execute(t1);
	  	 dConn.createStatement().execute(t2);
	  	 dConn.commit();
	  	 Log.getLogWriter().info("tables are created");
  	 } catch (SQLException se) {
  		 SQLHelper.handleSQLException(se);
  	 }
  	}
  }
  
  protected void testForeignKey() {
  	Log.getLogWriter().info("into foreign key testing");
  	Connection dConn = getDiscConnection();
  	int count = 0;
  	try {
  		if (getMyTid() == 1 ) {
  			Log.getLogWriter().info("inserting into t1");
	  		String sql = "insert into trade.t1 values (1, 1)";
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("insert into t1 of row: " + count);
	  	} 
  		if (getMyTid() == 0 ) {
  			MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("inserting into t2");
  			try {
		  		String sql = "insert into trade.t2 values (?, ?)";
		  		java.sql.PreparedStatement stmt = dConn.prepareStatement(sql);
		  		stmt.setInt(1, 1);
		  		stmt.setInt(2, 1);
		  		count = stmt.executeUpdate();
		  		Log.getLogWriter().info("insert into t2 of row:" + count);
  			} catch (SQLException e) {
  				Log.getLogWriter().info("insert into t2 failed");
  				SQLHelper.printSQLException(e);
  			}
	  	}
  		waitForBarrier();
  		getLock();
  		if (getMyTid() == 0 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t2");
	  	}
  		else if (getMyTid() == 1 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t1");
	  	} 
  		releaseLock();
  		waitForBarrier();
  		/*
  		//concurrent delete and insert
  		if (getMyTid() == 1 ) {
  			MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("deleting from t1 ");
	  		String sql = "delete from trade.t1 where id = 1 ";
  		  //Log.getLogWriter().info("updating t1 ");
	  		//String sql = "update trade.t1 set id = 2 where id = 1 ";
	  		count = dConn.createStatement().executeUpdate(sql);
	  		Log.getLogWriter().info("delete from t1 of row: " + count);
	  		//Log.getLogWriter().info("update from t1 of row: " + count);
	  	} 
  		if (getMyTid() == 0 ) {
  			//MasterController.sleepForMs(2000);
  			Log.getLogWriter().info("inserting into t2");
  			try {
		  		String sql = "insert into trade.t2 values (?, ?)";
		  		java.sql.PreparedStatement stmt = dConn.prepareStatement(sql);
		  		stmt.setInt(1, 1);
		  		stmt.setInt(2, 1);
		  		count = stmt.executeUpdate();
		  		Log.getLogWriter().info("insert into t2 of row:" + count);
  			} catch (SQLException e) {
  				Log.getLogWriter().info("insert into t2 failed");
  				SQLHelper.printSQLException(e);
  			}
	  	}
  		waitForBarrier();
  		getLock();
  		if (getMyTid() == 0 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t2");
	  	}
  		else if (getMyTid() == 1 ) {
  			dConn.commit();
	  		Log.getLogWriter().info("commit insert into t1");
	  	} 
  		releaseLock();
  		waitForBarrier();
  		*/
  	}catch (SQLException se) {
 		 SQLHelper.handleSQLException(se);
 	 }
  }
  
  //when no verification is needed, pass dConn as null
  protected void populateTxTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDTxConnection(); 	
    populateTxTables(dConn, gConn);   
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  protected void doTxDMLOps() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDTxConnection(); 	
    doTxDMLOps(dConn, gConn);   
  }
  
  protected void doTxOps() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDTxConnection(); 
    if (random.nextInt(numOfWorkers/2) == 0) 
      doTxDDLOps(dConn, gConn);
    else
    	doTxDMLOps(dConn, gConn);   
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  //if dConn is not null, insert same records to both derby and gemfirexd
  protected void populateTxTables(Connection dConn, Connection gConn) { 
  	//populate table without txId
		Log.getLogWriter().info("setting hydra thread local curTxId to " + 0);
		curTxId.set(0);
    for (int i=0; i<dmlTables.length; i++) {
      DMLTxStmtIF dmlTxStmt= dmlTxFactory.createDMLTxStmt(dmlTables[i]);
      if (dmlTxStmt != null) {
      	dmlTxStmt.populateTx(dConn, gConn);
      	if (dConn !=null) waitForBarrier(); //added to avoid unnecessary foreign key reference error
      }
    }  
  }
  
  //need to make sure that ddl op also needs 
  protected void doTxDDLOps(Connection dConn, Connection gConn) {
  	//GFXD only
  	if (dConn ==null) {
  		doGFXDTxDDLOps(gConn);
  		return;
  	}
  	
    //with derby server to verify results
  	  	
    //before other dml ops committing
  	waitForBarrier();   	
  	
  	doVariousTxDDLOps(dConn, gConn); //during other dml ops commit time
  	
  	waitForBarrier(); //after other dml ops committing
  	
  }
  
  protected void doVariousTxDDLOps(Connection dConn, Connection gConn) {
  	//TODO currently only ddl is create/drop index, will need to add procedures 
  	//alter table and other ddl ops 
  	createIndex(gConn);
  	try {
  		gConn.commit();
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
  	//call procedures that modify SQL data, need to acquire/release lock
  	//so that only one thread could commit its ddl/dml operation  	
  }
  
  //only gfxd operations
  protected void doGFXDTxDDLOps(Connection gConn) {
  	//TODO to add other ddl operations
  	createIndex(); 
  	
  	//other ddl opeations could also occur at any time
  	
  }
  
  public static void HydraTask_setTableCols() {
    sqlTxTest.setTableCols();
  }
  
  //if dConn is not null, insert same records to both derby and gemfirexd
  protected void doTxDMLOps(Connection dConn, Connection gConn) { 	
  	//GFXD only
  	if (dConn ==null) {
  		doGFXDTxDMLOps(gConn);
  		return;
  	}
  	
  	//with derby server to verify results
  	int txId = (int) SQLTxBB.getBB().getSharedCounters().incrementAndRead(SQLTxBB.txId);
  	int maxOps = 10;
  	
  	int numOfOps = 0;
  	if (oneOpPerTx) numOfOps = 1;
  	else numOfOps = random.nextInt(maxOps)+1;
  	
  	Log.getLogWriter().info("This tx " + txId + " will perform " +  numOfOps + " operations.");
		HashMap<String, Integer> modifiedKeys = new HashMap<String, Integer>();
		modifiedKeys.put(thisTxId, txId);
		ArrayList<SQLException>dExList = new ArrayList<SQLException>();
		ArrayList<SQLException> sExList = new ArrayList<SQLException>();
		ArrayList<Object[]> derbyOps = new ArrayList<Object[]>();
		SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
		if (sql.dmlStatements.AbstractDMLStmt.byCidRange) {
			int cid = random.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary));
			Log.getLogWriter().info("txId " + txId + " use cids colocated with " + cid);
			SQLBB.getBB().getSharedMap().put("cid_txId_"+txId, cid);
		} //partitioned by cid range, put a random cid for this tx
		//set threadLocal for txId
		Log.getLogWriter().info("setting hydra thread local curTxId to " + txId);
		curTxId.set(txId);
		
  	for (int i=0; i<numOfOps; i++) {
  		doTxDMLOneOp(dConn, gConn, modifiedKeys, dExList, sExList );		
  	}
  	
  	//perform commit
  	boolean rollback = false;
  	boolean expectFailure = false;
  	boolean gotException = false;
  	boolean ignoreInDoubtTx = false;
  	SQLException gfxdCommitEx = null;
  	hydra.blackboard.SharedMap modifiedKeysByOtherTx = null;
  	if (random.nextInt(numOfWorkers) == 1) rollback = true;  //add rollback for gfxd
  	
  	waitForBarrier(); //before commit or rollback
  	
  	if (rollback) {
  		try {
  			Log.getLogWriter().info("rollback the operation in gfxd");
  			gConn.rollback(); //nothing to concern if op rolled back
  		} catch (SQLException se) {
	   		SQLHelper.handleSQLException(se);
	   	}
  	} //end rollback
   	
   	else { //for commit
	  	getLock();
	  	
	  	//commit gConn
	    try {
	    	modifiedKeysByOtherTx = SQLTxBB.getBB().getSharedMap();
	  		int beforeSize = modifiedKeysByOtherTx.size();
	  		Log.getLogWriter().info("before the commit, the tx bb map size is " + beforeSize);
	  		modifiedKeys.remove(thisTxId); //thisTxid is not a modified key
	  		int mapSize = modifiedKeys.size();
	  		Log.getLogWriter().info("to commit, modified key map size is " + mapSize);
	  		HashMap<String, Integer> mapIfCommit = new HashMap<String, Integer>();
	  		mapIfCommit.putAll(modifiedKeys); //duplicate copy of modifiedKeys
	  		mapIfCommit.putAll(modifiedKeysByOtherTx.getMap());  		
	  		int afterSize = mapIfCommit.size();
	  		Log.getLogWriter().info("if commit, the tx bb map size is " + afterSize);
	  		if (beforeSize + mapSize == afterSize)
	  			Log.getLogWriter().info("committing gfxd tx, does not expect failure");
	  		else {
	  			//check for txhistory table (non primary key case)
		    	for (String key: modifiedKeys.keySet()) {
		    		if (modifiedKeysByOtherTx.containsKey(key)) {
		    			if (modifiedKeys.get(key).intValue() == TradeTxHistoryDMLTxStmt.TXHISTORYINSERT
		    					&&  ((Integer)modifiedKeysByOtherTx.get(key)).intValue() == 
		    						TradeTxHistoryDMLTxStmt.TXHISTORYINSERT){
		    				//both are insert for the key, is accepted and not expect to fail	
		    				Log.getLogWriter().info("Got two tx to insert same in txHisotry, this is " +
		    						" acceptable for non primary key table, this key is " + key);
		    			}	else {
		    				expectFailure = true;
		    				Log.getLogWriter().info("committing gfxd tx, expects failure");
		    			}		    				
		    		}
		    	}  			
	  		}
	    	gConn.commit(); //to commit and avoid rollback the successful operations
	    } catch (SQLException se) {
	    	//TODO to modify test once #41475/#41476 is fixed
	    	// to handle when appropriate sql exception gfxd is thrown during commit time
	    	//when one of the dml operatin encounter an exceptin such as "23505"
	    	SQLHelper.printSQLException(se);
	    	gotException = true;
	    	if (se.getSQLState().equalsIgnoreCase("X0Z02") && !expectFailure) {
	    		throw new TestException("Got commit conflict exception, " +
	    				"but it should not as the tx does not have conflict keys");    		
	    	} else if (se.getSQLState().equalsIgnoreCase("X0Z02") && expectFailure) {
	    		Log.getLogWriter().info("Got the expected commit Conflict exception");
	    	} else if (se.getSQLState().equalsIgnoreCase("X0Z05") && oneOpPerTx) {
	    		Log.getLogWriter().info("For one operation per Tx, in doubt tx could be ignored");
	    		gfxdCommitEx = se;
	    		ignoreInDoubtTx = true;
	    	}
	    	  else SQLHelper.handleSQLException(se);
	    }
	    
	    if (expectFailure && !gotException) {
	    	StringBuffer str = new StringBuffer();
	    	StringBuffer txHistoryStr = new StringBuffer();
	    	str.append("This tx " + txId + " has modified the following keys " +
	    			"which are modified by other tx:\n" );
	    	for (String key: modifiedKeys.keySet()) {
	    		if (modifiedKeysByOtherTx.containsKey(key)) {
	    			if (modifiedKeys.get(key).intValue() <0) {
		    			if (modifiedKeys.get(key).intValue() == TradeTxHistoryDMLTxStmt.TXHISTORYINSERT
		    					&&  ((Integer)modifiedKeysByOtherTx.get(key)).intValue() == 
		    						TradeTxHistoryDMLTxStmt.TXHISTORYINSERT){
		    				//expected for both insert, do nothing
		    			} else {
		    				txHistoryStr.append (key + " in txHistory is modified/inserted in more than " +
		    						"one transaction concurrently");
		    			}
	    			}	//find out if they are txHistory case  			
	    			else {	
	    				str.append(key + " is already modified by txId: " + modifiedKeysByOtherTx.get(key) + "\n");
	    			}
	    		}
	    	}
	    	
	  		throw new TestException("Expect commit conflict exception, " +
					"but the tx does not get it\n" + str.toString()
					+ "\n" + txHistoryStr.toString());  
	 
	    }
	    
	    if (!gotException) {
	    	modifiedKeysByOtherTx.putAll(modifiedKeys); //put keys modified by this tx to the sqltxbb shared map
	    }
	   // */
	    
	    derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId); 
	  	//perform ops to derby
	  	if (!expectFailure) {
	  		Log.getLogWriter().info("performing tx ops in derby for txId " + txId);
	  		Log.getLogWriter().info("derby tx has " + derbyOps.size() + " operations");
	  		for (Object[] derbyOp: derbyOps) {
	  			DMLTxStmtIF dmlTxStmt= dmlTxFactory.createDMLTxStmt((Integer)derbyOp[0]); //which table
	  			dmlTxStmt.performDerbyTxOps(dConn, derbyOp, dExList);
	  		}
	  	}
	  		
	    //commit dConn
	    try {
		    if (expectFailure) {
		    	Log.getLogWriter().info("expect failure, roll back the derby tx");
		    	dConn.rollback();
		    } else {
		    	Log.getLogWriter().info("committing derby tx, does not expect failure");
		    	dConn.commit();
		    } 
	    } catch (SQLException se) {
	    	SQLHelper.handleSQLException(se);
	    }
	    
	    //no comparison for the lists if operations rolled back, 
	    //or single Op per Tx and got in doubt tx exception
	    if (!expectFailure || ignoreInDoubtTx) {
		    Log.getLogWriter().info("compare two exception lists");
		    if (gfxdCommitEx != null) {
		    	if (dExList.get(0)== null) {
		    		throw new TestException ("gfxd got in doubt exception, but derby did not have exception by the same cause");
		    	} else {
		    		SQLHelper.isSameRootSQLException(dExList.get(0).getSQLState(), gfxdCommitEx);
		    	}
		    }
		    else SQLHelper.compareExceptionLists(dExList, sExList);		    	
	    } 
	    
	    //end commit   
	    releaseLock();
   	}
  	waitForBarrier(); //after committing
  	
  	//clear the map for next op
  	SQLTxBB.getBB().getSharedMap().clear();
  	
  }
  
  //do one Tx Op to both dbs
  protected void doTxDMLOneOp(Connection dConn, Connection gConn,
  		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
    Log.getLogWriter().info("performing dmlTxOp, myTid is " + getMyTid());
    int table = dmlTables[random.nextInt(dmlTables.length)]; //get random table to perform dml
    
    while (modifiedKeys.size() == 1 && GFXDTxHelper.isReplicate(table) && !oneOpPerTx) {
    	Log.getLogWriter().info("table name is " + table);
    	table = dmlTables[random.nextInt(dmlTables.length)];    	
    } //first table should not be replicated table due to limitation
    //modifiedKeys has one entry of txId
    
    DMLTxStmtIF dmlTxStmt= dmlTxFactory.createDMLTxStmt(table); //dmlTxStmt of a table
    
    
    if (dmlTxStmt != null) {
	    //perform the opeartions
	    String operation = TestConfig.tab().stringAt(SQLPrms.dmlOperations);
	    if (operation.equals("query"))
	    	operation = "update"; 

	    if (operation.equals("insert"))
	      dmlTxStmt.insertTx(dConn, gConn, modifiedKeys, dExList, sExList);
	    else if (operation.equals("update"))
	      dmlTxStmt.updateTx(dConn, gConn, modifiedKeys, dExList, sExList);
	    else if (operation.equals("delete"))
	      dmlTxStmt.deleteTx(dConn, gConn, modifiedKeys, dExList, sExList);
	    else if (operation.equals("query"))
	    	;
	      // dmlTxStmt.queryTx(dConn, gConn, modifiedKeys, dExList, sExList);  //query are performed within the dmlTx stmt

	    else
	      throw new TestException("Unknown entry operation: " + operation);
    } else {
    	Log.getLogWriter().info("tx for this table has not been implemented yet");
    }
  }
  
  protected void doGFXDTxDMLOps(Connection gConn) {
  	int txId = (int) SQLTxBB.getBB().getSharedCounters().incrementAndRead(SQLTxBB.txId);
  	int maxOps = 5; 
  	int numOfOps = 0;
  	
  	if (oneOpPerTx) numOfOps = 1;
  	else numOfOps = random.nextInt(maxOps)+1;
  	
  	Log.getLogWriter().info("This tx " + txId + " will perform " +  numOfOps + " operations.");
		HashMap<String, Integer> modifiedKeys = new HashMap<String, Integer>();
		modifiedKeys.put(thisTxId, txId);
		ArrayList<SQLException> sExList = new ArrayList<SQLException>();
		if (sql.dmlStatements.AbstractDMLStmt.byCidRange) {
			int cid = random.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary));
			Log.getLogWriter().info("txId " + txId + " use cids colocated with " + cid);
			SQLBB.getBB().getSharedMap().put("cid_txId_"+txId, cid);
		} //partitioned by cid range, put a random cid for this tx
		
  	for (int i=0; i<numOfOps; i++) {
  		doTxDMLOneOp(null, gConn, modifiedKeys, null, sExList );		
  	}
  	
  	//commit gConn
    try {
    	gConn.commit(); //to commit and avoid rollback the successful operations
    } catch (SQLException se) {
    	//TODO to find out other exception might through at the commit time
    	//such as "23505" or other constrain violation in the dml operations
    	SQLHelper.printSQLException(se);
    	if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
    		Log.getLogWriter().info("Got the commit Conflict exception, continuing testing");
    	} else if (se.getSQLState().equalsIgnoreCase("X0Z05")) {
    		Log.getLogWriter().info("Got the in doubt transaction exception, continuing testing");
    	} 
    	  else
    		SQLHelper.handleSQLException(se);
    }
    
  }
  
  protected void waitForBarrier() {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numOfWorkers, "barrier");
    Log.getLogWriter().info("Waiting for " + numOfWorkers + " to meet at barrier");
    barrier.await();
  }
  
  protected void insertCustomersTxConflict() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDTxConnection(); 
    printIsolationLevel(gConn);
    insertCustomersTxConflict(dConn, gConn);   
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);  	
  }
  
  //if dConn is not null, insert same records to both derby and gemfirexd
  protected void insertCustomersTxConflict(Connection dConn, Connection gConn) {
  	int size = TestConfig.tab().intAt(SQLPrms.initCustomersSizePerThread, 100);
    TradeCustomersDMLTxStmt cust = new TradeCustomersDMLTxStmt();
    cust.insertCustomersTxConflict(dConn, gConn, size);
  }
  
  protected void updateCustomersTxConflict() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDTxConnection(); 	
    updateCustomersTxConflict(dConn, gConn);   
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);  	
  }
  
  protected void updateCustomersTxConflict(Connection dConn, Connection gConn) {
  	int size = 5;
    TradeCustomersDMLTxStmt cust = new TradeCustomersDMLTxStmt();
    cust.updateCustomersTxConflict(dConn, gConn, size);
  }
  
  
  //provide connection to gfxd/GFE
  protected Connection getGFXDTxConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getTxConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  //provide connection to gfxd/GFE -- used to set up dataStore in server groups
  protected Connection getGFXDTxConnection(Properties info) {
    Connection conn = null;
    try {
      conn = GFEDBManager.getTxConnection(info);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_verifyResultSets() {
    sqlTxTest.verifyResultSets();
  }
  
  public static void HydraTask_updateUsingScatterWhereCaluse() {
  	sqlTxTest.updateCustomersScatter();
  }
  
  private void updateCustomersScatter(){
  	TradeCustomersDMLTxStmt custTx = new TradeCustomersDMLTxStmt();
  	Connection gConn = getGFXDTxConnection();
  	printIsolationLevel(gConn);
  	custTx.updateCustomersTxScatter(null, gConn);
  }
	
  protected void printIsolationLevel(Connection conn) {
    try {
    	int isolation = conn.getTransactionIsolation();
    	String isoLevel;
    	switch (isolation) {
    	case Connection.TRANSACTION_NONE:
    		isoLevel = "TRANSACTION_NONE";
    		break;
    	case Connection.TRANSACTION_READ_COMMITTED:
    		isoLevel = "TRANSACTION_READ_COMMITTED";
    		break;
    	case Connection.TRANSACTION_REPEATABLE_READ:
    		isoLevel = "TRANSACTION_REPEATABLE_READ";
    		break;
    	case Connection.TRANSACTION_SERIALIZABLE:
    		isoLevel = "TRANSACTION_SERIALIZABLE";
    		break;
   		default:
    			isoLevel = "unknown";    		    		
    	}
    	Log.getLogWriter().info("the connection isolation level is " + isoLevel);
    	java.sql.SQLWarning w =conn.getWarnings();
    	SQLHelper.printSQLWarning(w);
    } catch (SQLException se) {
    	
    }
  }
	public static final String TXLOCK = "lockToPerformTx";
  public static final String LOCK_SERVICE_NAME = "MyLockService";
  protected static hydra.blackboard.SharedLock lock;
  protected static DistributedLockService dls;  //use shared lock for bridge
  protected void getLock() {
    //get the lock
    if (!SQLTest.isEdge) {
    	if (dls ==null)
    		dls = getLockService();
    	dls.lock(TXLOCK, -1, -1); //for distributed service
    }
    else {
	  	if (lock == null) 
	  		lock = SQLBB.getBB().getSharedLock();
	  	lock.lock();
    }    	
  }
  
  protected void releaseLock() {
    if (!SQLTest.isEdge ) { 
    	dls.unlock(TXLOCK);
    }
    else {
	  	lock.unlock();
    }
    	
  }
  
  private static DistributedLockService getLockService() {
    DistributedLockService dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
    if (dls == null) {
      DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      if (ds == null)
        throw new TestException("DistributedSystem is " + ds);
      dls = DistributedLockService.create(LOCK_SERVICE_NAME, ds);
    } else {

    }
    return dls;
  }
  
  
}
