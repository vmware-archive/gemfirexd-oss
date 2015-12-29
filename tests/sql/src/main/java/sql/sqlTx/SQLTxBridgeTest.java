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

import hydra.ClientVmInfo;
import hydra.DistributedSystemHelper;
import hydra.HostHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.PortHelper;
import hydra.Prms;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.ddlStatements.DDLStmtIF;
import sql.ddlStatements.FunctionDDLStmt;
import sql.ddlStatements.Procedures;
import sql.sqlBridge.SQLBridgeTest;
import util.PRObserver;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import com.pivotal.gemfirexd.internal.drda.NetworkServerControl;

public class SQLTxBridgeTest extends SQLTxTest {
  protected static SQLTxBridgeTest sqlTxbtest;
  protected static boolean useMcast;
  protected static String locatorString;
  protected static Properties bootProps;
  protected static Properties connProps;
  protected static String hostName;
  protected static int portNo;
  protected static NetworkServerControl netServer = null;  
  
  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }
  	
  public static synchronized void HydraTask_initialize() {
    if (sqlTxbtest == null) {
      sqlTxbtest = new SQLTxBridgeTest();
    }
    
	  PRObserver.installObserverHook();
	  PRObserver.initialize(RemoteTestModule.getMyVmid());
	  
	  bootProps = sqlTxbtest.getGemFireProperties();
	  bootProps.setProperty("host-data", "true"); 
	  hostName = HostHelper.getIPAddress().getHostName();
    
    Log.getLogWriter().info("Connecting with properties: " + bootProps);
    //Log.getLogWriter().info("hostname is  " + hostName);   

		sqlTxbtest.initialize();
  }

	protected void initialize() {
		super.initialize();
	}
  
  public static synchronized void HydraTask_initialize_replayHA() {
    Object initDelayed = SQLBB.getBB().getSharedMap().get("initDelayed_" + RemoteTestModule.getMyVmid());
    if (initDelayed == null) {
            SQLBB.getBB().getSharedMap().put("initDelayed_" + RemoteTestModule.getMyVmid(), "true");
            Log.getLogWriter().info("first time for init DDL replay, do nothing");
            return;
    }
	  if (sqlTxbtest == null) {
			sqlTxbtest = new SQLTxBridgeTest();
		
			HydraTask_initialize();
			sqlTxbtest.createGFXDDB();
			
			int numOfPRs = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.numOfPRs);
			List<ClientVmInfo>vms = new ArrayList<ClientVmInfo>();
			vms.add(new ClientVmInfo(RemoteTestModule.getMyVmid()));
			PRObserver.waitForRebalRecov(vms, 1, numOfPRs, null, null, false);
			StopStartVMs.StopStart_initTask();
	  } 
  	int sleepMS = 30000;
  	Log.getLogWriter().info("sleep for " + sleepMS/1000 + " sec");
  	MasterController.sleepForMs(sleepMS);
  }
  
  //each connection server should have only one thread performing this task. 
  public static synchronized void HydraTask_initConnBridgePort() {
  	portNo = PortHelper.getRandomPort(); 	
  	int count = (int) SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.initServerPort);
  	while (count != 1) {
  		count = (int) SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.initServerPort);
  	}
  	HashMap	<String, ArrayList<Integer>> map = (HashMap) SQLBB.getBB().getSharedMap().get("serverPorts");
  	ArrayList<Integer> portLists;
  	if ( map== null) {
  		map = new HashMap <String, ArrayList<Integer>>();
  		portLists = new ArrayList<Integer>(); 	
  	} else {
  		portLists = map.get(hostName);
  		if (portLists == null) portLists = new ArrayList<Integer>();
  	}
  	portLists.add(portNo);
  	map.put(hostName, portLists);
  	SQLBB.getBB().getSharedMap().put("serverPorts", map);
  	SQLBB.getBB().getSharedCounters().zero(SQLBB.initServerPort);  	
  	
  	Log.getLogWriter().info("host " + hostName + " port " + portNo + 
			" is available for client connection");
  	//bootProps.put("port", portNo);
  }
  
  protected static String getLocatorString() {
    List<DistributedSystemHelper.Endpoint> endpoints = DistributedSystemHelper.getEndpoints();
    return DistributedSystemHelper.endpointsToString(endpoints);
  }
  
  //create gfxd database (through connection) without ds
  public static synchronized void HydraTask_createGFXDDB() {
    sqlTxbtest.createGFXDDB();
    Object tableCreated = SQLBB.getBB().getSharedMap().get("tableCreated_" + RemoteTestModule.getMyVmid());
    if (tableCreated == null) {
      SQLBB.getBB().getSharedMap().put("tableCreated_" + RemoteTestModule.getMyVmid(), "true");
      Log.getLogWriter().info("first time, table has not been created, do nothing");
      return;
    } //first time table has not been created
    if (isHATest) {
    	addObserver();
    }
  }
  
  protected void createGFXDDB() {  
    startGFXDDB(bootProps);
  }
  
  protected static void addObserver() {
		int numOfPRs = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.numOfPRs);
		List<ClientVmInfo>vms = new ArrayList<ClientVmInfo>();
		vms.add(new ClientVmInfo(RemoteTestModule.getMyVmid()));
		PRObserver.waitForRebalRecov(vms, 1, numOfPRs, null, null, false);
		StopStartVMs.StopStart_initTask();
  }
  
  public static synchronized void HydraTask_initEdges() {
  	if (sqlTxbtest == null) {
  		sqlTxbtest = new SQLTxBridgeTest();  		
  	} 
    
    hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
    testUniqueKeys = TestConfig.tab().booleanAt(SQLPrms.testUniqueKeys, true);
    randomData =  TestConfig.tab().booleanAt(SQLPrms.randomData, false);
    isSerial =  TestConfig.tab().booleanAt(Prms.serialExecution, false);
    isSingleDMLThread = TestConfig.tab().booleanAt(SQLPrms.isSingleDMLThread, false);
    usingTrigger = TestConfig.tab().booleanAt(SQLPrms.usingTrigger, false);
    queryAnyTime = TestConfig.tab().booleanAt(SQLPrms.queryAnyTime, true);
    hasNetworth =  TestConfig.tab().booleanAt(SQLPrms.hasNetworth, false);
    isEdge = true;
  }
  
  public static synchronized void HydraTask_createGFXDNetServer() throws Exception {
    sqlTxbtest.createGFXDNetServer();
  }
  
  protected void createGFXDNetServer() throws Exception { 
  	netServer = new NetworkServerControl(InetAddress.getLocalHost(), portNo);
    netServer.start(new PrintWriter(System.out));
    while(true) {
    	MasterController.sleepForMs(500);
      try {
        netServer.ping();
        break;
      }
      catch (Exception e) {
      	Log.getLogWriter().info("could not ping netServer "+TestHelper.getStackTrace(e));
      }
    }
    netServer.logConnections(true);
  }
  
  public static void HydraTask_createGFESchemasByClients() {
  	sqlTxbtest.createGFESchemasByClients();
  }
  
  protected void createGFESchemasByClients() {
    Connection conn = getGFXDClientTxConnection();
    Log.getLogWriter().info("creating schema in gfxd.");
    if (!testServerGroupsInheritence) createSchemas(conn);
    else {
      String[] schemas = SQLPrms.getGFESchemas(); //with server group
      createSchemas(conn, schemas); 
    }
    Log.getLogWriter().info("done creating schema in gfxd.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_createGFETablesByClients() {
	  sqlTxbtest.createGFETablesByClients();
  }
  
  protected void createGFETablesByClients() {
    Connection conn = getGFXDClientTxConnection();
    Log.getLogWriter().info("creating tables in gfxd.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfxd.");
    closeGFEConnection(conn);
  }
  
  protected Connection getGFXDClientTxConnection() {
    Connection conn = null;
    try {
      conn = sql.GFEDBClientManager.getTxConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the thin client tx connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getGFXDClientTxConnection(Properties info) {
    Connection conn = null;
    try {
      conn = sql.GFEDBClientManager.getTxConnection(info);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the thin client tx connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_populateTxTables(){
    sqlTxbtest.populateTxTables();
  }
  
  //when no verification is needed, pass dConn as null
  protected void populateTxTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDClientTxConnection();
    populateTxTables(dConn, gConn);   
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  //perform dml statement
  public static void HydraTask_doTxDMLOps() {
    sqlTxbtest.doTxDMLOps();
  }
  
  //perform dml statement
  public static void HydraTask_doTxOps() {
    sqlTxbtest.doTxOps();
  }
  
  protected void doTxOps() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientTxConnection();
    
    if (random.nextInt(numOfWorkers/2) == 0) 
      doTxDDLOps(dConn, gConn);
    else
    	doTxDMLOps(dConn, gConn);   
    
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  protected void doTxDMLOps() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when test only GFXD connections.
    Connection gConn = getGFXDClientTxConnection();
    doTxDMLOps(dConn, gConn);
  }
  
  public static void HydraTask_createIndex() {
    sqlTxbtest.createIndex();
  }
  
  protected void createIndex() {
    Connection gConn = getGFXDClientTxConnection();
    createIndex(gConn);
  }
  
  protected List<ClientVmInfo> stopStartVMs(int numToKill) { 	
  	return stopStartVMs(numToKill, "store");
  }
  
  //initialize the map contains column information for each table
  public static void HydraTask_setTableCols() {
    sqlTxbtest.setTableCols();
  }
  
  //can be called only after table has been created  
  protected void setTableCols() {
    Connection gConn = getGFXDClientTxConnection();
    setTableCols(gConn);
  }
  
  public static synchronized void HydraTask_cycleStoreVms() {
    sqlTxbtest.cycleStoreVms();
  }
  
  public static synchronized void HydraTask_createDiscDB() {
  	sqlTxbtest.createDiscDB();
  }
 
  public static synchronized void HydraTask_createDiscSchemas() {
  	sqlTxbtest.createDiscSchemas();
  }
  public static synchronized void HydraTask_createDiscTables () {
  	sqlTxbtest.createDiscTables();
  }
  
  public static void HydraTask_verifyResultSets() {
    sqlTxbtest.verifyResultSets();
  }
  
  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verify of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      cleanConnection(getGFXDClientTxConnection());
      return;
    }
    Connection dConn = getDiscConnection();
    Connection gConn = getGFXDClientTxConnection();
    
    verifyResultSets(dConn, gConn);
  }
  
  /*
  public static void HydraTask_createProcedures() {
  	sqlTxbtest.createProcedures();
  }
  
  public static void HydraTask_verifyProcedures() {
  	sqlTxbtest.verifyProcedures();
  }
  
  protected void createProcedures() {
    Log.getLogWriter().info("performing create procedure Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    
    sql.ddlStatements.ProcedureDDLStmt procStmt = new sql.ddlStatements.ProcedureDDLStmt();
    procStmt.createDDLs(dConn, gConn);

    if (dConn!= null) {
      commit(dConn);
      closeDiscConnection(dConn);
    }
    
    commit(gConn);
    closeGFEConnection(gConn);  
  }
  
  //get results set using CallableStatmenet
  protected void verifyProcedures() {
    if (!hasDerbyServer) {
      Log.getLogWriter().warning("skipping verify of query results through "
          + "created procedure call due to manageDerbyServer as false, myTid="
          + getMyTid());
      return;
    }
    Log.getLogWriter().info("verifying query results through created procedure call, myTid is " + getMyTid());
    Connection dConn = getDiscConnection();
    Connection gConn = getGFXDClientConnection();

    verifyProcedures(dConn, gConn);
    
    commit(dConn);
    commit(gConn);    
    closeDiscConnection(dConn);
    closeGFEConnection(gConn);  
  }
  
  public static void HydraTask_callProcedures() {
    sqlTxbtest.callProcedures();
  }
  
  //get results set using CallableStatmenet
  protected void callProcedures() {
    Log.getLogWriter().info("call procedures, myTid is " + getMyTid());
    Connection dConn = null;
    if (hasDerbyServer){
      dConn = getDiscConnection();
    }
    Connection gConn = getGFXDClientConnection();
    Procedures.callProcedures(dConn, gConn);
    
    if (hasDerbyServer){
      commit(dConn);
      closeDiscConnection(dConn);
    }
    commit(gConn);    
    closeGFEConnection(gConn);  
  }
  
  public static void HydraTask_doOp() {
    int num = 10;
    boolean dmlOp = (random.nextInt(num)==0)? false: true; //1 out of num chance to do ddl operation
    if (dmlOp) sqlTxbtest.doDMLOp();
    else sqlTxbtest.doDDLOp();  
  }
  
  protected void doDDLOp() {    
    //TODO differentiate which ddl statements to be performed
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();

    //perform the opeartions
    int ddl = ddls[random.nextInt(ddls.length)];
    DDLStmtIF ddlStmt= ddlFactory.createDDLStmt(ddl); //dmlStmt of a table
    
    ddlStmt.doDDLOp(dConn, gConn);

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
   
    Log.getLogWriter().info("done DDLOp");
  }
  
  public static void HydraTask_createFuncForProcedures() {
  	sqlTxbtest.createFuncForProcedures();
  }
  
  protected void createFuncForProcedures() {
    Log.getLogWriter().info("performing create function multiply Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    
    try {
      FunctionDDLStmt.createFuncMultiply(dConn);
      FunctionDDLStmt.createFuncMultiply(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }

    if (dConn!= null) {
      commit(dConn);
      closeDiscConnection(dConn);
    }
    
    commit(gConn);
    closeGFEConnection(gConn);  
  }
  
  public static void HydraTask_createGFXDDBSG() {
  	sqlTxbtest.createGFXDDBSG();
  }
  
  public static void HydraTask_createGFXDDBSGDBSynchronizer() {
  	sqlTxbtest.createGFXDDBSGDBSynchronizer();
  }
  
  public static synchronized void HydraTask_createDBSynchronizer() {
  	sqlTxbtest.createDBSynchronizer();
  }
  
  //use client driver connection
  protected void createDBSynchronizer()  {
		Connection conn = getGFXDClientConnection(); 
		createDBSynchronizer(conn);
  }
  
  public static synchronized void HydraTask_startDBSynchronizer() {
  	sqlTxbtest.startDBSynchronizer();
  }
  
  //use client driver connection
  protected void startDBSynchronizer() {
  	Connection conn = getGFXDClientConnection();
  	startDBSynchronizer(conn);
  }
  
  public static void HydraTask_populateTablesDBSynchronizer() {
  	sqlTxbtest.populateTablesDBSynchronizer();
  } 
  
  //use client driver connection
  protected void populateTablesDBSynchronizer() {
  	Connection gConn = getGFXDClientConnection();
  	populateTables(null, gConn); //pass dConn as null for DBSynchronizer
  	closeGFEConnection(gConn);
  }
  
  public static void HydraTask_doDMLOpDBSynchronizer() {
  	sqlTxbtest.doDMLOpDBSynchronizer();
  }
  
  //use client driver connection
  protected void doDMLOpDBSynchronizer() {
  	Connection gConn = getGFXDClientConnection();
  	doDMLOp(null, gConn); //do not operation on derby directly
  }
  
  public static void HydraTask_putLastKeyDBSynchronizer() {
  	sqlTxbtest.putLastKeyDBSynchronizer();
  }
  
  //to wait until the last key is in the derby database 
  protected void putLastKeyDBSynchronizer() {
  	Connection dConn = getDiscConnection();
  	Connection gConn = getGFXDClientConnection();
  	putLastKeyDBSynchronizer(dConn, gConn);  	
  }
  
  public static void  HydraTask_verifyResultSetsDBSynchronizer() {
  	sqlTxbtest.verifyResultSetsDBSynchronizer();
  }
  
  protected void verifyResultSetsDBSynchronizer() {
  	Connection dConn = getDiscConnection();
  	Connection gConn = getGFXDClientConnection();
  	verifyResultSets(dConn, gConn);  	
  }
*/
}
