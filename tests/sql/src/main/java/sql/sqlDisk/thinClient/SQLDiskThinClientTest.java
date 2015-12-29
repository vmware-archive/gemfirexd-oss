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
package sql.sqlDisk.thinClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.pivotal.gemfirexd.tools.GfxdSystemAdmin;
import com.pivotal.gemfirexd.tools.GfxdUtilLauncher;

import hydra.Log;
import hydra.MasterController;
import hydra.Prms;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.FabricServerHelper.Endpoint;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.sqlBridge.SQLBridgeTest;
import sql.sqlutil.ResultSetHelper;

public class SQLDiskThinClientTest extends SQLBridgeTest {
  protected static SQLDiskThinClientTest sdtc = null;
  protected static boolean isLocator = false;
  
  public static synchronized void HydraTask_initEdges() {
    if (sdtc == null) {
      sdtc = new SQLDiskThinClientTest();     
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
  
  public static synchronized void HydraTask_initLocator() {
    if (sdtc == null) {
      sdtc = new SQLDiskThinClientTest();     
    } 
    isLocator = true;    
  }
  
  public static void HydraTask_createGFESchemas() {
    sdtc.createGFESchemasByClients();
  }
  
  public static void HydraTask_createGFETables() {
    sdtc.createGFETablesByClients();
  }
  
  public static synchronized void HydraTask_createDiskStores() {
    sdtc.createDiskStores();
  }
  
  public static void HydraTask_populateTables(){
    sdtc.populateTables();
  }
  
  //perform dml statement
  public static void HydraTask_doDMLOp() {
    sdtc.doDMLOp();
  }
  
  public static void HydraTask_checkConstraints() {
    if (!withReplicatedTables) sdtc.checkConstraints();
  }
  
  protected void checkConstraints() {
    Connection gConn = getGFXDClientConnection();
       
    checkUniqConstraints(gConn);
    checkFKConstraints(gConn);
    
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_writePersistToBB() {
    sdtc.writePersistToBB();
  }

  //need to configured in the conf that only one site will run this
  protected void writePersistToBB() {
    Connection gConn = getGFXDClientConnection();
    writePersistToBB(gConn);
    closeGFEConnection(gConn);
  }
  
  public synchronized static void HydraTask_stopFabricServer() {
    FabricServerHelper.stopFabricServer();
  } 
  
  public static void HydraTask_verifyPersistFromBB() {
    sdtc.verifyPersistFromBB();
  }
  
  protected void verifyPersistFromBB() {
    Connection gConn = getGFXDClientConnection();
    verifyPersistFromBB(gConn);
    closeGFEConnection(gConn);
  }
  
  public static synchronized void HydraTask_createDiscDB() {
    sdtc.createDiscDB();
  }


  public static synchronized void HydraTask_createDiscSchemas() {
    sdtc.createDiscSchemas();
  }

  public static void HydraTask_createDuplicateDiscSchemas() {
    sdtc.createDuplicateDiscSchemas();
  }
  
  public static synchronized void HydraTask_createDiscTables () {
    sdtc.createDiscTables();
  }
  
  public static void HydraTask_setTableCols() {
    sdtc.setTableCols();
  }

  //can be called only after table has been created
  protected void setTableCols() {
    Connection gConn = getGFXDClientConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  //perform ddl statement
  public static void HydraTask_createIndex() {
    if (createIndex) sdtc.createIndex();
  }

  protected void createIndex() {
    Connection gConn = getGFXDClientConnection();
    createIndex(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }

  protected void createIndex(Connection conn) {
    sql.ddlStatements.IndexDDLStmt indexStmt = new sql.ddlStatements.IndexDDLStmt();
    indexStmt.doDDLOp(null, conn);
  }
  
  public static void HydraTask_createConcIndex() {
    //reduce the rate of create index concurrently
    if (createIndex && random.nextInt(10 * numOfWorkers) == 1) sdtc.createIndex();
  }
  
  public static void  HydraTask_verifyResultSets() {
    sdtc.verifyResultSets();
  }
  
  public static void HydraTask_createGfxdLocatorTask() {
    FabricServerHelper.createLocator();
  }
  
  public static void HydraTask_startGfxdLocatorTask() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    FabricServerHelper.startLocator(networkServerConfig);
    
    //write locator info to bb for client to use in shut-down-all
    List <Endpoint> locatorList = FabricServerHelper.getSystemEndpoints();
    Log.getLogWriter().info("loctorList is " + locatorList);
    SQLBB.getBB().getSharedMap().put("locatorList", locatorList);
    
  }
  
  @SuppressWarnings("unchecked")
  public static synchronized void HydraTask_shut_down_all() {
    List <Endpoint> locatorList = (List<Endpoint>) SQLBB.getBB().getSharedMap().get("locatorList");
    //Only need one locator's end point
    Endpoint locator = locatorList.get(0);
    String host = locator.getHost();
    int port = locator.getPort();
    Log.getLogWriter().info("shut-down-all target locator host: " + host + " and port: " + port);
    //GfxdUtilLauncher.main(new String[] {"shut-down-all",  "-locators="+ host
    //    +":"+port});
    new GfxdSystemAdmin().shutDownAll(
        "shut-down-all",
        Arrays.asList(new String[] { "-locators=" + host + ":" + port }));
  }

  public static synchronized void HydraTask_waitforServerShutdown() {
    sdtc.waitforServerShutdown();
  }
  
  protected void waitforServerShutdown() {
    int sleepms = 30 * 1000;
    MasterController.sleepForMs(sleepms);
    
    while (true) {
      if (serversShutdown()) {
        break;
      } else {
        MasterController.sleepForMs(sleepms);
        Log.getLogWriter().info("servers still available, waits for " + sleepms/1000 + " seconds");
      }
    }
  }
  
  protected boolean serversShutdown() {
    String sqlstat = "select * from trade.customers";
    try {
      Connection conn = sql.GFEDBClientManager.getConnection();
      conn.createStatement().executeQuery(sqlstat);
    } catch (SQLException se) {
      if (se.getSQLState().equals("08006") && se.getMessage().contains("Failed after trying all available servers")) {
        return true;
      } else {
        SQLHelper.printSQLException(se);
        return false;
      }
    }
    return false;
  }
  
  public static void HydraTask_restart() {
    if (isLocator) {
      HydraTask_createGfxdLocatorTask();
      HydraTask_startGfxdLocatorTask();
    } else {
      HydraTask_startFabricServer();
      HydraTask_stopFabricServer();
    }
  }
  
  public static void HydraTask_getConvertibleQuery() {
    sdtc.getConvertibleQuery();
  }
 
  protected void getConvertibleQuery() {
    Connection gConn = getGFXDClientConnection();
    getConvertibleQuery(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  protected void getConvertibleQuery(Connection conn) {
    String sql = "select cid, sid, qty from trade.portfolio where cid=? and sid =?";
    ResultSet temp = null;
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      ResultSet rs = conn.createStatement().executeQuery("select * from trade.portfolio");
      while (rs.next()) {
        ps.setInt(1, rs.getInt("CID"));
        ps.setInt(2, rs.getInt("SID"));
        temp = ps.executeQuery();
        ResultSetHelper.asList(temp, false);
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_simpleTest() {
    sdtc.simpleTest();
  }
  
  protected void simpleTest() {
    Connection conn = getGFXDClientConnection();
    getQuery(conn);
    closeGFEConnection(conn);
  }
  
}
