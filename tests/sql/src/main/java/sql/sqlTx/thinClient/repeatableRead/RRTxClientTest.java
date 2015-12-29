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
package sql.sqlTx.thinClient.repeatableRead;

import hydra.ClientVmInfo;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.Prms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import sql.GFEDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.dmlStatements.AbstractDMLStmt;
import sql.sqlTx.SQLDistRRTxTest;
import sql.sqlutil.PooledConnectionC3P0;
import sql.sqlutil.PooledConnectionDBCP;
import sql.sqlutil.PooledConnectionTomcat;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class RRTxClientTest extends SQLDistRRTxTest {
  protected static RRTxClientTest rrClientTest;
  public static boolean useGemFireXDHA = TestConfig.tab().booleanAt(SQLPrms.
      useGemFireXDHA, false);
  
  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its distributed system.
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

  public static synchronized void HydraTask_initializeServer() {
    if (rrClientTest == null) {
      rrClientTest = new RRTxClientTest();
    }
    
    PRObserver.installObserverHook();
    PRObserver.initialize(RemoteTestModule.getMyVmid());
    
    initializeParams();
  }
  
  public static synchronized void HydraTask_initClient() {
    if (rrClientTest == null) {
      rrClientTest = new RRTxClientTest();     
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
  
  
  private static void initializeParams() {
    hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
    testUniqueKeys = TestConfig.tab().booleanAt(SQLPrms.testUniqueKeys, true);
    randomData =  TestConfig.tab().booleanAt(SQLPrms.randomData, false);
    isSerial =  TestConfig.tab().booleanAt(Prms.serialExecution, false);
    isSingleDMLThread = TestConfig.tab().booleanAt(SQLPrms.isSingleDMLThread, false);
    usingTrigger = TestConfig.tab().booleanAt(SQLPrms.usingTrigger, false);
    queryAnyTime = TestConfig.tab().booleanAt(SQLPrms.queryAnyTime, true);
    hasNetworth =  TestConfig.tab().booleanAt(SQLPrms.hasNetworth, false);
  }
  
  public synchronized static void HydraTask_startFabricServer() {
    rrClientTest.startFabricServer();
  }
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }
  
  public synchronized static void HydraTask_stopFabricServer() {
    rrClientTest.stopFabricServer();
  }
  
  public static void HydraTask_createGfxdSchemasByClients() {
    rrClientTest.createGfxdSchemasByClients();
  }
  
  protected void createGfxdSchemasByClients() {
    Connection conn = getGFXDClientRRTxConnection();
    Log.getLogWriter().info("creating schema in gfxd.");
    if (!testServerGroupsInheritence) createSchemas(conn);
    else {
      String[] schemas = SQLPrms.getGFESchemas(); //with server group
      createSchemas(conn, schemas); 
    }
    Log.getLogWriter().info("done creating schema in gfxd.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_createGfxdTablesByClients() {
    rrClientTest.createGfxdTablesByClients();
  }
  
  protected void createGfxdTablesByClients() {
    Connection conn = getGFXDClientRRTxConnection();
    Log.getLogWriter().info("creating tables in gfxd.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfxd.");
    closeGFEConnection(conn);
  }

  protected Connection getGFXDClientTxConnection() {
    Connection conn = null;
    try {
      conn = sql.GFEDBClientManager.getTxConnection(GFEDBManager.Isolation.READ_COMMITTED);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
 
  protected Connection getGFXDClientRRTxConnection() {
    Connection conn = null;
    boolean retry = false;
    do {
      try {
        if (useC3P0 && useGfxdConfig)
          conn = PooledConnectionC3P0.getRRConnection();
        else if (useDBCP && useGfxdConfig)
          conn = PooledConnectionDBCP.getRRConnection();
        else if (useTomcatConnPool && useGfxdConfig)
          conn = PooledConnectionTomcat.getRRConnection();
        else
          conn = sql.GFEDBClientManager.getTxConnection(GFEDBManager.Isolation.REPEATABLE_READ);
        
        retry = false;
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        if (AbstractDMLStmt.gfxdtxHANotReady && isHATest && SQLHelper.gotTXNodeFailureException(e)) {
          retry = true;
          Log.getLogWriter().info("Could not get connection due to Node failure and will retry, continue testing");
        }
        else 
          throw new TestException ("Not able to get the connection " + TestHelper.getStackTrace(e));
      }
    } while (retry); //see #39229, needs to retry for now, until failover HA is ready
    return conn;
  }

  @Override
  protected Connection getNewGfxdConnection() {
    return getGFXDClientRRTxConnection();
  }

  public static void HydraTask_createDiscDB() {
    rrClientTest.createDiscDB();
  }
  
  public static void HydraTask_createDiscSchemas() {
    rrClientTest.createDiscSchemas();
  }
  
  public static void HydraTask_createDiscTables() {
    rrClientTest.createDiscTables();
  }
  
  public static void HydraTask_setTableCols() {
    rrClientTest.setTableCols();
  }
  
  //can be called only after table has been created
  @Override
  protected void setTableCols() {
    Connection gConn = getGFXDClientRRTxConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_initConnections() {
    rrClientTest.initThreadLocalConnection();
  }
  
  /** `
   * Sets the per-thread Connection with isolation none for certain query.
   */
  @Override
  protected void initThreadLocalConnection() {
    Connection gfxdConn = getGFXDClientConnection();
    gfxdNoneTxConn.set(gfxdConn); 
  }
 
  /* 
  protected Connection getGFXDClientConnection() {
    Connection conn = null;
    try {
      conn = sql.GFEDBClientManager.getConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  */
 
  public static void HydraTask_initThreadLocals() {
    rrClientTest.initThreadLocals();
  }
  
  public static void HydraTask_populateTxTables() {
    rrClientTest.populateTxTables();
  }
  
  @Override
  protected void populateTxTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } 
    //Connection gConn = getGFXDClientRRTxConnection(); 
    //work around issue mentioned in #48712, commit may return earlier
    Properties info = new Properties();
    info.setProperty(syncCommits, "true");
    Connection gConn = getGFXDClientRRTxConnection(info); //RR
    
    populateTables(dConn, gConn); 
    commit(gConn);
    commit(dConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyResultSets() {
    rrClientTest.verifyResultSets();
  }
  
  @Override
  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      cleanConnection(getGFEConnection());
      return;
    }
    Connection dConn = getDiscConnection();
    Connection gConn = getGFXDClientRRTxConnection();

    verifyResultSets(dConn, gConn);
  }
  
  public static void HydraTask_doDMLOp() {
    rrClientTest.doDMLOp();
  }

  @Override
  protected void doDMLOp() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }

    Connection gConn = null;
    if (mixRR_RC && random.nextBoolean()) {
      gConn = getGFXDClientTxConnection();
      doDMLOp(dConn, gConn, false);      
    } else {
      gConn = getGFXDClientRRTxConnection(); 
      doDMLOp(dConn, gConn, true);  
    }

    if (dConn!=null) {
      closeDiscConnection(dConn);
      //Log.getLogWriter().info("closed the disc connection");
    }
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }

  public static void HydraTask_createIndex() {
    rrClientTest.createIndex();
  }
  
  @Override
  protected void createIndex() {
    Connection gConn = random.nextBoolean() ? 
        getGFXDClientConnection() : getGFXDClientRRTxConnection();
    createRRTxIndex(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_checkConstraints() {
    rrClientTest.checkConstraints();
  }
  
  protected void checkConstraints() {
    Connection gConn = getGFXDClientRRTxConnection();
    
    checkUniqConstraints(gConn);
    checkFKConstraints(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_checkRRKeys() {
    rrClientTest.checkRRKeys();
  }

  protected void checkRRKeys() {
    if (getMyTid()%3 != 0 && random.nextInt(20)!=1) {
      Log.getLogWriter().info("do dml ops instead of checking repeatable read keys");
      doDMLOp();
      return; 
    }
    
    Connection gConn = getGFXDClientRRTxConnection();    
    checkRRKeys(gConn);
    commitRRGfxdOnly(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_cycleStoreVms() {
    if (cycleVms) rrClientTest.cycleStoreVms();
  }
  
  protected List <ClientVmInfo> stopStartVMs(int numToKill) {   
    if (useGemFireXDHA)
      return stopStartVMs(numToKill, "server");
    else
      return stopStartVMs(numToKill, "store");
  }
  
  protected Connection getGFXDClientRRTxConnection(Properties info) {
    Connection conn = null;
    try {
      if (useC3P0 && useGfxdConfig)
        conn = PooledConnectionC3P0.getRRConnection(info);
      else if (useDBCP && useGfxdConfig)
        conn = PooledConnectionDBCP.getRRConnection(info);
      else if (useTomcatConnPool && useGfxdConfig)
        conn = PooledConnectionTomcat.getRRConnection(info);
      else
        conn = sql.GFEDBClientManager.getRRTxConnection(info);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
}
