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
package sql.sqlTx.thinClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import hydra.ClientVmInfo;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.Prms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import sql.GFEDBManager;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.dmlStatements.AbstractDMLStmt;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlTx.SQLTxBB;
import sql.sqlTx.SQLTxPrms;
import sql.sqlutil.PooledConnectionC3P0;
import sql.sqlutil.PooledConnectionDBCP;
import sql.sqlutil.PooledConnectionTomcat;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class SQLDistTxClientTest extends SQLDistTxTest {
  protected static SQLDistTxClientTest sqlTxClientTest;
  static boolean isTicket43188Fixed = false;
  
  public static Object lock = new Object();
  public static int concUpdateCount = 0;
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
    if (sqlTxClientTest == null) {
      sqlTxClientTest = new SQLDistTxClientTest();
    }
    
    PRObserver.installObserverHook();
    PRObserver.initialize(RemoteTestModule.getMyVmid());
    
    initializeParams();
  }
  
  public static synchronized void HydraTask_initClient() {
    if (sqlTxClientTest == null) {
      sqlTxClientTest = new SQLDistTxClientTest();     
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
    sqlTxClientTest.startFabricServer();
  }
  
  public synchronized static void HydraTask_stopFabricServer() {
    sqlTxClientTest.stopFabricServer();
  }
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }
  
  public static void HydraTask_createGfxdSchemasByClients() {
    sqlTxClientTest.createGfxdSchemasByClients();
  }
  
  protected void createGfxdSchemasByClients() {
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
  
  public static void HydraTask_createGfxdTablesByClients() {
    sqlTxClientTest.createGfxdTablesByClients();
  }
  
  protected void createGfxdTablesByClients() {
    Connection conn = getGFXDClientTxConnection();
    Log.getLogWriter().info("creating tables in gfxd.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfxd.");
    closeGFEConnection(conn);
  }

  public Connection getGFXDClientTxConnection() {
    Connection conn = null;
    boolean retry = false;
    do {
      try {
        if (useC3P0 && useGfxdConfig)
          conn = PooledConnectionC3P0.getRCConnection();
        else if (useDBCP && useGfxdConfig)
          conn = PooledConnectionDBCP.getRCConnection();
        else if (useTomcatConnPool && useGfxdConfig)
          conn = PooledConnectionTomcat.getRCConnection();
        else
          conn = sql.GFEDBClientManager.getTxConnection(GFEDBManager.Isolation.READ_COMMITTED);
        
        retry = false;
        
        if (needNewConnection()) {
          SQLDistTxTest.needNewConnAfterNodeFailure.set(false);
          Log.getLogWriter().info("got new connection: needNewConnAfterNodeFailure is reset to false");
        }
        
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
  
  public static void HydraTask_createDiscDB() {
    sqlTxClientTest.createDiscDB();
  }
  
  public static void HydraTask_createDiscSchemas() {
    sqlTxClientTest.createDiscSchemas();
  }
  
  public static void HydraTask_createDiscTables() {
    sqlTxClientTest.createDiscTables();
  }
  
  public static void HydraTask_setTableCols() {
    sqlTxClientTest.setTableCols();
  }
  
  //can be called only after table has been created
  protected void setTableCols() {
    Connection gConn = getGFXDClientTxConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_initConnections() throws SQLException {
    sqlTxClientTest.initThreadLocalConnection();
  }
  
  /** `
   * Sets the per-thread Connection with isolation none for certain query.
   */
  protected void initThreadLocalConnection() throws SQLException {
    Connection gfxdConn = getGFXDClientConnection();
    log().info("gfxdNoneTxConn isolation is set to " + Connection.TRANSACTION_NONE
        + " and getTransactionIsolation() is "+ gfxdConn.getTransactionIsolation());
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
    sqlTxClientTest.initThreadLocals();
  }
  
  public static void HydraTask_setCriticalHeapPercentage() {
    sqlTxClientTest.setCriticalHeapPercentage();
  }
  
  public static void HydraTask_createNewTables() {
    sqlTxClientTest.createNewTables();
  }
  
  protected void createNewTables() {
    sqlTxClientTest.addCustomerV1Table();
    sqlTxClientTest.addNetworthV1Table();
    
    //set bb for first iteration
    SQLTxBB.getBB().getSharedCounters().increment(SQLTxBB.iterations);
  }
  
  public static void HydraTask_alterCustomersTableGenerateAlways() {
    sqlTxClientTest.alterCustomersTableGenerateAlways();
  }
  
  protected void alterCustomersTableGenerateAlways() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  
    Connection gConn = getGFXDClientTxConnection();
    try {
      if (addGenIdColInCustomersv1) {
        alterCustomersAddGenId(dConn, gConn);
        updateGenIdCol(dConn, gConn);
      }
      alterCustomersTableGenerateAlways(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    closeGFEConnection(gConn);
  }
  
  protected void setCriticalHeapPercentage() {
    Connection gConn = getGFXDClientTxConnection();
    setCriticalHeapPercentage(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_populateTxTables() {
    sqlTxClientTest.populateTxTables();
  }
  
  protected void populateTxTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();  
    } 
    
    //Connection gConn = getGFXDClientTxConnection(); 
    //work around issue mentioned in #48712, commit may return earlier
    Properties info = new Properties();
    info.setProperty(syncCommits, "true");
    Connection gConn = getGFXDClientTxConnection(info); //read_committed
       
    populateTables(dConn, gConn); 
    commit(gConn);
    commit(dConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyResultSets() {
    sqlTxClientTest.verifyResultSets();
  }
  
  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      cleanConnection(getGFEConnection());
      return;
    }
    Connection dConn = getDiscConnection();
    Connection gConn = getGFXDClientTxConnection();

    verifyResultSets(dConn, gConn);
  }
  
  public static void HydraTask_doDMLOp() {
    sqlTxClientTest.doDMLOp();
  }
  
  protected void doDMLOp() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    Connection gConn = getGFXDClientTxConnection(); 
    doDMLOp(dConn, gConn);

    if (dConn!=null) {
      closeDiscConnection(dConn);
      //Log.getLogWriter().info("closed the disc connection");
    }
    
    //work around an issue of no connection fail over and new connection is required case
    //need to revisit once txn ha is supported
    try {
      Log.getLogWriter().info("commit the gfxd txn again");
      gConn.commit();
    } catch (SQLException se) {
      if (isHATest && SQLHelper.gotTXNodeFailureException(se)) {
        Log.getLogWriter().info("got node failure exception during Tx with HA support, continue testing");
        return;
      } else if (isHATest && se.getSQLState().equals("08003")) {
        Log.getLogWriter().info("connection has been closed -- this could happen when txn HA support" +
        		" is not ready and a new connection has been acquired, continue testing");
        Log.getLogWriter().info("done dmlOp");
        return;
      } else SQLHelper.handleSQLException(se);
    }
    
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }

  public static void HydraTask_createIndex() {
    sqlTxClientTest.createIndex();
  }
  
  protected void createIndex() {
    Connection gConn = random.nextBoolean() ? 
        getGFXDClientConnection() : getGFXDClientTxConnection();
    createTxIndex(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setInitialData() {
    sqlTxClientTest.setInitialData();
  }
  
  //update the column value that going to be updated and verified during the test
  protected void setInitialData() {   
    Connection gConn = getGFXDClientTxConnection();
    setInitialData(gConn); 
    int maxCid = (int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary);
    Log.getLogWriter().info("maxCid in the test is " + maxCid ); 
    concUpdateTxMaxCid = (int) SQLTxBB.getBB().getSharedCounters().add(SQLTxBB.concUpdateTxMaxCid, maxCid);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_concUpdateTx() {
    sqlTxClientTest.concUpdateTx();
  }
  
  protected void concUpdateTx() {
    Connection gConn = getGFXDClientTxConnection();
    int num = 3; //reduce the number of the thread concurrently performing this tasks
    int count = 0;
    synchronized (lock) {
      count = ++concUpdateCount;
    }
    
    if (count < num) {
      gConn = concUpdateTx(gConn);
      synchronized (lock) {
        --concUpdateCount;
      }
    }
    else {
      synchronized (lock) {
        --concUpdateCount;
      }
      doDMLGfxdOnlyOps(gConn);
    }
    
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyConcUpdateTx() {
    sqlTxClientTest.verifyConcUpdateTx();
  }
  
  protected void verifyConcUpdateTx() {
    Connection gConn = getGFXDClientTxConnection();
    if (isTicket43909fixed) verifyConcUpdateTx(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_useUpdatableResultSetForInit() {
    sqlTxClientTest.useUpdatableResultSet(true);
  }
  
  public static void HydraTask_useUpdatableResultSet() {
    sqlTxClientTest.useUpdatableResultSet(false);
  }
  
  //update the column value that going to be updated and verified during the test
  protected void useUpdatableResultSet(boolean isInitTask) {   
    Connection gConn = getGFXDClientTxConnection();
    if (isTicket43935Fixed) useUpdatableResultSet(gConn, isInitTask);  
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_useScrollableUpdatableResultSet() {
    sqlTxClientTest.useScrollableUpdatableResultSet();
  }
  
  protected void useScrollableUpdatableResultSet() {   
    Connection gConn = getGFXDClientTxConnection();
    if (isTicket43932Fixed && isTicket45071fixed) useScrollableUpdatableResultSet(gConn); 
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_useNonScrollableUpdatableResultSet() {
    sqlTxClientTest.useNonScrollableUpdatableResultSet();
  }
  
  protected void useNonScrollableUpdatableResultSet() {   
    Connection gConn = getGFXDClientTxConnection();
    if (isTicket43935Fixed) useNonScrollableUpdatableResultSet(gConn); 
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyUpdatbleRsTx() {
    sqlTxClientTest.verifyUpdatbleRsTx();
  }
  
  protected void verifyUpdatbleRsTx() {
    Connection gConn = getGFXDClientTxConnection();
    verifyUpdatbleRsTx(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_useSelectForUpdateTx() {
    sqlTxClientTest.useSelectForUpdateTx();
  }
  
  protected void useSelectForUpdateTx() {   
    Connection gConn = getGFXDClientTxConnection();
    useSelectForUpdateTx(gConn); 
    closeGFEConnection(gConn);
  }  
  
  protected boolean updateSelectForUpdateTx(Connection conn, int tid, int lowCid, int highCid) {       
    try {
      Statement s = isTicket41738Fixed ? conn.createStatement() : 
        conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      s.setCursorName("updateCursor");
      String sql = "select * from trade.networth where cid > " 
        + lowCid + " and cid <= " + highCid + " FOR UPDATE OF securities"; 
      Log.getLogWriter().info(sql);
      ResultSet updatableRs = s.executeQuery(sql);
      PreparedStatement psCurrentOf = null;
      PreparedStatement psPK = null;
      if (isTicket41738Fixed) 
        psCurrentOf = conn.prepareStatement("update trade.networth set securities = securities + ? "
         + " where current of updateCursor");
      
      psPK = conn.prepareStatement("update trade.networth set securities = securities + ? "
          + " where cid = ? ");
      
      boolean success = false;
      if (isTicket41738Fixed && isTicket43188Fixed) {
        success = updateSelectForUpdateTx(updatableRs, psCurrentOf, tid);
        updatableRs.close();
        return success;
      }
      else {
        if (random.nextBoolean()) {
          success = updateSelectForUpdatePKTx(updatableRs, psPK, tid);
          updatableRs.close();
          return success;
        }
        else {
          if (!isTicket43188Fixed) {
            success = updateSelectForUpdatePKTx(updatableRs, psPK, tid);
            updatableRs.close();
            return success;            
          } else {
            success = updateSelectForUpdateRsTx(updatableRs, tid);
            updatableRs.close();
            return success;
          }
        }
      }
          
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        SQLHelper.printSQLException(se);
        
        return false; //expected conflict issue during query execution for acquiring the locks 
      } else SQLHelper.handleSQLException(se); 
    }
    return true;
  }
  
  public static void HydraTask_checkConstraints() {
    sqlTxClientTest.checkConstraints();
  }
  
  protected void checkConstraints() {
    Connection gConn = getGFXDClientTxConnection();
    
    checkUniqConstraints(gConn);
    checkFKConstraints(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  //return true if commit successfully.
  protected boolean commitGfxdOnly(Connection conn) {
    if (batchingWithSecondaryData || isHATest) {
      try {
        Log.getLogWriter().info("committing the ops for gfxd");
        conn.commit();
      } catch (SQLException se) {
        if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
          Log.getLogWriter().info("detected expected conflict during commit due to batching, continuing test");
          return false; 
        } else if (SQLHelper.gotTXNodeFailureException(se) && isHATest) {
          log().info("commit failed in gfxd due to node failure " + se
             + "(" + se.getSQLState() + ")", se);
          return false;
        }
        else
          SQLHelper.handleSQLException(se);
      } 
    } else super.commit(conn);
    return true;
  }
  
  public static void HydraTask_cycleStoreVms() {
    if (cycleVms) sqlTxClientTest.cycleStoreVms();
  }
  
  protected List <ClientVmInfo> stopStartVMs(int numToKill) {   
    if (useGemFireXDHA)
      return stopStartVMs(numToKill, "server");
    else
      return stopStartVMs(numToKill, "store");
  }
  
  private Connection getGFXDClientTxConnection(Properties info) {
    Connection conn = null;
    try {
      if (useC3P0 && useGfxdConfig)
        conn = PooledConnectionC3P0.getRCConnection(info);
      else if (useDBCP && useGfxdConfig)
        conn = PooledConnectionDBCP.getRCConnection(info);
      else if (useTomcatConnPool && useGfxdConfig)
        conn = PooledConnectionTomcat.getRCConnection(info);
      else
        conn = sql.GFEDBClientManager.getTxConnection(info);
      
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
}
