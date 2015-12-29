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
package sql.wan.thinClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import hydra.ClientVmInfo;
import hydra.EdgeHelper;
import hydra.HydraVector;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.ddlStatements.FunctionDDLStmt;
import sql.wan.SQLWanPrms;
import sql.wan.WanTest;
import util.TestException;
import util.TestHelper;


public class WanClientTest extends WanTest {
  protected static WanClientTest wanClientTest = new WanClientTest();
  
  static int numOfClientThreadsPerSite = conftab.intAt(SQLWanPrms.numOfClientThreadsPerSite, 0);
  static int numOfClientNodes = conftab.intAt(SQLWanPrms.numOfClientNodes, 0);
  static int numOfServersPerSite = conftab.intAt(SQLWanPrms.numOfServersPerSite);
  
  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    FabricServerHelper.createLocator();
  }
  
  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public static void startAndConnectLocatorTask() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    if (networkServerConfig == null) {
      Log.getLogWriter().info("Starting peer locator only");
      FabricServerHelper.startLocator();
    } else {
      Log.getLogWriter().info("Starting network locator");
      FabricServerHelper.startLocator(networkServerConfig);
    }    
  }
  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    FabricServerHelper.stopLocator();
  }

  public synchronized static void HydraTask_initialize() {
    wanClientTest.initialize();
  }
  
  public static synchronized void HydraTask_initEdges() {
    isEdge = true;    
  }
  
  //determine myWanSite, derbyDDLThread, gfeDDLThread, etc
  public static void HydraTask_initWanClientTest() { 
    if (numOfWanSites > 5) throw new TestException("test could not handle more than 5 wan sites yet");
    //if (numOfWanSites < 3) throw new TestException("test should have more than 2 wan sites");

    if (isEdge) 
      myWanSite = EdgeHelper.toWanSite(RemoteTestModule.getMyClientName());
    else
      myWanSite = FabricServerHelper.getDistributedSystemId();
    
    if (isEdge) {
      int whichOne =(int) bb.getSharedCounters().incrementAndRead(
          SQLBB.wanDerbyDDLThread);
      if (whichOne == 1) {
        derbyDDLThread = myTid();
        Log.getLogWriter().info("derbyDDLThread is " + derbyDDLThread);
      } //determine derby ddl thread,
      
      if ((myTid())%numOfClientThreadsPerSite == 0) {
        gfeDDLThread = myTid();
        Log.getLogWriter().info("gfeDDLThread is " + gfeDDLThread);
        
        Log.getLogWriter().info("testUniqueKeys is " + testUniqueKeys);
        Log.getLogWriter().info("withReplicateTables is " + TestConfig.tab().booleanAt(SQLPrms.withReplicatedTables, false));
      } //determine gfe ddl thread, each wan site has one
    } else {    
      if (numOfClientNodes == 0) throw new TestException("test issue, num of clients must set in the conf");
      
      //computes sender in each site, will use 2 in each sit
       Log.getLogWriter().info("myVMid is " + myVMid + " numOfClientNode is " + numOfClientNodes + " numOfLocators is " + numOfLocators);
      if (myVMid >= numOfClientNodes + numOfLocators) {
        int serverId = myVMid - (numOfClientNodes + numOfLocators);
        if (serverId % numOfServersPerSite == 0 || serverId % numOfServersPerSite == 1)
          isSender = true;
      }
    }
  }
  
  //init wan configuration settings, only one thread to execute this method
  public static void HydraTask_initBBForWanConfig() {
    if (myTid() == derbyDDLThread) {
      wanClientTest.initBBForWanConfig();
    }
  }
  
  public static synchronized void HydraTask_startFabricServerTask() {
    wanClientTest.startFabricServerTask();
  }
  
  protected void startFabricServerTask() {
    if (isSender) startFabricServerSGSender();
    else startFabricServer();
  }
  
  public static synchronized void HydraTask_startFabricServerSGTask() {
    wanClientTest.startFabricServerSGTask();
  }
  
  protected void startFabricServerSGTask() {
    if (isSender) startFabricServerSGSender();
    else startFabricServerSG();
  }
  
  //write to bb for data stores to randomly chose from
  public static void HydraTask_initForServerGroup() {
    wanClientTest.initForServerGroup();
  }
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }
  
  @SuppressWarnings("unchecked")
  protected String getSGForNode() {
    HydraVector vec = TestConfig.tab().vecAt(SQLPrms.serverGroups);
    int whichOne = -1;
    
    if (numOfServersPerSite < 6) {
      throw new TestException ("not enough data node per site to satisfy all server group requirement" +
          " must have at least 6 server node per site, but this test only has " + numOfServersPerSite);
    }
    
    if (myVMid >= numOfClientNodes + numOfLocators) {
      int storeId = myVMid - numOfClientNodes - numOfLocators - 2; //2 senders
      whichOne = storeId % numOfServersPerSite;
      Log.getLogWriter().info("which one is " + whichOne);
    } //calculate which one 
    
    if (whichOne >= vec.size()) {
      whichOne=vec.size()-1; //default to last element, if vms are more
    }

    String sg = (String)((HydraVector)vec.elementAt(whichOne)).elementAt(0); //get the item

    if (sg.startsWith("random")) {
      ArrayList<String> serverGroups = (ArrayList<String>) SQLBB.getBB().getSharedMap()
        .get("serverGroups");
      if(whichOne>=serverGroups.size()) {
        whichOne = random.nextInt(serverGroups.size()); //randomly select one
      }
      sg = serverGroups.get(whichOne); //a random server group except for the first 4 data stores
    }
    Log.getLogWriter().info("This server is in " + sg);
    
    return sg;
  }
  
  //determine the thread will do derby DDL ops
  public static void HydraTask_createDiscDB() {
    if (derbyDDLThread == myTid()) {
      wanClientTest.createDiscDB();
    }
  }
  
  public static void HydraTask_createDiscSchemas() {
    if (derbyDDLThread == myTid()) {
      wanClientTest.createDiscSchemas();
    }
  }
  
  public static void HydraTask_createDiscTables() {
    if (derbyDDLThread == myTid()) {
      wanClientTest.createDiscTables();
    }
  }
  
  public static void HydraTask_createGFESchemas() {     
    if (myTid() == gfeDDLThread) {
      wanClientTest.createGFESchemas();
    }
  }
  
  protected void createGFESchemas() {
    Connection conn = getGFXDClientConnection();
    createGFESchemas(conn);
    closeGFEConnection(conn);
  }
  
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
  
  protected Connection getGFXDClientConnection(Properties info) {
    Connection conn = null;
    try {
      conn = sql.GFEDBClientManager.getConnection(info);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static synchronized void HydraTask_createGatewaySenders() {
    if (myTid() == gfeDDLThread) {
      wanClientTest.createGatewaySenders();
    }    
  }
  
  protected void createGatewaySender(int remoteSiteId) {
    String ddl = getSenderDDL(remoteSiteId);
    
    Connection conn = getGFXDClientConnection();
    try {
      Log.getLogWriter().info("executing " + ddl);
      conn.createStatement().execute(ddl);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    commit(conn);
    closeGFEConnection(conn);
  }
  
  public static synchronized void HydraTask_createGatewayReceivers() {
    if (myTid() == gfeDDLThread) {
      wanClientTest.createGatewayReceivers();
    }    
  }
    
  //which server group receivers should be on
  protected void createGatewayReceivers() {
    //using sender server group same as receiver group
    createGatewayReceivers(sgSender);
  }
  
  protected void createGatewayReceivers(String sg) {
    Connection gConn = getGFXDClientConnection();
    createGatewayReceivers(gConn, sg);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_createGFETables() {      
    if (myTid() == gfeDDLThread) {
      wanClientTest.createGfxdWanTables();
    }
  }
  
  protected void createGfxdWanTables() {
    if (testPartitionBy && !useSamePartitionAllWanSites) {
      //need to create table synchronously to avoid partitioned key setting by another site
      getLock();
      createGfxdTables();
      releaseLock();      
    } else
      createGfxdTables();
  }
  
  protected void createGfxdTables() {
    Connection conn = getGFXDClientConnection();
    Log.getLogWriter().info("dropping tables in gfe.");
    dropTables(conn); //drop table before creating it -- impact for ddl replay
    Log.getLogWriter().info("done dropping tables in gfe");
    Log.getLogWriter().info("creating tables in gfe.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfe.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_populateTables() {
    if (dumpThreads && RemoteTestModule.getCurrentThread().getThreadId() == derbyDDLThread)
      wanClientTest.dumpThreads();
    else
      wanClientTest.populateTables();
  }
  
  protected void populateTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    } //only verification case need to populate derby tables
    Connection gConn = getGFXDClientConnection();
    populateTables(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_putLastKey() {
    if (myTid() == gfeDDLThread) {
      wanClientTest.putLastKey();
    }
  }
  
  protected void putLastKey() {
    Connection gConn = getGFXDClientConnection();
    putLastKey(gConn); 
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setTableCols() {
    wanClientTest.setTableCols();
  }
  
  //can be called only after table has been created
  protected void setTableCols() {
    Connection gConn = getGFXDClientConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_checkQueueEmpty() {
    wanClientTest.checkQueueEmpty();
  }
  
  public static void HydraTask_verifyPublisherResultSets() {
    wanClientTest.verifyResultSets();
  }
  
  public static void HydraTask_verifyResultSets() {    
    wanClientTest.verifyResultSets();
  }
  
  protected void verifyResultSets() {
    if (myTid()!= gfeDDLThread) return;    
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      return;
    }
    
    int sleepMs = 100;
    while (!isLastKeyArrived()) {
      Log.getLogWriter().info("last key is not arrived yet, sleep for " + sleepMs + " ms");
      hydra.MasterController.sleepForMs(sleepMs);             
    } 
    
    Connection dConn = getDiscConnection();
    Connection gConn = getGFXDClientConnection();

    verifyResultSets(dConn, gConn);
    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  //check if the the lastKey is arrived.
  protected boolean isLastKeyArrived() {
    Connection gConn = getGFXDClientConnection();
    boolean isLastKeyArrived = isLastKeyArrived(gConn);
    closeGFEConnection(gConn);
    return isLastKeyArrived;
  }
  
  public static void HydraTask_verifyWanSiteReplication() {
    wanClientTest.verifyResultSetsFromBB();
  }
  
  protected void verifyResultSetsFromBB() {
    Connection gConn = getGFXDClientConnection(); 
    verifyResultSetsFromBB(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_createFunctionToPopulate(){
    wanClientTest.createFunctionToPopulate();
  }
  
  protected void createFunctionToPopulate() {
    Connection gConn = getGFXDClientConnection();
    try {
      FunctionDDLStmt.createFuncPortf(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void  HydraTask_createIndex() {
    wanClientTest.createIndex();
  }
  
  protected void createIndex() {
    boolean gotLock = false;
    synchronized(indexLock) {
      gotLock = toCreateIndex;
      toCreateIndex = false;
    }
    if (gotLock) {
      Connection gConn = getGFXDClientConnection();
      createIndex(gConn);
      closeGFEConnection(gConn);
      synchronized(indexLock) {
        toCreateIndex = true; //allow next one to create index
      }
    }
  }
  
  public static void HydraTask_createProcedures() {
    wanClientTest.createProcedures();
  }
  
  protected void createProcedures() {
    Log.getLogWriter().info("performing create procedure Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    createProcedures(dConn, gConn);
    
    if (dConn!= null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_callProcedures() {
    wanClientTest.callProcedures();
  }
  
  protected void callProcedures() {
    Log.getLogWriter().info("call procedures, myTid is " + getMyTid());
    Connection dConn = null;
    if (hasDerbyServer){
      dConn = getDiscConnection();
    }
    Connection gConn = getGFXDClientConnection();
    callProcedures(dConn, gConn);

    if (hasDerbyServer){
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_doOp() {
    wanClientTest.doOps();
  }
  
  protected void doOps() {
    int num = 10;
    boolean dmlOp = (random.nextInt(num)==0)? false: true; //1 out of num chance to do ddl operation
    if (dmlOp) doDMLOp();
    else doDDLOp();
  }
  
  protected void doDDLOp() {
    //TODO differentiate which ddl statements to be performed
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    
    doDDLOp(dConn, gConn);

    if (dConn!=null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);

    Log.getLogWriter().info("done ddlOp");
  }
  
  public static void HydraTask_doDMLOp() {
    wanClientTest.doDMLOp();
  }
  
  protected void doDMLOp() {    
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    doDMLOp(dConn, gConn);

    if (dConn!=null) {
      closeDiscConnection(dConn);
      //Log.getLogWriter().info("closed the disc connection");
    }
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }
  
  public static void HydraTask_createDiskStores() {
    if (myTid() == gfeDDLThread)
      wanClientTest.createDiskStores();
  }
  
  protected void createDiskStores() {
    Connection conn = getGFXDClientConnection();
    createDiskStores(conn);
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_writeSiteOneToBB() {
    wanClientTest.writeBaseSiteToBB();
  }
  
  //need to configured in the conf that only one site will run this
  protected void writeBaseSiteToBB() {
    Connection gConn = getGFXDClientConnection(); 
    writeBaseSiteToBB(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_createFuncForProcedures() {
    if (myTid() == gfeDDLThread) {
      wanClientTest.createFuncForProcedures();
    }
  }
  
  protected void createFuncForProcedures() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGFXDClientConnection();
    createFuncForProcedures(dConn, gConn);
    
    if (dConn!= null) {
      commit(dConn);
      closeDiscConnection(dConn);
    }
    
    commit(gConn);
    closeGFEConnection(gConn);  
  }
  
  public static void HydraTask_cycleStoreVms() {
    wanClientTest.cycleStoreVms();
  }
  
  protected List<ClientVmInfo> stopStartNonSenderVMs(int numToKill) {    
    return stopStartNonSenderVMs(numToKill, "server_" + myWanSite + "_" );
  }
  
  public static void HydraTask_cycleSenderVms() {
    wanClientTest.cycleSenderVms();
  }
  
  public static void HydraTask_checkConstraints() {
    if (myTid() == derbyDDLThread) {
      wanClientTest.checkConstraints();
    } //only one thread is needed as test will verify if all wan sites get replica of others
  }
  
  protected void checkConstraints() {
    Connection gConn = getGFXDClientConnection();    
    checkUniqConstraints(gConn);
    checkFKConstraints(gConn);
    closeGFEConnection(gConn);
  }
  
  protected void doDDLOp(Connection dConn, Connection gConn) {
    super.doDDLOp(dConn, gConn);
  }

}
