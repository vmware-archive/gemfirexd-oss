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
package sql.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import hydra.Log;
import hydra.Prms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.ClientVmInfo;
import sql.ClientDiscDBManager;
import sql.GFEDBClientManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.ddlStatements.AuthorizationDDLStmt;
import sql.ddlStatements.FunctionDDLStmt;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class SQLSecurityClientTest extends SQLSecurityTest {
  protected static SQLSecurityClientTest ssClientTest;
  
  public static synchronized void HydraTask_initialize() {
    if (ssClientTest == null) {
      ssClientTest = new SQLSecurityClientTest();
      //if (sqlTest == null) sqlTest = new SQLTest();
      
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());

      ssClientTest.initialize();
    }
  }
  
  public static void HydraTask_setupUsers() {
    ssClientTest.setupUsers();
  }
  
  public static void HydraTask_initUsers() {
    ssClientTest.initUsers();
  }
  
  public static void HydraTask_createAuthDiscDB() {
    ssClientTest.createAuthDiscDB();
  }
  
  public static void HydraTask_setupDerbyUserAuthentication() {
    ssClientTest.setupDerbyUserAuthentication();
  }
  
  public static synchronized void HydraTask_startAuthFabricServer() {
    if (serverStarted[0]) {
      Log.getLogWriter().info("fabric server is started");
      //work around #42499
      return;  
    }
    
    ssClientTest.startAuthFabricServer();
  } 
  
  public static void HydraTask_startAuthFabricServerAsSuperUser() {
    ssClientTest.startAuthFabricServerAsSuperUser();
  }
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }
  
  public static synchronized void HydraTask_initEdges() {
    if (ssClientTest == null) {
      ssClientTest = new SQLSecurityClientTest();     
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
  
  public static void HydraTask_setupGfxdUserAuthentication() {
    if (!useLDAP) ssClientTest.setupGfxdUserAuthentication();
  } 
  
  public static void HydraTask_changePassword() {
    if (!useLDAP) ssClientTest.changePassword();
  } 
  
  protected void changePassword() {
    Log.getLogWriter().info("in gfxd to change my password");
    Connection conn = getAuthGfxdClientConnection();
    try {
      changePassword(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
    
  public static synchronized void HydraTask_createDiscDB() {
    ssClientTest.createDiscDB();
  }
  
  public static synchronized void HydraTask_createDiscSchemas() {
    ssClientTest.createDiscSchemas();
  }

  public static synchronized void HydraTask_createDiscTables(){
    ssClientTest.createDiscTables();
  }
  
  public static void HydraTask_createGFESchemas() {
    ssClientTest.createGFESchemas();
  }
  
  //use gfe conn to create schemas
  protected void createGFESchemas() {
    Connection conn = null;
    if (!sqlAuthorization || !bootedAsSuperUser)
     conn = getAuthGfxdClientConnection();
    else {
      conn = getGfxdSuperUserClientConnection();
    }
    Log.getLogWriter().info("creating schemas in gfxd.");
    if (!testServerGroupsInheritence) createSchemas(conn);
    else {
      String[] schemas = SQLPrms.getGFESchemas(); //with server group
      createSchemas(conn, schemas);
    }
    Log.getLogWriter().info("done creating schemas in gfxd.");
    closeGFEConnection(conn);
  }
  
  protected Connection getAuthGfxdClientConnection() {
    Connection conn = null;
    try {
      conn = GFEDBClientManager.getAuthConnection(getMyTid());
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getAuthGfxdConnection(String user, String password) throws SQLException {
    Connection conn = null;
    try {
      conn = GFEDBClientManager.getAuthConnection(user, password);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw se;
    }
    return conn;
  }
  
  protected Connection getGfxdSuperUserClientConnection() {
    Connection conn = null;
    try {
      conn = GFEDBClientManager.getSuperUserConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection as system user " 
          + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_createGFETables(){
    ssClientTest.createFuncMonth();  //partition by expression
    ssClientTest.createGFETables();
  }
  
  protected void createFuncMonth(){
    Connection conn = getAuthGfxdClientConnection();
    createFuncMonth(conn);
    closeGFEConnection(conn);
  }

  protected void createGFETables() {
    Connection conn = getAuthGfxdClientConnection();
    Log.getLogWriter().info("dropping tables in gfe.");
    dropTables(conn); //drop table before creating it -- impact for ddl replay
    Log.getLogWriter().info("done dropping tables in gfe");
    Log.getLogWriter().info("creating tables in gfe.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfe.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_createFunctionToPopulate() {
    ssClientTest.createFunctionToPopulate();
  }
  
  protected void createFunctionToPopulate() {
    Connection gConn = getAuthGfxdClientConnection();
    try {
      FunctionDDLStmt.createFuncPortf(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setTableCols() {
    ssClientTest.setTableCols();
  }
  
  protected void setTableCols() {
    Connection gConn = getAuthGfxdClientConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_createFuncForSubquery() {
    ssClientTest.createFuncForSubquery();
  }
  
  protected void createFuncForSubquery() {
    Log.getLogWriter().info("performing create function maxCid Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }
    Connection gConn = getAuthGfxdClientConnection();

    try {
      FunctionDDLStmt.createFuncMaxCid(dConn);
      FunctionDDLStmt.createFuncMaxCid(gConn);
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
  
  public static void HydraTask_setSuperUserConnections() {
    ssClientTest.initThreadLocalConnection();
  }
  
  /** 
   * Sets the per-thread Connection instance.
   */
  protected void initThreadLocalConnection() {
    if (hasDerbyServer) {
      Connection derbySuperUser = getDerbySuperUserConnection();
      derbySuperUserConn.set(derbySuperUser);
    }
    if (bootedAsSuperUser) {
      Connection gfxdSuperUser = getGfxdSuperUserClientConnection();
      gfxdSuperUserConn.set(gfxdSuperUser);
    } else {
      Log.getLogWriter().info("booted as systemUser, getting connection as systemUser");
      Connection gfxdSystemUser = getGfxdSystemUserConnection();
      gfxdSystemUserConn.set(gfxdSystemUser);
    } //used for querying without need to check authorization   
  }
  
  protected Connection getGfxdSystemUserConnection() {
    Connection conn = null;
    try {
      conn = GFEDBClientManager.getAuthConnection(systemUser);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection as system user " 
          + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_delegatePrivilege() {
    ssClientTest.delegatePrivilege();
  }

  protected void delegatePrivilege() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdClientConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();

    authDDL.delegateGrantOption(dConn, gConn);
    
    commit(dConn);
    commit(gConn);
    
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_grantDelegatedPrivilege() {
    ssClientTest.grantDelegatedPrivilege();
  }
  
  public static void HydraTask_revokeDelegatedPrivilege() {
    ssClientTest.revokeDelegatedPrivilege();
  }

  protected void grantDelegatedPrivilege() {
  Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdClientConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();
    authDDL.useDelegatedPrivilege(dConn, gConn);

    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  protected void revokeDelegatedPrivilege() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
      Connection dConn =null;
      if (hasDerbyServer) {
        dConn = getAuthDerbyConnection();
      }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdClientConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();
    authDDL.revokeDelegatedPrivilege(dConn, gConn);

    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_populateTables() {
    ssClientTest.populateTables();
  }
  
  protected void populateTables() {
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdClientConnection();
    populateTables(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setUserAccessPriviledge() {
    ssClientTest.setUserAccessPriviledge(); 
  }
  
  protected void setUserAccessPriviledge() {
    Connection dConn = null;
    Connection gConn = getGfxdSuperUserClientConnection();
    setUserAccessPriviledge(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_provideAllPrivToAll() {
    ssClientTest.provideAllPrivToAll();
  }
  
  protected void provideAllPrivToAll() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    } 
    Connection gConn = getAuthGfxdClientConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();

    authDDL.provideAllPrivToAll(dConn, gConn);

    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyResultSets() {
    ssClientTest.verifyResultSets();
  }
  
  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      return;
    }
    Connection dConn = getAuthDerbyConnection();
    Connection gConn = getAuthGfxdClientConnection();

    verifyResultSets(dConn, gConn);
    closeDiscConnection(dConn);
    closeGFEConnection(gConn);    
  }
  
  public static void HydraTask_doOps() {
    ssClientTest.doOps();
  }

  protected void doOps() {
    if (random.nextInt(100) == 1) {
      connectWithInvalidPassword();
    }
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdClientConnection();
    doOps(dConn, gConn);
    
    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
    if (hasDerbyServer) {
      waitForBarrier();
    }
  }
  
  protected void doAuthOp() {
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdClientConnection();
    doAuthOp(dConn, gConn);
    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  public static synchronized void HydraTask_shutdownFabricServers() {
    if (ssClientTest == null) {
      ssClientTest = new SQLSecurityClientTest();
    }
    ssClientTest.shutdownFabricServers();
  }
  
  protected void connectWithInvalidPassword() {
    if (hasDerbyServer) {
      List<SQLException> exList = new ArrayList<SQLException>();
      try {
        ClientDiscDBManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
      }
      try {
        GFEDBClientManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
      }
      SQLHelper.handleMissedSQLException(exList);
    } else {
      try {
        GFEDBClientManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        if (se.getSQLState().equalsIgnoreCase("08004")) {
          Log.getLogWriter().info("Got expected invalid credentials, continuing tests");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      throw new TestException("does not get invalid credentials exception while using " +
          "invalid credentials");
    }

  }

  public static void HydraTask_shutdownFabricLocators() {
    ssClientTest.shutdownFabricLocators();
  }

  public static synchronized void HydraTask_cycleStoreVms() {
    if (ssClientTest == null) {
      ssClientTest = new SQLSecurityClientTest();
    }

    ssClientTest.cycleStoreVms();
  }

  protected List <ClientVmInfo> stopStartVMs(int numToKill) {
    return stopStartVMs(numToKill, "server");
  }
  
  public static void HydraTask_createProcedures() {
    ssClientTest.createProcedures();
  }
  
  protected void createProcedures() {
    Log.getLogWriter().info("performing create procedure Op, myTid is " + getMyTid());
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdClientConnection();
    createProcedures(dConn, gConn);
    
    if (dConn!= null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }

}
