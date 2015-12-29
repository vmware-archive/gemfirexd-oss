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
package sql.security.thinClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import sql.ClientDiscDBManager;
import sql.GFEDBClientManager;
import sql.GFEDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.ddlStatements.AuthorizationDDLStmt;
import sql.security.SQLSecurityTest;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class SQLBridgeSecurityTest extends SQLSecurityTest {
  protected static SQLBridgeSecurityTest sbsTest;
  
  public static synchronized void HydraTask_initialize() {
    if (sbsTest == null) {
      sbsTest = new SQLBridgeSecurityTest();
      if (ssTest == null) ssTest = new SQLSecurityTest();
      
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());

      sbsTest.initialize();
    }
  }
  
  public static void HydraTask_setupGfxdUserAuthentication() {
    sbsTest.setupGfxdUserAuthentication();
  }
  
  protected void setupGfxdUserAuthentication() {
    Log.getLogWriter().info("in gfxd setupUserAuth");
    Connection conn = getGfxdSuperUserClientConnection();
    logGfxdSystemProperties();
    try {
      createGfxdUserAuthentication(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }
  
  public static void HydraTask_createGFESchemas() {
    sbsTest.createGFESchemas();
  }
  
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
  
  public static void HydraTask_createGFETables(){
    sbsTest.createFuncMonth();  //partition by expression
    sbsTest.createGFETables();
  }
  
  protected void createFuncMonth(){
    Connection conn = getAuthGfxdClientConnection();
    createFuncMonth(conn);
  }

  protected void createGFETables() {
    Connection conn = getAuthGfxdClientConnection();
    //Connection conn = getAuthGfxdConnection("trade");
    Log.getLogWriter().info("dropping tables in gfe.");
    dropTables(conn); //drop table before creating it -- impact for ddl replay
    Log.getLogWriter().info("done dropping tables in gfe");
    Log.getLogWriter().info("creating tables in gfe.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfe.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_setTableCols() {
    sbsTest.setTableCols();
  }
  
  protected void setTableCols() {
    Connection gConn = getAuthGfxdClientConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
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
  
  protected Connection getGfxdSystemUserClientConnection() {
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
  
  public static void HydraTask_connectWithInvalidPassword() {
    sbsTest.connectWithInvalidPassword();
  }
  
  public static void HydraTask_connectWithNoCredentials() {
    sbsTest.connectWithNoCredentials();
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
  
  protected void connectWithNoCredentials() {
    List<SQLException> exList = new ArrayList<SQLException>();
    if (hasDerbyServer) {
      try {
        ClientDiscDBManager.getNoneAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
      }
      try {
        GFEDBClientManager.getNoneAuthConnection(getMyTid());
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
          "no credentials");
    }
    
  }
  
  public static void HydraTask_setSuperUserConnections() {
    sbsTest.initThreadLocalConnection();
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
      Connection gfxdSystemUser = getGfxdSystemUserClientConnection();
      gfxdSystemUserConn.set(gfxdSystemUser);
    } //used for querying without need to check authorization
  }
  
  public static void HydraTask_provideAllPrivToAll() {
    sbsTest.provideAllPrivToAll();
  }
  
  protected void provideAllPrivToAll() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdClientConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();
    authDDL.provideAllPrivToAll(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_populateTables() {
    sbsTest.populateTables();
  }
  
  protected void populateTables() {
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdClientConnection();
    populateTables(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_verifyResultSets() {
    sbsTest.verifyResultSets();
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
    sbsTest.doOps();
  }

  protected void doOps() {
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
}
