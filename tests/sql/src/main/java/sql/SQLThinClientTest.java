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
package sql;

import java.sql.Connection;
import java.sql.SQLException;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import sql.GFEDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class SQLThinClientTest extends SQLTest {
  //do not understand why the SQLTest was implemented in this way, it seems want it to 
  //singleton -- do not change the SQLTest, but will introduce a new way which I think
  //better to implement this class
  protected static SQLThinClientTest sqlThinClientTest = new SQLThinClientTest();
  protected SQLThinClientTest() {};

  //inherit from the SQLTest the methods
  //HydraTask_createGfxdLocatorTask()
  //HydraTask_startGfxdLocatorTask()
  //HydraTask_stopGfxdLocatorTask
  
  public static synchronized void HydraTask_initializeServer() {    
    PRObserver.installObserverHook();
    PRObserver.initialize(RemoteTestModule.getMyVmid());
    initSQLTest();
  }

  public static synchronized void HydraTask_initClient() {
    initSQLTest();
  }

  public synchronized static void HydraTask_startFabricServer() {
    sqlThinClientTest.startFabricServer();
  }

  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
  }

  public static void HydraTask_createGfxdSchemasByClients() {
    sqlThinClientTest.createGfxdSchemasByClients();
  }

  protected void createGfxdSchemasByClients() {
    Connection conn = getGFEConnection();
    Log.getLogWriter().info("creating schema in gfxd.");
    if (!testServerGroupsInheritence) {
      createSchemas(conn);
    } else {
      String[] schemas = SQLPrms.getGFESchemas(); //with server group
      createSchemas(conn, schemas);
    }
    Log.getLogWriter().info("done creating schema in gfxd.");
    closeGFEConnection(conn);
  }

  public static void HydraTask_createGfxdTablesByClients() {
    sqlThinClientTest.createGfxdTablesByClients();
  }

  protected void createGfxdTablesByClients() {
    Connection conn = getGFEConnection();
    Log.getLogWriter().info("creating tables in gfxd.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfxd.");
    closeGFEConnection(conn);
  }

  //use getGFEConnection() instead now
  //it uses thin client driver, as long as isEdge is set
  //use this especially after product default has been changed to read committed
  /* 
  protected Connection getGFXDClientTxConnection() {
    Connection conn = null;
    try {
      //conn = sql.GFEDBClientManager.getTxConnection(GFEDBManager.Isolation.READ_COMMITTED);
      //Use 0 isolation level first and to see if it can expanded to higher in future
      conn = sql.GFEDBClientManager.getTxConnection(GFEDBManager.Isolation.NONE);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get the connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  */

  public static void HydraTask_createDiscDB() {
    sqlThinClientTest.createDiscDB();
  }

  public static void HydraTask_createDiscSchemas() {
    sqlThinClientTest.createDiscSchemas();
  }

  public static void HydraTask_createDiscTables() {
    sqlThinClientTest.createDiscTables();
  }

  public static void HydraTask_setTableCols() {
    sqlThinClientTest.setTableCols();
  }

  //can be called only after table has been created
  protected void setTableCols() {
    Connection gConn = getGFEConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }

  /* avoid using this after product default change to isolation to read committed
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
  
  public static void HydraTask_populateTxTables() {
    sqlThinClientTest.populateTxTables();
  }

  protected void populateTxTables() {
    Connection dConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    Connection gConn = getGFEConnection();
    populateTables(dConn, gConn);
    commit(gConn);
    commit(dConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }


  public static void HydraTask_verifyResultSets() {
    sqlThinClientTest.verifyResultSets();
  }


  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      cleanConnection(getGFEConnection());
      return;
    }
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection();

    verifyResultSets(dConn, gConn);
  }

  public static void HydraTask_doDMLOp() {
    sqlThinClientTest.doDMLOp();
  }

  protected void doDMLOp() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    Connection gConn = getGFEConnection();
    doDMLOp(dConn, gConn);

    if (dConn!=null) {
      closeDiscConnection(dConn);
      //Log.getLogWriter().info("closed the disc connection");
    }
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }

  public static void HydraTask_queryOnJoinOp() {
    sqlThinClientTest.queryOnJoinOp();
  }
  
  protected void queryOnJoinOp() {
    Log.getLogWriter().info("performing queryOnJoin, myTid is " + getMyTid());

    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    } 
    Connection gConn = getGFEConnection();

    queryOnJoinOp(dConn, gConn);

    if (dConn!=null) {
      closeDiscConnection(dConn);
      //Log.getLogWriter().info("closed the disc connection");
    }
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }
  
  protected static void initSQLTest() {
    sqlTest = new SQLTest();
    sqlTest.initialize();
    //not initiated in the SQLTest
    isEdge = true;    
  }
}
