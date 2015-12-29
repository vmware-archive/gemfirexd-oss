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
package sql.wan.tx;

import hydra.Log;
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.SQLException;

import sql.GFEDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.dmlDistTxStatements.DMLDistTxStmtIF;
import sql.dmlDistTxStatements.TradeNetworthDMLDistTxStmt;
import sql.dmlStatements.AbstractDMLStmt;
import sql.sqlTx.SQLTxBB;
import sql.sqlutil.DMLDistTxStmtsFactory;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.wan.WanTest;
import util.TestException;
import util.TestHelper;

public class SQLWanTxTest extends WanTest {
  protected static SQLWanTxTest wanTxTest;
  protected static DMLDistTxStmtsFactory dmlDistTxFactory = new DMLDistTxStmtsFactory();
  protected static DMLTxStmtsFactory dmlTxFactory = new DMLTxStmtsFactory();
  
  public synchronized static void HydraTask_initialize() {
    if (wanTxTest == null) {
      wanTxTest = new SQLWanTxTest();
      wanTxTest.initialize();
    }   
  }
  
  //determine myWanSite, derbyDDLThread, gfeDDLThread, etc
  public static void HydraTask_initWanTest() {
    wanTxTest.initWanTest();
  }
  
  //init wan configuration settings, only one thread to execute this method
  public static void HydraTask_initBBForWanConfig() {
    if (myTid() == derbyDDLThread) {
      wanTxTest.initBBForWanConfig();
    }
  }
  
  public static synchronized void HydraTask_startFabricServerTask() {
    wanTxTest.startFabricServerTask();
  }
  
  public static void HydraTask_createGFESchemas() {     
    if (myTid() == gfeDDLThread) {
      wanTxTest.createGFESchemas();
    }
  }
  
  public static void HydraTask_createDiskStores() {
    if (myTid() == gfeDDLThread)
      wanTxTest.createDiskStores();
  }
  
  public static synchronized void HydraTask_createGatewaySenders() {
    if (myTid() == gfeDDLThread) {
      wanTxTest.createGatewaySenders();
    }    
  }
  
  public static synchronized void HydraTask_createGatewayReceivers() {
    if (myTid() == gfeDDLThread) {
      wanTxTest.createGatewayReceivers();
    }    
  }
    
  public static void HydraTask_createGFETables() {      
    if (myTid() == gfeDDLThread) {
      wanTxTest.createGFETables();
    }
  }
  
  public Connection getGFEConnection() {
    return getGFXDTxConnection(GFEDBManager.Isolation.READ_COMMITTED);
  }
  
  protected Connection getGFXDTxConnection(GFEDBManager.Isolation isolation) {
    Connection conn = null;
    try {
      conn = GFEDBManager.getTxConnection(isolation);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_populateTables() {
    wanTxTest.populateTables();
  }
  
  public static void HydraTask_putLastKey() {
    if (myTid() == gfeDDLThread) {
      wanTxTest.putLastKey();
    }
  }
  
  public static void HydraTask_checkQueueEmpty() {
    wanTxTest.checkQueueEmpty();
  }
  
  public static void HydraTask_writeSiteOneToBB() {
    wanTxTest.writeBaseSiteToBB();
  }
  
  public static void HydraTask_verifyWanSiteReplication() {
    wanTxTest.verifyResultSetsFromBB();
  }
  
  public static void HydraTask_doDMLOp() {
    wanTxTest.doDMLOp();
  }
  
  @Override
  protected void doDMLOp(Connection dConn, Connection gConn) {
    doDMLGfxdOnlyOps(gConn);
    //only gfxd ops for now
  }
  
  protected void doDMLGfxdOnlyOps(Connection gConn) {
    int txId = (int) SQLTxBB.getBB().getSharedCounters().incrementAndRead(SQLTxBB.txId);
    Log.getLogWriter().info("performing dmlOp, myTid is " + getMyTid() + " and txId is " + txId);
    boolean withDerby = false;
    int total = 20;
    int numOfIter = random.nextInt(total) + 1;
    for (int i=0; i<numOfIter; i++) {
      doDMLGfxdOnlyOp(gConn, 1, withDerby);
    }
    commitOrRollbackGfxdOnly(gConn);
  }
  
  protected void doDMLGfxdOnlyOp(Connection gConn, int size, boolean withDerby) {
    for (int i=0; i<size; i++) {
      doOneGfxdOnlyDMLOp(gConn, withDerby);
    }
  }
  
  @SuppressWarnings("static-access")
  protected void doOneGfxdOnlyDMLOp(Connection gConn, boolean withDerby) {
    boolean succeed = true;
    int table = dmlTables[random.nextInt(dmlTables.length)]; //get random table to perform dml
    DMLDistTxStmtIF dmlStmt= dmlDistTxFactory.createDMLDistTxStmt(table); //dmlStmt of a table

    if (table != dmlTxFactory.TRADE_CUSTOMERS && table != dmlTxFactory.TRADE_NETWORTH
      && table != dmlTxFactory.TRADE_SECURITIES && table != dmlTxFactory.TRADE_PORTFOLIO
      && table != dmlTxFactory.TRADE_SELLORDERS) return;
    //perform the opeartions
    String operation = TestConfig.tab().stringAt(SQLPrms.dmlOperations);
    if (operation.equals("insert")) {
      succeed = dmlStmt.insertGfxd(gConn, withDerby); 
    }
    else if (operation.equals("update")) {
      succeed = dmlStmt.updateGfxd(gConn, withDerby);
    }
    else if (operation.equals("delete"))
      succeed = dmlStmt.deleteGfxd(gConn, withDerby);
    else if (operation.equals("query"))
      succeed = dmlStmt.queryGfxd(gConn, withDerby);
    else
      throw new TestException("Unknown dml operation: " + operation);
    
    //insert into networth table() -- should use trigger to insert the row
    if (table == dmlTxFactory.TRADE_CUSTOMERS && operation.equals("insert") && succeed) {
      Log.getLogWriter().info("inserting into networth table");
      TradeNetworthDMLDistTxStmt networth = new TradeNetworthDMLDistTxStmt();
      if (testUniqueKeys)
        succeed = networth.insertGfxd(gConn, withDerby, AbstractDMLStmt.getCid(gConn)); //random
      else
        succeed = networth.insertGfxd(gConn, withDerby, AbstractDMLStmt.getCid()); //random
    }
    
    if (!succeed) {
      Log.getLogWriter().info("this tx get conflict exception");
    }     
  }
  
  protected void commitOrRollbackGfxdOnly(Connection gConn) { 
    int chanceToRollback = 20;
    if (random.nextInt(chanceToRollback) == 1) {
      Log.getLogWriter().info("roll back the tx");
      rollback(gConn);
    } else {
      Log.getLogWriter().info("commit the tx");
      commitGfxdOnly(gConn);
    }
  }
  
  //return true when commit successfully
  protected boolean commitGfxdOnly(Connection gConn) {
    commit(gConn);
    return true;
  }
  
  protected void rollback(Connection conn) {
    if (conn == null) return;
    try {
      conn.rollback();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_setTableCols() {
    wanTxTest.setTableCols();
  }
  
  public static void  HydraTask_createIndex() {
    wanTxTest.createIndex();
  }
  
  public static void HydraTask_createFuncForProcedures() {
    if (myTid() == gfeDDLThread) {
      wanTxTest.createFuncForProcedures();
    }
  }
  
  public static void HydraTask_createProcedures() {
    wanTxTest.createProcedures();
  }
  
  public static void HydraTask_callProcedures() {
    wanTxTest.callProcedures();
  }
  
  public static void HydraTask_doOp() {
    wanTxTest.doOps();
  }
  
  @Override
  protected void populateTables(Connection dConn, Connection gConn) { 
    //populate table without txId
    for (int i=0; i<dmlTables.length; i++) {
      //reuse the dmlStmt, as currently each insert is being committed
      DMLDistTxStmtIF dmlStmt= dmlDistTxFactory.createDMLDistTxStmt(dmlTables[i]);
      if (dmlStmt != null) {
        dmlStmt.populate(dConn, gConn);
        if (dConn !=null) waitForBarrier(); //added to avoid unnecessary foreign key reference error
      }
    }  
  }
}
