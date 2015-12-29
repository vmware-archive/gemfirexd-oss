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
package sql.view;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import hydra.Log;
import sql.SQLHelper;
import sql.SQLThinClientTest;
import sql.sqlutil.ResultSetHelper;

public class ViewTest extends SQLThinClientTest {
  public static ViewTest viewTest = new ViewTest();

  public static void HydraTask_createDiscViews() {
    viewTest.createDiscViews();
  }
 
  
  public static void HydraTask_createGfxdViewsByClients() {
    viewTest.createGfxdViews(true);
  }
  
  public static void HydraTask_populateViewBaseTablesByClients() {
    viewTest.populateViewBaseTables(true);
  }
 
  public static void HydraTask_queryViewByClients() {
    viewTest.queryView(true);
  }
  
  public static void HydraTask_queryAndVerifyViewByClients() {
    viewTest.queryAndVerifyView(true);
  }
 
  public static void HydraTask_verifyViewResultsByClients() {
    viewTest.verifyViewResults(true);
  }
 
  public static void HydraTask_createGfxdViewsByPeers() {
    viewTest.createGfxdViews(false);
  }
  
  public static void HydraTask_populateViewBaseTablesByPeers() {
    viewTest.populateViewBaseTables(false);
  }
 
  public static void HydraTask_queryViewByPeers() {
    viewTest.queryView(false);
  }
  
  public static void HydraTask_queryAndVerifyViewByPeers() {
    viewTest.queryAndVerifyView(false);
  }
 
  public static void HydraTask_verifyViewResultsByPeers() {
    viewTest.verifyViewResults(false);
  }
  
  protected void queryView(boolean gfxdclient) {
    Vector<String> queryVec = ViewPrms.getQueryViewsStatements();
    String viewQuery = queryVec.get(random.nextInt(queryVec.size()));
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection(); 
    //provided for thin client driver as long as isEdge is set
    //use this especially after product default has been changed to read committed
    queryResultSets(dConn, gConn, viewQuery, false);
    closeDiscConnection(dConn); 
    closeGFEConnection(gConn);
  }
  
  protected void queryAndVerifyView(boolean gfxdclient) {
    Vector<String> queryVec = ViewPrms.getQueryViewsStatements();
    String viewQuery = queryVec.get(random.nextInt(queryVec.size()));
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection();
    queryResultSets(dConn, gConn, viewQuery, true);
    closeDiscConnection(dConn); 
    closeGFEConnection(gConn);
  }
  
  protected void verifyViewResults(boolean gfxdclient) {
    Vector<String> queryVec = ViewPrms.getQueryViewsStatements();
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection(); 
    for (int i = 0; i < queryVec.size(); i++) {
      String viewQuery = queryVec.get(i);
      queryResultSets(dConn, gConn, viewQuery, true);
    }
    closeDiscConnection(dConn); 
    closeGFEConnection(gConn);
  }
  
  protected void queryResultSets(Connection dConn, Connection gConn, String queryStr, boolean verify) {
    try {
      Log.getLogWriter().info("Query view: " + queryStr);
      ResultSet dRS = dConn.createStatement().executeQuery(queryStr);
      ResultSet gRS = gConn.createStatement().executeQuery(queryStr);
      if (verify) {
        ResultSetHelper.compareResultSets(dRS, gRS);
      }
      commit(dConn);
      commit(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }  
  }
  
  protected void populateViewBaseTables(boolean gfxdclient) {
    String viewDataPath = ViewPrms.getViewDataFilePath();
    if (hasDerbyServer) {
      Connection conn = getDiscConnection();
      Log.getLogWriter().info("Populating view base tables in disc using sql script: " + viewDataPath);
      SQLHelper.runDerbySQLScript(conn, viewDataPath, true);
      Log.getLogWriter().info("done populating view base tables in disc.");
      closeDiscConnection(conn);      
    }
    Connection conn = getGFEConnection();
    Log.getLogWriter().info("Populating view base tables in gfxd using sql script: " + viewDataPath);
    SQLHelper.runSQLScript(conn, viewDataPath, true);
    Log.getLogWriter().info("done populating view base tables in gfxd.");
    closeGFEConnection(conn);  
  }
  
  protected void createDiscViews() {
    if (!hasDerbyServer) {
      return;
    }   
    Connection conn = getDiscConnection();
    String viewDDLPath = ViewPrms.getViewDDLFilePath();
    Log.getLogWriter().info("creating views on disc using sql script: " + viewDDLPath);
    SQLHelper.runDerbySQLScript(conn, viewDDLPath, true);
    Log.getLogWriter().info("done creating views on disc.");
    closeDiscConnection(conn);
  }
  
  protected void createGfxdViews(boolean gfxdclient) {
    Connection conn = getGFEConnection();
    String viewDDLPath = ViewPrms.getViewDDLFilePath();
    Log.getLogWriter().info("creating views in gfxd using sql script: " + viewDDLPath);
    SQLHelper.runSQLScript(conn, viewDDLPath, true);
    Log.getLogWriter().info("done creating views in gfxd.");
    closeGFEConnection(conn);
  }
}
