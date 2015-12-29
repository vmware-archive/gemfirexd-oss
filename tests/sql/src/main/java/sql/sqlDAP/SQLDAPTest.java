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
package sql.sqlDAP;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import hydra.HydraVector;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import util.PRObserver;

public class SQLDAPTest extends SQLTest {
  protected static SQLDAPTest dapTest = null;
  public static boolean concurrentDropDAPOp = TestConfig.tab().booleanAt(SQLDAPPrms.concurrentDropDAPOp, false);
  
  public static boolean cidByRange = TestConfig.tab().booleanAt(SQLDAPPrms.cidByRange, false);
  public static boolean tidByList = TestConfig.tab().booleanAt(SQLDAPPrms.tidByList, false);
  //cidByRange & tidByList default to false,  as PartitionClause
  //setting partition strategy based on this setting -- 
  static String dapList = "DAPList";
  
  public static synchronized void HydraTask_initialize() {
    if (dapTest == null) {
      dapTest = new SQLDAPTest();
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
      if (sqlTest == null) sqlTest = new SQLTest();
    } 
    dapTest.initialize();
  }
  
  protected void initialize() {
    super.initialize();
  }
  
  public static void HydraTask_createDAProcedures() {
    dapTest.createDAProcedures();
  }

  protected void createDAProcedures() {
    Log.getLogWriter().info("performing create data aware procedure Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getHdfsQueryConnection();
    createDAProcedures(dConn, gConn);
    
    if (dConn!= null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  protected void createDAProcedures(Connection dConn, Connection gConn) {
    sql.ddlStatements.DAPDDLStmt procStmt = new sql.ddlStatements.DAPDDLStmt();
    procStmt.createDDLs(dConn, gConn);

    commit(dConn);
    commit(gConn);
  }
  
  public static void HydraTask_callDAProcedures() {
    dapTest.callDAProcedures();
  }

  //get results set using CallableStatmenet
  protected void callDAProcedures() {
    Log.getLogWriter().info("call procedures, myTid is " + getMyTid());
    Connection dConn = null;
    if (hasDerbyServer){
      dConn = getDiscConnection();
    }
    Connection gConn = getHdfsQueryConnection();
    
    if (setCriticalHeap) resetCanceledFlag();
    if (setTx && isHATest) resetNodeFailureFlag();
    if (setTx && testEviction) resetEvictionConflictFlag();
    
    callProcedures(dConn, gConn);

    if (hasDerbyServer){
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  protected void callProcedures(Connection dConn, Connection gConn) {
    new DAProcedures().callProcedures(dConn, gConn);
    
    if (setTx) {
      commit(gConn);
      commit(dConn);
    } else {
      commit(dConn);
      commit(gConn);
    }
  }
  
  public static synchronized void HydraTask_createGFXDDBSG() {
    dapTest.createGFXDDBSG();
  }
  
  protected void createGFXDDBSG() {
    Properties info = getGemFireProperties();

    HydraVector vec = TestConfig.tab().vecAt(SQLPrms.serverGroups);
    int whichOne = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(
        SQLBB.dataStoreCount); //need to start from 0
    if (whichOne <= vec.size()) {
      whichOne--;
    } else {
      whichOne=vec.size()-1; //default to last element, if vms are more than configured
    }

    
    int myVMId = RemoteTestModule.getMyVmid();
    
    String sg = null;
    sg = (String) SQLDAPBB.getBB().getSharedMap().get("dataStoreSG" + myVMId);
    if (sg == null) {
      sg =(String)((HydraVector)vec.elementAt(whichOne)).elementAt(0); //get the item
      SQLDAPBB.getBB().getSharedMap().put("dataStoreSG" + myVMId, sg);
    }
    Log.getLogWriter().info("This data store for vmId " + myVMId + " is in " + sg);

    /* we do not need to set up the server group properties if
     * this server is in default/all server group
     */
    if (!sg.equals("default")) info.setProperty("server-groups", sg);

    info.setProperty("host-data", "true");
    Log.getLogWriter().info("Connecting with properties: " + info);
    
    startGFXDDB(info);
  }
  
  public static void HydraTask_getServerGroups() {
    String[] tableNames = SQLPrms.getTableNames();
    SharedMap sgMap = SQLBB.getBB().getSharedMap(); 
    for (String table: tableNames) {
      String tableSG = table.substring(table.indexOf('.') +1) + "SG";  //"customersSG" etc.
      String sg = (String) sgMap.get(tableSG);
      Log.getLogWriter().info(table + " server group is " + sg);
      String[] sgArray = sg.split(",");
      ArrayList<String> sgList = new ArrayList<String>(Arrays.asList(sgArray));
      sgMap.put(tableSG+dapList, sgList);
    }
  }
  

  @SuppressWarnings("unchecked")
  public static String getWrongServerGroups(String table) {
    SharedMap sgMap = SQLBB.getBB().getSharedMap(); 
    ArrayList<String> sgList = (ArrayList<String>)sgMap.get(table + "SG" + dapList);
    StringBuilder sb = new StringBuilder ();
    if (!sgList.contains("SG1")) sb.append("SG1,");
    if (!sgList.contains("SG2")) sb.append("SG2,");
    if (!sgList.contains("SG3")) sb.append("SG3,");
    if (!sgList.contains("SG4")) sb.append("SG4,");
    
    if (sb.length() == 0) return null;
    else sb.deleteCharAt(sb.lastIndexOf(","));
    //Log.getLogWriter().info("sb is " + sb.toString());
    return sb.toString();
  }
  
  @SuppressWarnings("unchecked")
  public static String getServerGroups(String table) {
    SharedMap sgMap = SQLBB.getBB().getSharedMap(); 
    ArrayList<String> sgList = (ArrayList<String>)sgMap.get(table + "SG" + dapList);
    StringBuilder sb = new StringBuilder ();
    if (sgList.contains("SG1")) sb.append("SG1,");
    if (sgList.contains("SG2")) sb.append("SG2,");
    if (sgList.contains("SG3")) sb.append("SG3,");
    if (sgList.contains("SG4")) sb.append("SG4,");
    
    if (sb.length() == 0) return null;
    else sb.deleteCharAt(sb.lastIndexOf(","));
    //Log.getLogWriter().info("sb is " + sb.toString());
    return sb.toString();
  }
  
  public static void HydraTask_createProcessorAlias() {
    dapTest.createProcessorAlias();
  }
  
  protected void createProcessorAlias() {
    Connection gConn = getGFEConnection();
    try {
      //String sql = "create alias trade.customProcessor for 'sql.sqlDAP.CustomProcessor'";

      String sql = "create alias customProcessor for 'sql.sqlDAP.CustomProcessor'";
      gConn.createStatement().execute(sql);
      Log.getLogWriter().info("executed the statement: " + sql );
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_doDDLOp() {
    dapTest.doDDLOp();
  }
}
