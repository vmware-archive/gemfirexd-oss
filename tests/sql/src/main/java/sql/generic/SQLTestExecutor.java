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
package sql.generic;

import hydra.Log;
import hydra.gemfirexd.FabricServerHelper;

import java.util.Properties;

import com.pivotal.gemfirexd.FabricServer;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;

import sql.generic.dbsync.SQLDBSyncTest;

/**
 *  
 * @author Rahul Diyewar
 */


public class SQLTestExecutor {
  protected static SQLTestable sqlTest;

  public static synchronized void HydraTask_initialize() {
    if (sqlTest == null) {
      sqlTest = new SQLTestFactory().getSQLTestObject();
    } 
    sqlTest.initialize();
  }

  public static synchronized void HydraTask_startFabricServer() {
    FabricServer fs = FabricServiceManager.getFabricServerInstance();
    FabricService.State status = fs.status();
    if (!status.equals(FabricService.State.RUNNING)) {
      sqlTest.beforeFabricServer();
      sqlTest.startFabricServer();
      sqlTest.afterFabricServer();
    }
  }

  public static synchronized void HydraTask_createGfxdLocatorTask() {
    sqlTest.createGfxdLocator();
  }

  public static synchronized void HydraTask_startGfxdLocatorTask() {
    sqlTest.startGfxdLocator();
  }

  public static synchronized void HydraTask_createDiscDB() {
    sqlTest.createDerbyDB();
  }

  public static synchronized void HydraTask_createDiscSchemas() {
    sqlTest.createDerbySchemas();
  }

  public static synchronized void HydraTask_createDiscTables() {
    sqlTest.createDerbyTables();
  }

  public static synchronized void HydraTask_createGFESchemas() {
    sqlTest.createGFESchemas();
  }

  public static synchronized void HydraTask_createDiskStores() {
    sqlTest.createDiskStores();
  }

  public static synchronized void HydraTask_createGFETables() {
    sqlTest.createGFETables();
  }

  public static synchronized void HydraTask_createDBSynchronizer(){
    ((SQLDBSyncTest)sqlTest).createDBSynchronizer();
  }
  
  public static synchronized void HydraTask_startDBSynchronizer(){
    ((SQLDBSyncTest)sqlTest).startDBSynchronizer();
  }

  public static synchronized void HydraTask_putLastKeyDBSynchronizer(){
    ((SQLDBSyncTest)sqlTest).putLastKeyDBSynchronizer();
  }
  
  public static synchronized void HydraTask_populateTables() {
    sqlTest.populateTables();
  }

  public static void HydraTask_doDMLOp() {
    sqlTest.doDMLOp();
  }

  public static void HydraTask_cycleStoreVms(){
    sqlTest.cycleStoreVms();
  }
  
  public static synchronized void HydraTask_verifyResultSets() {
    sqlTest.verifyResultSets();
  }
  
  public static synchronized void HydraTask_shutDownAllFabricServers(){
    sqlTest.shutDownAllFabricServers();
  }
  
  public static synchronized void HydraTask_createView(){
    sqlTest.createView();
  }
  
  public static synchronized void HydraTask_populateViewTables(){
    sqlTest.populateViewTables();
  }
  
  public static synchronized void HydraTask_queryViews(){
    sqlTest.queryViews();    
  }
  
  public static synchronized void HydraTask_verifyViews(){
    sqlTest.verifyViews();  
  }
  
  public static synchronized void HydraTask_createIndex(){
    sqlTest.createIndex();  
  }
  
  public static synchronized void HydraTask_createProcedures(){
    sqlTest.createProcedures();
  }
  
  public static synchronized void HydraTask_createFunctions(){
    sqlTest.createFunctions();
  }
  public static synchronized void HydraTask_doDDLOp(){
    sqlTest.doDDLOp();  
  }
  
  public static synchronized void HydraTask_callProcedures(){
    sqlTest.callProcedures();
  }
}
