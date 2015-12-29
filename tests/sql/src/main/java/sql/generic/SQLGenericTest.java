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
import hydra.RemoteTestModule;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;

import java.sql.Connection;
import java.util.Properties;

import sql.SQLBB;
import sql.SQLHelper;
import sql.view.ViewPrms;
import util.PRObserver;

/**
 * 
 * @author Rahul Diyewar
 */

public class SQLGenericTest implements SQLTestable {
  SQLOldTest sqlTest;
  DerbyTest derbyTest;
  MemberType memberType;
  String nodeServerGroup;
  Boolean isVMInitialized = new Boolean(false);

  public SQLGenericTest() {
    Log.getLogWriter().info("Creating SQLGenericTest");
    sqlTest = new SQLOldTest();
  }

  public SQLOldTest getSQLOldTest(){
    return sqlTest;
  }
  
  public String getNodeServerGroup() {
    return nodeServerGroup;
  }

  public void setNodeServerGroup(String nodeServerGroup) {
    this.nodeServerGroup = nodeServerGroup;
  }

  @Override
  public void initialize() {
    doTestLevelConfiguration();
    doVMLevelConfiguration();
    doThreadLevelConfiguration();
  }

  // should be executed once in a test execution
  // do work like bb setup
  public void doTestLevelConfiguration() {
    if (SQLBB.getBB().getSharedCounters()
        .incrementAndRead(SQLBB.testLevelConfiguration) == 1) {
      Log.getLogWriter().info("Configuring global test level config.");
      if (SQLGenericPrms.hasServerGroups())
        sqlTest.initForServerGroup();
    }
  }

  public synchronized void doVMLevelConfiguration() {
    if (!isVMInitialized.booleanValue()) {
      Log.getLogWriter().info("Configuring vm level config.");
      sqlTest.initialize();
      memberType = MemberType.getMemberType();
      Log.getLogWriter().info("Member type is " + memberType);

      if (sqlTest.hasDerbyServer) {
        derbyTest = new DerbyTest(this);
        Log.getLogWriter().info("Created DerbyTest ");
      }

      if (SQLGenericPrms.isHA()) {
        Log.getLogWriter().info(
            "HA is enabled in the tests, configuring PRObserver");
        PRObserver.installObserverHook();
        PRObserver.initialize(RemoteTestModule.getMyVmid());
      }
    }
  }

  public void doThreadLevelConfiguration() {
    // to thread level configuration such as ThreadLocals
    Log.getLogWriter().info("Configuring thread level config.");
  }

  @Override
  public MemberType getMemberType() {
    return memberType;
  }

  @Override
  public void beforeFabricServer() {
    if (SQLGenericPrms.hasServerGroups()) {
      nodeServerGroup = sqlTest.getSGForNode();
    }
  }

  @Override
  public void startFabricServer() {
    Log.getLogWriter().info("Starting the fabric server");
    Properties p = FabricServerHelper.getBootProperties();
    if (nodeServerGroup != null && !nodeServerGroup.equals("default"))
      p.setProperty(sqlTest.SERVERGROUP, nodeServerGroup);

    sqlTest.startFabricServer(p);
  }

  @Override
  public void afterFabricServer() {
  }

  @Override
  public void createGfxdLocator() {
    sqlTest.isLocator = true;
    FabricServerHelper.createLocator();
  }

  @Override
  public void startGfxdLocator() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    FabricServerHelper.startLocator(networkServerConfig);
  }

  @Override
  public void createDerbyDB() {
    if (sqlTest.hasDerbyServer) {
      derbyTest.createDiscDB();
    }
  }

  @Override
  public void createDerbySchemas() {
    if (sqlTest.hasDerbyServer) {
      derbyTest.createDiscSchemas();
    }
  }

  @Override
  public void createDerbyTables() {
    if (sqlTest.hasDerbyServer) {
      derbyTest.createDiscTables();
    }
  }

  @Override
  public void createDiskStores() {
    sqlTest.createDiskStores();
  }

  @Override
  public void createGFESchemas() {
    sqlTest.createGFESchemas();
  }

  @Override
  public void createGFETables() {
    sqlTest.createGFETables();
  }

  @Override
  public void populateTables() {
    sqlTest.populateTables();
  }

  @Override
  public void doDMLOp() {
    sqlTest.doDMLOp();
  }

  @Override
  public void cycleStoreVms() {
    sqlTest.cycleStoreVms();
  }

  @Override
  public void verifyResultSets() {
    sqlTest.verifyResultSets();
  }

  @Override
  public void shutDownAllFabricServers() {
    sqlTest.shutDownAllFabricServers();
  }

  @Override
  public void createView() {
    if (SQLGenericPrms.hasView())
      sqlTest.createViews();
  }

  @Override
  public void populateViewTables() {
    if (SQLGenericPrms.hasView())
      sqlTest.populateViewTables();
  }

  @Override
  public void queryViews() {
    if (SQLGenericPrms.hasView())
      sqlTest.queryViews();
  }

  @Override
  public void verifyViews() {
    if (SQLGenericPrms.hasView())
      sqlTest.verifyViews();
  }
  
  public void createIndex(){
    sqlTest.createIndex();
  }
  
  public void callProcedures(){
    sqlTest.callProcedures();
  }
  
  public void createProcedures(){
    sqlTest.callProcedures();
  }
  
  public void createFunctions(){
    sqlTest.createFunctions();
  }
  public void doDDLOp(){
    sqlTest.doDDLOp();
  }
}
