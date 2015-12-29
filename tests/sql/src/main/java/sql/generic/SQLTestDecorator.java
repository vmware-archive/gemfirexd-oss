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

/**
 * 
 * @author Rahul Diyewar
 */

public abstract class SQLTestDecorator implements SQLTestable {
  protected SQLTestable sqlTest;

  public SQLTestDecorator(SQLTestable sqlTest) {
    this.sqlTest = sqlTest;
    String currentName = this.getClass().getName();
    String parentName = sqlTest.getClass().getName();
    Log.getLogWriter().info("Decorated " + currentName + " on " + parentName);
  }

  @Override
  public void initialize() {
    sqlTest.initialize();
  }

  @Override
  public MemberType getMemberType() {
    return sqlTest.getMemberType();
  }

  @Override
  public void beforeFabricServer() {
    sqlTest.beforeFabricServer();
  }

  @Override
  public void startFabricServer() {
    sqlTest.startFabricServer();
  }

  @Override
  public void afterFabricServer() {
    sqlTest.afterFabricServer();
  }

  @Override
  public void createGfxdLocator() {
    sqlTest.createGfxdLocator();
  }

  @Override
  public void startGfxdLocator() {
    sqlTest.startGfxdLocator();
  }

  @Override
  public void createDerbyDB() {
    sqlTest.createDerbyDB();
  }

  @Override
  public void createDerbySchemas() {
    sqlTest.createDerbySchemas();
  }

  @Override
  public void createDerbyTables() {
    sqlTest.createDerbyTables();
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
    sqlTest.createView();
  }
  
  @Override
  public void populateViewTables() {
    sqlTest.populateViewTables();    
  }
  
  @Override
  public void queryViews() {
    sqlTest.queryViews();    
  }
  
  @Override
  public void verifyViews() {
    sqlTest.verifyViews();    
  }  
  
  public void createIndex(){
    sqlTest.createIndex();
  }
  
  public void createFunctions(){
    sqlTest.createFunctions();
  }
  
  public void createProcedures(){
    sqlTest.createProcedures();
  }
  
  public void doDDLOp(){
    sqlTest.doDDLOp();
  }
  
  public void callProcedures(){
    sqlTest.callProcedures();
  }
}
