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

/**
 * 
 * @author Rahul Diyewar
 */

public interface SQLTestable {

  void initialize();

  MemberType getMemberType();
  
  void beforeFabricServer();

  void startFabricServer();

  void afterFabricServer();

  void createGfxdLocator();

  void startGfxdLocator();

  void createDerbyDB();

  void createDerbySchemas();

  void createDerbyTables();

  void createGFESchemas();

  void createDiskStores();

  void createGFETables();

  void populateTables();

  void doDMLOp();

  void cycleStoreVms();

  void verifyResultSets();
  
  void shutDownAllFabricServers();

  void createView();

  void populateViewTables();

  void queryViews();

  void verifyViews();

  void createIndex();

  void createProcedures();

  void createFunctions();

  void doDDLOp();

  void callProcedures();
  
}
