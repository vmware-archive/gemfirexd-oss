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
package sql.generic.dbsync;

import java.sql.Connection;

import hydra.ClientVmInfo;
import hydra.Log;
import hydra.RemoteTestModule;
import sql.SQLBB;
import sql.SQLTest;
import sql.generic.MemberType;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.SQLTestDecorator;
import sql.generic.SQLTestable;

public class SQLDBSyncTest extends SQLTestDecorator {

  public SQLDBSyncTest(SQLTestable sqlTest) {
    super(sqlTest);
  }

  @Override
  public void beforeFabricServer() {
    super.beforeFabricServer();
    if (sqlTest.getMemberType().equals(MemberType.DBsyncDatastore)) {
      setClientVmInfoForDBSynchronizerNode();
      String sg = ((SQLGenericTest) sqlTest).getNodeServerGroup();
      sg = (sg == null) ? SQLTest.sgDBSync : "," + SQLTest.sgDBSync;
      ((SQLGenericTest) sqlTest).setNodeServerGroup(sg);
      Log.getLogWriter().info("Configured server-group = " + sg);
    }
  }

  protected void setClientVmInfoForDBSynchronizerNode() {
    ClientVmInfo target = new ClientVmInfo(RemoteTestModule.getMyVmid());
    int num = (int) SQLBB.getBB().getSharedCounters()
        .incrementAndRead(SQLBB.asynchDBTargetVm);
    if (num == 1) {
      SQLBB.getBB().getSharedMap().put("asyncDBTarget1", target);
      Log.getLogWriter().info(
          "asyncDBTarget1: client vmID is " + target.getVmid());
    } else {
      ClientVmInfo target1 = (ClientVmInfo) SQLBB.getBB().getSharedMap()
          .get("asyncDBTarget1");
      if (target1 != null && target1.getVmid() == RemoteTestModule.getMyVmid()) {
        // restarted vm will have the same logic vmId.
        SQLBB.getBB().getSharedMap().put("asyncDBTarget1", target);
        Log.getLogWriter().info(
            "asyncDBTarget1: client vmID is " + target.getVmid());
      } else {
        SQLBB.getBB().getSharedMap().put("asyncDBTarget2", target);
        Log.getLogWriter().info(
            "asyncDBTarget2: client vmID is " + target.getVmid());
      }
    }
  }

  public void createDBSynchronizer() {
    ((SQLGenericTest) sqlTest).getSQLOldTest().createDBSynchronizer();
  }

  public void startDBSynchronizer() {
    ((SQLGenericTest) sqlTest).getSQLOldTest().startDBSynchronizer();
  }

  public void putLastKeyDBSynchronizer() {
    ((SQLGenericTest) sqlTest).getSQLOldTest().putLastKeyDBSynchronizer();
  }

  @Override
  public void verifyResultSets() {
    ((SQLGenericTest) sqlTest).getSQLOldTest().verifyResultSetsDBSynchronizer();
  }
  
  @Override
  public void populateTables() {
    Connection gConn = ((SQLGenericTest) sqlTest).getSQLOldTest().getGFEConnection();
    ((SQLGenericTest) sqlTest).getSQLOldTest().populateTables(null, gConn);    
    ((SQLGenericTest) sqlTest).getSQLOldTest().closeGFEConnection(gConn);
  }
  
  @Override
  public void doDMLOp() {
    SQLOldTest test = ((SQLGenericTest) sqlTest).getSQLOldTest();
    if (SQLTest.networkPartitionDetectionEnabled) {
      test.doDMLOp_HandlePartition();
    } else {
      Connection gConn = test.getHdfsQueryConnection();      
      test.doDMLOp(null, gConn);       
      test.closeGFEConnection(gConn);
    }
    Log.getLogWriter().info("done dmlOp");
  }
}
