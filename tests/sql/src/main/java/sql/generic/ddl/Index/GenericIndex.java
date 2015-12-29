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
package sql.generic.ddl.Index;

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sql.SQLTest;
import sql.generic.SQLGenericBB;
import sql.generic.SQLGenericPrms;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.create.DDLStmtIF;
import util.TestException;

public class GenericIndex implements DDLStmtIF {

  Executor executor;

  Connection gConn, dConn;

  private String lastIndexOpTime = "lastIndexOpTime";

  static long lastDDLOpTime = 0;

  private int waitPeriod;

  boolean dropIndex = TestConfig.tab().booleanAt(SQLGenericPrms.dropIndex,
      false);

  List<String> ddlStmt;

  static List<String> indexNames = new ArrayList<String>();

  Map<String, String> stmtPlaceHolders;

  public GenericIndex(List<String> ddlStmt) {
    waitPeriod = 3;
    this.ddlStmt = ddlStmt;
    stmtPlaceHolders = new HashMap<String, String>();
  }

  public void setExecutor(Executor executor) {
    this.executor = executor;
    this.gConn = executor.getGConn();
    this.dConn = executor.getDConn();
  }

  // create index does not modify sql data
  public void createDDL() {
    doDDLOp();
  }

  public void doDDLOp() {

    if (!RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTaskTypeString().equalsIgnoreCase("INITTASK")) {
      if (!SQLOldTest.allowConcDDLDMLOps) {
        Log.getLogWriter().info(
            "This test does not run with concurrent ddl with dml ops, "
                + "abort the op");
        return;
      }
      else {
        if (SQLTest.limitConcDDLOps) {
          // this is test default setting, only one concurrent index op in 3
          // minutes
          if (!doOps()) {
            Log.getLogWriter().info(
                "Does not meet criteria to perform concurrent ddl right now, "
                    + "abort the op");
            return;
          }
          else {
            Long lastUpdateTime = (Long)SQLGenericBB.getBB().getSharedMap()
                .get(lastIndexOpTime);
            if (lastUpdateTime != null) {
              long now = System.currentTimeMillis();
              lastDDLOpTime = lastUpdateTime;
              if (now - lastDDLOpTime < waitPeriod * 60 * 1000) {
                SQLGenericBB.getBB().getSharedCounters().zero(
                    SQLGenericBB.perfLimitedIndexDDL);
                Log.getLogWriter().info(
                    "Does not meet criteria to perform concurrent ddl abort");
                return;
              }
            }
          }
        }
        else {
          // without limiting concurrent DDL ops
          // this needs to be specifically set to run from now on
        }
        Log.getLogWriter().info("performing concurrent index op in main task");
      }

    }

    performIndexOperation();

    // reset flag/timer for next ddl op
    if (!RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTaskTypeString().equalsIgnoreCase("INITTASK")) {
      if (SQLOldTest.limitConcDDLOps) {
        long now = System.currentTimeMillis();
        SQLGenericBB.getBB().getSharedMap().put(lastIndexOpTime, now);
        Log.getLogWriter().info(lastIndexOpTime + " is set to " + now / 60000);
        lastDDLOpTime = now;
        Log.getLogWriter().info("setting perfLimitedIndexDDL to zero");
        SQLGenericBB.getBB().getSharedCounters().zero(
            SQLGenericBB.perfLimitedIndexDDL);
        Log.getLogWriter().info(
            "read  perfLimitedIndexDDL :"
                + SQLGenericBB.getBB().getSharedCounters().read(
                    SQLGenericBB.perfLimitedIndexDDL));
      }
    }
  }

  private boolean doOps() {
    if (lastDDLOpTime == 0) {
      Long lastUpdateTime = (Long)SQLGenericBB.getBB().getSharedMap().get(
          lastIndexOpTime);
      if (lastUpdateTime == null || lastUpdateTime == 0)
        return checkBBForDDLOp();
      else
        lastDDLOpTime = lastUpdateTime;
    }

    long now = System.currentTimeMillis();

    if (now - lastDDLOpTime < waitPeriod * 60 * 1000) {
      return false;
    }
    else {
      lastDDLOpTime = (Long)SQLGenericBB.getBB().getSharedMap().get(
          lastIndexOpTime);
      // avoid synchronization in the method, so need to check one more time
      if (now - lastDDLOpTime < waitPeriod * 60 * 1000) {
        return false;
      }
      else
        return checkBBForDDLOp();
    }
  }

  private boolean checkBBForDDLOp() {
    int perfLimitedConcDDL = (int)SQLGenericBB.getBB().getSharedCounters()
        .incrementAndRead(SQLGenericBB.perfLimitedIndexDDL);
    return perfLimitedConcDDL == 1;
  }

  public void performIndexOperation() {
    for (String stmt : ddlStmt) {
      Log.getLogWriter().info("Executing ..." + stmt);
      IndexOperation operation = new IndexOperation(stmt, executor,
          stmtPlaceHolders, getAction(stmt));
      operation.executeStatement();
    }
  }

  private DDLAction getAction(String stmt) {
    if (stmt.toUpperCase().startsWith(DDLAction.CREATE.name()))
      return DDLAction.CREATE;
    if (stmt.toUpperCase().startsWith(DDLAction.DROP.name()))
      return DDLAction.DROP;
    if (stmt.toUpperCase().startsWith(DDLAction.RENAME.name()))
      return DDLAction.RENAME;

    throw new TestException("Invalid stmt " + stmt);
  }

}
