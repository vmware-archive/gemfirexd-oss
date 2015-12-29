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
package sql.generic.ddl.procedures;

import hydra.Log;
import hydra.RemoteTestModule;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import sql.SQLHelper;
import sql.generic.SQLGenericBB;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;
import sql.generic.ddl.procedures.ProcedureExceptionHandler.Action;

public abstract class ProcedureTest extends GeneralProcedure {

  private long lastDDLOpTime = 0;

  private int waitPeriod = 3; // 3 minutes

  private String lastProcOpTime = "lastProcOpTime";

  boolean modifyPartitionKey = false;

  abstract protected int getOutputResultSetCount();

  protected abstract HashMap<TableInfo, ColumnInfo> getModifiedColumns();

  public void setExecutor(Executor executor, int loc) {
    this.executor = executor;
    dConn = executor.getDConn();
    gConn = executor.getGConn();
    this.loc = loc;
    modifyPartitionKey = modifyPartitionKey();
    procName = getProcName();
    ddlStmt = getDdlStmt();
    derbyStmt = getDerbyDdlStmt();
    populateCalledFunctionInfoInBB();
  }

  private boolean modifiesData() {
    if (ddlStmt.toLowerCase().contains(" modifies "))
      return true;
    else
      return false;

  }

  // get counter to see if there is thread operation on the procedure
  // need to change generically
  protected int getCounter() {
    if (!dropProc)
      return 1;
    // no concurrent dropping procedure, so allow concurrent procedure calls
    int counter = (int)SQLGenericBB.getBB().getSharedCounters()
        .incrementAndRead(SQLGenericBB.procedureCounter[loc]);

    return counter;
  }

  public void doDDLOp() {

    if (!executeCreateProcedure())
      return;

    createDropProc();

    // reset flag/timer for next ddl op
    if (!RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTaskTypeString().equalsIgnoreCase("INITTASK")) {
      if (SQLOldTest.limitConcDDLOps) {
        long now = System.currentTimeMillis();
        SQLGenericBB.getBB().getSharedMap().put(lastProcOpTime, now);
        lastDDLOpTime = now;
        SQLGenericBB.getBB().getSharedCounters().zero(
            SQLGenericBB.perfLimitedProcDDL);
      }
    }

  }

  protected String getDerbyDdlStmt() {
    return getDdlStmt();
  }

  protected CallableStatement getDerbyCallableStatement(Connection conn)
      throws SQLException {
    return getCallableStatement(conn);
  }

  public void callProcedure() {
    boolean[] success = new boolean[1];
    Object[] inOut = new Object[2];
    if (modifyPartitionKey || (SQLOldTest.isHATest && modifiesData()))
      return;

    if (dConn != null) {
      exceptionHandler = new ProcedureExceptionHandler(Action.EXECUTE, false);
      if (modifiesData())
        callModifyProcedures(inOut);
      else
        callNonModifyProcedures(inOut);
    }
    else {
      exceptionHandler = new ProcedureExceptionHandler(Action.EXECUTE, true);
      ResultSet[] rs = callGFEProcedure(inOut, success);
      if (success[0])
        processRS(rs);
    }
  }

  // modify procedure does not return result sets
  protected void callModifyProcedures(Object[] inOut) {
    // to see if able to perfrom the task on calling the procedure, needed for
    // concurrent dml, ddl
    // as the nested connection on the server will release the lock for the
    // procedure in conn.close()
    if (dConn != null) {
      if (!doOp()) {
        Log.getLogWriter().info(
            "Other threads are performing op on the procedure " + procName
                + ", abort this operation");
        return; // other threads performing operation on the procedure, abort
      }
    }

    int count = 0;
    boolean[] success = new boolean[1];

    callDerbyProcedure(inOut, success);
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log
            .getLogWriter()
            .info(
                "could not call the derby procedure due to issues, abort this operation");
        zeroCounter();
        return; // not able to call derby procedure
      }

      exceptionHandler.getExList().clear();
      count++;
      callDerbyProcedure(inOut, success);
    }
    boolean[] successForHA = new boolean[1];

    Object[] gfxd = new Object[2];
    callGFEProcedure(gfxd, successForHA);
    while (!successForHA[0]) {
      callGFEProcedure(inOut, successForHA); // retry
      // TODO current only procedure modifies data as f1=f1+x type,
      // which does not run in HA test due to limitation
      // need to add modify procedure that modify data and could be retried
    }
    SQLHelper.handleMissedSQLException(exceptionHandler.getExList());

    verifyInOutParameters(inOut, gfxd);

    if (dConn != null) {
      zeroCounter();
    } // zero the counter after op is finished
  }

  // non-modify procedure returns result sets
  protected void callNonModifyProcedures(Object[] inOut) {
    // to see if able to perfrom the task on calling the procedure, needed for
    // concurrent dml, ddl
    if (dConn != null) {
      if (!doOp()) {
        Log.getLogWriter().info(
            "Other threads are performing op on the procedure " + procName
                + ", abort this operation");
        return; // other threads performing operation on the procedure, abort
      }
    }
    Log.getLogWriter().info(
        "Lock is acquired on " + procName + " by Thread :"
            + SqlUtilityHelper.tid());
    Object[] gfxdInOut = new Object[2];
    boolean[] success = new boolean[1];
    Log.getLogWriter().info("calling derby proc");
    ResultSet[] derbyRS = callDerbyProcedure(inOut, success);
    if (derbyRS == null && exceptionHandler.exList.size() == 0) {
      zeroCounter();
      return; // could not get rs from derby, therefore can not verify
    }
    Log.getLogWriter().info("calling gfxd proc");
    ResultSet[] gfeRS = callGFEProcedure(gfxdInOut, success);
    while (!success[0]) {
      gfeRS = callGFEProcedure(gfxdInOut, success); // retry
    }
    SQLHelper.handleMissedSQLException(exceptionHandler.exList);
    if (derbyRS != null)
      compareResultSets(derbyRS, gfeRS);
    else {
      zeroCounter();
      return;
    }

    verifyInOutParameters(inOut, gfxdInOut);

    if (dConn != null) {
      zeroCounter();
    } // zero the counter after op is finished
  }

  private boolean executeCreateProcedure() {

    if (!RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTaskTypeString().equalsIgnoreCase("INITTASK")) {
      if (!SQLOldTest.allowConcDDLDMLOps) {
        Log.getLogWriter().info(
            "This test does not run with concurrent ddl with dml ops, "
                + "abort the op");
        return false;
      }
      else {
        if (SQLOldTest.limitConcDDLOps) {
          // this is test default setting, only one concurrent procedure op in 3
          // minutes
          if (!perfDDLOp()) {
            Log.getLogWriter().info(
                "Does not meet criteria to perform concurrent ddl right now, "
                    + "abort the op");
            return false;
          }
          else {
            Long lastUpdateTime = (Long)SQLGenericBB.getBB().getSharedMap()
                .get(lastProcOpTime);
            if (lastUpdateTime != null) {
              long now = System.currentTimeMillis();
              lastDDLOpTime = lastUpdateTime;
              if (now - lastDDLOpTime < waitPeriod * 60 * 1000) {
                SQLGenericBB.getBB().getSharedCounters().zero(
                    SQLGenericBB.perfLimitedProcDDL);
                Log.getLogWriter().info(
                    "Does not meet criteria to perform concurrent ddl abort");
                return false;
              }
            }
          }

        }
        Log.getLogWriter().info(
            "performing conuccrent procedure op in main task");
      }
    }

    // to see if able to perfrom the task on creating/dropping the procedure
    if (dConn != null) {
      if (!doOp()) {
        Log.getLogWriter().info(
            "Other threads are performing op on the procedure " + procName
                + ", abort this operation");
        return false; // other threads performing operation on the procedure,
                      // abort
      }
    }

    return true;
  }

  private boolean perfDDLOp() {
    if (lastDDLOpTime == 0) {
      Long lastUpdateTime = (Long)SQLGenericBB.getBB().getSharedMap().get(
          lastProcOpTime);
      if (lastUpdateTime == null)
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
          lastProcOpTime);
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
        .incrementAndRead(SQLGenericBB.perfLimitedProcDDL);
    return perfLimitedConcDDL == 1;
  }

  public boolean modifyPartitionKey() {
    // needs to be implemeted.
    HashMap<TableInfo, ColumnInfo> columns = getModifiedColumns();
    if (columns == null)
      return false;
    else {
      for (Map.Entry<TableInfo, ColumnInfo> row : columns.entrySet()) {
        TableInfo table = row.getKey();
        ColumnInfo column = row.getValue();
        if (table.getPartitioningColumns().contains(column)) {
          return true;
        }
        else
          return false;
      }
    }

    return false;
  }

  @Override
  protected String getDerbyProcName() {
    // TODO Auto-generated method stub
    return procName;
  }

}
