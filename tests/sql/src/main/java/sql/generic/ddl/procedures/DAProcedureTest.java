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

import java.sql.ResultSet;

import sql.SQLHelper;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.ddl.procedures.ProcedureExceptionHandler.Action;
import sql.sqlutil.ResultSetHelper;

public abstract class DAProcedureTest extends GeneralProcedure {

  protected int maxNumOfTries = 2;

  public String gfxdProcName;

  // if the DAP is executed on serverGroups
  abstract protected boolean procCalledWithSG();

  public void doDDLOp() {
    // to see if able to perfrom the task on creating/dropping the procedure
    if (dConn != null && SQLOldTest.allowConcDDLDMLOps) {
      if (!doOp()) {
        Log.getLogWriter().info(
            "Other threads are performing op on the procedure " + gfxdProcName
                + ", abort this operation");
        return; // other threads performing operation on the procedure, abort
      }

    }

    createDropProc();
  }

  public void callProcedure() {
    if (!testServerGroupsInheritence && procCalledWithSG()) {
      return;
    }
    if (dConn != null) {
      if (SQLOldTest.allowConcDDLDMLOps) {// only one thread is allowed to
                                              // perform call procedure
        if (!doOp()) {
          Log.getLogWriter().info(
              "Other threads are performing op on the procedure "
                  + getProcName() + ", abort this operation");
          return; // other threads performing operation on the procedure, abort
        }
      }
      exceptionHandler = new ProcedureExceptionHandler(Action.EXECUTE, false);
      callProcedureOnBothDB();

      if (SQLOldTest.allowConcDDLDMLOps) {
        zeroCounter();
      }

    }
    else {
      exceptionHandler = new ProcedureExceptionHandler(Action.EXECUTE, true);
      callProcedureGfxdOnly();

    }

  }

  protected void callProcedureOnBothDB() {
    Object[] inOut = new Object[5];
    boolean[] success = new boolean[1];

    ResultSet[] derbyRS = callDerbyProcedure(inOut, success);
    if (derbyRS == null && exceptionHandler.getExList().size() == 0) {
      return; // could not get rs from derby, therefore can not verify
    }

    boolean[] successForHA = new boolean[1];
    Object[] gfxd = new Object[5];
    ResultSet[] gfxdRS = callGFEProcedure(gfxd, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGFEProcedure(inOut, successForHA);
    }
    SQLHelper.handleMissedSQLException(exceptionHandler.getExList());
    if (gfxdRS == null && SQLOldTest.allowConcDDLDMLOps)
      return;
    compareResultSets(derbyRS, gfxdRS);

    verifyInOutParameters(inOut, gfxd);
  }

  protected void callProcedureGfxdOnly() {

    boolean[] successForHA = new boolean[1];
    Object[] gfxd = new Object[5];
    ResultSet[] gfxdRS = callGFEProcedure(gfxd, successForHA);

    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callProcedureGfxdOnly(); // retry
    }

    if (gfxdRS == null && SQLOldTest.allowConcDDLDMLOps)
      return;
    ResultSetHelper.asList(gfxdRS[0], false);

  }
}
