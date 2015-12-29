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
package sql.generic.ddl.Functions;

import hydra.Log;
import hydra.MasterController;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import sql.SQLHelper;
import sql.SQLTest;
import sql.generic.SQLGenericBB;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.Executor;
import sql.generic.ddl.Executor.ExceptionAt;
import sql.generic.ddl.create.DDLStmtIF;
import sql.generic.ddl.procedures.ProcedureExceptionHandler;
import sql.generic.ddl.procedures.ProcedureExceptionHandler.Action;

abstract public class FunctionTest implements DDLStmtIF {
  protected boolean dropFunc = SQLOldTest.dropFunc;

  Executor executor;

  Connection dConn, gConn;

  ProcedureExceptionHandler exceptionHandler;

  final int maxNumOfTries = 1;

  ArrayList<String> procNames, lockAcquiredOnProc = new ArrayList<String>();

  String procWithoutLock;

  abstract protected String getFunctionName();

  abstract protected String getFunctionDdl();

  public void setExecutor(Executor executor) {
    this.executor = executor;
    this.dConn = executor.getDConn();
    this.gConn = executor.getGConn();
  }

  public void createDDL() {
    Log.getLogWriter().info(
        "performing create Fuunction  Op, myTid is " + SqlUtilityHelper.tid());
    HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
    map.put(getFunctionName().toUpperCase(), new ArrayList<String>());
    SQLGenericBB.getBB().getSharedMap().put(SQLOldTest.FuncProcMap, map);
    executor.executeOnBothDb(getFunctionDdl(), getFunctionDdl(),
        new ProcedureExceptionHandler(Action.CREATE, false));
  }

  // functions are used in procedures/other operations
  public void doDDLOp() {
    int chance = 100;
    if (!dropFunc || SQLTest.random.nextInt(chance) != 1) {
      Log.getLogWriter().info("will not perform drop function");
      return;
    }

    procNames = getDependentProcList();

    if (dConn != null) {
      if (doOps()) {
        Log.getLogWriter().info(
            "Other threads are performing op on the Procedure "
                + procWithoutLock + ", abort this operation");
        return; // other threads performing operation on the procedure, abort
      }
    }

    // some operations will drop the function, some will create; some will do
    // drop and create
    int choice = 3; // 3 choices, 0 do drop only, 1 both drop and create, 2
    // create only
    int op = SQLOldTest.random.nextInt(choice);

    exceptionHandler = new ProcedureExceptionHandler(Action.CREATE, false);

    if (op == 0 || op == 1) {
      String dropStatement = "drop function " + getFunctionName();
      if (dConn != null) {
        ExceptionAt exception = executor.executeOnBothDb(dropStatement,
            dropStatement, exceptionHandler);
        if (exception == ExceptionAt.DERBY) {
          zeroCounter();
          return;
        }
        exception = executor.executeOnBothDb(getFunctionDdl(),
            getFunctionDdl(), exceptionHandler);
        if (exception == ExceptionAt.DERBY) {
          zeroCounter();
          executor.rollbackDerby();
          return;
        }

        SQLHelper.handleMissedSQLException(exceptionHandler.getExList());
      }
      else {
        executor.executeOnBothDb(dropStatement, dropStatement,
            new ProcedureExceptionHandler(Action.CREATE, true));
      }
    }

    if (op == 2) {
      if (dConn != null) {
        ExceptionAt exception = executor.executeOnBothDb(getFunctionDdl(),
            getFunctionDdl(), exceptionHandler);
        if (exception == ExceptionAt.DERBY) {
          zeroCounter();
          return;
        }
      }
      else {
        executor.executeOnBothDb(getFunctionDdl(), getFunctionDdl(),
            new ProcedureExceptionHandler(Action.CREATE, true));
      }
    }

    if (dConn != null) {
      zeroCounter();
    } // zero the counter after op is finished
  }

  protected void zeroCounter() {
    for (String procName : lockAcquiredOnProc) {
      int loc = (Integer)SQLGenericBB.getBB().getSharedMap().get(
          procName.toUpperCase());
      Log.getLogWriter().info(
          "zeros counter SQLGenericBB.procedureCounter[" + loc
              + "] for Procedure " + procName);
      SQLGenericBB.getBB().getSharedCounters().zero(
          SQLGenericBB.procedureCounter[loc]);
    }
  }

  // only one thread can do operation on the procedure, to see if any other
  // thread is
  // operating on the procedure
  protected boolean doOp(String procName) {
    int doOp = getCounter(procName);
    int count = 0;
    while (doOp != 1) {
      if (count > maxNumOfTries)
        return false; // try to operation, if not do not operate
      count++;
      MasterController.sleepForMs(100 * SQLOldTest.random.nextInt(30));
      // sleep from 0 -2900 ms
      doOp = getCounter(procName);
      Log.getLogWriter().info("Trying again to see if i can get hold on proc");
    }
    return true;
  }

  protected boolean doOps() {
    if (procNames == null)
      return true;

    for (String procName : procNames) {
      boolean success = doOp(procName);
      if (!success) {
        procWithoutLock = procName;
        zeroCounter();
        return false;
      }
    }
    return true;
  }

  // get counter to see if there is thread operation on the procedure
  // need to change generically
  protected int getCounter(String procName) {
    // no concurrent dropping procedure, so allow concurrent procedure calls
    int loc = (Integer)SQLGenericBB.getBB().getSharedMap().get(
        procName.toUpperCase());
    int counter = (int)SQLGenericBB.getBB().getSharedCounters()
        .incrementAndRead(SQLGenericBB.procedureCounter[loc]);
    return counter;
  }

  private ArrayList<String> getDependentProcList() {
    Map<String, ArrayList<String>> functionProcMap = (Map<String, ArrayList<String>>)SQLGenericBB
        .getBB().getSharedMap().get(SQLOldTest.FuncProcMap);
    ArrayList<String> listOfProc = functionProcMap.get(getFunctionName()
        .toUpperCase());
    if (listOfProc != null && listOfProc.size() > 0) {
      return listOfProc;
    }
    return null;
  }

}
