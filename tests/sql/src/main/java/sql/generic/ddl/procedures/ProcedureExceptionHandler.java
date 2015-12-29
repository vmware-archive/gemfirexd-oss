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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;

import sql.SQLHelper;
import sql.SQLTest;
import sql.generic.ddl.GenericExceptionHandler;

public class ProcedureExceptionHandler implements GenericExceptionHandler {
  ArrayList<SQLException> exList = new ArrayList<SQLException>();

  public ArrayList<SQLException> getExList() {
    return exList;
  }

  public enum Action {
    CREATE, EXECUTE
  };

  Action action;

  boolean concurrent;

  public ProcedureExceptionHandler(Action action, boolean concurrent) {
    this.action = action;
    this.concurrent = concurrent;
  }

  public boolean handleDerbyException(Connection derby, SQLException se) {
    if (!SQLHelper.checkDerbyException(derby, se)) {
      return false;
    }
    else {
      SQLHelper.handleDerbySQLException(se, exList);
      return true;
    }
  }

  public void afterDerbyExecution() {

  }

  public void afterGemxdExecution() {

  }

  public boolean handleGfxdException(SQLException se) {
    SQLHelper.handleGFGFXDException(se, exList);
    return true;
  }

  public boolean handleGfxdExceptionOnly(SQLException se) {
    if (action == Action.CREATE) {
      return handleGfxdExceptionCreateOnly(se);
    }
    else {
      if (concurrent)
        return handleGfxdExceptionExecuteConcurrentOnly(se);
      else
        return handleGfxdExceptionExecuteOnly(se);
    }
  }

  public boolean handleGfxdExceptionCreateOnly(SQLException se) {
    if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94"))
      Log.getLogWriter().info(
          "expected procedrue does not exist exception, continuing test");
    else if (se.getSQLState().equals("X0Y68"))
      Log.getLogWriter().info(
          "expected procedrue already exist exception, continuing test");
    else if (se.getSQLState().equals("42507") && SQLTest.testSecurity)
      Log.getLogWriter().info(
          "expected authorization exception, continuing test");
    else
      SQLHelper.handleSQLException(se); // handle the exception
    return true;
  }

  private boolean handleGfxdExceptionExecuteConcurrentOnly(SQLException se) {
    if (se.getSQLState().equals("42Y03")) {
      // procedure does not exists in concurrent testing
      Log.getLogWriter().info("got expected exception, continuing test");
      return false; // whether process the resultSet or not
    }
    else if (se.getSQLState().equals("38000")) {
      // function used in procedure does not exists in concurrent testing
      Log.getLogWriter().info("got expected exception, continuing test");
      return false; // whether process the resultSet or not
    }
    else if (se.getSQLState().equals("X0Z01") && SQLTest.isHATest) {
      // node went down during procedure call
      Log.getLogWriter().info("got expected exception, continuing test");
      return false; // whether process the resultSet or not
    }
    else if (se.getSQLState().equals("42504") && SQLTest.testSecurity) {
      Log.getLogWriter().info(
          "expected authorization exception, continuing test");
      return false;
    }
    else if (se.getSQLState().equals("X0Z02") && SQLTest.hasTx) {
      Log.getLogWriter().info("expected conflict exception, continuing test");
      return false;
    }
    else
      SQLHelper.handleSQLException(se);
    return true;
  }

  private boolean handleGfxdExceptionExecuteOnly(SQLException se) {
    if (se.getSQLState().equals("X0Z01")) {
      return false;
    }
    else {
      SQLHelper.handleGFGFXDException(se, exList);
      return true;
    }
  }

  public boolean handleGfxdWarningsOnly(SQLWarning warnings) {
    return true;
  }
}
