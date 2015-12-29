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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

import sql.SQLHelper;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.GenericExceptionHandler;

public class IndexExceptionHandler implements GenericExceptionHandler {

  DDLAction action;

  IndexOperation index;

  public IndexExceptionHandler(DDLAction action, IndexOperation index) {
    this.action = action;
    this.index = index;

  }

  @Override
  public void afterDerbyExecution() {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterGemxdExecution() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean handleDerbyException(Connection derby, SQLException se) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean handleGfxdException(SQLException se) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean handleGfxdExceptionOnly(SQLException se) {
    if (action == DDLAction.CREATE) {
      return handleGfxdExceptionCreateOnly(se);
    }
    if (action == DDLAction.DROP) {
      return handleGfxdExceptionDropOnly(se);
    }

    SQLHelper.handleSQLException(se);
    return true;
  }

  private boolean handleGfxdExceptionCreateOnly(SQLException se) {
    SQLHelper.printSQLException(se);
    if (se.getSQLState().equalsIgnoreCase("23505")) {
      Log.getLogWriter().info(
          "Got the expected exception creating unique index, continuing test");

    }
    else if (se.getSQLState().equalsIgnoreCase("X0Z15")) {
      Log.getLogWriter().info(
      "Got the expected exception creating global hash index, continuing test");
    }
    else if (se.getSQLState().equalsIgnoreCase("XSAS3")) {
      Log.getLogWriter().info(
          "Got the expected exception creating global hash index with sort"
              + ", continuing test");
    }
    else if (se.getSQLState().equalsIgnoreCase("X0Z08")
        && SQLOldTest.isOfflineTest) {
      Log.getLogWriter().info(
          "Got the expected exception during creating index when no nodes are available"
              + ", continuing test");
    }
    else if (se.getSQLState().equalsIgnoreCase("X0X67")) {
      Log.getLogWriter().info(
          "Got the expected exception during creating index on BLOB field"
              + ", continuing test");
    }
    else if (se.getSQLState().equalsIgnoreCase("42Y62")) {
      Log.getLogWriter().info(
          "Got the expected exception not able to create index on a view"
              + ", continuing test");
    }
    else
      SQLHelper.handleSQLException(se);
    return true;

  }

  private boolean handleGfxdExceptionDropOnly(SQLException se) {

    if (se.getSQLState().equalsIgnoreCase("42X65") && !index.getIndexExist()) {
      Log.getLogWriter().info(
          "Got the expected index does not exist exception "
              + ", continuing test");
    }
    else if (se.getSQLState().equalsIgnoreCase("X0Z08")
        && SQLOldTest.isOfflineTest) {
      Log.getLogWriter().info(
          "Got the expected exception during dropping index when no nodes are available"
              + ", continuing test");
    }
    else {
      SQLHelper.handleSQLException(se);
    }
    return true;
  }

  public boolean handleGfxdWarningsOnly(SQLWarning warnings) {
    // index already exist
    if (warnings.getSQLState().equalsIgnoreCase("01504")) {
      return false;
    }
    else
      return true;

  }

}
