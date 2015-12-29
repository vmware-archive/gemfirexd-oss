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
package sql.generic.ddl;

import hydra.Log;
import hydra.MasterController;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import sql.generic.SQLOldTest;
import sql.SQLHelper;
import sql.generic.dmlstatements.GenericDML;
import util.TestException;
import util.TestHelper;

/**
 * Executor
 * 
 * @author Namrata Thanvi
 */

public class Executor {

  Connection gConn, dConn;

  public Connection getGConn() {
    return gConn;
  }

  public void setGConn(Connection conn) {
    gConn = conn;
  }

  public Connection getDConn() {
    return dConn;
  }

  public void setDConn(Connection conn) {
    dConn = conn;
  }

  PreparedStatement ps;

  public enum ExceptionMapKeys {
    DERBY, GEMXD
  };

  public enum ExceptionAt {
    DERBY, GEMXD, NONE
  };

  public Executor(Connection conn) {
    this.gConn = conn;
    this.dConn = null;
  }

  public Executor(Connection dConn, Connection gConn) {
    this.dConn = dConn;
    this.gConn = gConn;
  }

  // Trying to create something generic with proper ExceptionHanldling at one
  // Place

  public ExceptionAt executeOnBothDb(String derby, String gemxd,
      GenericExceptionHandler exceptionHandler) {
    boolean continueWithGemxd = true;
    try {
      dConn.createStatement().executeUpdate(derby);
      Log.getLogWriter().info("Derby - Executed " + derby);
    } catch (SQLException se) {
      Log.getLogWriter().info("Derby Exception - " + se.getMessage());
      continueWithGemxd = exceptionHandler.handleDerbyException(dConn, se);
    }

    exceptionHandler.afterDerbyExecution();

    if (!continueWithGemxd)
      return ExceptionAt.DERBY;

    try {
      gConn.createStatement().executeUpdate(gemxd);
      Log.getLogWriter().info("Gemxd - Executed " + gemxd);
    } catch (SQLException se) {
      Log.getLogWriter().info("Gemxd Exception - " + se.getMessage());
      if (exceptionHandler.handleGfxdException(se) == false) {
        return ExceptionAt.GEMXD;
      }
    }

    exceptionHandler.afterGemxdExecution();
    return ExceptionAt.NONE;
  }

  // executeQueryOnbothDb - new implementation
  public ResultSet[] executeQueryOnBothDB(String queryDerby, String queryGemxd,
      int maxNumOfRetryDerby, int maxNumOfRetryGemxd, int retrySleepMs,
      GenericExceptionHandler exceptionHandler, boolean[] success) {
    int count = 0;
    ResultSet[] rs = new ResultSet[2];
    try {
      rs[0] = executeQuery(queryDerby, maxNumOfRetryDerby, retrySleepMs,
          "Derby");
      success[0] = true;
      exceptionHandler.afterDerbyExecution();
    } catch (SQLException se) {
      exceptionHandler.handleDerbyException(dConn, se);
    }

    if (rs[0] != null) {
      try {
        rs[1] = executeQuery(queryGemxd, maxNumOfRetryGemxd, retrySleepMs,
            "Gemxd");
        success[1] = true;

        if (rs[1] == null) {
          success[1] = false;
          handleNullRs();
        }

        exceptionHandler.afterGemxdExecution();
      } catch (SQLException se) {
        exceptionHandler.handleGfxdException(se);
      }
    }
    return rs;
  }

  public ResultSet executeQueyOnGfxdOnly(String query, int maxNumOfTry,
      int sleep, GenericExceptionHandler exceptionHandler) {
    ResultSet rs = null;
    try {
      rs = executeQuery(query, maxNumOfTry, sleep, "Gfxd");
      if (rs == null) {
        handleNullRs();
      }
    } catch (SQLException se) {
      exceptionHandler.handleGfxdExceptionOnly(se);
    }

    return rs;
  }

  public ResultSet executeQuery(String query, int maxNumOfTry, int sleep,
      String db) throws SQLException {
    int count = 0;
    boolean success = false;
    ResultSet rs = null;
    while (count < maxNumOfTry && !success) {
      count++;
      MasterController.sleepForMs(GenericDML.rand.nextInt(sleep));
      PreparedStatement ps = dConn.prepareStatement(query);
      rs = ps.executeQuery();
      success = true;
    }
    if (count >= maxNumOfTry && !success) {
      Log.getLogWriter().info(
          "Could not get the lock to finisht the operation in " + db
              + ", abort this operation");
    }
    return rs;
  }

  public void handleNullRs() {
    if (SQLOldTest.isHATest) {
      Log.getLogWriter().info("Testing HA and did not get GFXD result set");
    }
    else if (SQLOldTest.setCriticalHeap) {
      Log.getLogWriter().info("got XCL54 and does not get query result");
    }
    else
      throw new TestException("Not able to get gfe result set after retry");
  }

  public boolean executeOnGfxdOnly(String executionString,
      GenericExceptionHandler exceptionHandler) {
    try {
      Statement stmt = gConn.createStatement();
      stmt.executeUpdate(executionString);
      Log.getLogWriter().info(" Completed Execution of ... " + executionString);
      SQLWarning warning = stmt.getWarnings(); // test to see there is a warning
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
        return exceptionHandler.handleGfxdWarningsOnly(warning);
      }
    } catch (SQLException se) {
      exceptionHandler.handleGfxdExceptionOnly(se);
      return false;
    }

    return true;
  }

  public void execute(String executionString) throws SQLException {

    gConn.createStatement().execute(executionString);

  }

  public ResultSet executeQuery(String executionString) throws SQLException {

    return gConn.createStatement().executeQuery(executionString);

  }

  public int execute(String executionString, List<Object> parameters)
      throws SQLException {

    int columnIndex = 1;
    ps = gConn.prepareStatement(executionString);
    for (Object param : parameters) {
      setValues(param, columnIndex++);
    }
    return ps.executeUpdate();

  }

  public HashMap<ExceptionMapKeys, SQLException> executeOnlyOnGFXD(
      String executionString) {
    HashMap<ExceptionMapKeys, SQLException> exceptionMap = new HashMap<ExceptionMapKeys, SQLException>();
    try {
      gConn.createStatement().execute(executionString);
    } catch (SQLException se) {
      exceptionMap.put(ExceptionMapKeys.GEMXD, se);
    }
    return exceptionMap;
  }

  public HashMap<ExceptionMapKeys, SQLException> executeOnBothDb(
      String executionString) {
    HashMap<ExceptionMapKeys, SQLException> exceptionMap = new HashMap<ExceptionMapKeys, SQLException>();
    if (SQLOldTest.hasDerbyServer) {
      try {
        dConn.createStatement().execute(executionString);
      } catch (SQLException se) {
        exceptionMap.put(ExceptionMapKeys.DERBY, se);
      }
    }
    try {
      gConn.createStatement().execute(executionString);
    } catch (SQLException se) {
      exceptionMap.put(ExceptionMapKeys.GEMXD, se);
    }
    return exceptionMap;
  }

  public ResultSet executeQuery(String executionString, List<Object> parameters)
      throws SQLException {

    int columnIndex = 1;
    ps = gConn.prepareStatement(executionString);
    for (Object param : parameters) {
      setValues(param, columnIndex++);
    }
    return ps.executeQuery();

  }

  public Connection getConnection() {
    return gConn;
  }

  public void setValues(Object value, int columnIndex) throws SQLException {

    if (value instanceof Integer) {
      ps.setInt(columnIndex, (Integer)value);
    }
    else if (value instanceof Long) {
      ps.setLong(columnIndex, (Long)value);
    }
    else if (value instanceof Float) {
      ps.setFloat(columnIndex, (Float)value);
    }
    else if (value instanceof Double) {
      ps.setDouble(columnIndex, (Double)value);
    }
    else if (value instanceof BigDecimal) {
      ps.setBigDecimal(columnIndex, (BigDecimal)value);
    }
    else if (value instanceof Boolean) {
      ps.setBoolean(columnIndex, (Boolean)value);
    }
    else if (value instanceof Blob) {
      ps.setBlob(columnIndex, (Blob)value);
    }
    else if (value instanceof String) {
      ps.setString(columnIndex, (String)value);
    }
    else if (value instanceof Date) {
      ps.setDate(columnIndex, (Date)value);
    }
    else if (value instanceof Byte[]) {
      ps.setBytes(columnIndex, (byte[])value);
    }
    else if (value instanceof Short) {
      ps.setShort(columnIndex, (Short)value);
    }
    else if (value instanceof Time) {
      ps.setTime(columnIndex, (Time)value);
    }
    else if (value instanceof Timestamp) {
      ps.setTimestamp(columnIndex, (Timestamp)value);
    }
    else if (value instanceof Clob) {
      ps.setClob(columnIndex, (Clob)value);
    }
    else {
      Log.getLogWriter().info(
          "This dataType is yet not supported : " + value.getClass()
              + " for object " + value);
    }

  }

  public void rollback() {
    if (dConn != null)
      rollbackDerby();
    rollbackGfxd();

  }

  public void commit() {
    if (dConn != null)
      commitDerby();
    commitGfxd();
  }

  public void commitDerby() {
    try {
      Log.getLogWriter().info(" Derby - Commit  started ");
      dConn.commit();
      Log.getLogWriter().info(" Derby - Commit  Completed ");
    } catch (SQLException se) {
      throw new TestException(" Error while commiting the derby database "
          + " sqlState : " + se.getSQLState() + " error message : "
          + se.getMessage() + TestHelper.getStackTrace(se));
    }
  }

  public void commitGfxd() {
    try {
      Log.getLogWriter().info(" Gfxd - Commit  started ");
      gConn.commit();
      Log.getLogWriter().info(" Gfxd - Commit  Completed ");
    } catch (SQLException se) {
      throw new TestException(" Error while commiting the Gfxd database "
          + " sqlState : " + se.getSQLState() + " error message : "
          + se.getMessage() + TestHelper.getStackTrace(se));
    }
  }

  public void rollbackDerby() {
    try {
      Log.getLogWriter().info(" Derby - Rollback  started ");
      dConn.rollback();
      Log.getLogWriter().info(" Derby - Rollback  Completed ");
    } catch (SQLException se) {
      throw new TestException(" Error while doing rollback derby database "
          + " sqlState : " + se.getSQLState() + " error message : "
          + se.getMessage() + TestHelper.getStackTrace(se));
    }
  }

  public void rollbackGfxd() {
    try {
      Log.getLogWriter().info(" Gfxd - Rollback  started ");
      gConn.rollback();
      Log.getLogWriter().info(" Gfxd - Rollback  Completed ");
    } catch (SQLException se) {
      throw new TestException(" Error while doing rollback Gfxd database "
          + " sqlState : " + se.getSQLState() + " error message : "
          + se.getMessage() + TestHelper.getStackTrace(se));
    }
  }
}
