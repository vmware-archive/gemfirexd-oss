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
package objects.query;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public abstract class BaseSQLQueryFactory extends BaseQueryFactory implements SQLQueryFactory {

  public ResultSet execute(String stmt, Connection conn) throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    ResultSet rs = null;
    Statement s = conn.createStatement();
    boolean result = s.execute(stmt);
    if (result == true) {
      rs = s.getResultSet();
      //logResultSetSize(rs);
    }
    if (logQueries) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    s.close();
    return rs;
  }

  public int executeUpdate(String stmt, Connection conn) throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    Statement s = conn.createStatement();
    int result = s.executeUpdate(stmt);
    s.close();
    if (logQueries) {
      Log.getLogWriter().info(
          "Executed: " + stmt + " on: " + conn + " ended with result = "
              + result);
    }
    //close statement here because we don't return a result set.
    s.close();
    //conn.commit();
    return result;
  }

  public ResultSet executeQuery(String stmt, Connection conn)
      throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery(stmt);
    //logResultSetSize(rs);
    if (logQueries) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    s.close();
    return rs;
  }

  /*
  protected int logResultSetSize(ResultSet rs) throws SQLException {
    int rsSize = 0;
    if (logQueryResultSize) {
      while (rs.next()) {
        rsSize++;
      }
      Log.getLogWriter().info("Returned " + rsSize + " rows");
    }
    return rsSize;
  }
  */

  public ResultSet executeQueryPreparedStatement(PreparedStatement pstmt)
      throws SQLException {
    //if (logQueries) {
    //  Log.getLogWriter().info("Executing query: " + pstmt);
    //}
    ResultSet rs = pstmt.executeQuery();
    if (logWarnings) {
      logWarnings(pstmt);
    }
    //if (logQueries) {
    //  Log.getLogWriter().info("Executed query: " + pstmt);
    //}
    return rs;
  }

  public int executeUpdatePreparedStatement(PreparedStatement pstmt)
      throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing query: " + pstmt);
    }
    int numUpdated = pstmt.executeUpdate();
    if (logWarnings) {
      logWarnings(pstmt);
    }
    if (logQueries) {
      Log.getLogWriter().info("Executed query: " + pstmt + ", " + numUpdated);
    }
    return numUpdated;
  }

  protected void logWarnings(PreparedStatement pstmt)
  throws SQLException {
    SQLWarning warning = pstmt.getWarnings();
    while (warning != null) {
      String message = warning.getMessage();
      //String sqlState = warning.getSQLState();
      int errorCode = warning.getErrorCode();
      Log.getLogWriter().warning("While executing prepared statement: " + pstmt
                               + " got error code: " + errorCode
                               + " for: " + message);
      warning = warning.getNextWarning();
    }
  }
}
