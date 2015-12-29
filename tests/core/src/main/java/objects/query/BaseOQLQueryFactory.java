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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

//import hydra.CacheHelper;
import hydra.Log;

import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collection;

public abstract class BaseOQLQueryFactory extends BaseQueryFactory implements OQLQueryFactory {

  /*
  public Query prepareOQLStatement(String stmt) {
    Cache cache = CacheHelper.getCache();
    return cache.getQueryService().newQuery(stmt);
  }
  */

  /*
  public ResultSet execute(String stmt, Connection conn) throws SQLException {
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    ResultSet rs = null;
    Statement s = conn.createStatement();
    boolean result = s.execute(stmt);
    if (result == true) {
      rs = s.getResultSet();
      //logResultSetSize(rs);
    }
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    return rs;
  }

  public int executeUpdate(String stmt, Connection conn) throws SQLException {
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    Statement s = conn.createStatement();
    int result = s.executeUpdate(stmt);
    s.close();
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info(
          "Executed: " + stmt + " on: " + conn + " ended with result = "
              + result);
    }
    conn.commit();
    return result;
  }

  public ResultSet executeQuery(String stmt, Connection conn)
      throws SQLException {
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    }
    Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery(stmt);
    //logResultSetSize(rs);
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    return rs;
  }
  */
  /*
  protected int logResultSetSize(ResultSet rs) throws SQLException {
    int rsSize = 0;
    if (QueryPrms.logQueryResultSize()) {
      while (rs.next()) {
        rsSize++;
      }
      Log.getLogWriter().info("Returned " + rsSize + " rows");
    }
    return rsSize;
  }
  */

  public Object executeQueryPreparedStatement(Query pstmt, Object[] params)
      throws NameResolutionException, TypeMismatchException,
      FunctionDomainException, QueryInvocationTargetException {
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executing query: " + pstmt.getQueryString());
    }
    //Collection rs = new ArrayList();
    Object rs = pstmt.execute(params);
    /*
    if (value instanceof Collection) {
      rs.addAll((Collection)value);
    }
    else {
      rs.add(value);
    }
    */
    if (QueryPrms.logQueries()) {
      Log.getLogWriter().info("Executed query: " + pstmt);
    }
    return rs;
  }
}
