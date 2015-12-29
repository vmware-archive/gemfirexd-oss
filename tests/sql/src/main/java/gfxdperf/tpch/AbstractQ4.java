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
package gfxdperf.tpch;

import hydra.Log;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Encapsulates TPC-H Q4.
 */
public abstract class AbstractQ4 extends Query {

// Query from TPCH-H spec
// select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '[DATE]' and o_orderdate < date '[DATE]' + interval '3' month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority;

  /** No-arg constructor
   * 
   */
  public AbstractQ4() {
    super();
    queryName = "Q4";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String dateStr = "1993-07-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(dateStr + " 00:00:00.0");
    ResultSet rs = executeQuery(date, timestamp);
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    Object[] args = getRandomArgs();
    Date date = (Date)(args[0]);
    Timestamp timestamp = (Timestamp)(args[1]);
    ResultSet rs = executeQuery(date, timestamp);
    return readResultSet(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    Object[] args = getRandomArgs();
    Date date = (Date)(args[0]);
    Timestamp timestamp = (Timestamp)(args[1]);
    executeQueryPlan(date, timestamp);
  }
  
  /** Return an array of random args for this query.
   *    DATE is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.   
   *  @return An array containing [0] date(Date), [1] timestamp(Timestamp) (where timestamp is equivalent to date)
   */
  protected Object[] getRandomArgs() {
    String year = "199" + (this.rng.nextInt(4) + 3);
    String month = String.format("%02d", this.rng.nextInt((year.equals("1997") ? 9 : 11)) + 1);
    String dateStr = year + "-" + month + "-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(date + " 00:00:00.0");
    return new Object[] {date, timestamp};
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#createResultRow()
   */
  @Override
  public ResultRow createResultRow() {
    return new Result();
  }
  
  /** Execute the TPC-H query with the specific parameters for this query.
   * 
   * @param date Date parameter for this query.
   * @param timestamp Timestamp parameter for this query, representing the same value as date.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(Date date, Timestamp timestamp) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with date=" + date);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setDate(1, date);
    preparedQueryStmt.setTimestamp(2, timestamp);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param date Date parameter for this query.
   * @param timestamp Timestamp parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(Date date, Timestamp timestamp) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with date=" + date);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setDate(1, date);
    preparedQueryPlanStmt.setTimestamp(2, timestamp);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    String o_orderpriority = null;
    int ordercount = 0;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      o_orderpriority = rs.getString(1);
      ordercount = rs.getInt(2);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" o_orderpriority=").append(o_orderpriority)
        .append(" ordercount=").append(ordercount);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(o_orderpriority + "|")
        .append(String.format("%22d", ordercount));
      return sb.toString();
    }
  }
}
