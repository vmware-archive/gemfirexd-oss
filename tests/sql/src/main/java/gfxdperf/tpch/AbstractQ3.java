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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Encapsulates TPC-H Q3.
 */
public abstract class AbstractQ3 extends Query {

// Query from TPCH-H spec
// select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = '[SEGMENT]' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '[DATE]' and l_shipdate > date '[DATE]' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate

  // date is randomly selected from  [1995-03-01 .. 1995-03-31].
  public static final String BASE_DATE_STR = "1995-03-";
  
  /** No-arg constructor
   * 
   */
  public AbstractQ3() {
    super();
    queryName = "Q3";
    // this query is limited to the top 10 elements; the sql to limit is added in subclasses
    query = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = ? and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < ? and l_shipdate > ? group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String segment = String.format(SEGMENT_FORMAT_STR, "BUILDING");
    Date date = Date.valueOf("1995-03-15");
    
    ResultSet rs = executeQuery(segment, date);
    
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    Object[] args = getRandomArgs();
    String segment = (String)(args[0]);
    Date date = (Date)(args[1]);
    ResultSet rs = executeQuery(segment, date);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    Object[] args = getRandomArgs();
    String segment = (String)(args[0]);
    Date date = (Date)(args[1]);
    executeQueryPlan(segment, date);
  }
  
  /** Return an array of random args for this query.
   *    SEGMENT is randomly selected within the list of values defined for Segments in Clause 4.2.2.13;
   *    DATE is a randomly selected day within [1995-03-01 .. 1995-03-31].
   *  @return An array containing [0] segment(String), [1] date(Date)
   */
  protected Object[] getRandomArgs() {
    String segment = SEGMENT[this.rng.nextInt(SEGMENT.length-1)];
    String dateStr = BASE_DATE_STR + (this.rng.nextInt(30) + 1);
    Date date = Date.valueOf(dateStr);
    return new Object[] {segment, date};
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
   * @param segment Segment parameter for this query.
   * @param date Date parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(String segment, Date date) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with segment=" + segment + " date=" + date);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setString(1, segment);
    preparedQueryStmt.setDate(2, date);
    preparedQueryStmt.setDate(3, date);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param segment Segment parameter for this query.
   * @param date Date parameter for this query.
   * @param region Region parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(String segment, Date date) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with segment=" + segment + " date=" + date);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setString(1, segment);
    preparedQueryPlanStmt.setDate(2, date);
    preparedQueryPlanStmt.setDate(3, date);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    public int l_orderKey;
    public BigDecimal revenue;
    public Date o_orderDate;
    public int o_shipPriority;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      l_orderKey = rs.getInt(1);
      revenue = rs.getBigDecimal(2);
      o_orderDate = rs.getDate(3);
      o_shipPriority = rs.getInt(4);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" l_orderKey=").append(l_orderKey)
        .append(" revenue=").append(revenue)
        .append(" o_orderDate=").append(o_orderDate)
        .append(" o_shipPriority=").append(o_shipPriority);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%22d",l_orderKey) + "|")
        .append(revenue.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(o_orderDate + "|")
        .append(String.format("%20d",o_shipPriority));
      return sb.toString();
    }
  }
}
