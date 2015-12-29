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
import java.sql.Timestamp;

/**
 * Encapsulates TPC-H Q5.
 */
public abstract class AbstractQ5 extends Query {

// Query from TPCH-H spec
// select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = '[REGION]' and o_orderdate >= date '[DATE]' and o_orderdate < date '[DATE]' + interval '1' year group by n_name order by revenue desc

  /** No-arg constructor
   * 
   */
  public AbstractQ5() {
    super();
    queryName = "Q5";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String region = "ASIA";
    String dateStr = "1994-01-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(dateStr + " 00:00:00.0");
    ResultSet rs = executeQuery(region, date, timestamp);
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    Object[] args = getRandomArgs();
    String region = (String)(args[0]);
    Date date = (Date)(args[1]);
    Timestamp timestamp = (Timestamp)(args[2]);
    ResultSet rs = executeQuery(region, date, timestamp);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    Object[] args = getRandomArgs();
    String region = (String)(args[0]);
    Date date = (Date)(args[1]);
    Timestamp timestamp = (Timestamp)(args[2]);
    executeQueryPlan(region, date, timestamp);
  }
  
  /** Return an array of random args for this query.
   *    REGION is randomly selected within the list of values defined for R_NAME in 4.2.3;
   *    DATE is the first of January of a randomly selected year within [1993 .. 1997].
   *  @return An array containing [0] region(String), [1] date(Date) [2] timestamp(Timestamp)
   *          (where timestamp is equivalent to date)
   */
  protected Object[] getRandomArgs() {
    String region = String.format(REGION_FORMAT_STR, REGION[this.rng.nextInt(REGION.length-1)]);
    String dateStr = "199" + (this.rng.nextInt(4)+3) + "-01-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(date + " 00:00:00.0");
    return new Object[] {region, date, timestamp};
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
   * @param region Region parameter for this query.
   * @param date Region parameter for this query.
   * @param timestamp Date as a timestamp parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(String region, Date date, Timestamp timestamp) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with region=" + region + ", date=" + date + ", timestamp=" + timestamp);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setString(1, region);
    preparedQueryStmt.setDate(2, date);
    preparedQueryStmt.setTimestamp(3, timestamp);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(String region, Date date, Timestamp timestamp) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with region=" + region + ", date=" + date + ", timestamp=" + timestamp);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setString(1, region);
    preparedQueryPlanStmt.setDate(2, date);
    preparedQueryPlanStmt.setTimestamp(3, timestamp);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    String n_name;
    BigDecimal revenue;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      n_name = rs.getString(1);
      revenue = rs.getBigDecimal(2);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" n_name=").append(n_name)
        .append(" revenue=").append(revenue);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(n_name + "|")
        .append(revenue.setScale(2, RoundingMode.HALF_UP));
      return sb.toString();
    }
  }
}
