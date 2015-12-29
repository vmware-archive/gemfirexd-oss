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

import gfxdperf.PerfTestException;
import hydra.Log;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Encapsulates TPC-H Q6.
 */
public abstract class AbstractQ6 extends Query {

// Query from TPCH-H spec
// select sum(l_extendedprice*l_discount) as revenue from lineitem where l_shipdate >= date '[DATE]' and l_shipdate < date '[DATE]' + interval '1' year and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 and l_quantity < [QUANTITY];
  /** No-arg constructor
   * 
   */
  public AbstractQ6() {
    super();
    queryName = "Q6";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String dateStr = "1994-01-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(dateStr + " 00:00:00.0");
    double discount = 0.06;
    int quantity = 24; 
    ResultSet rs = executeQuery(date, timestamp, discount, quantity);
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
    double discount = (Double)(args[2]);
    int quantity = (Integer)(args[3]);
    ResultSet rs = executeQuery(date, timestamp, discount, quantity);
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
    double discount = (Double)(args[2]);
    int quantity = (Integer)(args[3]);
    executeQueryPlan(date, timestamp, discount, quantity);
  }
  
  /** Return an array of random args for this query.
   *    DATE is the first of January of a randomly selected year within [1993 .. 1997];
   *    DISCOUNT is randomly selected within [0.02 .. 0.09];
   *    QUANTITY is randomly selected within [24 .. 25]
   *  @return An array containing [0] date(Date), [1] timestamp(Timestamp) [2] discount(Double) [3] quentity(Integer)
   *          (where timestamp is equivalent to date)
   */
  protected Object[] getRandomArgs() {
    String dateStr = "199" + (this.rng.nextInt(4)+3) + "-01-01";
    Date date = Date.valueOf(dateStr);
    Timestamp timestamp = Timestamp.valueOf(date + " 00:00:00.0");
    double discount = (this.rng.nextInt(8)+2) * 0.01;
    int quantity = this.rng.nextInt(1)+24;
    return new Object[] {date, timestamp, discount, quantity};
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
   * @param timestamp The timestamp that is equivalent to date.
   * @param discount Discount parameter for this query.
   * @param quantity Quantity parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(Date date, Timestamp timestamp, double discount, int quantity) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with date=" + date + ", timestamp=" + timestamp + ", discount=" + discount + ", quantity=" + quantity);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setDate(1, date);
    preparedQueryStmt.setTimestamp(2, timestamp);
    preparedQueryStmt.setDouble(3, discount);
    preparedQueryStmt.setDouble(4, discount);
    preparedQueryStmt.setInt(5, quantity);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param date Date parameter for this query.
   * @param timestamp The timestamp that is equivalent to date.
   * @param discount Discount parameter for this query.
   * @param quantity Quantity parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(Date date, Timestamp timestamp, double discount, int quantity) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with date=" + date + ", timestamp=" + timestamp + ", discount=" + discount + ", quantity=" + quantity);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setDate(1, date);
    preparedQueryPlanStmt.setTimestamp(2, timestamp);
    preparedQueryPlanStmt.setDouble(3, discount);
    preparedQueryPlanStmt.setDouble(4, discount);
    preparedQueryPlanStmt.setInt(5, quantity);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    BigDecimal revenue;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      revenue = rs.getBigDecimal(1);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" revenue=").append(revenue);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(revenue.setScale(2, RoundingMode.HALF_UP));
      return sb.toString();
    }
  }
}
