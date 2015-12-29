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
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Encapsulates TPC-H Q1.
 */
public abstract class AbstractQ1 extends Query {

  // Query from TPCH-H spec
  // select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '[?]' day (3) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus

  /** No-arg constructor
   */
  public AbstractQ1() {
    super();
    queryName = "Q1";
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    int delta = 90; // the value for which TPC-H provides an expected answer
    ResultSet rs = executeQuery(delta);

    return validateQuery(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    // DELTA is randomly selected within [60. 120]
    int delta = 60 + this.rng.nextInt(61);

    ResultSet rs = executeQuery(delta);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    // DELTA is randomly selected within [60. 120]
    int delta = 60 + this.rng.nextInt(61);
    executeQueryPlan(delta);
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
   * @param delta Delta parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(int delta) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with delta=" + delta);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setInt(1, delta); 
    return preparedQueryStmt.executeQuery();
  }

  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param delta Delta parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while getting the query plan.
   */
  protected void executeQueryPlan(int delta) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with delta=" + delta);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setInt(1, delta);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }

  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    public String l_returnflag;
    public String l_linestatus;
    public BigDecimal sum_qty;
    public BigDecimal sum_base_price;
    public BigDecimal sum_disc_price;
    public BigDecimal sum_charge;
    public BigDecimal avg_qty;
    public BigDecimal avg_price;
    public BigDecimal avg_disc;
    public int count_order;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      l_returnflag = rs.getString(1);
      l_linestatus = rs.getString(2);
      sum_qty = rs.getBigDecimal(3);
      sum_base_price = rs.getBigDecimal(4);
      sum_disc_price = rs.getBigDecimal(5);
      sum_charge = rs.getBigDecimal(6);
      avg_qty = rs.getBigDecimal(7);
      avg_price = rs.getBigDecimal(8);
      avg_disc = rs.getBigDecimal(9);
      count_order = rs.getInt(10);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" l_returnflag=").append(l_returnflag)
        .append(" l_linestatus=").append(l_linestatus)
        .append(" sum_qty=").append(sum_qty)
        .append(" sum_base_price=").append(sum_base_price)
        .append(" sum_disc_price=").append(sum_disc_price)
        .append(" sum_charge=").append(sum_charge)
        .append(" avg_qty=").append(avg_qty)
        .append(" avg_price=").append(avg_price)
        .append(" avg_disc=").append(avg_disc)
        .append(" count_order=" + count_order);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(l_returnflag + "|")
        .append(l_linestatus + "|")
        .append(sum_qty.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(sum_base_price.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(sum_disc_price.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(sum_charge.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(avg_qty.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(avg_price.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(avg_disc.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(String.format("%22d",count_order));
      return sb.toString();
    }
  }
}
