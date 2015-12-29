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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulates TPC-H Q7.
 */
public abstract class AbstractQ7 extends Query {

// Query from TPCH-H spec
// select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]') or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year

  /** No-arg constructor
   * 
   */
  public AbstractQ7() {
    super();
    queryName = "Q7";
    query = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = ? and n2.n_name = ?) or (n1.n_name = ? and n2.n_name = ?)) and l_shipdate between '1995-01-01' and '1996-12-31') as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String nation1 = "FRANCE";
    String nation2 = "GERMANY";
    ResultSet rs = executeQuery(nation1, nation2);
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    String[] args = getRandomArgs();
    String nation1 = args[0];
    String nation2 = args[1];
    ResultSet rs = executeQuery(nation1, nation2);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    String[] args = getRandomArgs();
    String nation1 = args[0];
    String nation2 = args[1];
    executeQueryPlan(nation1, nation2);
  }
  
  /** Return an array of random args for this query.
   *    NATION1 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
   *    NATION2 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3 and must be different than NATION1
   *  @return An array containing [0] nation1, [1] nation2
   */
  protected String[] getRandomArgs() {
    int index = rng.nextInt(NATION.length-1);
    String nation1 = NATION[index];
    List<String> remainingNations = new ArrayList(Arrays.asList(NATION));
    remainingNations.remove(index);
    String nation2 = remainingNations.get(rng.nextInt(remainingNations.size()-1));
    return new String[] {nation1, nation2};
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
   * @param nation1 First nation parameter for this query.
   * @param nation2 Second nation parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(String nation1, String nation2) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with nation1=" + nation1 + ", nation2=" + nation2);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setString(1, nation1);
    preparedQueryStmt.setString(2, nation2);
    preparedQueryStmt.setString(3, nation2);
    preparedQueryStmt.setString(4, nation1);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param nation1 First nation parameter for this query.
   * @param nation2 Second nation parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(String nation1, String nation2) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with nation1=" + nation1 + ", nation2=" + nation2);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setString(1, nation1);
    preparedQueryPlanStmt.setString(2, nation2);
    preparedQueryPlanStmt.setString(3, nation2);
    preparedQueryPlanStmt.setString(4, nation1);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
   String suppNation;
   String custNation;
   String year;
   BigDecimal revenue;
 
    // TBD

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      suppNation = rs.getString(1);
      custNation = rs.getString(2);
      year = rs.getString(3);
      revenue = rs.getBigDecimal(4);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" suppNation=").append(suppNation)
        .append(" custNation=").append(custNation)
        .append(" year=").append(year)
        .append(" revenue=").append(revenue);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%-25s",suppNation) + "|")
        .append(String.format("%-25s",custNation) + "|")
        .append(String.format("%20s", year) + "|")
        .append(revenue.setScale(2, RoundingMode.HALF_UP));
      return sb.toString();
    }
  }
}
