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

/**
 * Encapsulates TPC-H Q8.
 */
public abstract class AbstractQ8 extends Query {

// Query from TPCH-H spec
// select o_year, sum(case when nation = '[NATION]' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = '[REGION]' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = '[TYPE]' ) as all_nations group by o_year order by o_year;

  /** No-arg constructor
   * 
   */
  public AbstractQ8() {
    super();
    queryName = "Q8";
    query = "select o_year, sum(case when nation = ? then volume else 0 end) / sum(volume) as mkt_share from ( select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = ? and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = ? ) as all_nations group by o_year order by o_year";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    String nation = "BRAZIL";
    String region = "AMERICA";
    String type = "ECONOMY ANODIZED STEEL";
    ResultSet rs = executeQuery(nation, region, type);
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    String[] args = getRandomArgs();
    String nation = args[0];
    String region = args[1];
    String type = args[2];
    ResultSet rs = executeQuery(nation, region, type);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    String[] args = getRandomArgs();
    String nation = args[0];
    String region = args[1];
    String type = args[2];
    executeQueryPlan(nation, region, type);
  }
  
  /** Return an array of random args for this query.
   *    NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
   *    REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to N_REGIONKEY for the selected NATION in item 1 above;
   *    TYPE is randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13.
   *  @return An array containing [0] nation, [1] region [2] type
   */
  protected String[] getRandomArgs() {
    String nation = NATION[this.rng.nextInt(NATION.length-1)];
    String region = nationToRegionMap.get(nation);
    String type = SYLLABLE1[this.rng.nextInt(SYLLABLE1.length-1)] + " " + SYLLABLE2[this.rng.nextInt(SYLLABLE2.length-1)] + " " +
                  SYLLABLE3[this.rng.nextInt(SYLLABLE3.length-1)];
    return new String[] {nation, region, type};
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
   * @param nation Nation parameter for this query.
   * @param region Region parameter for this query.
   * @param type Type parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(String nation, String region, String type) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with nation=" + nation + ", region=" + region + ", type=" + type);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setString(1, nation);
    preparedQueryStmt.setString(2, region);
    preparedQueryStmt.setString(3, type);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param nation Nation parameter for this query.
   * @param region Region parameter for this query.
   * @param type Type parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(String nation, String region, String type) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with nation=" + nation + ", region=" + region + ", type=" + type);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setString(1, nation);
    preparedQueryPlanStmt.setString(2, region);
    preparedQueryPlanStmt.setString(3, type);
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    String year;
    BigDecimal mktShare;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      year = rs.getString(1);
      mktShare = rs.getBigDecimal(2);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" year=").append(year)
        .append(" mktShare=").append(mktShare);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%20s", year) + "|")
        .append(mktShare.setScale(2, RoundingMode.HALF_UP));
      return sb.toString();
    }
  }
}
