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
 * Encapsulates TPC-H Q2.
 */
public abstract class AbstractQ2 extends Query {

// Query from TPCH-H spec
// select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = ? and p_type like '%?' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = '?' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = '?' ) order by s_acctbal desc, n_name, s_name, p_partkey

  /** No-arg constructor
   * 
   */
  public AbstractQ2() {
    super();
    queryName = "Q2";
    // this query is limited to the top 10 elements; the sql to limit is added in subclasses
    this.query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = ? and p_type like ? and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = ? and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = ?) order by s_acctbal desc, n_name, s_name, p_partkey";
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    // the values for which TPC-H provides an expected answer
    int size = 15;
    String type = "BRASS";
    String region = String.format(REGION_FORMAT_STR,"EUROPE");
    ResultSet rs = executeQuery(size, type, region);
    
    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    Object[] args = getRandomArgs();
    int size = (Integer)(args[0]);
    String type = (String)(args[1]);
    String region = (String)(args[2]);
    ResultSet rs = executeQuery(size, type, region);
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    Object[] args = getRandomArgs();
    int size = (Integer)(args[0]);
    String type = (String)(args[1]);
    String region = (String)(args[2]);
    executeQueryPlan(size, type, region);
  }
  
  /** Return an array of random args for this query.
   *    SIZE is randomly selected within [1, 50]
   *    TYPE is randomly selected from the TYPE array
   *    REGION is randomly selected from the REGION array
   *  @return An array containing [0] size(Integer), [1] type(String), [2] region(String)
   */
  protected Object[] getRandomArgs() {
    int size = 1 + this.rng.nextInt(50);
    String type = TYPE[this.rng.nextInt(TYPE.length-1)];
    String region = String.format(REGION_FORMAT_STR, REGION[this.rng.nextInt(REGION.length-1)]);
    return new Object[] {size, type, region};
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
   * @param size Size parameter for this query.
   * @param type Type parameter for this query.
   * @param region Region parameter for this query.
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(int size, String type, String region) throws SQLException {
    if (logDML) {
      Log.getLogWriter().info(queryName + ": " + query);
      Log.getLogWriter().info("Executing " + queryName + " with size=" + size + " type=" + type + " region=" + region);
    }
    if (preparedQueryStmt == null) {
      preparedQueryStmt = this.connection.prepareStatement(query);
    }
    preparedQueryStmt.setInt(1, size);
    preparedQueryStmt.setString(2, "%" + type);
    preparedQueryStmt.setString(3, region);
    preparedQueryStmt.setString(4, region);
    return preparedQueryStmt.executeQuery();
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @param size Size parameter for this query.
   * @param type Type parameter for this query.
   * @param region Region parameter for this query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(int size, String type, String region) throws SQLException {
    Log.getLogWriter().info("Getting query plan for " + queryName + " with size=" + size + 
        " type=" + type + " region=" + region);
    if (preparedQueryPlanStmt == null) {
      preparedQueryPlanStmt = this.connection.prepareStatement("explain " + query);
    }
    preparedQueryPlanStmt.setInt(1, size);
    preparedQueryPlanStmt.setString(2, "%" + type);
    preparedQueryPlanStmt.setString(3, region);
    preparedQueryPlanStmt.setString(4, region);    
    ResultSet rs = preparedQueryPlanStmt.executeQuery();
    logQueryPlan(rs);
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    public BigDecimal s_acctbal;
    public String s_name;
    public String n_name;
    public int p_partkey;
    public String p_mfgr;
    public String s_address;
    public String s_phone;
    public String s_comment;

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      s_acctbal = rs.getBigDecimal(1);
      s_name = rs.getString(2);
      n_name = rs.getString(3);
      p_partkey = rs.getInt(4);
      p_mfgr = rs.getString(5);
      s_address = rs.getString(6);
      s_phone = rs.getString(7);
      s_comment = rs.getString(8);
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" s_acctbal=").append(s_acctbal)
        .append(" s_name=").append(s_name)
        .append(" n_name=").append(n_name)
        .append(" p_partkey=").append(p_partkey)
        .append(" p_mfgr=").append(p_mfgr)
        .append(" s_address=").append(s_address)
        .append(" s_phone=").append(s_phone)
        .append(" s_comment=").append(s_comment);
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      sb.append(s_acctbal.setScale(2, RoundingMode.HALF_UP) + "|")
        .append(s_name + "|")
        .append(n_name + "|")
        .append(String.format("%20d",p_partkey) + "|")
        .append(p_mfgr + "|")
        .append(String.format("%-40s",s_address) + "|")
        .append(s_phone + "|")
        .append(String.format("%-101s",s_comment));
      return sb.toString();
    }
  }
}
