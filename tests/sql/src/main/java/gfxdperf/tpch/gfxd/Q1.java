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
/**
 * 
 */
package gfxdperf.tpch.gfxd;

import gfxdperf.tpch.AbstractQ1;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author lynng
 *
 */
public class Q1 extends AbstractQ1 {
  
  /** Constructor
   * 
   * @param conn The Oracle connection.
   * @param rng Random number generator.
   */
  public Q1(Connection conn, Random rng) {
    super();
    this.connection = conn;
    this.rng = rng;
    // this query differs from oracle in the "cast" clause
    query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= cast({fn timestampadd(SQL_TSI_DAY, ?, timestamp('1998-12-01 23:59:59'))} as DATE) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
  }
  
  /**
   * Validates the query with specific parameters for which TPC-H provides an
   * expected answer. This can only be used with a scaling factor of 1G.
   */
  @Override
  public String validateQuery() throws SQLException {
    int delta = 90; // the value for which TPC-H provides an expected answer
    ResultSet rs = executeQuery(-delta); // negative due to date implementation, see bug 50503

    return validateQuery(rs);
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    // DELTA is randomly selected within [60. 120]
    int delta = 60 + this.rng.nextInt(61);

    ResultSet rs = executeQuery(-delta); // negative due to date implementation, see bug 50503
    return readResultSet(rs);
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    // DELTA is randomly selected within [60. 120]
    int delta = 60 + this.rng.nextInt(61);
    executeQueryPlan(-delta); // negative due to date implementation, see bug 50503
  }

}
