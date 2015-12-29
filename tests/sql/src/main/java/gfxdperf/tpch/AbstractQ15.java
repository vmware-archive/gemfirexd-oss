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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Encapsulates TPC-H Q15.
 */
public abstract class AbstractQ15 extends Query {

// Query from TPCH-H spec
// TBD

  /** No-arg constructor
   * 
   */
  public AbstractQ15() {
    super();
    queryName = "Q15";
    query = "TBD";
    throw new PerfTestException("Query 15 is not yet implemented");
   }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#validateQuery()
   */
  @Override
  public String validateQuery() throws SQLException {
    throw new PerfTestException("Query 15 is not yet implemented");
  }

  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQuery()
   */
  @Override
  public int executeQuery() throws SQLException {
    throw new PerfTestException("Query 15 is not yet implemented");
  }
  
  /* (non-Javadoc)
   * @see gfxdperf.tpch.Query#executeQueryPlan()
   */
  @Override
  public void executeQueryPlan() throws SQLException {
    throw new PerfTestException("Query 15 is not yet implemented");
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
   * @return The ResultSet from executing the query.
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected ResultSet executeQuery(Object argsTBD) throws SQLException {
    throw new PerfTestException("Query 15 is not yet implemented");
  }
  
  /** Execute a query to obtain a query plan with the given query argument
   * 
   * @throws SQLException Thrown if any exceptions are encountered while executing this query.
   */
  protected void executeQueryPlan(Object argsTBD) throws SQLException {
    throw new PerfTestException("Query 15 is not yet implemented");
  }
  
  /**
   * Encapsulates a result row this TPC-H query.
   */
  public static class Result implements ResultRow {
    // columns in a result row
    // TBD

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#init()
     */
    @Override
    public void init(ResultSet rs) throws SQLException {
      throw new PerfTestException("Query 15 is not yet implemented");
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (true) throw new PerfTestException("Query 15 is not yet implemented");
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see gfxdperf.tpch.ResultRow#toAnswerLineFormat()
     */
    @Override
    public String toAnswerLineFormat() {
      StringBuilder sb = new StringBuilder();
      if (true) throw new PerfTestException("Query 15 is not yet implemented");
      return sb.toString();
    }
  }
}
