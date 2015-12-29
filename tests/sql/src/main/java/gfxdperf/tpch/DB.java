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

import gfxdperf.tpch.TPCHPrms.TableName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 */
public class DB {
  protected Random rng;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init(Random random) throws DBException {
    this.rng = random;
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
  }

  /**
   * Drops the table.
   */
  public void dropTable(TableName tableName) throws DBException {
    throw new UnsupportedOperationException();
  }

  /** 
   * Return the Connection for this db.
   */
  public Connection getConnection() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Imports the table from the given file.
   */
  public void importTable(String fn, TableName tableName) throws DBException {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates the given index.
   */
  public void createIndex(String index) throws DBException {
    throw new UnsupportedOperationException();
  }

  /**
   * Validates all supported TPC-H queries.
   */
  public void validateQueries() throws DBException {
    throw new UnsupportedOperationException();
  }

  /**
   * Validates the TPC-H query with the given number.
   */
  public void validateQuery(int queryNum) throws DBException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes the TPC-H query with the given number.
   */
  public void executeQuery(int queryNum) throws DBException {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Executes a query plan for the TPC-H query with the given number.
   */
  public void executeQueryPlan(int queryNum) throws DBException {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Executes the given sql string.
   * @param sqlStr A String containing sql to execute.
   * @return The result of the execution, either a ResultSet or update count (Integer)
   * @throws DBException Thrown if anything goes wrong during execution.
   */
  public Object executeSql(String sqlStr) throws DBException {
    try {
      Statement stmt = getConnection().createStatement();
      boolean result = stmt.execute(sqlStr);
      if (result) { 
        return stmt.getResultSet();
      } else {
        return stmt.getUpdateCount();
      }
    } catch (SQLException e) {
      String s = "Problem executing:" + sqlStr;
      throw new DBException(s, e);
    }
  }
  
}
