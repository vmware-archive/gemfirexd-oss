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

package objects.query;

import com.gemstone.gemfire.cache.Region;

import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface for objects that support SQL queries.
 */
public interface SQLQueryFactory extends QueryFactory {

  public String getCreateSchemaStatement();
  public String getDropSchemaStatement();
  public List getTableStatements();
  public List getDropTableStatements();

  /**
   * Returns an List of index statements to execute
   *
   * @throws QueryFactoryException if the indexType is not found.
   */
  public List getIndexStatements();
  
  /**
   * Returns an List of index statements to execute
   *
   * @throws QueryFactoryException if the constraints are not found.
   */
  public List getConstraintStatements();
  
  /**
   * Returns the list of statements needed to insert the tables.
   */
  public List getInsertStatements(int id);
  
  /**
   * Returns the list of statements formed for preparation needed to insert into the tables.
   */
  public List getPreparedInsertStatements();
  
  /**
   * Given a list of prepared insert statements, will fill in parameters based on i, and then execute
   */
  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int i)
      throws SQLException;
  
  /**
   * Given a prepared statement, will fill in parameters based on i, and then execute
   */
  public ResultSet fillAndExecutePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException;
  
  /**
   * Given a prepared statement, will fill in parameters based on i, and then execute
   */
  public int fillAndExecuteUpdatePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException;
  
  //Possible to put these into an abstract class.
  public ResultSet execute(String stmt, Connection conn) throws SQLException;
  public ResultSet executeQuery(String stmt, Connection conn)
      throws SQLException;
  public int executeUpdate(String stmt, Connection conn) throws SQLException;
  /**
   * This method will be used to pull data from the result set so we know we are actually
   * retrieving data and not just a cursor to data
   */
  public int readResultSet(int queryType, ResultSet rs) throws SQLException;
  
  //hack for getting to the regions directly
  public Region getRegionForQuery(int queryType);
  public Object directGet(Object key, Region region);
  
}
