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

package com.pivotal.gemfirexd.callbacks;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Used to represent the meta-data information of a table, or projection of some
 * columns of a table.
 * 
 * @author swale
 */
public interface TableMetaData extends ResultSetMetaData {

  /**
   * Get the actual position of a given column in the table. For a full table
   * meta-data this will always return the argument itself as the result.
   * 
   * @param column
   *          position of the column (1-based); for a projection it is the
   *          position of column in the projection
   * 
   * @throws SQLException
   *           if column could not be found, or there is no corresponding
   *           mapping to a column in table (SQLState: S0022)
   */
  public int getTableColumnPosition(int column) throws SQLException;

  /**
   * Get the position of a column (1-based) given its name. For a full table
   * meta-data this will return the position of the column in the table, while
   * for a projection of some columns it will return the position in the
   * projection.
   * 
   * @param columnName
   *          name of the column
   * 
   * @throws SQLException
   *           if column with given name could not be found (SQLState: 42X04)
   */
  public int getColumnPosition(String columnName) throws SQLException;

  /**
   * Get the column width as declared in the DDL.
   */
  public int getDeclaredColumnWidth(int column) throws SQLException;

  /**
   * Get the version of the schema of the table for which this metadata object
   * was obtained. This is a unique number for the schema of a table that
   * changes after an ALTER TABLE has been executed.
   */
  public int getSchemaVersion();
}
