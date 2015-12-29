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
package sql.memscale;

import java.util.ArrayList;
import java.util.List;

import util.TestException;

/**
 * @author lynng
 *
 */
public class TableDefinition implements java.io.Serializable {

  private String schemaName = null;
  private String tableName = null;
  private List<String> columnNames = new ArrayList<String>();
  private List<String> columnTypes = new ArrayList<String>();
  private List<String> columnTypeLengths = new ArrayList<String>();
  private List<Boolean> columnIsPrimaryKey = new ArrayList<Boolean>();

  public TableDefinition(String schemaNameArg, String tableNameArg) {
    schemaName = schemaNameArg;
    tableName = tableNameArg;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }
  
  public String getFullTableName() {
    return schemaName + "." + tableName;
  }

  public int getNumColumns() {
    return columnNames.size();
  }

  public String getColumnName(int index) {
    return columnNames.get(index);
  }

  public String getColumnType(int index) {
    return columnTypes.get(index);
  }

  public String getColumnLength(int index) {
    return columnTypeLengths.get(index);
  }
  
  public boolean columnIsPrimaryKey(int index) {
    return columnIsPrimaryKey.get(index);
  }

  public void addColumn(String columnName, String columnType, boolean isPrimaryKey) {
    addColumn(columnName, columnType, null, isPrimaryKey);
  }

  /** Add a column to the table
   * 
   * @param columnName
   * @param columnType
   * @param length
   * @param isPrimaryKey
   */
  public void addColumn(String columnName, String columnType, String length, boolean isPrimaryKey) {
    if ((columnName == null) || (columnType == null)) {
      throw new TestException("columnName " + columnName + " and/or columnType " + columnType + " is null"); 
    }
    columnNames.add(columnName);
    columnTypes.add(columnType);
    columnTypeLengths.add(length);
    columnIsPrimaryKey.add(isPrimaryKey);
  }

  /** Return a create table statement to create the table
   * 
   * @return A String that can be executed to create the table.
   */
  public String getCreateTableStatement() {
    StringBuilder stmt = new StringBuilder();
    stmt.append("CREATE TABLE " + getSchemaName() + "." + getTableName() + " (");
    int numColumns = getNumColumns();
    for (int i = 0; i < numColumns; i++) {
      stmt.append(getColumnName(i) + " " + getColumnType(i));
      String length = getColumnLength(i);
      if (length != null) {
        stmt.append("(" + length + ")");
      }
      if (i != numColumns-1) {
        stmt.append(", ");
      }
    }
    StringBuilder pkClause = new StringBuilder();
    for (int i = 0; i < numColumns; i++) {
       if (columnIsPrimaryKey(i)) {
         if (pkClause.length() > 0) {
           pkClause.append(", ");
         }
         pkClause.append(getColumnName(i));
       }
    }
    if (pkClause.length() > 0) {
      pkClause.insert(0, ", PRIMARY KEY (");
      pkClause.append(")");
      stmt.append(pkClause);
    }
    stmt.append(")");
    return stmt.toString();
  }
}
