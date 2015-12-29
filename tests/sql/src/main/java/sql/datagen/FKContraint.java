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
package sql.datagen;

/**
 * 
 * @author Rahul Diyewar
 */

public class FKContraint {
  private String tableName;
  private String columnName;
  private String parentTable;
  private String parentColumn;

  public FKContraint(String table, String col, String parentTable,
      String parentCol) {
    this.tableName = table;
    this.columnName = col;
    this.parentTable = parentTable;
    this.parentColumn = parentCol;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getParentColumn() {
    return parentColumn;
  }

  public String getParentTable() {
    return parentTable;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "FK_" + tableName + "." + columnName + "_ON_" + parentTable + "."
        + parentColumn;
  }
}
