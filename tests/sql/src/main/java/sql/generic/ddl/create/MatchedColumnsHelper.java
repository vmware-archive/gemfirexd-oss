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
package sql.generic.ddl.create;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public class MatchedColumnsHelper {
  Random random = SQLOldTest.random;

  public List<ColumnInfo> getNonClobBlobXmlTypeColumns(TableInfo table,
      int numCols) {
    List<ColumnInfo> columns = table.getColumnList();
    int colCnt = columns.size();
    List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < numCols; i++) {
      ColumnInfo col = null;
      boolean f = false;
      for (int j = 0; j < 10 && !f; j++) {
        col = columns.get(random.nextInt(colCnt));
        if (col.getColumnType() != Types.BLOB
            && col.getColumnType() != Types.CLOB
            && col.getColumnType() != Types.SQLXML
            && !partColumns.contains(col)) {
          f = true;
        }
      }
      if (f)
        partColumns.add(col);
    }
    return partColumns;
  }

  public List<ColumnInfo> getNumericTypeColumns(TableInfo table, int numCols) {
    List<ColumnInfo> columns = table.getColumnList();
    int colCnt = columns.size();
    List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < numCols; i++) {
      ColumnInfo col = null;
      boolean f = false;
      for (int j = 0; j < 10 && !f; j++) {
        col = columns.get(random.nextInt(colCnt));
        if ((col.getColumnType() == Types.INTEGER
            || col.getColumnType() == Types.DOUBLE
            || col.getColumnType() == Types.FLOAT
            || col.getColumnType() == Types.SMALLINT
            || col.getColumnType() == Types.BIGINT
            || col.getColumnType() == Types.DECIMAL
            || col.getColumnType() == Types.NUMERIC || col.getColumnType() == Types.REAL)
            && !partColumns.contains(col)) {
          f = true;
        }
      }
      if (f)
        partColumns.add(col);
    }
    return partColumns;
  }

  public List<ColumnInfo> getStringTypeColumns(TableInfo table, int numCols) {
    List<ColumnInfo> columns = table.getColumnList();
    int colCnt = columns.size();
    List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < numCols; i++) {
      ColumnInfo col = null;
      boolean f = false;
      for (int j = 0; j < 10 && !f; j++) {
        col = columns.get(random.nextInt(colCnt));
        if ((col.getColumnType() == Types.CHAR
            || col.getColumnType() == Types.LONGNVARCHAR || col.getColumnType() == Types.VARCHAR)
            && !partColumns.contains(col)) {
          f = true;
        }
      }
      if (f)
        partColumns.add(col);
    }
    return partColumns;
  }

  public List<ColumnInfo> getNumericOrStringTypeColumns(TableInfo table,
      int numCols) {
    List<ColumnInfo> columns = table.getColumnList();
    int colCnt = columns.size();
    List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < numCols; i++) {
      ColumnInfo col = null;
      boolean f = false;
      for (int j = 0; j < 10 && !f; j++) {
        col = columns.get(random.nextInt(colCnt));
        if ((col.getColumnType() == Types.INTEGER
            || col.getColumnType() == Types.DOUBLE
            || col.getColumnType() == Types.FLOAT
            || col.getColumnType() == Types.SMALLINT
            || col.getColumnType() == Types.BIGINT
            || col.getColumnType() == Types.DECIMAL
            || col.getColumnType() == Types.NUMERIC
            || col.getColumnType() == Types.REAL
            || col.getColumnType() == Types.CHAR
            || col.getColumnType() == Types.LONGNVARCHAR || col.getColumnType() == Types.VARCHAR)
            && !partColumns.contains(col)) {
          f = true;
        }
      }
      if (f)
        partColumns.add(col);
    }
    return partColumns;
  }

  public List<ColumnInfo> getDateTypeColumns(TableInfo table, int numCols) {
    List<ColumnInfo> columns = table.getColumnList();
    int colCnt = columns.size();
    List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < numCols; i++) {
      ColumnInfo col = null;
      boolean f = false;
      for (int j = 0; j < 10 && !f; j++) {
        col = columns.get(random.nextInt(colCnt));
        if ((col.getColumnType() == Types.DATE
            || col.getColumnType() == Types.TIME || col.getColumnType() == Types.TIMESTAMP)
            && !partColumns.contains(col)) {
          f = true;
        }
      }
      if (f)
        partColumns.add(col);
    }
    return partColumns;
  }
}
