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
package sql.generic.dmlstatements;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import sql.generic.SqlUtilityHelper;

public class DBRow {

  private List<Column> columns = new LinkedList<Column>();

  private Map<String, Column> columnMap = new HashMap<String, Column>();

  String tableName;

  public DBRow(String tableName) {
    // to give fully qualified name
    this.tableName = tableName + ".";
  }

  public DBRow() {
    this.tableName = "";
  }

  public void addColumn(String name, int type, Object value) {
    if (!name.contains(tableName)) {
      name = tableName + name;
    }
    Column column = new Column(name, type, value);
    columns.add(column);
    columnMap.put(name, column);

  }

  public static class Column {

    private String name;

    private String shortName;

    private int type;

    private Object value;

    private int specialType;

    public Column(String name, int type, Object value) {
      this.name = name;
      if (name != null)
        this.shortName = SqlUtilityHelper.getColumnName(name, false);
      this.type = type;
      this.value = value;
    }

    public Column(String name, int type, Object value, int specialType) {
      this.name = name;
      if (name != null)
        this.shortName = SqlUtilityHelper.getColumnName(name, false);
      this.type = type;
      this.value = value;
      this.specialType = specialType;
    }

    public String getName() {
      return name;
    }

    public String getShortName() {
      return shortName;
    }

    public Object getValue() {
      return value;
    }

    public int getType() {
      return type;
    }

    public int getSpecialType() {
      return specialType;
    }

  }

  public Collection<Column> getColumns() {
    return Collections.unmodifiableCollection(columns);
  }

  public Column find(String column) {
    if (!column.contains(tableName)) {
      column = tableName + column;
    }
    return columnMap.get(column);
  }
}
