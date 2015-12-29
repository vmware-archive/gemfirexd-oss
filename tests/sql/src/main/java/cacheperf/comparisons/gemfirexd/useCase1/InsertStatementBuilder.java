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
package cacheperf.comparisons.gemfirexd.useCase1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * A tool for building simple insert prepared statements.
 */
public class InsertStatementBuilder {

  private String tableName;

  private LinkedHashMap<String, Object> fields = new LinkedHashMap<String, Object>();

  public String getSql() {
    StringBuilder sql = new StringBuilder(200);
    StringBuilder var = new StringBuilder(30);
    sql.append("insert into ").append("SEC_OWNER.").append(tableName).append(" (");
    for (Iterator<String> fieldIter = fields.keySet().iterator(); fieldIter.hasNext();) {
      sql.append(fieldIter.next());
      if (fieldIter.hasNext()) {
        sql.append(", ");
        var.append("?,");
      } else {
        var.append("?");
      }
    }
    sql.append(") values (").append(var).append(")");
    return sql.toString();
  }

  public Object[] getSqlValues() {
    ArrayList<Object> array = new ArrayList<Object>();
    for (Iterator<String> fieldIter = fields.keySet().iterator(); fieldIter.hasNext();) {
      String fieldName = fieldIter.next();
      array.add(fields.get(fieldName));
    }
    return array.toArray(new Object[] {});
  }

  public InsertStatementBuilder insertInto(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public InsertStatementBuilder set(String columnName, Object val) {
    if (null != columnName && columnName.length() > 0 && null != val) {
      // if (val instanceof String || val instanceof Number || val
      // instanceof Date) {
      fields.put(columnName, val);
      // } else {
      // throw new
      // UnsupportedDataTypeException(val.getClass().toString());
      // }
    }
    return this;
  }
}
