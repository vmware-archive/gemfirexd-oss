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

import hydra.Log;

import java.util.List;

import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public class PartitionByRangeClause extends AbstractPartitionClause {

  @Override
  public boolean generatePartitionClause(TableInfo table) {
    // " partition by range (sid) ( VALUES BETWEEN 0 AND 409, VALUES BETWEEN 409 AND 1102, VALUES BETWEEN 1102 AND 1251) "
    // " partition by range (bid) (VALUES BETWEEN 0.0 AND 25.0,  VALUES BETWEEN 25.0  AND 35.0) "
    boolean clauseFound = false;
    String valuesstr = "";
    List<ColumnInfo> columns = matchedColumnHelper.getNumericOrStringTypeColumns(table, 1);
    String column = "";
    if (columns.size() == 1) {
      clauseFound = true;
      ColumnInfo col = columns.get(0);
      column = col.getColumnName();
      partColumns.add(col);
      List<Object> vals = col.getValueList();
      int valuesCount = vals.size();
      int groups;
      if (valuesCount == 1 || valuesCount == 2){
        groups = 1;
      }else{
        groups = random.nextInt(1 + valuesCount/3) + 1; 
      }
      int diff = valuesCount / groups;

      Log.getLogWriter().info("Partition by range for "
          + table.getFullyQualifiedTableName() + " on column "
          + col.getColumnName() + ", groups=" + groups
          + " ColumnValuesCount=" + valuesCount);

      String quot = isOfNumberType(col) ? "" : "'";

      for (int i = 0; i < groups; i++) {
        int _min = diff * i;
        int _max = _min + diff;
        if (_max > valuesCount || i == groups - 1)
          _max = valuesCount - 1;
        String v = "VALUES BETWEEN " + quot + vals.get(_min) + quot
            + " AND " + quot + vals.get(_max) + quot;
        valuesstr += v;
        if (i != groups - 1)
          valuesstr += ", ";
      }
      partitionClause = availablePartitionClause[4] + "(" + column + ")"
          + " (" + valuesstr + ")";
    }

    return clauseFound;
  }

}
