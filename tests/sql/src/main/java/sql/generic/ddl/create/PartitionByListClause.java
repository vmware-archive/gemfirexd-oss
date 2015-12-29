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

public class PartitionByListClause extends AbstractPartitionClause {

  @Override
  public boolean generatePartitionClause(TableInfo table) {
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
      String quot = isOfNumberType(col) ? "" : "'";
      int valuesCount = vals.size();
      int groups;
      if (valuesCount == 1 || valuesCount == 2){
        groups = valuesCount;
      }else{
        groups = random.nextInt(1 + valuesCount/3) + 1; 
      }
      int diff = valuesCount / groups;

      Log.getLogWriter().info("Partition by list for "
          + table.getFullyQualifiedTableName() + " on column "
          + col.getColumnName() + ", groups=" + groups
          + " ColumnValuesCount=" + valuesCount);

      int _min = 0;
      int _max = 0;
      for (int i = 0; i < groups; i++) {
        String v = "VALUES (";
        _min = _max;
        _max = _min + random.nextInt(diff) + 1;
        if (_max > valuesCount || i == groups - 1)
          _max = valuesCount;

        for (int j = _min; j < _max; j++) {
          v += quot + vals.get(j) + quot;
          if (j != _max - 1)
            v += ", ";
        }
        v += ")";
        valuesstr += v;
        if (i != groups - 1)
          valuesstr += ", ";
      }
      partitionClause = availablePartitionClause[3] + "(" + column + ")"
          + " (" + valuesstr + ")";
    }
    return clauseFound;
  }

}
