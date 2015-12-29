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

import java.util.List;

import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public class PartitionByFunctionClause extends AbstractPartitionClause {

  @Override
  public boolean generatePartitionClause(TableInfo table) {
    //" partition by  $FUNCTION "
    //  partition by (trade.getHighPrice(histprice))
    boolean clauseFound = false;
    List<ColumnInfo> columns = matchedColumnHelper.getDateTypeColumns(table, 1);
    if(columns.size() > 0){
      clauseFound = true;
      String column = columns.get(0).getColumnName();
      String function = " (month (" + column + ")) ";
      partColumns.add(columns.get(0));
      
      //todo - for UDT, integer, double or string
      partitionClause = " PARTITION BY " + function;  
    }
    return clauseFound;
  }
}
