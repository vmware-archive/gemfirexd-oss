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

public class PartitionByColumnsClause extends AbstractPartitionClause {

  @Override
  public boolean generatePartitionClause(TableInfo table) {
    //   " partition by column (bid) ",
    //   " partition by column (ordertime, status) ",
    // does not supported CLOB,BLOB and XML columns
    // UDT columns is supported
    boolean clauseFound = false;
    String columnsStr = "";
    
    int totalPartitionColumn;
    int f = random.nextInt(10);
    if ((Integer) f / 5 == 0) {
      totalPartitionColumn = 1; // select 1 column 50% time
    } else if ((Integer) f / 8 == 0) {
      totalPartitionColumn = 2; // select 2 columns for rest 30% times
    } else {
      totalPartitionColumn = 3; //select 3 columns for rest 20% times
    }
    
    List<ColumnInfo> columns = matchedColumnHelper.getNonClobBlobXmlTypeColumns(table, totalPartitionColumn);
         
    for (int i=0; i<columns.size(); i++){
      ColumnInfo col = columns.get(i);
      partColumns.add(col);
      columnsStr += col.getColumnName();
      if(i != totalPartitionColumn-1) columnsStr+=", ";
      clauseFound = true;
    }

    partitionClause = " " + availablePartitionClause[2] + " (" + columnsStr + ") ";

    return clauseFound;
  }

}
