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

import sql.generic.GenericBBHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.FKConstraint;
import sql.generic.ddl.PKConstraint;
import sql.generic.ddl.TableInfo;

public class DefaultPartitionClause extends AbstractPartitionClause {
  
  public boolean generatePartitionClause(TableInfo table) {
    boolean isDefaultColocation = false;
    // if fk parent is partitioned on all its primary keys and they are all are
    // referenced by child
    // then child is partitioned on foreign keys
    List<FKConstraint> fkConstraints = table.getForeignKeys();
    for (FKConstraint c : fkConstraints) {

      List<ColumnInfo> fkColumns = c.getColumns();
      List<ColumnInfo> refColumns = c.getParentColumns();

      String parentTable = c.getParentTable();
      TableInfo parent = GenericBBHelper.getTableInfo(parentTable);
      List<ColumnInfo> parentPrimaryList = parent.getPrimaryKey().getColumns();

      if (refColumns.containsAll(parentPrimaryList)
          && parentPrimaryList.containsAll(refColumns)) {
        List<ColumnInfo> parentPartitionedList = parent
            .getPartitioningColumns();
        if (parentPartitionedList.size() == parentPrimaryList.size()
            && parentPartitionedList.containsAll(parentPrimaryList)) {
          partColumns.addAll(fkColumns);
          isDefaultColocation = true;
          break;
        }
      }
    }

    if (!isDefaultColocation) {
      PKConstraint pk = table.getPrimaryKey();
      if (pk!=null){
        List<ColumnInfo> primaryColumns = pk.getColumns();
        partColumns.addAll(primaryColumns);  
      }
    }
    return true;
  }
    
  public String getParitionClause(){
    return "";
  }
}
