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
package sql.generic.ddl.alter;

import java.util.ArrayList;
import java.util.List;

import sql.generic.ddl.Constraint;
import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;
import sql.generic.ddl.UniqueConstraint;

public class AlterUniqueOperation extends AlterConstraintOperation {

  AlterUniqueOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
    this.constraintType = Constraint.ConstraintType.UNIQUE;
  }
  
  public AlterUniqueOperation parse(){
    switch (action) {
      case ADD :
        addUnique();
        break;
      case DROP:
        dropConstraint();
        break;
     }
    return this;
  }
  
  protected  void addUnique (){
    this.tableInfo = getTableInfoForAddConstraint(); 
    constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
    List<ColumnInfo> uniqColumns = getColumnForAddUnique();
    for (ColumnInfo column:uniqColumns) {
      if (columnName.length() == 0)
        columnName = column.getColumnName();
      else
        columnName = columnName + "," + column.getColumnName();
    }
   
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), constraintName,columnName);
    
  }
	  
  protected List<ColumnInfo> getColumnForAddUnique() {
    List<ColumnInfo> columns = new ArrayList<ColumnInfo>(tableInfo.getColumnList());
    List<ColumnInfo> uniqColumns = new ArrayList<ColumnInfo>();
    int numColumnsToAdd = SQLOldTest.random.nextInt(Math.min(2,columns.size())) + 1;
    for (int i = 0; i < numColumnsToAdd; i++) {
      ColumnInfo c = columns.get(SQLOldTest.random.nextInt(columns.size()));
      uniqColumns.add(c);
      columns.remove(c);
    }
    return uniqColumns;
  }
  
  protected TableInfo getTableInfoForDropConstraint() { 
    tablesWithUniqueConstraint = new ArrayList<String>(constraintHolder.getTablesWithUniqConstraint());
    if (tablesWithUniqueConstraint.size() == 0)
      return null;
    tableName = tablesWithUniqueConstraint.get(SQLOldTest.random.nextInt(tablesWithUniqueConstraint.size()));
    return GenericBBHelper.getTableInfo(tableName);
  }
  
  protected UniqueConstraint getConstraint(){
    uniqueConstraints = new ArrayList<UniqueConstraint>(tableInfo.getUniqueKeys());
    return uniqueConstraints.get(SQLOldTest.random.nextInt(uniqueConstraints.size()));
  }

}
