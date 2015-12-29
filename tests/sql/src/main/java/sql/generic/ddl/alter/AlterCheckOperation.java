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

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.CheckConstraint;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.Constraint;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;

public class AlterCheckOperation  extends AlterConstraintOperation {

  CheckConstraint checkCons;
  
  AlterCheckOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
    this.constraintType = Constraint.ConstraintType.CHECK;
  }
  
  public AlterCheckOperation parse(){
    switch (action) {
      case ADD :
    	addCheckConstraint();
        break;
      case DROP:
    	dropConstraint();
        break;
     }
    return this;
  }
  
  protected  void addCheckConstraint (){
    this.tableInfo = getTableInfoForAddConstraint(); 
    constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
    List<ColumnInfo> checkColumns =  getColumnForAddCheck();
    String definition = "";     //TODO - get regex for check
    checkCons = new CheckConstraint(tableInfo.getFullyQualifiedTableName(), constraintName, checkColumns, definition); 
    for(ColumnInfo column:checkColumns){
    	if(columnName.length()==0)
    		columnName = column.getColumnName();
    	else
    		columnName = "," + column.getColumnName();
    }
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), constraintName,columnName);
  }
  
  protected List<ColumnInfo> getColumnForAddCheck(){
    List<ColumnInfo> columns = new ArrayList<ColumnInfo>(tableInfo.getColumnList());
    List<ColumnInfo> checkColumns = new ArrayList<ColumnInfo>();
    //TODO add support for multiple columns
    int numCol = SQLOldTest.random.nextInt(1) + 1;
    for (int i = 0 ; i < numCol; i++) {
      ColumnInfo column = columns.get(SQLOldTest.random.nextInt(columns.size()));
      columns.remove(column);
      checkColumns.add(column);
    }
    return checkColumns;
}
  
  protected  TableInfo getTableInfoForDropConstraint(){ 
    tablesWithCheckConstraint =  new ArrayList<String>(constraintHolder.getTablesWithCheckConstraint()); 
    if(tablesWithCheckConstraint.size()==0)
        return null;
    tableName = tablesWithCheckConstraint.get(SQLOldTest.random.nextInt(tablesWithCheckConstraint.size())); 
    return GenericBBHelper.getTableInfo(tableName);
   }
  
  protected CheckConstraint getConstraint(){
    checkConstraints = new ArrayList<CheckConstraint>(tableInfo.getCheckConstraints());
    return checkConstraints.get(SQLOldTest.random.nextInt(checkConstraints.size()));
  }

}
