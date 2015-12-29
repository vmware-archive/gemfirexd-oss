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

import hydra.Log;

import java.util.ArrayList;
import java.util.List;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.Constraint;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.PKConstraint;
import sql.generic.ddl.TableInfo;

/*
 * Supports ::
 * "ALTER TABLE [table-name] ADD CONSTRAINT [constraint-name] PRIMARY KEY [column-name]"
 * "ALTER TABLE [table-name] ADD PRIMARY KEY [column-name]"
 * "ALTER TABLE [table-name] ADD [PRIMARY-KEY]"
 * "ALTER TABLE [table-name] DROP PRIMARY KEY"
 */

public class AlterPrimaryKeyOperation  extends AlterConstraintOperation {

  final String CONSTRAINT = " CONSTRAINT ";
  final String PRIMARYKEY = " PRIMARY KEY ";
  final String keyToReplace = "[PRIMARY-KEY]";
  boolean falseTesting = false;
  AlterPrimaryKeyOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
    this.constraintType = Constraint.ConstraintType.PK_CONSTRAINT;
  }
  
  public AlterPrimaryKeyOperation parse(){   
    switch (action) {
      case ADD :
        addPK();
        break;
      case DROP:
        dropConstraint();
        break;
      default : 
     }
    return this;
  }
  
  protected  void addPK (){
    this.tableInfo = getTableInfoForAddPK();
    constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
    List<ColumnInfo> pkColumns = getPKColumn();
    for (ColumnInfo column : pkColumns) {
      if (columnName.length() == 0)
        columnName = column.getColumnName();
      else
        columnName = columnName + "," + column.getColumnName();
    }
    String replacement = "";
    if(ddl.contains(AlterKeywordParser.PRIMARY_KEY.getKeyword())){
      if(SQLOldTest.random.nextInt()%2==0)
        replacement = CONSTRAINT + constraintName;
      replacement = replacement + PRIMARYKEY + "(" + columnName + ")";
      parsedDDL  = parsedDDL.replace(keyToReplace, replacement);
      replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), "","");
    } else {
      replacePlaceHolders(tableInfo.getFullyQualifiedTableName(),constraintName,columnName);
    }
  }

  protected  TableInfo getTableInfoForAddPK(){
    String tableName;
    List<String> pkTables = new ArrayList<String>(constraintHolder.getTablesWithPKConstraint());
    List<String> tables = new ArrayList<String>(constraintHolder.getTablesList());
    List<String> nonPkTables = new ArrayList<String>();
    nonPkTables.addAll(tables);
    nonPkTables.removeAll(pkTables);
    if(SQLOldTest.random.nextInt() % 20 != 0 && nonPkTables.size()!=0){
      tableName = nonPkTables.get(SQLOldTest.random.nextInt(nonPkTables.size()));
      return GenericBBHelper.getTableInfo(tableName);
    } else { //Negative testing for ADD PK
      falseTesting = true;
      tableName = tables.get(SQLOldTest.random.nextInt(tables.size()));
      return GenericBBHelper.getTableInfo(tableName);
    }
  }

  protected List<ColumnInfo> getPKColumn() {
    List<ColumnInfo> columns = new ArrayList<ColumnInfo>(tableInfo.getColumnList());
    List<ColumnInfo> possColumnsForPK = new ArrayList<ColumnInfo>();
    List<ColumnInfo> pkColumns = new ArrayList<ColumnInfo>();
    for (ColumnInfo col : columns) {
      if (!col.isNull())
        possColumnsForPK.add(col);
    }
    if(possColumnsForPK == null || possColumnsForPK.size()!=0 ){
      int numColumnsToAdd = SQLOldTest.random.nextInt(Math.min(3,possColumnsForPK.size())) + 1;
      for (int i = 0; i < numColumnsToAdd; i++) {
        ColumnInfo c = possColumnsForPK.get(SQLOldTest.random.nextInt(possColumnsForPK.size()));
        possColumnsForPK.remove(c);
        pkColumns.add(c);
      }
    } else { //Negative testing for newPK for null column
        Log.getLogWriter().info("DDL : Did not any column for new PK... ");
        ColumnInfo col = columns.get(SQLOldTest.random.nextInt(columns.size()));
        pkColumns.add(col);
    }
    return pkColumns;
  }  

  protected  TableInfo getTableInfoForDropConstraint(){
    tablesWithPKConstraint = new ArrayList<String>(constraintHolder.getTablesWithPKConstraint());
    if(tablesWithPKConstraint.size()==0 )
       return null;
    tableName = tablesWithPKConstraint.get(SQLOldTest.random.nextInt(tablesWithPKConstraint.size()));
    return GenericBBHelper.getTableInfo(tableName);
   }
  
  protected PKConstraint getConstraint(){
    return tableInfo.getPrimaryKey(); 
  }

}
