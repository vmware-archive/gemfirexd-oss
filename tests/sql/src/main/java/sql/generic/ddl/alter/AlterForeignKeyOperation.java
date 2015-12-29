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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.Constraint;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.FKConstraint;
import sql.generic.ddl.TableInfo;

public class AlterForeignKeyOperation extends AlterConstraintOperation {

  AlterForeignKeyOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
    this.constraintType = Constraint.ConstraintType.FK_CONSTRAINT;
  }
  
  public AlterForeignKeyOperation parse(){
    switch (action) {
      case ADD :
        addFKConstraint();
        break;
      case DROP:
        dropConstraint();
        break;
     }
    return this;
  }

  protected  void addFKConstraint (){
    HashMap<String, List<FKConstraint>> droppedFKMap = new HashMap<String, List<FKConstraint>>(constraintHolder.getDroppedFkList());
    if (!droppedFKMap.isEmpty()) {
      Set<String> tables = droppedFKMap.keySet();
      tableName = (String)(tables.toArray())[SQLOldTest.random.nextInt(tables.size())];
      this.tableInfo = GenericBBHelper.getTableInfo(tableName);
      List<FKConstraint> droppedFKs =  droppedFKMap.get(tableName);

      FKConstraint constraint = droppedFKs.get(SQLOldTest.random.nextInt(droppedFKs.size()));

      //get columnName list
      List<ColumnInfo> fkColumns = constraint.getColumns();
      for(ColumnInfo c : fkColumns){
        if (columnName.length() == 0)
          columnName = c.getColumnName();
        else
          columnName = columnName + "," + c.getColumnName();
      }
      List<ColumnInfo> refColumns = constraint.getParentColumns();
      String refColumn = "" ;
      for(ColumnInfo c : refColumns){
        if (refColumn.length() == 0)
          refColumn = c.getColumnName();
        else
          refColumn = refColumn + "," + c.getColumnName();
      }
      // give new constraint name.
      constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
      constraint.setConstraintName(constraintName);
      String ref = " REFERENCES " + constraint.getParentTable() + " (" + refColumn+ ") ON DELETE RESTRICT";
      replacePlaceHolders(tableInfo.getFullyQualifiedTableName(),constraintName,"(" + columnName + ")" + ref);
    } else {
      Log.getLogWriter().info("Did not get FKs to be added, continuing the test...");
      executeDDL = false;
    }
  }
          
  protected  TableInfo getTableInfoForDropConstraint(){ 
    tablesWithFKConstraint = new ArrayList<String>(constraintHolder.getTablesWithFKConstraint());
    if(tablesWithFKConstraint.size()==0)
      return null;
    tableName = tablesWithFKConstraint.get(SQLOldTest.random.nextInt(tablesWithFKConstraint.size())); 
    return GenericBBHelper.getTableInfo(tableName);
  }
  
  protected FKConstraint getConstraint(){
    fkConstraints = new ArrayList<FKConstraint>(tableInfo.getForeignKeys());
    return fkConstraints.get(SQLOldTest.random.nextInt(fkConstraints.size()));
  }

}
