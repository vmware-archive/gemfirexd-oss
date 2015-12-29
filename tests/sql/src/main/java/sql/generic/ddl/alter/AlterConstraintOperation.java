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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.CheckConstraint;
import sql.generic.ddl.Constraint;
import sql.generic.ddl.Constraint.ConstraintType;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.FKConstraint;
import sql.generic.ddl.PKConstraint;
import sql.generic.ddl.TableInfo;
import sql.generic.ddl.UniqueConstraint;

public class AlterConstraintOperation extends Operation {
  
  ConstraintType constraintType;

  List<String> tablesWithCheckConstraint;
  List<String> tablesWithFKConstraint;
  List<String> tablesWithPKConstraint;
  List<String> tablesWithUniqueConstraint;
  
  List<CheckConstraint> checkConstraints;
  List<FKConstraint> fkConstraints;
  PKConstraint pkConstraint;
  List<UniqueConstraint> uniqueConstraints;
  Constraint dropConstraint;
  
  AlterConstraintOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
  }
  
  public AlterConstraintOperation parse(){
    switch (action) {
      case ADD : // TODO - can be removed or made generic.
        addNewConstraint();
        break;
      case DROP:
        dropConstraint();
        break;
     }
    return this;
  }
  
  public void handleAlterDDLException (SQLException  derbyException , SQLException gemxdException){
    Log.getLogWriter().info("Derby Exception is : " + derbyException);  
    Log.getLogWriter().info("GFXD Exception is : " + gemxdException);  
    if(gemxdException!=null){
      if (action == DDLAction.DROP && gemxdException.getSQLState().equals("0A000") 
          && dropConstraint.getConstraintType()==Constraint.ConstraintType.PK_CONSTRAINT) {
          // gfxd failed - PK constraint could not be dropped
          Log.getLogWriter().info(
              "gfxd failed - PK constraint could not be dropped"); 
          if(derbyException==null)
            executor.rollbackDerby(); 
          return;
        }
    }
    if (derbyException != null && gemxdException == null) {
      if (!isFixed51273 && derbyException.getSQLState().equals("23505")
          && this.action == DDLAction.ADD) {
        Log.getLogWriter().info("Got bug #51273 - Unique constraint failed to add in derby but successful in gfxd.");
        executor.rollbackGfxd();
        return;
      }
    }
    compareExceptions(derbyException,gemxdException);
  }
  
  public void handleAlterDDLException (SQLException gemxd){ 
    Log.getLogWriter().info("GFXD Exception is : " + gemxd);      
  }
  
  protected  void addNewConstraint(){
    tableInfo = getTableInfoForAddConstraint();
    constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), constraintName,"");
  }
  
  protected void dropConstraint() {
    this.tableInfo = getTableInfoForDropConstraint();
    if (tableInfo == null) {
      // Did not get table with any constraint, continue the test to perform negative testing on false constraint.
      Log.getLogWriter().info(
          "Did not find any table with required constraint, continuing with random table. ");
      this.tableInfo = getTableInfoForAddConstraint();
      constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
    }
    else {
      dropConstraint = getConstraint();
      constraintName = dropConstraint.getConstraintName();
    }
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), constraintName, "");
  }
  
  protected Constraint getConstraint(){
    List<Constraint> tableConstraints = new ArrayList<Constraint>();
    checkConstraints = new ArrayList<CheckConstraint>(tableInfo.getCheckConstraints());
    fkConstraints = new ArrayList<FKConstraint>(tableInfo.getForeignKeys());
    pkConstraint = tableInfo.getPrimaryKey();
    uniqueConstraints = new ArrayList<UniqueConstraint>(tableInfo.getUniqueKeys());
    tableConstraints.addAll(checkConstraints);
    tableConstraints.add(pkConstraint);
    tableConstraints.addAll(fkConstraints);
    tableConstraints.addAll(uniqueConstraints);
    Constraint constraint = tableConstraints.get(SQLOldTest.random.nextInt(tableConstraints.size()));
    constraintType = constraint.getConstraintType();
    return  constraint; 
  }
          
  protected  TableInfo getTableInfoForDropConstraint(){
    List<String> tables = new ArrayList<String>();
    tablesWithFKConstraint = new ArrayList<String>(constraintHolder.getTablesWithFKConstraint());
    tablesWithUniqueConstraint = new ArrayList<String>(constraintHolder.getTablesWithUniqConstraint());
    tablesWithCheckConstraint = new ArrayList<String>(constraintHolder.getTablesWithCheckConstraint());
    tablesWithPKConstraint = new ArrayList<String>(constraintHolder.getTablesWithPKConstraint());
    tables.addAll(tablesWithFKConstraint);
    tables.addAll(tablesWithUniqueConstraint);
    tables.addAll(tablesWithCheckConstraint);
    tables.addAll(tablesWithPKConstraint);
    if(tables.size()==0)
        return null;
    tableName = tables.get(SQLOldTest.random.nextInt(tables.size())); 
    return GenericBBHelper.getTableInfo(tableName);
  }
  
  // get random table to add any constraint
  protected  TableInfo getTableInfoForAddConstraint(){
    List<String> tables = constraintHolder.getTablesList();
    tableName = tables.get(SQLOldTest.random.nextInt(tables.size()));
    return GenericBBHelper.getTableInfo(tableName);
  }

}
