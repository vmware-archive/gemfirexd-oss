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

import hydra.Log;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.Constraint;
import sql.generic.ddl.PKConstraint;
import sql.generic.ddl.TableInfo;
import sql.generic.ddl.UniqueConstraint;
import sql.generic.ddl.Constraint.ConstraintType;
import sql.generic.ddl.ConstraintInfoHolder;

public class ColumnDefinition{
  StringBuilder columnDef = new StringBuilder();
  TableInfo tableInfo;
  String columnName;
  DataType type;
  String checkDef;
  ConstraintInfoHolder constraintHolder = GenericBBHelper.getConstraintInfoHolder();
  
  boolean defaultDefInConstraint = false;
  
  ConstraintType constraintType;
  String constraintName ="";
  
  public ColumnDefinition(TableInfo tableInfo, String columnName, DataType type) {
    this.tableInfo = tableInfo;
    this.columnName = columnName;
    this.type = type;
  }

  public String getDefinition() {
    if(SQLOldTest.random.nextInt() % 2 == 0)
      columnDef.append(getConstraints());
    if(columnDef.indexOf("PRIMARY KEY") != -1)
      columnDef.append(getDefaultDefinition());
    if ((type == DataType.BIGINT || type == DataType.INTEGER) && (SQLOldTest.random.nextInt() % 2 == 0)) {
      columnDef.append(getGeneratedDefaultString());
    } else if (getConstantExpressionBasedOnType() != null && (SQLOldTest.random.nextInt() % 2 == 0)) {
      columnDef.append(getDefaultDefinition());
    }
    return columnDef.toString();
  }

  // generate [ GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( START WITH integer-constant [, INCREMENT BY integer-constant ] ) ] ] syntax
  private String getGeneratedDefaultString() {
    StringBuilder generatedDefault = new StringBuilder("GENERATED ");
    if (SQLOldTest.random.nextInt() % 2 == 0)
      generatedDefault.append(" ALWAYS  ");
    else
      generatedDefault.append(" BY DEFAULT ");

    generatedDefault.append(" AS IDENTITY ");

    if (SQLOldTest.random.nextInt() % 2 == 0) {
      generatedDefault.append(" (START WITH ").append(
          SQLOldTest.random.nextInt(1) + 1);
      if (SQLOldTest.random.nextInt() % 2 == 0)
        generatedDefault.append(" , INCREMENTED BY ").append(
            SQLOldTest.random.nextInt(1) + 1);
      generatedDefault.append(" ) ");
    }
    Log.getLogWriter().info(
        " generatedDefaultString is " + generatedDefault.toString());
    return generatedDefault.toString();

  }

  // generate [NOT NULL] [ WITH ] DEFAULT { constant-expression | NULL } syntax
  private String getDefaultDefinition() {
    StringBuilder defaultDef = new StringBuilder();
    if (!defaultDefInConstraint) {
      defaultDefInConstraint = true;
      if (SQLOldTest.random.nextInt() % 2 == 0
          || columnDef.indexOf("PRIMARY KEY") != -1)
        defaultDef.append(" NOT NULL");
      if (SQLOldTest.random.nextInt() % 2 == 0)
        defaultDef.append(" WITH ");
      defaultDef.append(" DEFAULT ");
      if (SQLOldTest.random.nextInt() % 2 == 0
          || defaultDef.indexOf("NOT NULL") != -1) {
        defaultDef.append(getConstantExpressionBasedOnType());
      } else {
        defaultDef.append(" NULL ");
      }
      Log.getLogWriter().info(
          " final column default definition is " + defaultDef.toString());
    }
    return defaultDef.toString();
  }

  //TODO - need to have handling for other dataTypes currently it is only for numeric.
  private String getConstantExpressionBasedOnType() {
    switch (type) {
      case BIGINT:
        return ((Long)SQLOldTest.random.nextLong()).toString();
      case DECIMAL:
        return ((Float)SQLOldTest.random.nextFloat()).toString();
      case DOUBLE:
        return ((Double)SQLOldTest.random.nextDouble()).toString();
      case FLOAT:
        return ((Float)SQLOldTest.random.nextFloat()).toString();
      case INTEGER:
        return ((Integer)SQLOldTest.random.nextInt()).toString();
      case NUMERIC:
        return ((Integer)SQLOldTest.random.nextInt()).toString();
      case REAL:
        return ((Double)SQLOldTest.random.nextDouble()).toString();
      case SMALLINT:
        return ((Integer)SQLOldTest.random.nextInt()).toString();
    }
    return null;
  }

  private String getConstraints() {
    StringBuilder constraintDef = new StringBuilder();
    constraintType = ConstraintType.values()[SQLOldTest.random.nextInt(ConstraintType.values().length)];
    
    if(SQLOldTest.random.nextInt() % 2 == 0){
      constraintName = AlterUtilityHelper.getRandomConstraintName(tableInfo.getTableName(),constraintType);
      constraintDef.append(" CONSTRAINT ").append(constraintName);
    }
    
    switch(constraintType) {
      case FK_CONSTRAINT : 
        String refTableName = getParentTable();
        TableInfo refTable = GenericBBHelper.getTableInfo(refTableName);
        String refColumn = getRefColumn(refTable);
        constraintDef.append(" REFERCENCES ").append(refTableName).append(" (").append(refColumn).append(")");
        break;
      case CHECK : //TODO - get logical regex
        constraintDef.append(" CHECK ").append(" (").append(columnName).append(getRegularExpr()).append(")");
        break;
      case PK_CONSTRAINT: 
        constraintDef.append(" PRIMARY KEY ");
        break;
      case UNIQUE : 
        constraintDef.append(" UNIQUE ");
      break;
    }
    Log.getLogWriter().info(
        " generatedConstraintString is " + constraintDef.toString());
    return constraintDef.toString();
  }

  private String getRefColumn(TableInfo refTable) {
    String refColumnName = "";
    List<Constraint> tableConstraints = new ArrayList<Constraint>();
    PKConstraint pkConstraint = refTable.getPrimaryKey();
    List<UniqueConstraint> uniqueConstraints = new ArrayList<UniqueConstraint>(refTable.getUniqueKeys());
    tableConstraints.add(pkConstraint);
    tableConstraints.addAll(uniqueConstraints);
    Constraint constraint = tableConstraints.get(SQLOldTest.random.nextInt(tableConstraints.size()));
    List<ColumnInfo> uniqOrPKColumns = constraint.getColumns();
    for(ColumnInfo column:uniqOrPKColumns){
      if(refColumnName.length()==0)
              refColumnName = column.getColumnName();
      else
              refColumnName = "," + column.getColumnName();
  }
    return refColumnName;
  }

  private String getParentTable() {
    List<String> tableList = new ArrayList<String>();
    List<String> pkTables = new ArrayList<String>(constraintHolder.getTablesWithPKConstraint());
    List<String> uniqTables = new ArrayList<String>(constraintHolder.getTablesWithUniqConstraint());
    tableList.addAll(pkTables);
    tableList.addAll(uniqTables);
    return tableList.get(SQLOldTest.random.nextInt(tableList.size()));
  }

  private String getRegularExpr() {
    return "";
  }

}
