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

import sql.SQLTest;
import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;
import util.TestException;
import util.TestHelper;

public class AlterColumnOperation extends Operation {

  public enum AlterColumn {
    DEFAULT(" default "),DATATYPE (" data type "), SETNULL (" null"), ALWAYS (" always ");
    String opType;
    AlterColumn(String opType) {
      this.opType = opType;
    }
    
    public String getOpType (){
      return opType.toUpperCase();
    }
    
    public static AlterColumn getOperation(String ddl) {
      if (ddl.contains(ALWAYS.getOpType())) {
        return ALWAYS;
      }else if (ddl.contains(DEFAULT.getOpType())) {
        return DEFAULT;
      } else if (ddl.contains(DATATYPE.getOpType())) {
        return DATATYPE;
      } else if (ddl.contains(SETNULL.getOpType())) {
        return SETNULL;
      } else return null;
    }
  }
  
  DataType dataType;
  ColumnInfo column;
  final String defaultToReplace = "[DEFAULT]";
  final String dataTypeToReplace = "[DATA-TYPE]"; 
  
  List<ColumnInfo> intNotNullColumns = new ArrayList<ColumnInfo>();
  List<ColumnInfo> intColumns = new ArrayList<ColumnInfo>();
  List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
  
  AlterColumn setOp;
  String columnDefinition;
  
  public AlterColumnOperation(String ddl, Executor executor, DDLAction action) {
    super(ddl, executor, action);
    this.tableInfo = getTableInfo();
  }

  public AlterColumnOperation parse() {
    switch (action) {
      case ADD:
        addNewColumn();
        break;
      case DROP:
        dropColumn();
        break;
      case SET:
        setColumn();
        break;
    }
    return this;
  }

  protected TableInfo getTableInfo() {
    int tableLoc = SQLOldTest.random.nextInt(constraintHolder.getTablesList().size());
     tableName = constraintHolder.getTablesList().get(tableLoc);
     return GenericBBHelper.getTableInfo(tableName);
  }

  protected void addNewColumn() {
    executeDDL = false;
    columnName = AlterUtilityHelper.getGenericColumnName(tableInfo
        .getFullyQualifiedTableName());
    dataType = DataType.getRandomDataType();
    if(SQLOldTest.random.nextInt() % 3 == 0){
      columnDefinition = getNewColumnDefinition();
    }
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), "", columnName + " " + dataType.name() + " " + columnDefinition);
  }

  protected String getNewColumnDefinition() {
    return new ColumnDefinition(tableInfo,columnName, dataType).getDefinition();
  }
  
  protected void dropColumn() {
    List<ColumnInfo> columns = new ArrayList<ColumnInfo>(tableInfo.getColumnList());
    column = columns.get(SQLOldTest.random.nextInt(columns.size()));
    columnName = column.getColumnName();
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), "", columnName);
  }

  public void setColumn() {
    columns = new ArrayList<ColumnInfo>(tableInfo.getColumnList());
    setOp = AlterColumn.getOperation(ddl);
    
    switch (setOp) {
      case ALWAYS:
        getIntegerTypeColumns();
        if (intNotNullColumns.size() != 0)
          column = intNotNullColumns.get(SQLOldTest.random.nextInt(intNotNullColumns.size()));
        else if (intColumns.size() != 0) // Negative testing coverage nullable column
          column = intColumns.get(SQLOldTest.random.nextInt(intColumns.size()));
        else      // Negative testing coverage if table has no integer columnType
          column = columns.get(SQLOldTest.random.nextInt(columns.size()));
        break;
      case SETNULL:
        column = columns.get(SQLOldTest.random.nextInt(columns.size())); break;
      case DEFAULT: // TODO get default based on column type.
        column = columns.get(SQLOldTest.random.nextInt(columns.size())); break;
      case DATATYPE: // TODO get new datatype based on old data type.
        column = columns.get(SQLOldTest.random.nextInt(columns.size())); break;
      default:
        Log.getLogWriter().info(
            "Invalid alter syntax for alter column in " + ddl);
        executeDDL = false;
        return;
    }
    columnName = column.getColumnName();
    replacePlaceHolders(tableInfo.getFullyQualifiedTableName(), "",
        columnName);
  }
  
  public void getIntegerTypeColumns(){
    for (ColumnInfo col : columns) {
      if (col.getColumnType() == java.sql.Types.BIGINT
          || col.getColumnType() == java.sql.Types.INTEGER
          || col.getColumnType() == java.sql.Types.SMALLINT) {
        if (!col.isNull())
          intNotNullColumns.add(col);
        intColumns.add(col);
      }
    }
  }
  
  public void handleAlterDDLException(SQLException derby, SQLException gemxd) {
    Log.getLogWriter().info("Derby Exception is : " + derby);  
    Log.getLogWriter().info("GFXD Exception is : " + gemxd); 
    if(gemxd!=null && derby==null){
        if (gemxd.getSQLState().equals("X0Z01") && SQLTest.isHATest) {
          Log.getLogWriter().info("gfxd failed - operation due to node failure");
          executor.rollbackDerby(); // gfxd failed - operation due to node failure
          return;
      }
      else if (action == DDLAction.DROP) {
        String partitionClause = tableInfo.getPartitioningClause().toUpperCase();
        if (gemxd.getSQLState().equals("0A000")  && partitionClause.contains("PARTITION") 
            && tableInfo.getPartitioningColumns().contains(column)) {
            Log.getLogWriter().info("gfxd failed - partitioned column could not be dropped");
            executor.rollbackDerby(); // gfxd failed - partitioned column could not be dropped
            return;
        }
        else if (gemxd.getSQLState().equals("0A000") && tableInfo.getPrimaryKey().getColumns().contains(column)) {
          Log.getLogWriter().info(
              "gfxd failed - primary column could not be dropped");
          executor.rollbackDerby(); // gfxd failed - primary column could not be dropped
          return;
        }
        else if (gemxd.getSQLState().equals("X0Y25")) {
          Log.getLogWriter()
              .info(
                  "gfxd failed - to drop column as the column is part of constraint.");
          executor.rollbackDerby(); // gfxd failed - to drop column as the column is part of constraint.
          return;
        }
      }else if(action == DDLAction.ADD){
          if(gemxd.getSQLState().equals("0A000")){
            Log.getLogWriter().info("gfxd failed - to add column.");
          }
        }
    }
    compareExceptions(derby,gemxd);
  }
  
  public void handleAlterDDLException (SQLException gemxd){ 
    Log.getLogWriter().info("GFXD Exception is : " + gemxd);   
    if (parsedDDL.contains(" ALTER ") && !parsedDDL.contains(" ALWAYS ")) {
      if (parsedDDL.contains(" NOT NULL ") && gemxd.getSQLState().equalsIgnoreCase("42Z20")
          && (tableInfo.getPrimaryKey().getColumns().contains(column))) {
        Log.getLogWriter().info(
            "gfxd failed - primary key or unique key column cannot be nullable.");
      }else if (gemxd.getSQLState().equalsIgnoreCase("0A000")
          || gemxd.getSQLState().startsWith("42Z")) {
        Log.getLogWriter().info(
            "Got expected exception for column modification not supported");
      } else
        throw new TestException(
            "Did not get expected exception for column modification"
                + TestHelper.getStackTrace(gemxd));
    }
  }
  
}
