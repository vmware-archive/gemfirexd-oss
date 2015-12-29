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
package sql.generic.dmlstatements;

import static sql.generic.SqlUtilityHelper.getColumnName;
import static sql.generic.SqlUtilityHelper.getTableName;
import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sql.datagen.DataGeneratorHelper;
import sql.generic.dmlstatements.DBRow.Column;
import util.TestException;
import util.TestHelper;
import sql.generic.SqlUtilityHelper;

/**
 * AbstractDMLOperation
 * 
 * @author Namrata Thanvi
 */

public abstract class AbstractDMLOperation implements DMLOperation {
  public static final String DML_COLUMN_PATTERN = "::[a-zA-Z0-9_. \\[\\](<>)$]*::";

  protected String statement;

  private String preparedStmt;

  private DMLPreparedStatement dmlPreparedStatement = new DMLPreparedStatement();

  protected DMLExecutor executor;

  private DBRow preparedColumnMap;

  Map<String, DBRow> dataPopulatedForTheTable;

  public List<String> columnConsumedInStmt;

  public boolean performOperation = false;

  public boolean executable = true;

  public String preparedStmtForLogging = "";

  // to be used in case of insert and delete and update operations
  protected String tableName = "";

  private boolean executeOperation = true;

  Operation operation;

  public AbstractDMLOperation(DMLExecutor executor, String statement,
      Operation operation) {
    Log.getLogWriter().info("Parsing [" + statement + "]");
    this.statement = statement;
    this.executor = executor;
    this.operation = operation;
    this.preparedStmt = statement;
    fetchDataInMapFromDB();
    if (executable) {
      generateData(SqlUtilityHelper.getMatchedPattern(getStatement(),
          DML_COLUMN_PATTERN));
      preparedColumnMap = new DBRow();
      dataPopulatedForTheTable = new HashMap<String, DBRow>();
      columnConsumedInStmt = new ArrayList<String>();
    }
  }

  public AbstractDMLOperation(DMLExecutor executor, String statement,
      DBRow preparedColumnMap, Map<String, DBRow> dataPopulatedForTheTable,
      Operation operation) {
    Log.getLogWriter().info("Parsing [" + statement + "]");
    this.statement = statement;
    this.executor = executor;
    this.preparedStmt = statement;
    // this.preparedStmt = statement.replaceAll(DML_COLUMN_PATTERN, "?");
    this.preparedColumnMap = preparedColumnMap;
    columnConsumedInStmt = new ArrayList<String>();
    this.dataPopulatedForTheTable = dataPopulatedForTheTable;
    fetchDataInMapFromDB();
    if (executable) {
      generateData(SqlUtilityHelper.getMatchedPattern(getStatement(),
          DML_COLUMN_PATTERN));
    }
    this.operation = operation;
    setTableNameFromStatement();
  }

  public void fetchDataInMapFromDB() {

    List<String> columnList = SqlUtilityHelper.getMatchedPattern(statement,
        AbstractDMLOperation.DML_COLUMN_PATTERN);
    DBRow dbrow = null;
    String tableName = " ";
    for (String column : columnList) {
      tableName = getTableName(column);
      if (!dataPopulatedForTheTable.containsKey(tableName)) {
        try {
          DBRow dataMap = executor.getRandomRowForDml(tableName);
          if (dataMap != null) {
            dataPopulatedForTheTable.put(tableName, dataMap);
          }
          else
            executable = false;
        } catch (Exception e) {
          throw new TestException(
              "Exception occurred as not able to get Random Row For DML For tablename : "
                  + tableName + TestHelper.getStackTrace(e));
        }
      }
    }
  }

  public abstract void generateData(List<String> columns);

  public String getStatement() {
    return statement.toLowerCase();
  }

  public String getPreparedStmt() {
    return preparedStmt;
  }

  public DMLPreparedStatement getDMLPreparedStatement() {
    return dmlPreparedStatement;
  }

  public List<String> getColumnList() {
    return SqlUtilityHelper.getMatchedPattern(getStatement(),
        DML_COLUMN_PATTERN);
  }

  public void createPrepareStatement(Connection conn) throws SQLException {
    try {

      generateActualPrepareStatement();
      if (GenericDMLHelper.isDerby(conn) && operation == Operation.PUT) {
        preparedStmt = preparedStmt.replace("PUT", "INSERT");
      }
      Log.getLogWriter().info(
          (GenericDMLHelper.isDerby(conn) ? "Derby - " : "Gemxd - ")
              + "Creating prepare Statement " + preparedStmt);
      preparedStmtForLogging = preparedStmt;
      PreparedStatement stmt = conn.prepareStatement(getPreparedStmt());
      Log.getLogWriter().info(
          (GenericDMLHelper.isDerby(conn) ? "Derby - " : "Gemxd - ")
              + "Created prepare Statement " + preparedStmt);
      preparedStmtForLogging = " ";
      int i = 1;
      PrepareStatementSetter ps = new PrepareStatementSetter(stmt);

      for (String columnName : getColumnList()) {
        Column column = preparedColumnMap.find(columnName);
        if (column.getValue() instanceof List)
          setListValuesInPrepareStatement(ps, column, (List<DBRow>)column
              .getValue());
        else {
          preparedStmtForLogging += " "
              + getColumnName(column.getName(), false).toUpperCase() + ":"
              + column.getValue() + ",";
          ps.setValues(column.getValue(), column.getType());
        }

      }
      preparedStmtForLogging += "  [ "
          + preparedStmt.substring(0, preparedStmt.length()) + " ]";
      setPreparedStmt(conn, stmt);
    } catch (SQLException se) {
      // later need to catch each exception and throw all the remaining
      // exceptions.
      // handle update on Partition Key Exception
      throw se;
    }
  }

  public String getPreparedStmtForLogging() {
    return preparedStmtForLogging;
  }

  private void setPreparedStmt(Connection conn, PreparedStatement stmt) {
    try {
      if (conn.getMetaData().getDriverName().toLowerCase().contains("derby")) {
        dmlPreparedStatement.setDerbyPreparedStmt(stmt);
        return;
      }
      dmlPreparedStatement.setGemXDPreparedStmt(stmt);
    } catch (SQLException se) {

      throw new TestException("unable to set prepare statement in database "
          + se.getSQLState() + TestHelper.getStackTrace(se));
    }
  }

  public DMLPreparedStatement getPreparedStmt(Connection conn) {
    return getDMLPreparedStatement();
  }

  protected void setListValuesInPrepareStatement(PrepareStatementSetter stmt,
      DBRow.Column column, List<DBRow> values) {
    for (DBRow row : values) {
      preparedStmtForLogging += " "
          + getColumnName(column.getName(), false).toUpperCase() + ":"
          + row.find(getColumnName(column.getName(), true)).getValue() + ",";
      stmt.setValues(
          row.find(getColumnName(column.getName(), true)).getValue(), column
              .getType());
    }

  }

  public void getColumnValuesFromDataBase(List<String> columnList) {
    for (String column : columnList) {
      DBRow dbRow = dataPopulatedForTheTable.get(getTableName(column));
      Column dbColumn = dbRow.find(getColumnName(column, true));
      if (preparedColumnMap.find(column) == null && dbRow.find(column) == null) {
        GenerateColumnValueOnExpression columnValue = new GenerateColumnValueOnExpression(
            dbRow, executor, getPreparedStmt());
        preparedColumnMap.addColumn(column, dbColumn.getType(), columnValue
            .getColumnValue(column));
        preparedStmt = columnValue.getModifiedPrepareStatement();

      }
      else {
        // get column from DBMap
        preparedColumnMap.addColumn(column, dbColumn.getType(), dbColumn
            .getValue());
      }

    }
  }

  public void getRowFromDataGeneratorAndLoadColumnValues(List<String> columnList) {
    String tableName = getTableName(columnList.get(0));
    Map<String, Object> columnValuesfromDataGenerator = DataGeneratorHelper
        .getNewRow(tableName);

    for (String column : columnList) {
      DBRow dbRow = dataPopulatedForTheTable.get(getTableName(column));
      Column dbColumn = dbRow.find(column);
      if (preparedColumnMap.find(column.toLowerCase()) == null) {
        preparedColumnMap.addColumn(column, dbColumn.getType(), (column
            .equals(tableName + ".tid") ? GenericDMLHelper.getMyTid()
            : columnValuesfromDataGenerator.get(dbColumn.getShortName().toUpperCase())));
      }
    }

  }

  @Override
  public void execute() {
    if (executeOperation) {
      executor.executeStatement(this);
      executor.commitAll();
    }
  }

  public Operation getOperation() {
    return operation;
  }

  public String toString() {
    return statement;
  }

  public void generateActualPrepareStatement() {
    this.preparedStmt = preparedStmt.replaceAll(DML_COLUMN_PATTERN, "?");
  }

  public boolean getExecutable() {
    return executable;
  }

  abstract void setTableNameFromStatement();

  public String getTableNameForOperation() {
    return tableName;
  }

}
