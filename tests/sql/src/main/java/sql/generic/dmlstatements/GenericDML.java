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

import hydra.Log;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import sql.generic.SQLGenericPrms;
import sql.generic.SQLOldTest;
import sql.generic.dmlstatements.DBRow.Column;
import util.TestException;

public class GenericDML {
  public static final Random rand = SQLOldTest.random;

  private DBRow preparedColumnMap = new DBRow();

  Map<String, DBRow> dataPopulatedForTheTable = new HashMap<String, DBRow>();

  HashMap<String, Column> columnConsumedInStmt = new HashMap<String, Column>();

  private DMLExecutor genericDMLExecutor;

  /**
   * GenericDML
   * 
   * @author Namrata Thanvi
   */

  private List<DMLOperation> operationsList;

  public GenericDML(List<String> dmlStatements, Connection dConn,
      Connection gConn) throws Exception {
    this(dmlStatements, new GenericDMLExecutor(dConn, gConn));
  }

  public GenericDML(List<String> dmlStatements, DMLExecutor executor)
      throws Exception {
    genericDMLExecutor = executor;
    operationsList = prepareOperationsForStatements(dmlStatements);
  }

  public List<DMLOperation> prepareOperationsForStatements(
      List<String> dmlStatements) throws Exception {

    List<DMLOperation> operations = new LinkedList<DMLOperation>();
    for (String stmt : dmlStatements) {
      try {
        DMLOperation operationForStatement = getOperationForStatement(stmt);
        if (operationForStatement != null)
          operations.add(operationForStatement);
      } catch (Exception e) {
        throw e;
      }
    }
    return operations;
  }

  public DMLOperation getOperationForStatement(String stmt) throws Exception {
    stmt = convertCase(stmt);
    if (!performOperation(stmt)) {
      Log.getLogWriter().info("skipping the operation " + stmt);
      return null;
    }
    if (stmt.startsWith(Operation.INSERT.getValue()))
      return new InsertOperation(genericDMLExecutor, stmt, preparedColumnMap,
          dataPopulatedForTheTable, Operation.INSERT);
    if (stmt.startsWith(Operation.PUT.getValue()))
      return new InsertOperation(genericDMLExecutor, stmt, preparedColumnMap,
          dataPopulatedForTheTable, Operation.PUT);
    if (stmt.startsWith(Operation.UPDATE.getValue()))
      return new UpdateOperation(genericDMLExecutor, stmt, preparedColumnMap,
          dataPopulatedForTheTable);
    if (stmt.startsWith(Operation.SELECT.getValue()))
      return new SelectOperation(genericDMLExecutor, stmt, preparedColumnMap,
          dataPopulatedForTheTable);
    if (stmt.startsWith(Operation.DELETE.getValue()))
      return new DeleteOperation(genericDMLExecutor, stmt, preparedColumnMap,
          dataPopulatedForTheTable);
    throw new Exception(
        "Not a valid statement, statement should be INSERT/UPDATE/DELETE or SELECT. Stmt is:"
            + stmt);
  }

  private String convertCase(String stmt) {

    String StmtFinal = stmt.toUpperCase().trim();
    if (!stmt.contains("'"))
      return StmtFinal;

    int endIndex = stmt.lastIndexOf("'");
    int nextIndex = 0, startIndex = 0;

    while (nextIndex < endIndex) {
      startIndex = nextIndex + stmt.substring(nextIndex).indexOf("'") + 1;
      nextIndex = startIndex + stmt.substring(startIndex).indexOf("'") + 1;
      StmtFinal = StmtFinal.replace(StmtFinal.substring(startIndex, nextIndex),
          stmt.substring(startIndex, nextIndex));

    }
    return StmtFinal;

  }

  private boolean performOperation(String stmt) throws Exception {
    if (SQLGenericPrms.getDMLOperations() == null)
      return true;

    if (stmt.startsWith(Operation.INSERT.getValue())
        && SQLGenericPrms.getDMLOperations().contains(
            Operation.INSERT.getValue().toLowerCase()))
      return true;
    if (stmt.startsWith(Operation.PUT.getValue())
        && SQLGenericPrms.getDMLOperations().contains(
            Operation.PUT.getValue().toLowerCase()))
      return true;
    if (stmt.startsWith(Operation.UPDATE.getValue())
        && SQLGenericPrms.getDMLOperations().contains(
            Operation.UPDATE.getValue().toLowerCase()))
      return true;
    if (stmt.startsWith(Operation.SELECT.getValue())
        && SQLGenericPrms.getDMLOperations().contains(
            Operation.SELECT.getValue().toLowerCase()))
      return true;
    if (stmt.startsWith(Operation.DELETE.getValue())
        && SQLGenericPrms.getDMLOperations().contains(
            Operation.DELETE.getValue().toLowerCase()))
      return true;

    return false;
  }

  public void execute() throws TestException {
    genericDMLExecutor.executeStatements(operationsList);
  }

}
