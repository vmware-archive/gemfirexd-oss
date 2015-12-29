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

import java.util.List;
import java.util.Map;

/**
 * InsertOperation
 * 
 * @author Namrata Thanvi
 */

public class InsertOperation extends AbstractDMLOperation implements
    DMLOperation {
  boolean insert = false;

  public InsertOperation(DMLExecutor executor, String statement,
      Operation operation) {
    super(executor, statement, operation);
    if (operation == Operation.INSERT)
      insert = true;
    else
      insert = false;
  }

  public InsertOperation(DMLExecutor executor, String statement,
      DBRow preparedColumnMap, Map<String, DBRow> dataPopulatedForTheTable,
      Operation operation) {
    super(executor, statement, preparedColumnMap, dataPopulatedForTheTable,
        operation);
    if (operation == Operation.INSERT)
      insert = true;
    else
      insert = false;
  }

  @Override
  public void generateData(List<String> columnList) {
    if (GenericDML.rand.nextInt() % 20 == 1) {
      // retry same row from database
      getColumnValuesFromDataBase(columnList);
    }
    getRowFromDataGeneratorAndLoadColumnValues(columnList);
  }

  void setTableNameFromStatement() {
    int intoIndex = statement.trim().toLowerCase().indexOf("into") + 5;
    int lastIndex = statement.trim().toLowerCase().indexOf(" ", intoIndex);
    tableName = statement.trim().substring(intoIndex, lastIndex).trim();
  }

}
