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
package sql.generic.ddl.Index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;
import util.TestException;

//pending task 
//1. to update generic Index . indexNames
//2. synchornize the task
//3. Adding exceptions in the exception handler - As per the runs
//4. synchronize with alter ddl's - later after the exception handling and executor logic is corrected

public class IndexOperation {
  Executor executor;

  Map<String, String> stmtPlaceHolders;

  DDLAction action;

  String ddlStmt;

  String parsedStmt;

  String tableName;

  String schema;

  String newIndex = "", dropIndex = "";

  final String DDL_COLUMN_PATTERN = "::[a-zA-Z0-9_. \\[\\]]*::";

  Random random = SQLOldTest.random;

  int length = 10;

  boolean indexExist = true;

  public IndexOperation(String stmt, Executor executor,
      Map<String, String> stmtPlaceHolders, DDLAction action) {
    this.executor = executor;
    this.stmtPlaceHolders = stmtPlaceHolders;
    this.action = action;
    this.ddlStmt = stmt;
    this.parsedStmt = stmt;
    parseDDLStmt();
  }

  protected void parseDDLStmt() {

    List<String> patterns = getMatchedPattern(ddlStmt, DDL_COLUMN_PATTERN);
    String value = "";

    populateRequiredFields();

    for (String pattern : patterns) {
      if (stmtPlaceHolders.containsKey(pattern)) {
        value = stmtPlaceHolders.get(pattern);
      }
      else {
        value = getPlaceHolderValue(pattern);
        stmtPlaceHolders.put(pattern, value);
      }

      parsedStmt = parsedStmt.replaceAll(pattern, value);
      populateRequiredFields();

    }

    populateRequiredFields();
  }

  protected void executeStatement() {
    if (GenericIndex.indexNames.contains(dropIndex)) {
      indexExist = true;
      GenericIndex.indexNames.remove(dropIndex);
    }
    else {
      indexExist = false;
    }
    if (executor.executeOnGfxdOnly(parsedStmt, new IndexExceptionHandler(
        action, this))) {
      if (action == DDLAction.CREATE) {
        GenericIndex.indexNames.add(newIndex);
      }
    }
    executor.commit();
  }

  private void populateRequiredFields() {
    if (action == DDLAction.CREATE) {
      int startIndex = parsedStmt.toLowerCase().indexOf(" index ");
      int middleIndex = parsedStmt.toLowerCase().indexOf(" on ");
      int endIndex = parsedStmt.toLowerCase().indexOf("(");
      newIndex = parsedStmt.substring(startIndex + 7, middleIndex).trim();
      tableName = parsedStmt.substring(middleIndex + 4, endIndex).trim();
      schema = SqlUtilityHelper.getSchemaFromTableName(tableName);
      newIndex = schema.trim() + "." + newIndex;
    }
    else if (action == DDLAction.DROP) {
      int startIndex = parsedStmt.toLowerCase().indexOf(" index ");
      dropIndex = parsedStmt.substring(startIndex + 6, parsedStmt.length())
          .trim();
    }
  }

  public List<String> getMatchedPattern(String stmt, String regEx) {
    List<String> matchedPattern = new ArrayList<String>();
    Pattern pattern = Pattern.compile(regEx);
    Matcher matcher = pattern.matcher(stmt);
    while (matcher.find()) {
      matchedPattern.add(matcher.group());
    }
    return matchedPattern;
  }

  protected String getPlaceHolderValue(String pattern) {
    if (pattern.toLowerCase().contains("table")) {
      tableName = getTableName();
      return tableName;
    }
    if (pattern.toLowerCase().contains("column"))
      return getColumnName();

    if (pattern.toLowerCase().contains("index") && action == DDLAction.CREATE) {
      return getIndexNameForCreate();
    }
    if (pattern.toLowerCase().contains("index") && action == DDLAction.DROP) {
      return getIndexNameForDrop();
    }
    throw new TestException("invalid pattern " + pattern
        + " in the DDL statement" + ddlStmt);
  }

  private String getTableName() {
    return SqlUtilityHelper.getRandomTableName();
  }

  private String getColumnName() {
    TableInfo info = GenericBBHelper.getTableInfo(tableName);
    List<ColumnInfo> columns = info.getColumnList();
    int columnLoc = random.nextInt(columns.size() - 1);
    String columnName = columns.get(columnLoc).getColumnName();
    if (parsedStmt.contains(columnName)) {
      return getColumnName();
    }
    else {
      return columnName;
    }

  }

  private String getIndexNameForCreate() {
    return "Index_" + SqlUtilityHelper.getRandomString(length);
  }

  private String getIndexNameForDrop() {
    int size = GenericIndex.indexNames.size();
    if (size > 0) {
      String indexName = GenericIndex.indexNames.get(random
          .nextInt(GenericIndex.indexNames.size()));
      return indexName;
    }
    else {
      return getIndexNameForCreate();
    }
  }

  public boolean getIndexExist() {
    return indexExist;
  }

}
