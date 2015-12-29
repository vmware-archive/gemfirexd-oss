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
package sql.generic;

import hydra.HydraVector;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.TestTask;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sql.SQLPrms;
import sql.generic.ddl.Executor;
import sql.generic.dmlstatements.GenerateLikeExpression;
import util.TestException;
import util.TestHelper;

/**
 * SqlUtilityHelper
 * 
 * @author Namrata Thanvi
 */

public class SqlUtilityHelper {
 
  public static List<String> getMatchedPattern(String stmt, String regEx) {
    List<String> matchedPattern = new ArrayList<String>();
    Pattern pattern = Pattern.compile(regEx);
    Matcher matcher = pattern.matcher(stmt);
    while (matcher.find()) {
      // Log.getLogWriter().info("GenericDML - getMatchedPattern  ColumnName is :"
      // + matcher.group());
      matchedPattern.add(matcher.group().toLowerCase().replaceAll("::", "")
          .trim().replaceAll("\\s+", " "));
    }
    return matchedPattern;
  }
  
  public static String getTableName(String columnNameWithRelationShip) {
    String[] split = columnNameWithRelationShip.split("\\(");
    String[] splitString;
    if (split.length > 1) {
      splitString = split[1].trim().split("\\.");
    } else {
      splitString = split[0].split("\\.");
    }

    return (splitString.length > 2 ? splitString[splitString.length - 3] + "."
        + splitString[splitString.length - 2]
        : splitString[splitString.length - 2]).toLowerCase()
        .replaceFirst("::", "").replace(")", "");

  }

  public static String getTableNameWithoutSchema(String tableNameWithSchema) {
    return tableNameWithSchema.trim().split("\\.")[1];
  }

  public static String getSchemaFromTableName(String tableNameWithSchema) {
    return tableNameWithSchema.trim().split("\\.")[0];
  }

  public static String getColumnName(String columnNameWithRelationShip,
      boolean fullQualifiedName) {

    String[] columnNameWoRelationship = columnNameWithRelationShip.trim()
        .split("[<>!$\\[]");
    // remove substr from columnName to get columnName
    if (columnNameWoRelationship[0].toLowerCase().contains("substr")) {
      columnNameWoRelationship[0] = GenerateLikeExpression
          .getLikeColumnName(columnNameWoRelationship[0]);
    }
    String[] splitString = columnNameWoRelationship[0].split("\\.");
    if (fullQualifiedName)
      return (getTableName(columnNameWoRelationship[0]) + "." + splitString[splitString.length - 1])
          .toLowerCase().replaceAll("::", "").trim();
    else
      return splitString[splitString.length - 1].trim().toLowerCase();

  }

  public static String getRandomString(int length) {
    String mystr = "";
    int start = 97;
    int end = 122;
    int charLength = SQLOldTest.random.nextInt(length) + 3;
    while (mystr.length() < charLength) {
      mystr += (char) ((int) (SQLOldTest.random.nextDouble() * (end - start + 1)) + start);
    }
    return mystr;
  }

  
  public static int getRelativeColumnName(String columnName) {
    String relativeName = columnName.substring(columnName.indexOf("$") + 1).trim();
    return Integer.parseInt(relativeName) - 1;
  }

  /**
   * Gets the client's hydra thread id. Exactly one logical hydra thread in the
   * whole test has a given tid. The tids are numbered consecutively starting
   * from 0.
   */
  public static int tid() {
    return RemoteTestModule.getCurrentThread().getThreadId();
  }

  /**
   * Gets the client's hydra threadgroup id. Exactly one logical hydra thread in
   * the threadgroup containing this thread has a given tgid. The tgids are
   * numbered consecutively starting from 0.
   */
  public static int tgid() {
    return RemoteTestModule.getCurrentThread().getThreadGroupId();
  }

  /**
   * Gets the client's hydra task threadgroup id. Exactly one logical hydra
   * thread in all of the threadgroups assigned to the current task has a given
   * ttgid. The ttgids are numbered consecutively starting from 0.
   */
  public static int ttgid() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    return task.getTaskThreadGroupId(tgname, tgid());
  }

  /**
   * Gets total threads running current task
   */
  public static int totalTaskThreads() {
    return RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTotalThreads();
  }
  
  /**
   * which tables can be used for ddl or dml operations
   * @return String array of table names
   */
  @SuppressWarnings("unchecked")
  public static String[] getTableNames() {
    Vector tables = TestConfig.tasktab().vecAt(SQLPrms.dmlTables,  TestConfig.tab().vecAt(SQLPrms.dmlTables, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = ((String)tables.elementAt(i)).toUpperCase(); //get what tables are in the tests
    }
    return strArr;
  }
  
  public static String getRandomTableName() {
    String[] tables = getTableNames();
    int loc = SQLOldTest.random.nextInt(tables.length - 1);
    return tables[loc];
  }
  
  public static int getRowsInTable(String tableName , Connection conn){
    String query = "select count(*)  from " + tableName;
    Executor executor = new Executor(conn);
    try{
    ResultSet  rs = executor.executeQuery(query);
    while (rs.next())
      return rs.getInt(1);
    
    return 0;
    } catch ( SQLException se ) {
      throw new TestException ( TestHelper.getStackTrace(se));
    }
  }
}
