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
package sql.memscale;

import hydra.BasePrms;

public class SqlMemScalePrms extends BasePrms {
  
  /**
   * (int) The number of seconds to sleep
   */
  public static Long sleepSec;
  public static int getSleepSec() {
    Long key = sleepSec;
    int value = tasktab().intAt(key, tab().intAt(key, 1000));
    return value;
  }
  
  /**
   * (int) The number of tables to create.
   */
  public static Long numTables;
  public static int getNumTables() {
    Long key = numTables;
    int value = tasktab().intAt(key, tab().intAt(key, 1));
    return value;
  }

  /**
   * (int) The number of columns to create per table.
   */
  public static Long numColumnsPerTable;
  public static int getNumColumnsPerTable() {
    Long key = numColumnsPerTable;
    int value = tasktab().intAt(key, tab().intAt(key, 1));
    return value;
  }

  /**
   * (String) A gemfirexd data type for a column
   */
  public static Long columnType;
  public static String getColumnType() {
    Long key = columnType;
    String value = tasktab().stringAt(key, tab().stringAt(key, "INTEGER"));
    return value;
  }
  
  /**
   * (String) A gemfirexd data type for a lob column
   */
  public static Long lobColumnType;
  public static String getLobColumnType() {
    Long key = lobColumnType;
    String value = tasktab().stringAt(key, tab().stringAt(key, "CLOB"));
    return value;
  }
  
  /**
   * (int) The length of a VARCHAR data type.
   */
  public static Long varCharLength;
  public static int getVarCharLength() {
    Long key = varCharLength;
    int value = tasktab().intAt(key, tab().intAt(key, 1000));
    return value;
  }
  
  /**
   * (int) The percentage of tables that should include a lob column.
   */
  public static Long percentTablesWithLobs;
  public static int getPercentTablesWithLobs() {
    Long key = percentTablesWithLobs;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }
  
  /**
   * (int) The percentage of columns within a table that are lobs
   */
  public static Long percentLobColumns;
  public static int getPercentLobColumns() {
    Long key = percentLobColumns;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }
  
  /**
   * (String) The size of a LOB
   */
  public static Long lobLength;
  public static String getLobLength() {
    Long key = lobLength;
    String value = tasktab().stringAt(key, tab().stringAt(key));
    return value;
  }
  
  /**
   * (int) The number of rows per table.
   */
  public static Long numRowsPerTable;
  public static int getNumRowsPerTable() {
    Long key = numRowsPerTable;
    int value = tasktab().intAt(key, tab().intAt(key, 100));
    return value;
  }
  
  /**
   * (String) A create table clause to append to the create table statement.
   *          Examples are: "REPLICATE", "PARTITION BY PRIMARY KEY OFFHEAP"
   */
  public static Long tableClause;
  public static String getTableClause() {
    Long key = tableClause;
    String value = tasktab().stringAt(key, tab().stringAt(key, ""));
    return value;
  }
  
  /**
   * (int) The number of server threads across all wan sites
   */
  public static Long numServerThreads;
  public static int getNumServerThreads() {
    Long key = numServerThreads;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }
  
  /** (int) The number of seconds to run.
   *        We cannot use hydra's taskTimeSec parameter
   *        because of a small window of opportunity for the test to hang 
   *        when one thread receives a task but the other did not because
   *        they hit hydra's time limiet.
   */
  public static Long secondsToRun;  
  public static int getSecondsToRun() {
    Long key = secondsToRun;
    int value = tasktab().intAt(key, tab().intAt(key, 600));
    return value;
  }
  
  /** (int) The number of seconds that should elapse before the test forces an off-heap 
   *        memory compaction.
   */
  public static Long compactionIntervalSec;  
  public static int getCompactionIntervalSec() {
    Long key = compactionIntervalSec;
    int value = tasktab().intAt(key, tab().intAt(key, 600));
    return value;
  }
  
  /** (boolean) If true, then the test creates prepared statements in advance and reuses
   *            them throughout the test for inserts, if false a new prepared statement is created
   *            and closed for each insert.
   */
  public static Long reusePreparedStatements;  
  public static boolean getReusePreparedStatements() {
    Long key = reusePreparedStatements;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return value;
  }
  
// ================================================================================
static {
   BasePrms.setValues(SqlMemScalePrms.class);
}

}
