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

package sql.backupAndRestore;

import java.io.File;

import hydra.BasePrms;

/**
 * The BackupAndRestorePrms class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 1.3
 */
public class BackupAndRestorePrms extends BasePrms {

  public static Long restartDuringTest; // Used to determine if the test is to restart the Fabric Server during the test
  public static Long stopVmsDuringTest; // Used to determine if the test is to stop a VM during the test

  static {
    BasePrms.setValues(BackupAndRestorePrms.class);
  }

  /**
   * (boolean) If true the test should run online backup.
   */
  public static Long doBackup;
  public static boolean getDoBackup() {
    Long key = doBackup;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean) If true the test should perform incremental backups.
   */
  public static Long incrementalBackups;
  public static boolean getIncrementalBackups() {
    Long key = incrementalBackups;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * The directory where the backup files are to be stored
   */
  public static Long backupPath;
  public static String getBackupPath() {
    Long key = backupPath;
    String value = tasktab().stringAt(key, tab().stringAt(key, "default"));
    // Special logic for setting the directory when no param is used
    if (value.equals("default")) {
      // If the value comes back null (not set) then set it to the test directory
      value = System.getProperty("user.dir");
    } else {
      // If the value comes back not null (set) then add the test directory's name to the end (for uniqueness)
      File userDir = new File(System.getProperty("user.dir"));
      value += System.getProperty("file.separator") + userDir.getName();
    }
    return value;
  }

  /**
   * (String) The the unique part of the vsd archive name
   */
  public static Long archiveName;
  public static String getArchiveName() {
    Long key = archiveName;
    return tasktab().stringAt(key, tab().stringAt(key, "server"));
  }

  /**
   * (int) The number (in MB) when to perform a backup
   */
  public static Long backupAfterMBofData;
  public static int getBackupAfterMBofData() {
    Long key = backupAfterMBofData;
    return tasktab().intAt(key, tab().intAt(key, 50));
  }

  /**
   * (int) The number (in MB) that we want to create during the data load
   */
  public static Long initialDataMB;
  public static int getInitialDataMB() {
    Long key = initialDataMB;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   * (int) The number (in MB) that we want to create during the Ops
   */
  public static Long desiredDataOpsMB;
  public static int getDesiredDataOpsMB() {
    Long key = desiredDataOpsMB;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   * (double) The percentage amount to use when updating the systems Eviction Percentage
   */
  public static Long evictionHeapPercentage;
  public static double getEvictionHeapPercentage() {
    Long key = evictionHeapPercentage;
    return tasktab().doubleAt(key, tab().doubleAt(key, 80));
  }

  /**
   * (int) The number of Executions
   */
  public static Long nbrOfExecutions;
  public static int getNbrOfExecutions() {
    Long key = nbrOfExecutions;
    return tasktab().intAt(key, tab().intAt(key, 2));
  }

  /**
   * (String) A GemFireXD data type for a column
   */
  public static Long columnType;
  public static String getColumnType() {
    Long key = columnType;
    return tasktab().stringAt(key, tab().stringAt(key, "INTEGER"));
  }

  /**
   * (int) The number of columns that are to be lobs.
   */
  public static Long nbrLobColumns;
  public static int getNbrLobColumns() {
    Long key = nbrLobColumns;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (String) A GemFireXD data type for a lob column
   */
  public static Long lobColumnType;
  public static String getLobColumnType() {
    Long key = lobColumnType;
    return tasktab().stringAt(key, tab().stringAt(key, "CLOB"));
  }

  /**
   * (String) The size of a LOB
   */
  public static Long lobLength;
  public static String getLobLength() {
    Long key = lobLength;
    return tasktab().stringAt(key, tab().stringAt(key));
  }

  /**
   * (int) The maximum number of validation errors to report.
   * 0 = report all errors
   */
  public static Long maxValidationErrors;
  public static int getMaxValidationErrors() {
    Long key = maxValidationErrors;
    return tasktab().intAt(key, tab().intAt(key, -1));
  }

  /**
   * (int) The number of tables to create.
   */
  public static Long nbrTables;
  public static int getNbrTables() {
    Long key = nbrTables;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   * (int) The number of columns to create per table.
   */
  public static Long nbrColumnsPerTable;
  public static int getNbrColumnsPerTable() {
    Long key = nbrColumnsPerTable;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /** (boolean) If true, then the test creates prepared statements in advance and reuses
   *            them throughout the test for inserts, if false a new prepared statement is created
   *            and closed for each insert.
   */
  public static Long reusePreparedStatements;
  public static boolean getReusePreparedStatements() {
    Long key = reusePreparedStatements;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (String) A create table clause to append to the create table statement.
   *          Examples are: "REPLICATE", "PARTITION BY PRIMARY KEY"
   */
  public static Long extraTableClause;
  public static String getExtraTableClause() {
    Long key = extraTableClause;
    return tasktab().stringAt(key, tab().stringAt(key, ""));
  }

  /**
   * (int) The percentage of the operations that should be inserts
   */
  public static Long insertPercent;
  public static int getInsertPercent() {
    Long key = insertPercent;
    return tasktab().intAt(key, tab().intAt(key, 80));
  }
  /**
   * (int) The percentage of the operations that should be updates
   */
  public static Long updatePercent;
  public static int getUpdatePercent() {
    Long key = updatePercent;
    return tasktab().intAt(key, tab().intAt(key, 15));
  }
  /**
   * (int) The percentage of the operations that should be deletes
   */
  public static Long deletePercent;
  public static int getDeletePercent() {
    Long key = deletePercent;
    return tasktab().intAt(key, tab().intAt(key, 5));
  }

  /**
   * (String) A to be used at the end the key used to store the timings in the BlackBoard Map
   */
  public static Long timingKeySuffix;
  public static String getTimingKeySuffix() {
    Long key = timingKeySuffix;
    return tasktab().stringAt(key, tab().stringAt(key, ""));
  }

  /**
   * (int) The length of a VARCHAR data type.
   */
  public static Long varCharLength;
  public static int getVarCharLength() {
    Long key = varCharLength;
    return tasktab().intAt(key, tab().intAt(key, 1000));
  }
}
