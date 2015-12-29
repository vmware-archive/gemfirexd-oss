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
package sql.datagen;

import hydra.blackboard.Blackboard;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DataGenerator Blackboard
 * 
 * @author Rahul Diyewar
 */

public class DataGeneratorBB extends Blackboard {
  static String SQL_BB_NAME = "DataGenerator_BB";

  static String SQL_BB_TYPE = "RMI";

  // table row queue prefix, format: TableRowQueue_<schemaName.tableName>
  final static String TABLE_ROW_QUEUE_PREFIX = "TableRowQueue_";

  // flag for table row count low threshold reach, format:
  // TableRowQueue_<schemaName.tableName>
  final static String TABLE_ROWS_LOW_PREFIX = "TableRowdLow_";

  // CSV file location, format: TableCSV_<schemaName.TableName>
  final static String TABLE_CSV_PREFIX = "TableCSV_";

  // act as a flag to init DataGenerator
  public static int PARSE_MAPPER = 0;

  public static DataGeneratorBB bbInstance = null;

  public static DataGeneratorBB getBB() {
    if (bbInstance == null) {
      synchronized (DataGeneratorBB.class) {
        if (bbInstance == null) {
          bbInstance = new DataGeneratorBB(SQL_BB_NAME, SQL_BB_TYPE);
        }
      }
    }
    return bbInstance;
  }

  public DataGeneratorBB() {
  }

  public DataGeneratorBB(String name, String type) {
    super(name, type, DataGeneratorBB.class);
  }

  public static String getCSVFileName(String fullTableName) {
    String key = TABLE_CSV_PREFIX + fullTableName.toUpperCase();
    return (String)bbInstance.getSharedMap().get(key);
  }

  public static void setCSVFileName(String fullTableName, String csv) {
    String key = TABLE_CSV_PREFIX + fullTableName.toUpperCase();
    bbInstance.getSharedMap().put(key, csv);
  }

  public static Queue<Map<String, Object>> getRowsFromBB(String fullTableName) {
    final Queue<Map<String, Object>> tableRowsQueue = new LinkedBlockingQueue<Map<String, Object>>();
    String key = TABLE_ROW_QUEUE_PREFIX + fullTableName.toUpperCase();
    bbInstance.getSharedLock().lock();
    Queue<Map<String, Object>> allRowsQueue = (Queue<Map<String, Object>>)bbInstance
        .getSharedMap().get(key);
    int queueSize = allRowsQueue.size();
    if (queueSize < 500) {
      setRowsLowThresholdReached(fullTableName, true);
    }
    int rowsToReturn = queueSize > 100 ? 100 : queueSize;    
    for (int i = 0; i < rowsToReturn; i++) {
      tableRowsQueue.add(allRowsQueue.poll());
    }
    bbInstance.getSharedMap().put(key, allRowsQueue);
    bbInstance.getSharedLock().unlock();
    return tableRowsQueue;
  }

  public static boolean isRowsLowThresholdReached(String fullTableName) {
    String flagKey = TABLE_ROWS_LOW_PREFIX + fullTableName.toUpperCase();
    Boolean f = (Boolean)bbInstance.getSharedMap().get(flagKey);
    return (f == null) ? true : f.booleanValue();
  }

  public static void setRowsLowThresholdReached(String fullTableName,
      boolean bool) {
    String flagKey = TABLE_ROWS_LOW_PREFIX + fullTableName.toUpperCase();
    bbInstance.getSharedMap().put(flagKey, new Boolean(bool));
  }

  public static void addTableRowsToBB(String fullTableName,
      List<Map<String, Object>> rowList) {
    String key = TABLE_ROW_QUEUE_PREFIX + fullTableName.toUpperCase();
    bbInstance.getSharedLock().lock();
    Queue<Map<String, Object>> allRowsQueue = (Queue<Map<String, Object>>)bbInstance
        .getSharedMap().get(key);
    if (allRowsQueue == null) {
      allRowsQueue = new LinkedBlockingQueue<Map<String, Object>>();
    }
    allRowsQueue.addAll(rowList);    
    bbInstance.getSharedMap().put(key, allRowsQueue);  
    bbInstance.getSharedLock().unlock();
  }
}
