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
package cacheperf.comparisons.gemfirexd.useCase1;

import hydra.BasePrms;
import hydra.HydraConfigException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class UseCase1Prms extends BasePrms {

  static {
    setValues(UseCase1Prms.class);
  }

  /**
   * (int)
   * Batch size. Defaults to 100.
   */
  public static Long batchSize;
  public static int getBatchSize() {
    Long key = batchSize;
    return tasktab().intAt(key, tab().intAt(key, 100));
  }

  /**
   * (boolean)
   * Whether to export tables to files.  Defaults to false.
   */
  public static Long exportTables;
  public static boolean exportTables() {
    Long key = exportTables;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * List of (String)
   * Tables to import.
   */
  public static Long importTables;
  public static List<String> getImportTables() {
    Long key = importTables;
    Vector<String> val = tasktab().vecAt(key, tab().vecAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " was not specified";
      throw new HydraConfigException(s);
    }
    return new ArrayList(val);
  }

  /**
   * (String)
   * Directory for tables to import.
   */
  public static Long importTableDir;
  public static String getImportTableDir() {
    Long key = importTableDir;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " was not specified";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Heap percentaeg. Defaults to 60.
   */
  public static Long heapPercentage;
  public static double getHeapPercentage() {
    Long key = heapPercentage;
    return tasktab().doubleAt(key, tab().doubleAt(key, 60));
  }

  /**
   * (String)
   * DDL file to use.
   */
  public static Long ddlFile;
  public static String getDDLFile() {
    Long key = ddlFile;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " was not specified";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * List of (String)
   * SQL command files to run.
   */
  public static Long sqlCommandFiles;
  public static List<String> getSQLCommandFiles() {
    Long key = sqlCommandFiles;
    Vector<String> val = tasktab().vecAt(key, tab().vecAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " was not specified";
      throw new HydraConfigException(s);
    }
    return new ArrayList(val);
  }

  /**
   * (String)
   * Name of a distributed system.
   */
  public static Long dsName;
  public static String getDSName() {
    Long key = dsName;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " was not specified";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of milliseconds to sleep in each iteration of a workload task.
   * Defaults to 0.
   */
  public static Long workloadThrottleMs;
  public static int getWorkloadThrottleMs() {
    Long key = workloadThrottleMs;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (int)
   * Number of milliseconds in each chunk of throttle sleep time.
   * Defaults to 10000.
   */
  public static Long workloadThrottleChunkMs;
  public static int getWorkloadThrottleChunkMs() {
    Long key = workloadThrottleChunkMs;
    return tasktab().intAt(key, tab().intAt(key, 10000));
  }

  /**
   * (int)
   * Number of seconds to sleep in each iteration of {@link UseCase1Client
   * #useCase1DummyWorkloadTask}. Defaults to 5.
   */
  public static Long dummyWorkloadSleepSec;
  public static int getDummyWorkloadSleepSec() {
    Long key = dummyWorkloadSleepSec;
    return tasktab().intAt(key, tab().intAt(key, 5));
  }

  /**
   * (int)
   * Number of seconds for the traffic cop to sleep each time. Defaults to 300.
   */
  public static Long trafficCopSleepSec;
  public static int getTrafficCopSleepSec() {
    Long key = trafficCopSleepSec;
    return tasktab().intAt(key, tab().intAt(key, 300));
  }

  /**
   * (int)
   * Number of buckets per server.
   */
  public static Long numBuckets;
  public static int getNumBuckets() {
    Long key = numBuckets;
    return tab().intAt(key, 113);
  }

  /**
   * (int)
   * Number of clients in the whole test.
   */
  public static Long numClients;
  public static int getNumClients() {
    Long key = numClients;
    return tab().intAt(key);
  }

  /**
   * (int)
   * Number of ETL clients that preload data.
   */
  public static Long numETLLoadClients;
  public static int getNumETLLoadClients() {
    Long key = numETLLoadClients;
    return tab().intAt(key);
  }

  /**
   * (int)
   * Number of ETL gateway servers that drain queues.
   */
  public static Long numETLGatewayServers;
  public static int getNumETLGatewayServers() {
    Long key = numETLGatewayServers;
    return tab().intAt(key);
  }

  /**
   * (int)
   * Number of rows to preload into the SECT_CHANNEL_DATA table. Defaults to 100000.
   */
  public static Long numSectChannelDataRows;
  public static int getNumSectChannelDataRows() {
    Long key = numSectChannelDataRows;
    return tasktab().intAt(key, tab().intAt(key, 100000));
  }

  /**
   * (int)
   * Number of servers shutting down in each shut-down-all.
   */
  public static Long numServersPerShutDownAll;
  public static int getNumServersPerShutDownAll() {
    Long key = numServersPerShutDownAll;
    return tab().intAt(key);
  }

  /**
   * (boolean)
   * Whether to record statement statistics.  Defaults to false.
   */
  public static Long timeStmts;
  public static boolean timeStmts() {
    Long key = timeStmts;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to use a thin client when doing 'gfxd run'. Defaults to false.
   */
  public static Long useThinClient;
  public static boolean useThinClient() {
    Long key = useThinClient;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
}
