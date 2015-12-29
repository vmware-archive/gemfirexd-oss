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
package gfxdperf.ycsb.gfxd;

import hydra.BasePrms;
import hydra.HydraConfigException;
import java.sql.Connection;

/**
 * A class used to store keys for test configuration settings.
 */
public class GFXDPrms extends BasePrms {
  
  static {
    setValues(GFXDPrms.class);
  }

  public static final int TRANSACTION_NONE = 0;

  private static final String DEFAULT_HDFS_CLIENT_CONFIG_FILE =
    "$JTESTS/gfxdperf/ycsb/gfxd/gfxd-client-config.xml";

  public static enum ConnectionType {
    peer, thin
    ;
  }

//------------------------------------------------------------------------------

  /**
   * Autocommit setting. Defaults to false.
   */
  public static Long autocommit;

  public static boolean getAutoCommit() {
    Long key = autocommit;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of buckets in the table. Applies only to partitioned tables.
   */
  public static Long bucketCount;

  public static int getBucketCount() {
    Long key = bucketCount;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val < 0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s); 
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Name of a connection type found in {@link #ConnectionType} used to create
   * connections in threads doing YCSB workloads.
   */
  public static Long connectionType;

  public static ConnectionType getConnectionType() {
    Long key = connectionType;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s); 
    }
    for (ConnectionType c : ConnectionType.values()) {
      if (val.equalsIgnoreCase(c.toString())) {
        return c; 
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s); 
  }

//------------------------------------------------------------------------------

  /**
   * Whether to enable per-connection statement-level statistics. Defaults to
   * false. To turn on statement timing as well, use {@link #enableTimeStats}.
   */
  public static Long enableStats;

  public static boolean enableStats() {
    Long key = enableStats;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * Whether to enable per-connection statement-level time statistics. Defaults
   * to false. This has no effect unless {@link enableStats} is set to true.
   */
  public static Long enableTimeStats;

  public static boolean enableTimeStats() {
    Long key = enableTimeStats;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to evict all incoming data. Applies only to partitioned tables
   * where {@link #useHDFS} is true. Defaults to false.
   */
  public static Long evictIncoming;

  public static boolean evictIncoming() {
    Long key = evictIncoming;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * Whether to throw an exception and fail the test if the load for any
   * table is imbalanced. This includes buckets, primary buckets, and data.
   * Defaults to true. Only used by tasks that check balance.
   */
  public static Long failOnLoadImbalance;

  public static boolean getFailOnLoadImbalance() {
    Long key = failOnLoadImbalance;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
 
//------------------------------------------------------------------------------

  /**
   * (String)
   * Full path to the HDFS client configuration file. Defaults to {@link
   * #DEFAULT_HDFS_CLIENT_CONFIG_FILE}.
   */
  public static Long hdfsClientConfigFile;

  public static String getHDFSClientConfigFile() {
    Long key = hdfsClientConfigFile;
    return tasktab().stringAt(key, tab().stringAt(key,
                              DEFAULT_HDFS_CLIENT_CONFIG_FILE));
  }

//------------------------------------------------------------------------------

  /**
   * Whether to flush HDFS AEQs. Defaults to false.
   */
  public static Long hdfsFlushQueues;

  public static boolean hdfsFlushQueues() {
    Long key = hdfsFlushQueues;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * Whether to force major compaction on HDFS. Defaults to false.
   */
  public static Long hdfsForceCompaction;

  public static boolean hdfsForceCompaction() {
    Long key = hdfsForceCompaction;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * Whether this is an HA test. Defaults to false. Use this flag to determine
   * whether to tolerate certain exceptions, such as closing a connection after
   * a server has failed.
   */
  public static Long isHA;

  public static boolean isHA() {
    Long key = isHA;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * Whether to log individual DML operations. Defaults to false.
   */
  public static Long logDML;

  public static boolean logDML() {
    Long key = logDML;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (int)
   * Partitioned table redundancy. Applies only to partitioned tables.
   * Defaults to 1.
   */
  public static Long partitionRedundancy;

  public static int getPartitionRedundancy() {
    Long key = partitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to use synchronous writes for persistent tables. Defaults to true.
   */
  public static Long persistSynchronous;

  public static boolean persistSynchronous() {
    Long key = persistSynchronous;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to query HDFS. For use as a query hint. Applies only to partitioned
   * tables where {@link #useHDFS} is true. Defaults to false.
   */
  public static Long queryHDFS;

  public static boolean queryHDFS() {
    Long key = queryHDFS;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Frequency with which to generate query plans, in seconds. Defaults to 0
   * (no query plans are generated).
   */
  public static Long queryPlanFrequency;

  public static int getQueryPlanFrequency() {
    Long key = queryPlanFrequency;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Maximum number of threads to generate query plans. Defaults to 1.
   */
  public static Long queryPlanners;

  public static int getQueryPlanners() {
    Long key = queryPlanners;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of threads doing the workload. Used to pass this information to
   * server threads for monitoring the workload.
   */
  public static Long threadCount;

  public static int getThreadCount() {
    Long key = threadCount;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val == -1) {
      String s = BasePrms.nameForKey(threadCount) + " is missing";
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * Transaction isolation. Defaults to "none".
   */
  public static Long txIsolation;

  public static int getTxIsolation() {
    Long key = txIsolation;
    String val = tasktab().stringAt(key, tab().stringAt(key, "none"));
    return getTxIsolation(key, val);
  }

  private static int getTxIsolation(Long key, String val) {
    if (val.equalsIgnoreCase("none")) {
      return TRANSACTION_NONE;
    }
    else {
      if (val.equalsIgnoreCase("read_uncommitted") ||
          val.equalsIgnoreCase("readUncommitted")) {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
      }
      else if (val.equalsIgnoreCase("read_committed") ||
               val.equalsIgnoreCase("readCommitted")) {
        return Connection.TRANSACTION_READ_COMMITTED;
      }
      else if (val.equalsIgnoreCase("repeatable_read") ||
               val.equalsIgnoreCase("repeatableRead")) {
        return Connection.TRANSACTION_REPEATABLE_READ;
      }
      else if (val.equalsIgnoreCase("serializable")) {
        return Connection.TRANSACTION_SERIALIZABLE;
      }
      else {
        String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
        throw new HydraConfigException(s);
      }
    }
  }

  public static String getTxIsolation(int n) {
    switch (n) {
      case TRANSACTION_NONE:
           return "none";
      case Connection.TRANSACTION_READ_UNCOMMITTED:
           return "read_uncommitted";
      case Connection.TRANSACTION_READ_COMMITTED:
           return "read_committed";
      case Connection.TRANSACTION_REPEATABLE_READ:
           return "repeatable_read";
      case Connection.TRANSACTION_SERIALIZABLE:
           return "serializable";
      default:
        String s = "Unknown transaction isolation level: " + n;
        throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to store data in HDFS. Applies only to partitioned tables.
   * Defaults to false.
   */
  public static Long useHDFS;

  public static boolean useHDFS() {
    Long key = useHDFS;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to make HDFS data write-only. Applies only to partitioned tables
   * where {@link #useHDFS} is true. Defaults to false.
   */
  public static Long useHDFSWriteOnly;

  public static boolean useHDFSWriteOnly() {
    Long key = useHDFSWriteOnly;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * Whether to use PUT DML. Defaults to false.
   */
  public static Long usePutDML;

  public static boolean usePutDML() {
    Long key = usePutDML;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * Whether to use a global index by partitioning on non-primary key FIELD0.
   * Defaults to false.
   * <p>
   * To avoid updates on the partitioning key, also set {@link
   * gfxdperf.ycsb.core.workloads.CoreWorkloadPrms#fieldStart} to a value
   * greater than 0 but less than {@link
   * gfxdperf.ycsb.core.workloads.CoreWorkloadPrms#fieldCount}.
   */
  public static Long useGlobalIndex;

  public static boolean useGlobalIndex() {
    Long key = useGlobalIndex;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
}
