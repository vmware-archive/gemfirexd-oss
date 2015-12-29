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
package gfxdperf.tpch.gfxd;

import hydra.BasePrms;
import hydra.HydraConfigException;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * A class used to store keys for test configuration settings.
 */
public class GFXDPrms extends BasePrms {
  
  static {
    setValues(GFXDPrms.class);
  }

  public static final String ALL = "ALL";
  public static final int TRANSACTION_NONE = 0;

  private static final String DEFAULT_HDFS_CLIENT_CONFIG_FILE =
    "$JTESTS/gfxdperf/tpch/gfxd/gfxd-client-config.xml";

  public static enum ConnectionType {
    peer, thin
    ;
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
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
   * connections in threads doing TPCH workloads.
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
   * (boolean)
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
   * (boolean)
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
   * (boolean)
   * Whether to flush HDFS AEQs. Defaults to false.
   */
  public static Long hdfsFlushQueues;

  public static boolean hdfsFlushQueues() {
    Long key = hdfsFlushQueues;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to force major compaction on HDFS. Defaults to false.
   */
  public static Long hdfsForceCompaction;

  public static boolean hdfsForceCompaction() {
    Long key = hdfsForceCompaction;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (List of String(s))
   * Tables to make HDFS-enabled. Defaults to an empty list.
   * Can be specified as {@link #ALL} to include all tables.
   */
  public static Long hdfsTables;

  public static List<String> getHDFSTables() {
    Long key = hdfsTables;
    return asTablesList(tab().vecAt(key, null));
  }

//------------------------------------------------------------------------------

  /**
   * (List of String(s))
   * Indexes to add. Defaults to null.
   */
  public static Long indexes;

  public static List<String> getIndexes() {
    Long key = indexes;
    Vector v = tab().vecAt(key, null);
    List<String> l = new ArrayList();
    if (v != null) {
      l.addAll(v);
    }
    return l;
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
   * (List of String(s))
   * Tables to make offheap-enabled. Defaults to an empty list.
   * Can be specified as {@link #ALL} to include all tables.
   */
  public static Long offHeapTables;

  public static List<String> getOffHeapTables() {
    Long key = offHeapTables;
    return asTablesList(tab().vecAt(key, null));
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
   * (List of String(s))
   * Tables to make persistent. Defaults to an empty list.
   * Can be specified as {@link #ALL} to include all tables.
   */
  public static Long persistentTables;

  public static List<String> getPersistentTables() {
    Long key = persistentTables;
    return asTablesList(tab().vecAt(key, null));
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
   * (no query plans are generated). -1 means generate plans for all queries.
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
   * (boolean)
   * Whether to skip constraint checks and use PUT DML for inserts. Defaults to
   * false. Typically used when importing data.
   */
  public static Long skipConstraintChecks;

  public static boolean skipConstraintChecks() {
    Long key = skipConstraintChecks;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to skip listeners for inserts. Defaults to false. Typically used
   * when importing data.
   */
  public static Long skipListeners;

  public static boolean skipListeners() {
    Long key = skipListeners;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * (String)
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
        String s = "Illegal value for " + nameForKey(key) + ": " + val;
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
// Support methods

  private static List<String> asTablesList(Vector v) {
    List<String> tables = new ArrayList();
    if (v != null) {
      tables.addAll(v);
    }
    for (String table : tables) {
      if (table.equalsIgnoreCase(ALL)) {
        // this is all we need to know
        tables = new ArrayList();
        tables.add(ALL);
        return tables;
      }
    }
    return tables;
  }
}
