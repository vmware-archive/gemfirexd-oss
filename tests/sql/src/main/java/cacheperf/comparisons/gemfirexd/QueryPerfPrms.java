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

package cacheperf.comparisons.gemfirexd;

import hydra.BasePrms;
import hydra.HostHelper;
import hydra.HydraConfigException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import objects.query.QueryPrms;

/**
 *  A class used to store keys for test configuration settings.
 */
public class QueryPerfPrms extends BasePrms {

  public static final String DEFAULT_DATABASE_NAME = "perfTest";
  public static final String DEFAULT_DATABASE_SERVER_HOST = "localhost";
  public static final int DEFAULT_DATABASE_SERVER_PORT = 3306;
  public static final String DEFAULT_USER = "root";
  public static final String DEFAULT_PASSWORD = "gemstone-test";

  public static final int TRANSACTION_NONE = 0;

  /**
   * Whether to enable per-connection statement-level statistics.
   * Defaults to false.
   * @see enableTimeStats
   */
  public static Long enableStats;
  public static boolean enableStats() {
    Long key = enableStats;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to enable per-connection statement-level time statistics.
   * Defaults to false.  This has no effect when {@link enableStats} is false.
   */
  public static Long enableTimeStats;
  public static boolean enableTimeStats() {
    Long key = enableTimeStats;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to enable memory stats.  If set true, the VM must not be run with
   * -XX:+DisableExplicitGC.  Defaults to false.
   */
  public static Long enableMemoryStats;
  public static boolean enableMemoryStats() {
    Long key = enableMemoryStats;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    if (val) {
      List inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
      if (inputArgs.contains("-XX:+DisableExplicitGC")) {
        String s = BasePrms.nameForKey(enableMemoryStats) + "=" + val
                 + " is incompatible with -XX:+DisableExplicitGC."
                 + " Add \"hydra.VmPrms-extraVMArgs += -XX:-DisableExplicitGC\""
                 + " to the local.conf file and try again.";
        throw new HydraConfigException(s);
      }
    }
    return val;
  }
 
  /**
   * Frequency with which to generate query plans, in seconds.
   * Defaults to 0 (no plans are generated).
   */
  public static Long queryPlanFrequency;
  public static long getQueryPlanFrequency() {
    Long key = queryPlanFrequency;
    return tasktab().longAt(key, tab().longAt(key, 0));
  }
  
  /**
   * Maximum number of threads to generate query plans.  Defaults to 1.
   */
  public static Long maxQueryPlanners;
  public static int getMaxQueryPlanners() {
    Long key = maxQueryPlanners;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }
 
  /**
   * Whether to create indexes as configured.  Defaults to true.
   */
  public static Long createIndexes;
  public static boolean createIndexes() {
    Long key = createIndexes;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
 
  /**
   * Database name used in connection URL.  Defaults to {@link
   * #DEFAULT_DATABASE_NAME}.
   */
  public static Long databaseName;
  public static String getDatabaseName() {
    Long key = databaseName;
    return tasktab().stringAt(key, tab().stringAt(key, DEFAULT_DATABASE_NAME));
  }

  /**
   * Database server host used in connection URL.  Defaults to {@link
   * #DEFAULT_DATABASE_SERVER_HOST}.  Also uses the default value if the
   * server host is the local host.
   */
  public static Long databaseServerHost;
  public static String getDatabaseServerHost() {
    Long key = databaseServerHost;
    String host = tasktab().stringAt(key, tab().stringAt(key,
                                          DEFAULT_DATABASE_SERVER_HOST));
    return HostHelper.isLocalHost(host) ? "localhost" : host;
  }

  /**
   * Database server port used in connection URL.  Defaults to {@link
   * #DEFAULT_DATABASE_SERVER_PORT}.
   */
  public static Long databaseServerPort;
  public static int getDatabaseServerPort() {
    Long key = databaseServerPort;
    return tasktab().intAt(key, tab().intAt(key,
                                DEFAULT_DATABASE_SERVER_PORT));
  }

  /**
   * User used in connection URL.  Defaults to {@link #DEFAULT_USER}.
   */
  public static Long user;
  public static String getUser() {
    Long key = user;
    return tasktab().stringAt(key, tab().stringAt(key, DEFAULT_USER));
  }

  /**
   * Password used in connection URL.  Defaults to {@link #DEFAULT_PASSWORD}.
   * Can be specified as {@link #NONE}, in which case no password is used.
   */
  public static Long password;
  public static String getPassword() {
    Long key = password;
    String val = tasktab().stringAt(key, tab().stringAt(key, DEFAULT_PASSWORD));
    return val.equalsIgnoreCase(BasePrms.NONE) ? null : val;
  }

  /**
   * Eviction heap percentage to use in the SYS.SET_EVICTION_HEAP_PERCENTAGE
   * system call.  Defaults to 0 which does nothing.
   */
  public static Long evictionHeapPercentage;
  public static int getEvictionHeapPercentage() {
    Long key = evictionHeapPercentage;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0 || val > 100) {
      String s = BasePrms.nameForKey(key) + " must be between 0 and 100: "
               + val;
      throw new HydraConfigException(s);
    }
    return val;
  }
  
  /**
   * DB synchronizer name.  Defaults to null.
   */
  public static Long dbSynchronizerName;
  public static String getDBSynchronizerName() {
    Long key = dbSynchronizerName;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * DB synchronizer configuration.  Defaults to the empty string.
   */
  public static Long dbSynchronizerConfig;
  public static String getDBSynchronizerConfig() {
    Long key = dbSynchronizerConfig;
    return tasktab().stringAt(key, tab().stringAt(key, ""));
  }

  /**
   * DB synchronizer server groups.  Defaults to null.
   */
  public static Long dbSynchronizerServerGroups;
  public static String getDBSynchronizerServerGroups() {
    Long key = dbSynchronizerServerGroups;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (String)
   * The backend database API for the DB synchronizer (e.g., MYSQL, ORACLE).
   */
  public static Long dbapi;
  public static int getDBAPI() {
    Long key = dbapi;
    String val = tab().stringAt(key);
    if (val.equalsIgnoreCase(QueryPrms.MYSQL_API)) {
      return QueryPrms.MYSQL;
    } else if (val.equalsIgnoreCase(QueryPrms.MYSQLC_API)) {
      return QueryPrms.MYSQLC;
    } else if (val.equalsIgnoreCase(QueryPrms.ORACLE_API)) {
      return QueryPrms.ORACLE;
    } else if (val.equalsIgnoreCase(QueryPrms.GPDB_API)) {
      return QueryPrms.GPDB;
    } else {
      String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Percentage of updates to do in mixed put/update task.  Defaults to 0.
   */
  public static Long updatePercentage;
  public static int getUpdatePercentage() {
    Long key = updatePercentage;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
  
  /**
   * Transaction isolation.  Defaults to TRANSACTION_NONE.
   */
  public static Long txIsolation;
  public static int getTxIsolation() {
    Long key = txIsolation;
    String val = tasktab().stringAt(key, tab().stringAt(key, "none"));
    return getTxIsolation(key, val);
  }
  private static int getTxIsolation(Long key, String val) {
    if (val.equalsIgnoreCase("transaction_none") ||
        val.equalsIgnoreCase("transactionNone") ||
        val.equalsIgnoreCase("none")) {
      return TRANSACTION_NONE;
    }
    else {
      if (val.equalsIgnoreCase("transaction_read_uncommitted") ||
          val.equalsIgnoreCase("transactionReadUncommitted") ||
          val.equalsIgnoreCase("read_uncommitted") ||
          val.equalsIgnoreCase("readUncommitted")) {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
      }
      else if (val.equalsIgnoreCase("transaction_read_committed") ||
               val.equalsIgnoreCase("transactionReadCommitted") ||
               val.equalsIgnoreCase("read_committed") ||
               val.equalsIgnoreCase("readCommitted")) {
        return Connection.TRANSACTION_READ_COMMITTED;
      }
      else if (val.equalsIgnoreCase("transaction_repeatable_read") ||
               val.equalsIgnoreCase("transactionRepeatableRead") ||
               val.equalsIgnoreCase("repeatable_read") ||
               val.equalsIgnoreCase("repeatableRead")) {
        return Connection.TRANSACTION_REPEATABLE_READ;
      }
      else if (val.equalsIgnoreCase("transaction_serializable") ||
               val.equalsIgnoreCase("transactionSerializable") ||
               val.equalsIgnoreCase("serializable")) {
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
           return "transaction_read_uncommitted";
      case Connection.TRANSACTION_READ_COMMITTED:
           return "transaction_read_committed";
      case Connection.TRANSACTION_REPEATABLE_READ:
           return "transaction_repeatable_read";
      case Connection.TRANSACTION_SERIALIZABLE:
           return "transaction_serializable";
      default:
        String s = "Unknown transaction isolation level: " + n;
        throw new QueryPerfException(s);
    }
  }

  /**
   * (boolean)
   * Whether to check data load balance in {@link QueryPerfClient
   * #checkDataLoadTask}.  Defaults to true.
   */
  public static Long checkDataLoadEnabled;
  public static boolean checkDataLoadEnabled() {
    Long key = checkDataLoadEnabled;
    return tab().booleanAt(key, true);
  }

  /**
   * (List of String(s))
   * Sizer hints to set in {@link QueryPerfClient#reportMemoryAnalyticsTask}.
   * Defaults to null.
   * <p>
   * For example:
   * <code>
   *   cacheperf.comparisons.gemfirexd.QueryPerfPrms-sizerHints =
   *             withMemoryFootPrint
   *             ;
   * </code>
   */
  public static Long sizerHints;
  public static List<String> getSizerHints() {
    Long key = sizerHints;
    Vector vals = tasktab().vecAt(key, tab().vecAt(key, null));
    List<String> flags = null;
    if (vals != null) {
      flags = new ArrayList();
      flags.addAll(vals);
    }
    return flags;
  }
 
  /**
   * (List of "property=value" String(s))
   * System properties and their values to set in {@link QueryPerfClient
   * #configureDebuggingTask}.  Defaults to null.
   * <p>
   * For example:
   * <code>
   *   cacheperf.comparisons.gemfirexd.QueryPerfPrms-systemProperties =
   *             "DistributionManager.VERBOSE=true"
   *             "TXStateProxy.LOG_FINE=false"
   *             ;
   * </code>
   */
  public static Long systemProperties;
  public static Map<String,String> getSystemProperties() {
    Long key = systemProperties;
    Vector vals = tasktab().vecAt(key, tab().vecAt(key, null));
    Map<String,String> props = null;
    if (vals != null) {
      props = new HashMap();
      for (Iterator i = vals.iterator(); i.hasNext();) {
        String val = (String)i.next();
        String[] propval = val.split("=");
        if (propval.length != 2) {
          String s = BasePrms.nameForKey(key)
              + " contains a value that is not of the form \"property=value\": "
              + val;
          throw new HydraConfigException(s);
        }
        props.put(propval[0], propval[1]);
      }
    }
    return props;
  }
 
  /**
   * (List of String(s))
   * Sanity manager trace flags to set in {@link QueryPerfClient
   * #configureDebuggingTask}.  Defaults to null.
   * <p>
   * For example:
   * <code>
   *   cacheperf.comparisons.gemfirexd.QueryPerfPrms-traceFlags =
   *             DumpOptimizedTree
   *             LockTrace
   *             ;
   * </code>
   */
  public static Long traceFlags;
  public static List<String> getTraceFlags() {
    Long key = traceFlags;
    Vector vals = tasktab().vecAt(key, tab().vecAt(key, null));
    List<String> flags = null;
    if (vals != null) {
      flags = new ArrayList();
      flags.addAll(vals);
    }
    return flags;
  }
 
  /**
   * (boolean)
   * Whether to use data created in a previous test.  Defaults to false.
   * Not for use as a task attribute.
   */
  public static Long useExistingData;
  public static boolean useExistingData() {
    Long key = useExistingData;
    boolean val = tab().booleanAt(key, false);
    if (val
        && QueryPrms.getAPI() != QueryPrms.MYSQL
        && QueryPrms.getAPI() != QueryPrms.MYSQLC
        && QueryPrms.getAPI() != QueryPrms.ORACLE
        && QueryPrms.getAPI() != QueryPrms.GPDB) {
      String s = " Unsupported query API: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * Whether to use network locator or network server endpoints when connecting.
   * Defaults to true.
   */
  public static Long useNetworkLocator;
  public static boolean useNetworkLocator() {
    Long key = useNetworkLocator;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
 
  /**
   * Logical name of hydra client to kill.
   */
  public static Long killTarget;
  public static String getKillTarget() {
    Long key = killTarget;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * Whether this is an HA test.
   * Defaults to false.
   */
  public static Long isHA;
  public static boolean isHA() {
    Long key = isHA;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to retry when starting a fabric server when it fails due to XBM09.
   * Defaults to false.
   */
  public static Long retryOnXBM09;
  public static boolean retryOnXBM09() {
    Long key = retryOnXBM09;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Number of seconds to sleep before a shutdown-all. Defaults to 0, which does
   * not sleep.
   */
  public static Long sleepBeforeShutdownSec;
  public static int getSleepBeforeShutdownSec() {
    Long key = sleepBeforeShutdownSec;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
 
  static {
    setValues( QueryPerfPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
