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

package cacheperf;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;

import distcache.DistCache;

import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class CachePerfStats extends PerformanceStatistics {

  /** <code>CachePerfStats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  public static final String OPS = "operations";
  public static final String OP_TIME = "operationTime";
  public static final String OP_TIME1 = "operationTime1";
  public static final String OP_TIME2 = "operationTime2";
  public static final String OP_TIME3 = "operationTime3";
  public static final String CACHEOPENS = "cacheOpens";
  public static final String CACHEOPEN_TIME = "cacheOpenTime";
  public static final String CACHECLOSES = "cacheCloses";
  public static final String CACHECLOSE_TIME = "cacheCloseTime";
  public static final String REGISTERINTERESTS = "registerInterests";
  public static final String REGISTERINTEREST_TIME = "registerInterestTime";
  public static final String CREATES = "creates";
  public static final String CREATE_TIME = "createTime";
  public static final String CREATEKEYS = "createKeys";
  public static final String CREATEKEY_TIME = "createKeyTime";
  public static final String PUTALLS = "putAlls";
  public static final String PUTS = "puts";
  public static final String PUT_TIME = "putTime";
  public static final String GETALLS = "getAlls";
  public static final String GETS = "gets";
  public static final String GET_TIME = "getTime";
  public static final String UPDATES = "updates";
  public static final String UPDATE_TIME = "updateTime";
  public static final String COMBINEDPUTGETS = "combinedPutGets";
  public static final String COMBINEDPUTGET_TIME = "combinedPutGetTime";
  public static final String PUTCOMPARISONS = "putComparisons";
  public static final String PUTCOMPARISON_TIME = "putComparisonTime";
  public static final String GETCOMPARISONS = "getComparisons";
  public static final String GETCOMPARISON_TIME = "getComparisonTime";
  public static final String DESTROYS = "destroys";
  public static final String DESTROY_TIME = "destroyTime";
  public static final String CREATE_EVENTS      = "createEvents";
  public static final String DESTROY_EVENTS     = "destroyEvents";
  public static final String UPDATE_EVENTS      = "updateEvents";
  public static final String UPDATE_LATENCY     = "updateLatency";
  public static final String NULLS = "nulls";
  public static final String LATENCY_SPIKES      = "latencySpikes";
  public static final String NEGATIVE_LATENCIES  = "negativeLatencies";

  public static final String USECASE12_UPDATES = "useCase12Updates";
  public static final String USECASE12_UPDATE_TIME = "useCase12UpdatesTime";
  public static final String CONNECTS = "connects";
  public static final String CONNECT_TIME = "connectTime";
  public static final String DISCONNECTS = "disconnects";
  public static final String DISCONNECT_TIME = "disconnectTime";
  public static final String QUERIES = "queries";
  public static final String QUERY_RESULTS = "querieResults";
  public static final String QUERY_TIME = "queryTime";
  public static final String REBALANCES = "rebalances";
  public static final String REBALANCE_TIME = "rebalanceTime";
  public static final String RECOVERIES = "recoveries";
  public static final String RECOVERY_TIME = "recoveryTime";
  public static final String LOCKS = "locks";
  public static final String LOCK_TIME = "lockTime";
  public static final String GATEWAY_QUEUE_DRAINS = "gatewayQueueDrains";
  public static final String GATEWAY_QUEUE_DRAIN_TIME = "gatewayQueueDrainTime";
  public static final String LOCALRECOVERY_TIME = "localRecoveryTime";
  public static final String REMOTERECOVERY_TIME = "remoteRecoveryTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>CachePerfStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntGauge
        (
          VM_COUNT,
          "When aggregated, the number of VMs using this statistics object.",
          "VMs"
        ),
      factory().createIntCounter
        ( 
         OPS,
         "Number of operations completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OP_TIME,
         "Total time spent doing operations.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OP_TIME1,
         "Total time spent connecting DS.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OP_TIME2,
         "Total time spent creating cache and closing it.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createLongCounter
        ( 
         OP_TIME3,
         "Total time spent disconnecting DS.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CACHEOPENS,
         "Number of cache opens completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         CACHEOPEN_TIME,
         "Total time spent opening caches.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CACHECLOSES,
         "Number of cache closes completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         CACHECLOSE_TIME,
         "Total time spent closing caches.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         REGISTERINTERESTS,
         "Number of register interests completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         REGISTERINTEREST_TIME,
         "Total time spent registering interest.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CREATES,
         "Number of creates completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         CREATE_TIME,
         "Total time spent doing creates.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CREATEKEYS,
         "Number of createKeys completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         CREATEKEY_TIME,
         "Total time spent doing createKeys.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         PUTALLS,
         "Number of putAlls completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createIntCounter
        ( 
         PUTS,
         "Number of puts completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         PUT_TIME,
         "Total time spent doing puts.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         COMBINEDPUTGETS,
         "Number of combined put and get operations completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         COMBINEDPUTGET_TIME,
         "Total time spent doing combined put and gets.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         GETALLS,
         "Number of getAlls completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createIntCounter
        ( 
         GETS,
         "Number of gets completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         GET_TIME,
         "Total time spent doing gets.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         UPDATES,
         "Number of updates completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         UPDATE_TIME,
         "Total time spent doing updates.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         PUTCOMPARISONS,
         "Number of put comparisons completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         PUTCOMPARISON_TIME,
         "Total time spent doing put comparisons.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         GETCOMPARISONS,
         "Number of get comparisons completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         GETCOMPARISON_TIME,
         "Total time spent doing get comparisons.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         DESTROYS,
         "Number of destroys completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         DESTROY_TIME,
         "Total time spent doing destroys.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CREATE_EVENTS,
         "Number of create events.",
         "events",
	 largerIsBetter
         ),
      factory().createIntCounter
        ( 
         DESTROY_EVENTS,
         "Number of destroy events.",
         "events",
	 largerIsBetter
         ),
      factory().createIntCounter
        ( 
         UPDATE_EVENTS,
         "Number of update events.",
         "events",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         UPDATE_LATENCY,
         "Latency of update operations.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         NULLS,
         "Number of null objects received.",
         "nulls",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         LATENCY_SPIKES,
         "Number of latency spikes.",
         "spikes",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         NEGATIVE_LATENCIES,
         "Number of negative latencies (caused by insufficient clock skew correction).",
         "negatives",
	 !largerIsBetter
         ),
      factory().createIntCounter
        (
         USECASE12_UPDATES,
         "Number of UseCase12 updates completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        (
         USECASE12_UPDATE_TIME,
         "Total time spent doing UseCase12 updates",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         CONNECTS,
         "Number of connects completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         CONNECT_TIME,
         "Total time spent doing connects.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         DISCONNECTS,
         "Number of disconnects completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         DISCONNECT_TIME,
         "Total time spent doing disconnects.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         QUERIES,
         "Number of queries completed.",
         "operations",
         largerIsBetter
         ),
      factory().createIntCounter
        ( 
         QUERY_RESULTS,
         "Size of query result set.",
         "entries",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         QUERY_TIME,
         "Total time spent doing queries.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         REBALANCES,
         "Number of rebalances completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         REBALANCE_TIME,
         "Total time spent doing rebalances.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         RECOVERIES,
         "Number of recoveries completed.",
         "operations",
         largerIsBetter
         ),
      factory().createLongCounter
        ( 
         RECOVERY_TIME,
         "Total time spent doing recoveries.",
         "nanoseconds",
         !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         LOCKS,
         "Number of locks completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         LOCK_TIME,
         "Total time spent doing locks.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createIntCounter
        ( 
         GATEWAY_QUEUE_DRAINS,
         "Number of gateway queue drains completed.",
         "operations",
	 largerIsBetter
         ),
      factory().createLongCounter
        ( 
         GATEWAY_QUEUE_DRAIN_TIME,
         "Total time spent waiting for gateway queues to drain.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createLongCounter
        ( 
         LOCALRECOVERY_TIME,
         "Total time spent recovering from disk.",
         "nanoseconds",
	 !largerIsBetter
         ),
      factory().createLongCounter
        ( 
         REMOTERECOVERY_TIME,
         "Total time spent recovering from a remote VM (disk recovery with gii correlation).",
         "nanoseconds",
	 !largerIsBetter
        )
    };
  }

  public static CachePerfStats getInstance() {
    CachePerfStats cps =
         (CachePerfStats)getInstance(CachePerfStats.class, SCOPE);
    cps.incVMCount();
    return cps;
  }
  public static CachePerfStats getInstance(int scope) {
    CachePerfStats cps =
         (CachePerfStats)getInstance(CachePerfStats.class, scope);
    cps.incVMCount();
    return cps;
  }
  public static CachePerfStats getInstance(String name) {
    CachePerfStats cps =
         (CachePerfStats)getInstance(CachePerfStats.class, SCOPE, name);
    cps.incVMCount();
    return cps;
  }
  public static CachePerfStats getInstance( String name, String trimspecName ) {
    CachePerfStats cps =
         (CachePerfStats)getInstance(CachePerfStats.class, SCOPE, name, trimspecName);
    cps.incVMCount();
    return cps;
  }

  /////////////////// Construction / initialization ////////////////

  public CachePerfStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

   public int getOps() {
     return statistics().getInt(OPS);
   }
   public long getOpTime() {
     return statistics().getLong(OP_TIME);
   }
   public int getCacheOpens() {
     return statistics().getInt(CACHEOPENS);
   }
   public long getCacheOpenTime() {
     return statistics().getLong(CACHEOPEN_TIME);
   }
   public int getCacheCloses() {
     return statistics().getInt(CACHECLOSES);
   }
   public long getCacheCloseTime() {
     return statistics().getLong(CACHECLOSE_TIME);
   }
   public int getRegisterInterests() {
     return statistics().getInt(REGISTERINTERESTS);
   }
   public long getRegisterInterestTime() {
     return statistics().getLong(REGISTERINTEREST_TIME);
   }
   public int getCreates() {
     return statistics().getInt(CREATES);
   }
   public long getCreateTime() {
     return statistics().getLong(CREATE_TIME);
   }
   public int getCreateKeys() {
     return statistics().getInt(CREATEKEYS);
   }
   public long getCreateKeyTime() {
     return statistics().getLong(CREATEKEY_TIME);
   }
   public int getPuts() {
     return statistics().getInt(PUTS);
   }
   public long getPutTime() {
     return statistics().getLong(PUT_TIME);
   }
   public int getCombinedPutGets() {
     return statistics().getInt(COMBINEDPUTGETS);
   }
   public long getCombinedPutGetTime() {
     return statistics().getLong(COMBINEDPUTGET_TIME);
   }
   public int getGets() {
     return statistics().getInt(GETS);
   }
   public long getGetTime() {
     return statistics().getLong(GET_TIME);
   }
   public int getUpdates() {
     return statistics().getInt(UPDATES);
   }
   public long getUpdateTime() {
     return statistics().getLong(UPDATE_TIME);
   }
   public int getPutComparisons() {
     return statistics().getInt(PUTCOMPARISONS);
   }
   public long getPutComparisonTime() {
     return statistics().getLong(PUTCOMPARISON_TIME);
   }
   public int getGetComparisons() {
     return statistics().getInt(GETCOMPARISONS);
   }
   public long getGetComparisonTime() {
     return statistics().getLong(GETCOMPARISON_TIME);
   }
   public int getDestroys() {
     return statistics().getInt(DESTROYS);
   }
   public long getDestroyTime() {
     return statistics().getLong(DESTROY_TIME);
   }
   public int getNulls() {
     return statistics().getInt(NULLS);
   }
   public int getLatencySpikes() {
       return statistics().getInt(LATENCY_SPIKES);
   }  
   public int getNegativeLatencies() {
       return statistics().getInt(NEGATIVE_LATENCIES);
   }  
   public int getUseCase12Updates() {
     return statistics().getInt(USECASE12_UPDATES);
   }
   public long getUseCase12UpdateTime() {
     return statistics().getLong(USECASE12_UPDATE_TIME);
   }
   public int getConnects() {
     return statistics().getInt(CONNECTS);
   }
   public long getConnectTime() {
     return statistics().getLong(CONNECT_TIME);
   }
   public int getDisConnects() {
     return statistics().getInt(DISCONNECTS);
   }
   public long getDisConnectTime() {
     return statistics().getLong(DISCONNECT_TIME);
   }
   public int getQueries() {
     return statistics().getInt(QUERIES);
   }
   public int getQueryResults() {
     return statistics().getInt(QUERY_RESULTS);
   }
   public long getQueryTime() {
     return statistics().getLong(QUERY_TIME);
   }
   public int getRebalances() {
     return statistics().getInt(REBALANCES);
   }
   public long getRebalanceTime() {
     return statistics().getLong(REBALANCE_TIME);
   }
   public int getRecoveries() {
     return statistics().getInt(RECOVERIES);
   }
   public long getRecoveryTime() {
     return statistics().getLong(RECOVERY_TIME);
   }
   public int getLocks() {
     return statistics().getInt(LOCKS);
   }
   public long getLockTime() {
     return statistics().getLong(LOCK_TIME);
   }
   public int getGatewayQueueDrains() {
     return statistics().getInt(GATEWAY_QUEUE_DRAINS);
   }
   public long getGatewayQueueDrainTime() {
     return statistics().getLong(GATEWAY_QUEUE_DRAIN_TIME);
   }
  
  /////////////////// Updating stats /////////////////////////

  /**
   * increase the time on the optional histogram by the supplied amount
   */
  public void incHistogram(HistogramStats histogram, long amount) {
    if (histogram != null) {
      histogram.incBin(amount);
    }
  }

  /**
   * increase the count on the vmCount
   */
  private synchronized void incVMCount() {
    if (!VMCounted) {
      statistics().incInt(VM_COUNT, 1);
      VMCounted = true;
    }
  }
  private static boolean VMCounted = false;

  /**
   * sets the count on the ops to the supplied amount
   * (for statless clients only)
   */
  protected void setOps(int amount) {
    statistics().setInt(OPS, amount);
  }
  /**
   * sets the time on the ops to the supplied amount
   * (for statless clients only)
   */
  protected void setOpTime(long amount) {
    statistics().setLong(OP_TIME, amount);
  }
  protected void setOpTime1(long amount) {
    statistics().setLong(OP_TIME1, amount);
  }
  protected void setOpTime2(long amount) {
    statistics().setLong(OP_TIME2, amount);
  }
  protected void setOpTime3(long amount) {
    statistics().setLong(OP_TIME3, amount);
  }

  /**
   * increase the count on the cache opens
   */
  public void incCacheOpens(boolean isMainWorkload) {
    incCacheOpens(1, isMainWorkload);
  }
  /**
   * increase the count on the cache opens by the supplied amount
   */
  public void incCacheOpens(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(CACHEOPENS, amount);
  }
  /**
   * increase the time on the cache opens by the supplied amount
   */
  public void incCacheOpenTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(CACHEOPEN_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the cache open
   */
  public long startCacheOpen() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the cache open started 
   */
  public void endCacheOpen(long start, boolean isMainWorkload, DistCache theCache,
                           HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCacheOpens(1, isMainWorkload);
    incCacheOpenTime(elapsed, isMainWorkload);
    CachePerfStatsVersion.endDiskRecovery(elapsed, theCache, statistics());
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the cache closes
   */
  public void incCacheCloses(boolean isMainWorkload) {
    incCacheCloses(1, isMainWorkload);
  }
  /**
   * increase the count on the cache closes by the supplied amount
   */
  public void incCacheCloses(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(CACHECLOSES, amount);
  }
  /**
   * increase the time on the cache closes by the supplied amount
   */
  public void incCacheCloseTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(CACHECLOSE_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the cache close
   */
  public long startCacheClose() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the cache close started 
   */
  public void endCacheClose(long start, boolean isMainWorkload,
                            HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCacheCloses(1, isMainWorkload);
    incCacheCloseTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the register interests
   */
  public void incRegisterInterests(boolean isMainWorkload) {
    incRegisterInterests(1, isMainWorkload);
  }
  /**
   * increase the count on the register interests by the supplied amount
   */
  public void incRegisterInterests(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(REGISTERINTERESTS, amount);
  }
  /**
   * increase the time on the register interests by the supplied amount
   */
  public void incRegisterInterestTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(REGISTERINTEREST_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the register interest
   */
  public long startRegisterInterest() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the register interest started 
   */
  public void endRegisterInterest(long start, boolean isMainWorkload,
                                  HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incRegisterInterests(1, isMainWorkload);
    incRegisterInterestTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the creates
   */
  public void incCreates(boolean isMainWorkload) {
    incCreates(1, isMainWorkload);
  }
  /**
   * increase the count on the creates by the supplied amount
   */
  public void incCreates(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(CREATES, amount);
  }
  /**
   * increase the time on the creates by the supplied amount
   */
  public void incCreateTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(CREATE_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the create
   */
  public long startCreate() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the create started 
   */
  public void endCreate(long start, boolean isMainWorkload,
                        HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCreates(1, isMainWorkload);
    incCreateTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the create started 
   */
  public void endCreate(long start, int amount, boolean isMainWorkload,
                        HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCreates(amount, isMainWorkload);
    incCreateTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the createKeys
   */
  public void incCreateKeys(boolean isMainWorkload) {
    incCreateKeys(1, isMainWorkload);
  }
  /**
   * increase the count on the createKeys by the supplied amount
   */
  public void incCreateKeys(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(CREATEKEYS, amount);
  }
  /**
   * increase the time on the createKeys by the supplied amount
   */
  public void incCreateKeyTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(CREATEKEY_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the createKey
   */
  public long startCreateKey() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the createKey started 
   */
  public void endCreateKey(long start, boolean isMainWorkload,
                           HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCreateKeys(1, isMainWorkload);
    incCreateKeyTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the putAlls
   */
  public void incPutAlls() {
    statistics().incInt(PUTALLS, 1);
  }
  /**
   * increase the count on the puts
   */
  public void incPuts(boolean isMainWorkload) {
    incPuts(1, isMainWorkload);
  }
  /**
   * increase the count on the puts by the supplied amount
   */
  public void incPuts(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(PUTS, amount);
  }
  /**
   * increase the time on the puts by the supplied amount
   */
  public void incPutTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(PUT_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the put
   */
  public long startPut() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the put started 
   */
  public void endPut(long start, boolean isMainWorkload,
                     HistogramStats histogram) {
    endPut(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the put started 
   */
  public void endPut(long start, int amount, boolean isMainWorkload,
                     HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incPuts(amount, isMainWorkload);
    incPutTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the put started 
   */
  public void endPutAll(long start, int amount, boolean isMainWorkload,
                        HistogramStats histogram) {
    incPutAlls();
    endPut(start, amount, isMainWorkload, histogram);
  }

  /**
   * increase the count on the combined put gets
   */
  public void incCombinedPutGets(boolean isMainWorkload) {
    incCombinedPutGets(1, isMainWorkload);
  }
  /**
   * increase the count on the combined put gets by the supplied amount
   */
  public void incCombinedPutGets(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(COMBINEDPUTGETS, amount);
  }
  /**
   * increase the time on the combined put gets by the supplied amount
   */
  public void incCombinedPutGetTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(COMBINEDPUTGET_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the combined put get
   */
  public long startCombinedPutGet() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the combined put get started 
   */
  public void endCombinedPutGet(long start, boolean isMainWorkload,
                                HistogramStats histogram) {
    endCombinedPutGet(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the combined put get started 
   */
  public void endCombinedPutGet(long start, int amount, boolean isMainWorkload,
                                HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incCombinedPutGets(amount, isMainWorkload);
    incCombinedPutGetTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the getAlls
   */
  public void incGetAlls() {
    statistics().incInt(GETALLS, 1);
  }
  /**
   * increase the count on the gets
   */
  public void incGets(boolean isMainWorkload) {
    incGets(1, isMainWorkload);
  }
  /**
   * increase the count on the gets by the supplied amount
   */
  public void incGets(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(GETS, amount);
  }
  /**
   * increase the time on the gets by the supplied amount
   */
  public void incGetTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(GET_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the get
   */
  public long startGet() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the get started 
   */
  public void endGet(long start, boolean isMainWorkload,
                     HistogramStats histogram) {
    endGet(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the get started 
   */
  public void endGet(long start, int amount, boolean isMainWorkload,
                     HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incGets(amount, isMainWorkload);
    incGetTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the get started 
   */
  public void endGetAll(long start, int amount, boolean isMainWorkload,
                        HistogramStats histogram) {
    incGetAlls();
    endGet(start, amount, isMainWorkload, histogram);
  }

  /**
   * increase the count on the updates
   */
  public void incUpdates(boolean isMainWorkload) {
    incUpdates(1, isMainWorkload);
  }
  /**
   * increase the count on the updates by the supplied amount
   */
  public void incUpdates(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(UPDATES, amount);
  }
  /**
   * increase the time on the updates by the supplied amount
   */
  public void incUpdateTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(UPDATE_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the update
   */
  public long startUpdate() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the update started 
   */
  public void endUpdate(long start, boolean isMainWorkload,
                        HistogramStats histogram) {
    endUpdate(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the update started 
   */
  public void endUpdate(long start, int amount, boolean isMainWorkload,
                        HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incUpdates(amount, isMainWorkload);
    incUpdateTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the put comparisons
   */
  public void incPutComparisons(boolean isMainWorkload) {
    incPutComparisons(1, isMainWorkload);
  }
  /**
   * increase the count on the put comparisons by the supplied amount
   */
  public void incPutComparisons(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(PUTCOMPARISONS, amount);
  }
  /**
   * increase the time on the put comparisons by the supplied amount
   */
  public void incPutComparisonTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(PUTCOMPARISON_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the put comparisons
   */
  public long startPutComparisons() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the put comparisons started 
   */
  public void endPutComparison(long start, boolean isMainWorkload,
                               HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incPutComparisons(1, isMainWorkload);
    incPutComparisonTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the get comparisons
   */
  public void incGetComparisons(boolean isMainWorkload) {
    incGetComparisons(1, isMainWorkload);
  }
  /**
   * increase the count on the get comparisons by the supplied amount
   */
  public void incGetComparisons(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(GETCOMPARISONS, amount);
  }
  /**
   * increase the time on the get comparisons by the supplied amount
   */
  public void incGetComparisonTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(GETCOMPARISON_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the get comparisons
   */
  public long startGetComparisons() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the get comparisons started 
   */
  public void endGetComparisons(long start, boolean isMainWorkload,
                                HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incGetComparisons(1, isMainWorkload);
    incGetComparisonTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the destroys
   */
  public void incDestroys(boolean isMainWorkload) {
    incDestroys(1, isMainWorkload);
  }
  /**
   * increase the count on the destroys by the supplied amount
   */
  public void incDestroys(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(DESTROYS, amount);
  }
  /**
   * increase the time on the destroys by the supplied amount
   */
  public void incDestroyTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(DESTROY_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the destroy
   */
  public long startDestroy() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the destroy started 
   */
  public void endDestroy(long start, boolean isMainWorkload,
                         HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incDestroys(1, isMainWorkload);
    incDestroyTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count of cache create events by the supplied amount
   */
  public void incCreateEvents(int amount) {
    statistics().incInt(CREATE_EVENTS, amount);
  }

  /**
   * increase the count of cache destroy events by the supplied amount
   */
  public void incDestroyEvents(int amount) {
    statistics().incInt(DESTROY_EVENTS, amount);
  }

  /**
   * increase the count of cache updates events by the supplied amount
   */
  public void incUpdateEvents(int amount) {
    statistics().incInt(UPDATE_EVENTS, amount);
  }
  /**
   * increase the latency on the cache updates by the supplied amount
   * 
   */
  public void incUpdateLatency(long amount) {
    long nonZeroAmount = amount;
    if (nonZeroAmount == 0) { // make non-zero to ensure non-flatline
      nonZeroAmount = 1; // nanosecond
    }
    statistics().incInt(UPDATE_EVENTS, 1);
    statistics().incLong(UPDATE_LATENCY, nonZeroAmount);
  }

  /**
   * increases the count of latency spikes.
   * @param amount amount to increase count
   * @since 5.0
   * @author jfarris
   */
  public void incLatencySpikes(int amount) {
    statistics().incInt(LATENCY_SPIKES, amount);
  }

  /**
   * increases the count of negative latencies.
   * @param amount amount to increase count
   * @since 5.0
   * @author lises
   */
  public void incNegativeLatencies(int amount) {
    statistics().incInt(NEGATIVE_LATENCIES, amount);
  }

  /**
   * increase the count on the nulls
   */
  public void incNulls() {
    incNulls(1);
  }
  /**
   * increase the count on the nulls by the supplied amount
   */
  public void incNulls(int amount) {
    statistics().incInt(NULLS, amount);
  }
  /**
   * increase the count on the UseCase12 updates
   */
  public void incUseCase12Updates(boolean isMainWorkload) {
    incUseCase12Updates(1, isMainWorkload);
  }
  /**
   * increase the count on the UseCase12 updates by the supplied amount
   */
  public void incUseCase12Updates(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(USECASE12_UPDATES, amount);
  }
  /**
   * increase the time on the UseCase12 updates by the supplied amount
   */
  public void incUseCase12UpdateTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(USECASE12_UPDATE_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the UseCase12 Update
   */
  public long startUseCase12Update() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the UseCase12 update started 
   */
  public void endUseCase12Update(long start, boolean isMainWorkload,
                              HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incUseCase12Updates(1, isMainWorkload);
    incUseCase12UpdateTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the connects
   */
  public void incConnects(boolean isMainWorkload) {
    incConnects(1, isMainWorkload);
  }
  /**
   * increase the count on the connects by the supplied amount
   */
  public void incConnects(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(CONNECTS, amount);
  }

  /**
   * increase the time on the connects by the supplied amount
   */
  public void incConnectTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(CONNECT_TIME, amount);
  }

  /**
   * @return the timestamp that marks the start of the connect
   */
  public long startConnect() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the connect started 
   */
  public void endConnect(long start, boolean isMainWorkload,
                         HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incConnects(1, isMainWorkload);
    incConnectTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the disconnects
   */
  public void incDisconnects(boolean isMainWorkload) {
    incDisconnects(1, isMainWorkload);
  }
  /**
   * increase the count on the disconnects by the supplied amount
   */
  public void incDisconnects(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(DISCONNECTS, amount);
  }

  /**
   * increase the time on the disconnects by the supplied amount
   */
  public void incDisconnectTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(DISCONNECT_TIME, amount);
  }

  /**
   * @return the timestamp that marks the start of the disconnect
   */
  public long startDisconnect() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the disconnect started 
   */
  public void endDisconnect(long start, boolean isMainWorkload,
                            HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incDisconnects(1, isMainWorkload);
    incDisconnectTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the queries
   */
  public void incQueries(boolean isMainWorkload) {
    incQueries(1, isMainWorkload);
  }
  /**
   * increase the count on the queries by the supplied amount
   */
  public void incQueries(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(QUERIES, amount);
  }
  /**
   * increase the count on the query results by the supplied amount
   */
  public void incQueryResults(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(QUERY_RESULTS, amount);
  }
  /**
   * increase the time on the queries by the supplied amount
   */
  public void incQueryTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(QUERY_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the query
   */
  public long startQuery() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the query started
   */
  public void endQuery(long start, int results, boolean isMainWorkload,
                       HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incQueries(1, isMainWorkload);
    incQueryResults(results, false);
    incQueryTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the rebalances
   */
  public void incRebalances(boolean isMainWorkload) {
    incRebalances(1, isMainWorkload);
  }
  /**
   * increase the count on the rebalances by the supplied amount
   */
  public void incRebalances(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(REBALANCES, amount);
  }
  /**
   * increase the time on the rebalances by the supplied amount
   */
  public void incRebalanceTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(REBALANCE_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the rebalance
   */
  public long startRebalance() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the rebalance started
   */
  public void endRebalance(long start, boolean isMainWorkload,
                           HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incRebalances(1, isMainWorkload);
    incRebalanceTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the recoveries
   */
  public void incRecoveries(boolean isMainWorkload) {
    incRecoveries(1, isMainWorkload);
  }
  /**
   * increase the count on the recoveries by the supplied amount
   */
  public void incRecoveries(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(RECOVERIES, amount);
  }
  /**
   * increase the time on the recoveries by the supplied amount
   */
  public void incRecoveryTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(RECOVERY_TIME, amount);
  }
  /**
   * @param elapsed the total time taken by recovery
   */
  public void endRecovery(long elapsed, boolean isMainWorkload,
                          HistogramStats histogram) {
    incRecoveries(1, isMainWorkload);
    incRecoveryTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the locks
   */
  public void incLocks(boolean isMainWorkload) {
    incLocks(1, isMainWorkload);
  }
  /**
   * increase the count on the locks by the supplied amount
   */
  public void incLocks(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(LOCKS, amount);
  }
  /**
   * increase the time on the locks by the supplied amount
   */
  public void incLockTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(LOCK_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the lock
   */
  public long startLock() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the lock started 
   */
  public void endLock(long start, boolean isMainWorkload,
                      HistogramStats histogram) {
    endLock(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the lock started 
   */
  public void endLock(long start, int amount, boolean isMainWorkload,
                      HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incLocks(amount, isMainWorkload);
    incLockTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }

  /**
   * increase the count on the gateway queue drains
   */
  public void incGatewayQueueDrains(boolean isMainWorkload) {
    incGatewayQueueDrains(1, isMainWorkload);
  }
  /**
   * increase the count on the gateway queue drains by the supplied amount
   */
  public void incGatewayQueueDrains(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(GATEWAY_QUEUE_DRAINS, amount);
  }
  /**
   * increase the time on the gateway queue drains by the supplied amount
   */
  public void incGatewayQueueDrainTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(GATEWAY_QUEUE_DRAIN_TIME, amount);
  }
  /**
   * @return the timestamp that marks the start of the gateway queue drain
   */
  public long startGatewayQueueDrain() {
    return NanoTimer.getTime();
  }
  /**
   * @param start the timestamp taken when the gateway queue drain started 
   */
  public void endGatewayQueueDrain(long start, boolean isMainWorkload,
                                   HistogramStats histogram) {
    endGatewayQueueDrain(start, 1, isMainWorkload, histogram);
  }
  /**
   * @param amount amount to increment the counter
   * @param start the timestamp taken when the gateway queue drain started 
   */
  public void endGatewayQueueDrain(long start, int amount,
                                   boolean isMainWorkload,
                                   HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    incGatewayQueueDrains(amount, isMainWorkload);
    incGatewayQueueDrainTime(elapsed, isMainWorkload);
    incHistogram(histogram, elapsed);
  }
}
