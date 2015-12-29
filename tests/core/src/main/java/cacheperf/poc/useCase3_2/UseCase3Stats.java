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

package cacheperf.poc.useCase3_2;

import cacheperf.poc.useCase3_2.UseCase3Prms.ClientName;
import cacheperf.poc.useCase3_2.UseCase3Prms.RegionName;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.NanoTimer;
import distcache.DistCache;
import cacheperf.CachePerfException;
import perffmwk.PerformanceStatistics;

/**
 *  Implements statistics related to cache performance tests.
 */
public class UseCase3Stats extends PerformanceStatistics {

  /** <code>UseCase3Stats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  public static final String REGISTERINTEREST_REPS = "registerInterestReps";
  public static final String REGISTERINTEREST_REP_TIME = "registerInterestRepTime";
  public static final String CREATE_REPS = "createReps";
  public static final String CREATE_REP_TIME = "createRepTime";
  public static final String GET_REPS = "getReps";
  public static final String GET_REPS_IN_PROGRESS = "getRepsInProgress";
  public static final String GET_REP_TIME = "getRepTime";
  public static final String PUT_REPS = "putReps";
  public static final String PUT_REPS_IN_PROGRESS = "putRepsInProgress";
  public static final String PUT_REP_TIME = "putRepTime";
  public static final String GETALL_REPS = "getAllReps";
  public static final String GETALL_REPS_IN_PROGRESS = "getAllRepsInProgress";
  public static final String GETALL_REP_TIME = "getAllRepTime";
  public static final String PUTALL_REPS = "putAllReps";
  public static final String PUTALL_REPS_IN_PROGRESS = "putAllRepsInProgress";
  public static final String PUTALL_REP_TIME = "putAllRepTime";
  public static final String UPDATE_REP_EVENTS = "updateRepEvents";
  public static final String UPDATE_REP_LATENCY = "updateRepLatency";
  public static final String LATENCY_REP_SPIKES = "latencyRepSpikes";
  public static final String NEGATIVE_REP_LATENCIES = "negativeRepLatencies";
  public static final String NULL_REPS = "nullReps";

  public static final String REGISTERINTEREST_PARS = "registerInterestPars";
  public static final String REGISTERINTEREST_PAR_TIME = "registerInterestParTime";
  public static final String CREATE_PARS = "createPars";
  public static final String CREATE_PAR_TIME = "createParTime";
  public static final String GET_PARS = "getPars";
  public static final String GET_PARS_IN_PROGRESS = "getParsInProgress";
  public static final String GET_PAR_TIME = "getParTime";
  public static final String PUT_PARS = "putPars";
  public static final String PUT_PARS_IN_PROGRESS = "putParsInProgress";
  public static final String PUT_PAR_TIME = "putParTime";
  public static final String GETALL_PARS = "getAllPars";
  public static final String GETALL_PARS_IN_PROGRESS = "getAllParsInProgress";
  public static final String GETALL_PAR_TIME = "getAllParTime";
  public static final String PUTALL_PARS = "putAllPars";
  public static final String PUTALL_PARS_IN_PROGRESS = "putAllParsInProgress";
  public static final String PUTALL_PAR_TIME = "putAllParTime";
  public static final String UPDATE_PAR_EVENTS = "updateParEvents";
  public static final String UPDATE_PAR_LATENCY = "updateParLatency";
  public static final String LATENCY_PAR_SPIKES = "latencyParSpikes";
  public static final String NEGATIVE_PAR_LATENCIES = "negativeParLatencies";
  public static final String NULL_PARS = "nullPars";

  public static final String REGISTERINTEREST_REP_PERSISTS = "registerInterestRepPersists";
  public static final String REGISTERINTEREST_REP_PERSIST_TIME = "registerInterestRepPersistTime";
  public static final String CREATE_REP_PERSISTS = "createRepPersists";
  public static final String CREATE_REP_PERSIST_TIME = "createRepPersistTime";
  public static final String GET_REP_PERSISTS = "getRepPersists";
  public static final String GET_REP_PERSISTS_IN_PROGRESS = "getRepPersistsInProgress";
  public static final String GET_REP_PERSIST_TIME = "getRepPersistTime";
  public static final String PUT_REP_PERSISTS = "putRepPersists";
  public static final String PUT_REP_PERSISTS_IN_PROGRESS = "putRepPersistsInProgress";
  public static final String PUT_REP_PERSIST_TIME = "putRepPersistTime";
  public static final String GETALL_REP_PERSISTS = "getAllRepPersists";
  public static final String GETALL_REP_PERSISTS_IN_PROGRESS = "getAllRepPersistsInProgress";
  public static final String GETALL_REP_PERSIST_TIME = "getAllRepPersistTime";
  public static final String PUTALL_REP_PERSISTS = "putAllRepPersists";
  public static final String PUTALL_REP_PERSISTS_IN_PROGRESS = "putAllRepPersistsInProgress";
  public static final String PUTALL_REP_PERSIST_TIME = "putAllRepPersistTime";
  public static final String UPDATE_REP_PERSIST_EVENTS = "updateRepPersistEvents";
  public static final String UPDATE_REP_PERSIST_LATENCY = "updateRepPersistLatency";
  public static final String LATENCY_REP_PERSIST_SPIKES = "latencyRepPersistSpikes";
  public static final String NEGATIVE_REP_PERSIST_LATENCIES = "negativeRepPersistLatencies";
  public static final String NULL_REP_PERSISTS = "nullRepPersists";

  public static final String REGISTERINTEREST_PAR_PERSISTS = "registerInterestParPersists";
  public static final String REGISTERINTEREST_PAR_PERSIST_TIME = "registerInterestParPersistTime";
  public static final String CREATE_PAR_PERSISTS = "createParPersists";
  public static final String CREATE_PAR_PERSIST_TIME = "createParPersistTime";
  public static final String GET_PAR_PERSISTS = "getParPersists";
  public static final String GET_PAR_PERSISTS_IN_PROGRESS = "getParPersistsInProgress";
  public static final String GET_PAR_PERSIST_TIME = "getParPersistTime";
  public static final String PUT_PAR_PERSISTS = "putParPersists";
  public static final String PUT_PAR_PERSISTS_IN_PROGRESS = "putParPersistsInProgress";
  public static final String PUT_PAR_PERSIST_TIME = "putParPersistTime";
  public static final String GETALL_PAR_PERSISTS = "getAllParPersists";
  public static final String GETALL_PAR_PERSISTS_IN_PROGRESS = "getAllParPersistsInProgress";
  public static final String GETALL_PAR_PERSIST_TIME = "getAllParPersistTime";
  public static final String PUTALL_PAR_PERSISTS = "putAllParPersists";
  public static final String PUTALL_PAR_PERSISTS_IN_PROGRESS = "putAllParPersistsInProgress";
  public static final String PUTALL_PAR_PERSIST_TIME = "putAllParPersistTime";
  public static final String UPDATE_PAR_PERSIST_EVENTS = "updateParPersistEvents";
  public static final String UPDATE_PAR_PERSIST_LATENCY = "updateParPersistLatency";
  public static final String LATENCY_PAR_PERSIST_SPIKES = "latencyParPersistSpikes";
  public static final String NEGATIVE_PAR_PERSIST_LATENCIES = "negativeParPersistLatencies";
  public static final String NULL_PAR_PERSISTS = "nullParPersists";

  public static final String STOP_SERVERS = "stopServers";
  public static final String STOP_SERVER_TIME = "stopServerTime";
  public static final String STOP_SERVERS_IN_PROGRESS = "stopServersInProgress";

  public static final String START_SERVERS = "startServers";
  public static final String START_SERVER_TIME = "startServerTime";
  public static final String START_SERVERS_IN_PROGRESS = "startServersInProgress";

  public static final String STOP_DATAHOSTS = "stopDatahosts";
  public static final String STOP_DATAHOST_TIME = "stopDatahostTime";
  public static final String STOP_DATAHOSTS_IN_PROGRESS = "stopDatahostsInProgress";

  public static final String START_DATAHOSTS = "startDatahosts";
  public static final String START_DATAHOST_TIME = "startDatahostTime";
  public static final String START_DATAHOSTS_IN_PROGRESS = "startDatahostsInProgress";

  public static final String REBALANCES = "rebalances";
  public static final String REBALANCE_TIME = "rebalanceTime";
  public static final String REBALANCES_IN_PROGRESS = "rebalancesInProgress";

  public static final String SLEEPS = "sleeps";
  public static final String SLEEP_TIME = "sleepTime";
  public static final String SLEEPS_IN_PROGRESS = "sleepsInProgress";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>UseCase3Stats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntGauge(
          VM_COUNT,
          "Number of VMs connected with a given logical hydra client name.",
          "VMs"
        ),
      factory().createIntCounter( 
         REGISTERINTEREST_REPS,
         "Number of register interests completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         REGISTERINTEREST_REP_TIME,
         "Total time spent registering interest in the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         CREATE_REPS,
         "Number of creates completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         CREATE_REP_TIME,
         "Total time spent doing creates for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         GET_REPS,
         "Number of gets completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         GET_REPS_IN_PROGRESS,
         "Current number of gets for the Rep region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         GET_REP_TIME,
         "Total time spent doing gets for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         PUT_REPS,
         "Number of puts completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         PUT_REPS_IN_PROGRESS,
         "Current number of puts for the Rep region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         PUT_REP_TIME,
         "Total time spent doing puts for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         GETALL_REPS,
         "Number of getAlls completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         GETALL_REPS_IN_PROGRESS,
         "Current number of getAlls for the Rep region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         GETALL_REP_TIME,
         "Total time spent doing getAlls for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         PUTALL_REPS,
         "Number of putAlls completed for the Rep region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         PUTALL_REPS_IN_PROGRESS,
         "Current number of putAlls for the Rep region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         PUTALL_REP_TIME,
         "Total time spent doing putAlls for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         UPDATE_REP_EVENTS,
         "Number of update events for the Rep region.",
         "events", largerIsBetter
         ),
      factory().createLongCounter( 
         UPDATE_REP_LATENCY,
         "Latency of update events for the Rep region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         LATENCY_REP_SPIKES,
         "Number of latency spikes for the Rep region.",
         "spikes", !largerIsBetter
         ),
      factory().createIntCounter( 
         NEGATIVE_REP_LATENCIES,
         "Number of negative latencies for the Rep region (caused by insufficient clock skew correction).",
         "negatives", !largerIsBetter
         ),
      factory().createIntCounter( 
         NULL_REPS,
         "Number of gets from the Rep region that returned null.",
         "ops", !largerIsBetter
         ),
      factory().createIntCounter( 
         REGISTERINTEREST_PARS,
         "Number of register interests completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         REGISTERINTEREST_PAR_TIME,
         "Total time spent registering interest in the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         CREATE_PARS,
         "Number of creates completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         CREATE_PAR_TIME,
         "Total time spent doing creates for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         GET_PARS,
         "Number of gets completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         GET_PARS_IN_PROGRESS,
         "Current number of gets for the Par region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         GET_PAR_TIME,
         "Total time spent doing gets for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         PUT_PARS,
         "Number of puts completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         PUT_PARS_IN_PROGRESS,
         "Current number of puts for the Par region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         PUT_PAR_TIME,
         "Total time spent doing puts for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         GETALL_PARS,
         "Number of getAlls completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         GETALL_PARS_IN_PROGRESS,
         "Current number of getAlls for the Par region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         GETALL_PAR_TIME,
         "Total time spent doing getAlls for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         PUTALL_PARS,
         "Number of putAlls completed for the Par region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         PUTALL_PARS_IN_PROGRESS,
         "Current number of putAlls for the Par region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         PUTALL_PAR_TIME,
         "Total time spent doing putAlls for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         UPDATE_PAR_EVENTS,
         "Number of update events for the Par region.",
         "events", largerIsBetter
         ),
      factory().createLongCounter( 
         UPDATE_PAR_LATENCY,
         "Latency of update events for the Par region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         LATENCY_PAR_SPIKES,
         "Number of latency spikes for the Par region.",
         "spikes", !largerIsBetter
         ),
      factory().createIntCounter( 
         NEGATIVE_PAR_LATENCIES,
         "Number of negative latencies for the Par region (caused by insufficient clock skew correction).",
         "negatives", !largerIsBetter
         ),
      factory().createIntCounter( 
         NULL_PARS,
         "Number of gets from the Par region that returned null.",
         "ops", !largerIsBetter
         ),
      factory().createIntCounter( 
         REGISTERINTEREST_REP_PERSISTS,
         "Number of register interests completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         REGISTERINTEREST_REP_PERSIST_TIME,
         "Total time spent registering interest in the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         CREATE_REP_PERSISTS,
         "Number of creates completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         CREATE_REP_PERSIST_TIME,
         "Total time spent doing creates for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         GET_REP_PERSISTS,
         "Number of gets completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         GET_REP_PERSISTS_IN_PROGRESS,
         "Current number of gets for the RepPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         GET_REP_PERSIST_TIME,
         "Total time spent doing gets for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         PUT_REP_PERSISTS,
         "Number of puts completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         PUT_REP_PERSISTS_IN_PROGRESS,
         "Current number of puts for the RepPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         PUT_REP_PERSIST_TIME,
         "Total time spent doing puts for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         GETALL_REP_PERSISTS,
         "Number of getAlls completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         GETALL_REP_PERSISTS_IN_PROGRESS,
         "Current number of getAlls for the RepPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         GETALL_REP_PERSIST_TIME,
         "Total time spent doing getAlls for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         PUTALL_REP_PERSISTS,
         "Number of putAlls completed for the RepPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         PUTALL_REP_PERSISTS_IN_PROGRESS,
         "Current number of putAlls for the RepPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         PUTALL_REP_PERSIST_TIME,
         "Total time spent doing putAlls for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         UPDATE_REP_PERSIST_EVENTS,
         "Number of update events for the RepPersist region.",
         "events", largerIsBetter
         ),
      factory().createLongCounter( 
         UPDATE_REP_PERSIST_LATENCY,
         "Latency of update events for the RepPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         LATENCY_REP_PERSIST_SPIKES,
         "Number of latency spikes for the RepPersist region.",
         "spikes", !largerIsBetter
         ),
      factory().createIntCounter( 
         NEGATIVE_REP_PERSIST_LATENCIES,
         "Number of negative latencies for the RepPersist region (caused by insufficient clock skew correction).",
         "negatives", !largerIsBetter
         ),
      factory().createIntCounter( 
         NULL_REP_PERSISTS,
         "Number of gets from the RepPersist region that returned null.",
         "ops", !largerIsBetter
         ),
      factory().createIntCounter( 
         REGISTERINTEREST_PAR_PERSISTS,
         "Number of register interests completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         REGISTERINTEREST_PAR_PERSIST_TIME,
         "Total time spent registering interest in the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         CREATE_PAR_PERSISTS,
         "Number of creates completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         CREATE_PAR_PERSIST_TIME,
         "Total time spent doing creates for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         GET_PAR_PERSISTS,
         "Number of gets completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         GET_PAR_PERSISTS_IN_PROGRESS,
         "Current number of gets for the ParPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         GET_PAR_PERSIST_TIME,
         "Total time spent doing gets for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         PUT_PAR_PERSISTS,
         "Number of puts completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         PUT_PAR_PERSISTS_IN_PROGRESS,
         "Current number of puts for the ParPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         PUT_PAR_PERSIST_TIME,
         "Total time spent doing puts for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         GETALL_PAR_PERSISTS,
         "Number of getAlls completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         GETALL_PAR_PERSISTS_IN_PROGRESS,
         "Current number of getAlls for the ParPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         GETALL_PAR_PERSIST_TIME,
         "Total time spent doing getAlls for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter(
         PUTALL_PAR_PERSISTS,
         "Number of putAlls completed for the ParPersist region.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge(
         PUTALL_PAR_PERSISTS_IN_PROGRESS,
         "Current number of putAlls for the ParPersist region in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter(
         PUTALL_PAR_PERSIST_TIME,
         "Total time spent doing putAlls for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         UPDATE_PAR_PERSIST_EVENTS,
         "Number of update events for the ParPersist region.",
         "events", largerIsBetter
         ),
      factory().createLongCounter( 
         UPDATE_PAR_PERSIST_LATENCY,
         "Latency of update events for the ParPersist region.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         LATENCY_PAR_PERSIST_SPIKES,
         "Number of latency spikes for the ParPersist region.",
         "spikes", !largerIsBetter
         ),
      factory().createIntCounter( 
         NEGATIVE_PAR_PERSIST_LATENCIES,
         "Number of negative latencies for the ParPersist region (caused by insufficient clock skew correction).",
         "negatives", !largerIsBetter
         ),
      factory().createIntCounter( 
         NULL_PAR_PERSISTS,
         "Number of gets from the ParPersist region that returned null.",
         "ops", !largerIsBetter
         ),
      factory().createIntCounter( 
         STOP_DATAHOSTS,
         "Number of times a datahost has been stopped.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         STOP_DATAHOSTS_IN_PROGRESS,
         "Current number of datahost stops in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         STOP_DATAHOST_TIME,
         "Total time spent stopping datahosts.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         STOP_SERVERS,
         "Number of times a server has been stopped.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         STOP_SERVERS_IN_PROGRESS,
         "Current number of server stops in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         STOP_SERVER_TIME,
         "Total time spent stopping servers.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         START_DATAHOSTS,
         "Number of times a datahost has been started.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         START_DATAHOSTS_IN_PROGRESS,
         "Current number of datahost starts in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         START_DATAHOST_TIME,
         "Total time spent starting datahosts.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         START_SERVERS,
         "Number of times a server has been started.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         START_SERVERS_IN_PROGRESS,
         "Current number of server starts in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         START_SERVER_TIME,
         "Total time spent starting servers.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         REBALANCES,
         "Number of rebalances completed.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         REBALANCES_IN_PROGRESS,
         "Current number of rebalances in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         REBALANCE_TIME,
         "Total time spent doing rebalances.",
         "nanoseconds", !largerIsBetter
         ),
      factory().createIntCounter( 
         SLEEPS,
         "Number of sleeps completed.",
         "ops", largerIsBetter
         ),
      factory().createIntGauge( 
         SLEEPS_IN_PROGRESS,
         "Current number of sleeps in progress",
         "ops", largerIsBetter
         ),
      factory().createLongCounter( 
         SLEEP_TIME,
         "Total time spent doing sleeps.",
         "nanoseconds", !largerIsBetter
         )
    };
  }

  public static UseCase3Stats getInstance() {
    return (UseCase3Stats) getInstance( UseCase3Stats.class, SCOPE );
  }
  public static UseCase3Stats getInstance(int scope) {
    return (UseCase3Stats) getInstance( UseCase3Stats.class, scope );
  }
  public static UseCase3Stats getInstance(String name) {
    return (UseCase3Stats) getInstance(UseCase3Stats.class, SCOPE, name);
  }
  public static UseCase3Stats getInstance( String name, String trimspecName ) {
    return (UseCase3Stats) getInstance( UseCase3Stats.class, SCOPE, name, trimspecName );
  }

  public UseCase3Stats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
//------------------------------------------------------------------------------
// vmCount

  /**
   * increase the count on the vmCount
   */
  public void incVMCount() {
    this.statistics.incInt(VM_COUNT, 1);
  }

//------------------------------------------------------------------------------
// registerInterest

  public long startRegisterInterest() {
    return NanoTimer.getTime();
  }
  public void endRegisterInterest(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(REGISTERINTEREST_REPS, amount);
        this.statistics.incLong(REGISTERINTEREST_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(REGISTERINTEREST_PARS, amount);
        this.statistics.incLong(REGISTERINTEREST_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(REGISTERINTEREST_REP_PERSISTS, amount);
        this.statistics.incLong(REGISTERINTEREST_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(REGISTERINTEREST_PAR_PERSISTS, amount);
        this.statistics.incLong(REGISTERINTEREST_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// creates

  public long startCreate() {
    return NanoTimer.getTime();
  }
  public void endCreate(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(CREATE_REPS, amount);
        this.statistics.incLong(CREATE_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(CREATE_PARS, amount);
        this.statistics.incLong(CREATE_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(CREATE_REP_PERSISTS, amount);
        this.statistics.incLong(CREATE_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(CREATE_PAR_PERSISTS, amount);
        this.statistics.incLong(CREATE_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// gets

  public long startGet(RegionName regionName, int amount) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(GET_REPS_IN_PROGRESS, amount);
        break;
      case Par:
        this.statistics.incInt(GET_PARS_IN_PROGRESS, amount);
        break;
      case RepPersist:
        this.statistics.incInt(GET_REP_PERSISTS_IN_PROGRESS, amount);
        break;
      case ParPersist:
        this.statistics.incInt(GET_PAR_PERSISTS_IN_PROGRESS, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endGet(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(GET_REPS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GET_REPS, amount);
        this.statistics.incLong(GET_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(GET_PARS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GET_PARS, amount);
        this.statistics.incLong(GET_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(GET_REP_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GET_REP_PERSISTS, amount);
        this.statistics.incLong(GET_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(GET_PAR_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GET_PAR_PERSISTS, amount);
        this.statistics.incLong(GET_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// puts

  public long startPut(RegionName regionName, int amount) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(PUT_REPS_IN_PROGRESS, amount);
        break;
      case Par:
        this.statistics.incInt(PUT_PARS_IN_PROGRESS, amount);
        break;
      case RepPersist:
        this.statistics.incInt(PUT_REP_PERSISTS_IN_PROGRESS, amount);
        break;
      case ParPersist:
        this.statistics.incInt(PUT_PAR_PERSISTS_IN_PROGRESS, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endPut(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(PUT_REPS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUT_REPS, amount);
        this.statistics.incLong(PUT_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(PUT_PARS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUT_PARS, amount);
        this.statistics.incLong(PUT_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(PUT_REP_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUT_REP_PERSISTS, amount);
        this.statistics.incLong(PUT_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(PUT_PAR_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUT_PAR_PERSISTS, amount);
        this.statistics.incLong(PUT_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// getAlls

  public long startGetAll(RegionName regionName, int amount) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(GETALL_REPS_IN_PROGRESS, amount);
        break;
      case Par:
        this.statistics.incInt(GETALL_PARS_IN_PROGRESS, amount);
        break;
      case RepPersist:
        this.statistics.incInt(GETALL_REP_PERSISTS_IN_PROGRESS, amount);
        break;
      case ParPersist:
        this.statistics.incInt(GETALL_PAR_PERSISTS_IN_PROGRESS, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endGetAll(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(GETALL_REPS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GETALL_REPS, amount);
        this.statistics.incLong(GETALL_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(GETALL_PARS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GETALL_PARS, amount);
        this.statistics.incLong(GETALL_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(GETALL_REP_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GETALL_REP_PERSISTS, amount);
        this.statistics.incLong(GETALL_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(GETALL_PAR_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(GETALL_PAR_PERSISTS, amount);
        this.statistics.incLong(GETALL_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// putAlls

  public long startPutAll(RegionName regionName, int amount) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(PUTALL_REPS_IN_PROGRESS, amount);
        break;
      case Par:
        this.statistics.incInt(PUTALL_PARS_IN_PROGRESS, amount);
        break;
      case RepPersist:
        this.statistics.incInt(PUTALL_REP_PERSISTS_IN_PROGRESS, amount);
        break;
      case ParPersist:
        this.statistics.incInt(PUTALL_PAR_PERSISTS_IN_PROGRESS, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endPutAll(long start, int amount, RegionName regionName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (regionName) {
      case Rep:
        this.statistics.incInt(PUTALL_REPS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUTALL_REPS, amount);
        this.statistics.incLong(PUTALL_REP_TIME, elapsed);
        break;
      case Par:
        this.statistics.incInt(PUTALL_PARS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUTALL_PARS, amount);
        this.statistics.incLong(PUTALL_PAR_TIME, elapsed);
        break;
      case RepPersist:
        this.statistics.incInt(PUTALL_REP_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUTALL_REP_PERSISTS, amount);
        this.statistics.incLong(PUTALL_REP_PERSIST_TIME, elapsed);
        break;
      case ParPersist:
        this.statistics.incInt(PUTALL_PAR_PERSISTS_IN_PROGRESS, -1 * amount);
        this.statistics.incInt(PUTALL_PAR_PERSISTS, amount);
        this.statistics.incLong(PUTALL_PAR_PERSIST_TIME, elapsed);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// updateEvents and updateLatency, plus latencySpikes and negativeLatencies

  public void incUpdateLatency(long amount, RegionName regionName) {
    long nonZeroAmount = amount;
    if (nonZeroAmount == 0) { // make non-zero to ensure non-flatline
      nonZeroAmount = 1; // nanosecond
    }
    switch (regionName) {
      case Rep:
        this.statistics.incInt(UPDATE_REP_EVENTS, 1);
        this.statistics.incLong(UPDATE_REP_LATENCY, nonZeroAmount);
        break;
      case Par:
        this.statistics.incInt(UPDATE_PAR_EVENTS, 1);
        this.statistics.incLong(UPDATE_PAR_LATENCY, nonZeroAmount);
        break;
      case RepPersist:
        this.statistics.incInt(UPDATE_REP_PERSIST_EVENTS, 1);
        this.statistics.incLong(UPDATE_REP_PERSIST_LATENCY, nonZeroAmount);
        break;
      case ParPersist:
        this.statistics.incInt(UPDATE_PAR_PERSIST_EVENTS, 1);
        this.statistics.incLong(UPDATE_PAR_PERSIST_LATENCY, nonZeroAmount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

  public void incLatencySpikes(int amount, RegionName regionName) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(LATENCY_REP_SPIKES, amount);
        break;
      case Par:
        this.statistics.incInt(LATENCY_PAR_SPIKES, amount);
        break;
      case RepPersist:
        this.statistics.incInt(LATENCY_REP_PERSIST_SPIKES, amount);
        break;
      case ParPersist:
        this.statistics.incInt(LATENCY_PAR_PERSIST_SPIKES, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

  public void incNegativeLatencies(int amount, RegionName regionName) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(NEGATIVE_REP_LATENCIES, amount);
        break;
      case Par:
        this.statistics.incInt(NEGATIVE_PAR_LATENCIES, amount);
        break;
      case RepPersist:
        this.statistics.incInt(NEGATIVE_REP_PERSIST_LATENCIES, amount);
        break;
      case ParPersist:
        this.statistics.incInt(NEGATIVE_PAR_PERSIST_LATENCIES, amount);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

  public void incNulls(RegionName regionName) {
    switch (regionName) {
      case Rep:
        this.statistics.incInt(NULL_REPS, 1);
        break;
      case Par:
        this.statistics.incInt(NULL_PARS, 1);
        break;
      case RepPersist:
        this.statistics.incInt(NULL_REP_PERSISTS, 1);
        break;
      case ParPersist:
        this.statistics.incInt(NULL_PAR_PERSISTS, 1);
        break;
      default:
        String s = "Unknown region name: " + regionName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// stops

  public long startStop(ClientName clientName) {
    switch (clientName) {
      case data:
        this.statistics.incInt(STOP_DATAHOSTS_IN_PROGRESS, 1);
        break;
      case server:
        this.statistics.incInt(STOP_SERVERS_IN_PROGRESS, 1);
        break;
      default:
        String s = "Unknown client name: " + clientName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endStop(long start, ClientName clientName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (clientName) {
      case data:
        this.statistics.incInt(STOP_DATAHOSTS, 1);
        this.statistics.incLong(STOP_DATAHOST_TIME, elapsed);
        this.statistics.incInt(STOP_DATAHOSTS_IN_PROGRESS, -1);
        break;
      case server:
        this.statistics.incInt(STOP_SERVERS, 1);
        this.statistics.incLong(STOP_SERVER_TIME, elapsed);
        this.statistics.incInt(STOP_SERVERS_IN_PROGRESS, -1);
        break;
      default:
        String s = "Unknown client name: " + clientName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// starts

  public long startStart(ClientName clientName) {
    switch (clientName) {
      case data:
        this.statistics.incInt(START_DATAHOSTS_IN_PROGRESS, 1);
        break;
      case server:
        this.statistics.incInt(START_SERVERS_IN_PROGRESS, 1);
        break;
      default:
        String s = "Unknown client name: " + clientName;
        throw new CachePerfException(s);
    }
    return NanoTimer.getTime();
  }
  public void endStart(long start, ClientName clientName) {
    long elapsed = NanoTimer.getTime() - start;
    switch (clientName) {
      case data:
        this.statistics.incInt(START_DATAHOSTS, 1);
        this.statistics.incLong(START_DATAHOST_TIME, elapsed);
        this.statistics.incInt(START_DATAHOSTS_IN_PROGRESS, -1);
        break;
      case server:
        this.statistics.incInt(START_SERVERS, 1);
        this.statistics.incLong(START_SERVER_TIME, elapsed);
        this.statistics.incInt(START_SERVERS_IN_PROGRESS, -1);
        break;
      default:
        String s = "Unknown client name: " + clientName;
        throw new CachePerfException(s);
    }
  }

//------------------------------------------------------------------------------
// rebalances

  public long startRebalance() {
    this.statistics.incInt(REBALANCES_IN_PROGRESS, 1);
    return NanoTimer.getTime();
  }
  public void endRebalance(long start) {
    long elapsed = NanoTimer.getTime() - start;
    this.statistics.incInt(REBALANCES, 1);
    this.statistics.incLong(REBALANCE_TIME, elapsed);
    this.statistics.incInt(REBALANCES_IN_PROGRESS, -1);
  }

//------------------------------------------------------------------------------
// sleeps

  public long startSleep() {
    this.statistics.incInt(SLEEPS_IN_PROGRESS, 1);
    return NanoTimer.getTime();
  }
  public void endSleep(long start) {
    long elapsed = NanoTimer.getTime() - start;
    this.statistics.incInt(SLEEPS, 1);
    this.statistics.incLong(SLEEP_TIME, elapsed);
    this.statistics.incInt(SLEEPS_IN_PROGRESS, -1);
  }
}
