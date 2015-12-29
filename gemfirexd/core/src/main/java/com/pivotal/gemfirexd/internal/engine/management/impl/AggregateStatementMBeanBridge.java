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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.beans.stats.LongStatsDeltaAggregator;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAggregator;

/**
 * 
 * Aggregator to aggregate Statement stats from different members. Aggregation
 * Strategy: a) All Counter will be aggregated to show point in time delta
 * value.( across members) b) All GaugeCounter will be aggregated to show
 * absolute value across members
 * 
 * @author rishim
 * 
 */
public class AggregateStatementMBeanBridge {

  private static final String QN_NUM_TIMES_COMPILED = "QNNumTimesCompiled";

  private static final String QN_NUM_EXECUTIONS = "QNNumExecutions";

  private static final String QN_NUM_EXECUTION_IN_PROGRESS = "QNNumExecutionsInProgress";

  private static final String QN_NUM_TIMES_GLOBAL_INDEX_LOOKUP = "QNNumTimesGlobalIndexLookup";

  private static final String QN_NUM_ROWS_MODIFIED = "QNNumRowsModified";

  private static final String QN_PARSE_TIME = "QNParseTime";

  private static final String QN_BIND_TIME = "QNBindTime";

  private static final String QN_OPTIMIZE_TIME = "QNOptimizeTime";

  private static final String QN_ROUTING_INFO_TIME = "QNRoutingInfoTime";

  private static final String QN_GENERATE_TIME = "QNGenerateTime";

  private static final String QN_TOTAL_COMPILATION_TIME = "QNTotalCompilationTime";

  private static final String QN_EXECUTE_TIME = "QNExecuteTime";

  private static final String QN_PROJECTION_TIME = "QNProjectionTime";

  private static final String QN_TOTAL_EXECUTION_TIME = "QNTotalExecutionTime";

  private static final String QN_ROWS_MODIFICATION_TIME = "QNRowsModificationTime";

  private static final String QN_NUM_ROWS_SEEN = "QNNumRowsSeen";

  private static final String QN_MSG_SEND_TIME = "QNMsgSendTime";

  private static final String QN_MSG_SER_TIME = "QNMsgSerTime";

  private static final String QN_RESP_DESER_TIME = "QNRespDeSerTime";

  private AggregateStatementClusterStatsMonitor monitor;

  private LongStatsDeltaAggregator deltas;

  public AggregateStatementMBeanBridge() {
    this.monitor = new AggregateStatementClusterStatsMonitor();

    List<String> keysList = new ArrayList<String>();
    keysList.add(QN_NUM_TIMES_COMPILED);
    keysList.add(QN_NUM_EXECUTIONS);
    keysList.add(QN_NUM_EXECUTION_IN_PROGRESS);
    keysList.add(QN_NUM_TIMES_GLOBAL_INDEX_LOOKUP);
    keysList.add(QN_NUM_ROWS_MODIFIED);
    keysList.add(QN_PARSE_TIME);
    keysList.add(QN_BIND_TIME);
    keysList.add(QN_OPTIMIZE_TIME);
    keysList.add(QN_ROUTING_INFO_TIME);
    keysList.add(QN_GENERATE_TIME);
    keysList.add(QN_TOTAL_COMPILATION_TIME);
    keysList.add(QN_EXECUTE_TIME);
    keysList.add(QN_PROJECTION_TIME);
    keysList.add(QN_TOTAL_EXECUTION_TIME);
    keysList.add(QN_ROWS_MODIFICATION_TIME);
    keysList.add(QN_NUM_ROWS_SEEN);
    keysList.add(QN_MSG_SEND_TIME);
    keysList.add(QN_MSG_SER_TIME);

    deltas = new LongStatsDeltaAggregator(keysList);

  }
  
  private volatile int memberCount = 0;

  public int getMemberCount(){
    return memberCount;
  }
  
  

  public void aggregate(String member, FederationComponent newState, FederationComponent oldState) {
    if (newState != null && oldState == null) {
      // Proxy Addition , increase member count
      ++memberCount;
    }
    if (newState == null && oldState != null) {
      // Proxy Removal , decrease member count
      --memberCount;
    }
    

    monitor.aggregate(newState, oldState);
    deltas.aggregate(newState, oldState);

    
    insertIntoTable(member, newState, oldState);
  }
  

  
  private void insertIntoTable(String member, FederationComponent newState, FederationComponent oldState) {
    return ; //TODO to insert int a GEMFIREXD table
  }

  public long getBindTime() {
    return deltas.getDelta(QN_BIND_TIME);
  }

  public long getExecutionTime() {
    return deltas.getDelta(QN_EXECUTE_TIME);
  }

  public long getGenerateTime() {
    return deltas.getDelta(QN_GENERATE_TIME);
  }

  public long getNumExecution() {
    return deltas.getDelta(QN_NUM_EXECUTIONS);
  }

  public long getNumExecutionsInProgress() {
    return monitor.getNumExecutionsInProgresss();
  }

  public long getNumRowsModified() {
    return deltas.getDelta(QN_NUM_ROWS_MODIFIED);
  }

  public long getNumTimesCompiled() {
    return deltas.getDelta(QN_NUM_TIMES_COMPILED);
  }

  public long getNumTimesGlobalIndexLookup() {
    return deltas.getDelta(QN_NUM_TIMES_GLOBAL_INDEX_LOOKUP);
  }

  public long getOptimizeTime() {
    return deltas.getDelta(QN_OPTIMIZE_TIME);
  }

  public long getParseTime() {
    return deltas.getDelta(QN_PARSE_TIME);
  }

  public long getProjectionTime() {
    return deltas.getDelta(QN_PROJECTION_TIME);
  }

  public long getQNMsgSendTime() {
    return deltas.getDelta(QN_MSG_SEND_TIME);
  }

  public long getQNMsgSerTime() {
    return deltas.getDelta(QN_MSG_SER_TIME);
  }

  public long getQNNumRowsSeen() {
    return deltas.getDelta(QN_NUM_ROWS_SEEN);
  }

  public long getRoutingInfoTime() {
    return deltas.getDelta(QN_ROUTING_INFO_TIME);
  }

  public long getRowsModificationTime() {
    return deltas.getDelta(QN_ROWS_MODIFICATION_TIME);
  }

  public long getTotalCompilationTime() {
    return deltas.getDelta(QN_TOTAL_COMPILATION_TIME);
  }

  public long getTotalExecutionTime() {
    return deltas.getDelta(QN_TOTAL_EXECUTION_TIME);
  }

  public long getQNRespDeSerTime() {
    return deltas.getDelta(QN_RESP_DESER_TIME);
  }

  public AggregateStatementClusterStatsMonitor getMonitor() {
    return monitor;
  }

  class AggregateStatementClusterStatsMonitor {

    private StatsAggregator aggregator;

    private Map<String, Class<?>> typeMap;

    public void aggregate(FederationComponent newState, FederationComponent oldState) {
      aggregator.aggregate(newState, oldState);
    }

    public AggregateStatementClusterStatsMonitor() {
      this.typeMap = new HashMap<String, Class<?>>();
      intTypeMap();
      this.aggregator = new StatsAggregator(typeMap);
    }

    private void intTypeMap() {
      typeMap.put(QN_NUM_EXECUTION_IN_PROGRESS, Long.TYPE);

    }

    public long getNumExecutionsInProgresss() {
      return aggregator.getIntValue(QN_NUM_EXECUTION_IN_PROGRESS);
    }
  }
}
