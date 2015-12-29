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

package com.pivotal.gemfirexd.internal.engine;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.execute.CallbackStatement;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdConnectionWrapper;
import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SubQueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public final class GemFireXDQueryTimeStatistics extends
    GemFireXDQueryObserverAdapter {

  private static final long serialVersionUID = -2632361131680969916L;

  public static enum StatType {
    RESULT_SET_OPEN,
    RESULT_SET_EXECUTE,
    BEFORE_RESULT_SET_EXECUTE,
    AFTER_RESULT_SET_EXECUTE,
    RESULT_SET_CLOSE,
    RESULT_HOLDER_EXECUTE,
    BEFORE_RESULT_HOLDER_EXECUTE,
    RESULT_HOLDER_SERIALIZATION,
    RESULT_HOLDER_ITERATION,
    RESULT_HOLDER_READ,
    QUERY_OPTIMIZATION,
    COMPUTE_ROUTING_OBJECTS,
    PREPSTATEMENT_QUERY_EXECUTION,
    STATEMENT_QUERY_EXECUTION,
    GLOBAL_INDEX_LOOKUP,
    TOTAL_DISTRIBUTION,
    ORM_TIME,
    CONNECTION_CLOSE,
    TOTAL_EXECUTION
  }

  public static class QueryStatistics {

    private int numInvocations;

    private final AtomicLong totalTimeInNanos;

    private Object customObject;

    QueryStatistics() {
      this.numInvocations = 0;
      this.totalTimeInNanos = new AtomicLong(0);
    }

    public int getNumInvocations() {
      return this.numInvocations;
    }

    public long getTotalTimeInNanos() {
      return this.totalTimeInNanos.get();
    }

    public Object getCustomObject() {
      return this.customObject;
    }
  }

  private final ConcurrentHashMap<String, QueryStatistics[]> statsMap;

  private int totalInvocations;

  private final int dumpFreq;

  private static ThreadLocal<Long[]> startTimeStamps = new ThreadLocal<Long[]>() {
    @Override
    public Long[] initialValue() {
      int numTypes = StatType.values().length;
      Long[] initValues = new Long[numTypes];
      for (int index = 0; index < numTypes; ++index) {
        initValues[index] = Long.valueOf(0L);
      }
      return initValues;
    }
  };

  public final static String GLOBAL_STATS_NAME = "Global Timings";

  public GemFireXDQueryTimeStatistics(int dumpFreq) {
    this.statsMap = new ConcurrentHashMap<String, QueryStatistics[]>();
    this.totalInvocations = 0;
    this.dumpFreq = dumpFreq;
  }

  @Override
  public void beforeOptimizedParsedTree(String query, StatementNode qt,
      LanguageConnectionContext lcc) {
    startTimer(StatType.QUERY_OPTIMIZATION);
  }

  @Override
  public void afterOptimizedParsedTree(String query, StatementNode qt,
      LanguageConnectionContext lcc) {
    long elapsedNanos = stopTimer(StatType.QUERY_OPTIMIZATION);
    // also set the query plan if available
    Object customObject = null;
    if (lcc != null && (lcc.getOptimizerTrace() || lcc.explainConnection())) {
      customObject = lcc.getOptimizerTraceOutput();
    }
    updateStatisticsForQuery(query, StatType.QUERY_OPTIMIZATION, elapsedNanos,
        true, customObject);
  }

  @Override
  public void beforeQueryExecution(EmbedStatement stmt, Activation activation) {
    startTimer(StatType.TOTAL_EXECUTION);
  }

  @Override
  public boolean afterQueryExecution(CallbackStatement stmt, SQLException sqle) {
    long elapsedNanos = stopTimer(StatType.TOTAL_EXECUTION);
    long afterExecNanos = stopTimer(StatType.RESULT_SET_EXECUTE);
    updateStatisticsForQuery(stmt.getSQLText(), StatType.TOTAL_EXECUTION,
        elapsedNanos, true);
    updateStatisticsForQuery(stmt.getSQLText(),
        StatType.AFTER_RESULT_SET_EXECUTE, afterExecNanos, true);
    if (this.dumpFreq > 0 && ++this.totalInvocations >= this.dumpFreq) {
      this.totalInvocations = 0;
      dumpAllStats();
    }
    return false;
  }

  @Override
  public void beforeGemFireResultSetOpen(AbstractGemFireResultSet rs, LanguageConnectionContext lcc) {
    startTimer(StatType.RESULT_SET_OPEN);
  }

  @Override
  public void afterGemFireResultSetOpen(AbstractGemFireResultSet rs, LanguageConnectionContext lcc) {
    long elapsedNanos = stopTimer(StatType.RESULT_SET_OPEN);
    updateStatisticsForQuery(rs.getActivation().getPreparedStatement()
        .getUserQueryString(lcc), StatType.RESULT_SET_OPEN, elapsedNanos, true);
  }

  @Override
  public void beforeGemFireResultSetExecuteOnActivation(
      AbstractGemFireActivation activation) {
    long elapsedBeforeNanos = peekTimer(StatType.TOTAL_EXECUTION);
    updateStatisticsForQuery(activation.getPreparedStatement().getUserQueryString(activation.getLanguageConnectionContext()),
        StatType.BEFORE_RESULT_SET_EXECUTE, elapsedBeforeNanos, true);
    startTimer(StatType.RESULT_SET_EXECUTE);
  }

  @Override
  public void beforeComputeRoutingObjects(AbstractGemFireActivation activation) {
    startTimer(StatType.COMPUTE_ROUTING_OBJECTS);
  }

  @Override
  public void afterComputeRoutingObjects(AbstractGemFireActivation activation) {
    long elapsedNanos = stopTimer(StatType.COMPUTE_ROUTING_OBJECTS);
    updateStatisticsForQuery(activation.getPreparedStatement().getUserQueryString(activation.getLanguageConnectionContext()),
        StatType.COMPUTE_ROUTING_OBJECTS, elapsedNanos, true);
  }

  @Override
  public <T extends Serializable> void beforeQueryDistribution(
      StatementExecutorMessage<T> executorMessage, boolean streaming) {
    if(executorMessage == null) return;
    startTimer(StatType.TOTAL_DISTRIBUTION);
  }

  @Override
  public <T extends Serializable> void afterQueryDistribution(
      StatementExecutorMessage<T> executorMessage, boolean streaming) {
    if(executorMessage == null) return;
    long elapsedNanos = stopTimer(StatType.TOTAL_DISTRIBUTION);
    updateStatisticsForQuery(executorMessage.source(),
        StatType.TOTAL_DISTRIBUTION, elapsedNanos, true);
  }

  @Override
  public void afterGemFireResultSetExecuteOnActivation(
      AbstractGemFireActivation activation) {
    long elapsedNanos = stopTimer(StatType.RESULT_SET_EXECUTE);
    updateStatisticsForQuery(activation.getPreparedStatement().getUserQueryString(activation.getLanguageConnectionContext()),
        StatType.RESULT_SET_EXECUTE, elapsedNanos, true);
    startTimer(StatType.RESULT_SET_EXECUTE);
  }

  @Override
  public void beforeGemFireResultSetClose(AbstractGemFireResultSet rs,
      String query) {
    startTimer(StatType.RESULT_SET_CLOSE);
  }

  @Override
  public void afterGemFireResultSetClose(AbstractGemFireResultSet rs,
      String query) {
    long elapsedNanos = stopTimer(StatType.RESULT_SET_CLOSE);
    updateStatisticsForQuery(query, StatType.RESULT_SET_CLOSE, elapsedNanos,
        true);
  }

  @Override
  public void beforeResultHolderExecution(GfxdConnectionWrapper wrapper,
      EmbedStatement es) {
    long beforeElapsedNanos = peekTimer(StatType.PREPSTATEMENT_QUERY_EXECUTION);
    if (beforeElapsedNanos == 0) {
      beforeElapsedNanos = peekTimer(StatType.STATEMENT_QUERY_EXECUTION);
    }
    updateStatisticsForQuery(es.getSQLText(),
        StatType.BEFORE_RESULT_HOLDER_EXECUTE, beforeElapsedNanos, true);
    startTimer(StatType.RESULT_HOLDER_EXECUTE);
  }

  @Override
  public void beforeResultHolderIteration(GfxdConnectionWrapper wrapper,
      EmbedStatement es) {
    startTimer(StatType.RESULT_HOLDER_ITERATION);
  }

  @Override
  public void afterResultHolderIteration(GfxdConnectionWrapper wrapper,
      EmbedStatement es) {
    long elapsedNanos = stopTimer(StatType.RESULT_HOLDER_ITERATION);
    updateStatisticsForQuery(es.getSQLText(), StatType.RESULT_HOLDER_ITERATION,
        elapsedNanos, true);
  }

  @Override
  public void beforeResultHolderSerialization(GfxdConnectionWrapper wrapper,
      EmbedStatement es) {
    startTimer(StatType.RESULT_HOLDER_SERIALIZATION);
  }

  @Override
  public void afterResultHolderSerialization(GfxdConnectionWrapper wrapper,
      EmbedStatement es) {
    long elapsedNanos = stopTimer(StatType.RESULT_HOLDER_SERIALIZATION);
    updateStatisticsForQuery(es.getSQLText(),
        StatType.RESULT_HOLDER_SERIALIZATION, elapsedNanos, true);
  }

  @Override
  public void afterResultHolderExecution(GfxdConnectionWrapper wrapper,
      EmbedStatement es, String query) {
    long elapsedNanos = stopTimer(StatType.RESULT_HOLDER_EXECUTE);
    updateStatisticsForQuery(query, StatType.RESULT_HOLDER_EXECUTE,
        elapsedNanos, true);
  }

  @Override
  public void beforeResultSetHolderRowRead(RowFormatter rf, Activation act) {
    startTimer(StatType.RESULT_HOLDER_READ);
  }

  @Override
  public void afterResultSetHolderRowRead(RowFormatter rf, ExecRow row,
      Activation act) {
    long elapsedNanos = stopTimer(StatType.RESULT_HOLDER_READ);
    updateStatisticsForQuery(act.getPreparedStatement().getUserQueryString(act.getLanguageConnectionContext()),
        StatType.RESULT_HOLDER_READ, elapsedNanos, true);
  }

  @Override
  public void beforeQueryExecutionByStatementQueryExecutor(
      GfxdConnectionWrapper wrapper, EmbedStatement stmt, String query) {
    startTimer(StatType.STATEMENT_QUERY_EXECUTION);
  }

  @Override
  public void afterQueryExecutionByStatementQueryExecutor(
      GfxdConnectionWrapper wrapper, EmbedStatement stmt, String query) {
    long elapsedNanos = stopTimer(StatType.STATEMENT_QUERY_EXECUTION);
    updateStatisticsForQuery(query, StatType.STATEMENT_QUERY_EXECUTION,
        elapsedNanos, true);
  }

  @Override
  public void beforeQueryExecutionByPrepStatementQueryExecutor(
      GfxdConnectionWrapper wrapper, EmbedPreparedStatement pstmt, String query) {
    startTimer(StatType.PREPSTATEMENT_QUERY_EXECUTION);
  }

  @Override
  public void afterQueryExecutionByPrepStatementQueryExecutor(
      GfxdConnectionWrapper wrapper, EmbedPreparedStatement pstmt, String query) {
    long elapsedNanos = stopTimer(StatType.PREPSTATEMENT_QUERY_EXECUTION);
    updateStatisticsForQuery(pstmt.getSQLText(),
        StatType.PREPSTATEMENT_QUERY_EXECUTION, elapsedNanos, true);
  }

  @Override
  public void beforeConnectionCloseByExecutorFunction(long[] connectionIDs) {
    startTimer(StatType.CONNECTION_CLOSE);
  }

  @Override
  public void afterConnectionCloseByExecutorFunction(long[] connectionIDs) {
    long elapsedNanos = stopTimer(StatType.CONNECTION_CLOSE);
    updateStatisticsForQuery(GLOBAL_STATS_NAME, StatType.CONNECTION_CLOSE,
        elapsedNanos, true);
  }

  @Override
  public void beforeORM(Activation activation, AbstractGemFireResultSet rs) {
    startTimer(StatType.ORM_TIME);
  }

  @Override
  public void afterORM(Activation activation, AbstractGemFireResultSet rs) {
    long elapsedNanos = stopTimer(StatType.ORM_TIME);
    updateStatisticsForQuery(activation.getPreparedStatement().getUserQueryString(activation.getLanguageConnectionContext()),
        StatType.ORM_TIME, elapsedNanos, true);
  }

  @Override
  public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
      GenericPreparedStatement gps, LanguageConnectionContext lcc) {
    // TODO: add meaningful time stats for this
  }

  @Override
  public void subQueryInfoObjectFromOptmizedParsedTree(
      List<SubQueryInfo> qInfos, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
    // TODO: add meaningful time stats for this
  }

  @Override
  public void beforeForeignKeyConstraintCheckAtRegionLevel() {
    // TODO: add meaningful time stats for this
  }

  @Override
  public void beforeUniqueConstraintCheckAtRegionLevel() {
    // TODO: add meaningful time stats for this
  }

  @Override
  public void beforeGlobalIndexLookup(LanguageConnectionContext lcc,
      PartitionedRegion indexRegion, Serializable indexKey) {
    startTimer(StatType.GLOBAL_INDEX_LOOKUP);
  }

  @Override
  public void afterGlobalIndexLookup(LanguageConnectionContext lcc,
      PartitionedRegion indexRegion, Serializable indexKey, Object result) {
    long elapsedNanos = stopTimer(StatType.GLOBAL_INDEX_LOOKUP);
    if (lcc != null && lcc.getActivationCount() > 0) {
      final ExecPreparedStatement pstmt = lcc.getLastActivation()
          .getPreparedStatement();
      if (pstmt != null) {
        updateStatisticsForQuery(pstmt.getUserQueryString(lcc),
            StatType.GLOBAL_INDEX_LOOKUP, elapsedNanos, true);
      }
    }
  }

  @Override
  public void reset() {
    dumpAllStats();
    // System.out.println(statsString.toString());
    this.statsMap.clear();
  }

  @Override
  public void close() {
    reset();
  }

  public Iterator<Map.Entry<String, QueryStatistics[]>> getIterator() {
    return this.statsMap.entrySet().iterator();
  }

  private synchronized StringBuilder dumpAllStats() {
    StringBuilder statsString = new StringBuilder("Query time statistics:\n");
    QueryStatistics[] globalStatsArray = null;
    for (Map.Entry<String, QueryStatistics[]> statsEntry : this.statsMap
        .entrySet()) {
      String query = statsEntry.getKey();
      QueryStatistics[] statsArray = statsEntry.getValue();
      if (GLOBAL_STATS_NAME.equals(query)) {
        globalStatsArray = statsArray;
      }
      else {
        dumpStatsForQuery(query, statsArray, statsString);
      }
    }
    // dump the global stats at the end
    if (globalStatsArray != null) {
      dumpStatsForQuery(GLOBAL_STATS_NAME, globalStatsArray, statsString);
    }
    SanityManager.DEBUG_PRINT("QueryStats", statsString.toString());
    return statsString;
  }

  private void startTimer(StatType statType) {
    Long[] currValues = startTimeStamps.get();
    long startTime = System.nanoTime();
    currValues[statType.ordinal()] = Long.valueOf(startTime);
  }

  private long stopTimer(StatType statType) {
    Long[] currValues = startTimeStamps.get();
    long startTime = currValues[statType.ordinal()];
    // timer not started so nothing to be done
    if (startTime == 0) {
      return 0;
    }
    currValues[statType.ordinal()] = Long.valueOf(0L);
    return (System.nanoTime() - startTime);
  }

  private long peekTimer(StatType statType) {
    Long[] currValues = startTimeStamps.get();
    long startTime = currValues[statType.ordinal()];
    // timer not started so nothing to be done
    if (startTime == 0) {
      return 0;
    }
    return (System.nanoTime() - startTime);
  }

  private void updateStatisticsForQuery(String query, StatType statType,
      long incNanos, boolean incInvocations) {
    updateStatisticsForQuery(query, statType, incNanos, incInvocations, null);
  }

  private void updateStatisticsForQuery(String query, StatType statType,
      long incNanos, boolean incInvocations, Object customObject) {
    if (query != null) {
      QueryStatistics[] queryStatsArray = getOrCreateQueryStatisticsForQuery(
          query);
      QueryStatistics queryStats = queryStatsArray[statType.ordinal()];
      if (incInvocations) {
        ++queryStats.numInvocations;
      }
      queryStats.totalTimeInNanos.addAndGet(incNanos);
      if (customObject != null) {
        queryStats.customObject = customObject;
      }
    }
  }

  private QueryStatistics[] getOrCreateQueryStatisticsForQuery(String query) {
    QueryStatistics[] queryStatsArray = this.statsMap.get(query);
    if (queryStatsArray == null) {
      StatType[] allTypes = StatType.values();
      queryStatsArray = new QueryStatistics[allTypes.length];
      for (int index = 0; index < allTypes.length; ++index) {
        queryStatsArray[index] = new QueryStatistics();
      }
      QueryStatistics[] oldQueryStatsArray = this.statsMap.putIfAbsent(query,
          queryStatsArray);
      if (oldQueryStatsArray != null) {
        queryStatsArray = oldQueryStatsArray;
      }
    }
    return queryStatsArray;
  }

  private void dumpStatsForQuery(String query, QueryStatistics[] statsArray,
      StringBuilder statsString) {
    statsString.append("Time Statistics for: ").append(query).append('\n');
    StatType[] allStats = StatType.values();
    for (int index = 0; index < statsArray.length; ++index) {
      QueryStatistics stats = statsArray[index];
      statsString.append('\t').append(allStats[index]).append(" took ")
          .append(stats.totalTimeInNanos.get()).append("nanos for ")
          .append(stats.numInvocations).append(" invocations\n");
    }
  }
}
