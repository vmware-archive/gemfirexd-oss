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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.sql.Timestamp;

import com.gemstone.gemfire.internal.NanoTimer;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.ResultHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProcessorResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDeleteResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireInsertResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireRegionSizeResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireUpdateResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.NcjPullResultSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore.StoreStatistics;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;

/**
 * Capture summary of execution statistics <strong>per statement</strong> for
 * servicing statistics*VTI tables.
 * 
 * @author soubhikc
 * 
 */
public final class StatementStatisticsCollector extends
    AbstractStatisticsCollector {

  private StatementStats stats;

  private StoreStatistics selfStats;
  
  private boolean isQueryNode;
  
  private boolean statisticsTimingOn;

  public StatementStatisticsCollector(ResultSetStatisticsVisitor nextCollector) {
    super(nextCollector);
    stats = null;
    selfStats = Misc.getMemStore().getStoreStatistics();
  }

  @Override
  public ResultSetStatisticsVisitor getClone() {
    return new StatementStatisticsCollector(super.getClone());
  }
  
  @Override
  public <T> void process(EmbedConnection conn,
      StatementExecutorMessage<T> msg, EmbedStatement est,
      boolean isLocallyExecuted) throws StandardException {

    final long beginTime = NanoTimer.getTime();
    
    final ResultSet rs = est.getResultsToWrap();
    final Activation act = rs.getActivation();
    
    init(act, est.getGPrepStmt());
    
    stats = est.getStatementStats();
    
    if(stats == null) {
      return;
    }
    
    if( msg.getSender() != null) {
      sender = msg.getSender().toString();
    }
    
    final Timestamp beginExecTS = msg.getConstructTime();
    final Timestamp endExecTS = msg.getEndProcessTime();
    if(beginExecTS != null && endExecTS != null) {
      final long totalExecTime = (endExecTS.getTime() - beginExecTS.getTime()) * 1000000;
      stats.incStat(StatementStats.totExecutionTimeId, false,
          totalExecTime);
    }
    
    doXPLAIN(rs, act, false, statisticsTimingOn, isLocallyExecuted);

    processDistributionMessage(msg, null);
    
    selfStats.collectStatementStatisticsStats((NanoTimer.getTime()-beginTime));
    if (nextCollector != null) {
      nextCollector.process(conn, msg, est, isLocallyExecuted);
    }
    else {
      rs.resetStatistics();
    }
  }

  public <T> void process(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, ResultHolder rh,
      boolean isLocallyExecuted) throws StandardException {

    final long beginTime = NanoTimer.getTime();
    
    final ResultSet rs = rh.getERS().getSourceResultSet();
    final Activation act = rs.getActivation();

    init(act, rh.getGPrepStmt());
    
    if(stats == null) {
      return;
    }

    if( msg.getSender() != null) {
      sender = msg.getSender().toString();
    }

    final Timestamp beginExecTS = msg.getConstructTime();
    final Timestamp endExecTS = msg.getEndProcessTime();
    if(beginExecTS != null && endExecTS != null) {
      final long totalExecTime = (endExecTS.getTime() - beginExecTS.getTime()) * 1000000;
      stats.incStat(StatementStats.totExecutionTimeId, false,
          totalExecTime);
    }

    doXPLAIN(rs, act, false, statisticsTimingOn, isLocallyExecuted);

    processDistributionMessage(msg, rh);
    
    selfStats.collectStatementStatisticsStats((NanoTimer.getTime()-beginTime));
    if (nextCollector != null) {
      nextCollector.process(conn, msg, rh, isLocallyExecuted);
    }
    else {
      rs.resetStatistics();
    }
}
  
  @Override
  public void doXPLAIN(ResultSet rs, Activation activation,
      boolean genStatementDesc, boolean timeStatsEnabled, boolean isLocallyExecuted) throws StandardException {
    
    final long beginTime = NanoTimer.getTime();
    
    if (genStatementDesc) {
      init(activation, activation.getPreparedStatement());
      statisticsTimingOn = timeStatsEnabled;
      
      if(stats == null) {
        return;
      }

      final Timestamp beginExecTS = rs.getBeginExecutionTimestamp();
      final Timestamp endExecTS = rs.getEndExecutionTimestamp();
      if (beginExecTS != null && endExecTS != null) {
        final long totalExecTime = endExecTS.getTime() - beginExecTS.getTime();
        stats.incStat(StatementStats.totExecutionTimeId, isQueryNode,
            totalExecTime * 1000000);
      }
    }
    
    rs.accept(this);
    
    selfStats.collectStatementStatisticsStats((NanoTimer.getTime()-beginTime));
    if (genStatementDesc && nextCollector != null) {
      nextCollector.doXPLAIN(rs, activation, genStatementDesc, timeStatsEnabled, isLocallyExecuted);
    }
  }
  
  private void init(Activation activation,
      com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement preparedStatement) {
    final ExecPreparedStatement eps = (ExecPreparedStatement)preparedStatement;
    stats = eps.getStatementStats();
    isQueryNode = eps.getStatement().createQueryInfo();
    statisticsTimingOn = activation.getLanguageConnectionContext()
        .getStatisticsTiming();
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector: Time Statistics is "
                + statisticsTimingOn);
      }
    }
  }

  private final void record(final int stmt_id, final boolean incQN, final long value) {
    stats.incStat(stmt_id, incQN, value);
  }


  @Override
  public void clear() {

  }

  @Override
  public void setNumberOfChildren(int noChildren) {

  }
  
  public final <T> void processDistributionMessage(
      final StatementExecutorMessage<T> msg, final ResultHolder rh) {

      if(statisticsTimingOn) {
        record(StatementStats.dn_MsgDeSerTimeId, false, msg.getSerializeDeSerializeTime());
        record(StatementStats.dn_MsgProcessTimeId, false, msg.getProcessTime());
        
        if(rh != null) {
          record(StatementStats.dn_ResultIterationTimeId, false,
              rh.process_time);
          record(StatementStats.dn_RespSerTimeId, false, rh.ser_deser_time);
          record(StatementStats.dn_ThrottleTimeId, false, rh.throttle_time);
        }
      }
  }

  public final void recordMessageProcessTime(final long process_time) {
    record(StatementStats.dn_MsgProcessTimeId, false, process_time);
  }

  @Override
  public void visit(final ProjectRestrictResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit ProjectRestrictResultSet "
                + rs.isTopResultSet);
      }
    }
    if(!rs.isTopResultSet) {
      return;
    }
    
    if (statisticsTimingOn) {
      record(StatementStats.projectionTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numRowsProjectedId, false, rs.rowsSeen - rs.rowsFiltered);
  }

  private void handleDMLWriteResultSetDataNode(DMLWriteResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.rowsModificationTimeId, false, rs.getExecuteTime());
    }
    
    record(StatementStats.numRowsModifiedId, false, rs.modifiedRowCount());
  }
  
  @Override
  public void visit(DeleteResultSet rs, int overridable) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit DeleteResultSet ");
      }
    }
    
    handleDMLWriteResultSetDataNode(rs);
  }
  
  @Override
  public void visit(final InsertResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit InsertResultSet ");
      }
    }
    
    handleDMLWriteResultSetDataNode(rs);
  }

  @Override
  public void visit(final UpdateResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit UpdateResultSet ");
      }
    }
    
    handleDMLWriteResultSetDataNode(rs);
  }
 
  private void handleDMLWriteResultSetQueryNode(AbstractGemFireResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.rowsModificationTimeId, true, rs
          .getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.numRowsModifiedId, true, rs.modifiedRowCount());
  }
  
  @Override
  public void visit(GemFireUpdateResultSet rs, int overridable) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GemFireUpdateResultSet ");
      }
    }
    
    handleDMLWriteResultSetQueryNode(rs);
  }
  
  @Override
  public void visit(GemFireDeleteResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GemFireDeleteResultSet ");
      }
    }
    
    handleDMLWriteResultSetQueryNode(rs);
  }
  
  @Override
  public void visit(final GemFireInsertResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GemFireInsertResultSet ");
      }
    }
    
    handleDMLWriteResultSetQueryNode(rs);
  }

  @Override
  public void visit(final GroupedAggregateResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_groupedAggregationTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numRowsGroupedAggregatedId, false, rs.rowsSeen);
  }

  @Override
  public void visit(DistinctGroupedAggregateResultSet rs) {
     visit((GroupedAggregateResultSet)rs);
  }

  @Override
  public void visit(HashScanResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_hashScanTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numRowsHashScannedId, false, rs.rowsSeen);
  }

  @Override
  public void visit(MergeJoinResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(NestedLoopJoinResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_nlJoinTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numNLJoinRowsReturnedId, false, rs.rowsReturned);
  }

  @Override
  public void visit(HashJoinResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_hashJoinTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numHASHJoinRowsReturnedId, false, rs.rowsReturned);
  }

  @Override
  public void visit(ScalarAggregateResultSet rs, int overridable) {

  }

  @Override
  public void visit(BulkTableScanResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_tableScanTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    record(StatementStats.dn_numTableRowsScannedId, false, rs.rowsSeen - rs.rowsFiltered);
  }

  @Override
  public void visit(final IndexRowToBaseRowResultSet rs) {
    if (statisticsTimingOn) {
      record(StatementStats.dn_indexScanTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    record(StatementStats.dn_numIndexRowsScannedId, false, rs.rowsSeen - rs.rowsFiltered);
  }
  
  @Override
  public void visit(MultiProbeTableScanResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AnyResultSet anyResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final GemFireDistributedResultSet rs) {

    // createRe....();

    // create sort properties
    // aggregation , distinct, group by, special case outer join, n-way merge
    // happening

//    distributionPlan.processGFDistResultSet(rs);
  }
  
  @Override
  public void visit(final SortResultSet rs) {

    if (statisticsTimingOn) {
      record(StatementStats.dn_sortTimeId, false,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.dn_numRowsSortedId, false, rs.rowsSeen - rs.rowsFiltered);
    
    //TODO:sb:QS: add sort overflow information from sort props.
    
  }

  @Override
  public void visit(LastIndexKeyResultSet lastIndexKeyResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(MiscResultSet miscResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(OnceResultSet onceResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(ProcedureProcessorResultSet procedureProcessorResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(GfxdSubqueryResultSet gfxdSubqueryResultset) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GfxdSubqueryResultSet ");
      }
    }
    
    if (statisticsTimingOn) {
      record(StatementStats.dn_subQueryScanTimeId, false, gfxdSubqueryResultset
          .getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    record(StatementStats.dn_numSubQueryRowsSeen, false,
        gfxdSubqueryResultset.rowsSeen - gfxdSubqueryResultset.rowsFiltered);
  }
  
  @Override
  public void visit(NcjPullResultSet ncjPullResultSet) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit NcjPullResultSet ");
      }
    }
    
    if (statisticsTimingOn) {
      // TODO - change this
      record(StatementStats.dn_subQueryScanTimeId, false, ncjPullResultSet
          .getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    // TODO - change this
    record(StatementStats.dn_numSubQueryRowsSeen, false,
        ncjPullResultSet.rowsSeen - ncjPullResultSet.rowsFiltered);
  }

  @Override
  public void visit(TemporaryRowHolderResultSet temporaryRowHolderResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(WindowResultSet windowResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(TableScanResultSet rs, int overridable) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(JoinResultSet rs, int overridable) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(HashScanResultSet rs, int overridable) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(GemFireRegionSizeResultSet regionSizeResultSet) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GemFireRegionSizeResultSet ");
      }
    }
    
    if (statisticsTimingOn) {
      record(StatementStats.executeTimeId, true, regionSizeResultSet
          .getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
  }

  @Override
  public void visit(RowCountResultSet rs) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void visit(RowResultSet rs) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void visit(UnionResultSet rs) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void visit(GemFireResultSet rs) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector::visit GemFireResultSet ");
      }
    }
    
    if (statisticsTimingOn) {
      record(StatementStats.executeTimeId, true, rs
          .getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL));
    }
    
    record(StatementStats.qn_numRowsSeenId, true, rs.modifiedRowCount());
  }
}
