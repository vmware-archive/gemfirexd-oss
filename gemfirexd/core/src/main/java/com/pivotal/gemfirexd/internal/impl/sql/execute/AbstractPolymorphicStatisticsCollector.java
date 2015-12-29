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

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.distributed.ResultHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.OutgoingResultSetImpl;
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
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;

/**
 * This class is to handle ResultSets traversal generically. Currently, this
 * gets used to take common action during query plan generation.
 * <p>
 * {@link AbstractStatisticsCollector} class avoids such virtual calls for
 * lesser cost & should be preferred over this class where ever possible.
 * 
 * @author soubhikc
 * 
 */
abstract public class AbstractPolymorphicStatisticsCollector implements
    ResultSetStatisticsVisitor {

  protected AbstractPolymorphicStatisticsCollector() {
  }

  abstract public void visitVirtual(
      NoRowsResultSetImpl rs);

  abstract public void visitVirtual(
      BasicNoPutResultSetImpl rs);

  abstract public void visitVirtual(
      AbstractGemFireResultSet rs);

  abstract public void visitVirtual(
      OutgoingResultSetImpl rs);

  abstract public void visitVirtual(
      TemporaryRowHolderResultSet rs);

  @Override
  public <T> void process(EmbedConnection conn,
      StatementExecutorMessage<T> msg, EmbedStatement est,
      boolean isLocallyExecuted) throws StandardException {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  final public void doXPLAIN(
      ResultSet rs,
      Activation activation,
      boolean genStatementDesc, boolean timeStatsEnabled, boolean isLocallyExecuted) throws StandardException {
    // this is because of support for SET_DEPRECATED_RUNTIMESTATISTICS
//    throw new UnsupportedOperationException(
//        "Shouldn't be used by XPLAINFactory directly.");
  }

  public <T> void process(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, ResultHolder rh,
      boolean isLocallyExecuted) throws StandardException {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  public ResultSetStatisticsVisitor getClone() {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }
  
  public UUID getStatementUUID() {
     return null;
  }

  @Override
  final public ResultSetStatisticsVisitor getNextCollector() {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  final public void setNextCollector(
      ResultSetStatisticsVisitor collector) {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  final public void setNumberOfChildren(
      int noChildren) {
  }

  @Override
  final public void visit(
      ResultSet rs,
      int donotHonor) {
    throw new UnsupportedOperationException(
        "Shouldn't be used by XPLAINFactory directly.");
  }

  @Override
  final public void visit(
      NoRowsResultSetImpl rs,
      int donotHonor) {
    visitVirtual(rs);
  }

  @Override
  final public void visit(
      BasicNoPutResultSetImpl rs,
      int donotHonor) {
    visitVirtual(rs);
  }

  @Override
  final public void visit(
      DMLWriteResultSet rs,
      int donotHonor) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DMLVTIResultSet rs,
      int donotHonor) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      AbstractGemFireResultSet rs,
      int donotHonor) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      ScanResultSet rs,
      int donotHonor) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DeleteCascadeResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GemFireDeleteResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DistinctGroupedAggregateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DistinctScanResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      MergeJoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      NestedLoopJoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      HashJoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      NestedLoopLeftOuterJoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      HashLeftOuterJoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DistinctScalarAggregateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      BulkTableScanResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      MultiProbeTableScanResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      CallStatementResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DeleteResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DeleteVTIResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      InsertVTIResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      UpdateVTIResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      InsertResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      UpdateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      OutgoingResultSetImpl rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GemFireDistributedResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GemFireInsertResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GemFireResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GemFireUpdateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      CurrentOfResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      DependentResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GroupedAggregateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      HashScanResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      HashTableResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      IndexRowToBaseRowResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      JoinResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      MaterializedResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      NormalizeResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      ProjectRestrictResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      RowCountResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      RowResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      ScalarAggregateResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      ScrollInsensitiveResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      SetOpResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      SortResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      TableScanResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      UnionResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      VTIResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      AnyResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      LastIndexKeyResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      MiscResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      OnceResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      ProcedureProcessorResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      GfxdSubqueryResultSet rs) {
    visitVirtual(rs);

  }
  
  @Override
  final public void visit(
      NcjPullResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      TemporaryRowHolderResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  final public void visit(
      WindowResultSet rs) {
    visitVirtual(rs);

  }

  @Override
  public void visit(
      GemFireRegionSizeResultSet regionSizeResultSet) {

  }

}
