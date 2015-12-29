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
import com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdStatisticsVisitor;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.NcjPullResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;

/**
 * Visitor to collect <tt>ResultSet</tt> statistics from each class. Multiple
 * visit overloads is the tradeoff between below explanation and code
 * maintainability.
 * <p>
 * Every leaf classes of ResultSet hierarchy has overloaded version of visit
 * method, so that every class is handled individually.
 * <p>
 * This is different from Derby's original model of <tt>RuntimeStatistics</tt>
 * and <tt>ResultSetStatistics</tt> in terms of (a) temporary object creations
 * (b) multiple checks of <tt>instanceof</tt> & dynamic cast. This also avoids
 * one monolithic function handling for every type of <tt>ResultSet</tt>.
 * <p>
 * 
 * Guideline to maintain this class:
 * <ul>
 * <li>Any <tt><b>concrete</b></tt> class implementing one of the child
 * interfaces of <tt>ResultSet</tt> should get added like in the <i>LEAF classes
 * section</i>.
 * <li>Any <tt><b>abstract</b></tt> class implementing one of the child
 * interfaces of <tt>ResultSet</tt> should get added like in the <i>ROOT classes
 * section</i>.
 * <li>Any class <tt><b>extending</b></tt> existing more than one level deep
 * from <tt>ResultSet</tt> should get added like in <i>GRANDCHILD classes
 * section</i>.
 * </ul>
 * 
 * @author soubhikc
 * 
 */
public interface ResultSetStatisticsVisitor extends GfxdStatisticsVisitor {

  // =================================================================
  // ==== XPLAINVisitor methods ====
  // =================================================================
  /**
   * Call this method to reset the visitor for a new run over the statistics. A
   * default implementation should call this method automatically at first of a
   * call of doXPLAIN().
   */
  public void clear();

  /**
   * This method is the hook method which is called from the TopResultSet. It
   * starts the explanation of the current ResultSetStatistics tree and keeps
   * the information during one explain run.
   * @param genStatementDesc
   *          generate statement entry or not.
   * @param timeStatsEnabled 
   *          state of the timing stats enabled or not on the query node during
   *          collection. (#44201)
   * @param isLocallyExecuted TODO
   */
  public void doXPLAIN(
      ResultSet rs,
      Activation activation,
      boolean genStatementDesc, 
      boolean timeStatsEnabled, boolean isLocallyExecuted) throws StandardException;

  /**
   * This method gets invoked on the data nodes whereever the query gets routed
   * to capture remote query plans.
   * 
   * @param conn
   *          EmbedConnection on which query got executed.
   * @param msg
   *          ExecutorMessage that gets processed.
   * @param rh
   *          ResultHolder that is holding onto ResultSet.
   * @param isLocallyExecuted
   *          generate statement entry or not.
   * @param <T>
   * @throws StandardException
   */
  public <T> void process(final EmbedConnection conn,
      final StatementExecutorMessage<T> msg, ResultHolder rh,
      boolean isLocallyExecuted) throws StandardException;

  /**
   * 
   * @param conn
   *          EmbedConnection on which query got executed.
   * @param msg
   * @param est
   * @param isLocallyExecuted TODO
   * @param <T>
   * @throws StandardException
   */
  public <T> void process(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, final EmbedStatement est,
      boolean isLocallyExecuted) throws StandardException;

  /**
   * This method informs the visitor about the number of children. It has to be
   * called first! by the different explainable nodes before the visit method of
   * the visitor gets called. Each node knows how many children he has. The
   * visitor can use this information to resolve the relationship of the current
   * explained node to above nodes. Due to the top-down, pre-order, depth-first
   * traversal of the tree, this information can directly be exploited.
   * 
   * @param noChildren
   *          the number of children of the current explained node.
   */
  public void setNumberOfChildren(
      int noChildren);

  /**
   * Creates a blank copy of the visitor.
   * 
   * @return a new set of statistics collector chain.
   */
  public ResultSetStatisticsVisitor getClone();

  /**
   * Statement Plan UUID that captured the current execution.
   * 
   * @return null if explain_connection is off,
   * otherwise the statement id that can be used to extract the query plan.
   */
  public UUID getStatementUUID();
  
  // ==================================================================
  // additional helper methods
  // ==================================================================
  public ResultSetStatisticsVisitor getNextCollector();

  public void setNextCollector(
      ResultSetStatisticsVisitor collector);

  // =================================================================
  // ==== ROOT classes section ====
  // =================================================================
  //
  // following overloads are not allowed for override.
  // purpose of this class otherwise is lost.

  // basic derby's hierarchy
  public void visit(
      ResultSet rs,
      int donotHonor);

  public void visit(
      NoRowsResultSetImpl rs,
      int donotHonor);

  public void visit(
      BasicNoPutResultSetImpl rs,
      int donotHonor);

  public void visit(
      DMLWriteResultSet rs,
      int donotHonor);

  public void visit(
      DMLVTIResultSet rs,
      int donotHonor);

  // distribution hierarchy
  public void visit(
      AbstractGemFireResultSet rs,
      int donotHonor);

  // NoPutResultSet
  public void visit(
      ScanResultSet rs,
      int donotHonor);

  // ===============================================================
  // ==== GRANDCHILD classes section ===
  // ===============================================================
  //
  // following kind will be taken care by its parent.
  public void visit(
      DeleteCascadeResultSet rs);

  public void visit(
      GemFireDeleteResultSet rs);

  public void visit(
      DistinctGroupedAggregateResultSet rs);

  public void visit(
      DistinctScanResultSet rs);

  public void visit(
      MergeJoinResultSet rs);

  public void visit(
      NestedLoopJoinResultSet rs);

  public void visit(
      HashJoinResultSet rs);

  public void visit(
      NestedLoopLeftOuterJoinResultSet rs);

  public void visit(
      HashLeftOuterJoinResultSet rs);

  public void visit(
      DistinctScalarAggregateResultSet rs);

  public void visit(
      BulkTableScanResultSet rs);

  public void visit(
      MultiProbeTableScanResultSet rs);

  // =================================================================
  // ==== LEAF classes section ====
  // =================================================================
  //
  // following are kind of leaf in some sense & is profiled.

  // NoRowsResultSetImpl hierarchy
  public void visit(
      CallStatementResultSet rs);

  public void visit(
      DeleteResultSet rs); // hasChildren

  public void visit(
      DeleteVTIResultSet rs);

  public void visit(
      InsertVTIResultSet rs);

  public void visit(
      UpdateVTIResultSet rs);

  public void visit(
      InsertResultSet rs);

  public void visit(
      UpdateResultSet rs);

  // DAP related.
  public void visit(
      OutgoingResultSetImpl rs);

  // Distribution related.
  public void visit(
      GemFireDistributedResultSet rs);

  public void visit(
      GemFireInsertResultSet rs);

  public void visit(
      GemFireResultSet rs);

  public void visit(
      GemFireUpdateResultSet rs); // hasChildren

  // CursorResultSet hierarchy
  public void visit(
      CurrentOfResultSet rs);

  public void visit(
      DependentResultSet rs);

  public void visit(
      GroupedAggregateResultSet rs); // hasChildren

  public void visit(
      HashScanResultSet rs); // hasChildren

  public void visit(
      HashTableResultSet rs);

  public void visit(
      IndexRowToBaseRowResultSet rs);

  public void visit(
      JoinResultSet rs);

  public void visit(
      MaterializedResultSet rs);

  public void visit(
      NormalizeResultSet rs);

  public void visit(
      ProjectRestrictResultSet rs);

  public void visit(
      RowCountResultSet rs);

  public void visit(
      RowResultSet rs);

  public void visit(
      ScalarAggregateResultSet rs);

  public void visit(
      ScrollInsensitiveResultSet rs);

  public void visit(
      SetOpResultSet rs);

  public void visit(
      SortResultSet rs);

  public void visit(
      TableScanResultSet rs);

  public void visit(
      UnionResultSet rs);

  public void visit(
      VTIResultSet rs);

  // BasicNoPutResultSetImpl hierarchy
  public void visit(
      AnyResultSet anyResultSet);

  public void visit(
      LastIndexKeyResultSet lastIndexKeyResultSet);

  public void visit(
      MiscResultSet miscResultSet);

  public void visit(
      OnceResultSet onceResultSet);

  public void visit(
      ProcedureProcessorResultSet procedureProcessorResultSet);

  public void visit(
      GfxdSubqueryResultSet gfxdSubqueryResultset);
  
  public void visit(
      NcjPullResultSet ncjPullResultset);

  public void visit(
      TemporaryRowHolderResultSet temporaryRowHolderResultSet);

  public void visit(
      WindowResultSet windowResultSet);
  
  public void visit(
      GemFireRegionSizeResultSet regionSizeResultSet);

}
