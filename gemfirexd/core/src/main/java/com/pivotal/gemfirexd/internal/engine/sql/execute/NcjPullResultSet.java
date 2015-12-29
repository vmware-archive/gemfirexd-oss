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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet;
import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSetHashingStrategy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * NCJ Purpose
 * 
 * @author vivekb
 * 
 */
public class NcjPullResultSet extends NoPutResultSetImpl {

  final private String sourceString;

  private ResultSet source = null;

  private List<ExecRow> rowsCached;

  private boolean isCacheComplete = false;

  private Iterator<ExecRow> cacheIter = null;

  private boolean keepNcjRSOpen = false;

  final private GenericPreparedStatement ps;

  final private List<Integer> params;

  final private List<ArrayList<DataValueDescriptor>> extraParams = new ArrayList<ArrayList<DataValueDescriptor>>();

  final private int prID;

  private boolean openedOnce = false;

  private Activation childActivation = null;

  private boolean execNotNeeded = false;

  // Only for debug information
  final private boolean isRemoteScan;

  final private boolean hasVarLengthInList;

  /**
   * Constructor
   * 
   * @param ncjSql
   * @param parentActivation
   * @param rsNum
   * @param ps
   * @param params
   * @param isRemoteScan
   * @param hasVarLengthInList
   * @param prID
   */
  public NcjPullResultSet(String ncjSql, Activation parentActivation,
      List<Integer> params, GenericPreparedStatement ps, int rsNum,
      boolean isRemoteScan, boolean hasVarLengthInList, int prId) {
    super(parentActivation, rsNum, 0, 0);
    this.ps = ps;
    this.params = params;
    this.sourceString = ncjSql;
    this.isRemoteScan = isRemoteScan;
    this.hasVarLengthInList = hasVarLengthInList;
    this.prID = prId;
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullResultSet created for " + this.sourceString
                + " ,isRemoteScan=" + this.isRemoteScan
                + " ,hasVarLengthInList=" + this.hasVarLengthInList + " ,prID="
                + prId);
      }
    }
    
    // GemStone changes BEGIN
    printResultSetHierarchy();
    // GemStone changes END
  }

  @Override
  public void openCore() throws StandardException {
    this.beginTime = this.statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    this.isOpen = true;
    if (this.openedOnce) {
      this.cacheIter = this.rowsCached.iterator();
      this.numOpens++;
      if (this.statisticsTimingOn)
        this.openTime += getElapsedNanos(this.beginTime);
      return;
    }
    this.rowsCached = new ArrayList<ExecRow>();
    this.cacheIter = this.rowsCached.iterator();
    int batchSize = 0;
    int cacheSize = 0;
    LanguageConnectionContext lcc = this.activation
        .getLanguageConnectionContext();
    if (lcc != null) {
      batchSize = lcc.getNcjBatchSize();
      cacheSize = lcc.getNcjCacheSize();
    }
    if (GemFireXDUtils.TraceNCJ) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
          "NcjPullResultSet::openCore Sql=" + this.sourceString
              + " ,execNotNeeded=" + this.execNotNeeded + " ,isRemoteScan="
              + this.isRemoteScan + " ,hasVarLengthInList="
              + this.hasVarLengthInList + " ,batchSize=" + batchSize
              + " ,cacheSize=" + cacheSize);
    }

    this.childActivation = getChildActivation(this.activation, this.ps);
    setParametersOnChildActivation(this.childActivation,
        this.activation.getParameterValueSet(), this.params, this.extraParams,
        lcc);
    this.source = executeSqlAsPreparedStatement(this.activation,
        this.childActivation, this.ps, this.execNotNeeded, this.prID);
    if (GemFireXDUtils.TraceNCJ) {
      SanityManager
          .DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
              "NcjPullResultSet::openCore. Done with execution of Sql="
                  + this.sourceString + " ,childActivation="
                  + this.childActivation);
    }

    GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
        .getInstance();
    if (observer != null) {
      observer.ncjPullResultSetOpenCoreInvoked();
      observer.ncjPullResultSetVerifyBatchSize(batchSize);
      observer.ncjPullResultSetVerifyCacheSize(cacheSize);
      observer.ncjPullResultSetVerifyVarInList(this.hasVarLengthInList);
    }

    this.openedOnce = true;
    this.numOpens++;
    if (this.statisticsTimingOn)
      this.openTime += getElapsedNanos(this.beginTime);
  }

  @Override
  public void reopenCore() throws StandardException {
    openCore();
  }
  
  @Override
  public void forceReOpenCore() {
    this.openedOnce = false;
    this.isCacheComplete = false;
  }

  @Override
  public ExecRow getNextRowCore() throws StandardException {
    this.beginTime = this.statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    if (this.isCacheComplete) {
      if (this.cacheIter.hasNext()) {
        ExecRow row = this.cacheIter.next();
        if (row != null) {
          this.getActivation().setCurrentRow(row, this.resultSetNumber);
          this.rowsSeen++;
        }
        else {
          row = null;
        }
        if (this.statisticsTimingOn)
          this.nextTime += getElapsedNanos(this.beginTime);
        return row;
      }
      else {
        if (this.statisticsTimingOn)
          this.nextTime += getElapsedNanos(this.beginTime);
        return null;
      }
    }
    else {
      // check if there is any cached data
      if (this.cacheIter != null && this.cacheIter.hasNext()) {
        ExecRow row = this.cacheIter.next();
        if (row != null) {
          this.getActivation().setCurrentRow(row, this.resultSetNumber);
          this.rowsSeen++;
        }
        else {
          row = null;
        }
        if (this.statisticsTimingOn)
          this.nextTime += getElapsedNanos(this.beginTime);
        return row;
      }
      else {
        if (this.cacheIter != null) {
          this.cacheIter = null;
        }
        {
          final ExecRow row;
          if (this.execNotNeeded) {
            row = null;
          }
          else {
            row = this.source.getNextRow();
          }
          if (row != null) {
            this.getActivation().setCurrentRow(row, this.resultSetNumber);
            this.rowsCached.add(row);
          }
          else {
            // exhausted the results.
            this.isCacheComplete = true;
            try {
              this.keepNcjRSOpen = true;
              if (this.source != null) {
                this.source.close(false);
              }
            } catch (Exception ignore) {
              LogWriter logger = Misc.getCacheLogWriterNoThrow();
              logger.warning("Exception in closing Ncj Pull resultset", ignore);
            } finally {
              this.keepNcjRSOpen = false;
            }
          }
          // Update the run time statistics
          if (row != null) {
            this.rowsSeen++;
          }
          if (this.statisticsTimingOn)
            this.nextTime += getElapsedNanos(this.beginTime);
          return row;
        }
      }
    }
  }

  @Override
  public void close(boolean cleanupOnError) throws StandardException {
    this.beginTime = this.statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    if (this.isOpen && !this.keepNcjRSOpen) {
      if (this.source != null) {
        this.childActivation.close();
        this.source.close(cleanupOnError);
        this.rowsCached = null;
        this.cacheIter = null;
        this.isCacheComplete = false;
        this.openedOnce = false;
      }
      this.localTXState = null;
      this.localTXStateSet = false;
    }
    this.execNotNeeded = false;
    this.isOpen = false;
    if (this.statisticsTimingOn)
      this.closeTime += getElapsedNanos(this.beginTime);
  }

  public long estimateMemoryUsage() throws StandardException {
    long memory = 0;
    if (this.rowsCached != null) {
      for (ExecRow row : this.rowsCached) {
        memory += row.estimateRowSize();
      }
    }

    if (!this.isCacheComplete) {
      memory += this.lcc.getLanguageConnectionFactory().getExecutionFactory()
          .getResultSetStatisticsFactory().getResultSetMemoryUsage(source);
    }

    return memory;
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    // Do Nothing
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState)
      throws StandardException {
    // Do Nothing
  }

  /**
   * Return the total amount of time spent in this ResultSet
   * 
   * @param type
   *          CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
   *          ENTIRE_RESULTSET_TREE - time spent in this ResultSet and below.
   * 
   * @return long The total amount of time spent (in milliseconds).
   */
  @Override
  public final long getTimeSpent(int type, int timeType) {
    final long time = PlanUtils.getTimeSpent(this.constructorTime,
        this.openTime, this.nextTime, this.closeTime, timeType);

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullResultSet totalTime = " + time);
      }
    }

    if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY) {
      return time - this.source.getTimeSpent(ENTIRE_RESULTSET_TREE, ALL);
    }
    else {
      return timeType == ResultSet.ALL ? (time - this.constructorTime) : time;
    }
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.setNumberOfChildren(1);
    visitor.visit(this);
    this.source.accept(visitor);
  }

  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder,
      PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);

    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_NCJPULL);

    if (this.source != null && this.source instanceof NoPutResultSet) {
      ((NoPutResultSet)this.source).buildQueryPlan(builder,
          context.pushContext());
    }

    PlanUtils.xmlCloseTag(builder, context, this);

    return builder;
  }

  @Override
  public void setGfKeysForNCJoin(ArrayList<DataValueDescriptor> inlist)
      throws StandardException {
    if (this.hasVarLengthInList) {
      if (inlist.size() == 0) {
        /* This means that there is no value for this predicate 
         * and thus we should not execute this query fragment.
         */
        this.execNotNeeded = true;
      }
      this.extraParams.add(inlist);
    }
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullResultSet::setGfKeysForNCJoin called with keys-size "
                + inlist.size() + " ,execNotNeeded" + this.execNotNeeded
                + " ,hasVarLengthInList" + this.hasVarLengthInList);
      }

      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "NcjPullResultSet::setGfKeysForNCJoin called with keys " + inlist);
      }
    }
  }

  /*
   * @see GfxdSubqueryResultSet.openCore()
   */
  private static Activation getChildActivation(Activation activation,
      GenericPreparedStatement ps) throws StandardException {
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    Activation childActivation = ps.getActivation(lcc, false, ps.getSource(),
        true /*addToLCC*/);
    if (GemFireXDUtils.TraceNCJ) {
      SanityManager.DEBUG_PRINT(
          GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
          "NcjPullResultSet::getChildActivation. " + "activation ="
              + activation + " ;stmtID(act)=" + activation.getStatementID()
              + " ;stmtID(lcc)=" + lcc.getStatementId() + " ;rootID(act)="
              + activation.getRootID() + " ;level(act)="
              + activation.getStatementLevel());
    }

    childActivation.setTimeOutMillis(activation.getTimeOutMillis());
    childActivation.setMaxRows(0);
    childActivation.setIsPrepStmntQuery(true);
    /* This will be the non cacheable connection ID. */
    childActivation.setConnectionID(EmbedConnection.CHILD_NOT_CACHEABLE);
    /* OK to use parent's statement ID as the connection is not cacheable. */
    childActivation.setStatementID(lcc.getStatementId());
    childActivation.setExecutionID(activation.getExecutionID());
    /* set child activation's rootId to parent activation's statementId*/
    childActivation.setRootID(activation.getStatementID());
    /* set child activation's statementLevel to one more than parents*/
    childActivation.setStatementLevel(activation.getStatementLevel()+1);
    if (SanityManager.DEBUG) {
      if (activation.getStatementLevel() != 1) {
        SanityManager.ASSERT(
            activation.getStatementLevel() == 1,
            "Statement level should be one, but its "
                + activation.getStatementLevel());
      }
    }
    /* Only needed when this.isRemoteScan is true; harmless otherwise*/
    childActivation.setFunctionContext(activation.getFunctionContext());
    return childActivation;
  }

  /*
   * Set Parameters needed for execution of NCJ Query Fragments
   */
  private static void setParametersOnChildActivation(
      Activation childActivation, ParameterValueSet allPvs,
      List<Integer> paramsIndexList,
      List<ArrayList<DataValueDescriptor>> extraParams,
      LanguageConnectionContext lcc) throws StandardException {
    if (childActivation.getParameterValueSet() != null) {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceNCJ) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
              "NcjPullResultSet::setParametersOnChildActivation "
                  + "left param count="
                  + childActivation.getParameterValueSet().getParameterCount()
                  + " ,right-params-index=" + paramsIndexList
                  + " ,right-param-count=" + paramsIndexList.size()
                  + " ,extra-param-count=" + extraParams.size());
        }
      }

      if (childActivation.getParameterValueSet().getParameterCount() != (paramsIndexList
          .size() + extraParams.size())) {
        throw new AssertionError(
            "NcjPullResultSet::setParametersOnChildActivation Parameter Value Set "
                + "do not have all required parameters. " + "left param count="
                + childActivation.getParameterValueSet().getParameterCount()
                + " ,right param count=" + paramsIndexList.size()
                + " ,extra param count=" + extraParams.size());
      }

      Vector<DataValueDescriptor> paramDVDs = new Vector<DataValueDescriptor>(
          paramsIndexList.size() + extraParams.size());
      for (int paramIndex : paramsIndexList) {
        paramDVDs.add(allPvs.getParameter(paramIndex));
      }

      // We already set params.size() number of parameters.
      // Now we need to set the rest.
      if (extraParams.size() > 0) {
        int missingParamIndex = paramsIndexList.size();
        ParameterValueSet childPvs = childActivation.getParameterValueSet();
        for (ArrayList<DataValueDescriptor> inlist : extraParams) {
          // We already set DVDSet at RHS @see ParameterNode.setType()
          DVDSet nullDVDSet = (DVDSet)childPvs
              .getParameter(missingParamIndex++);
          DataTypeDescriptor dtp = nullDVDSet.getResultDescriptor();
          DVDSet newDVDSet = new DVDSet(dtp,
              DVDSetHashingStrategy.getInstance());
          for (DataValueDescriptor val : inlist) {
            if (!val.isNull()) {
              newDVDSet.addValueAndCheckType(val);
            }
          }

          paramDVDs.add(newDVDSet);
        }

        // Clear the list so in next execution it gets new values
        extraParams.clear();
      }

      // @see AbstractGemFireActivation.setupActivation
      GenericParameterValueSet ncjPvs = (GenericParameterValueSet)lcc
          .getLanguageFactory()
          .newParameterValueSet(
              lcc.getLanguageConnectionFactory().getClassFactory()
                  .getClassInspector(), paramDVDs.size(), false/*return parameter*/);

      // TODO: In case of issues, check for parameter index (can be +1)
      // Current initialisation has not taken care of Types (DataTypeDescriptor)
      ncjPvs.initialize(paramDVDs);
      childActivation.setParameters(ncjPvs, null /* Ok to pass null*/);

      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceNCJIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
              "NcjPullResultSet::setParametersOnChildActivation "
                  + "Params set=" + paramDVDs);
        }
      }
    }
    else {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullResultSet::setParametersOnChildActivation. "
                + "Activation ==" + childActivation + "  has Null params.");
      }

      if (paramsIndexList != null && paramsIndexList.size() > 0
          || extraParams.size() > 0) {
        throw new AssertionError(
            "NcjPullResultSet::setParametersOnChildActivation."
                + " Activation ==" + childActivation
                + "  has Null params but got list of parameters "
                + "  with right param count=" + paramsIndexList.size()
                + " ,extra param count=" + extraParams.size());
      }
    }
  }

  /*
   * For execution of NCJ Query Fragments
   */
  private static ResultSet executeSqlAsPreparedStatement(
      Activation parentActivation, Activation childActivation,
      GenericPreparedStatement ps, boolean execNotNeeded, int prID)
      throws StandardException {
    ResultSet rSet = null;
    if (!execNotNeeded) {
      ps.setFlags(true, true);
      rSet = ps.execute(childActivation, true,
          childActivation.getTimeOutMillis(), true, true);
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullResultSet::executeSqlAsPreparedStatement. Using activation =="
                + childActivation + " for fetching results. "
                + " Create-query-info=" + ps.createQueryInfo()
                + " ,query-info=" + ps.getQueryInfo());
      }
    }
    else {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager
            .DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
                "NcjPullResultSet::executeSqlAsPreparedStatement. Execution not needed");
      }
    }

    return rSet;
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager
            .DEBUG_PRINT(
                GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
                "ResultSet Created: "
                    + this.getClass().getSimpleName()
                    + " with resultSetNumber="
                    + resultSetNumber
                    + " with source = "
                    + (this.source != null ? this.source.getClass()
                        .getSimpleName() : null)
                    + " and source ResultSetNumber = "
                    + (this.source != null
                        && this.source instanceof NoPutResultSetImpl ? ((NoPutResultSetImpl)this.source)
                        .resultSetNumber() : -1));
      }
    }
  }
}
