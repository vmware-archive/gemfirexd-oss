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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.JDBC30Translation;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameter;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BasicNoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * 
 * @author Asif
 */
public class GfxdSubqueryResultSet extends NoPutResultSetImpl {

  private ResultSet source;

  //private final String sqlText;  
  //private final byte subqueryType = 0x00;
  final private  boolean whereClauseBased;
  final List<Integer> params;
  final private GenericPreparedStatement ps;
  private final boolean isGFEActvn;
  private List<DataValueDescriptor> cachedRows;
  private boolean cacheComplete;
  private Iterator<DataValueDescriptor> pointer;
  private boolean openedOnce;
  private Activation childActivation;
  private ValueRow templateRow;
  private final SelectQueryInfo sqi;
  private boolean keepSubqueryRSOpen = false;

  public GfxdSubqueryResultSet(boolean whereClauseBased, String subqueryText, 
      Activation parentActivation, SelectQueryInfo sqi, List<Integer> params,
      GenericPreparedStatement ps, int rsNum, boolean isGFEActvn) {
    super(parentActivation, rsNum, 0,0);
    //this.sqlText = subqueryText;
    this.whereClauseBased = whereClauseBased;
    this.params = params;
    this.ps = ps;
    this.isGFEActvn = isGFEActvn;
    this.sqi = sqi;
    recordConstructorTime();
    
    // GemStone changes BEGIN
    printResultSetHierarchy();
    // GemStone changes END
  }

  @Override
  public ExecRow getNextRowCore() throws StandardException {
    GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder.getInstance();
    if (observer != null) {
      observer.onGetNextRowCoreOfGfxdSubQueryResultSet(this);
    }
    beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    if (cacheComplete) {
      if (pointer.hasNext()) {
        DataValueDescriptor dvd = pointer.next();
        ValueRow row;
        if (dvd != null) {
          row = this.templateRow;
          row.setColumn(1, dvd);
          this.getActivation().setCurrentRow(row, this.resultSetNumber);
          rowsSeen++;
        }
        else {
          row = null;
        }
        if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
        return row;
      }
      else {
        if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
        return null;
      }
    }
    else {
      // check if there is any cached data
      if (this.pointer != null && this.pointer.hasNext()) {
        DataValueDescriptor dvd = this.pointer.next();
        ValueRow row;
        if (dvd != null) {
          row = this.templateRow;
          row.setColumn(1, dvd);
          this.getActivation().setCurrentRow(row, this.resultSetNumber);
          rowsSeen++;
        }
        else {
          row = null;
        }
        if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
        return row;
      }
      else {
        if (pointer != null) {
          pointer = null;
        }
        if (this.whereClauseBased) {
          ExecRow row = this.source.getNextRow();
          if (row != null) {
            DataValueDescriptor dvd = row.getColumn(1).getClone();
            row = this.templateRow;
            row.setColumn(1, dvd);
            this.getActivation().setCurrentRow(row, this.resultSetNumber);
            this.cachedRows.add(dvd);
          }
          else {
            // exhausted the results.
            this.cacheComplete = true;
            try {
              this.keepSubqueryRSOpen = true;
              this.source.close(false);              
            }catch(Exception ignore ) {
              LogWriter logger = Misc.getCacheLogWriterNoThrow();
              logger.warning("Exception in closing subquery resultset", ignore);
            }finally {
              this.keepSubqueryRSOpen = false;
            }
          }
          // Update the run time statistics
          if (row != null) {
            rowsSeen++;
          }
          if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
          return row;

        }
        else {
          throw new AssertionError("Supporting only where clause "
              + "based subquery, should not reach here");
        }
      }
    }
  }

  @Override
  public void reopenCore() throws StandardException {
    beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    isOpen = true;
    this.pointer = this.cachedRows.iterator();
    this.templateRow = new ValueRow(1);
    // openCore();
    numOpens++;
    if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
  }

  public void openCore() throws StandardException {
    try {
      beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
      isOpen = true;
      this.templateRow = new ValueRow(1);
      if (openedOnce) {
        this.pointer = this.cachedRows.iterator();
        numOpens++;
        if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
        return;
      }
      this.cachedRows = new ArrayList<DataValueDescriptor>();
      this.pointer = this.cachedRows.iterator();
      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset::openCore");
      }

      LanguageConnectionContext lcc = this.activation
          .getLanguageConnectionContext();

      // This statement has been prepared during the preparing of the parent
      // statement and is stored in the cache. As a result, it reduces the time
      // spent during the execution of the parent statement. But I do not know
      // how to obtain the sqlText during compiling time?
      // [sb] now we stuff in the sub-query string from SubqueryNode to ps
      // and here we extract from it. This makes GenericStatement.getSource()
      // at par with ps.getSource().
      // passing addToLcc as true so that we can cancel the subquery in 
      // case of a cancel request
      childActivation = ps
          .getActivation(lcc, false, ps.getSource(), true /*addToLCC*/);
      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset::Using activation ==" + childActivation);
      }

      childActivation.setTimeOutMillis(this.activation.getTimeOutMillis());
      childActivation.setMaxRows(0);
      childActivation.setIsPrepStmntQuery(this.sqi.isPreparedStatementQuery());
      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset::is sub query prepared statement =="
                + this.sqi.isPreparedStatementQuery());
      }
      /* This will be the non cacheable connection ID. */
      childActivation.setConnectionID(EmbedConnection.CHILD_NOT_CACHEABLE);
      /* OK to use parent's statement ID as the connection is not cacheable. */
      childActivation.setStatementID(lcc.getStatementId());
      childActivation.setExecutionID(getActivation().getExecutionID());
      ps.setFlags(true, true);

      if (this.sqi.getParameterCount() > 0) {
        if (childActivation.getParameterValueSet() != null
            && childActivation.getParameterValueSet() != this.getActivation()
                .getParameterValueSet()) {
          throw new AssertionError(
              "GfxdSubqueryResultSet: Parameter Value Set "
                  + "should have been null for subquery");
        }
        ParameterValueSet subPvs = createSubqueryPVSWrapper();
        childActivation.setParameters(subPvs, null /* Ok to pass null*/);
      }
      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset::Using activation ==" + childActivation
                + " for fetching results");
      }

      this.source = ps.execute(childActivation, true,
          childActivation.getTimeOutMillis(), true, true);

      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset::openCore:fetched subquery results");
      }

      GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder.getInstance();
      if (observer != null) {
        observer.independentSubqueryResultsetFetched(this.activation,
            this.source);
      }
      openedOnce = true;
      numOpens++;
      if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
    } catch (StandardException se) {
      if (se.getMessageId().equals(
          SQLState.LANG_STATEMENT_NEEDS_RECOMPILE_PARENT)) {
        throw StandardException
            .newException(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE);
      }
      else {
        throw se;
      }
    }
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
  public final long getTimeSpent(int type, int timeType) {
    final long time = PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceActivation) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ACTIVATION,
            "GfxdSubQueryResultset totalTime = " + time);
      }
    }

    if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY) {
      return time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, ALL);
    }
    else {
      return timeType == ResultSet.ALL ? (time - constructorTime) : time;
    }
  }

  @Override
  public void close(boolean cleanupOnError) throws StandardException {
    beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    if (this.isOpen && !this.keepSubqueryRSOpen) {
      if (this.source != null) {
        if (this.source instanceof BasicNoPutResultSetImpl) {
          ((BasicNoPutResultSetImpl)this.source).subqueryTrackingArray = null;
        }
        this.childActivation.close();
        this.source.close(cleanupOnError);
        this.cachedRows = null;
        this.pointer = null;
        this.templateRow = null;
        this.cacheComplete = false;
        this.openedOnce = false;
      }
      this.localTXState = null;
      this.localTXStateSet = false;
    }
    this.isOpen = false;
    if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
  }

  /*
  
  circular reference. So, BaseActivation now releases pvs on close.
  ----------------------------------------------------------------------------------------------------------------------------------
  acts java.util.ArrayList @ 0xe66b0380                                                                                             
  '- elementData java.lang.Object[16] @ 0xec5d5a68                                                                                  
   '- [15] com.pivotal.gemfirexd.internal.exe.ac3fb00280x0133xb258x88a0x0000036a25f00 @ 0xed16eec0                                   
      '- fc com.pivotal.gemfirexd.internal.engine.distributed.message.PrepStatementExecutorMessage @ 0xed16f570                      
         '- pvs com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet$SubqueryParameterValueSetWrapper @ 0xed16fcc0
            '- this$0 com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet @ 0xed16fdf0                           
               '- activation com.pivotal.gemfirexd.internal.exe.ac3e6e0271x0133xb258x88a0x0000036a25f00 @ 0xed16feb0                 
                  '- fc com.pivotal.gemfirexd.internal.engine.distributed.message.PrepStatementExecutorMessage @ 0xec7185b0          
  ----------------------------------------------------------------------------------------------------------------------------------

   */
  private abstract class SubqueryParameterValueSetWrapper 
 {
    final ParameterValueSet allPvs;

    public SubqueryParameterValueSetWrapper(ParameterValueSet all) {
      this.allPvs = all;
    }

    public ParameterValueSet getClone() {
      throw new IllegalStateException("This should not have got invoked");
    }

    public DataValueDescriptor getParameter(int position)
        throws StandardException {
      int indx;
      if (isGFEActvn) {
        indx = params.get(position).intValue();
      }
      else {
        indx = position;
      }
      return this.allPvs.getParameter(indx);
    }

    public int getParameterCount() {
      return params.size();
    }

    public DataValueDescriptor getParameterForGet(int position)
        throws StandardException {
      return this.allPvs.getParameterForGet(params.get(position));
    }

    public DataValueDescriptor getParameterForSet(int position)
        throws StandardException {
      return this.allPvs.getParameterForSet(params.get(position));
    }

    public short getParameterMode(int parameterIndex) {

      return this.allPvs.getParameterMode(params.get(parameterIndex));
    }

    public int getPrecision(int parameterIndex) {
      return this.allPvs.getPrecision(params.get(parameterIndex));
    }

    public DataValueDescriptor getReturnValueForSet() throws StandardException {
      throw new IllegalStateException("This should not have got invoked");
    }

    public int getScale(int parameterIndex) {
      return this.allPvs.getScale(params.get(parameterIndex));
    }

    public boolean hasReturnOutputParameter() {
      return false; // hard code it temporarily
    }

    public void initialize(DataTypeDescriptor[] types) throws StandardException {
      //throw new IllegalStateException("This should not have got invoked");

    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
        throws StandardException {
      this.allPvs.registerOutParameter(params.get(parameterIndex), sqlType,
          scale);

    }

    public void setParameterAsObject(int parameterIndex, Object value)
        throws StandardException {
      this.allPvs.setParameterAsObject(params.get(parameterIndex), value);

    }

    public void setParameterMode(int position, int mode) {
      this.allPvs.setParameterMode(params.get(position).intValue(), mode);

    }

    public void validate() throws StandardException {
      throw new IllegalStateException("This should not have got invoked");

    }

    public boolean isListOfConstants() {
      return allPvs.isListOfConstants();
    }

    public boolean canReleaseOnClose() {
      return true;
    }
  }
  
  private class GenricSubqueryPVSWrapper extends
      SubqueryParameterValueSetWrapper implements ParameterValueSet {
    GenricSubqueryPVSWrapper(ParameterValueSet pvs) {
      super(pvs);
    }

    @Override
    public int allAreSet() {
      GenericParameterValueSet gpvs = (GenericParameterValueSet)this.allPvs;
      for (Integer posn : params) {
        GenericParameter gp = gpvs.getGenericParameter(posn.intValue());
        if (!gp.isSet()) {
          switch (gp.getParameterMode()) {
            case JDBC30Translation.PARAMETER_MODE_OUT:
              break;
            case JDBC30Translation.PARAMETER_MODE_IN_OUT:
            case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
            case JDBC30Translation.PARAMETER_MODE_IN:
              return posn + 1;
          }
        }
      }

      return 0;
    }

    @Override
    public boolean checkNoDeclaredOutputParameters() {
      boolean hasDeclaredOutputParameter = false;
      GenericParameterValueSet gpvs = (GenericParameterValueSet)this.allPvs;
      for (Integer posn : params) {
        GenericParameter gp = gpvs.getGenericParameter(posn.intValue());

        switch (gp.getParameterMode()) {
          case JDBC30Translation.PARAMETER_MODE_IN:
            break;
          case JDBC30Translation.PARAMETER_MODE_IN_OUT:
          case JDBC30Translation.PARAMETER_MODE_OUT:
            hasDeclaredOutputParameter = true;
            break;
          case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
            gp.setParameterMode((short)JDBC30Translation.PARAMETER_MODE_IN);
            break;
        }
      }

      return hasDeclaredOutputParameter;
    }

    @Override
    public void clearParameters() {
      GenericParameterValueSet gpvs = (GenericParameterValueSet)this.allPvs;
      for (Integer posn : params) {
        GenericParameter gp = gpvs.getGenericParameter(posn.intValue());
        gp.clear();
      }
    }

    @Override
    public void transferDataValues(ParameterValueSet pvstarget)
        throws StandardException {
      GenericParameterValueSet gpvs = (GenericParameterValueSet)this.allPvs;
      int firstParam = pvstarget.hasReturnOutputParameter() ? 1 : 0;
      int paramSize = params.size();
      for (int i = firstParam; i < paramSize; i++) {
        GenericParameter oldp = gpvs.getGenericParameter(params.get(i)
            .intValue());
        if (oldp.getRegisterOutputType() != Types.NULL) {

          pvstarget.registerOutParameter(i, oldp.getRegisterOutputType(),
              oldp.getRegisterOutScale());

        }

        if (oldp.getIsSet()) {
          pvstarget.getParameterForSet(i).setValue(oldp.getValue());
        }
      }
    }
  }

  private class ConstantValueSubqueryPVSWrapper extends
      SubqueryParameterValueSetWrapper implements ConstantValueSet {

    private final DataTypeDescriptor[] paramTypes;
    //private BaseActivation ownedActivation;
    private final int[] tokenKind;
    private String[] tokenImages;

    ConstantValueSubqueryPVSWrapper(ConstantValueSet cvs) {
      super(cvs);
      ConstantValueSetImpl cvsi = (ConstantValueSetImpl)cvs;
      if (isGFEActvn) {
        int size = params.size();
        int[] actualTokKind = cvsi.getTokenKinds();
        String[] actualTokImages = cvsi.getTokenImages();
        this.paramTypes = new DataTypeDescriptor[size];
        this.tokenKind = new int[size];
        this.tokenImages = new String[size];
        int k = 0;
        for (Integer actualIndex : params) {
          this.tokenKind[k] = actualTokKind[actualIndex];
          this.tokenImages[k] = actualTokImages[actualIndex];
          k++;
        }
      }
      else {
        this.paramTypes = cvsi.getParamTypes();
        this.tokenImages = cvsi.getTokenImages();
        this.tokenKind = cvsi.getTokenKinds();
      }
    }

    @Override
    public int allAreSet() {
      return 0;
    }

    public boolean checkNoDeclaredOutputParameters() {
      return false;
    }

    public void clearParameters() {
      throw new IllegalStateException(
          "Soubhik: This should not have got invoked as batch operation is not possible for statements ");
    }

    public void transferDataValues(ParameterValueSet pvstarget)
        throws StandardException {
      ConstantValueSetImpl cpvsi = (ConstantValueSetImpl)this.allPvs;
      cpvsi.setTransferData(this.paramTypes, this.tokenKind, this.tokenImages);
    }

    @Override
    public String[] getTokenImages() {
      return this.tokenImages;
    }

    @Override
    public int[] getTokenKinds() {
      return this.tokenKind;
    }

    @Override
    public DataTypeDescriptor[] getParamTypes() {
      return this.paramTypes;
    }

    @Override
    public List<TypeCompiler> getOrigTypeCompilers() {
      return null;
    }

    @Override
    public void setActivation(Activation ownedActivation) {
      //this.ownedActivation = (BaseActivation)ownedActivation;
    }

    @Override
    public void refreshTypes(DataTypeDescriptor[] dtds) {
     /* throw new UnsupportedOperationException(
          "ConstantValueSubqueryPVSWrapper::Call not expected ");*/
    }

    @Override
    public void validate() {
      if (this.tokenImages == null || this.tokenImages.length <= 0) {
        SanityManager
            .THROWASSERT("ConstantValueSubqueryPVSWrapper: Token images "
                + "are supposed to be existent");
      }
      if (tokenKind == null || tokenKind.length <= 0) {
        SanityManager
            .THROWASSERT("ConstantValueSet: Token kind should be non-null");
      }

      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceStatementMatching) {
          StringBuilder sb = new StringBuilder(
              "ConstantValueSubqueryPVSWrapper: Validating tokens : [");

          for (int i = 0; i < tokenImages.length; i++) {
            if (i != 0) {
              sb.append(',');
            }
            sb.append(tokenImages[i]).append(" (")
                .append(ConstantValueSetImpl.tokenKindAsString(tokenKind[i]))
                .append(")");
          }
          sb.append("]");
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
              sb.toString());

          sb.append("]");
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
              sb.toString());
        }
      }
    }

    @Override
    public String getConstantImage(int position) {
      ConstantValueSet cvs = (ConstantValueSet)this.allPvs;
      return cvs.getConstantImage(params.get(position).intValue());
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString());
      sb.append(";Constant Tokens=" + Arrays.toString(this.tokenImages)
          + "; param count=" + getParameterCount());
      return sb.toString();
    }

    @Override
    public int validateParameterizedData() {
      return -1;
    }
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    
    visitor.setNumberOfChildren(1);
    
    visitor.visit(this);
    
    source.accept(visitor);
  }

  @Override
  public void resetStatistics() {
    super.resetStatistics();
    source.resetStatistics();
  }

  public long estimateMemoryUsage() throws StandardException {
    long memory = 0;
    if (cachedRows != null) {
      for (DataValueDescriptor dvd : this.cachedRows) {
        memory += dvd.estimateMemoryUsage();
      }
    }

    if (!cacheComplete) {
      memory += this.lcc.getLanguageConnectionFactory().getExecutionFactory()
          .getResultSetStatisticsFactory().getResultSetMemoryUsage(source);
    }

    return memory;
  }

  private ParameterValueSet createSubqueryPVSWrapper() {
    if (sqi.isPreparedStatementQuery()) {
      return new GenricSubqueryPVSWrapper(
          this.activation.getParameterValueSet());
    }
    else {
      return new ConstantValueSubqueryPVSWrapper(
          (ConstantValueSet)activation.getParameterValueSet());
    }
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_SUBQUERY);
    
    if(this.source != null && this.source instanceof NoPutResultSet) {
      ((NoPutResultSet)this.source).buildQueryPlan(builder, context.pushContext());
    }
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
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
