/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RowCountResultSet

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.message.RegionExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;



/**
 * This result set implements the filtering of rows needed for the <result
 * offset clause> and the <fetch first clause>.  It sits on top of the normal
 * SELECT's top result set, but under any ScrollInsensitiveResultSet needed for
 * cursors. The latter positioning is needed for the correct functioning of
 * <result offset clause> and <fetch first clause> in the presence of
 * scrollable and/or updatable result sets and CURRENT OF cursors.
 *
 * It is only ever generated if at least one of the two clauses is present.
 */
class RowCountResultSet extends NoPutResultSetImpl
    implements CursorResultSet
{
    // set in constructor and not altered during
    // life of object.
    final NoPutResultSet source;
    final private boolean runTimeStatsOn;
    private long offset;
    private long fetchFirst;
    final private GeneratedMethod offsetMethod;
    final private GeneratedMethod fetchFirstMethod;

    /**
     * True if we haven't yet fetched any rows from this result set.
     * Will be reset on close so the result set is ready to reuse.
     */
    private boolean virginal;

    /**
     * Holds the number of rows returned so far in this round of using the
     * result set.  Will be reset on close so the result set is ready to reuse.
     */
    private long rowsFetched;

    /**
     * RowCountResultSet constructor
     *
     * @param s               The source result set being filtered
     * @param a               The activation for this result set,
     *                        which provides the context for the row
     *                        allocation operation
     * @param resultSetNumber The resultSetNumber for the ResultSet
     * @param offsetMethod   Generated method
     * @param fetchFirstMethod Generated method
     * @param optimizerEstimatedRowCount
     *                        Estimated total # of rows by optimizer
     * @param optimizerEstimatedCost
     *                        Estimated total cost by optimizer
     * @exception StandardException Standard error policy
     */
    RowCountResultSet
        (NoPutResultSet s,
         Activation a,
         int resultSetNumber,
         GeneratedMethod offsetMethod,
         GeneratedMethod fetchFirstMethod,
         double optimizerEstimatedRowCount,
         double optimizerEstimatedCost)
            throws StandardException {

        super(a,
              resultSetNumber,
              optimizerEstimatedRowCount,
              optimizerEstimatedCost);

        this.offsetMethod = offsetMethod;
        this.fetchFirstMethod = fetchFirstMethod;

        source = s;

        virginal = true;
        rowsFetched = 0;

        /* Remember whether or not RunTimeStatistics is on */
        runTimeStatsOn =
            getLanguageConnectionContext().getRunTimeStatisticsMode();
        recordConstructorTime();
        
        // GemStone changes BEGIN
        printResultSetHierarchy();
        // GemStone changes END
    }

    //
    // NoPutResultSet interface
    //

    private void initializeOffsetAndFetchLimit() throws StandardException {
      if (offsetMethod != null) {
        DataValueDescriptor offVal = (DataValueDescriptor)offsetMethod
            .invoke(activation);

        if (offVal.isNotNull().getBoolean()) {
          offset = offVal.getLong();

          if (offset < 0) {
            throw StandardException.newException(
                SQLState.LANG_INVALID_ROW_COUNT_OFFSET, Long.toString(offset));
          }
          else {
            offset = offVal.getLong();
          }
        }
        else {
          throw StandardException.newException(
              SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL, "OFFSET");
        }
      }
      else {
        // not given
        offset = 0;
      }

      if (fetchFirstMethod != null) {
        DataValueDescriptor fetchFirstVal =
            (DataValueDescriptor)fetchFirstMethod.invoke(activation);

        if (fetchFirstVal.isNotNull().getBoolean()) {

          fetchFirst = fetchFirstVal.getLong();

          if (fetchFirst < 1) {
            throw StandardException.newException(SQLState
                .LANG_INVALID_ROW_COUNT_FIRST, Long.toString(fetchFirst));
          }
        }
        else {
          throw StandardException.newException(
              SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL, "FETCH FIRST/NEXT");
        }
      }
// GemStone changes BEGIN
      if (getLanguageConnectionContext().isConnectionForRemote()
          && activation.getFunctionContext() != null
          && !((RegionExecutorMessage<?>)activation.getFunctionContext())
              .allTablesAreReplicatedOnRemote()) {
        if (fetchFirst != -1) {
          fetchFirst += offset;
        }
        offset = 0;
      }
// GemStone changes END
      final long fetchFirst = this.fetchFirst;
      if (fetchFirst > 0) {
        source.setMaxSortingLimit(offset <= 0 ? fetchFirst : fetchFirst
            + offset);
      }
    }

    /**
     * Open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void openCore() throws StandardException {

        boolean constantEval = true;

        beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

        isOpen = true;
        initializeOffsetAndFetchLimit();
        source.openCore();
        //isOpen = true;

        numOpens++;

        if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
    }

    /**
     * Reopen a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void reopenCore() throws StandardException {

        //boolean constantEval = true;

        beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(isOpen,
                                 "RowCountResultSet not open, cannot reopen");

        isOpen = true;
        initializeOffsetAndFetchLimit();
        source.reopenCore();

        //isOpen = true;
        rowsFetched = 0;
        virginal = true;
        
        numOpens++;

        if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
    }

    /**
     * Return the requested values computed from the next row (if any)
     * <p>
     * @exception StandardException thrown on failure.
     * @exception StandardException ResultSetNotOpen thrown if not yet open.
     *
     * @return the next row in the result
     */
    public ExecRow  getNextRowCore() throws StandardException {

      ExecRow result = null;

      beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

      if (virginal) {
          /* (moved into initializeOffsetAndFetchLimit() invoked from
              openCore/reopenCore)
          if (offsetMethod != null) {
              DataValueDescriptor offVal
                  = (DataValueDescriptor)offsetMethod.invoke(activation);

              if (offVal.isNotNull().getBoolean()) {
                  offset = offVal.getLong();

                  if (offset < 0) {
                      throw StandardException.newException(
                          SQLState.LANG_INVALID_ROW_COUNT_OFFSET,
                          Long.toString(offset));
                  } else {
                      offset = offVal.getLong();
                  }
              } else {
                  throw StandardException.newException(
                      SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL,
                      "OFFSET");
              }
          } else {
              // not given
              offset = 0;
          }


          if (fetchFirstMethod != null) {
              DataValueDescriptor fetchFirstVal
                  = (DataValueDescriptor)fetchFirstMethod.invoke(activation);

              if (fetchFirstVal.isNotNull().getBoolean()) {

                  fetchFirst = fetchFirstVal.getLong();

                  if (fetchFirst < 1) {
                      throw StandardException.newException(
                          SQLState.LANG_INVALID_ROW_COUNT_FIRST,
                          Long.toString(fetchFirst));
                  }
              } else {
                  throw StandardException.newException(
                      SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL,
                      "FETCH FIRST/NEXT");
              }
          }
          //GemStone changes BEGIN                 
             if(getLanguageConnectionContext().isConnectionForRemote()
                 && activation.getFunctionContext() != null
                 && !((RegionExecutorMessage<?>)activation.getFunctionContext())
                 .allTablesAreReplicatedOnRemote()) {             
               if(fetchFirst != -1) {              
                 fetchFirst += offset;                  
               }              
               offset = 0;                 
             }                
          //GemStone changes END 
          */

          if (offset > 0) {
              // Only skip rows the first time around
              virginal = false;

              long offsetCtr = offset;

              // GemStone changes BEGIN
              final TXState localTXState = initLocalTXState();
              // GemStone changes END
              do {
                  result = source.getNextRowCore();
                  offsetCtr--;

                  if (result != null && offsetCtr >= 0) {
                      rowsFiltered++;
                      // GemStone changes BEGIN
                     // if (localTXState != null) {
                        filteredRowLocationPostRead(localTXState);
                      //}
                     // GemStone changes END
                  } else {
                      break;
                  }
              } while (true);
          } else {
              if (fetchFirstMethod != null && rowsFetched >= fetchFirst) {
                  result = null;
              } else {
                  result = source.getNextRowCore();
              }
          }
      } else {
          if (fetchFirstMethod != null && rowsFetched >= fetchFirst) {
              result = null;
          } else {
              result = source.getNextRowCore();
          }
      }


      if (result != null) {
          rowsFetched++;
          rowsSeen++;
      }

      setCurrentRow(result);

      if (runTimeStatsOn) {
          if (! isTopResultSet) {
               // This is simply for RunTimeStats.  We first need to get the
               // subquery tracking array via the StatementContext
              StatementContext sc = activation.getLanguageConnectionContext().
                  getStatementContext();
              subqueryTrackingArray = sc.getSubqueryTrackingArray();
          }

          if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
      }
      return result;
  }

    /**
     * Return the total amount of time spent in this ResultSet
     *
     * @param type
     *    CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
     *    ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
     *
     * @return long     The total amount of time spent (in milliseconds).
     */
    public final long getTimeSpent(int type, int timeType) {
        final long time = PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);

        if (type == CURRENT_RESULTSET_ONLY) {
            return  time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
        } else {
          // GemStone changes BEGIN
          return timeType == ALL ? (time - constructorTime) : time;
          /*(original code) return totTime; */
          // GemStone changes END
        }
    }

    // ResultSet interface

    /**
     * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#close
     */
    public void close(boolean cleanupOnError) throws StandardException {

        beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
        if ( isOpen ) {

            // we don't want to keep around a pointer to the
            // row ... so it can be thrown away.
            // REVISIT: does this need to be in a finally
            // block, to ensure that it is executed?
            clearCurrentRow();
            source.close(cleanupOnError);

            super.close(cleanupOnError);
        } else {
            if (SanityManager.DEBUG) {
                SanityManager.DEBUG("CloseRepeatInfo",
                                    "Close of RowCountResultSet repeated");
            }
        }

        // Reset state for result set reuse, if any
        virginal = true;
        rowsFetched = 0;

        if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
    }


    /**
     * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#finish
     */
    public void finish() throws StandardException {
        source.finish();
        finishAndRTS();
    }


    /**
     * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#clearCurrentRow
     */
    public final void clearCurrentRow()
    {
        currentRow = null;
        activation.clearCurrentRow(resultSetNumber);

        // Added this since we need it to keep in synch for updatable result
        // sets/cursors; this result set needs to be "transparent" in such
        // cases, cf. getCurrentRow which gets the current row from the source
        // as well.
        source.clearCurrentRow();
    }



    //
    // CursorResultSet interface
    //

    /**
     * Gets information from its source.
     *
     * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet#getRowLocation
     */
    public RowLocation getRowLocation() throws StandardException {

        return ( (CursorResultSet)source ).getRowLocation();
    }


    /**
     * Gets information from source
     *
     * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet#getCurrentRow
     * @return the last row returned.
     */

    /* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
     * once there is such a method.  (currentRow is redundant)
     */
    public ExecRow getCurrentRow() throws StandardException
    {
        return ( (CursorResultSet)source ).getCurrentRow();
        // return currentRow;
    }

    /**
     * Override of NoPutResultSetImpl method. Ask the source.
     */
    public boolean isForUpdate() {
        return source.isForUpdate();
    }


    /**
     * Return underlying result set (the source og this result set) if it is a
     * ProjectRestrictResultSet, else null.
     */
    public ProjectRestrictResultSet getUnderlyingProjectRestrictRS() {
        if (source instanceof ProjectRestrictResultSet) {
            return (ProjectRestrictResultSet)source;
        } else {
            return null;
        }
    }

// GemStone changes BEGIN

    @Override
    public void updateRowLocationPostRead() throws StandardException {
      this.source.updateRowLocationPostRead();
    }

    @Override
    public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
      this.source.filteredRowLocationPostRead(localTXState);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsMoveToNextKey() {
      return this.source.supportsMoveToNextKey();
    }

    @Override
    public int getScanKeyGroupID() {
      return this.source.getScanKeyGroupID();
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
    
    @Override
    public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
      
      super.buildQueryPlan(builder, context);
      
      PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_ROW_COUNT);
      
      if(this.source != null)
        this.source.buildQueryPlan(builder, context.pushContext());
      
      PlanUtils.xmlCloseTag(builder, context, this);
      return builder;
    }
    
    @Override
    public void printResultSetHierarchy() {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceNCJ) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
              "ResultSet Created: "
                  + this.getClass().getSimpleName()
                  + " with resultSetNumber="
                  + resultSetNumber
                  + " with source = "
                  + (this.source != null ? this.source.getClass().getSimpleName()
                      : null) + " and source ResultSetNumber = "
                  + (this.source != null ? this.source.resultSetNumber() : -1));
        }
      }
    }
// GemStone changes END
}
