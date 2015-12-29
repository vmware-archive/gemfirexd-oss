/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.OnceResultSet

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
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes an expression subquery's result set and verifies that only
 * a single scalar value is being returned.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class OnceResultSet extends NoPutResultSetImpl
{
	/* Statics for cardinality check */
	public static final int DO_CARDINALITY_CHECK		= 1;
	public static final int NO_CARDINALITY_CHECK		= 2;
	public static final int UNIQUE_CARDINALITY_CHECK	= 3;

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
	private GeneratedMethod emptyRowFun;
	private int cardinalityCheck;
	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //
    public OnceResultSet(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						 int cardinalityCheck, int resultSetNumber,
						 int subqueryNumber, int pointOfAttachment,
						 double optimizerEstimatedRowCount,
						 double optimizerEstimatedCost)
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		this.emptyRowFun = emptyRowFun;
		this.cardinalityCheck = cardinalityCheck;
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		/* NOTE: We can't get code generation
		 * to generate calls to reopenCore() for
		 * subsequent probes, so we just handle
		 * it here.
		 */
		if (isOpen)
		{
			reopenCore();
			return;
		}

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		isOpen = true;
        source.openCore();

		/* Notify StatementContext about ourself so that we can
		 * get closed down, if necessary, on an exception.
		 */
		if (statementContext == null)
		{
			statementContext = getLanguageConnectionContext().getStatementContext();
		}
		statementContext.setSubqueryResultSet(subqueryNumber, this, 
											  activation.getNumSubqueries());

		numOpens++;
	    //isOpen = true;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
	 * reopen a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException 
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(isOpen, "OnceResultSet already open");

        source.reopenCore();
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
     * Return the requested value computed from the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 *			  StandardException ScalarSubqueryCardinalityViolation
	 *						Thrown if scalar subquery returns more than 1 row.
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
	    ExecRow candidateRow = null;
		ExecRow secondRow = null;
	    ExecRow result = null;
    boolean isOffHeapEnabled = GemFireXDUtils.isOffHeapEnabled();
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		// This is an ASSERT and not a real error because this is never
		// outermost in the tree and so a next call when closed will not occur.
		if (SanityManager.DEBUG)
        	SanityManager.ASSERT( isOpen, "OpenResultSet not open");

	    if ( isOpen ) 
		{
			candidateRow = source.getNextRowCore();
			
			if (candidateRow != null)
			{
				switch (cardinalityCheck)
				{
					case DO_CARDINALITY_CHECK:
					case NO_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						if (cardinalityCheck == DO_CARDINALITY_CHECK)
						{
							/* Raise an error if the subquery returns > 1 row 
							 * We need to make a copy of the current candidateRow since
							 * the getNextRow() for this check will wipe out the underlying
							 * row.
							 */
						  if(isOffHeapEnabled) {
						    this.source.releasePreviousByteSource();
						  }
							secondRow = source.getNextRowCore();
							if (secondRow != null)
							{							 
								close(false);
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;							  
							}
						}
						result = candidateRow;
						break;

					case UNIQUE_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						secondRow = source.getNextRowCore();
						DataValueDescriptor orderable1 = candidateRow.getColumn(1);
						while (secondRow != null)
						{
							DataValueDescriptor orderable2 = secondRow.getColumn(1);
							if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true)))
							{
								close(false);
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;
							}
							if(isOffHeapEnabled) {
                this.source.releasePreviousByteSource();
              }
							secondRow = source.getNextRowCore();
						}
						result = candidateRow;
						break;

					default:
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT(
								"cardinalityCheck not unexpected to be " +
								cardinalityCheck);
						}
						break;
				}
			}
			else if (rowWithNulls == null)
			{
				rowWithNulls = (ExecRow) emptyRowFun.invoke(activation);
				result = rowWithNulls;
			}
			else
			{
				result = rowWithNulls;
			}
	    }

		setCurrentRow(result);
		rowsSeen++;

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
	    return result;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen ) 
		{
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

	        source.close(cleanupOnError);

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of OnceResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	}

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment()
	{
		return pointOfAttachment;
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public final long getTimeSpent(int type, int timeType)
	{
	        final long time = PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
		}
		else
		{
                        // GemStone changes BEGIN
                        return timeType == ALL ? (time - constructorTime) : time;
                        /*(original code) return totTime; */
                        // GemStone changes END
		}
	}

// GemStone changes BEGIN
  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_ONCE);
    
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
