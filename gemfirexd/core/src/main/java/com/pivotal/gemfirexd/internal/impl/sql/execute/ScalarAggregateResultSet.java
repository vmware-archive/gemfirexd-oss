/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ScalarAggregateResultSet

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
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * This ResultSet evaluates scalar, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the scalar aggregates when scanning the source during the 
 * first call to next().
 *
 */
class ScalarAggregateResultSet extends GenericAggregateResultSet
	implements CursorResultSet 
{

	/* Run time statistics variables */
	public int rowsInput;

    // set in constructor and not altered during
    // life of object.
	public 		boolean 			singleInputRow;
	protected 	ExecIndexRow 		sortTemplateRow;
	protected 	boolean 			isInSortedOrder;		// true if source results in sorted order

	// Cache ExecIndexRow for scalar aggregates
	protected ExecIndexRow sourceExecIndexRow;

	// Remember whether or not a next() has been satisfied
	private boolean nextSatisfied;

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine. 
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    ScalarAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					Activation a,
					GeneratedMethod ra,
					int resultSetNumber,
					boolean singleInputRow,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost) throws StandardException 
	{
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost
		    , true /* GemStoneAddition */);
		this.isInSortedOrder = isInSortedOrder;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"SARS(), source expected to be non-null");
		}
		sortTemplateRow = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		this.singleInputRow = singleInputRow;

		if (SanityManager.DEBUG)
		{
			SanityManager.DEBUG("AggregateTrace","execution time: "+ 
					a.getSavedObject(aggregateItem));
		}
		recordConstructorTime();
    }


	///////////////////////////////////////////////////////////////////////////////
	//
	// ResultSet interface (leftover from NoPutResultSet)
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Open the scan.  Load the sorter and prepare to get
	 * rows from it.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"SARS.openCore(), source expected to be non-null");
	    	SanityManager.ASSERT( ! isOpen, "ScalarAggregateResultSet already open");
		}

		isOpen = true;
		sourceExecIndexRow = getExecutionFactory().getIndexableRow(sortTemplateRow);

        source.openCore();

	    //isOpen = true;
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}


	protected int countOfRows;

	/* RESOLVE - THIS NEXT METHOD IS OVERRIDEN IN DistinctScalarResultSet
	 * BEACAUSE OF A JIT ERROR. THERE IS NO OTHER
	 * REASON TO OVERRIDE IT IN DistinctScalarAggregateResultSet.  THE BUG WAS FOUND IN
	 * 1.1.6 WITH THE JIT.
	 */
	/**
	 * Return the next row.  If it is a scalar aggregate scan
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
		if (nextSatisfied)
		{
			clearCurrentRow();
			return null;
		}

	    ExecIndexRow execIndexRow = null;
	    ExecIndexRow aggResult = null;
		//only care if it is a minAgg if we have a singleInputRow, then we know
		//we are only looking at one aggregate
		boolean minAgg = (singleInputRow && aggregates[0].getAggregatorInfo().aggregateName.equals("MIN"));
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if (isOpen)
	    {
			/*
			** We are dealing with a scalar aggregate.
			** Zip through each row and accumulate.
			** Accumulate into the first row.  Only
			** the first row is cloned.
			*/
	        while ((execIndexRow = getRowFromResultSet(false)) != null)
	        {
				/*
				** Use a clone of the first row as our result.  
				** We need to get a clone since we will be reusing
				** the original as the wrapper of the source row.
				** Turn cloning off since we wont be keeping any
				** other rows.
				*/
	       try {   
				if (aggResult == null)
				{
					/* No need to clone the row when doing the min/max 
					 * optimization for MIN, since we will not do another
					 * next on the underlying result set.
					 */
					aggResult = (singleInputRow && minAgg) ?
								execIndexRow :
								(ExecIndexRow) execIndexRow.getClone();
					
					initializeScalarAggregation(aggResult);
				}
				else
				{
					accumulateScalarAggregation(execIndexRow, aggResult, false);
				}
	       }finally {
	         this.source.releasePreviousByteSource();
	       }

				/* Only need to look at first single row if 
				 * min/max optimization is on and operation is MIN
				 * or if operation is MAX first non-null row since null sorts
				 * as highest in btree
				 * Note only 1 aggregate is allowed in a singleInputRow 
                 * optimization so we only need to look at the first aggregate
				 */
				if (singleInputRow && 
					(minAgg || 
                     !aggResult.getColumn(aggregates[0].aggregatorColumnId).isNull()))
				{
					break;
				}
	        }

			/*
			** If we have aggregates, we need to generate a
			** value for them now.  Only finish the aggregation
			** if we haven't yet (i.e. if countOfRows == 0).
			** If there weren't any input rows, we'll allocate
			** one here.
			*/
			if (countOfRows == 0)
			{
				aggResult = (ExecIndexRow)finishAggregation(aggResult);
				setCurrentRow(aggResult);
				countOfRows++;
			}
	    }

		nextSatisfied = true;
		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		return aggResult;
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

	        countOfRows = 0;
			sourceExecIndexRow = null;
			source.close(cleanupOnError);

			super.close(cleanupOnError);
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SortResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);

		nextSatisfied = false;
		isOpen = false;
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
		        // GemStone changes BEGIN
			/*(original code) return	totTime - originalSource.getTimeSpent(ENTIRE_RESULTSET_TREE);*/
                        return  time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
		        // GemStone changes END
		}
		else
		{
                        // GemStone changes BEGIN
                        return timeType == ALL ? (time - constructorTime) : time;
                        /*(original code) return totTime; */
                        // GemStone changes END
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// CursorResultSet interface
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * This result set has its row location from
	 * the last fetch done. Always returns null.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		return null;
	}

	/**
	 * This result set has its row from the last fetch done. 
	 * If the cursor is closed, a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned;
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "SortResultSet expected to be open");

		return currentRow;
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// SCAN ABSTRACTION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Get a row from the input result set.  
	 *
	 * @param doClone - true of the row should be cloned
	 *
	 * @exception StandardException Thrown on error
	 */	
	public ExecIndexRow getRowFromResultSet(boolean doClone)
		throws StandardException
	{
		ExecRow					sourceRow;
		ExecIndexRow			inputRow = null;	
	  boolean isOffHeapEnabled = GemFireXDUtils.isOffHeapEnabled();
		if ((sourceRow = source.getNextRowCore()) != null)
		{
		  
			rowsInput++;
			sourceExecIndexRow.execRowToExecIndexRow(
					doClone ? sourceRow.getClone() : sourceRow);
			inputRow = sourceExecIndexRow;
      
		}

		return inputRow;
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
	    	SanityManager.ASSERT(isOpen, "NormalizeResultSet already open");

		source.reopenCore();
		numOpens++;
        countOfRows = 0;
		nextSatisfied = false;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// AGGREGATION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Run accumulation on every aggregate in this
	 * row.  This method is useful when draining the source
	 * or sorter, depending on whether or not there were any
	 * distinct aggregates.  Remember, if there are distinct
	 * aggregates, then the non-distinct aggregates were
	 * calculated on the way into the sorter and only the
	 * distinct aggregates will be accumulated here.
	 * Otherwise, all aggregates will be accumulated here.
	 *
	 * @param	inputRow	the input row
	 * @param	accumulateRow	the row with the accumulator (may be the same as the input row.
	 * @param	hasDistinctAggregates does this scan have distinct
	 *			aggregates.  Used to figure out whether to merge
	 *			or accumulate nondistinct aggregates.
	 *
	 * @exception StandardException Thrown on error
	 */
	protected void accumulateScalarAggregation
	(
		ExecRow inputRow, 
		ExecRow accumulateRow, 
		boolean hasDistinctAggregates
	)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), 
					"Null row passed to accumulateScalarAggregation");
		}
		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			if	(hasDistinctAggregates && 
				 !currAggregate.getAggregatorInfo().isDistinct())
			{
				currAggregate.merge(inputRow, accumulateRow);
			}
			else
			{
				currAggregate.accumulate(inputRow, accumulateRow);
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// CLASS SPECIFIC
	//
	///////////////////////////////////////////////////////////////////////////////

	/*
	** Run the aggregator initialization method for
	** each aggregator in the row. 
	**
	** @param	row	the row to initialize
	**
	** @return Nothing.
	**
	** @exception	standard Derby exception
	*/
	private void initializeScalarAggregation(ExecRow row)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row != null, 
					"Null row passed to initializeScalarAggregation");
		}

		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			currAggregate.initialize(row);
			currAggregate.accumulate(row, row);
		}
	}


// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    if (this.source != null) {
      this.source.updateRowLocationPostRead();
    }
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    // nothing here; we don't want to release the locks on aggregates for
    // repeatable reads and in any case there might be other group by columns
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.setNumberOfChildren(1);
    visitor.visit(this);
    source.accept(visitor);
  }
  
  public void resetStatistics() {
    rowsInput = 0;
    super.resetStatistics();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_AGGREGATE);
    
    if(this.source != null) {
      this.source.buildQueryPlan(builder, context.pushContext());
    }
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
  }
// GemStone changes END
}
