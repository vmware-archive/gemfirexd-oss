/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.SortResultSet

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

import java.util.Properties;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableArrayHolder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes a source result set, sends it to the sorter,
 * and returns the results.  If distinct is true, removes
 * all but one copy of duplicate rows using DistinctAggregator,
 * which really doesn't aggregate anything at all -- the sorter
 * assumes that the presence of an aggregator means that
 * it should return a single row for each set with identical
 * ordering columns.
 * <p>
 * If aggregate is true, then it feeds any number of aggregates
 * to the sorter.  Each aggregate is an instance of GenericAggregator
 * which knows which Aggregator to call to perform the
 * aggregation.
 * <p>
 * Brief background on the sorter and aggregates: the sorter
 * has some rudimentary knowledge about aggregates.  If
 * it is passed aggregates, it will eliminate duplicates
 * on the ordering columns.  In the process it will call the
 * aggregator on each row that is discarded.
 * <p> 
 * Note that a DISTINCT on the SELECT list and an aggregate cannot 
 * be processed by the same SortResultSet(), if there are both
 * aggregates (distinct or otherwise) and a DISTINCT on the select
 * list, then 2 separate SortResultSets are required (the DISTINCT
 * is a sort on the output of the sort with the aggregation). 
 * <p>
 * Currently, all rows are fed through the sorter.  This is
 * true even if there is no sorting needed.  In this case
 * we feed every row in and just pull every row out (this is
 * an obvious area for a performance improvement).  We'll
 * need to know if the rows are sorted before we can make
 * any optimizations in this area.
 * <p>
 * <B>CLONING</B>: Cloning and sorts are an important topic.
 * Currently we do a lot of cloning.  We clone the following: <UL>
 * <LI> every row that is inserted into the sorter.  We
 * need to clone the rows because the source result set might
 * be reusing rows, and we need to be able to accumulate the
 * entire result set in the sorter. </LI>
 * <p>
 * There are two cloning APIs: cloning by the sorter on
 * rows that are not discarded as duplicates or cloning
 * in the SortResultSet prior to inserting into the sorter.
 * If we have any aggregates at all we always clone prior
 * to inserting into the sorter.  We need to do this 
 * because we have to set up the aggregators before passing
 * them into the sorter.  When we don't have aggregates
 * we let the sorter to the cloning to avoid unnecessary
 * clones on duplicate rows that are going to be discarded
 * anyway.
 *
 */
public final class SortResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{

	/* Run time statistics variables */
	public int rowsInput;
	public int rowsReturned;
	public boolean distinct;

    // set in constructor and not altered during
    // life of object.
  private NoPutResultSet source;
	private GeneratedMethod rowAllocator;
	private ColumnOrdering[] order;
	private ColumnOrdering[] savedOrder;
	private SortObserver observer;
	private ExecRow sortTemplateRow;
	public	boolean isInSortedOrder;				// true if source results in sorted order
	private	NoPutResultSet	originalSource; // used for run time stats only
	private int maxRowSize;

	/**
	 * Set by RowCountResultSet in case of FETCH FIRST/NEXT clause.
	 * Allows the sorter to be more intelligent and keep discarding higher
	 * elements where possible during the sort.
	 */
	private long maxSortLimit;

	// set in open and not modified thereafter
    private ScanController scanController;

	// argument to getNextRowFromRS()

        //GemStone changes BEGIN
        // increasing visibility
	//private ExecRow sortResultRow;
        ExecRow sortResultRow;

	// In order distincts
	//private ExecRow currSortedRow;
        ExecRow currSortedRow;
        //GemStone changes END
	private boolean nextCalled;
	private int numColumns;

	// used to track and close sorts
	private long genericSortId;
	private boolean dropGenericSort;

	// remember whether or not any sort was performed
	private boolean sorted;

	SortController sorter;

	// RTS
	public Properties sortProperties = new Properties();

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	distinct	if this is a DISTINCT select list.  
	 *		Also set to true for a GROUP BY w/o aggretates
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	orderingItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		ColumOrdering array used by this routine
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    public SortResultSet(NoPutResultSet s,
					boolean distinct,
					boolean isInSortedOrder,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost) throws StandardException 
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.distinct = distinct;
		this.isInSortedOrder = isInSortedOrder;
        source = s;
        originalSource = s;
		rowAllocator = ra;
		this.maxRowSize = maxRowSize;
		sortTemplateRow = (ExecRow) rowAllocator.invoke(activation);
		order = (ColumnOrdering[])
					((FormatableArrayHolder)
						(a.getSavedObject(orderingItem)))
					.getArray(ColumnOrdering.class);

		/* NOTE: We need to save order to another variable
		 * in the constructor and reset it on every open.
		 * This is important because order can get reset in the
		 * guts of execution below.  Subsequent sorts could get
		 * the wrong result without this logic.
		 */
		savedOrder = order;

		/*
		** Create a sort observer that are retained by the
		** sort.
		*/
		observer = new BasicSortObserver(true, distinct, sortTemplateRow, true);

		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
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
	  GemFireXDQueryObserver observer =
	      GemFireXDQueryObserverHolder.getInstance();
	  if(observer != null) {
	    observer.onSortResultSetOpen(this);
	  }
	  
		nextCalled = false;
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "SortResultSet already open");

		isOpen = true;
		/* NOTE: We need to save order to another variable
		 * in the constructor and reset it on every open.
		 * This is important because order can get reset in the
		 * guts of execution below.  Subsequent sorts could get
		 * the wrong result without this logic.
		 */
		order = savedOrder;

		sortResultRow = sortTemplateRow.getClone();

        source.openCore();

		/* If this is an in-order distinct then we do not need the sorter.
		 * (We filter out the duplicate rows ourselves.)
		 * We save a clone of the first row so that subsequent next()s
		 * do not overwrite the saved row.
		 */
		if (isInSortedOrder && distinct)
		{
			currSortedRow = getNextRowFromRS();
			if (currSortedRow != null)
			{
				currSortedRow = (ExecRow) currSortedRow.getClone();
			}
		}
		else
		{
			/*
			** Load up the sorter.
			*/
			scanController = loadSorter();
			sorted = true;
		}

	    //isOpen = true;
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	@Override
	public final void setMaxSortingLimit(long limit) {
	  this.maxSortLimit = limit;
	}

	/**
	 * Load up the sorter.  Feed it every row from the
	 * source scan.  When done, close
	 * the source scan and open the sort.  Return the sort
	 * scan controller.
	 *
	 * @exception StandardException thrown on failure.
	 *
	 * @return	the sort controller
 	 */
	private ScanController loadSorter()
		throws StandardException
	{
		//SortController 			sorter;
		long 					sortId;
		ExecRow 				sourceRow;
		ExecRow					inputRow;
		boolean					inOrder = (order.length == 0 || isInSortedOrder);
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;

		// find the language context and
        // Get the current transaction controller
		TransactionController tc = getTransactionController();
		sortId = tc.createSort((Properties)null, 
						sortTemplateRow,
						order,
						observer,
						inOrder,
						inputRowCountEstimate, // est rows
					 	maxRowSize,			// est rowsize
					 	maxSortLimit // number of rows to fetch
						);
		sorter = tc.openSort(sortId);
		genericSortId = sortId;
		dropGenericSort = true;
	  /* The sorter is responsible for doing the cloning */
		while ((inputRow = getNextRowFromRS()) != null) 
		{
			 sorter.insert(inputRow);      
		}
		source.close(false);
		sortProperties = sorter.getSortInfo().getAllSortInfo(sortProperties);
		sorter.completedInserts();

		return tc.openSortScan(sortId, activation.getResultSetHoldability());
	}


	/**
	 * Return the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
		if (!isOpen)
		{
			return null;
		}

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		// In order distinct
		if (isInSortedOrder && distinct)
		{
			// No rows, no work to do
			if (currSortedRow == null)
			{
				if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
				return null;
			}

			/* If this is the 1st next, then simply return the 1st row
			 * (which we got on the open()).
			 */
			if (! nextCalled)
			{
				nextCalled = true;
				numColumns = currSortedRow.getRowArray().length;
				if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
				rowsReturned++;
				setCurrentRow(currSortedRow);
				return currSortedRow;
			}

		    ExecRow sortResult = getNextRowFromRS();

			/* Drain and throw away rows until we find a new distinct row. */
			while (sortResult != null)
			{
				/* We found a new row.  Update the current row and return this one. */
				if (! filterRow(currSortedRow, sortResult))
				{
					/* Save a clone of the new row so that it doesn't get overwritten */
					currSortedRow = (ExecRow) sortResult.getClone();
					setCurrentRow(currSortedRow);
					if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
					rowsReturned++;
					return currSortedRow;
				}

				// Get the next row
				sortResult = getNextRowFromRS();
			}

			// We've drained the source, so no more rows to return
			currSortedRow = null;
			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
			return null;
		}
		else
		{
		    ExecRow sortResult = getNextRowFromRS();
      
			if (sortResult != null)
			{
				setCurrentRow(sortResult);
				rowsReturned++;
			}
			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		    return sortResult;
		}
	}

	/**
	 * Filter out the new row if it has the same contents as
	 * the current row.  (This allows us to process in-order
	 * distincts without a sorter.)
	 *
	 * @param currRow	The current row.
	 * @param newRow	The new row.
	 *
	 * @return	Whether or not to filter out the new row.
	 *
	 * @exception StandardException thrown on failure to get row location
	 */
	private boolean filterRow(ExecRow currRow, ExecRow newRow)
		throws StandardException
	{
		for (int index = 1; index <= numColumns; index++)
		{
			DataValueDescriptor currOrderable = currRow.getColumn(index);
			DataValueDescriptor newOrderable = newRow.getColumn(index);
			if (! (currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return false;
			}
		}
		return true;
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

			sortResultRow = null;
			sorter = null;
			closeSource();

			if (dropGenericSort)
			{
				getTransactionController().dropSort(genericSortId);
				dropGenericSort = false;
			}
			super.close(cleanupOnError);
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SortResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);

		isOpen = false;
	}

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
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
			return	time - originalSource.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
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
	 * the last fetch done. If the cursor is closed,
	 * a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		if (! isOpen) return null;

		// REVISIT: could we reuse the same rowlocation object
		// across several calls?
		RowLocation rl;
		rl = scanController.newRowLocationTemplate();
//              GemStone changes Begin
		rl = scanController.fetchLocation(rl);
//              GemStone changes End
		return rl;
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

		/*
			DISTINCT assumes the currentRow is good, since it
			is the only one with access to its sort scan result
		 */
		return currentRow;
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// SCAN ABSTRACTION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////
	/**
	 * Get the next output row for processing
	 */
	private ExecRow getNextRowFromRS()
		throws StandardException
	{
		return (scanController == null) ?
			getRowFromResultSet() :
			getRowFromSorter();
	}

	/**
	 * Get a row from the input result set.  
	 */	
	private ExecRow getRowFromResultSet()
		throws StandardException
	{
		ExecRow				sourceRow;
    this.source.releasePreviousByteSource();
		if ((sourceRow = source.getNextRowCore()) != null)
		{
			rowsInput++;
		}

		return sourceRow;
	}


	/**
	 * Get a row from the sorter.  Side effects:
	 * sets currentRow.
	 */
	private ExecRow getRowFromSorter()
		throws StandardException
	{
	  //GemStone changes BEGIN
	  /* this check happens only during table scan in derby
	   * but we want to check this more often because memory
	   * threshold is now added via BaseActivation. 
	   */
          checkCancellationFlag();
          //GemStone changes END
	  
		ExecRow			inputRow = null;	
		
		if (scanController.next())
		{
			// REMIND: HACKALERT we are assuming that result will
			// point to what sortResult is manipulating when
			// we complete the fetch.
			//currentRow = sortResultRow;

			//inputRow = sortResultRow;

// GemStone changes BEGIN
			inputRow = currentRow = sortResultRow = scanController
			    .fetchRow(sortResultRow);
			/*
			if (scanController instanceof SortBufferScan) {
                          ((SortBufferScan)scanController).setCurrentRakInfoInInputArray(inputRow);
                        }
                        */
// GemStone changes END
		}
		return inputRow;
	}

	/**
	 * Close the source of whatever we have been scanning.
	 */
	private void closeSource() throws StandardException
	{
		if (scanController == null)
		{
			/*
			** NOTE: do not null out source, we
			** may be opened again, in which case
			** we will open source again.
			*/
			source.close(false);
		}
		else
		{
			scanController.close();
			scanController = null;
		}
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    this.scanController.upgradeCurrentRowLocationLockToWrite();
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    if(localTXState != null) {
      this.scanController.releaseCurrentRowLocationReadLock();
    }    
      
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    if (source != null) {
      visitor.setNumberOfChildren(1);
    }
    else {
      visitor.setNumberOfChildren(0);
    }

    visitor.visit(this);

    if (source != null) {
      source.accept(visitor);
    }
  }
  
  @Override
  public boolean isReplicateIfSetOpSupported() throws StandardException {
    SanityManager.ASSERT(source instanceof BasicNoPutResultSetImpl, 
    "Member 'source' is expected to be of type BasicNoPutResultSetImpl");
    return ((BasicNoPutResultSetImpl)source).isReplicateIfSetOpSupported();
  }
  
  @Override
  public void resetStatistics() {
    rowsInput = 0;
    rowsReturned = 0;
    super.resetStatistics();
    source.resetStatistics();
  }
  
  public NoPutResultSet getSource() {
    return this.source;
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_GROUP);
    
    if(this.source != null) {
      this.source.buildQueryPlan(builder, context.pushContext());
    }
    
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
