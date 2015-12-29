/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GroupedAggregateResultSet

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
import java.util.TreeSet;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow;
import com.pivotal.gemfirexd.internal.iapi.error.SQLWarningFactory;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableArrayHolder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.impl.store.access.sort.SortBufferScan;

/**
 * This ResultSet evaluates grouped, non distinct aggregates.
 * It will scan the entire source result set and calculate
 * the grouped aggregates when scanning the source during the 
 * first call to next().
 *
 */
public class GroupedAggregateResultSet extends GenericAggregateResultSet
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsInput;
	public int rowsReturned;

    // set in constructor and not altered during
    // life of object.
	private ColumnOrdering[] order;
	private ExecRow sortTemplateRow;
	public	boolean	hasDistinctAggregate;	// true if distinct aggregate
	public	boolean isInSortedOrder;				// true if source results in sorted order
	private int maxRowSize;

	// set in open and not modified thereafter
    private ScanController scanController;

	// Cache ExecIndexRow
	//private ExecIndexRow sourceExecIndexRow;

	private ExecRow sortResultRow;

	// In order group bys
	private ExecRow currSortedRow;
// GemStone changes BEGIN
	// now directly using ExecAggregators here
	private ExecAggregator[] aggregators;
	// optimizing sameGroupingValues() by getting this flag directly
	// from underlying store if possible (only for sorted indexes)
	private boolean supportsMoveToNextKey;
	private boolean byteArrayStore;
	/* (original code)
	private boolean nextCalled;
	*/
	private int currentGroupID;

	/**
	 * Set by RowCountResultSet in case of FETCH FIRST/NEXT clause.
	 * Allows the sorter to be more intelligent and keep discarding higher
	 * elements where possible during the sort.
	 */
	private long maxSortLimit;
// GemStone changes END

	// used to track and close sorts
	private long distinctAggSortId;
	private boolean dropDistinctAggSort;
	private long genericSortId;
	private boolean dropGenericSort;
	private TransactionController tc;

	// RTS
	public Properties sortProperties = new Properties();

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine.  
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
    GroupedAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost) throws StandardException 
	{
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost
		    , false /* GemStonAddition */);
		this.isInSortedOrder = isInSortedOrder;
		//sortTemplateRow = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		sortTemplateRow = (ExecRow)rowAllocator.invoke(activation);
		order = (ColumnOrdering[])
					((FormatableArrayHolder)
						(a.getSavedObject(orderingItem)))
					.getArray(ColumnOrdering.class);
// GemStone changes BEGIN
		this.aggregators = getExecAggregators(aggInfoList, false, lcc);
// GemStone changes END

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
    //GemStone changes BEGIN
	public void	basicOpenCore(boolean isReopen) throws StandardException 
	{
    //GemStone changes END	  
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "GroupedAggregateResultSet already open");

// GemStone changes BEGIN
		isOpen = true;
		// don't clone for non-sorting case
		if (isInSortedOrder) {
		  sortResultRow = sortTemplateRow;
		}
		else {
		  sortResultRow = sortTemplateRow.getClone();
		}
		
		if(this.aggregators != null) {
		  for(ExecAggregator aggregator:this.aggregators) {
		    SystemAggregator sa = (SystemAggregator)aggregator;
		    if(sa != null) {
		      sa.clear();
		    }
		    
		  }
		}
		/* (original code)
		sortResultRow = sortTemplateRow.getClone();
		sortResultRow = getExecutionFactory().getIndexableRow(sortTemplateRow.getClone());
		sourceExecIndexRow = getExecutionFactory().getIndexableRow(sortTemplateRow.getClone());
		*/
// GemStone changes END
        if(isReopen) {
          source.reopenCore();
        }else {
          source.openCore();
        }
       // GemStone changes END
		/* If this is an in-order group by then we do not need the sorter.
		 * (We can do the aggregation ourselves.)
		 * We save a clone of the first row so that subsequent next()s
		 * do not overwrite the saved row.
		 */
		if (isInSortedOrder)
		{
// GemStone changes BEGIN
			supportsMoveToNextKey = source.supportsMoveToNextKey();
			this.currSortedRow = getRowFromResultSet();
			if (supportsMoveToNextKey) {
			  // at this point the bottom source may have moved
			  // to next key due to filtering etc. at higher layers
			  // so invoke it once to reset it
			  this.currentGroupID = this.source.getScanKeyGroupID();
			}
			else {
			  this.byteArrayStore =
			      currSortedRow instanceof AbstractCompactExecRow;
			}
			/* (original code)
			currSortedRow = getNextRowFromRS();
			*/
// GemStone changes END
			if (currSortedRow != null)
			{
// GemStone changes BEGIN
			 
				accumulate(currSortedRow);
			  
				/* (original code)
				currSortedRow = currSortedRow.getClone();
				initializeVectorAggregation(currSortedRow);
				*/
// GemStone changes END
			}
		}
		else
		{
			/*
			** Load up the sorter
			*/
			scanController = loadSorter();
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
	 * source scan.  If we have a vector aggregate, initialize
	 * the aggregator for each source row.  When done, close
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
		SortController 			sorter;
		long 					sortId;
		ExecRow 				sourceRow;
		ExecRow 				inputRow;
		int						inputRowCountEstimate = (int) optimizerEstimatedRowCount;
		boolean					inOrder = isInSortedOrder;

		tc = getTransactionController();

		ColumnOrdering[] currentOrdering = order;
	
// GemStone changes BEGIN
	 
		if (this.aggregates == null) {
		  this.aggregates = getSortAggregators(this.aggInfoList, false,
		      this.lcc, this.source);
		  final int numAggregates = this.aggregates.length;
		  for (int index = 0; index < numAggregates; index++) {
		    this.aggregates[index].cachedAggregator =
		        this.aggregators[index];
		  }
		}
	
// GemStone changes END
		
	
		/*
		** Do we have any distinct aggregates?  If so, we'll need
		** a separate sort.  We use all of the sorting columns and
		** drop the aggregation on the distinct column.  Then
		** we'll feed this into the sorter again w/o the distinct
		** column in the ordering list.
		*/
		if (aggInfoList.hasDistinct())
		{
			hasDistinctAggregate = true;
			
			GenericAggregator[] aggsNoDistinct = getSortAggregators(aggInfoList, true,
						activation.getLanguageConnectionContext(), source);
			SortObserver sortObserver = new AggregateSortObserver(true, aggsNoDistinct, aggregates,
																  sortTemplateRow);

			sortId = tc.createSort((Properties)null, 
					sortTemplateRow,
					order,
					sortObserver,
					false,			// not in order
					inputRowCountEstimate,				// est rows, -1 means no idea	
					maxRowSize,		// est rowsize
					0 // number of rows to fetch in not known at this point
					);
			sorter = tc.openSort(sortId);
			distinctAggSortId = sortId;
			dropDistinctAggSort = true;
			boolean isOffHeapEnabled = GemFireXDUtils.isOffHeapEnabled();
			
			while ((sourceRow = source.getNextRowCore())!=null) 
			{
			  //GemStone changes begin
			  /* The sorter is responsible for doing the cloning */
	      boolean rowInserted = false;
	     
	        rowInserted =sorter.insert(sourceRow);
	        rowsInput++;
	        if(isOffHeapEnabled) {
	          this.source.releasePreviousByteSource();
	        }
        
	    //GemStone changes end
			}
     
			/*
			** End the sort and open up the result set
			*/
			//GemStone changes BEGIN
			//See Bug # 47727 which results in recreation of backing HashTable
			//source.close();
			//GemStone changes END
			sortProperties = sorter.getSortInfo().getAllSortInfo(sortProperties);
			sorter.completedInserts();

			scanController = 
                tc.openSortScan(sortId, activation.getResultSetHoldability());
			
			/*
			** Aggs are initialized and input rows
			** are in order.  All we have to do is
			** another sort to remove (merge) the 
			** duplicates in the distinct column
			*/	
			inOrder = true;
			inputRowCountEstimate = rowsInput;
	
			/*
			** Drop the last column from the ordering.  The
			** last column is the distinct column.  Don't
			** pay any attention to the fact that the ordering
			** object's name happens to correspond to a techo
			** band from the 80's.
			**
			** If there aren't any ordering columns other
			** than the distinct (i.e. for scalar distincts)
			** just skip the 2nd sort altogether -- we'll
			** do the aggregate merge ourselves rather than
			** force a 2nd sort.
			*/
			if (order.length == 1)
			{
				return scanController;
			}

			ColumnOrdering[] newOrder = new ColumnOrdering[order.length - 1];
			System.arraycopy(order, 0, newOrder, 0, order.length - 1);
			currentOrdering = newOrder;
		}

		SortObserver sortObserver = new AggregateSortObserver(true, aggregates, aggregates,
															  sortTemplateRow);

		sortId = tc.createSort((Properties)null, 
						sortTemplateRow,
						currentOrdering,
						sortObserver,
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
		//GemStone changes BEGIN
                //See Bug # 47727 which results in recreation of backing HashTable
                //source.close();
                //GemStone changes END
		
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

		// In order group by
		if (isInSortedOrder)
		{
			// No rows, no work to do
			if (currSortedRow == null)
			{
				if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
				return null;
			}
      
// GemStone changes BEGIN
			// copy over the grouping columns from previous row
			// before calling the next on underlying store since
			// it may change the row
			// we need to copy the columns in every case since
			// higher layer may require those columns too
			// TODO: PERF: can avoid copying the ordering columns if
			// supportsMoveToNextKey is true and result row does not
			// contain those ordering columns
			copyOrderingColumns(this.sortResultRow, currSortedRow);
			// set the RAK, if any, for outer joins
			TreeSet<RegionAndKey> raks = this.currSortedRow
			    .getAllRegionAndKeyInfo();
			// TODO: PERF: we really need to rip off this RAK TreeSet
			// which is very heavy and is replicated at every layer
			// for every row; this and of course the non-incremental
			// HashMap based approach to consume results for ojoins
			if (raks != null && raks.size() > 0) {
			  this.sortResultRow.addAllKeys(raks);
			}
			this.source.releasePreviousByteSource();
			ExecRow nextRow = getRowFromResultSet();			
		    /* (original code)
		    ExecRow nextRow = getNextRowFromRS();
		    */
// GemStone changes END

			/* Drain and merge rows until we find new distinct values for the grouping columns. */
			while (nextRow != null)
			{
				/* We found a new set of values for the grouping columns.  
				 * Update the current row and return this group. 
				 */
				if (! sameGroupingValues(/* currSortedRow, */ nextRow))
				{
// GemStone changes BEGIN
					ExecRow result = sortResultRow;
					// now cloning only the required columns
					// if cannot get from underlying store
					finishOutput(result);
					// also accumulate the current row for next round
					accumulate(nextRow);
					currSortedRow = nextRow;
					if (statisticsTimingOn) {
					  nextTime += getElapsedNanos(beginTime);
					}
					rowsReturned++;
					//this.source.releasePreviousByteSource();
					return result;
					/* (original code)
					ExecRow result = currSortedRow;

					/* Save a clone of the new row so that it doesn't get overwritten *
					currSortedRow = nextRow.getClone();
					initializeVectorAggregation(currSortedRow);

					if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
					rowsReturned++;
					return finishAggregation(result);
					*/
// GemStone changes END
				}
				else
				{
					/* Same group - initialize the new row and then merge the aggregates */
// GemStone changes BEGIN
					accumulate(nextRow);
					this.source.releasePreviousByteSource();
					/* (original code)
					initializeVectorAggregation(nextRow);
					mergeVectorAggregates(nextRow, currSortedRow);
					*/
// GemStone changes END
				}

				// Get the next row
				nextRow = getRowFromResultSet();// GemStone change getNextRowFromRS();
			}

			// We've drained the source, so no more rows to return
// GemStone changes BEGIN
			ExecRow result = sortResultRow;
			sortResultRow = null;
			currSortedRow = null;
			finishOutput(result);
			if (statisticsTimingOn) {
			  nextTime += getElapsedNanos(beginTime);
			}
			rowsReturned++;
			return result;
                        /* (original code)
			ExecRow result = currSortedRow;
			currSortedRow = null;
			return finishAggregation(result);
			*/
// GemStone changes END
		}
		else
		{
	    ExecRow sortResult = null;

	    if ((sortResult = getNextRowFromRS()) != null)
			{
				setCurrentRow(sortResult);
			}

			/*
			** Only finish the aggregation
			** if we have a return row.  We don't generate
			** a row on a vector aggregate unless there was
			** a group.
			*/
			if (sortResult != null)
			{
				sortResult = finishAggregation(sortResult);
				currentRow = sortResult;
			}

			if (sortResult != null)
			{
				rowsReturned++;
			}

			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		    return sortResult;
		}
	}

	/**
	 * Return whether or not the new row has the same values for the 
	 * grouping columns as the current row.  (This allows us to process in-order
	 * group bys without a sorter.)
	 *
	 * @param currRow	The current row.
	 * @param newRow	The new row.
	 *
	 * @return	Whether or not to filter out the new row has the same values for the 
	 *			grouping columns as the current row.
	 *
	 * @exception StandardException thrown on failure to get row location
	 */
	private boolean sameGroupingValues(/* GemStone change ExecRow currRow ,*/ ExecRow newRow)
		throws StandardException
	{
// GemStone changes BEGIN
	  if (this.supportsMoveToNextKey) {
	    int groupID =  this.source.getScanKeyGroupID();
	    if(this.currentGroupID == groupID) {
	    	return true;
	    }else {
	    	this.currentGroupID = groupID;
	    	return false;
	    }
	  }
	  // current column row is a ValueRow
	  // TODO: PERF: see if CompactExecRow can be used when possible
	  for (ColumnOrdering colOrder : this.order) {
	    if (this.sortResultRow.compare(newRow, colOrder.getColumnId() + 1,
	        true) != 0) {
	      return false;
	    }
	  }
	  return true;
	  /* (original code)
		for (int index = 0; index < order.length; index++)
		{
			DataValueDescriptor currOrderable = currRow.getColumn(order[index].getColumnId() + 1);
			DataValueDescriptor newOrderable = newRow.getColumn(order[index].getColumnId() + 1);
			if (! (currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return false;
			}
		}
		return true;
	  */
// GemStone changes END
	}
	
//GemStone changes BEGIN
	@Override
	public void reopenCore() throws StandardException
        {
                close(false /* close source resultset*/, false);
                this.basicOpenCore(true /*for reopen*/);     
        }
	
	@Override
        public void close(final boolean cleanupOnError) throws StandardException
        {
                close(true /* close source resultset*/, cleanupOnError);
                
        }
	
	@Override
        public void openCore() throws StandardException
        {
            	  GemFireXDQueryObserver observer =
                    GemFireXDQueryObserverHolder.getInstance();
                if(observer != null) {
                  observer.onGroupedAggregateResultSetOpen(this);
                }
                this.basicOpenCore( false /* is Reopen*/);
                
        }
	
//GemStone changes END

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
//GemStone changes BEGIN
	//public void    close() throws StandardException
	public void	close(boolean closeSourceResultSet, final boolean cleanupOnError) throws StandardException
	{
 //GemStone changes end
	  beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if ( isOpen )
	    {
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

			sortResultRow = null;
			//sourceExecIndexRow = null;
			
			closeSource(closeSourceResultSet, cleanupOnError);
			

			if (dropDistinctAggSort)
			{
				tc.dropSort(distinctAggSortId);
				dropDistinctAggSort = false;
			}

			if (dropGenericSort)
			{
				tc.dropSort(genericSortId);
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
			/*(original code) return	totTime - originalSource.getTimeSpent(ENTIRE_RESULTSET_TREE);*/
                        return  time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
		}
		else
		{
                    // GemStone changes BEGIN
                    return timeType == ResultSet.ALL ? (time - constructorTime) : time;
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

// GemStone changes BEGIN
		if (this.scanController == null) {
		  if (this.source instanceof CursorResultSet) {
		    return ((CursorResultSet)this.source).getRowLocation();
		  }
		  else {
		    return null;
		  }
		}
		else {
		  // sorter cannot return current row location
		  return null;
		}
		/* (original code)
		// REVISIT: could we reuse the same rowlocation object
		// across several calls?
		RowLocation rl;
		rl = scanController.newRowLocationTemplate();
		rl = scanController.fetchLocation(rl);
		return rl;
		*/
// GemStone changes END
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
		ExecRow					sourceRow;
		//ExecIndexRow			inputRow = null;	
		 this.source.releasePreviousByteSource();
		if ((sourceRow = source.getNextRowCore()) != null)
		{
			rowsInput++;
			//sourceExecIndexRow.execRowToExecIndexRow(sourceRow);
			//inputRow = sourceExecIndexRow;
		}

		//return inputRow;
		return sourceRow;
	}


	/**
	 * Get a row from the sorter.  Side effects:
	 * sets currentRow.
	 */
	private ExecRow getRowFromSorter()
		throws StandardException
	{
// GemStone changes BEGIN
	  if (scanController.next()) {
	    // REMIND: HACKALERT we are assuming that result will
	    // point to what sortResult is manipulating when
	    // we complete the fetch.
	    currentRow = sortResultRow = scanController.fetchRow(sortResultRow);
	    /*
	    if (scanController instanceof SortBufferScan) {
	      ((SortBufferScan)scanController).setCurrentRakInfoInInputArray(
	          currentRow);
	    }
	    */
	    return currentRow;
	  }
	  else {
	    currentRow = null;
	    return null;
	  }
          /* (original code)
		ExecIndexRow			inputRow = null;	
		
		if (scanController.next())
		{
			// REMIND: HACKALERT we are assuming that result will
			// point to what sortResult is manipulating when
			// we complete the fetch.
			currentRow = sortResultRow;

			inputRow = getExecutionFactory().getIndexableRow(currentRow);

			scanController.fetch(inputRow);
		}
		return inputRow;
	  */
// GemStone changes END
	}

	//GemStone changes BEGIN
	/**
	 * Close the source of whatever we have been scanning.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	closeSource(boolean closeSourceResultSet, final boolean cleanupOnError) throws StandardException
	{
		if (scanController == null)
		{
			/*
			** NOTE: do not null out source, we
			** may be opened again, in which case
			** we will open source again.
			*/
		      if(closeSourceResultSet) {
			source.close(cleanupOnError) ;
		      }
		}
		else
		{
			scanController.close();
			scanController = null;
			if(closeSourceResultSet) {
			  this.source.close(cleanupOnError);
			}
		}
	}
	//GemStone changes END

	///////////////////////////////////////////////////////////////////////////////
	//
	// AGGREGATION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////
// GemStone changes BEGIN
	/* (original code)
	/**
	 * Run the aggregator initialization method for
	 * each aggregator in the row.  Accumulate the
	 * input column.  WARNING: initializiation performs
	 * accumulation -- no need to accumulate a row
	 * that has been passed to initialization.
	 *
	 * @param	row	the row to initialize
	 *
	 * @exception	standard Derby exception
	 *
	private void initializeVectorAggregation(ExecRow row)
		throws StandardException
	{
		int size = aggregates.length;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row != null, 
					"Null row passed to initializeVectorAggregation");
		}

		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];

			// initialize the aggregator
			currAggregate.initialize(row);

			// get the first value, accumulate it into itself
			currAggregate.accumulate(row, row);
		}
	}

	/**
	 * Run the aggregator merge method for
	 * each aggregator in the row.  
	 *
	 * @param	newRow	the row to merge
	 * @param	currRow the row to merge into
	 *
	 * @exception	standard Derby exception
	 *
	private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow)
		throws StandardException
	{
		for (int i = 0; i < aggregates.length; i++)
		{
			GenericAggregator currAggregate = aggregates[i];

			// merge the aggregator
			currAggregate.merge(newRow, currRow);
		}
	}
	*/

  protected final void accumulate(final ExecRow inputRow)
      throws StandardException {
    for (ExecAggregator agg : this.aggregators) {
      agg.accumulate(inputRow);
    }
  }

  protected final void finishOutput(final ExecRow outputRow)
      throws StandardException {
    setCurrentRow(outputRow);
    boolean eliminatedNulls = false;
    for (ExecAggregator agg : this.aggregators) {
      if (agg.finish(outputRow, this.byteArrayStore)) {
        eliminatedNulls = true;
      }
    }
    if (eliminatedNulls) {
      this.activation.addNullEliminatedWarning();
    }
  }

  private void copyOrderingColumns(final ExecRow resultRow,
      final ExecRow currRow) throws StandardException {
    for (ColumnOrdering colOrder : this.order) {
      final int colIndex = colOrder.getColumnId();
      resultRow.setValue(colIndex, currRow.getColumn(colIndex + 1));
    }
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    if (this.scanController == null) {
      this.source.updateRowLocationPostRead();
    }
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    // nothing here; we don't want to release the locks on aggregates for
    // repeatable reads and in any case there might be other group by columns
    //GemFireXDUtils.releaseByteSourceFromExecRow(this.currentRow);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsMoveToNextKey() {
    return this.supportsMoveToNextKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScanKeyGroupID() {
    if (this.scanController == null) {
      return this.source.getScanKeyGroupID();
    }
    else {
      throw new UnsupportedOperationException("not expected to be invoked");
    }
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.setNumberOfChildren(1);
    visitor.visit(this);
    source.accept(visitor);
  }
  
  @Override
  public void resetStatistics() {
    rowsInput = 0;
    rowsReturned = 0;
    super.resetStatistics();
  }

  public final long estimateMemoryUsage() throws StandardException {
    final ExecRow templateRow = this.sortTemplateRow;
    if (templateRow != null) {
      long sz = templateRow.estimateRowSize();
      final ExecRow resultRow = this.sortResultRow;
      if (resultRow != templateRow && resultRow != null) {
        sz += resultRow.estimateRowSize();
      }
      return sz;
    }
    else {
      return 1;
    }
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

// GemStone changes END
}
