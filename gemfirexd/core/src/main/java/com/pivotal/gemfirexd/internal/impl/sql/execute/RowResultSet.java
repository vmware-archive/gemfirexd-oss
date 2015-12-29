/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RowResultSet

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
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapResourceHolder;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes a constant row value and returns it as
 * a result set.
 * <p>
 * This class actually probably never underlies a select statement,
 * but in case it might and because it has the same behavior as the
 * ones that do, we have it implement CursorResultSet and give
 * reasonable answers.
 *
 */
class RowResultSet extends NoPutResultSetImpl
	implements CursorResultSet,OffHeapResourceHolder {

	/* Run time statistics variables */
	public int rowsReturned;

	private boolean canCacheRow;
	private boolean next;
	private GeneratedMethod row;
	private ExecRow		cachedRow;
	   //
    // class interface
    //
    RowResultSet
	(
		Activation 	activation, 
		GeneratedMethod row, 
		boolean 		canCacheRow,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost
	)
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

        this.row = row;
		this.canCacheRow = canCacheRow;
		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
    }

	/* This constructor takes in a constant row value, as the cache row.  See the
	 * usage in beetle 4373 for materializing subquery.
	 */
    RowResultSet
	(
		Activation 		activation, 
		ExecRow 		constantRow, 
		boolean 		canCacheRow,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost
	)
 {
    super(activation, resultSetNumber, optimizerEstimatedRowCount,
        optimizerEstimatedCost);

    beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    this.cachedRow = constantRow;
    this.canCacheRow = canCacheRow;
    recordConstructorTime();
    if (constantRow != null) {
      Object byteSource = constantRow.getByteSource();
      if (byteSource != null) {
        Class<?> bsClass = byteSource.getClass();
        if (!(bsClass == byte[].class || bsClass == byte[][].class)) {
          this.registerWithGemFireTransaction(this);
        }
      }
    }
    
    // GemStone changes BEGIN
    printResultSetHierarchy();
    // GemStone changes END
  }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * Sets state to 'open'.
	 *
	 * @exception StandardException thrown if activation closed.
     */
	public void	openCore() throws StandardException 
	{
	   	next = false;
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    isOpen = true;
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
     * If open and not returned yet, returns the row
     * after plugging the parameters into the expressions.
	 *
	 * @exception StandardException thrown on failure.
     */
	public ExecRow	getNextRowCore() throws StandardException {

		currentRow = null;
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (isOpen) 
		{
			if (!next) 
			{
	            next = true;
				if (cachedRow != null)
                {
                    currentRow = cachedRow;
                }
                else if (row != null)
                {
                    currentRow = (ExecRow) row.invoke(activation);
                    if (canCacheRow)
                    {
                        cachedRow = currentRow;
                    }
                }
				rowsReturned++;
			}
			setCurrentRow(currentRow);

			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
	    }
	    return currentRow;
	}

	/**
     * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#close
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (isOpen) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	    	next = false;

			super.close(cleanupOnError);
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of RowResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
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
		final long totTime = constructorTime + openTime + nextTime + closeTime;
		return totTime;
	}

	//
	// CursorResultSet interface
	//

	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void resetStatistics() {
    rowsReturned = 0;
    super.resetStatistics();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_ROW);
    
    PlanUtils.xmlCloseTag(builder, context, this);
    return builder;
    
  }
// GemStone changes END

  @Override
  public void release() {
    this.cachedRow.releaseByteSource();
  }

  @Override
  public void addByteSource(@Unretained OffHeapByteSource byteSource) {
  }

  @Override
  public void registerWithGemFireTransaction(OffHeapResourceHolder owner) {
    ((GemFireTransaction)this.activation.getLanguageConnectionContext().getTransactionExecute())
    .registerOffHeapResourceHolder(owner);
    
  }

  @Override
  public boolean optimizedForOffHeap() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void releaseByteSource(int positionFromEnd) {    
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "ResultSet Created: " + this.getClass().getSimpleName()
                + " with resultSetNumber=" + resultSetNumber);
      }
    }
  }
}
