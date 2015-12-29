/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.LastIndexKeyResultSet

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
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Return the last key in an index.  Used to perform
 * max().
 *
 */
class LastIndexKeyResultSet extends ScanResultSet
{
	// set in constructor and not altered during
	// life of object.
	protected long conglomId;
	protected GeneratedMethod resultRowAllocator;
	protected GeneratedMethod startKeyGetter;
	protected int startSearchOperator;
	protected GeneratedMethod stopKeyGetter;
	protected int stopSearchOperator;
	protected Qualifier[][] qualifiers;
	public String tableName;
	public String userSuppliedOptimizerOverrides;
	public String indexName;
	protected boolean runTimeStatisticsOn;

	// Run time statistics
	public String stopPositionString;
	public boolean coarserLock;
	public boolean returnedRow;

	/**
	 * A last index key result set returns the last row from
	 * the index in question.  It is used as an ajunct to max().
	 *
	 * @param activation 		the activation for this result set,
	 *		which provides the context for the row allocation operation.
	 * @param resultSetNumber	The resultSetNumber for the ResultSet
	 * @param resultRowAllocator a reference to a method in the activation
	 * 						that creates a holder for the result row of the scan.  May
	 *						be a partial row.  <verbatim>
	 *		ExecRow rowAllocator() throws StandardException; </verbatim>
	 * @param conglomId 		the conglomerate of the table to be scanned.
	 * @param tableName			The full name of the table
	 * @param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
	 * @param indexName			The name of the index, if one used to access table.
	 * @param colRefItem		An saved item for a bitSet of columns that
	 *							are referenced in the underlying table.  -1 if
	 *							no item.
	 * @param lockMode			The lock granularity to use (see
	 *							TransactionController in access)
	 * @param tableLocked		Whether or not the table is marked as using table locking
	 *							(in sys.systables)
	 * @param isolationLevel	Isolation level (specified or not) to use on scans
	 * @param optimizerEstimatedRowCount	Estimated total # of rows by
	 * 										optimizer
	 * @param optimizerEstimatedCost		Estimated total cost by optimizer
	 *
	 * @exception StandardException thrown when unable to create the
	 * 				result set
	 */
	public LastIndexKeyResultSet
	(
		Activation activation, 
		int	resultSetNumber,
		GeneratedMethod resultRowAllocator, 
		long conglomId, 
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost
	) throws StandardException
	{
		super(activation,
				resultSetNumber,
				resultRowAllocator,
				lockMode, tableLocked, isolationLevel,
                colRefItem,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		this.conglomId = conglomId;

		if (SanityManager.DEBUG) 
		{
			SanityManager.ASSERT( activation!=null, "this scan must get activation context");
			SanityManager.ASSERT( resultRowAllocator!= null, "this scan must get row allocator");
		}

		this.resultRowAllocator = resultRowAllocator;
		this.tableName = tableName;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = indexName;

		runTimeStatisticsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();

		/*
		** If scan tracing is turned on, print information about this
		** LastIndexKeyResultSet when it is first opened.  
		*/
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("ScanTrace"))
			{
				//traceScanParameters();
			}
		}

		activation.informOfRowCount(this, 1);
		
		recordConstructorTime();
    }

	/////////////////////////////////////////////////////
	// 
	// ResultSet interface (leftover from NoPutResultSet)
	// 
	/////////////////////////////////////////////////////

	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	boolean canGetInstantaneousLocks() {
		return true;
	}

	/**
	* open a scan on the table. scan parameters are evaluated
	* at each open, so there is probably some way of altering
	* their values...
	 *
	 * @exception StandardException thrown on failure to open
	*/
	public void	openCore() throws StandardException
	{
		ExecRow candidateCopy = candidate.getClone();

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
		{
		    SanityManager.ASSERT(!isOpen, "LastIndexKeyResultSet already open");
		}

		isOpen = true;
		TransactionController tc = activation.getTransactionController();

		initIsolationLevel();

		/*
		** Grab the last row.  Note that if there are deletes
		** left lying around and no real row to return, then
		** the row array gets set even though the scan doesn't
		** return a row, so be careful to handle this correctly.
		*/
		if (tc.fetchMaxOnBtree(
					conglomId,  // conglomerate to open
					0, 			// open mode
					lockMode,
					isolationLevel,
					accessedCols,
					candidateCopy.getRowArray()))
		{
			setCurrentRow(getCompactRow(candidateCopy, accessedCols, true));
		}
		else
		{
		    clearCurrentRow();
		}
			
		numOpens++;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
	 * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
		if (returnedRow || !isOpen)
		{
		    clearCurrentRow();
		} 
		else
		{
			returnedRow = true;
		}
		return currentRow;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 * @exception StandardException thrown on failure to close
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (isOpen)
	    {
			returnedRow = false;
		    clearCurrentRow();

			super.close(cleanupOnError);
			isOpen = false;
	    }
		else
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.DEBUG("CloseRepeatInfo","Close of LastIndexKeyResultSet repeated");
			}
		}

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

		/* RESOLVE - subtract out store time later, when available */
		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime;
		}
		else
		{
                        // GemStone changes BEGIN
                        return totTime - constructorTime;
                        /*(original code) return totTime; */
                        // GemStone changes END
		}
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
	public ExecRow getCurrentRow() throws StandardException 
	{
		return currentRow;
	}

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

	/**
	 * Print the parameters that constructed this result set to the
	 * trace stream.
	 */
/*
	private final void traceScanParameters()
	{
		if (SanityManager.DEBUG)
		{
			HeaderPrintWriter traceStream = SanityManager.GET_DEBUG_STREAM();

			traceStream.println("");
			traceStream.println("LastIndexKeyResultSet number " +
								resultSetNumber +
								" parameters:");

			traceStream.println("");
			traceStream.println("\tTable name: " + tableName);
			if (indexName != null)
			{
				traceStream.println("\tIndex name: " + indexName);
			}
			traceStream.println("");
		}
	}
*/
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    
    super.buildQueryPlan(builder, context);
    
    if (tableName != null)
      PlanUtils.xmlAttribute(builder, "tableName", this.tableName);
    
    if (indexName != null)
      PlanUtils.xmlAttribute(builder, "indexName", this.indexName);
    
    if (userSuppliedOptimizerOverrides != null)
      PlanUtils.xmlAttribute(builder, "userSuppliedOptimizerOverrides", this.userSuppliedOptimizerOverrides);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_LASTINDEXKEYSCAN);
    
    PlanUtils.xmlCloseTag(builder, context, this);
    return builder;
  }

}
