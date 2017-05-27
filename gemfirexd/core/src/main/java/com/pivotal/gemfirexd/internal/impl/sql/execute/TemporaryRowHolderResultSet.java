/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.TemporaryRowHolderResultSet

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

import java.sql.Timestamp;
import java.util.ArrayList;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.Row;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TargetResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLLongint;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils.Context;


/**
 * A result set to scan temporary row holders.  Ultimately, this
 * may be returned to users, hence the extra junk from the ResultSet
 * interface.
 *
 */
public final class TemporaryRowHolderResultSet implements CursorResultSet, NoPutResultSet, Cloneable
{
	private ExecRow[] 				rowArray;
	private int						numRowsOut;
	private ScanController			scan;
	private TransactionController	tc;
	private boolean 				isOpen;
	private boolean 				finished;
	private ExecRow					currentRow;
	private boolean                 isAppendable = false;
	private long                    positionIndexConglomId;
	private boolean 				isVirtualMemHeap;
	private boolean 				currRowFromMem;
	private TemporaryRowHolderImpl	holder;

	// the following is used by position based scan, as well as virtual memory style heap
	ConglomerateController			heapCC;
	private RowLocation				baseRowLocation;

	/**
	 * Constructor
	 *
	 * @param tc the xact controller
	 * @param rowArray the row array
	 */
	TemporaryRowHolderResultSet
	(
		TransactionController		tc,
		ExecRow[]					rowArray,
		boolean						isVirtualMemHeap,
		TemporaryRowHolderImpl		holder
   	)
	{

		this(tc, rowArray, isVirtualMemHeap, false, 0, holder);


	}

	/**
	 * Constructor
	 *
	 * @param tc the xact controller
	 * @param rowArray the row array
	 * @param isAppendable true,if we can insert rows after this result is created
	 * @param positionIndexConglomId conglomId of the index which has order rows
	 *                               are inserted and their row location 
	 */
	TemporaryRowHolderResultSet
	(
		TransactionController		tc,
		ExecRow[]					rowArray,
		boolean						isVirtualMemHeap,
		boolean                     isAppendable,
		long                        positionIndexConglomId,
		TemporaryRowHolderImpl		holder
	) 
	{
		this.tc = tc;
		this.rowArray = rowArray;
		this.numRowsOut = 0;
		isOpen = false;
		finished = false;
		this.isVirtualMemHeap = isVirtualMemHeap;
		this.isAppendable = isAppendable;
		this.positionIndexConglomId = positionIndexConglomId;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rowArray != null, "rowArray is null");
			SanityManager.ASSERT(rowArray.length > 0, "rowArray has no elements, need at least one");
		}

		this.holder = holder;
	}

	/**
	 * Reset the exec row array and reinitialize
	 *
	 * @param rowArray the row array
	 */
	public void reset(ExecRow[]	rowArray)
	{
		this.rowArray = rowArray;
		this.numRowsOut = 0;
		isOpen = false;
		finished = false;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rowArray != null, "rowArray is null");
			SanityManager.ASSERT(rowArray.length > 0, "rowArray has no elements, need at least one");
		}
	}


	/**
	 * postion scan to start from after where we stopped earlier
	 */
	public void reStartScan(long currentConglomId, long pconglomId) throws  StandardException 
	{
		if(isAppendable)
		{
            if (SanityManager.DEBUG) {
// GemStone changes BEGIN
              if (currentConglomId != holder.getTemporaryConglomId())
// GemStone changes END
                SanityManager.ASSERT(currentConglomId == holder.getTemporaryConglomId(),
                        "currentConglomId(" + currentConglomId + 
                        ") == holder.getTemporaryConglomeateId (" + 
                        holder.getTemporaryConglomId() + ")");
            }
			positionIndexConglomId = pconglomId;
			setupPositionBasedScan(numRowsOut);
		}else
		{
			numRowsOut--;
		}
	}


	/**
	 * Whip up a new Temp ResultSet that has a single
	 * row, the current row of this result set.
	 * 
	 * @param activation the activation
	 * @param rs the result set 
	 * 
	 * @return a single row result set
	 *
	 * @exception StandardException on error
	 */
	static TemporaryRowHolderResultSet getNewRSOnCurrentRow
	(
		Activation				activation,
		CursorResultSet 		rs
	) throws StandardException
	{
		TemporaryRowHolderImpl singleRow =
			new TemporaryRowHolderImpl(activation, null);
		singleRow.insert(rs.getCurrentRow());
		return (TemporaryRowHolderResultSet) singleRow.getResultSet();
	}

	/////////////////////////////////////////////////////////
	//
	// NoPutResultSet
	// 
	/////////////////////////////////////////////////////////
	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 */
	public void markAsTopResultSet()
	{ }

	/**
	 * Open the scan and evaluate qualifiers and the like.
	 * For us, there are no qualifiers, this is really a
	 * noop.
	 */
	public void openCore() throws StandardException
	{
		this.numRowsOut = 0;
		isOpen = true;
		currentRow = null;

		if(isAppendable)
			setupPositionBasedScan(numRowsOut);
	}

	/**
	 * Reopen the scan.  Typically faster than open()/close()
	 *
	 * @exception StandardException on error
	 */
	public void reopenCore() throws StandardException
	{
		numRowsOut = 0;
		isOpen = true;
		currentRow = null;

		if(isAppendable)
		{
			setupPositionBasedScan(numRowsOut);
			return;
		}

		if (scan != null)
		{
			scan.reopenScan(
                (DataValueDescriptor[]) null,		// start key value
                0,						// start operator
                null,					// qualifier
                (DataValueDescriptor[]) null,		// stop key value
                // GemStone changes BEGIN
                0, null);						// stop operator
	        // GemStone changes END
		}
	}

	/**
	 * Get the next row.
	 *
	 * @return the next row, or null if none
	 *
	 * @exception StandardException on error
	 */
	public ExecRow getNextRowCore()
		throws StandardException
	{

		if (!isOpen)
		{
			return (ExecRow)null;
		}
			
		if(isAppendable)
		{
			return getNextAppendedRow() ;
		}

		if (isVirtualMemHeap && holder.lastArraySlot >= 0)
		{
			numRowsOut++;
			currentRow = rowArray[holder.lastArraySlot];
			currRowFromMem = true;
			return currentRow;
		}
		else if (numRowsOut++ <= holder.lastArraySlot)
		{
			currentRow = rowArray[numRowsOut-1];
			return currentRow;
		}

		if (holder.getTemporaryConglomId() == 0)
		{
			return (ExecRow)null;
		}
			
		/*
		** Advance in the temporary conglomerate
		*/
		if (scan == null)
		{
			scan = 
                tc.openScan(
                    holder.getTemporaryConglomId(),
                    false,					// hold
                    0, 		// open read only
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    (FormatableBitSet) null, 
                    (DataValueDescriptor[]) null,		// start key value
                    0,						// start operator
                    null,					// qualifier
                    (DataValueDescriptor[]) null,		// stop key value
                    0                                           // stop operator
                    // GemStone changes BEGIN
                    , null);
		    // GemStone changes END
		}
		else if (isVirtualMemHeap && holder.state == TemporaryRowHolderImpl.STATE_INSERT)
		{
			holder.state = TemporaryRowHolderImpl.STATE_DRAIN;
			scan.reopenScan(
                (DataValueDescriptor[]) null,		// start key value
                0,						// start operator
                null,					// qualifier
                (DataValueDescriptor[]) null,		// stop key value
                // GemStone changes BEGIN
                0, null);						// stop operator
		// GemStone changes END
		}

		if (scan.next())
		{
			currentRow = rowArray[0].getNewNullRow();
//Gemstone changes BEGIN			
		//	scan.fetch(currentRow.getRowArray());
			scan.fetch(currentRow);
//Gemstone changes End			
			currRowFromMem = false;
			return currentRow;
		}
		return (ExecRow)null;
	}

	public void deleteCurrentRow()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isVirtualMemHeap, "deleteCurrentRow is not implemented");
		}
		if (currRowFromMem)
		{
			if (holder.lastArraySlot > 0)				// 0 is kept for template
				rowArray[holder.lastArraySlot] = null;  // erase reference
			holder.lastArraySlot--;
		}
		else
		{
			if (baseRowLocation == null)
				baseRowLocation = scan.newRowLocationTemplate();
//                      Gemstone changes Begin			
			baseRowLocation =scan.fetchLocation(baseRowLocation);
//                      Gemstone changes End
			if(heapCC == null)
			{
                heapCC = tc.openConglomerate(holder.getTemporaryConglomId(),
											  false,
											  TransactionController.OPENMODE_FORUPDATE,
											  TransactionController.MODE_TABLE,
											  TransactionController.ISOLATION_SERIALIZABLE);
			}
			heapCC.delete(baseRowLocation);
		}
	}


	//following variables are specific to the position based scans.
	DataValueDescriptor[] indexRow;
	ScanController indexsc;

	//open the scan of the temporary heap and the position index
	private void setupPositionBasedScan(long position) throws StandardException
	{

		//incase nothing is inserted yet into the temporary row holder
        if (holder.getTemporaryConglomId() == 0)
			return;
		if(heapCC == null)
		{
			heapCC = tc.openConglomerate( holder.getTemporaryConglomId(),
										  false,
										  0,
										  TransactionController.MODE_TABLE,
										  TransactionController.ISOLATION_SERIALIZABLE);

		}

		currentRow = rowArray[0].getNewNullRow();
		indexRow = new DataValueDescriptor[2];
		indexRow[0] = new SQLLongint(position);
		indexRow[1] = 	heapCC.newRowLocationTemplate();

		DataValueDescriptor[] searchRow =  new DataValueDescriptor[1];
		searchRow[0] = new SQLLongint(position);

		if(indexsc == null)
		{
			indexsc = tc.openScan(positionIndexConglomId,
								  false,                           // don't hold open across commit
								  0,                               // for read
								  TransactionController.MODE_TABLE,
								  TransactionController.ISOLATION_SERIALIZABLE,
								  (FormatableBitSet) null,                  // all fields as objects
								  searchRow,            	          // start position - first row
								  ScanController.GE,               // startSearchOperation
								  null,                            //scanQualifier,
								  null,                           // stop position - through last row
								  ScanController.GT               // stopSearchOperation
								// GemStone changes BEGIN
								  , null);
			                                        // GemStone changes END
		}else
		{

			indexsc.reopenScan(
						searchRow,                      	// startKeyValue
						ScanController.GE,            		// startSearchOp
						null,                         		// qualifier
						null, 		                        // stopKeyValue
						ScanController.GT             		// stopSearchOp
						// GemStone changes BEGIN
                                                , null
                                                // GemStone changes END
						);
		}
		
	} 


	//get the next row inserted into the temporary holder
	private ExecRow getNextAppendedRow() throws StandardException
	{
		if (indexsc == null) return null;
		if (!indexsc.fetchNext(indexRow))
		{
			return null;
		}
// GemStone changes BEGIN
		RowLocation baseRowLocation =  (RowLocation) indexRow[1];
		baseRowLocation = heapCC.fetch(baseRowLocation,
		    currentRow, (FormatableBitSet)null, false);
		boolean base_row_exists = baseRowLocation != null;
		if (base_row_exists) {
		  indexRow[1] = baseRowLocation;
		}
// GemStone changes END
        if (SanityManager.DEBUG)
        {
          if (!base_row_exists) {
            SanityManager.THROWASSERT("base row disappeared: " + indexRow[1]);
          }
        }
		numRowsOut++; 
		return currentRow;
	}



	/**
	 * Return the point of attachment for this subquery.
	 * (Only meaningful for Any and Once ResultSets, which can and will only
	 * be at the top of a ResultSet for a subquery.)
	 *
	 * @return int	Point of attachment (result set number) for this
	 *			    subquery.  (-1 if not a subquery - also Sanity violation)
	 */
	public int getPointOfAttachment()
	{
		return -1;
	}

	/**
	 * Return the isolation level of the scan in the result set.
	 * Only expected to be called for those ResultSets that
	 * contain a scan.
	 *
	 * @return The isolation level of the scan (in TransactionController constants).
	 */
	public int getScanIsolationLevel()
	{
		return TransactionController.ISOLATION_SERIALIZABLE;	
	}

	/**
	 * Notify a NPRS that it is the source for the specified 
	 * TargetResultSet.  This is useful when doing bulk insert.
	 *
	 * @param trs	The TargetResultSet.
	 */
	public void setTargetResultSet(TargetResultSet trs)
	{
	}

	/**
	 * Set whether or not the NPRS need the row location when acting
	 * as a row source.  (The target result set determines this.)
	 * 
	 */
	public void setNeedsRowLocation(boolean needsRowLocation)
	{
	}

	/**
	 * Get the estimated row count from this result set.
	 *
	 * @return	The estimated row count (as a double) from this result set.
	 */
	public double getEstimatedRowCount()
	{
		return 0d;
	}

	/**
	 * Get the number of this ResultSet, which is guaranteed to be unique
	 * within a statement.
	 */
	public int resultSetNumber()
	{
		return 0;
	}

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public void setCurrentRow(ExecRow row)
	{
		currentRow = row;
	}

	/**
	 * Clear the current row
	 *
	 */
	public void clearCurrentRow()
	{
		currentRow = null;
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen, "resultSet expected to be open");
		}

		return currentRow;
	}

	/**
	 * Returns the row location of the current base table row of the cursor.
	 * If this cursor's row is composed of multiple base tables' rows,
	 * i.e. due to a join, then a null is returned.  For
	 * a temporary row holder, we always return null.
	 *
	 * @return the row location of the current cursor row.
	 */
	public RowLocation getRowLocation()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen, "resultSet expected to be open");
		}
		return (RowLocation)null;
	}


	/**
	 * Clean up
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		isOpen = false;
		numRowsOut = 0;
		currentRow = null;
		if (scan != null)
		{
			scan.close();
			scan = null;
		}
// GemStone changes BEGIN
		if (this.closeHolder) {
		  this.holder.close();
		}
// GemStone changes END
	}


	//////////////////////////////////////////////////////////////////////////
	//
	// MISC FROM RESULT SET
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Returns TRUE if the statement returns rows (i.e. is a SELECT
	 * or FETCH statement), FALSE if it returns no rows.
	 *
	 * @return	TRUE if the statement returns rows, FALSE if not.
	 */
	public boolean	returnsRows()
	{
		return true;
	}

	public int modifiedRowCount() { return 0;};

	/**
	 * Tells the system that there will be calls to getNextRow().
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void open() throws StandardException
	{
		openCore();
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getAbsoluteRow(int row) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getAbsoluteRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getRelativeRow(int row) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getRelativeRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setBeforeFirstRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"setBeforeFirstRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getFirstRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getFirstRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the next row from the query, and returns NULL when there
	 * are no more rows.
	 *
	 * @return	The next row, or NULL if no more rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getNextRow() throws StandardException
	{
		return getNextRowCore();
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getPreviousRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getPreviousRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getLastRow()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setAfterLastRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called yet.");
		}

		return null;
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public boolean checkRowPosition(int isType)
	{
		return false;
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public int getRowNumber()
	{
		return 0;
	}

	/**
	 * Tells the system to clean up on an error.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void	cleanUp(boolean cleanupOnError) throws StandardException
	{
		close(cleanupOnError);
	}


	/**
		Find out if the ResultSet is closed or not.
		Will report true for result sets that do not return rows.

		@return true if the ResultSet has been closed.
	 */
	public boolean isClosed()
	{
		return !isOpen;
	}

	/**
	 * Tells the system that there will be no more access
	 * to any database information via this result set;
	 * in particular, no more calls to open().
	 * Will close the result set if it is not already closed.
	 *
	 * @exception StandardException	on error
	 */
	public void finish() throws StandardException
	{
		finished = true;
		close(false);
	}


	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return 0L;
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		return (Timestamp)null;
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		return (Timestamp)null;
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
		return 0L;
	}


	/**
	 * Get the subquery ResultSet tracking array from the top ResultSet.
	 * (Used for tracking open subqueries when closing down on an error.)
	 *
	 * @param numSubqueries		The size of the array (For allocation on demand.)
	 *
	 * @return NoPutResultSet[]	Array of NoPutResultSets for subqueries.
	 */
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		return (NoPutResultSet[])null;
	}

	/**
	 * Returns the name of the cursor, if this is cursor statement of some
	 * type (declare, open, fetch, positioned update, positioned delete,
	 * close).
	 *
	 * @return	A String with the name of the cursor, if any. Returns
	 *		NULL if this is not a cursor statement.
	 */
	public String	getCursorName()
	{
		return (String) null;
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"requiresRelocking() not expected to be called for " +
				getClass().getName());
		}
		return false;
	}

	/////////////////////////////////////////////////////////
	//
	// Access/RowSource -- not implemented
	// 
	/////////////////////////////////////////////////////////
	/**
		Get the next row as an array of column objects. The column objects can
		be a JBMS Storable or any
		Serializable/Externalizable/Formattable/Streaming type.
		<BR>
		A return of null indicates that the complete set of rows has been read.

		<p>
		A null column can be specified by leaving the object null, or indicated
		by returning a non-null getValidColumns.  On streaming columns, it can
		be indicated by returning a non-null get FieldStates.

		<p>
        If RowSource.needToClone() is true then the returned row (the 
        DataValueDescriptor[]) is guaranteed not to be modified by drainer of 
        the RowSource (except that the input stream will be read, of course) 
        and drainer will keep no reference to it before making the subsequent 
        nextRow call.  So it is safe to return the same DataValueDescriptor[] 
        in subsequent nextRow calls if that is desirable for performance 
        reasons.  
		<p>
        If RowSource.needToClone() is false then the returned row (the 
        DataValueDescriptor[]) may be be modified by drainer of the RowSource, 
        and the drainer may keep a reference to it after making the subsequent 
        nextRow call.  In this case the client should severe all references to 
        the row after returning it from getNextRowFromRowSource().

		@exception StandardException Standard Derby Error Policy
	 */
	public ExecRow getNextRowFromRowSource() throws StandardException
	{ 
		return null;
	}

	/**
        Does the caller of getNextRowFromRowSource() need to clone the row
        in order to keep a reference to the row past the 
        getNextRowFromRowSource() call which returned the row.  This call
        must always return the same for all rows in a RowSource (ie. the
        caller will call this once per scan from a RowSource and assume the
        behavior is true for all rows in the RowSource).

	 */
	public boolean needsToClone()
	{
		return false;
	}


	/**
	  getValidColumns describes the DataValueDescriptor[] returned by all 
      calls to the getNextRowFromRowSource() call. 

	  If getValidColumns returns null, the number of columns is given by the
	  DataValueDescriptor.length where DataValueDescriptor[] is returned by the
      preceeding getNextRowFromRowSource() call.  Column N maps to 
      DataValueDescriptor[N], where column numbers start at zero.

	  If getValidColumns return a non null validColumns FormatableBitSet the number of
	  columns is given by the number of bits set in validColumns.  Column N is
	  not in the partial row if validColumns.get(N) returns false.  Column N is
	  in the partial row if validColumns.get(N) returns true.  If column N is
	  in the partial row then it maps to DataValueDescriptor[M] where M is the
      count of calls to validColumns.get(i) that return true where i < N.  If
	  DataValueDescriptor.length is greater than the number of columns 
      indicated by validColumns the extra entries are ignored.  
	*/
	public FormatableBitSet getValidColumns()
	{
		return null;
	}

	/**
		closeRowSource tells the RowSource that it will no longer need to
		return any rows and it can release any resource it may have.
		Subsequent call to any method on the RowSource will result in undefined
		behavior.  A closed rowSource can be closed again.
	*/
	public void closeRowSource()
	{ }


	/////////////////////////////////////////////////////////
	//
	// Access/RowLocationRetRowSource -- not implemented
	// 
	/////////////////////////////////////////////////////////
	/**
		needsRowLocation returns true iff this the row source expects the
		drainer of the row source to call rowLocation after getting a row from
		getNextRowFromRowSource.

		@return true iff this row source expects some row location to be
		returned 
		@see #rowLocation
	 */
	public boolean needsRowLocation()
	{
		return false;
	}

	/**
		rowLocation is a callback for the drainer of the row source to return
		the rowLocation of the current row, i.e, the row that is being returned
		by getNextRowFromRowSource.  This interface is for the purpose of
		loading a base table with index.  In that case, the indices can be
		built at the same time the base table is laid down once the row
		location of the base row is known.  This is an example pseudo code on
		how this call is expected to be used:
		
		<BR><pre>
		boolean needsRL = rowSource.needsRowLocation();
		DataValueDescriptor[] row;
		while((row = rowSource.getNextRowFromRowSource()) != null)
		{
			RowLocation rl = heapConglomerate.insertRow(row);
			if (needsRL)
				rowSource.rowLocation(rl);
		}
		</pre><BR>

		NeedsRowLocation and rowLocation will ONLY be called by a drainer of
		the row source which CAN return a row location.  Drainer of row source
		which cannot return rowLocation will guarentee to not call either
		callbacks. Conversely, if NeedsRowLocation is called and it returns
		true, then for every row return by getNextRowFromRowSource, a
		rowLocation callback must also be issued with the row location of the
		row.  Implementor of both the source and the drain of the row source
		must be aware of this protocol.

		<BR>
		The RowLocation object is own by the caller of rowLocation, in other
		words, the drainer of the RowSource.  This is so that we don't need to
		new a row location for every row.  If the Row Source wants to keep the
		row location, it needs to clone it (RowLocation is a ClonableObject).
		@exception StandardException on error
	 */
	public void rowLocation(RowLocation rl) throws StandardException
	{ }

	/**
	 * @see NoPutResultSet#positionScanAtRowLocation
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void positionScanAtRowLocation(RowLocation rl) 
		throws StandardException 
	{
		// Only used for Scrollable insensitive result sets otherwise no-op
	}

	// Class implementation

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
	}

	/**
	 * Shallow clone this result set.  Used in trigger reference.
	 * beetle 4373.
	 */
	public Object clone()
	{
		Object clo = null;
		try {
			clo = super.clone();
		}
		catch (CloneNotSupportedException e) {}
		return clo;
	}
	public java.sql.SQLWarning getWarnings() {
		return null;
	}

	/**
	 * @see NoPutResultSet#updateRow
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void updateRow(ExecRow row) throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	@Override
	  public void deleteRowDirectly() throws StandardException {
	    throw new UnsupportedOperationException(
	        "This method should not have been invoked");
	  }
	
	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void markRowAsDeleted() throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * Return the <code>Activation</code> for this result set.
	 *
	 * @return activation
	 */
	public final Activation getActivation() {
		return holder.activation;
	}

  // GemStone changes BEGIN

  private boolean closeHolder;

  public void closeHolderOnClose(boolean close) {
    this.closeHolder = close;
  }

  @Override
  public final boolean canUpdateInPlace() {
    return false;
  }

  @Override
  public TXState initLocalTXState() {
    SanityManager.THROWASSERT("not expected to be invoked");
    // never reached
    return null;
  }

  @Override
  public void upgradeReadLockToWrite(final RowLocation rl,
      final GemFireContainer container) throws StandardException {
    SanityManager.THROWASSERT("not expected to be invoked");
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    SanityManager.THROWASSERT("not expected to be invoked");
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    if (this.isAppendable) {
      if (this.indexsc != null) {
        this.indexsc.releaseCurrentRowLocationReadLock();
      }
    }
    else if (this.scan != null) {
      this.scan.releaseCurrentRowLocationReadLock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsMoveToNextKey() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScanKeyGroupID() {
    throw new UnsupportedOperationException("not expected to be invoked");
  }

  /**
   * @see ResultSet#hasAutoGeneratedKeysResultSet
   */
  @Override
  public boolean hasAutoGeneratedKeysResultSet() {
    return false;
  }

  /**
   * @see ResultSet#flushBatch
   */
  @Override
  public void flushBatch() {
  }
  
  /**
   * @see ResultSet#closeBatch
   */
  @Override
  public void closeBatch() {
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public UUID getExecutionPlanID() {
    return null;
  }

  @Override
  public void markLocallyExecuted() {
    // no action
  }

  @Override
  public void resetStatistics() {
    // no action
  }

  /**
   * {@inheritDoc}
   */
  public boolean isDistributedResultSet() {
    return false;
  }

  private boolean hasLockReference;

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addLockReference(GemFireTransaction tran) {
    tran.getLockSpace().addResultSetRef();
    this.hasLockReference = true;
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean releaseLocks(GemFireTransaction tran) {
    if (this.hasLockReference) {
      tran.releaseAllLocks(false, true);
      this.hasLockReference = false;
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public void setGfKeysForNCJoin(ArrayList<DataValueDescriptor> keys)
      throws StandardException {
    throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
        " Currently this method is not implemented or overridden for class "
            + this.getClass().getSimpleName());
  }
  
  @Override
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn, GemFireContainer container)
      throws StandardException {
    return RowUtil.fetch(loc, destRow, validColumns, faultIn, container, null,
        null, 0, (GemFireTransaction)this.tc.getLanguageConnectionContext().
        getTransactionExecute());
  }

  public Context getNewPlanContext() {
    return new PlanUtils.Context();
  }

  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    PlanUtils.xmlBeginTag(builder, context, this);
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_AUTO_GEN_KEYS);
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
  }
  
  @Override
  public void releasePreviousByteSource() {
  }

  @Override
  public void setMaxSortingLimit(long limit) {
  }

  @Override
  public void checkCancellationFlag() throws StandardException {
    if (this.holder.activation != null) {
      this.holder.activation.checkCancellationFlag();
    }
  }
  
  @Override
  public void forceReOpenCore() throws StandardException { 
  }
// GemStone changes END

 

}
