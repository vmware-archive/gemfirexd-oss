/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ScrollInsensitiveResultSet

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
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.BackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 *
 * Provide insensitive scrolling functionality for the underlying
 * result set.  We build a disk backed hash table of rows as the 
 * user scrolls forward, with the position as the key.
 *
 * For read-only result sets the hash table will containg the
 * following columns:
 *<pre>
 *  +-------------------------------+
 *  | KEY                           |
 *  +-------------------------------+
 *  | Row                           |
 *  +-------------------------------+
 *</pre>
 * where key is the position of the row in the result set and row is the data.
 *
 * And for updatable result sets it will contain:
 * <pre>
 *  +-------------------------------+
 *  | KEY                           | [0]
 *  +-------------------------------+
 *  | RowLocation                   | [POS_ROWLOCATION]
 *  +-------------------------------+
 *  | Deleted                       | [POS_ROWDELETED]
 *  +-------------------------------+
 *  | Updated                       | [POS_ROWUPDATED]
 *  +-------------------------------+
 *  | Row                           | [extraColumns ... n]
 *  +-------------------------------+
 *</pre>
 * where key is the position of the row in the result set, rowLocation is
 * the row location of that row in the Heap, Deleted indicates whether the
 * row has been deleted, Updated indicates whether the row has been updated,
 * and row is the data.
 *
 */

public class ScrollInsensitiveResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;



	private int							sourceRowWidth;

	private	  BackingStoreHashtable		ht;
	private	  ExecRow					resultRow;

	// Scroll tracking
	private int positionInSource;
	private int currentPosition;
	private int lastPosition;
	private	boolean seenFirst;
	private	boolean seenLast;
	private	boolean beforeFirst = true;
	private	boolean afterLast;

	public int numFromHashTable;
	public int numToHashTable;

	private int maxRows;

    private boolean keepAfterCommit;

	/* The hash table will contain a different number of extra columns depending
	 * on whether the result set is updatable or not.
	 * extraColumns will contain the number of extra columns on the hash table,
	 * 1 for read-only result sets and LAST_EXTRA_COLUMN + 1 for updatable 
	 * result sets.
	 */
	private int extraColumns;
	
	/* positionInHashTable is used for getting a row from the hash table. Prior
	 * to getting the row, positionInHashTable will be set to the desired KEY.
	 */
	private SQLInteger positionInHashTable;

	/* Reference to the target result set. Target is used for updatable result
	 * sets in order to keep the target result set on the same row as the
	 * ScrollInsensitiveResultSet.  
	 */
	private CursorResultSet target;

	/* If the last row was fetched from the HashTable, updatable result sets
	 * need to be positioned in the last fetched row before resuming the 
	 * fetch from core.
	 */
	private boolean needsRepositioning;


	/* Position of the different fields in the hash table row for updatable
	 * result sets 
	 */
	private static final int POS_ROWLOCATION = 1;
	private static final int POS_ROWDELETED = 2;
	private static final int POS_ROWUPDATED = 3;
	private static final int LAST_EXTRA_COLUMN = 3;

	/**
	 * Constructor for a ScrollInsensitiveResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to scroll through
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 * @param sourceRowWidth			# of columns in the source row
	 *
	 * @exception StandardException	on error
	 */

	public ScrollInsensitiveResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  int sourceRowWidth,
							  double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost) throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.source = source;
		this.sourceRowWidth = sourceRowWidth;
        keepAfterCommit = activation.getResultSetHoldability();
		maxRows = activation.getMaxRows();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(maxRows != -1,
				"maxRows not expected to be -1");
		}

		positionInHashTable = new SQLInteger();
		needsRepositioning = false;
		if (isForUpdate()) {
			target = ((CursorActivation)activation).getTargetResultSet();
			extraColumns = LAST_EXTRA_COLUMN + 1;
		} else {
			target = null;
			extraColumns = 1;
		}
		
// GemStone changes BEGIN
		// ensure one initLocalTXState() call before getNextRowCore()
		initLocalTXState();
// GemStone changes END
		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
	}


	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the source. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
 	 * @exception StandardException thrown on failure 
     */
	public void	openCore() throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "ScrollInsensitiveResultSet already open");

		isOpen = true;
        source.openCore();
	    //isOpen = true;
		numOpens++;

		/* Create the hash table.  We pass
		 * null in as the row source as we will
		 * build the hash table on demand as
		 * the user scrolls.
		 * The 1st column, the position in the
		 * scan, will be the key column.
		 */
		final int[] keyCols = new int[] { 0 };
		
		/* We don't use the optimizer row count for this because it could be
		 * wildly pessimistic.  We only use Hash tables when the optimizer row count
		 * is within certain bounds.  We have no alternative for scrolling insensitive 
		 * cursors so we'll just trust that it will fit.
		 * We need BackingStoreHashtable to actually go to disk when it doesn't fit.
		 * This is a known limitation.
		 */
		ht = new BackingStoreHashtable(getTransactionController(),
									   null,
									   keyCols,
									   false,
										-1, // don't trust optimizer row count
									   HashScanResultSet.DEFAULT_MAX_CAPACITY,
									   HashScanResultSet.DEFAULT_INITIAL_CAPACITY,
									   HashScanResultSet.DEFAULT_MAX_CAPACITY,
									   false,
                                       keepAfterCommit);

		// When re-using language result sets (DERBY-827) we need to
		// reset some member variables to the value they would have
		// had in a newly constructed object.
		lastPosition = 0;
		needsRepositioning = false;
		numFromHashTable = 0;
		numToHashTable = 0;
		positionInSource = 0;
		seenFirst = false;
		seenLast = false;
		maxRows = activation.getMaxRows();

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
		setBeforeFirstRow();
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
		boolean constantEval = true;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		if (SanityManager.DEBUG)
		{
		    SanityManager.ASSERT(isOpen, "ScrollInsensitiveResultSet already open");
			SanityManager.THROWASSERT(
				"reopenCore() not expected to be called");
		}
		setBeforeFirstRow();
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
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	getAbsoluteRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "absolute");
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

                // Absolute 0 is defined to be before first!
		if (row == 0)
		{
                    setBeforeFirstRow();
                    return null;
		}

		if (seenLast && row > lastPosition) {
		   return setAfterLastRow();
		}		

		if (row > 0)
		{
			// position is from the start of the result set
			if (row <= positionInSource)
			{
				// We've already seen the row before
				return getRowFromHashTable(row);
			}
			
			/* We haven't seen the row yet, scan until we find
			 * it or we get to the end.
			 */
			int diff = row - positionInSource;
			ExecRow result = null;
			while (diff > 0)
			{
				if ((result = getNextRowFromSource()) != null)
				{
					diff--;
				}
				else
				{
					break;
				}
			}
			if (result != null) {
				result = getRowFromHashTable(row);
			}
			currentRow = result;
			return result;
		}
		else if (row < 0)
		{
			// position is from the end of the result set

			// Get the last row, if we haven't already
			if (!seenLast)
			{
				getLastRow();
			}

			// Note, for negative values position is from beyond the end
			// of the result set, e.g. absolute(-1) points to the last row
			int beyondResult = lastPosition + 1;
			if (beyondResult + row > 0)
			{
				// valid row
				return getRowFromHashTable(beyondResult + row);
			}
			else
			{
				// position before the beginning of the result set
				return setBeforeFirstRow();
			}
		}
 
		currentRow = null;
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
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	getRelativeRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "relative");
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		// Return the current row for 0
		if (row == 0)
		{
                    if (beforeFirst || afterLast || currentPosition==0) {
                        return null;
                    } else {
			return getRowFromHashTable(currentPosition);
                    }
		}
		else if (row > 0)
		{
			return getAbsoluteRow(currentPosition + row);
		}
		else
		{
			// row < 0
			if (currentPosition + row < 0)
			{
				return setBeforeFirstRow();
			}
			return getAbsoluteRow(currentPosition + row);
		}
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	setBeforeFirstRow() 
	{
		currentPosition = 0;
		beforeFirst = true;
		afterLast = false;
		currentRow = null;
		return null;
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	getFirstRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "first");
		}

		/* Get the row from the hash table if
		 * we have already seen it before.
		 */
		if (seenFirst)
		{
			return getRowFromHashTable(1);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		return getNextRowCore();
	}

	/**
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
		ExecRow result = null;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (!isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		if (seenLast && currentPosition == lastPosition) {
		   return setAfterLastRow();
		}

		/* Should we get the next row from the source or the hash table? */
		if (currentPosition == positionInSource)
		{
			/* Current position is same as position in source.
			 * Get row from the source.
			 */
			result = getNextRowFromSource();
			if (result !=null) {
				result = getRowFromHashTable(currentPosition);
			}
		}
		else if (currentPosition < positionInSource)
		{
			/* Current position is before position in source.
			 * Get row from the hash table.
			 */
			result = getRowFromHashTable(currentPosition + 1);
		}
		else
		{
			result = null;
		}

		if (result != null)
		{
			rowsSeen++;
			afterLast = false;
		}

		setCurrentRow(result);
		beforeFirst = false;
// GemStone changes BEGIN
		if (this.localTXState != null && this.isTopResultSet &&
		    result != null && isForUpdate()) {
		  updateRowLocationPostRead();
		}
// GemStone changes END

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);

		return result;
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	getPreviousRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		/* No row if we are positioned before the first row
		 * or the result set is empty.
		 */
		if (beforeFirst || currentPosition == 0)
		{
			currentRow = null;
			return null;
		}

		// Get the last row, if we are after it
		if (afterLast)
		{
			// Special case for empty tables
			if (lastPosition == 0)
			{
				afterLast = false;
				beforeFirst = false;
				currentRow = null;
				return null;
			}
			else
			{
				return getRowFromHashTable(lastPosition);
			}
		}

		// Move back 1
		currentPosition--;
		if (currentPosition == 0)
		{
			setBeforeFirstRow();
			return null;
		}
		return getRowFromHashTable(currentPosition);
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	getLastRow()
		throws StandardException
	{		
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
		}
		
		if (!seenLast) 
		{
			attachStatementContext();

			if (SanityManager.DEBUG)
			{
				if (!isTopResultSet)
				{
					SanityManager.THROWASSERT(
											  this + "expected to be the top ResultSet");
				}
			}
			
			/* Scroll to the end, filling the hash table as
			 * we scroll, and return the last row that we find.
			 */
			ExecRow result = null;
			while ((result = getNextRowFromSource()) != null);
		}
		
		if (SanityManager.DEBUG && !seenLast)
		{
			SanityManager.THROWASSERT(this + "expected to have seen last");
		}
		
		beforeFirst = false;
		afterLast = false;

		// Special case if table is empty
		if (lastPosition == 0)
		{
			currentRow = null;
			return null;
		}
		else
		{
			return getRowFromHashTable(lastPosition);
		}
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.Row
	 */
	public ExecRow	setAfterLastRow() 
		throws StandardException
	{
		if (! seenLast)
		{
			getLastRow();
		}
		if (lastPosition == 0) {
		   // empty rs special case
		   currentPosition = 0;
		   afterLast = false;
		} else {
		   currentPosition = lastPosition + 1;
		   afterLast = true;
		}

		beforeFirst = false;
		currentRow = null;
		return null;
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
	 * @exception StandardException Thrown on error.
     */
   public boolean checkRowPosition(int isType) throws StandardException
	{
		switch (isType) {
		case ISBEFOREFIRST:

			if (! beforeFirst)
			{
				return false;
			}

			//  Spec says to return false if result set is empty
			if (seenFirst)
			{
				return true;
			}
			else
			{
				ExecRow firstRow = getFirstRow();
				if (firstRow == null)
				{
					// ResultSet is empty
					return false;
				}
				else
				{
					// ResultSet is not empty - reset position
					getPreviousRow();
					return true;
				}
			}
		case ISFIRST:
			return (currentPosition == 1);
		case ISLAST:
			if (beforeFirst || afterLast || currentPosition==0 ||
				currentPosition<positionInSource)
			{
				return false;
			}			
			
			/* If we have seen the last row, we can tell if we are 
			 * on it by comparing currentPosition with lastPosition.
			 * Otherwise, we check if there is a next row.
			 */
			if (seenLast)
			{
				return (currentPosition == lastPosition);
			}
			else
			{
				final int savePosition = currentPosition;
				final boolean retval = (getNextRowFromSource() == null);
				getRowFromHashTable(savePosition);
				return retval;
			}
		case ISAFTERLAST:
			return afterLast;
		default:
			return false;
		}
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
		return currentRow == null ? 0 : currentPosition;
	}

	/* Get the next row from the source ResultSet tree and insert into the hash table */
	private ExecRow getNextRowFromSource() throws StandardException
	{
		ExecRow		sourceRow = null;
		ExecRow		result = null;

		/* Don't give back more rows than requested */
		if (maxRows > 0 && maxRows == positionInSource)
		{
			seenLast = true;
			lastPosition = positionInSource;
			afterLast = true;
			return null;
		}


		if (needsRepositioning) {
			positionInLastFetchedRow();
			needsRepositioning = false;
		}
		sourceRow = source.getNextRowCore();
    
    if (sourceRow != null) {

      seenFirst = true;
      beforeFirst = false;

      // long beginTCTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
      /*
       * If this is the first row from the source then we create a new row for
       * use when fetching from the hash table.
       */
      if (resultRow == null) {
        resultRow = activation.getExecutionFactory()
            .getValueRow(sourceRowWidth);
      }

      positionInSource++;
      currentPosition = positionInSource;

      RowLocation rowLoc = null;
      if (source.isForUpdate()) {
        rowLoc = ((CursorResultSet) source).getRowLocation();
      }

      addRowToHashTable(sourceRow, currentPosition, rowLoc, false);
      //TODO:Asif check if a Group by may be on top of scroll insensitive resultset
      this.source.releasePreviousByteSource();
    }
		// Remember whether or not we're past the end of the table
		else
		{
			if (! seenLast)
			{
				lastPosition = positionInSource;
			}
			seenLast = true;
			// Special case for empty table (afterLast is never true)
			if (positionInSource == 0)
			{
				afterLast = false;
			}
			else
			{
				afterLast = true;
				currentPosition = positionInSource + 1;
			}
		}

		return sourceRow;
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
			currentRow = null;
	        source.close(cleanupOnError);

			if (ht != null)
			{
				ht.close();
				ht = null;
			}

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ScrollInsensitiveResultSet repeated");
		setBeforeFirstRow();

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
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

	//
	// CursorResultSet interface
	//

	/**
	 * Gets information from its source. We might want
	 * to have this take a CursorResultSet in its constructor some day,
	 * instead of doing a cast here?
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public RowLocation getRowLocation() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source not CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
	}

	/**
	 * Gets information from last getNextRow call.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException
	{
		if (isForUpdate() && isDeleted()) {
			return null;
		} else {
			return currentRow;
		}
	}

	//
	// class implementation
	//

	/**
	 * Add a row to the backing hash table, keyed on position.
	 * When a row gets updated when using scrollable insensitive updatable
	 * result sets, the old entry for the row will be deleted from the hash 
	 * table and this method will be called to add the new values for the row
	 * to the hash table, with the parameter rowUpdated = true so as to mark 
	 * the row as updated. The latter is done in order to implement 
	 * detectability of own changes for result sets of this type.
	 *
	 * @param sourceRow	The row to add.
	 * @param position The key
	 * @param rowLoc The rowLocation of the row to add.
	 * @param rowUpdated Indicates whether the row has been updated.
	 *
	 */
	private void addRowToHashTable(ExecRow sourceRow, int position,
			RowLocation rowLoc, boolean rowUpdated)
		throws StandardException
	{
		DataValueDescriptor[] hashRowArray = new 
				DataValueDescriptor[sourceRowWidth + extraColumns];
		// 1st element is the key
		hashRowArray[0] = new SQLInteger(position);
		if (isForUpdate()) {
			hashRowArray[POS_ROWLOCATION] = rowLoc.getClone();
			hashRowArray[POS_ROWDELETED] = new SQLBoolean(false);
			hashRowArray[POS_ROWUPDATED] = new SQLBoolean(rowUpdated);
		}

		/* Copy rest of elements from sourceRow.
		 * NOTE: We need to clone the source row
		 * and we do our own cloning since the 1st column
		 * is not a wrapper.
		 */
		DataValueDescriptor[] sourceRowArray = sourceRow.getRowArray();

	//	System.out.println("XXX the source row width "+this.sourceRowWidth+" ExtraColumns="+extraColumns+" the source row length="+sourceRowArray.length);
                // GemStone changes BEGIN
		/*(original code) System.arraycopy(sourceRowArray, 0, hashRowArray, extraColumns, 
		    sourceRowArray.length);*/
                System.arraycopy(sourceRowArray, 0, hashRowArray, extraColumns,
                    sourceRowWidth);
                
		ht.putRow(true, hashRowArray, null);
		// GemStone changes END

		numToHashTable++;
	}

	/**
	 * Get the row at the specified position
	 * from the hash table.
	 *
	 * @param position	The specified position.
	 *
	 * @return	The row at that position.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	private ExecRow getRowFromHashTable(int position)
		throws StandardException
	{

		// Get the row from the hash table
		positionInHashTable.setValue(position);
		DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
				ht.get(positionInHashTable);


		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(hashRowArray != null,
				"hashRowArray expected to be non-null");
		}
		// Copy out the Object[] without the position.
		DataValueDescriptor[] resultRowArray = new 
				DataValueDescriptor[hashRowArray.length - extraColumns];
		System.arraycopy(hashRowArray, extraColumns, resultRowArray, 0, 
				resultRowArray.length);

		resultRow.setRowArray(resultRowArray);

		// Reset the current position to the user position
		currentPosition = position;

		numFromHashTable++;

		if (resultRow != null)
		{
			beforeFirst = false;
			afterLast = false;
		}

		if (isForUpdate()) {
			RowLocation rowLoc = (RowLocation) hashRowArray[POS_ROWLOCATION];
			// Keep source and target with the same currentRow
			((NoPutResultSet)target).setCurrentRow(resultRow);
			((NoPutResultSet)target).positionScanAtRowLocation(rowLoc);
			needsRepositioning = true;
		}
		
		setCurrentRow(resultRow);

		return resultRow;
	}
	
	/**
	 * Get the row data at the specified position 
	 * from the hash table.
	 *
	 * @param position	The specified position.
	 *
	 * @return	The row data at that position.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	private DataValueDescriptor[] getRowArrayFromHashTable(int position)
		throws StandardException
	{
		positionInHashTable.setValue(position);
		final DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
			ht.get(positionInHashTable);
		
		// Copy out the Object[] without the position.
		final DataValueDescriptor[] resultRowArray = new 
			DataValueDescriptor[hashRowArray.length - extraColumns];
		System.arraycopy(hashRowArray, extraColumns, resultRowArray, 0, 
						 resultRowArray.length);
		return resultRowArray;
	}

	/**
	 * Positions the cursor in the last fetched row. This is done before
	 * navigating to a row that has not previously been fetched, so that
	 * getNextRowCore() will re-start from where it stopped.
	 */
	private void positionInLastFetchedRow() throws StandardException {
		if (positionInSource > 0) {
			positionInHashTable.setValue(positionInSource);
			DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
					ht.get(positionInHashTable);
			RowLocation rowLoc = (RowLocation) hashRowArray[POS_ROWLOCATION];
			((NoPutResultSet)target).positionScanAtRowLocation(rowLoc);
			currentPosition = positionInSource;
		}
	}

	/**
	 * @see NoPutResultSet#updateRow
	 *
	 * Sets the updated column of the hash table to true and updates the row
	 * in the hash table with the new values for the row.
	 */
	public void updateRow(ExecRow row) throws StandardException {
		ExecRow newRow = row;
		boolean undoProjection = false;
		
		if (source instanceof ProjectRestrictResultSet) {
			newRow = ((ProjectRestrictResultSet)source).
				doBaseRowProjection(row);
			undoProjection = true;
		}
		positionInHashTable.setValue(currentPosition);
		DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
				ht.get(positionInHashTable);
		RowLocation rowLoc = (RowLocation) hashRowArray[POS_ROWLOCATION];
		ht.remove(new SQLInteger(currentPosition));
		addRowToHashTable(newRow, currentPosition, rowLoc, true);
		
		// Modify row to refer to data in the BackingStoreHashtable.
		// This allows reading of data which goes over multiple pages
		// when doing the actual update (LOBs). Putting columns of
		// type SQLBinary to disk, has destructive effect on the columns,
		// and they need to be re-read. That is the reason this is needed.
		if (undoProjection) {
			
			final DataValueDescriptor[] newRowData = newRow.getRowArray();
			
			// Array of original position in row
			final int[] origPos =((ProjectRestrictResultSet)source).
				getBaseProjectMapping(); 
			
			// We want the row to contain data backed in BackingStoreHashtable
			final DataValueDescriptor[] backedData = 
				getRowArrayFromHashTable(currentPosition);
			
			for (int i=0; i<origPos.length; i++) {
				if (origPos[i]>=0) {
					row.setColumn(origPos[i], backedData[i]);
				}
			}
		} else {
			row.setRowArray(getRowArrayFromHashTable(currentPosition));
		}
	}

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 *
	 * Sets the deleted column of the hash table to true in the current row.
	 */
	public void markRowAsDeleted() throws StandardException  {
		positionInHashTable.setValue(currentPosition);
		DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
				ht.get(positionInHashTable);
		RowLocation rowLoc = (RowLocation) hashRowArray[POS_ROWLOCATION];
		ht.remove(new SQLInteger(currentPosition));
		((SQLBoolean)hashRowArray[POS_ROWDELETED]).setValue(true);
		// Set all columns to NULL, the row is now a placeholder
		for (int i=extraColumns; i<hashRowArray.length; i++) {
			hashRowArray[i].setToNull();
		}
                // Gemstone changes BEGIN
		ht.putRow(true, hashRowArray, null);
		// Gemstone changes END
	}

	/**
	 * Returns TRUE if the row was been deleted within the transaction,
	 * otherwise returns FALSE
	 *
	 * @return True if the row has been deleted, otherwise false
	 *
	 * @exception StandardException on error
	 */
	public boolean isDeleted() throws StandardException  {
		if (currentPosition <= positionInSource && currentPosition > 0) {
			positionInHashTable.setValue(currentPosition);
			DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
					ht.get(positionInHashTable);
			return hashRowArray[POS_ROWDELETED].getBoolean();
		}
		return false;
	}

	/**
	 * Returns TRUE if the row was been updated within the transaction,
	 * otherwise returns FALSE
	 *
	 * @return True if the row has been deleted, otherwise false
	 *
	 * @exception StandardException on error
	 */
	public boolean isUpdated() throws StandardException {
		if (currentPosition <= positionInSource && currentPosition > 0) {
			positionInHashTable.setValue(currentPosition);
			DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) 
					ht.get(positionInHashTable);
			return hashRowArray[POS_ROWUPDATED].getBoolean();
		}
		return false;
	}

	public boolean isForUpdate() {
		return source.isForUpdate();
	}
	

// GemStone changes BEGIN
  public void setSourceRowWidth(int width) {
    this.sourceRowWidth = width;
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    this.source.updateRowLocationPostRead();
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    this.source.filteredRowLocationPostRead(localTXState);
  }
  
  @Override
  public void releasePreviousByteSource() {
    this.source.releasePreviousByteSource();
  }

  @Override
  public void accept(
      ResultSetStatisticsVisitor visitor) {
    if(source != null) {
      visitor.setNumberOfChildren(1);
    }
    else {
      visitor.setNumberOfChildren(0);
    }
    
    visitor.visit(this);
    
    if(source != null) {
      source.accept(visitor);
    }
    
  }
  
  @Override
  public void resetStatistics() {
    super.resetStatistics();
    source.resetStatistics();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isDistributedResultSet() {
    return this.source.isDistributedResultSet();
  }

  @Override
  protected AbstractGemFireResultSet getWrappedGemFireRS() {
    return this.source instanceof AbstractGemFireResultSet
        ? (AbstractGemFireResultSet)this.source : null;
  }

  @Override
  protected void attachStatementContext() throws StandardException {
    if (!isDistributedResultSet()) {
      super.attachStatementContext();
    }
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
// GemStone changes END

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
}
