/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.HashScanResultSet

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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableArrayHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableIntHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.BackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.store.access.KeyHasher;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes a conglomerate and a table filter builds a hash table on the 
 * specified column of the conglomerate on the 1st open.  Look up into the
 * hash table is done on the hash key column.  The hash table consists of
 * either <code>DataValueDescriptor[]</code>s or <code>List</code>s of
 * <code>DataValueDescriptor[]</code>. The store builds the hash table. When a
 * collision occurs, the store builds a <code>List</code> with the colliding
 * <code>DataValueDescriptor[]</code>s.
 */
public class HashScanResultSet extends ScanResultSet
	implements CursorResultSet
{
	private boolean		hashtableBuilt;
	private ExecIndexRow	startPosition;
	private ExecIndexRow	stopPosition;
	protected	ExecRow		compactRow;

	// Variable for managing next() logic on hash entry
	protected boolean	firstNext = true;
	private int			numFetchedOnNext;
	private int			entryVectorSize;
	private List		entryVector;
	// GemStone changes BEGIN
	private List rakVector;
	// GemStone changes END

    // set in constructor and not altered during
    // life of object.
    private long conglomId;
    protected StaticCompiledOpenConglomInfo scoci;
	private GeneratedMethod resultRowAllocator;
	private GeneratedMethod startKeyGetter;
	private int startSearchOperator;
	private GeneratedMethod stopKeyGetter;
	private int stopSearchOperator;
	public Qualifier[][] scanQualifiers;
	public Qualifier[][] nextQualifiers;
	private int initialCapacity;
	private float loadFactor;
	private int maxCapacity;
	public String tableName;
	public String userSuppliedOptimizerOverrides;
	public String indexName;
	public boolean forUpdate;
	private boolean runTimeStatisticsOn;
	public int[] keyColumns;
	private boolean sameStartStopPosition;
	private boolean skipNullKeyColumns;
	private boolean keepAfterCommit;

	protected BackingStoreHashtable hashtable;
	protected boolean eliminateDuplicates;		// set to true in DistinctScanResultSet

	// Run time statistics
	public Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public int hashtableSize;
	public boolean isConstraint;

	public static final	int	DEFAULT_INITIAL_CAPACITY = -1;
	public static final float DEFAULT_LOADFACTOR = (float) -1.0;
	public static final	int	DEFAULT_MAX_CAPACITY = -1;


    //
    // class interface
    //
    HashScanResultSet(long conglomId,
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] scanQualifiers,
		Qualifier[][] nextQualifiers,
		int initialCapacity,
		float loadFactor,
		int maxCapacity,
		int hashKeyItem,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		boolean skipNullKeyColumns,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
    {
		super(activation,
				resultSetNumber,
				resultRowAllocator,
				lockMode, tableLocked, isolationLevel,
                colRefItem,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
        this.scoci = scoci;
        this.conglomId = conglomId;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( activation!=null, "hash scan must get activation context");
			SanityManager.ASSERT( resultRowAllocator!= null, "hash scan must get row allocator");
			if (sameStartStopPosition)
			{
				SanityManager.ASSERT(stopKeyGetter == null,
					"stopKeyGetter expected to be null when sameStartStopPosition is true");
			}
		}

        this.resultRowAllocator = resultRowAllocator;

		this.startKeyGetter = startKeyGetter;
		this.startSearchOperator = startSearchOperator;
		this.stopKeyGetter = stopKeyGetter;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.scanQualifiers = scanQualifiers;
		this.nextQualifiers = nextQualifiers;
		this.initialCapacity = initialCapacity;
		this.loadFactor = loadFactor;
		this.maxCapacity = maxCapacity;
        this.tableName = tableName;
        this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
        this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.skipNullKeyColumns = skipNullKeyColumns;
		this.keepAfterCommit = activation.getResultSetHoldability();

		/* Retrieve the hash key columns */
		FormatableArrayHolder fah = (FormatableArrayHolder)
										(activation.
											getSavedObject(hashKeyItem));
		FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
		keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++)
		{
			keyColumns[index] = fihArray[index].getInt();
		}

		runTimeStatisticsOn = 
            getLanguageConnectionContext().getRunTimeStatisticsMode();

		compactRow =
				getCompactRow(candidate, accessedCols, false);
// GemStone changes BEGIN
		// ensure one initLocalTXState() call before getNextRowCore()
		initLocalTXState();
// GemStone changes END
		recordConstructorTime();
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

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
	    TransactionController tc;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "HashScanResultSet already open");

		isOpen = true;
        // Get the current transaction controller
        tc = activation.getTransactionController();

		initIsolationLevel();

		if (startKeyGetter != null)
		{
			startPosition = (ExecIndexRow) startKeyGetter.invoke(activation);
			if (sameStartStopPosition)
			{
				stopPosition = startPosition;
			}
		}
		if (stopKeyGetter != null)
		{
			stopPosition = (ExecIndexRow) stopKeyGetter.invoke(activation);
		}

		// Check whether there are any comparisons with unordered nulls
		// on either the start or stop position.  If there are, we can
		// (and must) skip the scan, because no rows can qualify
		if (skipScan(startPosition, stopPosition))
		{
			// Do nothing
			;
		}
		else if (! hashtableBuilt)
		{
			DataValueDescriptor[] startPositionRow = 
                startPosition == null ? null : startPosition.getRowArray();
			DataValueDescriptor[] stopPositionRow = 
                stopPosition == null ? null : stopPosition.getRowArray();

            hashtable = 
                tc.createBackingStoreHashtableFromScan(
                    conglomId,          // conglomerate to open
                    (forUpdate ? TransactionController.OPENMODE_FORUPDATE : 0),
                    lockMode,
                    isolationLevel,
                    accessedCols, 
                    startPositionRow,   
                    startSearchOperator,
                    scanQualifiers,
                    stopPositionRow,   
                    stopSearchOperator,
                    -1,                 // no limit on total rows.
                    keyColumns,      
                    eliminateDuplicates,// remove duplicates?
                    -1,                 // RESOLVE - is there a row estimate?
                    maxCapacity,
                    initialCapacity,    // in memory Hashtable initial capacity
                    loadFactor,         // in memory Hashtable load factor
                    runTimeStatisticsOn,
					skipNullKeyColumns,
					keepAfterCommit,
// GemStone changes BEGIN
// added activation argument
					activation);
// GemStone changes END


			checkCancellationFlag();
            if (runTimeStatisticsOn)
			{
				hashtableSize = hashtable.size();

				if (scanProperties == null)
				{
					scanProperties = new Properties();
				}

				try
				{
					if (hashtable != null)
					{
                        hashtable.getAllRuntimeStats(scanProperties);
					}
				}
				catch(StandardException se)
				{
					// ignore
				}
			}


			/* Remember that we created the hash table */
			hashtableBuilt = true;

			/*
			** Tell the activation about the number of qualifying rows.
			** Do this only here, not in reopen, because we don't want
			** to do this costly operation too often.
			*/
			activation.informOfRowCount(this, (long) hashtableSize);
		}

	    //isOpen = true;

		resetProbeVariables();

		numOpens++;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
	 * reopen this ResultSet.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException {
		TransactionController		tc;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen,
					"HashScanResultSet already open");
		}

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		resetProbeVariables();

		numOpens++;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	private void resetProbeVariables() throws StandardException
	{
	        // GemStone changes BEGIN
                rowsSeen += numFetchedOnNext;
	        // GemStone changes END
		firstNext = true;
		numFetchedOnNext = 0;
		entryVector = null;
		// GemStone changes END
		rakVector = null;
		// GemStone changes END
		entryVectorSize = 0;

		if (nextQualifiers != null)
		{
			clearOrderableCache(nextQualifiers);
		}
	}


	/**
     * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
	    checkCancellationFlag();
	    ExecRow result = null;
		DataValueDescriptor[] columns = null;
		// GemStone changes BEGIN
                Object rak = null;
                // GemStone changes END
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen && hashtableBuilt)
	    {
			/* We use a do/while loop to ensure that we continue down
			 * the duplicate chain, if one exists, until we find a
			 * row that matches on all probe predicates (or the
			 * duplicate chain is exhausted.)
			 */
			do 
			{
				if (firstNext)
				{			  
					firstNext = false;

					/* Hash key could be either a single column or multiple columns.
					 * If a single column, then it is the datavalue wrapper, otherwise
					 * it is a KeyHasher.
					 */
					Object hashEntry;
					if (keyColumns.length == 1)
					{
						hashEntry = hashtable.get(nextQualifiers[0][0].getOrderable());
						// GemStone changes BEGIN
						rak = hashtable.getFromRegionAndKeyHash(nextQualifiers[0][0].getOrderable());
                                                if (GemFireXDUtils.TraceOuterJoin) {
                                                  SanityManager.DEBUG_PRINT(
                                                      GfxdConstants.TRACE_OUTERJOIN_MERGING,
                                                      "HashScanResultSet::getNextRowCore_one hashEntry: "
                                                          + hashEntry
                                                          + ((hashEntry != null && hashEntry instanceof DataValueDescriptor[]) ? (Arrays
                                                              .toString(((DataValueDescriptor[])hashEntry)))
                                                              : "null") + " and rak: " + rak);
                                                }
						// GemStone changes END
					}
					else
					{
						KeyHasher mh = new KeyHasher(keyColumns.length);

                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(nextQualifiers.length == 1);
                        }

						for (int index = 0; index < keyColumns.length; index++)
						{
                            // For hashing only use the AND qualifiers 
                            // located in nextQualifiers[0][0...N], OR 
                            // qualifiers are checked down a bit by calling
                            // qualifyRow on rows returned from hash.

                            DataValueDescriptor dvd = 
                                nextQualifiers[0][index].getOrderable();

                            if (dvd == null)
                            {
                                mh = null;
                                break;
                            }
							mh.setObject(
                                index, nextQualifiers[0][index].getOrderable());
						}
						// GemStone changes BEGIN
						//hashEntry = (mh == null) ? null : hashtable.get(mh);
						if (mh == null) {
						  hashEntry = null;
						}
						else {
						  hashEntry = hashtable.get(mh);
                                                  if (GemFireXDUtils.TraceOuterJoin) {
                                                    SanityManager
                                                        .DEBUG_PRINT(
                                                            GfxdConstants.TRACE_OUTERJOIN_MERGING,
                                                            "HashScanResultSet::getNextRowCore_two hashEntry: "
                                                                + hashEntry
                                                                + ((hashEntry != null && hashEntry instanceof DataValueDescriptor[]) ? (Arrays
                                                                    .toString(((DataValueDescriptor[])hashEntry)))
                                                                    : "null") + " and rak: " + rak);
                                                  }
						  rak = hashtable.getFromRegionAndKeyHash(mh);
						}
						// GemStone changes END
					}

					if (hashEntry instanceof List)
					{
						entryVector = (List) hashEntry;
						entryVectorSize = entryVector.size();
						columns = 
                            (DataValueDescriptor[]) entryVector.get(0);
						// GemStone changes BEGIN
						if (rak != null) {
						  assert rak instanceof List;
						  rakVector = (List)rak;
						  rak = rakVector.get(0);
						}
						// GemStone changes END
					}
					else
					{
						entryVector = null;
						// GemStone changes BEGIN
						rakVector = null;
						// GemStone changes END
						entryVectorSize = 0;
						columns = (DataValueDescriptor[]) hashEntry;
					}
				}
				else if (numFetchedOnNext < entryVectorSize)
				{
					// We are walking a list and there are more rows left.
					columns = (DataValueDescriptor[]) 
                        entryVector.get(numFetchedOnNext);
					// GemStone changes BEGIN
					if (rakVector != null) {
					  rak = (RegionAndKey)rakVector.get(numFetchedOnNext);
					}
					// GemStone changes END
				}

				if (columns != null)
				{

					// See if the entry satisfies all of the other qualifiers

					/* We've already "evaluated" the 1st keyColumns qualifiers 
                     * when we probed into the hash table, but we need to 
                     * evaluate them again here because of the behavior of 
                     * NULLs.  NULLs are treated as equal when building and 
                     * probing the hash table so that we only get a single 
                     * entry.  However, NULL does not equal NULL, so the 
                     * compare() method below will eliminate any row that
					 * has a key column containing a NULL.
                     *
                     * The following code will also evaluate any OR clauses
                     * that may exist, while the above hashing does not 
                     * include them.
					 */

					if (RowUtil.qualifyRow(columns, null /* GemStone addition */, nextQualifiers, false))
					{
					  // GemStone changes BEGIN
					  if (compactRow != null) {
					    compactRow.clearAllRegionAndKeyInfo();
					  }
					  // GemStone changes END
						setCompactRow(compactRow, columns);

						rowsSeen++;

						result = compactRow;
					}
					else
					{
						result = null;
					}

					numFetchedOnNext++;
				}
				else
				{
					result = null;
				}
			}
			while (result == null && numFetchedOnNext < entryVectorSize);

		}

		setCurrentRow(result);

// GemStone changes BEGIN
		if (GemFireXDUtils.TraceOuterJoin) {
                  SanityManager.DEBUG_PRINT(
                      GfxdConstants.TRACE_OUTERJOIN_MERGING,
                      "HashScanResultSet::getNextRowCore_one current row set with row: " + result + " and rak: " + rak);
                }
		if (result != null) {
		  if (rak != null) {
		    assert rak instanceof RegionAndKey;
		    result.addRegionAndKey((RegionAndKey)rak);
		  }
		  if (this.localTXState != null && this.isTopResultSet &&
		      isForUpdate()) {
		    updateRowLocationPostRead();
		  }
		}
// GemStone changes END
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

			if (hashtableBuilt)
			{
				// This is where we get the scan properties for a subquery
				scanProperties = getScanProperties();
				// This is where we get the positioner info for inner tables
				if (runTimeStatisticsOn)
				{
					startPositionString = printStartPosition();
					stopPositionString = printStopPosition();
				}

				// close the hash table, eating any exception
				hashtable.close();
				hashtable = null;
				hashtableBuilt = false;
			}
			startPosition = null;
			stopPosition = null;

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of HashScanResultSet repeated");

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
	        final long time = PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);

		/* RESOLVE - subtract out store time later, when available */
		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	time;
		}
		else
		{
                      // GemStone changes BEGIN
                      return timeType == ALL ? (time - constructorTime) : time;
                     /*(original code) return totTime; */
                     // GemStone changes END
		}
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		// IndexRowToBaseRow needs to relock if we didn't keep the lock
		return(
            ((isolationLevel == 
                 TransactionController.ISOLATION_READ_COMMITTED)            ||
             (isolationLevel == 
                 TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK) ||
             (isolationLevel == 
                 TransactionController.ISOLATION_READ_UNCOMMITTED)));

	}

	//
	// CursorResultSet interface
	//

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

		if ( ! hashtableBuilt)
			return null;

		/* This method should only be called if the last column
		 * in the current row is a RowLocation.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(currentRow != null,
			  "There must be a current row when fetching the row location");
			Object rlCandidate =  currentRow.getLastColumn();
			if (! (rlCandidate instanceof RowLocation))
			{
				SanityManager.THROWASSERT(
					"rlCandidate expected to be instanceof RowLocation, not " +
					rlCandidate.getClass().getName());
			}
		}

		return (RowLocation) currentRow.getLastColumn();
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
		/* Doesn't make sense to call this method for this node since
		 * joins are not updatable.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT( 
			 "getCurrentRow() not expected to be called for HSRS");
		}

		return null;
	}

	public String printStartPosition()
	{
		return printPosition(startSearchOperator, startKeyGetter, startPosition);
	}

	public String printStopPosition()
	{
		if (sameStartStopPosition)
		{
			return printPosition(stopSearchOperator, startKeyGetter, startPosition);
		}
		else
		{
			return printPosition(stopSearchOperator, stopKeyGetter, stopPosition);
		}
	}

	/**
	 * Return a start or stop positioner as a String.
	 */
	private String printPosition(int searchOperator,
								 GeneratedMethod positionGetter,
								 ExecIndexRow eiRow)
	{
		String idt = "";

		String output = "";
		if (positionGetter == null)
		{
			return "\t" +
					MessageService.getTextMessage(SQLState.LANG_NONE) +
					"\n";
		}

		ExecIndexRow	positioner = null;

		try
		{
			positioner = (ExecIndexRow) positionGetter.invoke(activation);
		}
		catch (StandardException e)
		{

			if (eiRow == null)
			{
				return "\t" + MessageService.getTextMessage(
											SQLState.LANG_POSITION_NOT_AVAIL);
			}
			return "\t" + MessageService.getTextMessage(
							SQLState.LANG_UNEXPECTED_EXC_GETTING_POSITIONER) +
							"\n";
		}

		if (positioner == null)
		{
			return "\t" +
					MessageService.getTextMessage(SQLState.LANG_NONE) +
					"\n";
		}

		String searchOp = null;

		switch (searchOperator)
		{
			case ScanController.GE:
				searchOp = ">=";
				break;

			case ScanController.GT:
				searchOp = ">";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Unknown search operator " +
												searchOperator);
				}

				// This is not internationalized because we should never
				// reach here.
				searchOp = "unknown value (" + searchOperator + ")";
				break;
		}

		output += "\t" + MessageService.getTextMessage(
										SQLState.LANG_POSITIONER,
										searchOp,
										String.valueOf(positioner.nColumns()))
										+ "\n";
			
		output += "\t" + MessageService.getTextMessage(
										SQLState.LANG_ORDERED_NULL_SEMANTICS) +
										"\n";
		for (int position = 0; position < positioner.nColumns(); position++)
		{
			if (positioner.areNullsOrdered(position))
			{
				output = output + position + " ";
			}
		}
		
		return output + "\n";
	}

	public Properties getScanProperties()
	{
		return scanProperties;
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return forUpdate;
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    upgradeReadLockToWrite(getRowLocation(), null);
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    if(localTXState != null) {
      releaseRowLocationLock(getRowLocation(), null);
    }
  }

  @Override
  public void accept(
      ResultSetStatisticsVisitor visitor) {
    
    visitor.setNumberOfChildren(0);
    
    visitor.visit(this);
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {

    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_HASHSCAN);

    PlanUtils.xmlCloseTag(builder, context, this);
    return builder;
    
  }
// GemStone changes END
}
