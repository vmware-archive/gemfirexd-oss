/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.BulkTableScanResultSet

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
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController;
import com.pivotal.gemfirexd.internal.engine.access.index.SortedMap2IndexScanController;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OHAddressCache;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.GroupFetchScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Read a base table or index in bulk.  Most of the
 * work for this method is inherited from TableScanResultSet.
 * This class overrides getNextRowCore (and extends 
 * re/openCore) to use a row array and fetch rows
 * from the Store in bulk  (using fetchNextGroup).
 * <p>
 * Since it retrieves rows in bulk, locking is not
 * as is usual -- locks may have already been released
 * on rows as they are returned to the user.  Hence,
 *this ResultSet is not suitable for a query running
 * Isolation Level 1, cursor stability.
 * <p>
 * Note that this code is only accessable from an
 * optimizer override.  If it makes sense to have the
 * optimizer select bulk reads, then this should
 * probably be rolled into TableScanResultSet.
 *
 */
final class BulkTableScanResultSet extends TableScanResultSet
	implements CursorResultSet
{
// GemStone changes BEGIN
  // changed rowArray from DataValueDescriptor[][] to ExecRow[]
	private ExecRow[] rowArray;
	private MemHeapScanController mhsc;
	private GemFireContainer gfc;
	private final RowLocation[] rowLocationArray;
	private SortedMap2IndexScanController smScanController;
	private Object[] indexKeys;
	private int[] nodeVersions;
	private int[] scanKeyGroupID;

	/**
	 * The RowFormatter for the accessedCols. This will probably need to be
	 * moved up to a superclass.
	 */
	private RowFormatter rfForAccessedCols;

	private final StatementStats stats;

	@Override
	protected void initLocalTXState(final TXState localTXState,
	    final boolean forUpdate) {
	  super.initLocalTXState(localTXState, forUpdate);
	}
// GemStone changes END
	private int curRowPosition;
	private int numRowsInArray;


	private static int OUT_OF_ROWS = 0;

    /**
 	 * Constructor.  Just save off the rowsPerRead argument
	 * and pass everything else down to TableScanResultSet
	 * 
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory#getBulkTableScanResultSet
	 *
	 * @exception StandardException thrown on failure to open
	 */
    BulkTableScanResultSet(long conglomId,
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] qualifiers,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int indexColItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		int rowsPerRead,
		boolean oneRowScan,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost, StatementStats stats, 
		//GemStone changes BEGIN
		boolean delayScanOpening,
		boolean optimizeforOffHeap,
		boolean indexAccesesBaseTable,
		boolean supportsMoveToNextKey,
		String nonQualPreds   // not used currently
		//GemStone changes END	
        )
			throws StandardException
    {
// GemStone changes BEGIN
      this(conglomId, scoci, activation, (ExecRow)resultRowAllocator
          .invoke(activation), resultRowAllocator, resultSetNumber,
          startKeyGetter, startSearchOperator, stopKeyGetter, stopSearchOperator,
          sameStartStopPosition, qualifiers, tableName,
          userSuppliedOptimizerOverrides, indexName, isConstraint, forUpdate,
          colRefItem != -1 ? (FormatableBitSet)(activation.getSavedObject(colRefItem)) : null, indexColItem, lockMode,
          tableLocked, isolationLevel, rowsPerRead, oneRowScan,
          optimizerEstimatedRowCount, optimizerEstimatedCost, stats,
        //GemStone changes BEGIN
          delayScanOpening,
          optimizeforOffHeap,
          indexAccesesBaseTable,
          supportsMoveToNextKey,
          nonQualPreds
        //GemStone changes END
          );
      /* (original code)
		super(conglomId,
			scoci,
			activation,
			resultRowAllocator,
			resultSetNumber,
			startKeyGetter,
			startSearchOperator,
			stopKeyGetter,
			stopSearchOperator,
			sameStartStopPosition,
			qualifiers,
			tableName,
			userSuppliedOptimizerOverrides,
			indexName,
			isConstraint,
			forUpdate,
			colRefItem,
			indexColItem,
			lockMode,
			tableLocked,
			isolationLevel,
			rowsPerRead,
			oneRowScan,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost);

		if (SanityManager.DEBUG)
		{
			/* Bulk fetch of size 1 is same as a regular table scan
			 * and is supposed to be detected at compile time.
			 *
			if (rowsPerRead == 1)
			{
				SanityManager.THROWASSERT(
					"rowsPerRead not expected to be 1");
			}
			/* Bulk table scan implies that scan is not
			 * a 1 row scan.
			 *
			if (oneRowScan)
			{
				SanityManager.THROWASSERT(
					"oneRowScan expected to be false - " +
					"rowsPerRead = " + rowsPerRead);
			}
		}
      */
    }

    /**
     * Constructor. Just save off the rowsPerRead argument and pass everything
     * else down to TableScanResultSet
     * 
     * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory#
     *          getBulkTableScanResultSet
     * 
     * @exception StandardException
     *              thrown on failure to open
     */
    BulkTableScanResultSet(long conglomId, StaticCompiledOpenConglomInfo scoci,
        Activation activation, ExecRow candidate,
        GeneratedMethod resultRowAllocator, int resultSetNumber,
        GeneratedMethod startKeyGetter, int startSearchOperator,
        GeneratedMethod stopKeyGetter, int stopSearchOperator,
        boolean sameStartStopPosition, Qualifier[][] qualifiers,
        String tableName, String userSuppliedOptimizerOverrides,
        String indexName, boolean isConstraint, boolean forUpdate,
        FormatableBitSet accessedCols, int indexColItem, int lockMode,
        boolean tableLocked, int isolationLevel, int rowsPerRead,
        boolean oneRowScan, double optimizerEstimatedRowCount,
        double optimizerEstimatedCost, StatementStats stats, 
      //GemStone changes BEGIN
        boolean delayScanOpening,
        boolean optimizeForOffHeap,
        boolean indexAccessesBaseTable,
        boolean supportsMoveToNextKey,
        String nonQualPreds
      //GemStone changes END
        )
        throws StandardException {
      super(conglomId, scoci, activation, candidate, resultRowAllocator,
          resultSetNumber, startKeyGetter, startSearchOperator, stopKeyGetter,
          stopSearchOperator, sameStartStopPosition, qualifiers, tableName,
          userSuppliedOptimizerOverrides, indexName, isConstraint, forUpdate,
          accessedCols, indexColItem, lockMode, tableLocked, isolationLevel,
          rowsPerRead, oneRowScan, optimizerEstimatedRowCount,
          optimizerEstimatedCost,
        //GemStone changes BEGIN
          delayScanOpening, optimizeForOffHeap, indexAccessesBaseTable,
          supportsMoveToNextKey,nonQualPreds
        //GemStone changes END
          );

      if (SanityManager.DEBUG) {
        /* Bulk fetch of size 1 is same as a regular table scan
         * and is supposed to be detected at compile time.
         */
        if (rowsPerRead == 1) {
          SanityManager.THROWASSERT("rowsPerRead not expected to be 1");
        }
        /* Bulk table scan implies that scan is not
         * a 1 row scan.
         */
        if (oneRowScan) {
          SanityManager.THROWASSERT("oneRowScan expected to be false - "
              + "rowsPerRead = " + rowsPerRead);
        }
      }
      this.stats = stats;
      this.rowLocationArray = new RowLocation[this.rowsPerRead];
    }

// GemStone changes END
    /**
 	 * Open the scan controller
	 *
	 * @param tc transaction controller will open one if null
     *
	 * @exception StandardException thrown on failure to open
	 */
	protected void openScanController(TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor[] startPositionRow = startPosition == null ? null : startPosition.getRowArray();
		DataValueDescriptor[] stopPositionRow = stopPosition == null ? null : stopPosition.getRowArray();

		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			clearOrderableCache(qualifiers);
		}

		// Get the current transaction controller
		if (tc == null)
			tc = activation.getTransactionController();
		
		// Gemstone changes BEGIN.
		/*scanController = tc.openCompiledScan(
				activation.getResultSetHoldability(),
				(forUpdate ? TransactionController.OPENMODE_FORUPDATE : 0),
                lockMode,
                isolationLevel,
				accessedCols,
				startPositionRow,
					// not used when giving null start position
				startSearchOperator,
				qualifiers,
				stopPositionRow,
					// not used when giving null stop position
				stopSearchOperator,
				scoci,
				dcoci);*/
		
		scanController = tc.openCompiledScan(
        activation.getResultSetHoldability(),
        (forUpdate() ? TransactionController.OPENMODE_FORUPDATE : 0),
                lockMode,
                isolationLevel,
        accessedCols,
        startPositionRow,
          // not used when giving null start position
        startSearchOperator,
        qualifiers,
        stopPositionRow,
          // not used when giving null stop position
        stopSearchOperator,
        scoci,
        dcoci, activation);
		// Gemstone changes END.

		/* Remember that we opened the scan */
		this.setScanControllerOpened(true);
		//scanControllerOpened = true;

		rowsThisScan = 0;

		/*
		** Inform the activation of the estimated number of rows.  Only
		** do it here, not in reopen, so that we don't do this costly
		** check too often.
		*/
		activation.informOfRowCount(
									this,
									scanController.getEstimatedRowCount()
									);
		if (scanController instanceof SortedMap2IndexScanController) {
		  this.smScanController =
		      (SortedMap2IndexScanController)scanController;
		  this.indexKeys = new Object[this.rowsPerRead];
		  this.nodeVersions = new int[this.rowsPerRead];
		}else if(scanController instanceof MemHeapScanController
		    && ((MemHeapScanController)this.scanController).getGemFireContainer()
		    .isOffHeap()) {
		  ((MemHeapScanController)this.scanController).setOffHeapOwner(this);
		}
	}

	/**
	 * Open up the result set.  Delegate
	 * most work to TableScanResultSet.openCore().
	 * Create a new array with <rowsPerRead> rows
	 * for use in fetchNextGroup().
	 *
	 * @exception StandardException thrown on failure to open
	 */
	public void openCore() throws StandardException
	{
		super.openCore();

// GemStone changes BEGIN
		/*
		** Add the extra time we spent after
		** the super class -- TableScanResultSet()
		** already added up its time in openCore().
		*/
		final long lbeginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0; 
                if (this.scanController instanceof MemHeapScanController) {
                  this.mhsc = (MemHeapScanController)this.scanController;
                }
		rowArray = new ExecRow[rowsPerRead];

		// we only allocate the first row -- the
		// store clones as needed for the rest
		// of the rows
    
    // for now, only handling the base table scan case
    if (this.mhsc != null) {
      final GemFireContainer gfc = this.mhsc.getGemFireContainer();
      this.gfc = gfc;
      if (gfc.isByteArrayStore()) {
        // default candidate is a ValueRow...
        // change here to be a CompactExecRow
        // (throws away current candidate, not optimal but when it was
        // created there was no table reference to determine
        // if byte array store or not).
        // The dvds in the candidate may not be the complete row, but is not
        // limited to the accessedCols either, and has the "shape" of the
        // underlying table, so this is a case where the number of dvds may
        // not match the number of column descriptors. To determine the correct
        // column descriptors, truncate to the number of dvds.
        DataValueDescriptor[] candidateRowArray = this.candidate.getRowArray();
        /*this.candidate = this.gfc.getRowFormatter(candidateRowArray.length)
            .newCompactExecRow(candidateRowArray);*/
        final RowFormatter rf = gfc.getRowFormatter(candidateRowArray.length);
        this.candidate = gfc.newCompactExecRow(candidateRowArray, rf);
      }
    }
    this.rowArray[0] = this.candidate.getClone();
// GemStone changes END
    
		numRowsInArray = 0;
		curRowPosition = -1;
		
		if (statisticsTimingOn) openTime += getElapsedNanos(lbeginTime);
	}

	/**
	 * Reopen the result set.  Delegate
	 * most work to TableScanResultSet.reopenCore().
	 * Reuse the array of rows.
	 *
	 * @exception StandardException thrown on failure to open
	 */
	public void reopenCore() throws StandardException
	{
		super.reopenCore();
		numRowsInArray = 0;
		curRowPosition = -1;
	}
		
	/**
	 * Return the next row (if any) from the scan (if open).
	 * Reload the rowArray as necessary.
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public final ExecRow getNextRowCore() throws StandardException
	{	
		final long lbeginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	//GemStone changes BEGIN
    if (!this.isOpen && this.delayScanOpening()) {
      if (numOpens == 0) {
        this.basicOpenCore();
      } else {
        this.basicReopenCore();
      }
    }
  //GemStone changes END
// GemStone changes BEGIN
		ExecRow result = null;
		if (this.observer != null) {
		  this.observer.onGetNextRowCoreOfBulkTableScan(this);
		}
		checkCancellationFlag();
		
		final TXState localTXState = this.localTXState;
// GemStone changes END
		if (isOpen && (this.varflags & MASK_SCAN_CONTROLLER_OPENED) != 0)
		{
			if (currentRow == null)
			{
// GemStone changes BEGIN
        // useBytes for the compactRow only if this is a scan on a base table
        // that is storing rows as byte arrays
        boolean useBytes = false;
        if (this.mhsc != null) {
          useBytes = this.gfc.isByteArrayStore();
          if (useBytes && this.rfForAccessedCols == null) {
            this.rfForAccessedCols = this.gfc.getRowFormatter(accessedCols);
            if (accessedCols == null) {
              this.baseColumnMap = null;
            }
          }
        }
				currentRow =
					getCompactRow(candidate,
									accessedCols,
									isKeyed(),
                  useBytes,
                  this.gfc,
                  this.rfForAccessedCols,
                  false);
// GemStone changes END
			}

outer:		for (;;)
			{
				if (curRowPosition >= numRowsInArray - 1)
				{
					if (reloadArray() == OUT_OF_ROWS)
					{
						clearCurrentRow();
						setRowCountIfPossible(rowsThisScan);
                                                if (statisticsTimingOn) nextTime += getElapsedNanos(lbeginTime);
						return null;
					}
				}	

				while (++curRowPosition < numRowsInArray)
				{
					candidate.setRowArray(rowArray[curRowPosition]);
					currentRow = setCompactRow(candidate, currentRow);
					/*
					// GemStone changes BEGIN
					// Neeraj: clear existing if any
					currentRow.clearAllRegionAndKeyInfo();
					TreeSet<RegionAndKey> rakSet = rowArray[curRowPosition].getAllRegionAndKeyInfo();
					currentRow.addAllKeys(rakSet);
					// GemStone changes END
					*/
					rowsSeen++;
					rowsThisScan++;

					/*
					** Skip rows where there are start or stop positioners
					** that do not implement ordered null semantics and
					** there are columns in those positions that contain
					** null.
					*/
					if (skipRow(candidate))
					{
						rowsFiltered++;
// GemStone changes BEGIN
						
						// release lock taken by the ScanController
						//if (localTXState != null) {
						  filteredRowLocationPostRead(localTXState );
						//}
// GemStone changes END
						continue;
					}

					result = currentRow;
					break outer;
				}
			}
		}

		setCurrentRow(result);
// GemStone changes BEGIN
		setRegionAndKeyInfo(result);
		final RowLocation rl;
		if (localTXState != null && this.isTopResultSet &&
		    this.forUpdate() && result != null &&
		    (rl = this.rowLocationArray[this.curRowPosition]) != null) {
		  upgradeReadLockToWrite(rl, this.gfc);
		}
// GemStone changes END
		if (statisticsTimingOn) nextTime += getElapsedNanos(lbeginTime);
	    return result;
	}

	/*
	** Load up rowArray with a batch of
	** rows.
	*/
	private int reloadArray() throws StandardException
	{
		curRowPosition = -1;
		numRowsInArray =
				((GroupFetchScanController) scanController).fetchNextGroup(
// GemStone changes BEGIN
                                               rowArray, rowLocationArray,
                                               indexKeys, nodeVersions,
                                               scanKeyGroupID, null);
                                               /* (original code)
                                               rowArray, (RowLocation[]) null, 0);
                                               */
// GemStone changes END
		if (this.stats != null) {
		  this.stats.incNumReloadArray();
		  this.stats.incNumRowsLoaded(this.numRowsInArray);
		}
		return numRowsInArray;

	}
	/**
	 * If the result set has been opened,
	 * close the open scan.  Delegate most
	 * of the work to TableScanResultSet.
	 *
	 * @exception StandardException on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		/*
		** We'll let TableScanResultSet track
		** the time it takes to close up, so
		** no timing here.
		*/
	
	  
		super.close(cleanupOnError);
		numRowsInArray = -1;
		curRowPosition = -1;
		rowArray = null;
// GemStone changes BEGIN
		if (this.scanKeyGroupID != null) {
		  this.scanKeyGroupID = null;
		}
// GemStone changes END
	}

	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	protected boolean canGetInstantaneousLocks()
	{
		return !forUpdate();
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		// IndexRowToBaseRow needs to relock if we didn't keep the lock
		return(
          isolationLevel == TransactionController.ISOLATION_READ_COMMITTED   ||
          isolationLevel == TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK ||
          isolationLevel == TransactionController.ISOLATION_READ_UNCOMMITTED);
	}
// GemStone changes BEGIN

  @Override
  public RowLocation getRowLocation() throws StandardException {
    if (this.isOpen && this.scanControllerOpened()) {
      return this.rowLocationArray[this.curRowPosition];
    }
    return null;
  }

  @Override
  public final void updateRowLocationPostRead() throws StandardException {
    if (this.scanControllerOpened()) {
      upgradeReadLockToWrite(this.rowLocationArray[this.curRowPosition],
          this.gfc);
    }
  }

  @Override
  public final void filteredRowLocationPostRead(TXState localTXState)
      throws StandardException {
    if (localTXState != null && this.scanControllerOpened()
        && numRowsInArray != OUT_OF_ROWS) {
      releaseRowLocationLock(this.rowLocationArray[this.curRowPosition],
          this.gfc);
    }
    if (this.ohAddressCache != null) {
      this.basicReleasePreviousByteSource();
    }
  }

  private void basicReleasePreviousByteSource() {
    if (isHeapScan()) {
      if (this.candidate != null && numRowsInArray != OUT_OF_ROWS
          && curRowPosition != -1) {
        this.releaseByteSource(this.numRowsInArray - curRowPosition - 1);
      }
    }
    else {
      this.releaseByteSource(0);
    }
  }

  @Override
  public void releasePreviousByteSource() {
    if (this.ohAddressCache != null
        && (this.finalFlags & MASK_OPTIMIZE_FOR_OFFHEAP) != 0) {
      this.basicReleasePreviousByteSource();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsMoveToNextKey() {
    if (super.supportsMoveToNextKey()) {
      
      if (this.scanKeyGroupID == null) {
        this.scanKeyGroupID = new int[this.rowsPerRead];
      }
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScanKeyGroupID() {
    if (this.scanControllerOpened()) {
      if (this.curRowPosition >= 0) {
        return this.scanKeyGroupID[this.curRowPosition];
      }
      else {
        return -1;
      }
    }
    else {
      throw new UnsupportedOperationException("not expected to be invoked");
    }
  }

  @Override
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn,
      GemFireContainer container) throws StandardException {
    if (this.indexKeys != null) {
      return RowUtil.fetch(loc, destRow, validColumns, faultIn, container,
          this.smScanController, this.indexKeys[this.curRowPosition],
          this.nodeVersions[this.curRowPosition], this);
    }
    else {
      return RowUtil.fetch(loc, destRow, validColumns, faultIn, container,
          null, null, 0, this);
    }
  }
  
  protected OHAddressCache createOHAddressCache() {
    if (isOHAddressCacheNeeded()) {
      int memConglomType = ((MemConglomerate) scoci).getType();
      if (memConglomType == MemConglomerate.HASH1INDEX
          || memConglomType == MemConglomerate.SORTEDMAP2INDEX) {
        return super.createOHAddressCache();
      } else {
        if (optimizedForOffHeap()) {
          return new BatchOHAddressCache();
        } else {
          return GemFireTransaction.createOHAddressCache();
        }
      }
    } else {
      return null;
    }
  }

  protected final class BatchOHAddressCache implements OHAddressCache {
    private final long[] addresses ;
    private int curPos = 0 ;
    
    public BatchOHAddressCache() {
      addresses = new long[rowsPerRead];     
    }
    
    @Override
    public void put(long address) {      
      if(this.addresses[curPos] != 0) {
        throw new IllegalStateException("Cached address =" + this.addresses[curPos] 
            + " is unreleased");      
      }else if(curPos >= rowsPerRead) {
        throw new IllegalStateException("BatchOHAdress has previous unreleased positions " +
        		"causing no space to accomodate address="+ address);        
      }
      this.addresses[curPos] = address;
      ++ curPos;
    }

    @Override
    public void releaseByteSource(int positionFromEnd) {
      int posInArray = numRowsInArray - positionFromEnd -1;
      this.releaseAddressAndReset(posInArray);
      if(posInArray == numRowsInArray -1) {
        this.curPos = 0;
      }
    }

    @Override
    public void release() {
      for(int i = 0 ; i < rowsPerRead; ++i) {
        this.releaseAddressAndReset(i);
      }    
      this.curPos = 0;
    }
    
    private void releaseAddressAndReset(int position) {
      long address = this.addresses[position];
      if(address != 0) {
        Chunk.release(address, true);
        this.addresses[position] = 0;
      }
    }
  }
// GemStone changes END
}
