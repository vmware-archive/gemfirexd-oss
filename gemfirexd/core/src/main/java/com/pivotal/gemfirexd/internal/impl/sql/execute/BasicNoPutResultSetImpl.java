/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.BasicNoPutResultSetImpl

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

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXEntryState;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.locks.LockingPolicy;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.Row;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Abstract ResultSet for for operations that return rows but
 * do not allow the caller to put data on output pipes. This
 * basic implementation does not include support for an Activiation.
 * See NoPutResultSetImpl.java for an implementaion with support for
 * an activiation.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 */
//GemStone changes BEGIN
public abstract class BasicNoPutResultSetImpl
implements NoPutResultSet
//GemStone changes END
{
	/* Modified during the life of this object */
    /**
     * basic policy with this flag is to set it at the very start of openCore
     * so that any subsequent exception in openCore or otherwise will invoke
     * cleanup+close that will see the isOpen flag and attempt as much
     * cleanup as possible (#49479, #49485)
     */
    protected boolean isOpen;
    protected boolean finished;
	protected ExecRow	  currentRow;
	protected boolean isTopResultSet;
	private SQLWarning	warnings;

// GemStone changes BEGIN
	protected final GemFireXDQueryObserver observer =
	    GemFireXDQueryObserverHolder.getInstance();
	protected final LanguageConnectionContext lcc;
	protected boolean localTXStateSet;
	protected TXState localTXState;
	protected Object lockContext;
        protected UUID executionPlanID;
        protected boolean isLocallyExecuted = false;
        protected boolean hasLockReference;
// GemStone changes END
	/* Run time statistics variables */
	public int numOpens;
	public int rowsSeen;
	public int rowsFiltered;
	protected long startExecutionTime;
	protected long endExecutionTime;
	public volatile long beginTime;
	public long constructorTime;
	public long openTime;
	public long nextTime;
	public long closeTime;

	public double optimizerEstimatedRowCount;
	public double optimizerEstimatedCost;

	// set on demand during execution
	private StatementContext			statementContext;
	public NoPutResultSet[]			subqueryTrackingArray;
	ExecRow compactRow;

	// Set in the constructor and not modified
	protected final Activation	    activation;
	protected boolean				statisticsTimingOn;
	// GemStone changes BEGIN
        protected boolean                               runtimeStatisticsOn;
        protected boolean                               explainConnection;
        protected boolean                               statsEnabled;
	// GemStone changes END

	private transient TransactionController	tc;

	protected int[] baseColumnMap;

	/**
	 *  Constructor.
	    <BR>
		Sets beginTime for all children to use to measue constructor time.
	 *
	 *	@param	activation			The activation
	 *	@param	optimizerEstimatedRowCount	The optimizer's estimate of the
	 *										total number of rows for this
	 *										result set
	 *	@param	optimizerEstimatedCost		The optimizer's estimated cost for
	 *										this result set
	 */
	protected BasicNoPutResultSetImpl(Activation activation,
							double optimizerEstimatedRowCount,
							double optimizerEstimatedCost)
	{
                statisticsTimingOn = activation.getLanguageConnectionContext()
                    .getStatisticsTiming();
                if (statisticsTimingOn) {
                  beginTime = XPLAINUtil.nanoTime();
                  startExecutionTime = XPLAINUtil.currentTimeMillis();
                }
		this.activation = activation;
		this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
		this.optimizerEstimatedCost = optimizerEstimatedCost;
// GemStone changes BEGIN
		this.lcc = activation.getLanguageConnectionContext();
                if (isTopResultSet)
                {
                  runtimeStatisticsOn = lcc.getRunTimeStatisticsMode();
                  explainConnection = lcc.explainConnection();
                  statsEnabled = lcc.statsEnabled();
                }
// GemStone changes END
	}
	
	/**
	 * Allow sub-classes to record the total
	 * time spent in their constructor time.
	 *
	 */
	protected final void recordConstructorTime()
	{
		if (statisticsTimingOn)
		    constructorTime = getElapsedNanos(beginTime);
	}
	
	public final Activation getActivation()
	{
		return activation;
	}

	// NoPutResultSet interface

	/**
	 * This is the default implementation of reopenCore().
	 * It simply does a close() followed by an open().  If
	 * there are optimizations to be made (caching, etc), this
	 * is a good place to do it -- this will be overridden
	 * by a number of resultSet imlementations.  and SHOULD
	 * be overridden by any node that can get between a base
	 * table and a join.
	 *
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public void reopenCore() throws StandardException
	{
		close(false);
		openCore();	
	}

	/**
	 * @see NoPutResultSet#getNextRowCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public abstract ExecRow	getNextRowCore() throws StandardException;

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getPointOfAttachment() not expected to be called for " +
				getClass().getName());
		}
		return -1;
	}

	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 */
	public void markAsTopResultSet()
	{
		isTopResultSet = true;
	}

	/**
	 * @see NoPutResultSet#getScanIsolationLevel
	 */
	public int getScanIsolationLevel()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getScanIsolationLevel() not expected to be called for " +
				getClass().getName());
		}
		return 0;
	}

	/** @see NoPutResultSet#getEstimatedRowCount */
	public double getEstimatedRowCount()
	{
		return optimizerEstimatedRowCount;
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

	// ResultSet interface

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * NOTE: This method should only be called on the top ResultSet
	 * of a ResultSet tree to ensure that the entire ResultSet
	 * tree gets closed down on an error.  the openCore() method
	 * will be called for all other ResultSets in the tree.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public final void	open() throws StandardException 
	{
		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
				SanityManager.THROWASSERT(
				this + "expected to be the top ResultSet");
		}
// GemStone changes BEGIN
		if (this.isOpen && isDistributedResultSet()) {
		  return;
		}
                statisticsTimingOn = lcc.getStatisticsTiming();
                if(statisticsTimingOn && startExecutionTime == 0) {
                  startExecutionTime = XPLAINUtil.currentTimeMillis();
                }
                if (isTopResultSet)
                {
                  runtimeStatisticsOn = lcc.getRunTimeStatisticsMode();
                  explainConnection = lcc.explainConnection();
                  statsEnabled = lcc.statsEnabled();
                }
                AbstractGemFireResultSet gfSource = null;
                if (observer != null) {
                  if ((gfSource = getWrappedGemFireRS()) != null) {
                    observer.beforeGemFireResultSetOpen(gfSource, lcc);
                  }
                }
// GemStone changes END

		finished = false;

		attachStatementContext();
	       


		try {

// GemStone changes BEGIN
		  if (this.isOpen) {
		    reopenCore();
		  }
		  else
// GemStone changes END
			openCore();

		} catch (StandardException se) {
			activation.checkStatementValidity();
			throw se;
		}

		activation.checkStatementValidity();
// GemStone changes BEGIN
		if (observer != null) {
		  if (gfSource != null) {
		    observer.afterGemFireResultSetOpen(gfSource, lcc);
		  }
		}
// GemStone changes END
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, ABSOLUTE);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getAbsoluteRow() not expected to be called for " + getClass().getName());
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, RELATIVE);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getRelativeRow() not expected to be called for " + getClass().getName());
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, FIRST);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"setBeforeFirstRow() not expected to be called for " + getClass().getName());
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
    public boolean checkRowPosition(int isType) throws StandardException
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, FIRST);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getFirstRow() not expected to be called for " + getClass().getName());
		}

		return null;
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * NOTE: This method should only be called on the top ResultSet
	 * of a ResultSet tree to ensure that the entire ResultSet
	 * tree gets closed down on an error.  the getNextRowCore() method
	 * will be called for all other ResultSets in the tree.
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public final ExecRow	getNextRow() throws StandardException 
	{
	    if ( ! isOpen ) {
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, NEXT);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
				SanityManager.THROWASSERT(
				this + "expected to be the top ResultSet");
		}

		attachStatementContext();

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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, PREVIOUS);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getPreviousRow() not expected to be called.");
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, LAST);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called.");
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
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, LAST);
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}

			SanityManager.THROWASSERT(
				"setAfterLastRow() not expected to be called.");
		}

		return null;
	}


    /**
     * Returns true.
	 */
	 public boolean	returnsRows() { return true; }

	public final int	modifiedRowCount() { return 0; }

	/**
     * Clean up on error
	 * @exception StandardException		Thrown on failure
	 *
	 */
	public void	cleanUp(boolean cleanupOnError) throws StandardException
	{
		if (isOpen) {
			close(cleanupOnError);
		}
	}

	/**
		Report if closed.
	 */
	public final boolean	isClosed() {
	    return ( ! isOpen );
	}

	public void	finish() throws StandardException
	{
		finishAndRTS();
	}

	/**
	 * @exception StandardException on error
	 */	
	protected final void finishAndRTS() throws StandardException
	{

		if (!finished) {
			if (!isClosed())
				close(false);

			finished = true;

			if (isTopResultSet && activation.isSingleExecution())
				activation.close();
		}
	}

	/* The following methods are common to almost all sub-classes.
	 * They are overriden in selected cases.
	 */

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
              // GemStone changes BEGIN
              return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE, ResultSet.ALL) + constructorTime;
              /*(original code)
               return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE);
               */
              // GemStone changes END
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (startExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(startExecutionTime);
		}
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		if (endExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(endExecutionTime);
		}
	}

	/**
	 * @see ResultSet#getSubqueryTrackingArray
	 */
	public final NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		if (subqueryTrackingArray == null)
		{
			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
		}

		return subqueryTrackingArray;
	}

// GemStone changes BEGIN
	/* * commenting to use XPLAIN utils counters.
	 * Return the current time in milliseconds, if DEBUG and RunTimeStats is
	 * on, else return 0.  (Only pay price of system call if need to.)
	 *
	 * @return long		Current time in milliseconds.
	 * /
	protected final long getCurrentTimeMillis()
	{
		if (statisticsTimingOn)
		{
			return System.currentTimeMillis();
		}
		else
		{
			return 0;
		}
	}
	*/

	public final void upgradeReadLockToWrite(final RowLocation rl,
	    GemFireContainer container) {
	  final TXState tx = this.localTXState;
	  final RegionEntry entry = rl.getUnderlyingRegionEntry();
	  final LocalRegion dataRegion = tx.removeReadLockForScan(entry,
	      this.lockContext);

	  if (dataRegion != null) {
	    final LocalRegion region;
	    if (container != null) {
	      region = container.getRegion();
	    }
	    else {
	      if (dataRegion.isUsedForPartitionedRegionBucket()) {
	        region = dataRegion.getPartitionedRegion();
	      }
	      else {
	        region = dataRegion;
	      }
	      container = (GemFireContainer)region.getUserAttribute();
	    }
	    try {
	      final TXStateProxy txProxy = tx.getProxy();
	      if (this.observer != null) {
	        this.observer.lockingRowForTX(txProxy, container, entry, true);
	      }
	      // upgrade the lock since the entry has been qualified
	      txProxy.lockEntry(entry, entry.getKey(),
	          GemFireXDUtils.getRoutingObject(rl.getBucketID()),
	          region, dataRegion, true,
	          TXEntryState.getLockForUpdateOp());
	    } finally {
	      // now release the SH lock after the atomic lock upgrade, or as
	      // cleanup in case of lock upgrade failure
	      final LockingPolicy lockPolicy = tx.getLockingPolicy();
	      GemFireXDUtils.unlockEntryAfterRead(tx.getTransactionId(),
	          lockPolicy, lockPolicy.getReadLockMode(), entry, container,
	          dataRegion);
	    }
	  }
	}

	protected final void releaseRowLocationLock(final RowLocation rl,
	    final GemFireContainer container) {
	  if (rl != null) {
	    // assumes that local TXState is non-null
	    assert this.localTXStateSet && this.localTXState != null :
	      "unexpected localTXState=" + this.localTXState;
	    final TXState tx = this.localTXState;
	    final LockingPolicy lockPolicy = tx.getLockingPolicy();
	    GemFireXDUtils.releaseLockForReadOnPreviousEntry(
	        rl.getUnderlyingRegionEntry(), tx, tx.getTransactionId(),
	        lockPolicy, lockPolicy.getReadLockMode(), container, null,
	        this.lockContext);
	  }
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

	protected AbstractGemFireResultSet getWrappedGemFireRS() {
	  return null;
	}
// GemStone changes END

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
	 * Return the elapsed time in milliseconds, between now and the beginTime, if
	 * DEBUG and RunTimeStats is on, else return 0.  
	 * (Only pay price of system call if need to.)
	 *
	 * @return long		Elapsed time in nanoseconds.
	 */
	protected final long getElapsedNanos(final long beginTime)
	{
		SanityManager.ASSERT(statisticsTimingOn,
		    "getElapsedNanos: unexpected call with statisticsTimingOn as false");
		
                final long ts = XPLAINUtil.nanoTime();
                final long delta = ts - beginTime;
                
                if (GemFireXDUtils.TracePlanAssertion) {
                  SanityManager.ASSERT(delta >= 0, delta + " " + ts + " " + beginTime);
                }
                
                return delta >= 0 ? delta : 0;
	}

	/**
	 * Dump out the time information for run time stats.
	 *
	 * @return Nothing.
	 */
	protected final String dumpTimeStats(String indent, String subIndent)
	{
		return 
			indent +
			  MessageService.getTextMessage(SQLState.LANG_TIME_SPENT_THIS) +
			  " " + getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL) + "\n" +
			indent +
			  MessageService.getTextMessage(
				SQLState.LANG_TIME_SPENT_THIS_AND_BELOW) +
			  " " + getTimeSpent(NoPutResultSet.ENTIRE_RESULTSET_TREE, ResultSet.ALL) + "\n" +
			indent +
			  MessageService.getTextMessage(
				SQLState.LANG_TOTAL_TIME_BREAKDOWN) + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CONSTRUCTOR_TIME) +
			  " " + constructorTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_OPEN_TIME) +
			  " " + openTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_NEXT_TIME) +
			  " " + nextTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CLOSE_TIME) +
			  " " + closeTime;
	}


	/**
	  *	Attach this result set to the top statement context on the stack.
	  *	Result sets can be directly read from the JDBC layer. The JDBC layer
	  * will push and pop a statement context around each ResultSet.getNext().
	  * There's no guarantee that the statement context used for the last
	  * getNext() will be the context used for the current getNext(). The
	  * last statement context may have been popped off the stack and so
	  *	will not be available for cleanup if an error occurs. To make sure
	  *	that we will be cleaned up, we always attach ourselves to the top	
	  *	context.
	  *
	  *	The fun and games occur in nested contexts: using JDBC result sets inside
	  * user code that is itself invoked from queries or CALL statements.
	  *
	  *
	  * @exception StandardException thrown if cursor finished.
	  */
	protected	void	attachStatementContext() throws StandardException
	{
		if (isTopResultSet)
		{
			if (statementContext == null || !statementContext.onStack() )
			{
				statementContext = getLanguageConnectionContext().getStatementContext();
			}
			statementContext.setTopResultSet(this, subqueryTrackingArray);
			// Pick up any materialized subqueries
			if (subqueryTrackingArray == null)
			{
				subqueryTrackingArray = statementContext.getSubqueryTrackingArray();
			}
		}
// GemStone changes BEGIN
		else if (statementContext == null || !statementContext.onStack()) {
		  StatementContext localStatementContext = getLanguageConnectionContext()
		      .getStatementContext();
		  statementContext = (localStatementContext != null
		      && localStatementContext.isLightWeight()
		      ? localStatementContext : null);
// GemStone changes END
		}

	}

	/**
	  *	Cache the language connection context. Return it.
	  *
	  *	@return	the language connection context
	  */
	protected	final LanguageConnectionContext	getLanguageConnectionContext()
	{
// GemStone changes BEGIN
	  return this.lcc;
	/* (original code)
        return getActivation().getLanguageConnectionContext();
        */
// GemStone changes END
	}

	/** @see NoPutResultSet#resultSetNumber() */
	public int resultSetNumber() {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT(
				"resultSetNumber() should not be called on a " +
				this.getClass().getName()
				);
		}

		return 0;
	}

	//////////////////////////////////////////////////////
	//
	// UTILS	
	//
	//////////////////////////////////////////////////////

	/**
	 * Get a execution factory
	 *
	 * @return the execution factory
	 */
	final ExecutionFactory getExecutionFactory() 
	{
		return activation.getExecutionFactory();
	}

	/**
	 * Get the current transaction controller.
	 *
	 */
  	final TransactionController getTransactionController()
 	{
  		if (tc == null)
  		{
			tc = getLanguageConnectionContext().getTransactionExecute();
  		}
  		return tc;
  	}
  
  
	/**
	 * Get a compacted version of the candidate row according to the
	 * columns specified in the bit map. Share the holders between rows.
	 * If there is no bit map, use the candidate row as the compact row.
	 *
	 * Also, create an array of ints mapping base column positions to
	 * compact column positions, to make it cheaper to copy columns to
	 * the compact row, if we ever have to do it again.
	 *
	 * @param candidate		The row to get the columns from
	 * @param accessedCols	A bit map of the columns that are accessed in
	 *						the candidate row
	 * @param isKeyed		Tells whether to return a ValueRow or an IndexRow
	 *
	 * @return		A compact row.
	 */
	protected final ExecRow getCompactRow(ExecRow candidate,
                                  FormatableBitSet accessedCols,
                                  boolean isKeyed)
  throws StandardException {
// GemStone changes BEGIN
    // made default method to not use byte arrays in compact rows
    // and added method that take useByteStorage as argument
    return getCompactRow(candidate, accessedCols, isKeyed, false, null, null,
        true);
//GemStone changes END
  }

	/**
	 * Get a compacted version of the candidate row according to the
	 * columns specified in the bit map. Share the holders between rows.
	 * If there is no bit map, use the candidate row as the compact row.
	 *
	 * Also, create an array of ints mapping base column positions to
	 * compact column positions, to make it cheaper to copy columns to
	 * the compact row, if we ever have to do it again.
	 *
	 * @param candidate		The row to get the columns from
	 * @param accessedCols	A bit map of the columns that are accessed in
	 *						the candidate row
	 * @param isKeyed		Tells whether to return a ValueRow or an IndexRow
   * @param useByteStorage Tells whether to use a
   *        (GemStone) CompactExecRow (byte array storage) or
   *        ValueRow (DataValueDescriptor[]). isKeyed=true overrides this.
   * @param rf the RowFormatter for the compact row,
   *             ignored unless isKeyed is false and useByteStorage is true
	 *
	 * @return		A compact row.
	 */
	protected final ExecRow getCompactRow(ExecRow candidate,
									FormatableBitSet accessedCols,
									boolean isKeyed,
// GemStone changes BEGIN
									boolean useByteStorage,
									GemFireContainer gfc,
									RowFormatter rf,
									boolean copyColumns
// GemStone changes END
                  )
									throws StandardException
	{
		int		numCandidateCols = candidate.nColumns();

// GemStone changes BEGIN
		final int numCols;
		if (accessedCols == null || ((numCols = accessedCols
		    .getNumBitsSet()) == numCandidateCols
		    && accessedCols.lastSetBit() == (numCandidateCols - 1))) {
		/* (original code)
		if (accessedCols == null)
		{
		*/
		  /* TODO: PERF: below should be more efficient but is
		   * failing OffsetFetchNextTest for some reason
		  if (useByteStorage) {
		    this.compactRow = rf.newCompactExecRow(null);
		  }
		  else {
		    this.compactRow = candidate;
		  }
		  */
		  this.compactRow = candidate;
		  // avoid explicit copy of all columns and in turn de-serialize
		  // entire CompactExecRow
		  /* (original code)
      compactRow =  candidate;
			baseColumnMap = new int[numCandidateCols];
			for (int i = 0; i < baseColumnMap.length; i++)
				baseColumnMap[i] = i;
		  */
// GemStone changes END
		}
		else
		{
			//int numCols = accessedCols.getNumBitsSet(); // GemStone change to comment this out
			baseColumnMap = new int[numCols];

			if (compactRow == null)
			{
				if (isKeyed)
				{
          ExecutionFactory ex = getLanguageConnectionContext().getLanguageConnectionFactory().getExecutionFactory();
					compactRow = ex.getIndexableRow(numCols);
				}
				else
				{
// GemStone changes BEGIN
				  if (useByteStorage) {
				    compactRow = gfc.newCompactExecRow(null, rf);
				  }
				  else {
				    ExecutionFactory ex = getLanguageConnectionContext()
				        .getLanguageConnectionFactory().getExecutionFactory();
				    compactRow = ex.getValueRow(numCols);
				  }
				}
			}

			this.compactRow.setCompactColumns(accessedCols,
			    candidate, this.baseColumnMap, copyColumns);
			/* (original code)
			int position = 0;
			for (int i = accessedCols.anySetBit();
					i != -1;
					i = accessedCols.anySetBit(i))
			{
				// Stop looking if there are columns beyond the columns
				// in the candidate row. This can happen due to the
				// otherCols bit map.
				if (i >= numCandidateCols)
					break;

				DataValueDescriptor sc = candidate.getColumn(i+1);
				if (sc != null)
				{
					compactRow.setColumn(
									position + 1,
									sc
									);
				}
				baseColumnMap[position] = i;
				position++;
			}
			*/
// GemStone changes END
		}
		return compactRow;
	}

	/**
	 * Copy columns from the candidate row from the store to the given
	 * compact row. If there is no column map, just use the candidate row.
	 *
	 * This method assumes the above method (getCompactRow()) was called
	 * first. getCompactRow() sets up the baseColumnMap.
	 *
	 * @param candidateRow	The candidate row from the store
	 * @param compactRow	The compact row to fill in
	 *
	 * @return	The compact row to use
	 */
	protected final ExecRow setCompactRow(ExecRow candidateRow, ExecRow compactRow)
	throws StandardException {
		ExecRow	retval;

		//System.out.println("base col map " + baseColumnMap);
		if (baseColumnMap == null || baseColumnMap.length == 0)
		{
			retval = candidateRow;
		}
		else
		{
			retval = compactRow;
// GemStone changes BEGIN
			final int[] lbcm = baseColumnMap;
			if (lbcm != null) {
			  compactRow.setColumns(lbcm, true, candidateRow);
			}
			else {
			  compactRow.setColumns(compactRow.nColumns(),
			      candidateRow);
			}
			/* (original code)
			setCompactRow(compactRow, candidateRow.getRowArray());
			*/
// GemStone changes END
		}

		return retval;
	}

// GemStone changes BEGIN

  @Override
  public final TXState initLocalTXState() {
    if (this.localTXStateSet) {
      return this.localTXState;
    }
    this.localTXState = null;
    final TXStateInterface tx = ((GemFireTransaction)this.lcc
        .getTransactionExecute()).getActiveTXState();
    if (tx != null) {
      if (isForUpdate()) {
        this.localTXState = tx.getTXStateForWrite();
        initLocalTXState(this.localTXState, true);
      }
      else if (!tx.getLockingPolicy().zeroDurationReadLocks()) {
        this.localTXState = tx.getTXStateForRead();
        initLocalTXState(this.localTXState, false);
      }
    }
    this.localTXStateSet = true;
    return this.localTXState;
  }

  protected void initLocalTXState(TXState localTXState, boolean forUpdate) {
    this.lockContext = localTXState.getReadLocksForScanContext(this.lcc);
  }

  @Override
  public void deleteRowDirectly() throws StandardException {
    throw new UnsupportedOperationException(
        "This method should not have been invoked");
  }

  public boolean canUpdateInPlace() {
    return false;
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

  protected final void setCompactRow(ExecRow compactRow,
                                    DataValueDescriptor[] sourceRow)
  throws StandardException {

    DataValueDescriptor[] srcValues = new DataValueDescriptor[compactRow.nColumns()];
		int[] lbcm = baseColumnMap;
    
                if(lbcm != null) {
                  for (int i = 0; i < lbcm.length; i++)
                  {
                    
                    srcValues[i] = sourceRow[lbcm[i]];
                    
                  }
                }
                else {
                  for (int i = 0, size = compactRow.nColumns(); i < size; i++)
                  {
                    
                    srcValues[i] = sourceRow[i];
                    
                  }
                }
                  
    compactRow.setColumns(null, srcValues);
    /*
		DataValueDescriptor[] destRow = compactRow.getRowArray();
		int[] lbcm = baseColumnMap;

		for (int i = 0; i < lbcm.length; i++)
		{

			destRow[i] = sourceRow[lbcm[i]];

		}
     */
// GemStone changes END
	}

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
     * Checks whether the currently executing statement has been cancelled.
     */
	@Override
	public final void checkCancellationFlag()
        throws
            StandardException
	{
// GemStone changes BEGIN
	  if (activation.isQueryCancelled()) {
	    activation.checkCancellationFlag();
	  }
//	  final StatementContext ctx = this.statementContext;
//	  if (ctx != null && ctx.isCancelled()) {
//	    throw StandardException.newException(SQLState.
//	        LANG_STATEMENT_CANCELLED_OR_TIMED_OUT);
//	  }
// GemStone changes END
	  
//        StatementContext localStatementContext = getLanguageConnectionContext().getStatementContext();            
//        if (localStatementContext == null) {
//            return;
//        }

//        //GemStone changes BEGIN
//        if(!isTopResultSet) {
//           statementContext = ( localStatementContext.isLightWeight() ? localStatementContext : null);
//        }
//        //GemStone changes END
//        
//        if (localStatementContext.isCancelled()) {
//            throw StandardException.newException(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT);
//        }
    }

	protected final void addWarning(SQLWarning w) {

		if (isTopResultSet) {
			if (warnings == null)
				warnings = w;
			else 
				warnings.setNextWarning(w);
			return;
		}

		if (activation != null) {

			ResultSet rs = activation.getResultSet();
			if (rs instanceof BasicNoPutResultSetImpl) {
				((BasicNoPutResultSetImpl) rs).addWarning(w);
			}

		}
	}

	public final SQLWarning getWarnings() {
		SQLWarning w = warnings;
		warnings = null;
		return w;
	}
	
  // GemStone changes BEGIN
  public UUID getExecutionPlanID() {
    return executionPlanID;
  }
  
  public void markLocallyExecuted() {
    isLocallyExecuted = true;
  }
  
  /**
   * Changes done for Union, Intersection and Except Many subclass have this
   * method implemented before introduction here.
   * 
   * @throws StandardException
   */
  public boolean isReplicateIfSetOpSupported() throws StandardException {
    if (SanityManager.DEBUG) {
      SanityManager.DEBUG_PRINT("info:" + GfxdConstants.NOT_YET_IMPLEMENTED,
          " Set Operators do not yet supported for class: "
              + this.getClass().getSimpleName());
    }
    throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
        " Currently Set operators are not supported for these queries");
  }

  public void resetStatistics() {
    startExecutionTime = 0;
    endExecutionTime = 0;

    // leaving constructTime purposefully.
    openTime = 0;
    closeTime = 0;
    nextTime = 0;
    
    numOpens = 0;
    rowsSeen = 0;
    rowsFiltered = 0;

  }

  /**
   * {@inheritDoc}
   */
  public boolean isDistributedResultSet() {
    return false;
  }

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

  public PlanUtils.Context getNewPlanContext() {
    return new PlanUtils.Context();
  }
  
  @Override
  public StringBuilder buildQueryPlan(final StringBuilder builder, final PlanUtils.Context context) {

    PlanUtils.xmlBeginTag(builder, context, this);
    
    if (isTopResultSet) {
      PlanUtils.xmlAttribute(builder, "begin_execute_time", getBeginExecutionTimestamp());
      PlanUtils.xmlAttribute(builder, "end_execute_time", getEndExecutionTimestamp());
      context.totalExecTime = getExecuteTime();
      PlanUtils.xmlAttribute(builder, "total_execute_time", context.totalExecTime/1000000d, " ms");
    }
    
    long v;
    PlanUtils.xmlAttribute(builder, "net_execute_time", (v = getTimeSpent(CURRENT_RESULTSET_ONLY, ALL))/1000000d, " ms");
    double percent = ((double)v / (double)context.totalExecTime) * 100d;
    
    PlanUtils.xmlAttribute(builder, "percent_execute_time", PlanUtils.format.format(percent), "%");
    
    PlanUtils.xmlAttribute(builder, "construct_time", (v = getTimeSpent(CURRENT_RESULTSET_ONLY, CONSTRUCT_TIME)) < 0 ? 0 : v/1000000d, " ms");
    PlanUtils.xmlAttribute(builder, "open_time", (v = getTimeSpent(CURRENT_RESULTSET_ONLY, OPEN_TIME)) < 0 ? 0 : v/1000000d, " ms");
    PlanUtils.xmlAttribute(builder, "next_time", (v = getTimeSpent(CURRENT_RESULTSET_ONLY, NEXT_TIME)) < 0 ? 0 : v/1000000d, " ms");
    PlanUtils.xmlAttribute(builder, "close_time", (v = getTimeSpent(CURRENT_RESULTSET_ONLY, CLOSE_TIME)) < 0 ? 0 : v/1000000d, " ms");
    
    PlanUtils.xmlAttribute(builder, "num_opens", this.numOpens);
    PlanUtils.xmlAttribute(builder, "rows_seen", this.rowsSeen);
    PlanUtils.xmlAttribute(builder, "rows_filtered", this.rowsFiltered);
    
    return builder;
  }

  @Override
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn, GemFireContainer container
      )
      throws StandardException {
    return RowUtil.fetch(loc, destRow, validColumns, faultIn, container, null,
        null, 0, (GemFireTransaction)this.lcc.getTransactionExecute());
  }

  @Override
  public void releasePreviousByteSource() {
  }

  @Override
  public void setMaxSortingLimit(long limit) {
  }
  
  @Override
  public void forceReOpenCore() throws StandardException { 
  }
// GemStone changes END
}
