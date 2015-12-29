/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.MaterializedResultSet

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


// GemStone changes BEGIN
import java.util.Properties;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Materialize the underlying ResultSet tree into a temp table on the 1st open.
 * Return rows from temp table on subsequent scans.
 */

class MaterializedResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;



	private	ExecRow						materializedRowBuffer;
	protected long						materializedCID;
	public    boolean					materializedCreated;
	private   boolean					fromSource = true;
	protected ConglomerateController	materializedCC;
	protected ScanController			materializedScan;
	private TransactionController		tc;
	private   boolean					sourceDrained;

	public	  long						createTCTime;
	public	  long						fetchTCTime;

	/**
	 * Constructor for a MaterializedResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to be materialized
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 *
	 * @exception StandardException	on error
	 */

	public MaterializedResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost) throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.source = source;

        // Get the current transaction controller
        tc = activation.getTransactionController();

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
	    	SanityManager.ASSERT( ! isOpen, "MaterializedResultSet already open");

		isOpen = true;
        source.openCore();
	    //isOpen = true;
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
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
		    SanityManager.ASSERT(isOpen, "MaterializedResultSet already open");

		// Finish draining the source into the temp table
		while (! sourceDrained)
		{
			getNextRowFromSource();
		}

		// Results will now come from the temp table
		fromSource = false;

		// Close the temp table if open
		if (materializedScan != null)
		{
			materializedScan.close();
		}

		/* Open a scan on the temp conglomerate,
		 * if one exists.
		 */
		if (materializedCID != 0)
		{
			materializedScan = 
                tc.openScan(materializedCID,
                    false,		// hold
                    0,          // for update
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    (FormatableBitSet) null, // all fields as objects
                    null,		// start key value
                    0,			// start operator
                    null,		// qualifier
                    null,		// stop key value
                    0                   // stop operator 
                    // GemStone changes BEGIN
                    , null);
		    // GemStone changes END
		
			isOpen = true;
		}

		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
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

		/* Should we get the next row from the source or the materialized result set? */
		if (fromSource)
		{
			result = getNextRowFromSource();
		}
		else
		{
			result = getNextRowFromTempTable();
		}

		if (result != null)
		{
			rowsSeen++;
		}

		setCurrentRow(result);
// GemStone changes BEGIN
		if (this.localTXState != null && this.isTopResultSet &&
		    result != null && isForUpdate()) {
		  updateRowLocationPostRead();
		}
// GemStone changes END

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);

		return result;
	}

	/* Get the next row from the source ResultSet tree and insert into the temp table */
	private ExecRow getNextRowFromSource() throws StandardException
	{
		// Nothing to do if source is already drained
		if (sourceDrained)
		{
			return null;
		}

		ExecRow		sourceRow = null;
		ExecRow		result = null;

		sourceRow = source.getNextRowCore();

		if (sourceRow != null)
		{
			long beginTCTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
			/* If this is the first row from the source then clone it as our own
			 * for use when fetching from temp table.
			 * This is also the place where we create the temp table.
			 */
			if (materializedRowBuffer == null)
			{
				materializedRowBuffer = sourceRow.getClone();

				tc = activation.getTransactionController();
        
        // GemStone changes BEGIN
        Properties properties = new Properties();
        properties.setProperty(GfxdConstants.PROPERTY_SCHEMA_NAME,
                               GfxdConstants.SESSION_SCHEMA_NAME);
        properties.setProperty(GfxdConstants.PROPERTY_TABLE_NAME,
                               GfxdConstants.TEMP_TABLE_NAME);
        // GemStone changes END

				materializedCID = 
                    tc.createConglomerate(
                        "heap",	
                        materializedRowBuffer.getRowArray(),
                        null, 
                        (int[]) null, // TODO-COLLATION, implement collation in materialized result sets if necessary
                                          
                        // GemStone changes BEGIN
                        properties,
                        /* null, */
                        // GemStone changes END
                                          
                        (TransactionController.IS_TEMPORARY |
                         TransactionController.IS_KEPT));

				materializedCreated = true;
				materializedCC = 
                    tc.openConglomerate(
                        materializedCID, 
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);
			}
			materializedCC.insert(sourceRow.getRowArray());

			if (statisticsTimingOn) createTCTime += getElapsedNanos(beginTCTime);
		}
		// Remember whether or not we've drained the source
		else
		{
			sourceDrained = true;
		}

		return sourceRow;
	}

	/* Get the next Row from the temp table */
	private ExecRow getNextRowFromTempTable() throws StandardException
	{
		long beginTCTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		/* Get and return the next row from the temp conglomerate,
		 * if one exists.
		 */
		if (materializedScan != null && materializedScan.fetchNext(materializedRowBuffer))
		{
			if (statisticsTimingOn) fetchTCTime += getElapsedNanos(beginTCTime);
			return materializedRowBuffer;
		}
		else
		{
			return null;
		}
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

			if (materializedScan != null)
				materializedScan.close();
			materializedScan = null;

			if (materializedCC != null)
				materializedCC.close();
			materializedCC = null;

			if (materializedCreated)
				tc.dropConglomerate(materializedCID);

			materializedCreated = false;

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of MaterializedResultSet repeated");

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
	public ExecRow getCurrentRow() 
	{
		return currentRow;
	}

// GemStone changes BEGIN
  @Override
  public boolean isForUpdate() {
    return this.source.isForUpdate();
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    this.source.updateRowLocationPostRead();
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    this.source.filteredRowLocationPostRead(localTXState);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsMoveToNextKey() {
    return this.source.supportsMoveToNextKey();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScanKeyGroupID() {
    return this.source.getScanKeyGroupID();
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.setNumberOfChildren(1);
    visitor.visit(this);
    source.accept(visitor);
  }
  
  @Override
  public void resetStatistics() {
    if (SanityManager.ASSERT) {
      SanityManager.ASSERT(isClosed());
    }
    createTCTime = 0;
    fetchTCTime = 0;
    super.resetStatistics();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_TABLESCAN);
    
    if(this.source != null)
      this.source.buildQueryPlan(builder, context);
    
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
	//
	// class implementation
	//
}
