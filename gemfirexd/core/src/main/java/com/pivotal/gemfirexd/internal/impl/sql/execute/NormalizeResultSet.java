/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.NormalizeResultSet

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
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Cast the rows from the source result set to match the format of the
 * result set for the entire statement.
 */

class NormalizeResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;
	private	ExecRow			normalizedRow;
	private	int				numCols;
	private int				startCol;

	/* RESOLVE - We need to pass the ResultDescription for this ResultSet
	 * as a parameter to the constructor and use it instead of the one from
	 * the activation
	 */
	private ResultDescription resultDescription;

	/* info for caching DTSs */
	private DataTypeDescriptor[] desiredTypes;

	/**
	 * Constructor for a NormalizeResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to be normalized
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 * @param erdNumber					The integer for the ResultDescription
	 *
	 * @exception StandardException	on error
	 */

	public NormalizeResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  int erdNumber,
	 					      double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost,
							  boolean forUpdate) throws StandardException
	{
		super(activation, resultSetNumber, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost);
		this.source = source;

		if (SanityManager.DEBUG)
		{
			if (! (activation.getSavedObject(erdNumber)
							 instanceof ResultDescription))
			{
				SanityManager.THROWASSERT(
					"activation.getPreparedStatement().getSavedObject(erdNumber) " +
					"expected to be instanceof ResultDescription");
			}

			// source expected to be non-null, mystery stress test bug
			// - sometimes get NullPointerException in openCore().
			SanityManager.ASSERT(source != null,
				"NRS(), source expected to be non-null");
		}

		this.resultDescription = 
			(ResultDescription) activation.getSavedObject(erdNumber);

		numCols = resultDescription.getColumnCount();
		
		/*
		  An update row, for an update statement which sets n columns; i.e
		     UPDATE tab set x,y,z=.... where ...;
		  has,
		  before values of x,y,z after values of x,y,z and rowlocation.
		  need only normalize after values of x,y,z.
		  i.e insead of starting at index = 1, I need to start at index = 4.
		  also I needn't normalize the last value in the row.
	*/
		startCol = (forUpdate) ? ((numCols - 1)/ 2) + 1 : 1;
		normalizedRow = activation.getExecutionFactory().getValueRow(numCols);
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
	    	SanityManager.ASSERT( ! isOpen, "NormalizeResultSet already open");

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"NRS().openCore(), source expected to be non-null");
		}

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
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(isOpen, "NormalizeResultSet already open");

		source.reopenCore();
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
		ExecRow		sourceRow = null;
		ExecRow		result = null;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (!isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		sourceRow = source.getNextRowCore();
		if (sourceRow != null)
		{
			result = normalizeRow(sourceRow);
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
	        // GemStone addition
	        normalizedRow.resetRowArray();

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of NormalizeResultSet repeated");

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
			SanityManager.ASSERT(source instanceof CursorResultSet, "source is not a cursorresultset");
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

	//
	// class implementation
	//
	/**
	 * Normalize a row.  For now, this means calling constructors through
	 * the type services to normalize a type to itself.  For example,
	 * if you're putting a char(30) value into a char(15) column, it
	 * calls a SQLChar constructor with the char(30) value, and the
	 * constructor truncates the value and makes sure that no non-blank
	 * characters are truncated.
	 *
	 * In the future, this mechanism will be extended to do type conversions,
	 * as well.  I didn't implement type conversions yet because it looks
	 * like a lot of work, and we needed char and varchar right away.
	 *
	 * @param sourceRow		The row to normalize
	 *
	 * @return	The normalized row
	 *
 	 * @exception StandardException thrown on failure 
	 */
	private ExecRow normalizeRow(ExecRow sourceRow) throws StandardException
	{
		int					whichCol;

		if (desiredTypes == null)
		{
			desiredTypes = new DataTypeDescriptor[numCols];
			for (whichCol = 1; whichCol <= numCols; whichCol++)
			{
				DataTypeDescriptor dtd = resultDescription.getColumnDescriptor(whichCol).getType();

				desiredTypes[whichCol - 1] = dtd;
			}

		}

		for (whichCol = 1; whichCol <= numCols; whichCol++)
		{
			DataValueDescriptor sourceCol = sourceRow.getColumn(whichCol);
			if (sourceCol != null)
			{
				DataValueDescriptor	normalizedCol;
				// skip the before values in case of update
				if (whichCol < startCol)
					normalizedCol = sourceCol;
				else
					try {
						normalizedCol = 
						desiredTypes[whichCol - 1].normalize(sourceCol, 
									normalizedRow.getColumn(whichCol));
					} catch (StandardException se) {
						// Catch illegal null insert and add column info
						if (se.getMessageId().startsWith(SQLState.LANG_NULL_INTO_NON_NULL))
						{
							ResultColumnDescriptor columnDescriptor =
								resultDescription.getColumnDescriptor(whichCol);
							throw
								StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL, 
															   columnDescriptor.getName());
						}
						EmbedStatement.fillInColumnName(se, resultDescription
						    .getColumnDescriptor(whichCol).getName(), getActivation());
						//just rethrow if not LANG_NULL_INTO_NON_NULL
						throw se;
					}

				normalizedRow.setColumn(whichCol, normalizedCol);
			}
		}

		return normalizedRow;
	}

	/**
	 * @see NoPutResultSet#updateRow
	 */
	public void updateRow (ExecRow row) throws StandardException {
		source.updateRow(row);
	}

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 */
	public void markRowAsDeleted() throws StandardException {
		source.markRowAsDeleted();
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

  @Override
  public int getScanKeyGroupID() {
    return this.source.getScanKeyGroupID();
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
  public void releasePreviousByteSource() {
    this.source.releasePreviousByteSource();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_TABLESCAN);
    
    if(this.source != null)
      this.source.buildQueryPlan(builder, context.pushContext());
    
    PlanUtils.xmlCloseTag(builder, context, this);
    return builder;
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager
            .DEBUG_PRINT(
                GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
                "ResultSet Created: "
                    + this.getClass().getSimpleName()
                    + " with resultSetNumber="
                    + resultSetNumber
                    + " with source = "
                    + (this.source != null ? this.source.getClass()
                        .getSimpleName() : null)
                    + " and source ResultSetNumber = "
                    + (this.source != null
                        && this.source instanceof NoPutResultSetImpl ? ((NoPutResultSetImpl)this.source)
                        .resultSetNumber() : -1));
      }
    }
  }
  // GemStone changes END
}
