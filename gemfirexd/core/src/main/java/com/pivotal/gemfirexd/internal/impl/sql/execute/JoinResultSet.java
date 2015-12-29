/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.JoinResultSet

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
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes 2 NoPutResultSets and a join filter and returns
 * the join's rows satisfying the filter as a result set.
 *
 */
public abstract class JoinResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/* Run time statistics variables */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	public long restrictionTime;

	protected boolean isRightOpen;
	protected ExecRow leftRow;
	protected ExecRow rightRow;
	protected ExecRow mergedRow;

    // set in constructor and not altered during
    // life of object.
    public	  NoPutResultSet leftResultSet;
	protected int		  leftNumCols;
	public	  NoPutResultSet rightResultSet;
	protected int		  rightNumCols;
    protected GeneratedMethod restriction;
	public	  boolean oneRowRightSide;
	public	  boolean notExistsRightSide;  //right side is NOT EXISTS
	
	String userSuppliedOptimizerOverrides;
	// GemStone changes BEGIN
	public String[] leftResultSetColumnNames;
	public String[] rightResultSetColumnNames;
	// GemStone changes END

    /*
     * class interface
     *
     */
    JoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod restriction,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   String userSuppliedOptimizerOverrides, 
					                           // GemStone changes BEGIN
								   int leftResultColumnNames, 
								   int rightResultColumnNames
                                                                   // GemStone changes END
								   )
    {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost);
        this.leftResultSet = leftResultSet;
		this.leftNumCols = leftNumCols;
        this.rightResultSet = rightResultSet;
		this.rightNumCols = rightNumCols;
        this.restriction = restriction;
		this.oneRowRightSide = oneRowRightSide;
		this.notExistsRightSide = notExistsRightSide;

		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		
// GemStone changes BEGIN
                if (leftResultColumnNames != -1) {
                  this.leftResultSetColumnNames = (String[])getActivation()
                      .getSavedObject(leftResultColumnNames);
                }
                if (rightResultColumnNames != -1) {
                  this.rightResultSetColumnNames = (String[])getActivation()
                      .getSavedObject(rightResultColumnNames);
                }
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
	 * Clear any private state that changes during scans.
	 * This includes things like the last row seen, etc.
	 * THis does not include immutable things that are
	 * typically set up in the constructor.
	 * <p>
	 * This method is called on open()/close() and reopen()
	 */
	void clearScanState()
	{
		leftRow = null;
		rightRow = null;
		mergedRow = null;	
	}

	/**
     * open a scan on the join. 
	 * For a join, this means:
	 *	o  Open the left ResultSet
	 *  o  Do a getNextRow() on the left ResultSet to establish a position
	 *	   and get "parameter values" for the right ResultSet.
	 *	   NOTE: It is possible for the getNextRow() to return null, in which
	 *	   case there is no need to open the RightResultSet.  We must remember
	 *	   this condition.
	 *	o  If the getNextRow() on the left ResultSet succeeded, then open()
	 *	   the right ResultSet.
	 *
	 * scan parameters are evaluated at each open, so there is probably 
	 * some way of altering their values...
	 *
	 * @exception StandardException		Thrown on error
     */
        // GemStone changes BEGIN
	// Just brokeup method in 2
	public void	openCore() throws StandardException
	{
	        preOpen();
	        ncjOpen();
	        postOpen();
	}
	
	/*
	 * NCJ specific 
	 */
	public void ncjOpen() throws StandardException {
	  // DO Nothing here.
	  // Will be Overridden
	}
	
	protected void preOpen() throws StandardException
	{
		clearScanState();
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "JoinResultSet already open");

	        isOpen = true;
	}
	
	protected void postOpen() throws StandardException
	{
		leftResultSet.openCore();
		leftRow = leftResultSet.getNextRowCore();
		if (leftRow != null)
		{
			openRight();
			rowsSeenLeft++;
		}
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}
        // GemStone changes END
	/**
     * reopen a a join.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	reopenCore() throws StandardException 
	{
                // GemStone changes BEGIN
	        if(statisticsTimingOn) beginTime = XPLAINUtil.nanoTime();
	        // GemStone changes END
	  this.leftResultSet.releasePreviousByteSource();
	  this.rightResultSet.releasePreviousByteSource();     
		clearScanState();
		
		ncjOpen();

		// Reopen the left and get the next row
		leftResultSet.reopenCore();
		leftRow = leftResultSet.getNextRowCore();
		if (leftRow != null)
		{
			// Open the right
			openRight();
			rowsSeenLeft++;
		}
		else if (isRightOpen)
		{
			closeRight();
		}

		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}


	/**
	 * If the result set has been opened,
	 * close the open scan.
	 * <n>
	 * <B>WARNING</B> does not track close
	 * time, since it is expected to be called
	 * directly by its subclasses, and we don't
	 * want to skew the times
	 * 
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{

		if ( isOpen )
	    {
	        leftResultSet.close(cleanupOnError);
			if (isRightOpen)
			{
				closeRight();
			}

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of JoinResultSet repeated");

		clearScanState();
	}

	public void finish() throws StandardException {
		leftResultSet.finish();
		rightResultSet.finish();
		super.finish();
	}

	//
	// CursorResultSet interface
	//
	/**
	 * A join is combining rows from two sources, so it has no
	 * single row location to return; just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Join used in positioned update/delete");
		return null;
	}

	/**
	 * A join is combining rows from two sources, so it 
	 * should never be used in a positioned update or delete.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null value.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Join used in positioned update/delete");
		return null;
	}

	/* Class implementation */

	/**
	 * open the rightResultSet.  If already open,
	 * just reopen.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected void openRight() throws StandardException
	{
		if (isRightOpen)
		{
			rightResultSet.reopenCore();
		}	
		else
		{
			isRightOpen = true;
			rightResultSet.openCore();
		}
	}

	/**
	 * close the rightResultSet
	 *
	 */
	protected void closeRight() throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isRightOpen, "isRightOpen is expected to be true");
		rightResultSet.close(false);
		isRightOpen = false;
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    SanityManager.THROWASSERT("not expected to be invoked for joins");
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    // cannot release lock on left side unless all right side rows are
    // deemed as unqualified (which is hard to find at this point)
    if (this.isRightOpen) {
      this.rightResultSet.filteredRowLocationPostRead(localTXState);
    }
    
  }
  
  /*@Override
  public void releasePreviousByteSource()  {
    // cannot release lock on left side unless all right side rows are
    // deemed as unqualified (which is hard to find at this point)
    if (this.isRightOpen) {
      this.rightResultSet.releasePreviousByteSource();
    }
    
  }*/

  @Override
  public void accept(
      ResultSetStatisticsVisitor visitor) {

    int noChildren = 0;
    if (this.leftResultSet != null)
      noChildren++;
    if (this.rightResultSet != null)
      noChildren++;

    // inform the visitor
    visitor.setNumberOfChildren(noChildren);

    visitor.visit(this);

    if (leftResultSet != null) {
      leftResultSet.accept(visitor);
    }
    if (rightResultSet != null) {
      rightResultSet.accept(visitor);
    }

  }
  
  public StringBuilder buildQueryPlan(StringBuilder builder,
      PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    PlanUtils.xmlAttribute(builder, "rows_seen_left", this.rowsSeenLeft);
    PlanUtils.xmlAttribute(builder, "rows_seen_right", this.rowsSeenRight);
    PlanUtils.xmlAttribute(builder, "restriction_time", this.restrictionTime);
    
    if (userSuppliedOptimizerOverrides != null) {
      PlanUtils.xmlAttribute(builder, PlanUtils.TG_OPTIMIZER_OVERRIDE,
          this.userSuppliedOptimizerOverrides);
    }
    
    return builder; 
  }
  
  @Override
  public void resetStatistics() {
    rowsSeenLeft = 0;
    rowsSeenRight = 0;
    rowsReturned = 0;
    restrictionTime = 0;
    super.resetStatistics();
    leftResultSet.resetStatistics();
    rightResultSet.resetStatistics();
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(
            GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "ResultSet Created: "
                + this.getClass().getSimpleName()
                + " with resultSetNumber="
                + resultSetNumber
                + " with left-source = "
                + (this.leftResultSet != null ? this.leftResultSet.getClass()
                    .getSimpleName() : null)
                + " and left-source ResultSetNumber = "
                + (this.leftResultSet != null ? this.leftResultSet
                    .resultSetNumber() : -1)
                + " with right-source = "
                + (this.rightResultSet != null ? this.rightResultSet.getClass()
                    .getSimpleName() : null)
                + " and right-source ResultSetNumber = "
                + (this.rightResultSet != null ? this.rightResultSet
                    .resultSetNumber() : -1));
      }
    }
  }
// GemStone changes END
}
