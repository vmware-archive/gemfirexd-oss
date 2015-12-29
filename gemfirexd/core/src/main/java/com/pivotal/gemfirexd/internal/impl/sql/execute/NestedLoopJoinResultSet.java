/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.NestedLoopJoinResultSet

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
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;


/**
 * Takes 2 NoPutResultSets and a join filter and returns
 * the join's rows satisfying the filter as a result set.
 */
class NestedLoopJoinResultSet extends JoinResultSet
{
	private boolean returnedRowMatchingRightSide = false;
	private ExecRow rightTemplate;

// GemStone changes BEGIN
	protected boolean matchRight = false;
// GemStone changes END
	
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
	 * <p>
	 * WARNING: this should be implemented in every sub
	 * class and it should always call super.clearScanState().
	 */
	void clearScanState()
	{
		returnedRowMatchingRightSide = false;
		super.clearScanState();
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException		Thrown on error
	 * @exception StandardException		ResultSetNotOpen thrown if closed
	 * @return the next row in the join result
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
	    ExecRow result = null;
		boolean haveRow = false;
	    boolean restrict = false;
		int colInCtr;
		int colOutCtr;
	    DataValueDescriptor restrictBoolean;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
// GemStone changes BEGIN
		if (GemFireXDUtils.TraceOuterJoin) {
                  SanityManager.DEBUG_PRINT(
                      GfxdConstants.TRACE_OUTERJOIN_MERGING,
                      "NestedLoopJoinResultSet::getNextRowCore this is: "
                          + this + "(" + System.identityHashCode(this) + ") leftresultset is: "
                          + leftResultSet + "(" + System.identityHashCode(leftResultSet)
                          + " rightresultset is: " + rightResultSet + "("
                          + System.identityHashCode(rightResultSet));
                }
		
		if (observer != null) {
		  observer.onGetNextRowCore(this);
		}
		checkCancellationFlag();
		final TXState localTXState = this.localTXState;
// GemStone changes END
		if (! isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		/* If we have a row from the left side and the right side is not open, 
		 * then we get an error on the previous next, either on the next on
		 * the left or the open on the right.  So, we do a next on the left
		 * and then open the right if that succeeds.
		 */
		if (! isRightOpen && leftRow != null)
		{		 
// GemStone changes BEGIN
			if (this.matchRight) {
			  this.matchRight = false;			
			}
			else  {//if (localTXState != null) {
			  this.leftResultSet.filteredRowLocationPostRead(localTXState);
			}
//GemStone changes END
			this.leftResultSet.releasePreviousByteSource();
			leftRow = leftResultSet.getNextRowCore();

			if (leftRow == null)
			{
				closeRight();
			}
			else
			{
				rowsSeenLeft++;
				openRight();
			}
		}

		while (leftRow != null && !haveRow)
		{
		 
		  this.rightResultSet.releasePreviousByteSource();
		  
		  
			if (oneRowRightSide && returnedRowMatchingRightSide)
			{
				rightRow = null;
				returnedRowMatchingRightSide = false;
			}
			else
			{
				rightRow = rightResultSet.getNextRowCore();

				/* If this is a NOT EXISTS join, we just need to reverse the logic
				 * of EXISTS join.  To make the implementation simple, we create a
				 * right side template, which is never really needed. (beetle 5173)
				 */
				if (notExistsRightSide)
				{
					if (rightRow == null)      //none satisfied
						rightRow = rightTemplate;  //then we are
					else {
					  //this.rightResultSet.releasePreviousByteSource();
						rightRow = null;
					}
				}

				returnedRowMatchingRightSide = (rightRow != null);
			}

			if (rightRow == null)
			{
				/* Current scan on right is exhausted.  Need to close old scan 
				 * and open new scan with new "parameters".  openRight()	
				 * will reopen if already open.
				 */
// GemStone changes BEGIN
				if (this.matchRight) {
				  this.matchRight = false;	
				}
				else  {//if (localTXState != null) {
				  leftResultSet.filteredRowLocationPostRead(localTXState);
				}
// GemStone changes END
				this.leftResultSet.releasePreviousByteSource();
				leftRow = leftResultSet.getNextRowCore();
				if (leftRow == null)
				{
					closeRight();
				}
				else
				{
					rowsSeenLeft++;
					openRight();
				}
			}
			else
			{
				rowsSeenRight++;

				if (restriction != null)
				{
					restrictBoolean =
						(DataValueDescriptor) restriction.invoke(activation);

			        // if the result is null, we make it false --
					// so the row won't be returned.
					restrict = (! restrictBoolean.isNull()) &&
									restrictBoolean.getBoolean();

					if (! restrict)
					{
						/* Update the run time statistics */
						rowsFiltered++;
// Gemstone changes BEGIN
						// clear the region and key info
						rightRow.clearAllRegionAndKeyInfo();
						//if (localTXState != null) {
						  this.rightResultSet
						      .filteredRowLocationPostRead(localTXState);
						//}
// Gemstone changes END
						continue;
					}
				}

				/* Merge the rows, doing just in time allocation for mergedRow.
				 * (By convention, left Row is to left of right Row.)
				 */
				if (mergedRow == null)
				{
					mergedRow = getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
				}

				for (colInCtr = 1, colOutCtr = 1; colInCtr <= leftNumCols;
					 colInCtr++, colOutCtr++)
					{
						 mergedRow.setColumn(colOutCtr, 
											 leftRow.getColumn(colInCtr));
					}
				if (! notExistsRightSide)
				{
					for (colInCtr = 1; colInCtr <= rightNumCols; 
						 colInCtr++, colOutCtr++)
					{
						 mergedRow.setColumn(colOutCtr, 
											 rightRow.getColumn(colInCtr));
					}
				}
				// Gemstone changes BEGIN
                                if (mergedRow != null) {
                                  mergedRow.clearAllRegionAndKeyInfo();
                                  mergedRow.addAllKeys(leftRow.getAllRegionAndKeyInfo());
                                  mergedRow.addAllKeys(rightRow.getAllRegionAndKeyInfo());
                                  this.matchRight = true;
                                  if (GemFireXDUtils.TraceOuterJoin) {
                                    SanityManager.DEBUG_PRINT(
                                        GfxdConstants.TRACE_OUTERJOIN_MERGING,
                                        "NestedLoopJoinResultSet::getNextRowCore mergedrow is: "
                                            + mergedRow + "(" + System.identityHashCode(mergedRow)
                                            + ") and regionandkeyinfo: "
                                            + mergedRow.getAllRegionAndKeyInfo());
                                  }
                                }
		                // Gemstone changes END
				setCurrentRow(mergedRow);
				haveRow = true;
			}
		}

		/* Do we have a row to return? */
	    if (haveRow)
	    {
			result = mergedRow;
			rowsReturned++;
	    }
		else
		{
			clearCurrentRow();
		}

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
	    if ( isOpen )
	    {
			beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

			super.close(cleanupOnError);
			returnedRowMatchingRightSide = false;
// GemStone changes BEGIN
			this.matchRight = false;
// GemStone changes END
			if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	    }

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
			return	time - leftResultSet.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType) -
							  rightResultSet.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
		}
		else
		{
                        // GemStone changes BEGIN
                        return timeType == ALL ? (time - constructorTime) : time;
                        /*(original code) return totTime; */
                        // GemStone changes END
		}
	}

    /*
     * class interface
     *
     */
    NestedLoopJoinResultSet(NoPutResultSet leftResultSet,
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
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
			  activation, restriction, resultSetNumber, 
			  oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost, userSuppliedOptimizerOverrides,
                          // GemStone changes BEGIN
                          leftResultColumnNames, 
                          rightResultColumnNames
                          // GemStone changes END
			  );
		if (notExistsRightSide)
			rightTemplate = getExecutionFactory().getValueRow(rightNumCols);
    }
    
    @Override
    public StringBuilder buildQueryPlan(StringBuilder builder,
        PlanUtils.Context context) {
      
      final boolean isSuccess = context.setNested();
      
      super.buildQueryPlan(builder, context);
            
      if (!isSuccess) {
        return builder; // child will call after adding its own attributes if any. }
      }
  
      PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_JOIN_NL);
      
      endBuildQueryPlan(builder, context);
      
      PlanUtils.xmlCloseTag(builder, context, this);
      
      return builder;
    }
    
    
    public StringBuilder endBuildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
      
      if (this.leftResultSet != null) {
        this.leftResultSet.buildQueryPlan(builder, context.pushContext());
      }
      
      if (this.rightResultSet != null) {
        PlanUtils.xmlAddTag(builder, context, "Join");
        this.rightResultSet.buildQueryPlan(builder, context.pushContext());
      }
      
      return builder;
    }
    
}
