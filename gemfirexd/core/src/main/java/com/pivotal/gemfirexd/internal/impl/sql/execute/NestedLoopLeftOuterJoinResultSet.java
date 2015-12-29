/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.NestedLoopLeftOuterJoinResultSet

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

import java.util.TreeSet;

import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
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
 * the join's rows satisfying the filter as a result set
 * plus the rows from the left joined with a null row from
 * the right when there is no matching row in the right
 * result set.
 */
class NestedLoopLeftOuterJoinResultSet extends NestedLoopJoinResultSet
{
	protected GeneratedMethod emptyRowFun;
	/* Was this originally a right outer join? */
	private boolean wasRightOuterJoin;

	/* Have we found a matching row from the right yet? */
// GemStone changes BEGIN
	// in parent class
	//private boolean matchRight = false;
// GemStone changes END
	private boolean returnedEmptyRight = false;
	private ExecRow rightEmptyRow = null;

	public int emptyRightRowsReturned = 0;

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

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
	    DataValueDescriptor restrictBoolean;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (! isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

// GemStone changes BEGIN
		final TXState localTXState = this.localTXState;
                if (GemFireXDUtils.TraceOuterJoin) {
                  SanityManager.DEBUG_PRINT(
                      GfxdConstants.TRACE_OUTERJOIN_MERGING,
                      "NestedLoopLeftOuterJoinResultSet::getNextRowCore this is: "
                          + System.identityHashCode(this) + " leftresultset is: "
                          + leftResultSet + "(" + System.identityHashCode(leftResultSet)
                          + " rightresultset is: " + rightResultSet + "("
                          + System.identityHashCode(rightResultSet));
                }
                
        if (observer != null) {
          observer.onGetNextRowCore(this);
        }
        checkCancellationFlag();
// GemStone changes END
		/* Close right and advance left if we found no match
		 * on right on last next().
		 */
		if (returnedEmptyRight)
		{
			/* Current scan on right is exhausted.  Need to close old scan 
			 * and open new scan with new "parameters".  openRight will
	 		 * reopen the scan.
			 */
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
			returnedEmptyRight = false;
		}

		while (leftRow != null && !haveRow)
		{
		  this.rightResultSet.releasePreviousByteSource();
			rightRow = rightResultSet.getNextRowCore();

			if (rightRow == null)
			{
				/* If we haven't found a match on the right, then
				 * we join the left with a row of nulls from the
				 * right.
				 */
				if (! matchRight)
				{
					haveRow = true;
					returnedEmptyRight = true;
					if (rightEmptyRow == null)
					{
						rightEmptyRow = (ExecRow) emptyRowFun.invoke(activation);
					}

					getMergedRow(leftRow, rightEmptyRow);
					emptyRightRowsReturned++;
// GemStone changes BEGIN
					//if (localTXState != null) {
					  this.leftResultSet
					      .filteredRowLocationPostRead(localTXState);
					//}

					continue;
				}
				
			// GemStone changes END
				/* Current scan on right is exhausted.  Need to close old scan 
				 * and open new scan with new "parameters".  openRight()
				 * will reopen the scan.
				 */
				matchRight = false;
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
// GemStone changes BEGIN
						//if (localTXState != null) {
						  this.rightResultSet
						      .filteredRowLocationPostRead(localTXState);
						//}
// GemStone changes END
						continue;
					}
				}

				matchRight = true;

				getMergedRow(leftRow, rightRow);
				haveRow = true;
			}
		}

		/* Do we have a row to return? */
	    if (haveRow)
	    {
			result = mergedRow;
			setCurrentRow(mergedRow);
			rowsReturned++;
	    }
		else
		{
			clearCurrentRow();
		}

	    if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
	    return result;
	}

	protected void getMergedRow(ExecRow leftRow, ExecRow rightRow) 
			throws StandardException
	{
		int colInCtr;
		int colOutCtr;
		int leftNumCols;
		int rightNumCols;

		/* Reverse left and right for return of row if this was originally
		 * a right outer join.  (Result columns ordered according to
		 * original query.)
		 */
		if (wasRightOuterJoin)
		{
			ExecRow tmp;

			tmp = leftRow;
			leftRow = rightRow;
			rightRow = tmp;
			leftNumCols = this.rightNumCols;
			rightNumCols = this.leftNumCols;
		}
		else
		{
			leftNumCols = this.leftNumCols;
			rightNumCols = this.rightNumCols;
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
		for (colInCtr = 1; colInCtr <= rightNumCols; 
			 colInCtr++, colOutCtr++)
		{
			 mergedRow.setColumn(colOutCtr, 
								 rightRow.getColumn(colInCtr));
		}
		// Gemstone changes BEGIN
		if (mergedRow != null) {
		  mergedRow.clearAllRegionAndKeyInfo();
		  if (GemFireXDUtils.TraceOuterJoin) {
		    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_OUTERJOIN_MERGING,
		        "NestedLoopLeftOuterJoin::getMergedRow: left row rak: "
		        + leftRow.getAllRegionAndKeyInfo() + " for "
		        + ArrayUtils.objectRefString(leftRow) + "[ " + leftRow + " ], right row rak: "
		        + rightRow.getAllRegionAndKeyInfo() + " for "
		        + ArrayUtils.objectRefString(rightRow) + "[ " + rightRow + " ]");
		  }
		  mergedRow.addAllKeys(leftRow.getAllRegionAndKeyInfo());
		  mergedRow.addAllKeys(rightRow.getAllRegionAndKeyInfo());
		}
    		// Gemstone changes END
	}

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
		matchRight = false;
		returnedEmptyRight = false;
		rightEmptyRow = null;
		emptyRightRowsReturned = 0;
		super.clearScanState();
	}

	@Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    
	  //TODO:Asif: check if this condition is right for transaction too
    if (this.isRightOpen) {
      if(!this.returnedEmptyRight) {
        super.filteredRowLocationPostRead(localTXState);  
      }      
    }
  }

    /*
     * class interface
     *
     */
    NestedLoopLeftOuterJoinResultSet(
						NoPutResultSet leftResultSet,
						int leftNumCols,
						NoPutResultSet rightResultSet,
						int rightNumCols,
						Activation activation,
						GeneratedMethod restriction,
						int resultSetNumber,
						GeneratedMethod emptyRowFun,
						boolean wasRightOuterJoin,
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
			  oneRowRightSide, notExistsRightSide,
			  optimizerEstimatedRowCount, optimizerEstimatedCost, 
			  userSuppliedOptimizerOverrides,
                          // GemStone changes BEGIN
                          leftResultColumnNames, 
                          rightResultColumnNames
                          // GemStone changes END
			  );
		this.emptyRowFun = emptyRowFun;
		this.wasRightOuterJoin = wasRightOuterJoin;
    }
    
    @Override
    public StringBuilder buildQueryPlan(StringBuilder builder,
        PlanUtils.Context context) {
      
      final boolean isSuccess = context.setNested();
      
      super.buildQueryPlan(builder, context);
      
      if (!isSuccess) {
        return builder; // child will call after adding its own attributes if any. }
      }

      PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_JOIN_NL_LO);
      
      endBuildQueryPlan(builder, context);
  
      PlanUtils.xmlCloseTag(builder, context, this);        
      
      return builder;
    }
    
}
