/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.SetOpResultSet

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

import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.message.RegionExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.Orderable;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.compile.IntersectOrExceptNode;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Takes the result set produced by an ordered UNION ALL of two tagged result sets and produces
 * the INTERSECT or EXCEPT of the two input result sets. This also projects out the tag, the last column
 * of the input rows.
 */
class SetOpResultSet extends NoPutResultSetImpl
    implements CursorResultSet
{
    private final NoPutResultSet leftSource;
    private final NoPutResultSet rightSource;
    private final Activation activation;
    private final int opType;
    private final boolean all;
    private final int resultSetNumber;
    private DataValueDescriptor[] prevCols; /* Used to remove duplicates in the EXCEPT DISTINCT case.
                                             * It is equal to the previously output columns.
                                             */
    private int rightDuplicateCount; // Number of duplicates of the current row from the right input
    private ExecRow leftInputRow;
    private ExecRow rightInputRow;
// GemStone changes BEGIN
    private boolean addRegionAndKey;
    private boolean consumeLeftInputRow;
    private boolean consumeRightInputRow;
    private boolean ignoreLeftSourceIfReplicate;
    private boolean ignoreRightSourceIfReplicate;
// GemStone changes END
    private final int[] intermediateOrderByColumns;
    private final int[] intermediateOrderByDirection;
    private final boolean[] intermediateOrderByNullsLow;

    /* Run time statistics variables */
    private int rowsSeenLeft;
    private int rowsSeenRight;
    private int rowsReturned;

    SetOpResultSet( NoPutResultSet leftSource,
                    NoPutResultSet rightSource,
                    Activation activation, 
                    int resultSetNumber,
                    long optimizerEstimatedRowCount,
                    double optimizerEstimatedCost,
                    int opType,
                    boolean all,
                    int intermediateOrderByColumnsSavedObject,
                    int intermediateOrderByDirectionSavedObject,
                    int intermediateOrderByNullsLowSavedObject)
    {
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.activation = activation;
        this.resultSetNumber = resultSetNumber;
        this.opType = opType;
        this.all = all;

        //ExecPreparedStatement eps = activation.getPreparedStatement();
        intermediateOrderByColumns = (int[]) activation.getSavedObject(intermediateOrderByColumnsSavedObject);
        intermediateOrderByDirection = (int[]) activation.getSavedObject(intermediateOrderByDirectionSavedObject);
        intermediateOrderByNullsLow = (boolean[]) activation.getSavedObject(intermediateOrderByNullsLowSavedObject);
        recordConstructorTime();
        
        // GemStone changes BEGIN
        printResultSetHierarchy();
        // GemStone changes END
    }

	/**
     * open the first source.
 	 *	@exception StandardException thrown on failure
     */
	public void	openCore() throws StandardException 
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "SetOpResultSet already open");

        isOpen = true;
        leftSource.openCore();
        rightSource.openCore();
// GemStone changes BEGIN
        // ensure one initLocalTXState() call before getNextRowCore()
        initLocalTXState();
        final FunctionContext fc;
        if (lcc.isConnectionForRemote()
            && (fc = activation.getFunctionContext()) != null
            // Since all tables are Replicated, no need for extra optimization
            && !((RegionExecutorMessage<?>)fc).allTablesAreReplicatedOnRemote()) {
          // On Data Node
          consumeRightInputRow = consumeLeftInputRow = true;

          // We need to disable the sources if its a replicated region and incoming
          // region executor message says so to disable it (on all data nodes, except one)
          if (!(fc instanceof RegionExecutorMessage<?>)) {
            SanityManager.THROWASSERT("Given FunctionContext is expected "
                + "to be of type RegionExecutorMessage but got " + fc);
          }

          if (((RegionExecutorMessage<?>)fc)
              .doIgnoreReplicatesIfSetOperatorsOnRemote()) {  
            SanityManager.ASSERT(leftSource instanceof BasicNoPutResultSetImpl, 
            "Member 'leftSource' is expected to be of type BasicNoPutResultSetImpl");

            ignoreLeftSourceIfReplicate = ((BasicNoPutResultSetImpl)leftSource).isReplicateIfSetOpSupported();

            SanityManager.ASSERT(rightSource instanceof BasicNoPutResultSetImpl, 
            "Member 'rightSource' is expected to be of type BasicNoPutResultSetImpl");

            ignoreRightSourceIfReplicate = ((BasicNoPutResultSetImpl)rightSource).isReplicateIfSetOpSupported();
          }
          
          addRegionAndKey = true;
        }
        else
        {
        rightInputRow = rightSource.getNextRowCore();
// GemStone changes END
          if (rightInputRow != null)
          {
              rowsSeenRight++;
          }
// GemStone changes BEGIN
        }
// GemStone changes END
		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	} // end of openCore

// GemStone changes BEGIN
	/*
	 * Handle Intersect Or Except queries on Remote/data nodes
	 * Actually a sort merge upon incoming ordered rows 
	 * No rows are rejected - As None of the rows may get rejected, 
	 * we do not need to implement any reference to transaction
	 * related calls as seen in getNextRowCore.
	 */
    private ExecRow getNextRowCoreForRemote() throws StandardException
    {
          beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
          if ( isOpen )
      {
          
          if (consumeLeftInputRow && !ignoreLeftSourceIfReplicate) 
          {
            leftInputRow = leftSource.getNextRowCore();
            if (leftInputRow != null)
            {
                rowsSeenLeft++;
            }
            consumeLeftInputRow = false;
          }
          
          if (consumeRightInputRow && !ignoreRightSourceIfReplicate) 
          {
              rightInputRow = rightSource.getNextRowCore();
              if (rightInputRow != null)
              {
                  rowsSeenRight++;
              }
              consumeRightInputRow = false;
          }
          
          if (leftInputRow != null)
          {
              if (rightInputRow != null && (compare(leftInputRow.getRowArray(), rightInputRow.getRowArray())) > 0)
              {
                setCurrentRow(rightInputRow);
                updateCurrentRowWithRegionAndKey(false); // left - true, right - false
                consumeRightInputRow = true;
              }
              else 
              {
                setCurrentRow(leftInputRow);
                updateCurrentRowWithRegionAndKey(true); // left - true, right - false
                consumeLeftInputRow = true;
              }
          } else  if (rightInputRow != null) {
            setCurrentRow(rightInputRow);
            updateCurrentRowWithRegionAndKey(false); // left - true, right - false
            consumeRightInputRow = true;
          } else {
            setCurrentRow(null);//null
          }
      }
          
      if (currentRow != null) {
         rowsReturned++;
      }

      if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
      return currentRow;  
    }
// GemStone changes END
        
	/**
     * @return the next row of the intersect or except, null if there is none
 	 *	@exception StandardException thrown on failure
	 */
	public ExecRow	getNextRowCore() throws StandardException
    {
// GemStone changes BEGIN
          if (lcc.isConnectionForRemote()
              && activation.getFunctionContext() != null
              // Since all tables are Replicated, no need for extra optimization
              && !((RegionExecutorMessage<?>)activation.getFunctionContext())
              .allTablesAreReplicatedOnRemote()) {
            return getNextRowCoreForRemote();
          }
// GemStone changes END
          
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen )
        {
// GemStone changes BEGIN
	    final TXState localTXState = this.localTXState;
// GemStone changes END
            while( (leftInputRow = leftSource.getNextRowCore()) != null)
            {
                rowsSeenLeft++;

                DataValueDescriptor[] leftColumns = leftInputRow.getRowArray();
                if( !all)
                {
                    if( isDuplicate( leftColumns))
                        continue; // Get the next left row
                    prevCols = leftInputRow.getRowArrayClone();
                }
                int compare = 0;
                // Advance the right until there are no more right rows or leftRow <= rightRow
                while ( rightInputRow != null && (compare = compare(leftColumns, rightInputRow.getRowArray())) > 0)
                {
// GemStone changes BEGIN
                 // if (localTXState != null) {
                    this.rightSource.filteredRowLocationPostRead(localTXState);
                 // }
// GemStone changes END
                    rightInputRow = rightSource.getNextRowCore();
                    if (rightInputRow != null)
                    {
                        rowsSeenRight++;
                    }
                }

                if( rightInputRow == null || compare < 0)
                {
                    // The left row is not in the right source.
                    if( opType == IntersectOrExceptNode.EXCEPT_OP)
                        // Output this row
                        break;
                }
                else
                {
                    // The left and right rows are the same
                    if( SanityManager.DEBUG)
                        SanityManager.ASSERT( rightInputRow != null && compare == 0,
                                              "Intersect/Except execution has gotten confused.");
                    if ( all)
                    {
                        // Just advance the right input by one row.
// GemStone changes BEGIN
                       // if (localTXState != null) {
                          this.rightSource.filteredRowLocationPostRead(localTXState);
                        //}
// GemStone changes END
                        rightInputRow = rightSource.getNextRowCore();
                        if (rightInputRow != null)
                        {
                            rowsSeenRight++;
                        }
                    }

                    // If !all then we will skip past duplicates on the left at the top of this loop,
                    // which will then force us to skip past any right duplicates.
                    if( opType == IntersectOrExceptNode.INTERSECT_OP)
                        break; // output this row

                    // opType == IntersectOrExceptNode.EXCEPT_OP
                    // This row should not be ouput
                }
            }
        }

        setCurrentRow(leftInputRow);

        if (currentRow != null) {
           rowsReturned++;
        }

        if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
        return currentRow;
    } // end of getNextRowCore

    private void advanceRightPastDuplicates( DataValueDescriptor[] leftColumns)
        throws StandardException
    {
        while ((rightInputRow = rightSource.getNextRowCore()) != null)
        {
            rowsSeenRight++;

            if (compare(leftColumns, rightInputRow.getRowArray()) == 0) 
                continue;
        }
    } // end of advanceRightPastDuplicates
        
    private int compare( DataValueDescriptor[] leftCols, DataValueDescriptor[] rightCols)
        throws StandardException
    {
        for( int i = 0; i < intermediateOrderByColumns.length; i++)
        {
            int colIdx = intermediateOrderByColumns[i];
            if( leftCols[colIdx].compare( Orderable.ORDER_OP_LESSTHAN,
                                          rightCols[colIdx],
                                          true, // nulls should be ordered
                                          intermediateOrderByNullsLow[i],
                                          false))
                return -1 * intermediateOrderByDirection[i];
            if( ! leftCols[colIdx].compare( Orderable.ORDER_OP_EQUALS,
                                            rightCols[colIdx],
                                            true, // nulls should be ordered
                                            intermediateOrderByNullsLow[i],
                                            false))
                return intermediateOrderByDirection[i];
        }
        return 0;
    } // end of compare
    
    private boolean isDuplicate( DataValueDescriptor[] curColumns)
        throws StandardException
    {
        if( prevCols == null)
            return false;
        /* Note that intermediateOrderByColumns.length can be less than prevCols.length if we know that a
         * subset of the columns is a unique key. In that case we only need to look at the unique key.
         */
        for( int i = 0; i < intermediateOrderByColumns.length; i++)
        {
            int colIdx = intermediateOrderByColumns[i];
            if( ! curColumns[colIdx].compare( Orderable.ORDER_OP_EQUALS, prevCols[colIdx], true, false))
                return false;
        }
        return true;
    }

	public ExecRow getCurrentRow()
    {
        return currentRow;
    }
    
	/**
	 * If the result set has been opened,
	 * close the currently open source.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if ( isOpen )
        {
	    	clearCurrentRow();
            prevCols = null;
            leftSource.close(cleanupOnError);
            rightSource.close(cleanupOnError);
            super.close(cleanupOnError);
        }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SetOpResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	} // end of close

	public void	finish() throws StandardException
	{
		leftSource.finish();
		rightSource.finish();
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
			return	time - leftSource.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType)
              - rightSource.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
		}
		else
		{
                        // GemStone changes BEGIN
                        return timeType == ALL ? (time - constructorTime) : time;
                        /*(original code) return totTime; */
                        // GemStone changes END
		}
	} // end of getTimeSpent

	/**
     * @see CursorResultSet
	 *
     * @return the row location of the current cursor row.
     * @exception StandardException thrown on failure
	 */
	public RowLocation getRowLocation() throws StandardException
    {
        // RESOLVE: What is the row location of an INTERSECT supposed to be: the location from the
        // left side, the right side, or null?
        return ((CursorResultSet)leftSource).getRowLocation();
    }

    /**
     * Return the set operation of this <code>SetOpResultSet</code>
     *
     * @return the set operation of this ResultSet, the value is either 
     *         <code>IntersectOrExceptNode.INTERSECT_OP</code> for 
     *         Intersect operation or <code>IntersectOrExceptNode.EXCEPT_OP
     *         </code> for Except operation
     *         
     * @see    com.pivotal.gemfirexd.internal.impl.sql.compile.IntersectOrExceptNode
     */
    public int getOpType()
    {
        return opType;
    }

    /**
     * Return the result set number
     *
     * @return the result set number
     */
    public int getResultSetNumber()
    {
        return resultSetNumber;
    }

    /**
     * Return the left source input of this <code>SetOpResultSet</code>
     *
     * @return the left source input of this <code>SetOpResultSet</code>
     * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet
     */
    public NoPutResultSet getLeftSourceInput()
    {
        return leftSource;
    }

    /**
     * Return the right source input of this <code>SetOpResultSet</code>
     *
     * @return the right source input of this <code>SetOpResultSet</code>
     * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet
     */
    public NoPutResultSet getRightSourceInput()
    {
        return rightSource;
    }

    /**
     * Return the number of rows seen on the left source input
     *
     * @return the number of rows seen on the left source input
     */
    public int getRowsSeenLeft()
    {
        return rowsSeenLeft;
    }

    /**
     * Return the number of rows seen on the right source input
     *
     * @return the number of rows seen on the right source input
     */
    public int getRowsSeenRight()
    {
        return rowsSeenRight;
    }

    /**
     * Return the number of rows returned from the result set
     *
     * @return the number of rows returned from the result set
     */
    public int getRowsReturned()
    {
        return rowsReturned;
    }

// GemStone changes BEGIN

    @Override
    public void updateRowLocationPostRead() throws StandardException {
      this.leftSource.updateRowLocationPostRead();
    }

    @Override
    public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
      this.leftSource.filteredRowLocationPostRead(localTXState);
    }

    @Override
    public void accept(ResultSetStatisticsVisitor visitor) {
      int noOfChildren = 0;
      if(leftSource != null) {
        noOfChildren++;
      }
      if(rightSource != null) {
        noOfChildren++;
      }
      visitor.setNumberOfChildren(noOfChildren);
      
      visitor.visit(this);
      
      if(leftSource != null) {
        leftSource.accept(visitor);
      }
      
      if(rightSource != null) {
        rightSource.accept(visitor);
      }
    }
    
    @Override
    public void resetStatistics() {
      rowsSeenLeft = 0;
      rowsSeenRight = 0;
      rowsReturned = 0;
      super.resetStatistics();
      leftSource.resetStatistics();
      rightSource.resetStatistics();
    }
    
    /**
     * Update the current row to the row passed in. New information is helpful in
     * processing intersect/execute
     * 
     * @param boolean isLeftSideTreeOfSetOperatorNode
     */
    public final void updateCurrentRowWithRegionAndKey(
        boolean isLeftSideTreeOfSetOperatorNode) {
      if (currentRow == null) {
        SanityManager.THROWASSERT(
            "Set Operator handling: currentRow should not be null");
      }
      if (currentRow.getAllRegionAndKeyInfo() != null) {
        SanityManager.THROWASSERT(" Set Operator handling: currentRow should "
                + "have no region and key information ");
      }

      if (this.addRegionAndKey) {
        currentRow
            .addRegionAndKey(isLeftSideTreeOfSetOperatorNode ? RegionAndKey.TRUE
                : RegionAndKey.FALSE);
      }
    }
    
    @Override
    public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
      super.buildQueryPlan(builder, context);
      
      final String operator;
      if (this.opType == IntersectOrExceptNode.INTERSECT_OP) {
        operator = (all ? PlanUtils.OP_SET_INTERSECT_ALL : PlanUtils.OP_SET_INTERSECT);
      }
      else {
        operator = (all ? PlanUtils.OP_SET_EXCEPT_ALL : PlanUtils.OP_SET_EXCEPT);
      }
      PlanUtils.xmlAttribute(builder, "operator", operator);
      
      PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_SET_EXCEPT);
      
      if(this.leftSource != null)
        this.leftSource.buildQueryPlan(builder, context.pushContext());
      
      if(this.rightSource != null) {
        PlanUtils.xmlAddTag(builder, context, "setop");
        this.rightSource.buildQueryPlan(builder, context.pushContext());
      }
      
      PlanUtils.xmlCloseTag(builder, context, this);
      
      return builder;
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
                + (this.leftSource != null ? this.leftSource.getClass()
                    .getSimpleName() : null)
                + " and left-source ResultSetNumber = "
                + (this.leftSource != null ? this.leftSource.resultSetNumber()
                    : -1)
                + " with right-source = "
                + (this.rightSource != null ? this.rightSource.getClass()
                    .getSimpleName() : null)
                + " and right-source ResultSetNumber = "
                + (this.rightSource != null ? this.rightSource
                    .resultSetNumber() : -1));
      }
    }
  }
// GemStone changes END
}
