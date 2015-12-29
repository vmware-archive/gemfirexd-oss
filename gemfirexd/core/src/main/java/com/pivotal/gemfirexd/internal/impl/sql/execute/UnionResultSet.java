/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.UnionResultSet

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
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;


/**
 * Takes two result sets and returns their union (all).
 * (Any duplicate elimination is performed above this ResultSet.)
 *
 */
class UnionResultSet extends NoPutResultSetImpl
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;

    private int whichSource = 1; // 1 or 2, == the source we are currently on.
    private int source1FinalRowCount = -1;

	// these are set in the constructor and never altered
    public NoPutResultSet source1;
    public NoPutResultSet source2;

// GemStone changes BEGIN
    public ValueRow candidateRow;
    private boolean ignoreSourceOneIfReplicate = false;
    private boolean ignoreSourceTwoIfReplicate = false;
// GemStone changes END

    //
    // class interface
    //
	/*
     * implementation alternative: an array of sources,
     * using whichSource to index into the current source.
     */
    public UnionResultSet(NoPutResultSet source1, NoPutResultSet source2, 
						  Activation activation, 
						  int resultSetNumber, 
					      double optimizerEstimatedRowCount,
						  double optimizerEstimatedCost) 
	{
		
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source1 = source1;
        this.source2 = source2;
        recordConstructorTime();
        
        // GemStone changes BEGIN
        printResultSetHierarchy();
        // GemStone changes END
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open the first source.
 	 *	@exception StandardException thrown on failure
     */
	public void	openCore() throws StandardException 
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "UnionResultSet already open");

        isOpen = true;
        source1.openCore();
		numOpens++;

// GemStone changes BEGIN 
                // We need to disable any source if its a replicated region, and incoming
                // region executor message says so to disable it (on all data nodes, except one)
		// But only on remote/data nodes
		final FunctionContext fc;
		if (lcc.isConnectionForRemote()
		    && (fc = activation.getFunctionContext()) != null
            // Since all tables are Replicated, no need for extra optimization
            && !((RegionExecutorMessage<?>)fc).allTablesAreReplicatedOnRemote()) {
                  if (!(fc instanceof RegionExecutorMessage<?>)) {
                    SanityManager.THROWASSERT("Given FunctionContext is expected "
                        + "to be of type RegionExecutorMessage, got " + fc);
                  }

	          if (((RegionExecutorMessage<?>)fc)
	              .doIgnoreReplicatesIfSetOperatorsOnRemote()) {  
	            SanityManager.ASSERT(source1 instanceof BasicNoPutResultSetImpl, 
	            "Member 'source1' is expected to be of type BasicNoPutResultSetImpl");
	            
	            ignoreSourceOneIfReplicate = ((BasicNoPutResultSetImpl)source1).isReplicateIfSetOpSupported();

	            SanityManager.ASSERT(source2 instanceof BasicNoPutResultSetImpl, 
	            "Member 'source2' is expected to be of type BasicNoPutResultSetImpl");
	            
	            ignoreSourceTwoIfReplicate = ((BasicNoPutResultSetImpl)source2).isReplicateIfSetOpSupported();
	          }		     
		}
//GemStone changes END
		    
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
     * If there are rows still on the first source, return the
     * next one; otherwise, switch to the second source and
     * return a row from there.
 	 *	@exception StandardException thrown on failure
	 */
	public ExecRow	getNextRowCore() throws StandardException {
	    ExecRow result = null;
    boolean isOffHeapEnabled = GemFireXDUtils.isOffHeapEnabled();
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen ) {
	        switch (whichSource) {
	            case 1 :
// GemStone changes BEGIN
	              /* (original code)
	               result = source1.getNextRowCore();
	               */	       
	              if (!ignoreSourceOneIfReplicate) {
	                if(isOffHeapEnabled) {
	                  this.source1.releasePreviousByteSource();
	                }
	                result = source1.getNextRowCore();
	              }
// GemStone changes END
	                     if ( result == (ExecRow) null ) {
	                        //source1FinalRowCount = source1.rowCount();
	                        source1.close(false);
	                        whichSource = 2;
	                        source2.openCore();
// GemStone changes BEGIN
	                        /* (original code)
	                         result = source2.getNextRowCore();
	                         */              
	                        if (!ignoreSourceTwoIfReplicate) {
	                          if(isOffHeapEnabled) {
	                            this.source2.releasePreviousByteSource();
	                          }
	                          result = source2.getNextRowCore();
	                        }
// GemStone changes END	                        
							if (result != null)
							{
								rowsSeenRight++;
							}
	                     }
						 else
						 {
							 rowsSeenLeft++;
						 }
	                     break;
	            case 2 :
	              if(isOffHeapEnabled) {
	                this.source2.releasePreviousByteSource();
	              }
	              result = source2.getNextRowCore();
						 if (result != null)
						 {
							rowsSeenRight++;
						 }
	                     break;
	            default: 
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT( "Bad source number in union" );
	                break;
	        }
	    }

		setCurrentRow(result);
		if (result != null)
		{
			rowsReturned++;
// GemStone changes BEGIN
			if (this.candidateRow == null) {
			  this.candidateRow = new ValueRow(result.nColumns());
			}
			this.candidateRow.setRowArray(result);
// GemStone changes END
		}

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
// GemStone changes BEGIN
		// This change has been introduced to handle the scenario where 
		// one source has ExecRow while other has ValueRow type; 
		// so better make both valueRow i.e deserialise them
		return result != null ? this.candidateRow : null;
	    /* (original code)
	    return result;
	    */
// GemStone changes END
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
		if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();

	        switch (whichSource) {
	            case 1 : source1.close(cleanupOnError);
	                     break;
	            case 2 : source2.close(cleanupOnError);
	                     source1FinalRowCount = -1;
	                     whichSource = 1;
	                     break;
	            default: 
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT( "Bad source number in union" );
	                break;
	        }

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of UnionResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	}

	public void	finish() throws StandardException
	{
		source1.finish();
		source2.finish();
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
			return	time - source1.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType) -
							  source2.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
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
		A union has a single underlying row at a time, although
		from one of several sources.
	
		@see CursorResultSet
	 
		@return the row location of the current cursor row.
		@exception StandardException thrown on failure
	 */
	public RowLocation getRowLocation() throws StandardException {
	    switch (whichSource) {
	        case 1 : 
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(source1 instanceof CursorResultSet, "source not CursorResultSet");
				return ((CursorResultSet)source1).getRowLocation();
	        case 2 : 
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(source2 instanceof CursorResultSet, "source2 not CursorResultSet");
				return ((CursorResultSet)source2).getRowLocation();
	        default: 
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT( "Bad source number in union" );
	            return null;
	    }
	}

	/**
		A union has a single underlying row at a time, although
		from one of several sources.
	
		@see CursorResultSet
	 
		@return the current row.
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException{
	    ExecRow result = null;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isOpen, "TSRS expected to be open");
			if (!(whichSource == 1 || whichSource == 2))
			{
				SanityManager.THROWASSERT("whichSource expected to be 1 or 2, not " 
					+ whichSource);
			}
		}

	    switch (whichSource) 
		{
	        case 1: 
				result = ((CursorResultSet) source1).getCurrentRow();
	            break;

	        case 2: 
				result = ((CursorResultSet) source2).getCurrentRow();
	            break;
        }

		setCurrentRow(result);
// GemStone changes BEGIN
	        // This change has been introduced to handle the scenario where 
                // one source has ExecRow while other has ValueRow type; 
                // so better make both valueRow i.e deserialise them
		if (result != null) {
		  if (this.candidateRow == null) {
		    this.candidateRow = new ValueRow(result.nColumns());
		  }
		  this.candidateRow.setRowArray(result);
		  return this.candidateRow;
		}
		else {
		  return null;
		}
		/* (original code)
	    return result;
		 */
// GemStone changes END
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    switch (this.whichSource) {
      case 1:
        this.source1.updateRowLocationPostRead();
        break;
      case 2:
        this.source2.updateRowLocationPostRead();
        break;
      default:
        SanityManager.THROWASSERT("Bad source number " + this.whichSource
            + " in union");
    }
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    switch (this.whichSource) {
      case 1:
        this.source1.filteredRowLocationPostRead(localTXState);
        break;
      case 2:
        this.source2.filteredRowLocationPostRead(localTXState);
        break;
      default:
        SanityManager.THROWASSERT("Bad source number " + this.whichSource
            + " in union");
    }
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    int numChildren = 0;
    if (source1 != null) {
      numChildren++;
    }
    
    if(source2 != null) {
      numChildren++;
    }
    
    visitor.setNumberOfChildren(numChildren);
    
    visitor.visit(this);
    
    if(source1 != null) {
      source1.accept(visitor);
    }
    
    if(source2 != null) {
      source2.accept(visitor);
    }
    
  }
  
  @Override
  public void resetStatistics() {
    rowsSeenLeft = 0;
    rowsSeenRight = 0;
    rowsReturned = 0;
    super.resetStatistics();
    source1.resetStatistics();
    source2.resetStatistics();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {

    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_UNION);
    
    if(this.source1 != null)
      this.source1.buildQueryPlan(builder, context.pushContext());
    
    if(this.source2 != null) {
      PlanUtils.xmlAddTag(builder, context, "union");
      this.source2.buildQueryPlan(builder, context.pushContext());
    }
    
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
                    + " with left-source = "
                    + (this.source1 != null ? this.source1.getClass()
                        .getSimpleName() : null)
                    + " and left-source ResultSetNumber = "
                    + (this.source1 != null
                        && this.source1 instanceof NoPutResultSetImpl ? ((NoPutResultSetImpl)this.source1)
                        .resultSetNumber() : -1)
                    + " with right-source = "
                    + (this.source2 != null ? this.source2.getClass()
                        .getSimpleName() : null)
                    + " and right-source ResultSetNumber = "
                    + (this.source2 != null
                        && this.source2 instanceof NoPutResultSetImpl ? ((NoPutResultSetImpl)this.source2)
                        .resultSetNumber() : -1));
      }
    }
  }
// GemStone changes END
}
