/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ProjectRestrictResultSet

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

import java.util.ArrayList;
import java.util.TreeSet;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.catalog.types.ReferencedColumnsDescriptorImpl;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;


/**
 * Takes a table and a table filter and returns
 * the table's rows satisfying the filter as a result set.
 *
 */
public class ProjectRestrictResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{
	/* Run time statistics variables */
	public long restrictionTime;
	public long projectionTime;

    // set in constructor and not altered during
    // life of object.
    final NoPutResultSet source;
	public GeneratedMethod constantRestriction;
    public GeneratedMethod restriction;
	public boolean doesProjection;
    private GeneratedMethod projection;
	private int[]			projectMapping;
// GemStone changes BEGIN
	private boolean hasProjectMapping;
	private AbstractCompactExecRow rawRow;
	private boolean isOptimized;
	public String projectedColumns;     // The names of the columns that are projected
	                                     // Used for EXPLAIN
// GemStone changes END
	private boolean runTimeStatsOn;
	private ExecRow			mappedResultRow;
	public boolean reuseResult;

	private boolean shortCircuitOpen;

	private ExecRow projRow;

    //
    // class interface
    //
    ProjectRestrictResultSet(NoPutResultSet s,
					Activation a,
					GeneratedMethod r,
					GeneratedMethod p,
					int resultSetNumber,
					GeneratedMethod cr,
					int mapRefItem,
					boolean reuseResult,
					boolean doesProjection,
// GemStone changes BEGIN
					boolean isOptimized,
					String projectedColumns,
// GemStone changes END
				    double optimizerEstimatedRowCount,
					double optimizerEstimatedCost) 
		throws StandardException
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"PRRS(), source expected to be non-null");
		}
        restriction = r;
        projection = p;
		constantRestriction = cr;
		projectMapping = ((ReferencedColumnsDescriptorImpl) a.getSavedObject(mapRefItem)).getReferencedColumnPositions();
		this.reuseResult = reuseResult;
		this.doesProjection = doesProjection;

		// Allocate a result row if all of the columns are mapped from the source
		if (projection == null)
		{
			mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
// GemStone changes BEGIN
			this.hasProjectMapping = false;
			for (int col : this.projectMapping) {
			  if (col != -1) {
			    this.hasProjectMapping = true;
			    break;
			  }
			}
		}
                /* we need to infer whether whole row is getting projected out one-to-one
                 * to the underlying table or not. Issue is when Indexes are being used
                 * ResultColumnList#allExpressionsAreColumns() returns false as VirtualColumn 
                 * source RS is BaseTable and ResultSetNode (ProjectRestrictNode) source is
                 * IndexToBaseRow.
                 *     
                 * e.g. select * from Instrument where <....>, this ResultSet is created with 
                 * doesProjection as 'true' and also projection is not null. Looking at the 
                 * activation class dump its obvious projection here is redundant but isRedundant
                 * flag is not set because it does filter outs. 
                 * (@see ProjectRestrictNode#nopProjectRestrict).
                 * 
                 * The check is done in two steps. Step 1 is here and second
                 * step is performed in first call to doProjection() 
                 * step 1. see projectMapping is continuous and monotonically
                 * increasing.
                 * 
                 * step 2. whether all the columns of the table is being selected or
                 * not. @see doProjection(). 
                 */
		else {
		  boolean isProjectionMapsToStore = true;
		  this.hasProjectMapping = false;
		  for (int idx = 0; idx < projectMapping.length; idx++) {
		    /*  
		     * Assumption is, if project mapping arrays contains 1,2,3... 
		     * then definitely its mapping one to one to the store columns
		     * and hence need not be copied into.
		     */
		    int map = projectMapping[idx];
		    if (map != -1) {
		      this.hasProjectMapping = true;
		    }
		    if (map != (idx + 1)) {
		      isProjectionMapsToStore = false;
		      if (this.hasProjectMapping) {
		        break;
		      }
		    }
		  }
		  this.doesProjection = !isProjectionMapsToStore;
		}

		this.isOptimized = isOptimized;
		this.projectedColumns = projectedColumns;
		/* Remember whether or not RunTimeStatistics is on */
		runTimeStatsOn = this.lcc.getRunTimeStatisticsMode();
		// ensure one initLocalTXState() call before getNextRowCore()
		initLocalTXState();
		/* (original code)
		/* Remember whether or not RunTimeStatistics is on *
		runTimeStatsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();
		*/
// GemStone changes END
		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
    }

	//
	// NoPutResultSet interface 
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		boolean constantEval = true;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"PRRS().openCore(), source expected to be non-null");
		}

		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "ProjectRestrictResultSet already open");

		isOpen = true;
		if (constantRestriction != null)
		{
		    DataValueDescriptor restrictBoolean;
            restrictBoolean = (DataValueDescriptor) 
					constantRestriction.invoke(activation);

	            // if the result is null, we make it false --
				// so the row won't be returned.
            constantEval = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

		if (constantEval)
		{
	        source.openCore();
	        }
		else
		{
			shortCircuitOpen = true;
		}
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
		    SanityManager.ASSERT(isOpen, "ProjectRestrictResultSet not open, cannot reopen");

		if (constantRestriction != null)
		{
		    DataValueDescriptor restrictBoolean;
            restrictBoolean = (DataValueDescriptor) 
					constantRestriction.invoke(activation);

	            // if the result is null, we make it false --
				// so the row won't be returned.
            constantEval = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

		if (constantEval)
		{
	        source.reopenCore();
		}
		else
		{
			shortCircuitOpen = true;
		}
	    isOpen = true;

		numOpens++;

		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException {

	    ExecRow candidateRow = null;
	    ExecRow result = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;
		long	beginRT = 0;

		/* Return null if open was short circuited by false constant expression */
		if (shortCircuitOpen)
		{
			return result;
		}

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
// GemStone changes BEGIN
		final TXState localTXState = this.localTXState;
// GemStone changes END
	    do 
		{
			candidateRow = source.getNextRowCore();
			if (candidateRow != null) 
			{
				beginRT = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
				/* If restriction is null, then all rows qualify */
				if (restriction == null)
				{
					restrict = true;
				}
				else
				{
					setCurrentRow(candidateRow);
		            restrictBoolean = (DataValueDescriptor) 
											restriction.invoke(activation);
					if (statisticsTimingOn) restrictionTime += getElapsedNanos(beginRT);

		            // if the result is null, we make it false --
					// so the row won't be returned.
				    restrict = ((! restrictBoolean.isNull()) &&
								 restrictBoolean.getBoolean());
					if (! restrict)
					{
						rowsFiltered++;
// GemStone changes BEGIN
						
						filteredRowLocationPostRead(localTXState);
						
// GemStone changes END
					}
				}

				/* Update the run time statistics */
				rowsSeen++;
			}
	    } while ( (candidateRow != null) &&
	              (! restrict ) );

	    if (candidateRow != null) 
		{
			beginRT = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

			result = doProjection(candidateRow);//doProjectAndRelease(candidateRow);

			if (statisticsTimingOn) projectionTime += getElapsedNanos(beginRT);
// GemStone changes BEGIN
			TreeSet<RegionAndKey> regionAndKeyInfo = candidateRow
			    .getAllRegionAndKeyInfo();
			if (regionAndKeyInfo != null) {
			  result.clearAllRegionAndKeyInfo();
			  result.addAllKeys(regionAndKeyInfo);
			}
			if (localTXState != null && this.isTopResultSet &&
			    isForUpdate()) {
			  updateRowLocationPostRead();
			}
// GemStone changes END
        }
		/* Clear the current row, if null */
		else
		{
			clearCurrentRow();
		}


		currentRow = result;

		if (runTimeStatsOn)
		{
			if (! isTopResultSet)
			{
				/* This is simply for RunTimeStats */
				/* We first need to get the subquery tracking array via the StatementContext */
// GemStone changes BEGIN
				StatementContext sc = this.lcc.getStatementContext();
				/* (original code)
				StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
				*/
// GemStone changes END
				subqueryTrackingArray = sc.getSubqueryTrackingArray();
			}
			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		}
    	return result;
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

		if (type == CURRENT_RESULTSET_ONLY)
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

	// ResultSet interface

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{

	  // GemStone changes BEGIN
          if (!isOpen)
            return;
          
	  beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	  // GemStone changes END
	  
		/* Nothing to do if open was short circuited by false constant expression */
		if (shortCircuitOpen)
		{
			shortCircuitOpen = false;
			source.close(cleanupOnError);
			// GemStone changes BEGIN
                        if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
                        isOpen = false;
			// GemStone changes END
			return;
		}

		// GemStone changes BEGIN
		//moved to the beginning beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		// GemStone changes END
	    if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();

	        source.close(cleanupOnError);

			super.close(cleanupOnError);
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ProjectRestrictResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	}

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
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
	 * @exception StandardException thrown on failure.
	 */
	public RowLocation getRowLocation() throws StandardException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source is not CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
	}

	/**
	 * Gets last row returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException {
	    ExecRow candidateRow = null;
	    ExecRow result = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "PRRS is expected to be open");

		/* Nothing to do if we're not currently on a row */
		if (currentRow == null)
		{
			return null;
		}

		/* Call the child result set to get it's current row.
		 * If no row exists, then return null, else requalify it
		 * before returning.
		 */
		candidateRow = ((CursorResultSet) source).getCurrentRow();
		if (candidateRow != null) {
			setCurrentRow(candidateRow);
				/* If restriction is null, then all rows qualify */
            restrictBoolean = (DataValueDescriptor) 
					((restriction == null) ? null : restriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

	    if (candidateRow != null && restrict) 
	  	{
			  result =  doProjection(candidateRow); //doProjectAndRelease(candidateRow);
      }

		currentRow = result;
		/* Clear the current row, if null */
		if (result == null) {
			clearCurrentRow();
		}

		return currentRow;
	}

	

	/**
	 * Do the projection against the source row.  Use reflection
	 * where necessary, otherwise get the source column into our
	 * result row.
	 *
	 * @param sourceRow		The source row.
	 *
	 * @return		The result of the projection
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow doProjection(ExecRow sourceRow)
		throws StandardException
	{
		// No need to use reflection if reusing the result
		if (reuseResult && projRow != null)
		{
			/* Make sure we reset the current row based on the re-used
			 * result.  Otherwise, if the "current row" for this result
			 * set was nulled out in a previous call to getNextRow(),
			 * which can happen if this node is the right-side of
			 * a left outer join, the "current row" stored for this
			 * result set in activation.row would remain null, which
			 * would be wrong. DERBY-3538.
			 */
			setCurrentRow(projRow);
			return projRow;
		}

		ExecRow result;
		
// GemStone changes BEGIN
		/*
		 * if we are at top most ResultSet and got a CompactExecRow, definitely
		 * the underlying data is in byte form and need not deserialize until
		 * applying projection necessitates that. This is only possible in
		 * query service node because ResultHolder will create ValueRow at the 
		 * query node end.
		 * 
		 * For a single data node system, CompactExecRow will go out to the user 
		 * application and getColumn will deserialize at that moment. Thats
		 * why .getClone() is required, otherwise we can get away with it.
		 * 
		 * TODO: need to further check on this while enabling GroupBy/Having where multi level
		 * projection RS gets created.
		 */
		if (isTopResultSet && !doesProjection
		    && (sourceRow instanceof AbstractCompactExecRow)) {
		    /*
		     * step 2: of the ProjectRestrictResultSet() constructor check,
		     * whether column count is same or not. 
		     */
		  final AbstractCompactExecRow row =
		      (AbstractCompactExecRow)sourceRow;
		  if (row.nColumns() != projectMapping.length) {
		    doesProjection = true;
		  }
		  else {
		    if (this.rawRow == null) {
		      this.rawRow = row.getClone();
		    }
		    else {
		      this.rawRow.setRowArray(row);
		    }
		    result = this.rawRow;

		    //*** Repeating rest of the code from setCurrent onwards ***
		    /* We need to reSet the current row after doing the projection */
		    setCurrentRow(result);

		    /* Remember the result if reusing it */
		    if (reuseResult) {
		      projRow = result;
		    }
		    return result;
		  }
		}

		// TODO: PERF: see how to get rid of below
		/*
		if (this.projection != null) {
		  result = (ExecRow)projection.invoke(activation);
		}
		else {
		  result = mappedResultRow;
		}
		*/
		if (this.mappedResultRow != null) {
		  result = this.mappedResultRow;
		}
		else {
		  assert this.projection != null;
		  // Use reflection to do as much of projection as required
		  if (!this.isOptimized) {
		    result = (ExecRow)this.projection.invoke(this.activation);
		  }
		  else {
		    result = this.mappedResultRow = (ExecRow)this.projection
		        .invoke(this.activation);
		  }
		}
		// Copy any mapped columns from the source
		if (this.hasProjectMapping) {
		  for (int index = 0; index < projectMapping.length; index++) {
		    final int col = this.projectMapping[index];
		    if (col != -1) {
		      // TODO: PERF: see how to get rid of below when sourceRow
		      // is CompactExecRow (instead use RowFormatter with
		      // projection so avoid any copy, but do copy bytes if value
		      // was faulted in from disk to reduce memory footprint)
		      result.setColumn(index + 1, sourceRow.getColumn(col));
		    }
		  }
		}
		/* (original code)
		// Use reflection to do as much of projection as required
		if (projection != null)
		{
	        result = (ExecRow) projection.invoke(activation);
		}
		else
		{
			result = mappedResultRow;
		}

		// Copy any mapped columns from the source
		for (int index = 0; index < projectMapping.length; index++)
		{
			if (projectMapping[index] != -1)
			{
				result.setColumn(index + 1, sourceRow.getColumn(projectMapping[index]));
			}
		}
		*/
// GemStone changes END

		/* We need to reSet the current row after doing the projection */
		setCurrentRow(result);

		/* Remember the result if reusing it */
		if (reuseResult)
		{
			projRow = result;
		}
		return result;
	}

	/**
	 * Do the projection against the sourceRow. If the source of the result set
	 * is of type ProjectRestrictResultSet, the projection by that result set
	 * will also be performed.
	 *
	 * @param sourceRow row to be projected
	 *
	 * @return The result of the projection
	 *
	 * @exception StandardException thrown on failure.
	 */
	public ExecRow doBaseRowProjection(ExecRow sourceRow)
		throws StandardException
	{
		final ExecRow result;
		if (source instanceof ProjectRestrictResultSet) {
			ProjectRestrictResultSet prs = (ProjectRestrictResultSet) source;
			result = prs.doBaseRowProjection(sourceRow);
		} else {
			result = sourceRow.getNewNullRow();
			result.setRowArray(sourceRow.getRowArray());
		}
		return doProjection(result);
	}

	/**
	 * Get projection mapping array. The array consist of indexes which
	 * maps the column in a row array to another position in the row array.
	 * If the value is projected out of the row, the value is negative.
	 * @return projection mapping array.
	 */
	public int[] getBaseProjectMapping() 
	{
		final int[] result;
		if (source instanceof ProjectRestrictResultSet) {
			result = new int[projectMapping.length];
			final ProjectRestrictResultSet prs = (ProjectRestrictResultSet) source;
			final int[] sourceMap = prs.getBaseProjectMapping();
			for (int i=0; i<projectMapping.length; i++) {
				if (projectMapping[i] > 0) {
					result[i] = sourceMap[projectMapping[i] - 1];
				}
			}
		} else {
			result = projectMapping;
		}
		return result;
	} 
	
	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return source.isForUpdate();
	}

	/**
	 * @see NoPutResultSet#updateRow
	 */
	public void updateRow (ExecRow row) throws StandardException {
// GemStone changes BEGIN
	  // set args in activation if required
	  if (source.canUpdateInPlace()) {
	    ((BaseActivation)this.activation).setProjectMapping(
	        this.projectMapping);
	  }
	  source.updateRow(row);
	  /* (original derby code) source.updateRow(row); */
	}

	public boolean canUpdateInPlace() {
	  return source.canUpdateInPlace();
	}
// GemStone changes END

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 */
	public void markRowAsDeleted() throws StandardException {
		source.markRowAsDeleted();
	}

// GemStone changes BEGIN

	@Override
	public void updateRowLocationPostRead() throws StandardException {
	  this.source.updateRowLocationPostRead();
	}

	@Override
	public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
	  this.source.filteredRowLocationPostRead(localTXState);
	}

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
          
          NoPutResultSet[] subqueries = this.subqueryTrackingArray;
          int subqueryTrackingArrayLength = (subqueries == null) ? 0
              : subqueries.length;
          
          int noChildren = 0;
          
          boolean anyAttached = false;
          for (int index = 0; index < subqueryTrackingArrayLength; index++) {
            if (subqueries[index] != null
                && subqueries[index].getPointOfAttachment() == this.resultSetNumber) {
              noChildren++;
              anyAttached = true;
            }
          }
          
          if(this.source != null) {
            noChildren++;
          }
          
          visitor.setNumberOfChildren(noChildren);
          
          visitor.visit(this);
          
          if(this.source != null) {
            this.source.accept(visitor);
          }

          if (anyAttached) {
            for (int index = 0; index < subqueryTrackingArrayLength; index++) {
              if (subqueries[index] != null
                  && subqueries[index].getPointOfAttachment() == this.resultSetNumber) {
                subqueries[index].accept(visitor);
              }
            }
          }

        }
      
        @Override
        public void resetStatistics() {
          restrictionTime = 0;
          projectionTime = 0;
          super.resetStatistics();
          source.resetStatistics();
        }
        
        @Override
        public boolean isReplicateIfSetOpSupported() throws StandardException {
          // Dont think source will ever be not BasicNoPutResultSetImpl
          // If so happen, need to implement this method in NoPutResultSetImpl
          if (SanityManager.DEBUG) {
            SanityManager.ASSERT(source instanceof BasicNoPutResultSetImpl);
          }
          return ((BasicNoPutResultSetImpl)source).isReplicateIfSetOpSupported();
        }
        
        @Override
        public void setGfKeysForNCJoin(ArrayList<DataValueDescriptor> keys)
            throws StandardException {
          source.setGfKeysForNCJoin(keys);
        }
        
        @Override
        public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
          super.buildQueryPlan(builder, context);

          if (projectedColumns.length() > 100) {
            PlanUtils.xmlAttribute(builder, "details",projectedColumns.substring(0, 100));
          }
          else {
            PlanUtils.xmlAttribute(builder, PlanUtils.DETAILS, projectedColumns);
          }

          final String projType;
          if (this.restriction != null && this.doesProjection) {
            projType = PlanUtils.OP_PROJ_RESTRICT;
          }
          else if (this.doesProjection) {
            projType = PlanUtils.OP_PROJECT;
          }
          else if (this.restriction != null) {
            projType = PlanUtils.OP_FILTER;
          }
          else {
            projType = PlanUtils.OP_PROJ_RESTRICT;
          }
          PlanUtils.xmlTermTag(builder, context, projType);
          
          if(this.source != null) {
            this.source.buildQueryPlan(builder, context.pushContext());
          }
          
          PlanUtils.xmlCloseTag(builder, context, this);
          
          return builder;
        }
        
        @Override
        public void releasePreviousByteSource() {
          this.source.releasePreviousByteSource();
        }
// GemStone changes END

        public NoPutResultSet getSource() {
          return source;
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
}

