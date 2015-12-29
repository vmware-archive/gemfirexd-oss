/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.HashTableResultSet

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
import com.pivotal.gemfirexd.internal.catalog.types.ReferencedColumnsDescriptorImpl;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableArrayHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableIntHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.BackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.store.access.KeyHasher;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Builds a hash table on the underlying result set tree.
 *
 */
class HashTableResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{
	/* Run time statistics variables */
	public long restrictionTime;
	public long projectionTime;
	public int  hashtableSize;
	public Properties scanProperties;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
    public GeneratedMethod singleTableRestriction;
	public Qualifier[][] nextQualifiers;
    private GeneratedMethod projection;
	private int[]			projectMapping;
	protected boolean runTimeStatsOn;
	private ExecRow			mappedResultRow;
	public boolean reuseResult;
	public int[]			keyColumns;
	protected boolean		removeDuplicates;
	protected long			maxInMemoryRowCount;
        protected int			initialCapacity;
        protected float			loadFactor;
	protected boolean		skipNullKeyColumns;

	// Variable for managing next() logic on hash entry
	protected boolean		firstNext = true;
	protected int			numFetchedOnNext;
	protected int			entryVectorSize;
	protected List		entryVector;

	private boolean hashTableBuilt;
	private boolean firstIntoHashtable = true;

	protected ExecRow nextCandidate;
	private ExecRow projRow;

	protected BackingStoreHashtable ht;
	//GemStone changes BEGIN
        private List rakVector;
        //GemStone changes END

    //
    // class interface
    //
    HashTableResultSet(NoPutResultSet s,
					Activation a,
					GeneratedMethod str,
					Qualifier[][] nextQualifiers,
					GeneratedMethod p,
					int resultSetNumber,
					int mapRefItem,
					boolean reuseResult,
					int keyColItem,
					boolean removeDuplicates,
					long maxInMemoryRowCount,
					int	initialCapacity,
					float loadFactor,
					boolean skipNullKeyColumns,
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
				"HTRS(), source expected to be non-null");
		}
        singleTableRestriction = str;
		this.nextQualifiers = nextQualifiers;
        projection = p;
		projectMapping = ((ReferencedColumnsDescriptorImpl) a.getSavedObject(mapRefItem)).getReferencedColumnPositions();
		FormatableArrayHolder fah = (FormatableArrayHolder) a.getSavedObject(keyColItem);
		FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
		keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++)
		{
			keyColumns[index] = fihArray[index].getInt();
		}

		this.reuseResult = reuseResult;
		this.removeDuplicates = removeDuplicates;
		this.maxInMemoryRowCount = maxInMemoryRowCount;
		this.initialCapacity = initialCapacity;
		this.loadFactor = loadFactor;
		this.skipNullKeyColumns = skipNullKeyColumns;

		// Allocate a result row if all of the columns are mapped from the source
		if (projection == null)
		{
			mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
		}

		/* Remember whether or not RunTimeStatistics is on */
		runTimeStatsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();
		
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
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"HTRS().openCore(), source expected to be non-null");
		}

		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "HashTableResultSet already open");

		isOpen = true;

		if (! hashTableBuilt)
		{
	        source.openCore();

			/* Create and populate the hash table.  We pass
			 * ourself in as the row source.  This allows us
			 * to apply the single table predicates to the
			 * rows coming from our child as we build the
			 * hash table.
			 */
			ht = getBackingStoreHashtable();

			checkCancellationFlag();
			if (runTimeStatsOn)
			{
				hashtableSize = ht.size();

				if (scanProperties == null)
				{
					scanProperties = new Properties();
				}

				try
				{
					if (ht != null)
					{
                        ht.getAllRuntimeStats(scanProperties);
					}
				}
				catch(StandardException se)
				{
					// ignore
				}
			}

			//isOpen = true;
			hashTableBuilt = true;
		}

		resetProbeVariables();

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

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen,
					"HashTableResultSet already open");
		}

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;

		resetProbeVariables();

		numOpens++;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	protected void resetProbeVariables() throws StandardException
	{
		firstNext = true;
		numFetchedOnNext = 0;
		entryVector = null;
		//GemStone changes END
                rakVector = null;
                //GemStone changes END
		entryVectorSize = 0;

		if (nextQualifiers != null)
		{
			clearOrderableCache(nextQualifiers);
		}
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
	    checkCancellationFlag();
	    ExecRow result = null;
		DataValueDescriptor[] columns = null;
		// GemStone changes BEGIN
                Object rak = null;
                // GemStone changes END
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen )
	    {
			/* We use a do/while loop to ensure that we continue down
			 * the duplicate chain, if one exists, until we find a
			 * row that matches on all probe predicates (or the
			 * duplicate chain is exhausted.)
			 */
			do 
			{
				if (firstNext)
				{			  
					firstNext = false;

					/* Hash key could be either a single column or multiple 
                     * columns.  If a single column, then it is the datavalue 
                     * wrapper, otherwise it is a KeyHasher.
					 */
					Object hashEntry;
					if (keyColumns.length == 1)
					{
						hashEntry = ht.get(nextQualifiers[0][0].getOrderable());
						// GemStone changes BEGIN
						rak = ht.getFromRegionAndKeyHash(nextQualifiers[0][0].getOrderable());
						// GemStone changes END
					}
					else
					{
						KeyHasher mh = 
                            new KeyHasher(keyColumns.length);

						for (int index = 0; index < keyColumns.length; index++)
						{
                            // RESOLVE (mikem) - will need to change when we
                            // support OR's in qualifiers.
							mh.setObject(
                                index, nextQualifiers[0][index].getOrderable());
						}
						// GemStone changes BEGIN
						//hashEntry = ht.get(mh);
						if (mh == null) {
                                                  hashEntry = null;
                                                }
                                                else {
                                                  hashEntry = ht.get(mh);
                                                  rak = ht.getFromRegionAndKeyHash(mh);
                                                }
                                                // GemStone changes END
					}

					if (hashEntry instanceof List)
					{
						entryVector = (List) hashEntry;
						entryVectorSize = entryVector.size();
						columns = 
                            (DataValueDescriptor[]) entryVector.get(0);
						// GemStone changes BEGIN
                                                if (rak != null) {
                                                  assert rak instanceof List;
                                                  rakVector = (List)rak;
                                                  rak = rakVector.get(0);
                                                }
                                                // GemStone changes END
					}
					else
					{
						entryVector = null;
						entryVectorSize = 0;
						// GemStone changes BEGIN
                                                rakVector = null;
                                                // GemStone changes END
						columns = (DataValueDescriptor[]) hashEntry;
					}
				}
				else if (numFetchedOnNext < entryVectorSize)
				{
					// We are walking a list and there are more rows left.
					columns = (DataValueDescriptor[]) 
                        entryVector.get(numFetchedOnNext);
					//GemStone changes BEGIN
                                        if (rakVector != null) {
                                          rak = (RegionAndKey)rakVector.get(numFetchedOnNext);
                                        }
                                        //GemStone changes END
				}

				if (columns != null)
				{
					if (SanityManager.DEBUG)
					{
						// Columns is really a Storable[]
						for (int i = 0; i < columns.length; i++)
						{
							if (! (columns[0] instanceof Storable))
							{
								SanityManager.THROWASSERT(
								"columns[" + i + "] expected to be Storable, not " +
								columns[i].getClass().getName());
							}
						}
					}

					// See if the entry satisfies all of the other qualifiers
					boolean qualifies = true;

					/* We've already "evaluated" the 1st keyColumns qualifiers 
                     * when we probed into the hash table, but we need to 
                     * evaluate them again here because of the behavior of 
                     * NULLs.  NULLs are treated as equal when building and 
                     * probing the hash table so that we only get a single 
                     * entry.  However, NULL does not equal NULL, so the 
                     * compare() method below will eliminate any row that
					 * has a key column containing a NULL.
					 */

                    // RESOLVE (mikem) will have to change when qualifiers 
                    // support OR's.

                    if (SanityManager.DEBUG)
                    {
                        // we don't support 2 d qualifiers yet.
                        SanityManager.ASSERT(nextQualifiers.length == 1);
                    }
					for (int index = 0; index < nextQualifiers[0].length; index++)
					{
                        Qualifier q = nextQualifiers[0][index];

						qualifies = 
                            columns[q.getColumnId()].compare(
                                q.getOperator(),
                                q.getOrderable(),
                                q.getOrderedNulls(),
                                q.getUnknownRV());

						if (q.negateCompareResult()) 
						{ 
							qualifies = !(qualifies);
						} 

						// Stop if any predicate fails
						if (! qualifies)
						{
							break;
						}
					}

					if (qualifies)
					{

						for (int index = 0; index < columns.length; index++)
						{
							nextCandidate.setColumn(index + 1, columns[index]);
						}

						// GemStone changes BEGIN
	                                          if (nextCandidate != null) {
	                                            nextCandidate.clearAllRegionAndKeyInfo();
	                                          }
	                                          // GemStone changes END
						result = doProjection(nextCandidate);
					}
					else
					{
						result = null;
					}

					numFetchedOnNext++;
				}
				else
				{
					result = null;
				}
			}
			while (result == null && numFetchedOnNext < entryVectorSize);
		}

		setCurrentRow(result);

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);

		if (runTimeStatsOn)
		{
			if (! isTopResultSet)
			{
				/* This is simply for RunTimeStats */
				/* We first need to get the subquery tracking array via the StatementContext */
				StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
				subqueryTrackingArray = sc.getSubqueryTrackingArray();
			}
			if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		}
// GemStone changes BEGIN
		if (result != null) {
		  if (rak != null) {
		    assert rak instanceof RegionAndKey;
		    result.addRegionAndKey((RegionAndKey)rak);
		  }
		  if (this.localTXState != null && this.isTopResultSet &&
		      isForUpdate()) {
		    updateRowLocationPostRead();
		  }
		}
// GemStone changes END
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
                        return  time - source.getTimeSpent(ENTIRE_RESULTSET_TREE, timeType);
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
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();

	        source.close(cleanupOnError);

			super.close(cleanupOnError);

			if (hashTableBuilt)
			{
				// close the hash table, eating any exception
				ht.close();
				ht = null;
				hashTableBuilt = false;
			}
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ProjectRestrictResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
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
			SanityManager.ASSERT(source instanceof CursorResultSet, "source not instance of CursorResultSet");
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
					((singleTableRestriction == null) ? null : singleTableRestriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

	    if (candidateRow != null && restrict) 
		{
			result = doProjection(candidateRow);
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
	protected ExecRow doProjection(ExecRow sourceRow)
		throws StandardException
	{
		// No need to use reflection if reusing the result
		if (reuseResult && projRow != null)
		{
			return projRow;
		}

		ExecRow result;

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

		/* We need to reSet the current row after doing the projection */
		setCurrentRow(result);

		/* Remember the result if reusing it */
		if (reuseResult)
		{
			projRow = result;
		}
		return result;
	}

	// RowSource interface
		
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public ExecRow getNextRowFromRowSource()
		throws StandardException
	{
		ExecRow execRow = source.getNextRowCore();

		/* Use the single table predicates, if any,
		 * to filter out rows while populating the
		 * hash table.
		 */
 		while (execRow != null)
		{
		    boolean restrict = false;
		    DataValueDescriptor restrictBoolean;

			rowsSeen++;

			/* If restriction is null, then all rows qualify */
            restrictBoolean = (DataValueDescriptor) 
					((singleTableRestriction == null) ? null : singleTableRestriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
			if (!restrict)
			{
				execRow = source.getNextRowCore();
				continue;
			}

			if (targetResultSet != null)
			{
				/* Let the target preprocess the row.  For now, this
				 * means doing an in place clone on any indexed columns
				 * to optimize cloning and so that we don't try to drain
				 * a stream multiple times.  This is where we also
				 * enforce any check constraints.
				 */
				clonedExecRow = targetResultSet.preprocessSourceRow(execRow);
			}


			/* Get a single ExecRow of the same size
			 * on the way in so that we have a row
			 * to use on the way out.
			 */
			if (firstIntoHashtable)
			{
				nextCandidate = activation.getExecutionFactory().getValueRow(execRow.nColumns());
				firstIntoHashtable = false;
			}

			return execRow;
		}

		return null;
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		if (source == null) 
		{
			return false;
		}
		return source.isForUpdate();
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
  public void accept(
      ResultSetStatisticsVisitor visitor) {
    // compute number of children of this node, which get visited
    int noChildren = 0;
    if (this.source != null)
      noChildren++;
    if (this.subqueryTrackingArray != null) {
      noChildren += subqueryTrackingArray.length;
    }
    // inform the visitor
    visitor.setNumberOfChildren(noChildren);

    // pre-order, depth-first traversal
    // me first
    visitor.visit(this);
    // then my direct child
    if (source != null) {
      source.accept(visitor);
    }
    // and now the dependant resultsets, if there are any
    if (subqueryTrackingArray != null) {
      for (int index = 0; index < subqueryTrackingArray.length; index++) {
        if (subqueryTrackingArray[index] != null) {
          subqueryTrackingArray[index].accept(visitor);
        }
      }
    }
  }
  
  @Override
  public void resetStatistics() {
    restrictionTime = 0;
    projectionTime = 0;
    hashtableSize = 0;
    super.resetStatistics();
    source.resetStatistics();
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_HASHTABLE);
    
    if(this.source != null) {
      this.source.buildQueryPlan(builder, context.pushContext());
    }
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
  }
  
  /*
   * For NCJ Use
   */
  public Iterator hashTableIterator() throws StandardException {
    if (isOpen && hashTableBuilt) {
      return ht.valuesIterator();
    }
    return null;
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
  
  protected BackingStoreHashtable getBackingStoreHashtable()
      throws StandardException {
    // Get the current transaction controller
    TransactionController tc = activation.getTransactionController();
    return new BackingStoreHashtable(tc, this, keyColumns, removeDuplicates,
        (int)optimizerEstimatedRowCount, maxInMemoryRowCount,
        (int)initialCapacity, loadFactor, skipNullKeyColumns, false /* Not kept after a commit */);
  }
  
  /*
   * Especially for NCJ usage, during batching
   * 
   * Will return true if all entries from below have not been fetched yet
   */
  public boolean moreRowsExpected() {
    return false;
  }
  
  /*
   * Especially for NCJ usage, during batching
   * 
   * @see flag fillUpHashTable
   */
  public boolean fillUpHashTable() throws StandardException {
    SanityManager.THROWASSERT("Should be called for derived class");
    return false;
  }

  /*
   * Empty rows from Hash Table
   */
  public void purgeHashTable() {
    SanityManager.THROWASSERT("Should be called for derived class");
  }
// GemStone changes END
}
