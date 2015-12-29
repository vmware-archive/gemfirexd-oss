/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DependentResultSet

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

import java.util.Properties;
import java.util.Vector;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndexScanController;
import com.pivotal.gemfirexd.internal.engine.access.index.SortedMap2IndexScanController;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapResourceHolder;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TemporaryRowHolder;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;



/**
 * DependentResultSet should be used by only ON DELETE CASCADE/ON DELETE SET NULL ref
 * actions implementation to gather the rows from the dependent tables.  
 * Idea is to scan the foreign key index for the rows in 
 * the source table matelized temporary result set. Scanning of foreign key index gives us the 
 * rows that needs to be deleted on dependent tables. Using the row location 
 * we got from the index , base row is fetched.
*/
class DependentResultSet extends ScanResultSet implements CursorResultSet

{


	ConglomerateController heapCC;
	RowLocation	baseRowLocation;  // base row location we got from the index
	ExecRow indexRow; //templeate to fetch the index row
	IndexRow indexQualifierRow; // template for the index qualifier row
	ScanController indexSC;  // Index Scan Controller
	StaticCompiledOpenConglomInfo  indexScoci;
	DynamicCompiledOpenConglomInfo indexDcoci;
	int numFkColumns;
	boolean isOpen; // source result set is opened or not
	boolean deferred;
	TemporaryRowHolderResultSet source; // Current parent table result set
	TransactionController tc;
	String parentResultSetId;
	int[] fkColArray;
	RowLocation rowLocation;
    TemporaryRowHolder[] sourceRowHolders;
	TemporaryRowHolderResultSet[] sourceResultSets;
	int[] sourceOpened;
	int    sArrayIndex;
	Vector sVector;


    protected ScanController scanController;
	protected boolean		scanControllerOpened;
	protected boolean		isKeyed;
	protected boolean		firstScan = true;
	protected ExecIndexRow	startPosition;
	protected ExecIndexRow	stopPosition;

    // set in constructor and not altered during
    // life of object.
	protected long conglomId;
    protected DynamicCompiledOpenConglomInfo heapDcoci;
    protected StaticCompiledOpenConglomInfo heapScoci;
	protected GeneratedMethod resultRowAllocator;
	protected GeneratedMethod startKeyGetter;
	protected int startSearchOperator;
	protected GeneratedMethod stopKeyGetter;
	protected int stopSearchOperator;
	protected Qualifier[][] qualifiers;
	public String tableName;
	public String userSuppliedOptimizerOverrides;
	public String indexName;
	protected boolean runTimeStatisticsOn;
	public int rowsPerRead;
	public boolean forUpdate;
	private boolean sameStartStopPosition;

	// Run time statistics
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public boolean isConstraint;
	public boolean coarserLock;
	public boolean oneRowScan;
	protected long	rowsThisScan;

	//
    // class interface
    //
	DependentResultSet(
		long conglomId,
		StaticCompiledOpenConglomInfo scoci, 
		Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] qualifiers,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,		// ignored
		int rowsPerRead,
		boolean oneRowScan,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String parentResultSetId, 
		long fkIndexConglomId,
		int fkColArrayItem,
		int rltItem
		)	throws StandardException
	{
		super(activation, resultSetNumber, resultRowAllocator,
			  lockMode, tableLocked,
			  //Because the scan for the tables in this result set are done
			  //internally for delete cascades, isolation should be set to
			  //REPEATABLE READ irrespective what the user level isolation
			  //level is.
			  TransactionController.ISOLATION_REPEATABLE_READ,
              colRefItem,
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

		this.conglomId = conglomId;

		/* Static info created at compile time and can be shared across
		 * instances of the plan.
		 * Dynamic info created on 1st instantiation of this ResultSet as
		 * it cannot be shared.
		 */
        this.heapScoci = scoci;
        heapDcoci = activation.getTransactionController().getDynamicCompiledConglomInfo(conglomId);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( activation!=null, "table scan must get activation context");
			SanityManager.ASSERT( resultRowAllocator!= null, "table scan must get row allocator");
			if (sameStartStopPosition)
			{
				SanityManager.ASSERT(stopKeyGetter == null,
					"stopKeyGetter expected to be null when sameStartStopPosition is true");
			}
		}

        this.resultRowAllocator = resultRowAllocator;

		this.startKeyGetter = startKeyGetter;
		this.startSearchOperator = startSearchOperator;
		this.stopKeyGetter = stopKeyGetter;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.qualifiers = qualifiers;
		this.tableName = tableName;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = "On Foreign Key";  // RESOLVE , get actual indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.rowsPerRead = rowsPerRead;
		this.oneRowScan = oneRowScan;
		
		runTimeStatisticsOn = (activation != null &&
							   activation.getLanguageConnectionContext().getRunTimeStatisticsMode());

		tc = activation.getTransactionController();
		//values required to scan the forein key index.
		indexDcoci = tc.getDynamicCompiledConglomInfo(fkIndexConglomId);
		indexScoci = tc.getStaticCompiledConglomInfo(fkIndexConglomId);
		
		this.parentResultSetId = parentResultSetId;
		this.fkColArray = (int[])(activation.
						getSavedObject(fkColArrayItem));

		this.rowLocation = (RowLocation)(activation.
										 getSavedObject(rltItem));
		numFkColumns = fkColArray.length;
		indexQualifierRow = new IndexRow(numFkColumns);
		recordConstructorTime();
	}


	/**
	 * Get a scan controller positioned using searchRow as
	 * the start/stop position.  The assumption is that searchRow
	 * is of the same format as the index being opened. 
	 * @param searchRow			the row to match
	 * @exception StandardException on error
	 */


	private ScanController openIndexScanController(ExecRow searchRow)	throws StandardException
	{
		setupQualifierRow(searchRow);
		indexSC = tc.openCompiledScan(
					  false,                       				// hold 
					  TransactionController.OPENMODE_FORUPDATE, // update only
                      lockMode,									// lock Mode
					  isolationLevel,                           //isolation level
                      (FormatableBitSet)null, 							// retrieve all fields
                      indexQualifierRow.getRowArray(),    		// startKeyValue
                      ScanController.GE,            			// startSearchOp
                      null,                         			// qualifier
                      indexQualifierRow.getRowArray(),    		// stopKeyValue
                      ScanController.GT,             			// stopSearchOp 
					  indexScoci,
					  indexDcoci
                      );

		return indexSC;

	}

	
	//reopen the scan with a differnt search row
	private void reopenIndexScanController(ExecRow searchRow)	throws   StandardException
	{

		setupQualifierRow(searchRow);
		indexSC.reopenScan(
						indexQualifierRow.getRowArray(),    	// startKeyValue
						ScanController.GE,            		// startSearchOp
						null,                         		// qualifier
						indexQualifierRow.getRowArray(), 		// stopKeyValue
						ScanController.GT             		// stopSearchOp
						// GemStone changes BEGIN
                                                , null
                                                // GemStone changes BEGIN
						);
	}

	
	/*
	** Do reference copy for the qualifier row.  No cloning.
	** So we cannot get another row until we are done with
	** this one.
	*/
	private void setupQualifierRow(ExecRow searchRow)
	{
		Object[] indexColArray = indexQualifierRow.getRowArray();
		Object[] baseColArray = searchRow.getRowArray();

		for (int i = 0; i < numFkColumns; i++)
		{
			indexColArray[i] = baseColArray[fkColArray[i] - 1];
		}
	}


	private void  openIndexScan(ExecRow searchRow) throws StandardException
	{

		if (indexSC == null)
		{
			indexSC =  openIndexScanController(searchRow);
			//create a template for the index row
			indexRow = indexQualifierRow.getClone();
			indexRow.setColumn(numFkColumns + 1, rowLocation.getClone());	

		}else
		{
			reopenIndexScanController(searchRow);
		}
	}


	/**
	  Fetch a row from the index scan.

	  @return The row or null. Note that the next call to fetch will
	  replace the columns in the returned row.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchIndexRow()
		 throws StandardException
	{ 
		if (!indexSC.fetchNext(indexRow))
		{
			return null;
		}
		return indexRow;
	}

	

	/**
	  Fetch the base row corresponding to the current index row

	  @return The base row row or null.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchBaseRow()
		 throws StandardException
	{ 

		if (currentRow == null)
		{
			currentRow =
				getCompactRow(candidate, accessedCols, isKeyed);
		} 

		baseRowLocation = (RowLocation) indexRow.getLastColumn();
// Gemstone changes BEGIN
		baseRowLocation = heapCC.fetch(
		    baseRowLocation, candidate, accessedCols, false);
		boolean base_row_exists =baseRowLocation != null;
		if (base_row_exists) {
		  indexRow.setColumn(indexRow.getRowArray().length,
		      baseRowLocation);
		}
// Gemstone changes END

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(base_row_exists, "base row disappeared.");
        }

		return currentRow;
	}
	

	ExecRow searchRow = null; //the current row we are searching for

	//this function will return an index row on dependent table 
	public ExecRow	getNextRowCore() throws StandardException 
	{
		
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (searchRow == null)
		{
			//we are searching for a row first time
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}	
	
		ExecRow currentIndexRow = null;
	    while(searchRow != null)
		{
			//get if the current search row has  more 
			//than one row in the dependent tables
			currentIndexRow = fetchIndexRow();
	
			if(currentIndexRow !=null)
				break;
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
		if(currentIndexRow!= null)
		{
			rowsSeen++;
			return fetchBaseRow();
		}else
		{
			return currentIndexRow;
		}
		
		
	}


	//this function will return the rows from the parent result sets 
	private ExecRow	getNextParentRow() throws StandardException 
	{

		ExecRow cRow;
		TemporaryRowHolder rowHolder;

		if(sourceOpened[sArrayIndex] == 0)
		{
			rowHolder = sourceRowHolders[sArrayIndex];
			source = (TemporaryRowHolderResultSet)rowHolder.getResultSet();
			source.open(); //open the cursor result set
			sourceOpened[sArrayIndex] = -1;
			sourceResultSets[sArrayIndex] = source;
		}

		if(sourceOpened[sArrayIndex] == 1)
		{
			source = sourceResultSets[sArrayIndex];
			source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
							  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
			sourceOpened[sArrayIndex] = -1;
			
		}

		if(sVector.size() > sourceRowHolders.length)
		{
			addNewSources();
		}

		cRow = source.getNextRow();
		while(cRow == null &&  (sArrayIndex+1) <  sourceRowHolders.length)
		{

			//opening the next source;
			sArrayIndex++;
			if(sourceOpened[sArrayIndex] == 0)
			{
				rowHolder = sourceRowHolders[sArrayIndex];
				source = (TemporaryRowHolderResultSet)rowHolder.getResultSet();
				source.open(); //open the cursor result set
				sourceOpened[sArrayIndex] = -1;
				sourceResultSets[sArrayIndex] = source;
			}

			if(sourceOpened[sArrayIndex] == 1)
			{
				source = sourceResultSets[sArrayIndex];
				source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
								  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
				sourceOpened[sArrayIndex] = -1;
			}
		
			cRow = source.getNextRow();
		}

		if(cRow == null)
		{
			//which means no source has any more  currently rows.
			sArrayIndex = 0;
			//mark all the sources to  restartScan.
			for(int i =0 ; i < sourceOpened.length ; i++)
				sourceOpened[i] = 1;
		}
		
		return cRow;
	}



	/*
	** Open the heap Conglomerate controller
	**
	** @param transaction controller will open one if null
	*/
	public ConglomerateController openHeapConglomerateController()
		throws StandardException
	{
		return tc.openCompiledConglomerate(
                    false,
				    TransactionController.OPENMODE_FORUPDATE,
					lockMode,
					isolationLevel,
					heapScoci,
					heapDcoci);
	}




	/**
	  Close the all the opens we did in this result set.
	  */
	public void close(boolean cleanupOnError)
        throws StandardException
	{
		//save the information for the runtime stastics
		// This is where we get the scan properties for the reference index scans
		if (runTimeStatisticsOn)
		{
			startPositionString = printStartPosition();
			stopPositionString = printStopPosition();
			scanProperties = getScanProperties();
		}

		if (indexSC != null) 
		{
			indexSC.close();
			indexSC = null;
		}

		if ( heapCC != null )
		{
			heapCC.close();
			heapCC = null;
		}
		if(isOpen)
		{
			source.close(cleanupOnError);  
		}
		
		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	}

	public void	finish() throws StandardException
	{
		if (source != null)
			source.finish();
		finishAndRTS();
	}

	public void openCore() throws StandardException
	{
		initIsolationLevel();
		sVector = activation.getParentResultSet(parentResultSetId);
		int size = sVector.size();
		sourceRowHolders = new TemporaryRowHolder[size];
		sourceOpened = new int[size];
		sourceResultSets = new TemporaryRowHolderResultSet[size];
		for(int i = 0 ; i < size ; i++)
		{
			sourceRowHolders[i] = (TemporaryRowHolder)sVector.elementAt(i);
			sourceOpened[i] = 0;
		}

		//open the table scan
		heapCC = openHeapConglomerateController();
		numOpens++;
		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}


	private void addNewSources()
	{
		int size = sVector.size();
		TemporaryRowHolder[] tsourceRowHolders = new TemporaryRowHolder[size];
		int[] tsourceOpened = new int[size];
		TemporaryRowHolderResultSet[] tsourceResultSets = new TemporaryRowHolderResultSet[size];
		
		//copy the source we have now
		System.arraycopy(sourceRowHolders, 0, tsourceRowHolders, 0 , sourceRowHolders.length);
		System.arraycopy(sourceOpened, 0, tsourceOpened , 0 ,sourceOpened.length);
		System.arraycopy(sourceResultSets , 0, tsourceResultSets ,0 ,sourceResultSets.length);

		//copy the new sources
		for(int i = sourceRowHolders.length; i < size ; i++)
		{
			tsourceRowHolders[i] = (TemporaryRowHolder)sVector.elementAt(i);
			tsourceOpened[i] = 0;
		}

		sourceRowHolders = tsourceRowHolders;
		sourceOpened = tsourceOpened ;
		sourceResultSets = tsourceResultSets;
	}



	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	boolean canGetInstantaneousLocks()
	{
		return false;
	}


	public long getTimeSpent(int type, int timeType)
	{
		return PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);
	}


	//Cursor result set information.
	public RowLocation getRowLocation() throws StandardException
	{
		return baseRowLocation;
	}

	public ExecRow getCurrentRow() throws StandardException 
	{
		return currentRow;
	}


	public Properties getScanProperties()
	{
		if (scanProperties == null)
		{
			scanProperties = new Properties();
		}
		try
		{
			if (indexSC != null)
			{
				indexSC.getScanInfo().getAllScanInfo(scanProperties);
				/* Did we get a coarser lock due to
				 * a covering lock, lock escalation
				 * or configuration?
				 */
				coarserLock = indexSC.isTableLocked() && 
					(lockMode == TransactionController.MODE_RECORD);
			}
		}
		catch(StandardException se)
		{
				// ignore
		}

		return scanProperties;
	}

	public String printStartPosition()
	{
		return printPosition(ScanController.GE, indexQualifierRow);
	}

	public String printStopPosition()
	{
		return printPosition(ScanController.GT, indexQualifierRow);
	}


	/**
	 * Return a start or stop positioner as a String.
	 *
	 * If we already generated the information, then use
	 * that.  Otherwise, invoke the activation to get it.
	 */
	private String printPosition(int searchOperator, ExecIndexRow positioner)
	{
		String idt = "";
		String output = "";

		String searchOp = null;
		switch (searchOperator)
		{
			case ScanController.GE:
				searchOp = ">=";
				break;

			case ScanController.GT:
				searchOp = ">";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Unknown search operator " +
												searchOperator);
				}

				// NOTE: This does not have to be internationalized because
				// this code should never be reached.
				searchOp = "unknown value (" + searchOperator + ")";
				break;
		}

		if(positioner !=null)
		{
			output = output + "\t" +
				MessageService.getTextMessage(
										  SQLState.LANG_POSITIONER,
										  searchOp,
										  String.valueOf(positioner.nColumns())) +
				"\n";

			output = output + "\t" +
				MessageService.getTextMessage(
											  SQLState.LANG_ORDERED_NULL_SEMANTICS) +
				"\n";
			for (int position = 0; position < positioner.nColumns(); position++)
			{
				if (positioner.areNullsOrdered(position))
				{
					output = output + position + " ";
				}
			}
	
		}
	
		return output + "\n";
	}


	/**
	 * Return an array of Qualifiers as a String
	 */
	public String printQualifiers()
	{
		//There are no qualifiers in thie result set for index scans.
		String idt = "";
		return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
	}

// GemStone changes BEGIN

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    upgradeReadLockToWrite(this.baseRowLocation, null);
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    if(localTXState != null) {
      releaseRowLocationLock(this.baseRowLocation, null);
    }
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    if(sourceResultSets != null) {
      visitor.setNumberOfChildren(sourceResultSets.length);
    }
    visitor.visit(this);
    
    for(int i = 0; i < sourceResultSets.length; i++) {
      sourceResultSets[i].accept(visitor);
    }
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_TABLESCAN);
    
    for(int i = 0; i < sourceResultSets.length; i++) {
      this.source.buildQueryPlan(builder, context.pushContext());
    }
    
    PlanUtils.xmlCloseTag(builder, context, this);
    return builder;
  }

  @Override
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn,
      GemFireContainer container)
          throws StandardException {
    if (this.indexSC instanceof SortedMap2IndexScanController) {
      SortedMap2IndexScanController sc = (SortedMap2IndexScanController)indexSC;
      return RowUtil.fetch(loc, destRow, validColumns, faultIn, container, sc,
          sc.getCurrentKey(), sc.getCurrentNodeVersion(), 
          (GemFireTransaction)this.lcc.getTransactionExecute());
    }
    else {
      return RowUtil.fetch(loc, destRow, validColumns, faultIn, container,
          null, null, 0, (GemFireTransaction)this.lcc.getTransactionExecute());
    }
  }
// GemStone changes END
}
