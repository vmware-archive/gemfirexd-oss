/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RIBulkChecker

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





import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RecoveryLock;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndexScanController;
import com.pivotal.gemfirexd.internal.engine.access.index.Hash1IndexScanController;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.LanguageProperties;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.GenericScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.GroupFetchScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * Do a merge run comparing all the foreign keys from the
 * foreign key conglomerate against the referenced keys
 * from the primary key conglomerate.  The scanControllers
 * are passed in by the caller (caller controls locking on
 * said conglomerates).
 * <p>
 * The comparision is done via a merge.  Consequently,
 * it is imperative that the scans are on keyed conglomerates
 * (indexes) and that the referencedKeyScan is a unique scan.
 * <p>
 * Performance is no worse than N + M where N is foreign key 
 * rows and M is primary key rows.  
 * <p>
 * Bulk fetch is used to further speed performance.  The
 * fetch size is LanguageProperties.BULK_FETCH_DEFAULT
 *
 * @see LanguageProperties
 */
public class RIBulkChecker 
{
	private static final int EQUAL = 0;
	private static final int GREATER_THAN = 1;
	private static final int LESS_THAN = -1;

	private FKInfo			fkInfo;
	private GroupFetchScanController	referencedKeyScan;
	//private DataValueDescriptor[][]		referencedKeyRowArray;
	private GroupFetchScanController	foreignKeyScan;
	private DataValueDescriptor[][]		foreignKeyRowArray;
	private DataValueDescriptor[]   referenceIndexKey;
	private ConglomerateController	unreferencedCC;
	private int 			failedCounter;
	private boolean			quitOnFirstFailure;
	private	int				numColumns;
	private	int				currRefRowIndex;
	private	int				currFKRowIndex;
	private int				lastRefRowIndex;
	private int				lastFKRowIndex;
	private ExecRow			firstRowToFail;
	private final int[] partitionColIndexInRefKeyForSelectiveCheck;
	private final PartitionedRegion refTablePr;

    /**
     * Create a RIBulkChecker
	 * 
	 * @param referencedKeyScan		scan of the referenced key's
	 *								backing index.  must be unique
	 * @param foreignKeyScan		scan of the foreign key's
	 *								backing index
	 * @param templateRow			a template row for the indexes.
	 *								Will be cloned when it is used.
	 *								Must be a full index row.
	 * @param quitOnFirstFailure	quit on first unreferenced key
	 * @param unreferencedCC	put unreferenced keys here
	 * @param firstRowToFail		the first row that fails the constraint
	 *								is copied to this, if non-null
     */
    public RIBulkChecker
	(
			GroupFetchScanController    referencedKeyScan,
			GroupFetchScanController	foreignKeyScan,
			ExecRow					    templateRow,
			boolean					    quitOnFirstFailure,
			ConglomerateController	    unreferencedCC,
			ExecRow					    firstRowToFail,
			int [] partitionColIndexInRefKeyForSelectiveCheck,
			PartitionedRegion refTablePr
	)
	{
		this.referencedKeyScan = referencedKeyScan;
		this.foreignKeyScan = foreignKeyScan;
		this.quitOnFirstFailure = quitOnFirstFailure;
		this.unreferencedCC = unreferencedCC;
		this.firstRowToFail = firstRowToFail;
		this.partitionColIndexInRefKeyForSelectiveCheck =  partitionColIndexInRefKeyForSelectiveCheck;
                this.refTablePr = refTablePr;
		foreignKeyRowArray		= new DataValueDescriptor[GemFireXDUtils.DML_BULK_FETCH_SIZE][];
		foreignKeyRowArray[0]	= templateRow.getRowArrayClone();
// GemStone changes BEGIN
		// below not required since we have to do the lookup one by one
		/* (original code)
		referencedKeyRowArray	= new DataValueDescriptor[LanguageProperties.BULK_FETCH_DEFAULT_INT][];
		referencedKeyRowArray[0]= templateRow.getRowArrayClone();
		*/
// GemStone changes END
		failedCounter = 0;
		numColumns = templateRow.getRowArray().length - 1;
		this.referenceIndexKey = new DataValueDescriptor[numColumns];
		currFKRowIndex = -1; 
		currRefRowIndex = -1; 
	}

	/**
	 * Perform the check.
	 *
	 * @return the number of failed rows
	 *
	 * @exception StandardException on error
	 */
	public int doCheck()
		throws StandardException
	{
// GemStone changes BEGIN
    // skip FK checking during DDL replay since it is not required and otherwise
    // we will easily have deadlocks since nodes will block primary selection
    if (com.pivotal.gemfirexd.internal.engine.Misc
        .initialDDLReplayInProgress()) {
      return 0;
    }
    DataValueDescriptor[] foreignKey;

    // GemFireXD PK indexes (Hash1Index or GlobalIndex) are not
    // ordered, so no point in using the ordering algo below.
    // Just check contains for each FK; right now all nodes
    // will end up getting all primary keys of referenced table
    // for checking but we don't care much for efficiency in
    // alter table.
    DataValueDescriptor[] partitionCols = this.partitionColIndexInRefKeyForSelectiveCheck != null? new DataValueDescriptor[this.partitionColIndexInRefKeyForSelectiveCheck.length]:null;
    GfxdPartitionResolver refTableResolver = this.partitionColIndexInRefKeyForSelectiveCheck != null? (GfxdPartitionResolver)this.refTablePr.getPartitionResolver():null;
    
    int indexScanType = -1;
    if (this.referencedKeyScan instanceof MemIndexScanController) {
      indexScanType = ((MemIndexScanController)
          this.referencedKeyScan).getType();
    }
    
    while ((foreignKey = getNextFK()) != null) {
      // reopen referenced key scan with single key
      int partColCounter = 0;
      //TODO:Check if this loop is needed, if the data is filled in the same ref DVD object
      for(int i = 0; i < numColumns;++i) {
        this.referenceIndexKey[i] = foreignKey[i];
        if(this.partitionColIndexInRefKeyForSelectiveCheck != null) {
          if(  partColCounter < this.partitionColIndexInRefKeyForSelectiveCheck.length &&
              i == this.partitionColIndexInRefKeyForSelectiveCheck[partColCounter] ) {
            partitionCols[partColCounter++] = foreignKey[i];            
          }
        }
      }
      if(this.partitionColIndexInRefKeyForSelectiveCheck != null) {
        //Check this foreign key here only if the routing key corresponds to primary bucket at this node
        Object routingObject = refTableResolver.getRoutingObjectsForPartitioningColumns(partitionCols);
        int bucketId = PartitionedRegionHelper.getHashKey(this.refTablePr,routingObject);
        //Is current node primary for the bucket
        RegionAdvisor advisor = this.refTablePr.getRegionAdvisor(); 
        if( ! advisor.isPrimaryForBucket(bucketId) && advisor.isStorageAssignedForBucket(bucketId)) {
          continue;
        }
        
      }
      this.referencedKeyScan.reopenScan(this.referenceIndexKey, ScanController.GE, null,
          this.referenceIndexKey, ScanController.GT /* unused */, 
          // GemStone changes BEGIN
          null);
          // GemStone changes END
      if (!this.referencedKeyScan.next()) {
        if ((indexScanType == MemConglomerate.HASH1INDEX)
            || (indexScanType == MemConglomerate.GLOBALHASHINDEX)) {
          foreignKey = ((Hash1IndexScanController) this.referencedKeyScan)
              .getFailedKey();
        }
       
        /*
         * If all of the foreign key is not null and there are no referenced
         * keys, then everything fails ANSI standard says the referential
         * constraint is satisfied if either at least one of the values of the
         * referencing columns(i.e., foreign key) is null or the value of each
         * referencing column is equal to the corresponding referenced column in
         * the referenced table.
         */
        if (!anyNull(foreignKey)) {
          do {
            failure(foreignKey);
            if (this.quitOnFirstFailure) {
              return 1;
            }
          } while ((foreignKey = getNextFK()) != null);
          return this.failedCounter;
        }
      }
    }
    
    // for RIBulkChecker we do bulk checks in the referencedKeyScan.next()
    // in batches of Hash1IndexScanController.NUM_KEYS_FOR_BULK_CHECKS
    // this means that there could be some keys left still to be checked
    if ((indexScanType == MemConglomerate.HASH1INDEX)
        || (indexScanType == MemConglomerate.GLOBALHASHINDEX)) {
      if (!((Hash1IndexScanController) this.referencedKeyScan)
          .checkAnyAccumulatedKeys()) {
        foreignKey = ((Hash1IndexScanController) this.referencedKeyScan)
            .getFailedKey();
        if (!anyNull(foreignKey)) {
          do {
            failure(foreignKey);
            if (this.quitOnFirstFailure) {
              return 1;
            }
          } while ((foreignKey = getNextFK()) != null);
          return this.failedCounter;
        }
      }
    }
    
	  /* (original code)
		DataValueDescriptor[] foreignKey;
		DataValueDescriptor[] referencedKey;

		int compareResult;

		referencedKey = getNextRef();

		/*
		** 	For each foreign key
	 	**
		**		while (fk > pk)
		**			next pk
		**			if no next pk
		**				failed
		**
		**		if fk != pk
		**			failed
		*	
		while ((foreignKey = getNextFK()) != null)
		{
			/*
			** If all of the foreign key is not null and there are no
			** referenced keys, then everything fails
			** ANSI standard says the referential constraint is
			** satisfied if either at least one of the values of the
			** referencing columns(i.e., foreign key) is null or the
			** value of each referencing column is equal to the 
			** corresponding referenced column in the referenced table
			*
			if (!anyNull(foreignKey) && referencedKey == null)
			{
				do
				{
					failure(foreignKey);
					if (quitOnFirstFailure)
					{
							return 1;
					}
				} while ((foreignKey = getNextFK()) != null);
				return failedCounter;
			}

			while ((compareResult = greaterThan(foreignKey, referencedKey)) == GREATER_THAN)
			{
				if ((referencedKey = getNextRef()) == null)
				{
					do
					{
						failure(foreignKey);
						if (quitOnFirstFailure)
						{
							return 1;
						}
					} while ((foreignKey = getNextFK()) != null);
					return failedCounter;
				}
			}

			if (compareResult != EQUAL)
			{
				failure(foreignKey);
				if (quitOnFiNrstFailure)
				{
					return 1;
				}
			}	
		}
	  */
// GemStone changes END
		return failedCounter;
	}


	/*
	 * Use bulk fetch to get the next set of rows,
	 * or read the next out of our internal array.
	 */
	private DataValueDescriptor[] getNextFK()
		throws StandardException
	{
		if ((currFKRowIndex > lastFKRowIndex) ||
			(currFKRowIndex == -1))
		{
			int rowCount = 
            	foreignKeyScan.fetchNextGroup(foreignKeyRowArray, (RowLocation[]) null);

			if (rowCount == 0)
			{
				currFKRowIndex = -1;
				return null;
			}

			lastFKRowIndex = rowCount - 1;
			currFKRowIndex = 0;
		}

		return foreignKeyRowArray[currFKRowIndex++];
	}

	/*
	 * Use bulk fetch to get the next set of rows,
	 * or read the next out of our internal array.
	 */
	/*
	private DataValueDescriptor[] getNextRef()
		throws StandardException
	{
		if ((currRefRowIndex > lastRefRowIndex) ||
			(currRefRowIndex == -1))
		{
			int rowCount = 
            	referencedKeyScan.fetchNextGroup(referencedKeyRowArray, (RowLocation[]) null);

			if (rowCount == 0)
			{
				currRefRowIndex = -1;
				return null;
			}

			lastRefRowIndex = rowCount - 1;
			currRefRowIndex = 0;
		}

		return referencedKeyRowArray[currRefRowIndex++];
	}*/

	private void failure(DataValueDescriptor[] foreignKeyRow)
		throws StandardException
	{
		if (failedCounter == 0)
		{
			if (firstRowToFail != null)
			{
				firstRowToFail.setRowArray(foreignKeyRow);
				// clone it
				firstRowToFail.setRowArray(firstRowToFail.getRowArrayClone());
			}
		}
			
		failedCounter++;
		if (unreferencedCC != null)
		{
			unreferencedCC.insert(foreignKeyRow);
		}
	}	
	/*
	** Returns true if any of the foreign keys are null
	** otherwise, false.
	*/
	private boolean anyNull(DataValueDescriptor[] fkRowArray)
		throws StandardException
	{
		DataValueDescriptor	fkCol;
	
		/*
		** Check all columns excepting the row location.
		*/	
		for (int i = 0; i < numColumns; i++)
		{
			fkCol = (DataValueDescriptor)fkRowArray[i];

			/*
			** If ANY column in the fk is null, 
			** return true
			*/
			if (fkCol.isNull())
			{
				return true;
			}
		}
		return false;

	}

	private int greaterThan(DataValueDescriptor[] fkRowArray, DataValueDescriptor[] refRowArray)
		throws StandardException
	{
		DataValueDescriptor	fkCol;
		DataValueDescriptor	refCol;
		int 				result;
	
		/*
		** If ANY column in the fk is null,
 		** it is assumed to be equal
		*/	
 		if (anyNull(fkRowArray))
                    return EQUAL;

		for (int i = 0; i < numColumns; i++)
		{
			fkCol = (DataValueDescriptor)fkRowArray[i];
			refCol = (DataValueDescriptor)refRowArray[i];

			result = fkCol.compare(refCol);

			if (result == 1)
			{
				return GREATER_THAN;
			}
			else if (result == -1)
			{
				return LESS_THAN;
			}

			/*
			** If they are equal, go on to the next 
			** column.
			*/
		}
		
		/*
		** If we got here they must be equal
		*/
		return EQUAL;
	}
}
