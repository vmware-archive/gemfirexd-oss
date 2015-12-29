/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DistinctScanResultSet

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;





import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

import java.util.Enumeration;

/**
 * Eliminates duplicates while scanning the underlying conglomerate.
 * (Assumes no predicates, for now.)
 *
 */
class DistinctScanResultSet extends HashScanResultSet
{

	Enumeration element = null;


    //
    // class interface
    //
    DistinctScanResultSet(long conglomId, 
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		int hashKeyItem,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
    {
		super(conglomId, scoci, activation, resultRowAllocator, resultSetNumber,
			  (GeneratedMethod) null, // startKeyGetter
			  0,					  // startSearchOperator
			  (GeneratedMethod) null, // stopKeyGetter
			  0,					  // stopSearchOperator
			  false,				  // sameStartStopPosition
			  (Qualifier[][]) null,	  // scanQualifiers
			  (Qualifier[][]) null,	  // nextQualifiers
			  DEFAULT_INITIAL_CAPACITY, DEFAULT_LOADFACTOR, DEFAULT_MAX_CAPACITY,
			  hashKeyItem, tableName, userSuppliedOptimizerOverrides, indexName, isConstraint, 
			  false,				  // forUpdate
			  colRefItem, lockMode, tableLocked, isolationLevel,
			  false,
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

		// Tell super class to eliminate duplicates
		eliminateDuplicates = true;
    }

	//
	// ResultSet interface (override methods from HashScanResultSet)
	//

	/**
     * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
	    ExecRow result = null;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	    if ( isOpen )
	    {
			if (firstNext)
			{
				element = hashtable.elements();
				firstNext = false;
			}

			if (element.hasMoreElements())
			{
                DataValueDescriptor[] columns = (DataValueDescriptor[]) element.nextElement();

				setCompactRow(compactRow, columns);

				rowsSeen++;

				result = compactRow;
			}
			// else done
		}

		setCurrentRow(result);

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
	    return result;
	}
}
