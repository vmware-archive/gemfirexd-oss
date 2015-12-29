/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.AggregateSortObserver

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

import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.UserDataValue;

/**
 * This sort observer performs aggregation.
 *
 */
public class AggregateSortObserver extends BasicSortObserver
{

	protected GenericAggregator[]	aggsToProcess;
	protected GenericAggregator[]	aggsToInitialize;

	private int firstAggregatorColumn;

	/**
	 * Simple constructor
	 *
	 * @param doClone If true, then rows that are retained
	 *		by the sorter will be cloned.  This is needed
	 *		if language is reusing row wrappers.
	 *
	 * @param aggsToProcess the array of aggregates that 
	 *		need to be accumulated/merged in the sorter.
	 *
	 * @param aggsToInitialize the array of aggregates that
	 *		need to be iniitialized as they are inserted
	 *		into the sorter.  This may be different than
	 *		aggsToProcess in the case where some distinct
	 *		aggregates are dropped in the initial pass of
	 *		a two phase aggregation for scalar or vector
	 *		distinct aggregation.  The initialization process
	 *		consists of replacing an empty UserValue with a new, 
	 *		initialized aggregate of the appropriate type.
	 *		Note that for each row, only the first aggregate
	 *		in this list is checked to see whether initialization
	 *		is needed.  If so, ALL aggregates are initialized;
	 *		otherwise, NO aggregates are initialized.
	 *
	 * @param execRow	ExecRow to use as source of clone for store.
	 */
	public AggregateSortObserver(final boolean doClone, final GenericAggregator[] aggsToProcess, 
								 final GenericAggregator[] aggsToInitialize,
								 final ExecRow execRow)
	{
		super(doClone, false, execRow, true);
		this.aggsToProcess = aggsToProcess;
		this.aggsToInitialize = aggsToInitialize;

		/*
		** We expect aggsToInitialize and aggsToProcess to
		** be non null.  However, if it is deemed ok for them
		** to be null, it shouldn't be too hard to add the
		** extra null checks herein.
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(aggsToInitialize != null, "aggsToInitialize argument to AggregateSortObserver is null");
			SanityManager.ASSERT(aggsToProcess != null, "aggsToProcess argument to AggregateSortObserver is null");
		}

		if (aggsToInitialize.length > 0)
		{
			firstAggregatorColumn = aggsToInitialize[0].aggregatorColumnId;
		} 
	}

	/**
	 * Called prior to inserting a distinct sort
	 * key.  
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining
	 *
	 * @return the row to be inserted by the sorter.  If null,
	 *		then nothing is inserted by the sorter.  Distinct
	 *		sorts will want to return null.
	 *
	 * @exception StandardException never thrown
	 */
	public ExecRow insertNonDuplicateKey(final ExecRow insertRow)
		throws StandardException
	{
		final ExecRow returnRow = 
            super.insertNonDuplicateKey(insertRow);

		/*
		** If we have an aggregator column that hasn't been
		** initialized, then initialize the entire row now.	
		*/
		if (aggsToInitialize.length > 0 &&
			returnRow.isNull(firstAggregatorColumn+1) == RowFormatter.OFFSET_AND_WIDTH_IS_NULL)
		{
			for (int i = 0; i < aggsToInitialize.length; i++)
			{
				final GenericAggregator aggregator = aggsToInitialize[i];
				final UserDataValue wrapper = ((UserDataValue)returnRow.getColumn(aggregator.aggregatorColumnId+1));
				if (SanityManager.DEBUG)
				{
					if (!wrapper.isNull())
					{
						SanityManager.THROWASSERT("during aggregate "+
						"initialization, all wrappers expected to be empty; "+
						"however, the wrapper for the following aggregate " +
						"was not empty:" +aggregator+".  The value stored is "+
						wrapper.getObject());
					}
				}
				wrapper.setValue(aggregator.getAggregatorInstance());
				aggregator.accumulate(returnRow, returnRow);
			}
		}

		return returnRow;
	
	}	
	/**
	 * Called prior to inserting a duplicate sort
	 * key.  We do aggregation here.
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining.  It is a duplicate
	 * 		of existingRow.
	 *
	 * @param existingRow the row that is already in the
	 * 		the sorter which is a duplicate of insertRow
	 *
	 * @exception StandardException never thrown
	 */
	public ExecRow insertDuplicateKey(final ExecRow insertRow, final ExecRow existingRow) 
			throws StandardException
	{
		if (aggsToProcess.length == 0)
		{
			return null;
		}

		/*
		** If the other row already has an aggregator, then
		** we need to merge with it.  Otherwise, accumulate
		** it.
		*/
		for (int i = 0; i < aggsToProcess.length; i++)
		{
			final GenericAggregator aggregator = aggsToProcess[i];
			if (insertRow.isNull(aggregator.getColumnId()+1) == RowFormatter.OFFSET_AND_WIDTH_IS_NULL)
			{
				aggregator.accumulate(insertRow, existingRow);
			}
			else
			{
				aggregator.merge(insertRow, existingRow);
			}
		}
		return null;
	}
// GemStone changes BEGIN
	@Override
	public boolean eliminateDuplicate(Object insertRow,
	    Object existingRow) {
	  try {
	    return insertDuplicateKey((ExecRow)insertRow,
	        (ExecRow)existingRow) == null;
	  } catch (StandardException se) {
	    throw new GemFireXDRuntimeException(se);
	  }
	}

	@Override
	public boolean canSkipDuplicate() {
	  return true;
	}
// GemStone changes END
}
