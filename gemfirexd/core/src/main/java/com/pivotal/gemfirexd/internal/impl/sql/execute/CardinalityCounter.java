/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.CardinalityCounter

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






import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * This is a decorator (in Design Patterns Terminology)
 * class to enhance the functionality
 * of a RowLocationRetRowSource. It assumes that the rows are coming
 * in sorted order from the row source and it simply keeps track of
 * the cardinality of all the leading columns.
 */

public class CardinalityCounter implements RowLocationRetRowSource
{
	private RowLocationRetRowSource rowSource;
	private ExecRow prevKey;
	private long[] cardinality;
	private long numRows;

	public CardinalityCounter(RowLocationRetRowSource rowSource)
	{
		this.rowSource = rowSource;
	}

	/** @see RowLocationRetRowSource#needsRowLocation */
	public boolean needsRowLocation() 
	{ 
		return rowSource.needsRowLocation();
	}

	/** @see RowLocationRetRowSource#rowLocation */
	public void rowLocation(RowLocation rl) throws StandardException
	{ 
		rowSource.rowLocation(rl);
	}

	/** 
	 * Gets next row from the row source and update the count of unique values
	 * that are returned.
	 * @see RowLocationRetRowSource#getNextRowFromRowSource 
	 */
	public ExecRow getNextRowFromRowSource() throws StandardException
	{
		ExecRow nextRow;
		nextRow = rowSource.getNextRowFromRowSource();
		if (nextRow != null)
			keepCount(nextRow);
		return nextRow;
	}


	/** @see RowLocationRetRowSource#needsToClone */
	public boolean needsToClone()
	{
		return rowSource.needsToClone();
	}

	/** @see RowLocationRetRowSource#getValidColumns */
	public FormatableBitSet getValidColumns()
	{
		return rowSource.getValidColumns();
	}

	/** @see RowLocationRetRowSource#closeRowSource */
	public void closeRowSource()
	{
		rowSource.closeRowSource();
	}
	
	/* [sb] don't want to use this method, instead have ExecRow based cloning.
	private DataValueDescriptor[] clone(DataValueDescriptor[] nextRow)
	{
		DataValueDescriptor[] cloned;

		cloned = new DataValueDescriptor[nextRow.length];
		for (int i = 0; i < nextRow.length - 1; i++)
		{
			cloned[i] = ((DataValueDescriptor)nextRow[i]).getClone();
		}
		return cloned;
	}
	*/

	public void keepCount(ExecRow currentKey) throws StandardException
	{
		int numKeys = currentKey.nColumns() - 1; // always row location.
		numRows++;
		if (prevKey == null)
		{
			prevKey = currentKey.getClone();
			cardinality = new long[numKeys];
			for (int i = 0; i < numKeys; i++)
				cardinality[i] = 1;
			return;
		}
		
		int i;
		for (i = 1; i <= numKeys; i++)
		{
		        final long offsetWidth;
			if ( (offsetWidth = prevKey.isNull(i)) == RowFormatter.OFFSET_AND_WIDTH_IS_NULL)
				break;

			if (prevKey.compare(currentKey,i,offsetWidth, false) != 0)
			{
				// null out prevKey, so that the object gets 
				// garbage collected. is this too much object
				// creation? can we do setColumn or some such
				// in the object that already exists in prevKey?
				// xxxstatRESOLVE--
				prevKey = null; 
				prevKey = currentKey.getClone();
				break;
			}
		} // for
		
		for (int j = i; j <= numKeys; j++)
			cardinality[j - 1]++;
	}
	
	/** return the array of cardinalities that are kept internally. One value
	 * for each leading key; i.e c1, (c1,c2), (c1,c2,c3) etc.
	 * @return 	an array of unique values.
	 */
	public long[] getCardinality() { return cardinality; }

	/**
	 * get the number of rows seen in the row source thus far.
	 * @return total rows seen from the row source.
	 */
	public long getRowCount() { return numRows; }
}
