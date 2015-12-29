/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.CountAggregator

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


import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLLongint;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Aggregator for COUNT()/COUNT(*).  
 */
public final class CountAggregator 
	extends SystemAggregator
{
	private long value;
	private boolean isCountStar;

	/**
	 */
	public void setup(String aggregateName,
	    AggregatorInfo aggInfo /* GemStoneAddition */)
	{
		isCountStar = aggregateName.equals("COUNT(*)");
// GemStone changes BEGIN
		if (aggInfo != null) {
		  super.setup(aggInfo);
		}
	}

	@Override
	public void setup(String aggregateName,
	    DataValueDescriptor initValue) throws StandardException {
	  setup(aggregateName, (AggregatorInfo)null);
	  value = initValue.getLong();
	}
	
	

	@Override
	public String toString() {
	  return String.valueOf(value);
	}

	@Override
	public void accumulate(ExecRow inputRow) throws StandardException {
	  if (this.isCountStar) {
	    this.value++;
	  }
	  else {
	    if (inputRow.isNull(this.inputColumnPos) !=
	        RowFormatter.OFFSET_AND_WIDTH_IS_NULL) {
	      this.value++;
	    }
	    else if (!this.eliminatedNulls) {
	      this.eliminatedNulls = true;
	    }
	  }
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setResult(ExecRow row, int columnPos)
	    throws StandardException {
	  // below works currently for all cases since row is always a
	  // ValueRow/ExecIndexRow here
	  final DataValueDescriptor outputColumn = row.getColumn(columnPos);
	  outputColumn.setValue(this.value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean finish(ExecRow row, boolean isByteArray)
	    throws StandardException {
	  final boolean eliminatedNulls = this.eliminatedNulls;
	  final long result = this.value;
	  this.value = 0;
	  this.eliminatedNulls = false;
	  if (this.isAccumulateDistinctValues) {
	    if (SanityManager.DEBUG) {
	      SanityManager.ASSERT(
	          DVDSet.class.isInstance(row.getColumn(this.resultColumnPos)),
	          "ExecAggregator must be of type DVDSet");
	    }
	    return false;
	  }

	  if (isByteArray) {
	    // TODO: PERF: allow setting value directly in an ExecRow
	    // currently this is never a CompactExecRow
	    row.setValue(this.resultColumnPos - 1, new SQLLongint(result));
	  }
	  else {
	    DataValueDescriptor outputColumn = row.getColumn(
	        this.resultColumnPos);
	    outputColumn.setValue(result);
	  }
	  return eliminatedNulls;
	}

	@Override
	public void clear() {
	  this.value = 0;
	  super.clear();
	}
// GemStone changes END        

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception	StandardException	on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(addend instanceof CountAggregator,
				"addend is supposed to be the same type of aggregator for the merge operator");
		}

		value += ((CountAggregator)addend).value;
	}

	/**
	 * Return the result of the aggregation.  Just
	 * spit out the running count.
	 *
	 * @return the value as a Long 
	 */
	public DataValueDescriptor getResult()
	{
// GemStone changes BEGIN
	  // use SQLInteger if the value is in integer limits to avoid
	  // unnecessary conversions at higher layers
	  if (this.value >= Integer.MIN_VALUE && this.value <= Integer.MAX_VALUE) {
	    return new SQLInteger((int)this.value);
	  }
	  else {
	    return new SQLLongint(value);
	  }
	  /* (original code)
		return new com.pivotal.gemfirexd.internal.iapi.types.SQLLongint(value);
	  */
// GemStone changes END
	}


	/**
	 * Accumulate for count().  Toss out all nulls in this kind of count.
	 * Increment the count for count(*). Count even the null values.
	 *
	 * @param addend	value to be added in
	 * @param ga		the generic aggregator that is calling me
	 *
	 * @see ExecAggregator#accumulate
	 */
	public void accumulate(DataValueDescriptor addend /* GemStone change (original code), Object ga */)
		throws StandardException
	{
		if (isCountStar)
			value++;
		else
// GemStone changes BEGIN
		{
		  if (addend != null && !addend.isNull()) {
		    this.value++;
		  }
		  else if (!this.eliminatedNulls) {
		    this.eliminatedNulls = true;
		  }
		}
		  /* (original code)
			super.accumulate(addend, ga);
		  */
// GemStone changes END
	}

	protected final void basicAccumulate(DataValueDescriptor addend) {
			value++;
	}

	/**
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		CountAggregator ca = new CountAggregator();
		ca.isCountStar = isCountStar;
		return ca;
	}

	public boolean isCountStar()
	{
		return isCountStar;
	}

	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 * Although we are not expected to be persistent per se,
	 * we may be written out by the sorter temporarily.  So
	 * we need to be able to write ourselves out and read
	 * ourselves back in.  
	 * 
	 * @exception IOException thrown on error
	 */
	public final void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeBoolean(isCountStar);
		out.writeLong(value);
	}

	/** 
	* @see java.io.Externalizable#readExternal 
	*
	* @exception IOException io exception
	* @exception ClassNotFoundException on error
	*/
	public final void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		isCountStar = in.readBoolean();
		value = in.readLong();
	}	
	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId() { return StoredFormatIds.AGG_COUNT_V01_ID; }
	
// GemStone changes BEGIN
  @Override
  public int compareTo(Object o) {
    final long otherValue = ((CountAggregator)o).value;
    return value == otherValue ? 0
        : value < otherValue ? -1 : 1;
  }
// GemStone changes END
}
