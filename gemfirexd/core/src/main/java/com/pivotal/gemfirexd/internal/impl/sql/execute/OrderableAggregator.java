/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.OrderableAggregator

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
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.types.DataType;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;


import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Abstract aggregator for Orderable aggregates (max/min).
 *
 */
abstract class OrderableAggregator extends SystemAggregator
{
	protected DataValueDescriptor value;

	/**
	 */
// GemStone changes BEGIN
	@Override
	public void setup(String aggregateName,
	    AggregatorInfo aggInfo /* GemStoneAddition */) {
	  if (aggInfo != null) {
	    super.setup(aggInfo);
	  }
	}

	@Override
	public void setup(String aggregateName,
	    DataValueDescriptor initValue) throws StandardException {
	  setup(aggregateName, (AggregatorInfo)null);

	  value = initValue;
	}

	@Override
	public void clear() {
	  if (this.value != null) {
	    this.value.restoreToNull();
	  }
	  super.clear();
	}

	@Override
	public String toString() {
	  final DataValueDescriptor value = this.value;
	  if (value != null) {
	    return value.toString();
	  }
	  else {
	    return "NULL value (orderable aggregator)";
	  }
	}
// GemStone changes END        

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception StandardException on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(addend instanceof OrderableAggregator,
				"addend is supposed to be the same type of aggregator for the merge operator");
		}

		// Don't bother merging if the other has never been used.
		DataValueDescriptor bv = ((OrderableAggregator)addend).value;
		if (bv != null)
			this.basicAccumulate(bv);
	}

	/**
	 * Return the result of the operations that we
	 * have been performing.  Returns a DataValueDescriptor.
	 *
	 * @return the result as a DataValueDescriptor 
	 */
	public DataValueDescriptor getResult() throws StandardException
	{
		return value;
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
	 * ourselves back in.  We rely on formatable to handle
	 * situations where <I>value</I> is null.
	 * <p>
	 * Why would we be called to write ourselves out if we
	 * are null?  For scalar aggregates, we don't bother
	 * setting up the aggregator since we only need a single
	 * row.  So for a scalar aggregate that needs to go to
	 * disk, the aggregator might be null.
	 * 
	 * @exception IOException on error
	 *
	 * @see java.io.Externalizable#writeExternal
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		//GemStone changes BEGIN
		if(value != null) {
		  out.writeBoolean(true);
		  value.toData(out);
		}
		else {
                  out.writeBoolean(false);
		}
		//originally: out.writeObject(value);
                //GemStone changes END
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
                //GemStone changes BEGIN
		final boolean isValue = in.readBoolean();
		if(isValue) {
		  value = DataType.readDVD(in);
		}
		//originally: value = (DataValueDescriptor) in.readObject();
                //GemStone changes END
	}

// GemStone changes BEGIN
  @Override
  public int compareTo(Object o) {
    try {
      return value.compare(((OrderableAggregator)o).value);
    } catch (StandardException e) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "OrderableAggregator: compare failed ", e);
    }
  }
// GemStone changes END
}
