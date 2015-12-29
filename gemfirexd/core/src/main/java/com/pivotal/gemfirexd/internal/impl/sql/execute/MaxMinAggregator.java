/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.MaxMinAggregator

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


import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Aggregator for MAX()/MIN().  Defers most of its work
 * to OrderableAggregator.
 *
 * @see OrderableAggregator
 *
 */
public final class MaxMinAggregator 
	extends OrderableAggregator
{

	private boolean isMax; // true for max, false for min

	/**
	 */
// GemStone changes BEGIN
	@Override
	public void setup(String aggregateName, AggregatorInfo aggInfo) {
	  isMax = aggregateName.equals("MAX");
	  if (aggInfo != null) {
	    super.setup(aggInfo);
	  }
	}
	/* (original code)
	public void setup(String aggregateName)
	{
		super.setup(aggregateName);
		isMax = aggregateName.equals("MAX");
	}
	*/
	/**
	 * Accumulate
 	 *
	 * @param addend	value to be added in
	 *
	 * @exception StandardException on error
	 *
	 */
	protected void basicAccumulate(DataValueDescriptor addend) 
		throws StandardException
	{
// GemStone changes BEGIN
	  if (this.value == null) {
	    /* NOTE: We need to call getClone() since value gets
	     * reused underneath us
	     */
	    this.value = addend.getClone();
	  }
	  else if (this.value.isNull()) {
	    if (!addend.isNull()) {
	      this.value.setValue(addend);
	    }
	  }
	  else if (this.isMax) {
	    if (this.value.compare(addend) < 0) {
	      this.value.setValue(addend);
	    }
	  }
	  else {
	    if (this.value.compare(addend) > 0) {
	      this.value.setValue(addend);
	    }
	  }
	  /* (original code)
		if ( (value == null) ||
			      (isMax && (value.compare(addend) < 0)) ||
				  (!isMax && (value.compare(addend) > 0))
				  )
		{
			/* NOTE: We need to call getClone() since value gets
			 * reused underneath us
			 *
			value = addend.getClone();
		}
	  */
// GemStone changes END
	}

	/**
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		MaxMinAggregator ma = new MaxMinAggregator();
		ma.isMax = isMax;
		return ma;
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeBoolean(isMax);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isMax = in.readBoolean();
	}
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_MAX_MIN_V01_ID; }
}
