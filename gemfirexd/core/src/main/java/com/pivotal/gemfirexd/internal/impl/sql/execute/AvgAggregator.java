/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.AvgAggregator

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

// GemStone changes BEGIN
// GemStone changes EN

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	Aggregator for AVG(). Extends the SumAggregator and
	implements a count. Result is then sum()/count().
	To handle overflow we catch the exception for
	value out of range, then we swap the holder for
	the current sum to one that can handle a larger
	range. Eventually a sum may end up in a SQLDecimal
	which can handle an infinite range. Once this
	type promotion has happened, it will not revert back
	to the original type, even if the sum would fit in
	a lesser type.

 */
public final class AvgAggregator extends SumAggregator
{
	private long count;
	private int scale;
// GemStone changes BEGIN

	@Override
	public void setup(String aggregateName, DataValueDescriptor initValue)
	    throws StandardException {
	  throw new AssertionError("Should never had been called for Avg");
	}

	public void setup(String aggregateName, DataValueDescriptor initValue,
	    long initCount) throws StandardException {
	  value = initValue;
	  count = initCount;
	  if (count == 0) {
	    switch (initValue.getTypeFormatId()) {
	      case StoredFormatIds.SQL_INTEGER_ID:
	      case StoredFormatIds.SQL_LONGINT_ID:
	      case StoredFormatIds.SQL_SMALLINT_ID:
	      case StoredFormatIds.SQL_TINYINT_ID:
	        this.scale = 0;
	        break;
	      case StoredFormatIds.SQL_DOUBLE_ID:
	      case StoredFormatIds.SQL_REAL_ID:
	        this.scale = TypeId.DECIMAL_SCALE;
	        break;
	      default: // DECIMAL
	        this.scale = ((NumberDataValue)value).getDecimalValueScale();
	        if (this.scale < NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE) {
	          this.scale = NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE;
	        }
	        break;
	    }
          }
        }

	@Override
	public final void clear() {
	  super.clear();
	  this.count = 0;
	}
// GemStone changes END

	protected void basicAccumulate(DataValueDescriptor addend) 
		throws StandardException
	{

		if (count == 0) {

// GemStone changes BEGIN
		  switch (addend.getTypeFormatId()) {
		    case StoredFormatIds.SQL_INTEGER_ID:
		    case StoredFormatIds.SQL_LONGINT_ID:
		    case StoredFormatIds.SQL_SMALLINT_ID:
		    case StoredFormatIds.SQL_TINYINT_ID:
		      this.scale = 0;
		      break;
		    case StoredFormatIds.SQL_REAL_ID:
		    case StoredFormatIds.SQL_DOUBLE_ID:
		      this.scale = TypeId.DECIMAL_SCALE;
		      break;
		    default:
		      // DECIMAL
		      scale = ((NumberDataValue) addend).getDecimalValueScale();
		      if (scale < NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE) {
		        scale = NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE;
		      }
		      break;
		  }
		  /* (original code)
			String typeName = addend.getTypeName();
			if (   typeName.equals(TypeId.TINYINT_NAME)
				|| typeName.equals(TypeId.SMALLINT_NAME)
				|| typeName.equals(TypeId.INTEGER_NAME)
				|| typeName.equals(TypeId.LONGINT_NAME)) {
				scale = 0;
			} else if (   typeName.equals(TypeId.REAL_NAME)
				|| typeName.equals(TypeId.DOUBLE_NAME)) {
				scale = TypeId.DECIMAL_SCALE;
			} else {
				// DECIMAL
				scale = ((NumberDataValue) addend).getDecimalValueScale();
				if (scale < NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE)
					scale = NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE;
			}
		  */
// GemStone changes END
		}

		try {

			super.basicAccumulate(addend);
			count++;
			return;

		} catch (StandardException se) {

			if (!se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE))
				throw se;
		}


		/*
			Sum is out of range so promote

			TINYINT,SMALLINT -->> INTEGER

			INTEGER -->> BIGINT

			REAL -->> DOUBLE PRECISION

			others -->> DECIMAL
		*/

		// this code creates data type objects directly, it is anticipating
		// the time they move into the defined api of the type system. (djd).
// GemStone changes BEGIN
		this.value = promoteType();
		/* (original code)
		String typeName = value.getTypeName();
		
		DataValueDescriptor newValue;

		if (typeName.equals(TypeId.INTEGER_NAME)) {
			newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLLongint();
		} else if (typeName.equals(TypeId.TINYINT_NAME) || 
				   typeName.equals(TypeId.SMALLINT_NAME)) {
			newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLInteger();
		} else if (typeName.equals(TypeId.REAL_NAME)) {
			newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLDouble();
		} else {
			TypeId decimalTypeId = TypeId.getBuiltInTypeId(java.sql.Types.DECIMAL);
			newValue = decimalTypeId.getNull();
		}

		newValue.setValue(value);
		value = newValue;
		*/
// GemStone changes END

		basicAccumulate(addend);
	}

	public void merge(ExecAggregator addend)
		throws StandardException
	{
		AvgAggregator otherAvg = (AvgAggregator) addend;

		// if I haven't been used take the other.
		if (count == 0) {
			count = otherAvg.count;
// GemStone changes BEGIN
			if (value == null) {
			  // cloning as otherAvg gets reused from
			  // GDistributedRS#GemFireAggregator
			  value = otherAvg.value.getClone();
			}
			else {
			  value.setValue(otherAvg.value);
			}
// GemStone changes END
			scale = otherAvg.scale;
	                if(SanityManager.DEBUG) {
	                  if (GemFireXDUtils.TraceAggreg) {
	                    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
	                        " assigned to " + String.valueOf(value)
	                        +  " count " + String.valueOf(count));
	                  }
	                }
			return;
		}

		// Don't bother merging if the other is a NULL value aggregate.
		/* Note:Beetle:5346 fix change the sort to be High, that makes
		 * the neccessary for the NULL check because after the change 
		 * addend could have a  NULL value even on distincts unlike when 
		 * NULLs were sort order  Low, because by  sorting NULLs Low  
		 * they  happens to be always first row which makes it as 
		 * aggreagte result object instead of addends.
		 * Query that will fail without the following check:
		 * select avg(a) , count(distinct a) from t1;
		*/
		if(otherAvg.value != null)
		{
			// subtract one here as the accumulate will add one back in
			count += (otherAvg.count - 1);
			basicAccumulate(otherAvg.value);
		}
		if(SanityManager.DEBUG) {
		  if (GemFireXDUtils.TraceAggreg) {
		    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
		        " merged from " +  String.valueOf(otherAvg.value) +
		        " to " + String.valueOf(value) +  " count "
		        + String.valueOf(count));
		  }
		}
	}

	/**
	 * Return the result of the aggregation.  If the count
	 * is zero, then we haven't averaged anything yet, so
	 * we return null.  Otherwise, return the running
	 * average as a double.
	 *
	 * @return null or the average as Double
	 */
	public DataValueDescriptor getResult() throws StandardException
	{
		if (count == 0)
		{
			return null;
		}

		NumberDataValue sum = (NumberDataValue) value;
		NumberDataValue avg = (NumberDataValue) value.getNewNull();

                if(SanityManager.DEBUG) {
                  if (GemFireXDUtils.TraceAggreg) {
                    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
                        " SUM " + sum + " Count " + String.valueOf(count));
                  }
                }
		
		if (count > (long) Integer.MAX_VALUE)
		{
			// TINYINT, SMALLINT, INTEGER implement arithmetic using integers
			// If the sum is still represented as a TINYINT, SMALLINT or INTEGER
			// we cannot let their int based arithmetic handle it, since they
			// will perform a getInt() on the long value which will truncate the long value.
			// One solution would be to promote the sum to a SQLLongint, but its value
			// will be less than or equal to Integer.MAX_VALUE, so the average will be 0.
			String typeName = sum.getTypeName();

			if (typeName.equals(TypeId.INTEGER_NAME) ||
					typeName.equals(TypeId.TINYINT_NAME) || 
					   typeName.equals(TypeId.SMALLINT_NAME))
			{
				avg.setValue(0);
				return avg;
			}
		}

		NumberDataValue countv = new com.pivotal.gemfirexd.internal.iapi.types.SQLLongint(count);
		sum.divide(sum, countv, avg, scale);
                
		return avg;
	}

	/**
	 */
	public ExecAggregator newAggregator()
	{
		return new AvgAggregator();
	}

	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeLong(count);
		out.writeInt(scale);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		count = in.readLong();
		scale = in.readInt();
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
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_AVG_V01_ID; }
}
