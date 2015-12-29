/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.SumAggregator

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
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * Aggregator for SUM().  Defers most of its work
 * to OrderableAggregator.
 *
 */
public  class SumAggregator 
	extends OrderableAggregator
{
	/**
	 * Accumulate
 	 *
	 * @param addend	value to be added in
	 *
	 * @exception StandardException on error
	 *
	 * @see ExecAggregator#accumulate
	 */
	protected void basicAccumulate(DataValueDescriptor addend) 
		throws StandardException
	{

		/*
		** If we don't have any value yet, just clone
		** the addend.
		*/
		if (value == null)
		{ 
			/* NOTE: We need to call getClone() since value gets 
			 * reused underneath us
			 */
			value = addend.getClone();
		}
// GemStone changes BEGIN
		else if (value.isNull()) {
		  if (!addend.isNull()) {
		    value.setValue(addend);
		  }
		}
// GemStone changes END
		else
		{
			NumberDataValue	input = (NumberDataValue)addend;
			NumberDataValue nv = (NumberDataValue) value;
			               // GemStone changes BEGIN
			                 try{
			                // GemStone changes BEGIN
			value = nv.plus(
						input,						// addend 1
						nv,		// addend 2
						nv);	// result
// GemStone changes BEGIN
			                 } catch (StandardException se) {
			                    if (!se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE))
			                      throw se;
			                    value = promoteType();
			                    basicAccumulate(addend);
			                 }
// GemStone changes END
		}
	}

// GemStone changes BEGIN
  protected boolean typePromoted;

  @Override
  public void clear() {
    if (this.value != null) {
      if (this.typePromoted) {
        // reset to null to allow creating afresh
        this.value = null;
        this.typePromoted = false;
      }
      else {
        // try to reuse for groupings
        this.value.restoreToNull();
      }
    }
    super.clear();
  }

  protected final DataValueDescriptor promoteType() throws StandardException {
    /*
     * Sum is out of range so promote
     * 
     * TINYINT,SMALLINT -->> INTEGER
     * 
     * INTEGER -->> BIGINT
     * 
     * REAL -->> DOUBLE PRECISION
     * 
     * others -->> DECIMAL
     */

    // this code creates data type objects directly, it is anticipating
    // the time they move into the defined api of the type system. (djd).
    DataValueDescriptor newValue;
    switch (this.value.getTypeFormatId()) {
      case StoredFormatIds.SQL_INTEGER_ID:
        newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLLongint();
        break;
      case StoredFormatIds.SQL_SMALLINT_ID:
      case StoredFormatIds.SQL_TINYINT_ID:
        newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLInteger();
        break;
      case StoredFormatIds.SQL_REAL_ID:
        newValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLDouble();
        break;
      default:
        TypeId decimalTypeId = TypeId.getBuiltInTypeId(java.sql.Types.DECIMAL);
        newValue = decimalTypeId.getNull();
        break;
    }
    newValue.setValue(this.value);
    this.typePromoted = true;
    return newValue;
  }

// GemStone changes END

	/**
 	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		return new SumAggregator();
	}

	////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_SUM_V01_ID; }
}
