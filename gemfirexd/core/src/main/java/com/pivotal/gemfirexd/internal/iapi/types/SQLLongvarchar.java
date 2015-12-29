/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.types.SQLLongvarchar

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

package com.pivotal.gemfirexd.internal.iapi.types;

import java.text.RuleBasedCollator;

import com.gemstone.gemfire.internal.DSCODE;



import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.StringDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * SQLLongvarchar represents a LONG VARCHAR value with UCS_BASIC collation.
 *
 * SQLLongvarchar is mostly the same as SQLVarchar, so it is implemented as a
 * subclass of SQLVarchar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLLongvarchar
	extends SQLVarchar
{
	/*
	 * DataValueDescriptor interface.
	 *
	 * These are actually all implemented in the super-class, but we need
	 * to duplicate some of them here so they can be called by byte-code
	 * generation, which needs to know the class the method appears in.
	 */

	public String getTypeName()
	{
		return TypeId.LONGVARCHAR_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new SQLLongvarchar(getString());
		}
		catch (StandardException se)
		{
// GemStone changes BEGIN
		  throw GemFireXDRuntimeException.newRuntimeException(
		      "Unexpected exception", se);
		  /* (original code)
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception", se);
			return null;
		  */
// GemStone changes END
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongvarchar();
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;			
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLLongvarchar
		     CollatorSQLLongvarchar s = new CollatorSQLLongvarchar(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.pivotal.gemfirexd.internal.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_LONGVARCHAR_ID;
	}

	/*
	 * constructors
	 */

	public SQLLongvarchar()
	{
	}

	public SQLLongvarchar(String val)
	{
		super(val);
	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{
		//bug 5592 - for sql long varchar, any truncation is disallowed ie even the trailing blanks can't be truncated
		if (sourceValue.length() > desiredType.getMaximumWidth())
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), StringUtil.formatForPrint(sourceValue), String.valueOf(desiredType.getMaximumWidth()));

		setValue(sourceValue);
	}

	/**
	 * @see StringDataValue#concatenate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue concatenate(
				StringDataValue leftOperand,
				StringDataValue rightOperand,
				StringDataValue result)
		throws StandardException
	{
		super.concatenate(leftOperand, rightOperand, result);

		//bug 5600 - according to db2 concatenation documentation, for compatibility with previous versions, there is no automatic
		//escalation of results involving LONG data types to LOB data types. For eg, concatenation of a CHAR(200) value and a
		//completely full LONG VARCHAR value would result in an error rather than in a promotion to a CLOB data type

		//need to check for concatenated string for null value
		if ((result.getString() != null) && (result.getString().length() > TypeId.LONGVARCHAR_MAXWIDTH))
			throw StandardException.newException(SQLState.LANG_CONCAT_STRING_OVERFLOW, "CONCAT", String.valueOf(TypeId.LONGVARCHAR_MAXWIDTH));

		return result;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARCHAR_PRECEDENCE;
	}

// GemStone changes BEGIN

  @Override
  public byte getTypeId() {
    return DSCODE.HUGE_STRING;
  }

// GemStone changes END
}
