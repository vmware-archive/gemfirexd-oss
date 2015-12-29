/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.types.SQLLongVarbit

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





import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Limits;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.types.BitDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.Orderable;
import com.pivotal.gemfirexd.internal.iapi.types.StringDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQLLongVarbit represents the SQL type LONG VARCHAR FOR BIT DATA
 * It is an extension of SQLVarbit and is virtually indistinguishable
 * other than normalization.
 */
public class SQLLongVarbit extends SQLVarbit
{

	public String getTypeName()
	{
		return TypeId.LONGVARBIT_NAME;
	}

	/**
	 * Return max memory usage for a SQL LongVarbit
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_LONGVARCHAR_MAXWIDTH;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongVarbit();
	}

	/**
		Return my format identifier.

		@see com.pivotal.gemfirexd.internal.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_LONGVARBIT_ID;
	}


	/*
	 * Orderable interface
	 */


	/*
	 * Column interface
	 */


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */
	public SQLLongVarbit()
	{
	}

	public SQLLongVarbit(byte[] val)
	{
		super(val);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarbit, for example, when inserting into a SQLVarbit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * This overrides SQLBit -- the difference is that we don't
	 * expand SQLVarbits to fit the target.
	 * 
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 *
	 * @exception StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */

	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
					throws StandardException
	{
// GemStone changes BEGIN
	        // TODO: SW: right now commenting this out since we do
	        // deserialize the stream into memory and this causes the same
	        // stream to be read twice in case of bulk DMLs
	        /* (original code)
		if (source instanceof SQLLongVarbit) {
			// avoid creating an object in memory if a matching type.
			// this may be a stream.
			SQLLongVarbit other = (SQLLongVarbit) source;
			this.stream = other.stream;
			this.dataValue = other.dataValue;
		}
		else
		*/
// GemStone changes END
			setValue(source.getBytes());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARBIT_PRECEDENCE;
	}
}
