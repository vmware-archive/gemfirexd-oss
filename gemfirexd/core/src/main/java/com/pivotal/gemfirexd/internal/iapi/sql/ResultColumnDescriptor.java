/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor

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

package com.pivotal.gemfirexd.internal.iapi.sql;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.EngineConnection;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;

/**
 * A ResultColumnDescriptor describes a result column in a ResultSet.
 *
 */

public interface ResultColumnDescriptor
{
	/**
	 * Returns a DataTypeDescriptor for the column. This DataTypeDescriptor
	 * will not represent an actual value, it will only represent the type
	 * that all values in the column will have.
	 *
	 * @return	A DataTypeDescriptor describing the type of the column.
	 */
	DataTypeDescriptor	getType();

	/**
	 * Returns the name of the Column.
	 *
	 * @return	A String containing the name of the column.
	 */
	String	getName();

	/**
	 * Get the name of the schema for the Column's base table, if any.
	 * Following example queries will all return APP (assuming user is in schema APP)
	 * select t.a from t
	 * select b.a from t as b
	 * select app.t.a from t
	 *
	 * @return	The name of the schema of the Column's base table. If the column
	 *		is not in a schema (i.e. is a derived column), it returns NULL.
	 */
	String	getSourceSchemaName();

	/**
	 * Get the name of the underlying(base) table this column comes from, if any.
	 * Following example queries will all return T
	 * select a from t
	 * select b.a from t as b
	 * select t.a from t
	 *
	 * @return	A String containing the name of the base table of the Column
	 *		is in. If the column is not in a table (i.e. is a
	 * 		derived column), it returns NULL.
	 * @return	The name of the Column's base table. If the column
	 *		is not in a schema (i.e. is a derived column), it returns NULL.
	 */
	String	getSourceTableName();

	/**
	 * Return true if the column is wirtable by a positioned update.
	 *
	 * @return TRUE, if the column is a base column of a table and is 
	 * writable by a positioned update.
	 */
	boolean updatableByCursor();

	/**
	 * Get the position of the Column.
	 * NOTE - position is 1-based.
	 *
	 * @return	An int containing the position of the Column
	 *		within the table.
	 */
	int	getColumnPosition();

	/**
	 * Tell us if the column is an autoincrement column or not.
	 * 
	 * @return TRUE, if the column is a base column of a table and is an
	 * autoincrement column.
	 */
	boolean isAutoincrement();

	/*
	 * NOTE: These interfaces are intended to support JDBC. There are some
	 * JDBC methods on java.sql.ResultSetMetaData that have no equivalent
	 * here, mainly because they are of questionable use to us.  They are:
	 * getCatalogName() (will we support catalogs?), getColumnLabel(),
	 * isCaseSensitive(), isCurrency(),
	 * isDefinitelyWritable(), isReadOnly(), isSearchable(), isSigned(),
	 * isWritable()). The JDBC driver implements these itself, using
	 * the data type information and knowing data type characteristics.
	 */
// GemStone changes BEGIN
	/**
	 * Returns number of primary key columns if this column is part
	 * of the primary key else returns a value <= zero.
	 * 
	 * Used by the ADO.NET driver.
	 */
	short primaryKey(EngineConnection conn) throws StandardException;
// GemStone changes END
}
