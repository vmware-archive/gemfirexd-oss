/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData

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

package com.pivotal.gemfirexd.internal.impl.jdbc;



import com.gemstone.gnu.trove.TObjectIntHashMap;
import com.pivotal.gemfirexd.callbacks.TableMetaData;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.EngineConnection;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeUtilities;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;

import java.sql.SQLException;
import java.sql.Types;

/**
 * A ResultSetMetaData object can be used to find out about the types
 * and properties of the columns in a ResultSet.
 *
 * <p>
 * We take the (Derby) ResultDescription and examine it, to return
 * the appropriate information.

   <P>
   This class can be used outside of this package to convert a
   ResultDescription into a ResultSetMetaData object.
   <P>
   EmbedResultSetMetaData objects are shared across multiple threads
   by being stored in the ResultDescription for a compiled plan.
   If the required api for ResultSetMetaData ever changes so
   that it has a close() method, a getConnection() method or
   any other Connection or ResultSet specific method then
   this sharing must be removed.

   XXX: IMPORTANT NOTE: ALWAYS KEEP BELOW IN SYNC WITH
   GFXDServiceImpl.updateResultSet
 *
 */
public class EmbedResultSetMetaData
	implements /* ResultSetMetaData GemStone change */ TableMetaData {

	private final ResultColumnDescriptor[] columnInfo;

// GemStone changes BEGIN
    private final TObjectIntHashMap columnNameToPosition;
    private boolean qualifiedNamesPopulated;
    private int schemaVersion;

    /**
     * Returns false unless <code>interfaces</code> is implemented
     * 
     * @param interfaces
     *          a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly
     *         wraps an object that does.
     * @throws java.sql.SQLException
     *           if an error occurs while determining whether this is a wrapper
     *           for an object with the given interface.
     */
    @Override
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
      return interfaces.isInstance(this);
    }

    /**
     * Returns <code>this</code> if this class implements the interface
     * 
     * @param interfaces
     *          a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption
     *           if no object if found that implements the interface
     */
    @Override
    public <T> T unwrap(java.lang.Class<T> interfaces) throws SQLException {
      // Derby does not implement non-standard methods on
      // JDBC objects
      // hence return this if this class implements the interface
      // or throw an SQLException
      try {
        return interfaces.cast(this);
      } catch (ClassCastException cce) {
        throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP, interfaces);
      }
    }

    // extension methods for ADO.NET below

    /**
     * Returns number of primary key columns if this column is part
     * of the primary key else returns zero.
     */
    public final short primaryKey(int column, final EngineConnection conn)
        throws SQLException {
      validColumnNumber(column);
      final ResultColumnDescriptor rcd = columnInfo[column - 1];
      try {
        short pks = rcd.primaryKey(conn);
        return (pks > 0 ? pks : 0);
      } catch (StandardException se) {
        throw Util.generateCsSQLException(se);
      }
    }
// GemStone changes END
    
	//
	// constructor
	//
	public EmbedResultSetMetaData(ResultColumnDescriptor[] columnInfo) {
		this.columnInfo = columnInfo;
		this.columnNameToPosition = null; // GemStoneAddition
	}
// GemStone changes BEGIN

	public EmbedResultSetMetaData(ResultColumnDescriptor[] columnInfo,
	    boolean forTableMetaData, int schemaVersion) {
	  this.columnInfo = columnInfo;
	  if (forTableMetaData) {
	    this.columnNameToPosition = new TObjectIntHashMap(
	        columnInfo.length);
	    int columnPos = 1;
	    String columnName;
	    for (ResultColumnDescriptor rcd : columnInfo) {
	      if ((columnName = rcd.getName()) != null) {
	        this.columnNameToPosition.putIfAbsent(columnName, columnPos, 0);
	      }
	      columnPos++;
	    }
	  }
	  else {
	    this.columnNameToPosition = null;
	  }
	  this.schemaVersion = schemaVersion;
	}

	@Override
	public int getColumnPosition(String columnName) throws SQLException {
	  if (this.columnNameToPosition != null && columnName != null) {
	    int column = this.columnNameToPosition.get(columnName);
	    if (column > 0) {
	      return column;
	    }
	    String ucolumnName = StringUtil.SQLToUpperCase(columnName);
	    column = this.columnNameToPosition.get(ucolumnName);
	    if (column > 0) {
	      return column;
	    }
	    // populate qualified names
	    if (populateQualifiedColumnNames()) {
	      column = this.columnNameToPosition.get(columnName);
	      if (column > 0) {
	        return column;
	      }
	      column = this.columnNameToPosition.get(ucolumnName);
	      if (column > 0) {
	        return column;
	      }
	    }
	  }
	  throw Util.generateCsSQLException(SQLState.LANG_COLUMN_NOT_FOUND,
	      columnName);
	}

	@Override
	public int getTableColumnPosition(int columnPosition) {
	  return columnPosition;
	}

	public ResultColumnDescriptor getColumnDescriptor(int column)
	    throws SQLException {
	  validColumnNumber(column);
	  return columnInfo[column - 1];
	}

	public final boolean isTableReadOnly() {
	  // if this is for TableMetaData then its definitely meant to
	  // be read-only
	  return this.columnNameToPosition != null;
	}

	public final boolean isDefiniteWritable_(int column) {
	  // we just don't know if it is a base table column or not
	  return false;
	}

	@Override
	public int getDeclaredColumnWidth(int column) throws SQLException {
	  return getColumnTypeDescriptor(column).getMaximumWidth();
	}

	@Override
	public int getSchemaVersion() {
	  return this.schemaVersion;
	}

	private boolean populateQualifiedColumnNames() {
	  if (this.columnInfo != null && !this.qualifiedNamesPopulated) {
	    int columnPos = 1;
	    String columnName;
	    for (ResultColumnDescriptor rcd : this.columnInfo) {
	      columnName = rcd.getName();
	      // put qualified with tableName and schemaName (#41272)
	      String tableName = rcd.getSourceTableName();
	      if (tableName != null && tableName.length() > 0) {
	        this.columnNameToPosition.putIfAbsent(
	            tableName + '.' + columnName, columnPos, 0);
	        String schemaName = rcd.getSourceSchemaName();
	        if (schemaName != null && schemaName.length() > 0) {
	          this.columnNameToPosition.putIfAbsent(schemaName + '.'
	              + tableName + '.' + columnName, columnPos, 0);
	        }
	      }
	      columnPos++;
	    }
	    this.qualifiedNamesPopulated = true;
	    return true;
	  }
	  else {
	    return false;
	  }
	}
// GemStone changes END

	//
	// ResultSetMetaData interface
	//

    /**
     * What's the number of columns in the ResultSet?
     *
     * @return the number
     */
	public final int getColumnCount()	{
		return columnInfo.length;
	}

    /**
     * Is the column automatically numbered, thus read-only?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     *
     */
	public final boolean isAutoIncrement(int column) throws SQLException	{
        validColumnNumber(column);
		ResultColumnDescriptor rcd = columnInfo[column - 1];
		return rcd.isAutoincrement();
	}

    /**
     * Does a column's case matter?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isCaseSensitive(int column) throws SQLException	{
	  return DataTypeUtilities.isCaseSensitive(getColumnTypeDescriptor(column));
	}


    /**
     * Can the column be used in a where clause?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isSearchable(int column) throws SQLException	{
		validColumnNumber(column);

		// we have no restrictions yet, so this is always true
		// might eventually be false for e.g. extra-long columns?
		return true;
	}

    /**
     * Is the column a cash value?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isCurrency(int column) throws SQLException	{

		return DataTypeUtilities.isCurrency(getColumnTypeDescriptor(column));
	}

    /**
     * Can you put a NULL in this column?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return columnNoNulls, columnNullable or columnNullableUnknown
	 * @exception SQLException thrown on failure
     */
	public final int isNullable(int column) throws SQLException	{
		return DataTypeUtilities.isNullable(getColumnTypeDescriptor(column));
	}

    /**
     * Is the column a signed number?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isSigned(int column) throws SQLException	{
		return DataTypeUtilities.isSigned(getColumnTypeDescriptor(column));
	}


    /**
     * What's the column's normal max width in chars?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return max width
	 * @exception SQLException thrown on failure
     */
	public final int getColumnDisplaySize(int column) throws SQLException	{
		return DataTypeUtilities.getColumnDisplaySize(getColumnTypeDescriptor(column));
	}

    /**
     * What's the suggested column title for use in printouts and
     * displays?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final String getColumnLabel(int column) throws SQLException {
		ResultColumnDescriptor cd = columnInfo[column - 1];
		String s = cd.getName();

		// we could get fancier than this, but it's simple
    	return (s==null? "Column"+Integer.toString(column) : s);
	}


    /**
     * What's a column's name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
	 * @exception SQLException thrown on failure
     */
	public final String getColumnName(int column) throws SQLException	{
		ResultColumnDescriptor cd = columnInfo[column - 1];
		String s = cd.getName();
		// database returns null when no column name to differentiate from empty name
    	return (s==null? "" : s);

	}


    /**
     * What's a column's table's schema?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
	 * @exception SQLException thrown on failure
     */
	public final String getSchemaName(int column) throws SQLException	{
		ResultColumnDescriptor cd = columnInfo[column - 1];

		String s = cd.getSourceSchemaName();
		// database returns null when no schema name to differentiate from empty name
		return (s==null? "" : s);
	}

    /**
     * What's a column's number of decimal digits?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
	 * @exception SQLException thrown on failure
     */
	public final int getPrecision(int column) throws SQLException	{
		return DataTypeUtilities.getDigitPrecision(getColumnTypeDescriptor(column));
	}


    /**
     * What's a column's number of digits to right of the decimal point?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
	 * @exception SQLException thrown on failure
     */
	public final int getScale(int column) throws SQLException	{
		DataTypeDescriptor dtd = getColumnTypeDescriptor(column);
		// REMIND -- check it is valid to ask for scale
		return dtd.getScale();
	}

    /**
     * What's a column's table name?
     *
     * @return table name or "" if not applicable
	 * @exception SQLException thrown on failure
     */
	public final String getTableName(int column) throws SQLException {
		ResultColumnDescriptor cd = columnInfo[column - 1];
		String s = cd.getSourceTableName();

		// database returns null when no table name to differentiate from empty name
		return (s==null? "" : s);
	}

    /**
     * What's a column's table's catalog name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name or "" if not applicable.
	 * @exception SQLException thrown on failure
     */
	public final String getCatalogName(int column) throws SQLException {
		validColumnNumber(column);
		return "";
	}

    /**
     * What's a column's SQL type?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type
     * @see Types
	 * @exception SQLException thrown on failure
     */
	public final int getColumnType(int column) throws SQLException {
		DataTypeDescriptor dtd = getColumnTypeDescriptor(column);
		return dtd.getTypeId().getJDBCTypeId();
	}

    /**
     * What's a column's data source specific type name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name
	 * @exception SQLException thrown on failure
     */
	public final String getColumnTypeName(int column) throws SQLException	{
		DataTypeDescriptor dtd = getColumnTypeDescriptor(column);
		return dtd.getTypeId().getSQLTypeName();
	}

    /**
     * Is a column definitely not writable?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isReadOnly(int column) throws SQLException {
		validColumnNumber(column);

// GemStone changes BEGIN
		// if this is for TableMetaData then its definitely meant to
		// be read-only
		return isTableReadOnly();
		/* (original code)
		// we just don't know if it is a base table column or not
		return false;
		*/
// GemStone changes END
	}

    /**
     * Is it possible for a write on the column to succeed?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isWritable(int column) throws SQLException {
		validColumnNumber(column);
		return columnInfo[column - 1].updatableByCursor();
	}

    /**
     * Will a write on the column definitely succeed?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if so
	 * @exception SQLException thrown on failure
     */
	public final boolean isDefinitelyWritable(int column) throws SQLException	{
		validColumnNumber(column);

// GemStone changes BEGIN
		return isDefiniteWritable_(column);
		/* (original code)
		// we just don't know if it is a base table column or not
		return false;
		*/
// GemStone changes END
	}

	/*
	 * class interface
	 */

	private void validColumnNumber(int column) throws SQLException {
	  if (column < 1 ||
		        column > getColumnCount() )
			    throw Util.generateCsSQLException(
                      SQLState.COLUMN_NOT_FOUND, new Integer(column));
	}

	private DataTypeDescriptor getColumnTypeDescriptor(int column) throws SQLException 
	{
		validColumnNumber(column);

		ResultColumnDescriptor cd = columnInfo[column - 1];

		return cd.getType();
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 2.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     * JDBC 2.0
     *
     * <p>Return the fully qualified name of the Java class whose instances 
     * are manufactured if ResultSet.getObject() is called to retrieve a value 
     * from the column.  ResultSet.getObject() may return a subClass of the
     * class returned by this method.
	 *
	 * @exception SQLException Feature not inplemented for now.
     */
    public final String getColumnClassName(int column) throws SQLException {
		
		return getColumnTypeDescriptor(column).getTypeId().getResultSetMetaDataTypeName();
	}


	public static ResultColumnDescriptor getResultColumnDescriptor(String name, int jdcbTypeId, boolean nullable) {

		return new com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor(
			name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdcbTypeId, nullable));
	}
	public static ResultColumnDescriptor getResultColumnDescriptor(String name, int jdcbTypeId, boolean nullable, int length) {

		return new com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor(
			name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdcbTypeId, nullable, length));
	}
	public static ResultColumnDescriptor getResultColumnDescriptor(String name, DataTypeDescriptor dtd) {
		return new com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor(name, dtd);
	}
}
