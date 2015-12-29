/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.load;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Allows users to extend the builtin Import class for custom overrides when
 * loading data using SYSCS_UTIL.IMPORT_TABLE_EX/IMPORT_DATA_EX procedures.
 * 
 * The Import procedure implements a {@link ResultSet}, so users can
 * override/transform the individual columns or rows being returned from the
 * import procedure. Use the {@link #getColumnType(int)} method to get the
 * actual type of a column ({@link #getMetaData()} will only return
 * binary/lob/string/udt types). The {@link #getCurrentRow()} method will return
 * the whole row as an array of strings while {@link #getCurrentLineNumber()}
 * returns the current line number (latter can be useful for error messages). To
 * override individual columns, one or more of the following methods can be
 * overridden:
 * <p>
 * {@link ResultSet#getBytes(int)}: for BINARY types; data is normally
 * serialized as a hex string
 * <p>
 * {@link ResultSet#getBlob(int)}: for BLOB data types; note that if the LOBs
 * are in an external file (IMPORT_XXX_LOBS_FROM_EXTFILE procedure), then an
 * override may need to take care of reading from that external file
 * <p>
 * {@link ResultSet#getClob(int)}: for CLOB data types; note that if the LOBs
 * are in an external file (IMPORT_XXX_LOBS_FROM_EXTFILE procedure), then an
 * override may need to take care of reading from that external file
 * <p>
 * {@link ResultSet#getObject(int)}: for user-defined types; data is normally
 * serialized as hex string, so typically implementations will invoke
 * {@link ResultSet#getBytes(int)} to get the bytes and then deserialize it
 * <p>
 * {@link ResultSet#getString(int)}: for all other data types the string read
 * from the file is returned
 */
public class Import extends com.pivotal.gemfirexd.internal.impl.load.ImportBase {

  /**
   * Constructor to invoke Import from a select statement. This will be invoked
   * internally by the import procedures.
   */
  public Import(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      int noOfColumnsExpected, String columnTypes, boolean lobsInExtFile,
      int importCounter, String columnTypeNames, String udtClassNamesString)
      throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, false, noOfColumnsExpected, columnTypes, lobsInExtFile,
        importCounter, columnTypeNames, udtClassNamesString);
  }
  
  /**
   * Constructor to invoke Import from a select statement. This will be invoked
   * internally by the import procedures.
   * 
   * This have additional argument of controlling whether input file itself have
   * column specification list in the first line or not.
   */
  public Import(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      boolean hasColumnDefinitionInFile, int noOfColumnsExpected,
      String columnTypes, boolean lobsInExtFile, int importCounter,
      String columnTypeNames, String udtClassNamesString) throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, hasColumnDefinitionInFile, noOfColumnsExpected,
        columnTypes, lobsInExtFile, importCounter, columnTypeNames,
        udtClassNamesString);
  }
}
