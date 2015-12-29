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
package sql.datagen;

import hydra.Log;

import java.sql.SQLException;

import com.gemstone.gemfire.LogWriter;

import sql.sqlutil.UDTPrice;

/**
 * 
 * @author Rahul Diyewar
 */

public class ImportOraDG extends com.pivotal.gemfirexd.load.Import {
  static public LogWriter log = Log.getLogWriter();

  public ImportOraDG(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      boolean hasColumnDefination, int noOfColumnsExpected, String columnTypes,
      boolean lobsInExtFile, int importCounter, String columnTypeNames,
      String udtClassNamesString) throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, hasColumnDefination, noOfColumnsExpected, columnTypes,
        lobsInExtFile, importCounter, columnTypeNames, udtClassNamesString);
  }

  public ImportOraDG(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      int noOfColumnsExpected, String columnTypes, boolean lobsInExtFile,
      int importCounter, String columnTypeNames, String udtClassNamesString)
      throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, false, noOfColumnsExpected, columnTypes, lobsInExtFile,
        importCounter, columnTypeNames, udtClassNamesString);
  }

  public Object getObject(int columnIndex) throws SQLException {
    String val = super.getString(columnIndex);
    String columnname = super.getMetaData().getColumnName(columnIndex);
    if (val != null && val.length() > 0) {
      if (getColumnType(columnIndex) == java.sql.Types.JAVA_OBJECT) {
        // rahul - todo - make it generic, currently supported only UTDPrice
        return UDTPrice.toUDTPrice(val);
      } else {
        log.error("Not a Java Object for column:" + columnname + " val:" + val);
        return null;
      }
    } else {
      log.error("Null for column:" + columnname + " val:" + val);
      return val;
    }
  }
}
