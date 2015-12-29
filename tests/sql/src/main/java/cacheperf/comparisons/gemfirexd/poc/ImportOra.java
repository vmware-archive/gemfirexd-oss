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
package cacheperf.comparisons.gemfirexd.poc;

import java.sql.SQLException;

public class ImportOra extends com.pivotal.gemfirexd.load.Import {

  public ImportOra(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      int noOfColumnsExpected, String columnTypes, boolean lobsInExtFile,
      int importCounter, String columnTypeNames, String udtClassNamesString)
      throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, noOfColumnsExpected, columnTypes,
        lobsInExtFile, importCounter, columnTypeNames, udtClassNamesString);
  }

  public String getString(int columnIndex) throws SQLException {
    String val = super.getString(columnIndex);
    if (val != null && val.length() > 0) {
      switch (getColumnType(columnIndex)) {
        case java.sql.Types.DATE:
          return val.substring(0, 4) + '-' + val.substring(4, 6) + '-'
            + val.substring(6, 8);
        case java.sql.Types.TIMESTAMP:
          return val.substring(0, 4) + '-' + val.substring(4, 6) + '-'
            + val.substring(6, 8) + ' ' + val.substring(8);
        default:
          return val;
      }
    }
    else {
      return val;
    }
  }
}
