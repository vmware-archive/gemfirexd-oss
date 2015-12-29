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
package sql.poc.useCase2.perf;

import java.sql.Blob;
import java.sql.SQLException;

import com.pivotal.gemfirexd.load.ImportBlob;


public class ImportOraSp extends com.pivotal.gemfirexd.load.Import{
  public ImportOraSp(String inputFileName, String columnDelimiter,
      String characterDelimiter, String codeset, long offset, long endPosition,
      int noOfColumnsExpected, String columnTypes, boolean lobsInExtFile,
      int importCounter, String columnTypeNames, String udtClassNamesString)
      throws SQLException {
    super(inputFileName, columnDelimiter, characterDelimiter, codeset, offset,
        endPosition, noOfColumnsExpected, columnTypes, lobsInExtFile,
        importCounter, columnTypeNames, udtClassNamesString);
  }

  @Override
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
  
  String stringToHex(String str) { 
    char[] chars = str.toCharArray();
    StringBuffer strBuffer = new StringBuffer();
    for (int i = 0; i < chars.length; i++) {
      strBuffer.append(Integer.toHexString((int) chars[i]));
    }
    return strBuffer.toString();
  }
  
  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    if (this.lobsInExtFile) {
      return super.getBlob(columnIndex);
    }
    else {
      // data is in the main export file, stored in hex format.
      String hexData = getCurrentRow()[columnIndex - 1];
      byte[] data = null;
      if (hexData != null) {
        //TODO temporarily work around the wrong formated data in sample useCase2 DOC_3.dat
        //should not be included as this is wrong 
        hexData = stringToHex(hexData);
        //TODO the above needs to be removed
        
        
        
        data = fromHexString(hexData, 0, hexData.length());
        // fromHexString() returns null if the hex string
        // is invalid one. It is invalid if the data string
        // length is not multiple of 2 or the data string
        // contains non-hex characters.
        if (data != null) {
          this.wasNull = false;
          return new ImportBlob(data);
        }
        else {
          throw new SQLException("An invalid hexadecimal string '" + hexData
              + "' detected in the import file at line "
              + getCurrentLineNumber() + " column " + columnIndex, "XIE0N",
              20000);
        }
      }
      else {
        this.wasNull = true;
        return null;
      }
    }
  }

  public static byte[] fromHexString(final String s, final int offset,
      int length) {
    int j = 0;
    final byte[] bytes;
    if ((length % 2) != 0) {
      // prepend a zero at the start
      bytes = new byte[(length + 1) >>> 1];
      final int low_nibble = Character.digit(s.charAt(offset), 16);
      if (low_nibble == -1) {
        // illegal format
        return null;
      }
      bytes[j++] = (byte)(low_nibble & 0x0f);
      length--;
    }
    else {
      bytes = new byte[length >>> 1];
    }

    final int end = offset + length;
    for (int i = offset; i < end; i += 2) {
      final int high_nibble = Character.digit(s.charAt(i), 16);
      final int low_nibble = Character.digit(s.charAt(i + 1), 16);
      if (high_nibble == -1 || low_nibble == -1) {
        // illegal format
        return null;
      }
      bytes[j++] = (byte)(((high_nibble << 4) & 0xf0) | (low_nibble & 0x0f));
    }
    return bytes;
  }
}
