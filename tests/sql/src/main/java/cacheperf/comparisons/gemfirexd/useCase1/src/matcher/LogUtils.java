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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher;

import hydra.Log;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class LogUtils {

  public static String getErrorStateValueArrayStr(int[] errorStateValue) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < errorStateValue.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append(errorStateValue[i]);
    }
    return sb.toString();
  }

  public static String getResultSetArrayStr(ResultSet[] rs, int colWidths)
  throws SQLException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < rs.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append(getResultSetStr(rs[i], colWidths));
    }
    return sb.toString();
  }

  public static String getResultSetStr(ResultSet rs, int colWidths)
  throws SQLException {
    if (rs == null) {
      return "null";
    }
    ResultSetMetaData rsmd = rs.getMetaData();
    int colCount = rsmd.getColumnCount();
    StringBuffer sb = new StringBuffer();
    for (int i = 1; i <= colCount; i++) {
      String label = String.format("%1$#" + colWidths + "s", rsmd.getColumnLabel(i));
      sb.append(label + "|");
    }
    sb.append("\n");
    while (rs.next()) {
      StringBuffer row = new StringBuffer();
      for (int i = 1; i <= colCount; i++) {
        String value = rs.getString(i);
        if (value == null) {
          value = "null";
        }
        value = value.substring(0, Math.min(20, value.length()));
        value = String.format("%1$#" + colWidths + "s", value.trim());
        row.append(value + "|");
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
