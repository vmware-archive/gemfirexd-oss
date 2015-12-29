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

import com.gemstone.gemfire.LogWriter;

/**
 * 
 * @author Rahul Diyewar
 */

public class MappedColumnInfo {
  static public LogWriter log = Log.getLogWriter();

  private String columnName;
  private int minLen = -1;
  private int maxLen = -1;
  private int prec = -1;
  private boolean isFKParent;

  public MappedColumnInfo(String columnName) {
    this.columnName = columnName;
  }

  public void setRange(int min, int max, int prec) {
    this.minLen = min;
    this.maxLen = max;
    this.prec = prec;
  }

  public int getMinLen() {
    return minLen;
  }

  public int getMaxLen() {
    return maxLen;
  }

  public int getPrec() {
    return prec;
  }

  public String getColumnName() {
    return columnName;
  }

  public boolean isFKParent() {
    return isFKParent;
  }

  public void setFKParent(boolean isFKParent) {
    this.isFKParent = isFKParent;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.getClass().getName() + "[")
        .append("columnName=" + getColumnName())
        .append(",minLen=" + getMinLen()).append(",maxLen=" + getMaxLen())
        .append(",isFKParent=" + isFKParent()).append(",prec=" + getPrec())
        .append("]");
    return sb.toString();
  }
}
