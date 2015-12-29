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

/**
 * 
 * @author Rahul Diyewar
 */

public class FKMappedColumn extends MappedColumnInfo {
  private String fkParentTable;
  private String fkParentColumn;
  private int minRepeatValue = -1;
  private int maxRepeatValue = -1;

  public FKMappedColumn(String columnName) {
    super(columnName);
  }

  public void setFKRelationship(String fkParentStr, int minRepeat, int maxRepeat) {
    String parent = fkParentStr.trim().toUpperCase();
    fkParentTable = parent.substring(0, parent.lastIndexOf("."));
    fkParentColumn = parent.substring(parent.lastIndexOf(".") + 1);
    minRepeatValue = minRepeat;
    maxRepeatValue = maxRepeat;
  }

  public String getFullFkParentColumn() {
    return fkParentTable + "." + fkParentColumn;
  }

  public String getFkParentColumn() {
    return fkParentColumn;
  }

  public String getFkParentTable() {
    return fkParentTable;
  }

  public int getMinRepeatValue() {
    return minRepeatValue;
  }

  public int getMaxRepeatValue() {
    return maxRepeatValue;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.getClass().getName() + "[")
        .append("columnName=" + getColumnName())
        .append(",fkParentTable=" + fkParentTable)
        .append(",fkParentColumn=" + fkParentColumn)
        .append(",minRepeatValue=" + minRepeatValue)
        .append(",maxRepeatValue=" + maxRepeatValue).append("]");
    return sb.toString();
  }
}
