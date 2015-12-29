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

public class ValueListMappedColumn extends MappedColumnInfo {
  private Object[] valueList;
  private boolean randomize;

  /**
   * 
   * @author Rahul Diyewar
   */

  public ValueListMappedColumn(String columnName, Object[] valueList,
      boolean random) {
    super(columnName);
    this.valueList = valueList;
    this.randomize = random;
  }

  public Object[] getValueList() {
    return valueList;
  }

  public boolean isRandomize() {
    return randomize;
  }

  @Override
  public String toString() {
    String values = "[";
    int c = valueList.length;
    int i = 0;
    while (i < 5 && i < c) {
      values += valueList[i] + " ";
      i++;
    }
    values += (i < c) ? "...]" : "]";

    StringBuffer sb = new StringBuffer();
    sb.append(this.getClass().getName() + "[")
        .append("columnName=" + getColumnName())
        .append(",randomize=" + randomize).append(",valueList=" + values)
        .append("]");
    return sb.toString();
  }
}
