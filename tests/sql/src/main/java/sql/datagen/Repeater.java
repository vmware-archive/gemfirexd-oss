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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import com.google.common.primitives.Ints;

/**
 * @author Rahul Diyewar
 */
public class Repeater {
  String columnName;

  int numTimes;

  public Repeater(String columnName, int numTimes) {
    this.columnName = columnName;
    this.numTimes = numTimes;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    Repeater other = (Repeater)obj;
    return columnName.equals(other.columnName);
  }

  public int hashCode() {
    return columnName.hashCode();
  }

  public String toString() {
    return "repeat=" + numTimes;
  }
}
