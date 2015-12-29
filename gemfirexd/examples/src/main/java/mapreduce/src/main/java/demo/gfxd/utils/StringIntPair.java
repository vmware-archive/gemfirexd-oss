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
package demo.gfxd.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringIntPair implements WritableComparable<StringIntPair> {

  private String first;
  private int second;

  public StringIntPair() {}

  public StringIntPair(String first, int second) {
    this.first = first;
    this.second = second;
  }

  public void set(String left, int right) {
    first = left;
    second = right;
  }

  public String getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readUTF();
    second = in.readInt();
  }

  @Override
  public int compareTo(StringIntPair o) {
    int c = first.compareTo(o.first);
    if (c != 0) {
      return c;
    } else if (second != o.second) {
      return second < o.second ? -1 : 1;
    } else {
      return 0;
    }
  }

  @Override
  public int hashCode() {
    return first.hashCode() + 157 * second;
  }

  @Override
  public boolean equals(Object right) {
    if (right instanceof StringIntPair) {
      StringIntPair r = (StringIntPair) right;
      return r.first == first && r.second == second;
    } else {
      return false;
    }
  }
}
