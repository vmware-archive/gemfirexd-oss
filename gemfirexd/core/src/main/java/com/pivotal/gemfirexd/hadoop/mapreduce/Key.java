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
package com.pivotal.gemfirexd.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A hadoop serializable representation of a changed row's primary key. The key
 * object is created by input format for internal use and is provided to user as
 * a reference to the event. It cannot be and should not be used for updating data
 * in database.
 * 
 * @author ashvina
 */
public class Key implements WritableComparable<Key> {

  private byte[] key;
  private static final byte[] EMPTY_BYTES = new byte[0];

  public Key() {
    setKey(null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(key.length);
    out.write(key, 0, key.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    key = new byte[len];
    in.readFully(key, 0, len);
  }

  @Override
  public final int compareTo(Key k) {
    byte[] b1 = this.key;
    byte[] b2 = k.key;

    return WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
  }

  public final void setKey(byte[] k) {
    if (k != null) {
      this.key = k;
    }
    else {
      this.key = EMPTY_BYTES;
    }
  }

  public final byte[] getKey() {
    return key;
  }
}
