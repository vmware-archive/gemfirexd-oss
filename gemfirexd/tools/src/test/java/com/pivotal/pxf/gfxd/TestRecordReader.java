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
package com.pivotal.pxf.gfxd;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.pivotal.pxf.gfxd.util.TestDataHelper;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;

/**
 * @author ashetkar
 *
 */
public class TestRecordReader implements RecordReader<Object, Object> {

  private int numOfRecords = TestDataHelper.RECORDS_PER_SPLIT;
  private int currentRecord = 0;
  protected InputSplit split;

  public TestRecordReader(JobConf job, InputSplit split) {
    super();
    this.split = split;
  }

  public void close() throws IOException {
    
  }

  public boolean next(Object key, Object val) throws IOException {
    if (this.currentRecord == this.numOfRecords) {
      return false;
    }
    // It serializes the test data before returning. The resolver should
    // separate the individual field data from it.
    Object[] record = TestDataHelper.getSplitRecords(this.split).get(this.currentRecord);
    byte[] k = TestDataHelper.serialize(record[0]);
    byte[] v = TestDataHelper.serialize(record);
    ((KeyClass)key).setValue(k);
    ((ValueClass)val).setValue(v);
    ++this.currentRecord;
    return true;
  }

  public Object createKey() {
    return new KeyClass();
  }

  public Object createValue() {
    return new ValueClass();
  }

  public long getPos() throws IOException {
    return 0;
  }

  public float getProgress() throws IOException {
    return 0;
  }

  public class KeyClass extends Key {
    private byte[] bytes;
    public void setValue(byte[] val) {
      this.bytes = val;
    }
    public byte[] getValue() {
      return this.bytes;
    }
    public String toString() {
      return "hello" + this.bytes;
    }
  }

  public class ValueClass extends Row {
    private byte[] bytes;
    public void setValue(byte[] val) {
      this.bytes = val;
    }
    public byte[] getValue() {
      return this.bytes;
    }
    public String toString() {
      return "hello" + this.bytes;
    }
  }

}

