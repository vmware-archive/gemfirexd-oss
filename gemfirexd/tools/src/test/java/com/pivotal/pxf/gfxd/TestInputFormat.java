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

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.pivotal.pxf.gfxd.util.TestDataHelper;

/**
 * @author ashetkar
 */
public class TestInputFormat<K, V> extends FileInputFormat<K, V>  {

  private String regionName;
  
  public TestInputFormat(String regionName) {
    this.regionName = regionName;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    return new TestRecordReader(job, split);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return TestDataHelper.getSplits(this.regionName);
  }

}
