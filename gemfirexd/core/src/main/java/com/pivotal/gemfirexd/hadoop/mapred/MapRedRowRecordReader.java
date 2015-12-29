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
package com.pivotal.gemfirexd.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HDFSSplitIterator;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowRecordReader;

/**
 * Wrapper around {@link RowRecordReader} to make the reader compliant with
 * mapred.* api, a.k.a. old map reduce api
 * 
 * @author ashvina
 */
public class MapRedRowRecordReader extends RowRecordReader implements
    RecordReader<Key, Row> {
  
  Row row;

  void initialize(InputSplit split, Configuration job) throws IOException {
    Path[] path = getSplitPaths(split);
    long[] start = getStartOffsets(split);
    long[] len = getLengths(split);

    FileSystem fs = getSplitPaths(split)[0].getFileSystem(job);

    long startTS = job.getLong(RowInputFormat.START_TIME_MILLIS, 0l);
    long endTS = job.getLong(RowInputFormat.END_TIME_MILLIS, 0l);
    this.splitIterator = HDFSSplitIterator.newInstance(fs, path, start, len, startTS, endTS);

    instantiateGfxdLoner(job);
  }
  
  protected Path[] getSplitPaths(InputSplit split){
    CombineFileSplit combineSplit = (CombineFileSplit) split;
    return combineSplit.getPaths();
  }
  
  protected long[] getStartOffsets(InputSplit split){
    CombineFileSplit combineSplit = (CombineFileSplit) split;
    return combineSplit.getStartOffsets();
  }
  
  protected long[] getLengths(InputSplit split){
    CombineFileSplit combineSplit = (CombineFileSplit) split;
    return combineSplit.getLengths();
  }
  
  @Override
  public final Key createKey() {
    return new Key();
  }

  @Override
  public final Row createValue() {
    this.row = new Row(rs);
    return row;
  }

  @Override
  public final boolean next(Key key, Row value) throws IOException {
    if (nextRow()) {
      key.setKey(super.getRowKeyBytes());
      // The result set's cursor has already been advanced. So nothing needed
      // here as of now, till we start serializing result set
      return true;
    }
    else {
      key.setKey(null);
      return false;
    }
  }

  @Override
  public long getPos() throws IOException {
    // there is no efficient way to find the position of key in hoplog file.
    return 0;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public float getProgress() throws IOException {
    return super.getResultSetProgress();
  }
}
