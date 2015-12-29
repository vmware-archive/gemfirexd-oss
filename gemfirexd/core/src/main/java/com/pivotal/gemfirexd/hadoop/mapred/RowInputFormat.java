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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil.HoplogOptimizedSplitter;

/**
 * This class extends from {@link InputFormat} to provide table data reading
 * ability.
 * 
 * @author ashvina
 */
public class RowInputFormat implements InputFormat<Key, Row> {
  // this class reuses code in the parent. Hence it is important to make sure
  // that the config strings are the same. It is important to define the name
  // here to avoid confusion in user's code as the parent and child classes have
  // the same name.
  /**
   * The home directory for gemfirexd data in HDFS. This should match the HOMEDIR
   * setting that was used with the CREATE HDFSSTORE in statement. 
   */
  public static final String HOME_DIR = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.HOME_DIR;
  /**
   * The name of the table to process, for example APP.CUSTOMERS. This should
   * match the table name used with the CREATE TABLE statement.
   */
  public static final String INPUT_TABLE = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.INPUT_TABLE;
  
  /**
   * If this filter is set, an event will be provided to map reduce job if it
   * happened after this timestamp in milliseconds
   */
  public static final String START_TIME_MILLIS = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.START_TIME_MILLIS;
  /**
   * If this filter is set, an event will be provided to map reduce job if it
   * happened before this timestamp in milliseconds
   */
  public static final String END_TIME_MILLIS = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.END_TIME_MILLIS;
  
  /**
   * In this mode checkpoint data is provided to job. Mapper, instead of getting
   * all the events that happened in the system, which also includes old values
   * of a key, gets the latest value known at the time of creating the
   * checkpoint only. Checkpoint may not include latest data present in the
   * system when the job is executed. In the checkpoint mode, time filters are
   * ignored
   */
  public static final String CHECKPOINT_MODE = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.CHECKPOINT_MODE;
  
  /**
   * A valid property starting with this prefix will be used to tune input
   * format's behavior.
   */
  public static final String PROPERTY_PREFIX = com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat.PROPERTY_PREFIX;
  
  protected final Logger logger;

  protected Configuration conf;

  public RowInputFormat() {
    this.logger = LoggerFactory.getLogger(RowInputFormat.class);
  }

  @Override
  public RecordReader<Key, Row> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    if (logger.isInfoEnabled()) {
      StringBuffer sb = new StringBuffer();
      Path[] splitpaths = getSplitPaths(split);
      for (Path path : splitpaths) {
        sb.append(", ").append(path);
      }
      logger.info("Creating record reader for " + sb.toString());
    }
    MapRedRowRecordReader reader = getRecordReader();
    reader.initialize(split, job);
    return reader;
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    this.conf = job;

    Collection<FileStatus> hoplogs = new com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat()
        .getHoplogs(job);
    InputSplit[] splits = createSplits(job, hoplogs);
    
    return splits;
  }

  private InputSplit[] createSplits(JobConf job, Collection<FileStatus> hoplogs)
      throws IOException {
    if (hoplogs == null || hoplogs.isEmpty()) {
      return new InputSplit[0];
    }
    
    HoplogOptimizedSplitter splitter = new HoplogOptimizedSplitter(hoplogs);
    List<org.apache.hadoop.mapreduce.InputSplit> mr2Splits = splitter.getOptimizedSplits(conf);
    InputSplit[] splits = new InputSplit[mr2Splits.size()];
    int i = 0;
    for (org.apache.hadoop.mapreduce.InputSplit inputSplit : mr2Splits) {
      org.apache.hadoop.mapreduce.lib.input.CombineFileSplit mr2Spit;
      mr2Spit = (org.apache.hadoop.mapreduce.lib.input.CombineFileSplit) inputSplit;
      
      CombineFileSplit split = new CombineFileSplit(job, mr2Spit.getPaths(),
          mr2Spit.getStartOffsets(), mr2Spit.getLengths(),
          mr2Spit.getLocations());
      splits[i] = getSplit(split);
      i++;
    }

    return splits;
  }
  
  protected InputSplit getSplit(CombineFileSplit split) {
      return split;
  }
  
  protected MapRedRowRecordReader getRecordReader() {
    return new MapRedRowRecordReader();
  }
  
  protected Path[] getSplitPaths(InputSplit split){
    CombineFileSplit combineSplit = (CombineFileSplit) split;
    return combineSplit.getPaths();
  }
}
