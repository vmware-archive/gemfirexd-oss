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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil.HoplogOptimizedSplitter;
import com.pivotal.gemfirexd.internal.engine.Misc;

/**
 * This class extends from {@link InputFormat} to provide table data reading
 * ability.
 * 
 * @author ashvina
 */
public class RowInputFormat extends InputFormat<Key, Row> {
  /**
   * The home directory for gemfirexd data in HDFS. This should match the HOMEDIR
   * setting that was used with the CREATE HDFSSTORE in statement. 
   */
  public static final String HOME_DIR = "gfxd.input.homedir";
  /**
   * The name of the table to process, for example APP.CUSTOMERS. This should
   * match the table name used with the CREATE TABLE statement.
   */
  public static final String INPUT_TABLE = "gfxd.input.tablename";
  
  /**
   * If this filter is set, an event will be provided to map reduce job if it
   * happened after this timestamp in milliseconds
   */
  public static final String START_TIME_MILLIS = "gfxd.input.starttimemillis";
  /**
   * If this filter is set, an event will be provided to map reduce job if it
   * happened before this timestamp in milliseconds
   */
  public static final String END_TIME_MILLIS = "gfxd.input.endtimemillis";
  
  /**
   * In this mode checkpoint data is provided to job. Mapper, instead of getting
   * all the events that happened in the system, which also includes old values
   * of a key, gets the latest value known at the time of creating the
   * checkpoint only. Checkpoint may not include latest data present in the
   * system when the job is executed. In the checkpoint mode, time filters are
   * ignored
   */
  public static final String CHECKPOINT_MODE = "gfxd.input.checkpointmode";
  
  /**
   * A valid property starting with this prefix will be used to tune input
   * format's behavior.
   */
  public static final String PROPERTY_PREFIX = "gfxd.input.property.";

  protected Configuration conf;
  private final Logger logger;

  public RowInputFormat() {
    logger = LoggerFactory.getLogger(RowInputFormat.class);
  }
  
  @Override
  public RecordReader<Key, Row> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    RowRecordReader reader = new RowRecordReader();
    reader.initialize(split, context);
    return reader;
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException,
      InterruptedException {
    this.conf = job.getConfiguration();

    Collection<FileStatus> hoplogs = getHoplogs(conf);
    return createSplits(hoplogs);
  }

  /**
   * Identifies filters provided in the job configuration and creates a list of
   * sorted hoplogs satisfying the filters. If there are no sorted hoplogs,
   * checks if sequential hoplogs are present and executes the same process for
   * sequential hoplogs
   * 
   * @return list of hoplogs
   * @throws IOException
   */
  public Collection<FileStatus> getHoplogs(Configuration job)
      throws IOException {
    String name = job.get(INPUT_TABLE);
    //Get the region name from the table name
    String regionName = getRegionName(name);
    String regionFolder = HdfsRegionManager.getRegionFolder(regionName);
    logger.info("InputFormat for " + name + " is reading " + regionFolder);
    if (name == null || name.trim().isEmpty()) {
      // incomplete job configuration, region name must be provided
      logger.info("Input table name is not correctly configured");
      return new ArrayList<FileStatus>();
    }

    String home = job.get(HOME_DIR, HDFSStore.DEFAULT_HOME_DIR);
    Path regionPath = new Path(home + "/" + regionFolder);
    FileSystem fs = regionPath.getFileSystem(job);

    long start = job.getLong(START_TIME_MILLIS, 0l);
    long end = job.getLong(END_TIME_MILLIS, 0l);
    boolean checkpoint = job.getBoolean(CHECKPOINT_MODE, true);
    
    return HoplogUtil.filterHoplogs(fs, regionPath, start, end, checkpoint);
  }
  
  /**
   * Convert a gemfirexd table name to the region name
   */
  private static String getRegionName(String name) {
    name = getFullyQualifiedTableName(name);
    return Misc.getRegionPath(name);
  }

  public static String getFullyQualifiedTableName(String name) {
    //TODO - see if there is a gfxd utility that does this correctly
    if(!name.contains(".")) {
      name = "APP." + name;
    }
    return name;
  }

  public List<InputSplit> createSplits(Collection<FileStatus> hoplogs) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (hoplogs == null || hoplogs.isEmpty()) {
      return splits;
    }
    
    HoplogOptimizedSplitter splitter = new HoplogOptimizedSplitter(hoplogs);
    return splitter.getOptimizedSplits(conf);
  }
}
