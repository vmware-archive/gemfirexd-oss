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

package hydra;

/**
 * A class used to store keys for HDFS store configuration settings.  The
 * settings are used to create instances of {@link HDFSStoreDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values, fields, and subfields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values, fields, and subfields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 * <p>
 * Subfields are order-dependent, as stated in the javadocs for parameters that
 * use them.
 */
public class HDFSStorePrms extends BasePrms {

  static {
    setValues(HDFSStorePrms.class); // initialize parameters
  }

  /**
   * (String(s))
   * Logical names of the HDFS store descriptions and actual names of the HDFS
   * stores.  Each name must be unique.  Defaults to null.  Not for use with
   * oneof, range, or robing.
   */
  public static Long names;

  /**
   * (boolean(s))
   * Auto minor compaction for each HDFS store.
   */
  public static Long autoCompaction;

  /**
   * (boolean(s))
   * Auto major compaction for each HDFS store.
   */
  public static Long autoMajorCompaction;

  /**
   * (int(s))
   * Batch size (in MB) per bucket for each HDFS event queue.
   */
  public static Long batchSizeMB;

  /**
   * (int(s))
   * Batch time interval for each HDFS event queue.
   */
  public static Long batchTimeInterval;

  /**
   * (float(s))
   * Block cache size for each HDFS store.
   */
  public static Long blockCacheSize;

  /**
   * (String(s))
   * Compaction strategy for each HDFS store.
   */
  public static Long compactionStrategy;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * for each HDFS event queue, as found in {@link DiskStorePrms#names}.
   * This is a required parameter if {@link #persistent} is true.
   */
  public static Long diskStoreName;

  /**
   * (boolean(s))
   * Disk synchronous for each HDFS event queue.
   */
  public static Long diskSynchronous;

  /**
   * (int(s))
   * File rollover interval for each write-only HDFS store, in seconds.
   */
  public static Long fileRolloverInterval;

  /**
   * (String(s))
   * Name of the Hadoop cluster used for each HDFS store, as found in {@link
   * HadoopPrms#names}. This is a required parameter.
   */
  public static Long hadoopName;

  /**
   * (String(s))
   * HDFS client configuration file name for each HDFS store. Defaults to null.
   * Note that the contents of this file depend on the which hadoop
   * distribution is being used. See {@link hadoopName}.
   */
  public static Long hdfsClientConfigFile;

  /**
   * (String(s))
   * Home directory for regions using each HDFS store. Defaults to the name of
   * the test directory.
   */
  public static Long homeDir;

  /**
   * (int(s))
   * Major compaction interval minutes for each HDFS store.
   */
  public static Long majorCompactionIntervalMins;

  /**
   * (int(s))
   * Major compaction max threads for each HDFS store.
   */
  public static Long majorCompactionMaxThreads;

  /**
   * (int(s))
   * Max file size for each write-only HDFS store, in MB.
   */
  public static Long maxFileSize;

  /**
   * (int(s))
   * Max input file count for compaction for each HDFS store.
   */
  public static Long maxInputFileCount;

  /**
   * (int(s))
   * Max input file size MB for compaction for each HDFS store.
   */
  public static Long maxInputFileSizeMB;

  /**
   * (int(s))
   * Max threads for minor compaction for each HDFS store.
   */
  public static Long maxThreads;

  /**
   * (int(s))
   * Maximum memory (in MB) for each HDFS event queue.
   */
  public static Long maximumQueueMemory;

  /**
   * (int(s))
   * Min input file count for compaction for each HDFS store.
   */
  public static Long minInputFileCount;

  /**
   * (int(s))
   * Old files cleanup interval minutes for compaction for each HDFS store.
   */
  public static Long oldFilesCleanupIntervalMins;

  /**
   * (boolean(s))
   * Persistence for each HDFS event queue. When true, requires {@link
   * #diskStoreName} to be set as well.
   */
  public static Long persistent;
}
