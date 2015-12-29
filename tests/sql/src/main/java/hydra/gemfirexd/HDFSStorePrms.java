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

package hydra.gemfirexd;

import hydra.BasePrms;

/**
 * A class used to store keys for HDFS store configuration settings.  The
 * settings are used to create instances of {@link hydra.gemfirexd.HDFSStoreDescription}.
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
   * (int(s))
   * Batch size per bucket for each HDFS event queue, in MB.
   */
  public static Long batchSize;

  /**
   * (int(s))
   * Batch time interval for each HDFS event queue, in milliseconds.
   */
  public static Long batchTimeInterval;

  /**
   * (float(s))
   * Block cache size for each HDFS store.
   */
  public static Long blockCacheSize;

  /**
   * (String(s))
   * HDFS client configuration file name for each HDFS store. Defaults to null.
   * <p>
   * The contents of this file depend on the which hadoop distribution is being
   * used. See {@link hadoopName}.
   * <p>
   * The value of this parameter is not versioned. If the path contains
   * <code>$JTESTS</code>, this is always expanded relative to the current test
   * tree.
   * <p>
   * For tests that use {@link FabricServerPrms} to configure fabric servers,
   * hydra uses the {@link hydra.HostDescription} of a representative server 
   * to expand variables such as <code>$JTESTS</code> and <code>$PWD</code>.
   * This supports cross-platform testing where, for example, a Windows client
   * uses {@link HDFSStoreHelper#getHDFSStoreDDL} and executes it on Linux
   * servers, where the path is set to <code>$JTESTS/mypkg/myfile.xml</code>.
   * All servers must have the same value for $JTESTS or hydra will complain.
   * Hydra restricts its checks to servers that host data. If the servers
   * handling network clients are different that the data hosts, then the
   * network servers should be run on the same platform as the servers.
   * <p>
   * For tests where clients and servers run on Windows, simply use a Window
   * path. For example, <code>$JTESTS\\mypkg\\myfile.xml</code>. Again, all
   * servers must have the same value for $JTESTS.
   * <p>
   * Tests not using hydra support for fabric servers expand the path
   * using the current JVM's environment. Such tests will not work with
   * clients and servers on different platforms.
   */
  public static Long clientConfigFile;

  /**
   * (String(s))
   * Compaction strategy for each HDFS store.
   */
  public static Long compactionStrategy;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * for each HDFS event queue, as found in {@link
   * hydra.gemfirexd.DiskStorePrms#names}.
   * This is a required parameter if {@link #queuePersistent} is true.
   */
  public static Long diskStoreName;

  /**
   * (boolean(s))
   * Disk synchronous for each HDFS event queue.
   */
  public static Long diskSynchronous;

  /**
   * (int(s))
   * Dispatcher threads per table for each HDFS store.
   */
  public static Long dispatcherThreads;

  /**
   * (String(s))
   * Name of the Hadoop cluster used for each HDFS store, as found in {@link
   * hydra.HadoopPrms#names}. This is a required parameter.
   */
  public static Long hadoopName;

  /**
   * (String(s))
   * Home directory for regions using each HDFS store. Defaults to the name of
   * the test directory.
   */
  public static Long homeDir;

  /**
   * (boolean(s))
   * Major compaction for each HDFS store.
   */
  public static Long majorCompact;

  /**
   * (int(s))
   * Major compaction interval for each HDFS store, in minutes.
   */
  public static Long majorCompactionInterval;

  /**
   * (int(s))
   * Major compaction threads for each HDFS store.
   */
  public static Long majorCompactionThreads;

  /**
   * (int(s))
   * Max input file count for compaction for each HDFS store.
   */
  public static Long maxInputFileCount;

  /**
   * (int(s))
   * Max input file size for compaction for each HDFS store, in MB.
   */
  public static Long maxInputFileSize;

  /**
   * (int(s))
   * Maximum memory for each HDFS event queue, in MB.
   */
  public static Long maxQueueMemory;

  /**
   * (int(s))
   * Max file size for each write-only HDFS store, in MB.
   */
  public static Long maxWriteOnlyFileSize;

  /**
   * (int(s))
   * Min input file count for compaction for each HDFS store.
   */
  public static Long minInputFileCount;

  /**
   * (boolean(s))
   * minor compaction for each HDFS store.
   */
  public static Long minorCompact;

  /**
   * (int(s))
   * Max threads for minor compaction for each HDFS store.
   */
  public static Long minorCompactionThreads;

  /**
   * (int(s))
   * Controls when expired hoplog files are deleted for each HDFS store,
   * in minutes.
   */
  public static Long purgeInterval;

  /**
   * (boolean(s))
   * Persistence for each HDFS event queue. When true, requires {@link
   * #diskStoreName} to be set as well.
   */
  public static Long queuePersistent;

  /**
   * (int(s))
   * File rollover interval for each write-only HDFS store, in seconds.
   */
  public static Long writeOnlyFileRolloverInterval;
}
