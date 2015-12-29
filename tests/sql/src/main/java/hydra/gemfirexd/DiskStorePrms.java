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
 * A class used to store keys for disk store configuration settings.  The
 * settings are used to create instances of {@link DiskStoreDescription}.
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
public class DiskStorePrms extends BasePrms {

  static {
    setValues(DiskStorePrms.class); // initialize parameters
  }

  /**
   * (String(s))
   * Logical names of the disk store descriptions and actual names of the disk
   * stores.  Each name must be unique.  Defaults to null.  Not for use with
   * oneof, range, or robing.
   */
  public static Long names;

  /**
   * (boolean(s))
   * Allow force compaction for each disk store.
   */
  public static Long allowForceCompaction;

  /**
   * (boolean(s))
   * Auto compact for each disk store.
   */
  public static Long autoCompact;

  /**
   * (int(s))
   * Compaction threshold for each disk store.
   */
  public static Long compactionThreshold;

  /**
   * (Comma-separated lists of String(s))
   * Names of directories in the sys-disk-dir for each disk store, as found in
   * {@link FabricServerDescription#getSysDiskDir}. Defaults to "data".
   */
  public static Long dirNames;

  /**
   * (int(s))
   * Maximum of amount of space, in megabytes, to use for each directory
   * defined in {@link #dirNames}. Defaults to unlimited.
   */
  public static Long maxDirSize;

  /**
   * (long(s))
   * Max oplog size for each disk store, in megabytes.
   */
  public static Long maxLogSize;

  /**
   * (int(s))
   * Queue size for each disk store.
   */
  public static Long queueSize;

  /**
   * (long(s))
   * Time interval for each disk store, in milliseconds.
   */
  public static Long timeInterval;

  /**
   * (int(s))
   * Write buffer size for each disk store, in bytes.
   */
  public static Long writeBufferSize;
}
