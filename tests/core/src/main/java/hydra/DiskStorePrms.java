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
   * (Comma-separated String plus String(s) list)
   * Absolute base paths for disk directories for each disk store, where each
   * entry consists of a physical host name and list of base paths.  Defaults
   * to null, which uses the same resource directory path configured in {@link
   * HostPrms}.
   * <p>
   * Example:
   *   <code>
   *   hydra.DiskStorePrms-names = store1 store2;
   *   hydra.DiskStorePrms-diskDirNum = 3;
   *   hydra.DiskStorePrms-diskDirBases =
   *     // store1
   *     wheat  /export/wheat1/users/me/scratch
   *            /export/wheat2/users/me/scratch
   *     millet /export/millet1/users/me/scratch
   *            /export/millet2/users/me/scratch
   *            /export/millet3/users/me/scratch
   *     ,
   *     // store2
   *     default
   *     ;
   *   </code>
   * A region using store1 on host wheat will spread its {@link #diskDirNum}
   * disk directories round robin onto the wheat file systems (wheat1, wheat2,
   * wheat1), while a region using store1 on host millet will use the millet
   * file systems (millet1, millet2, millet3).  A region using store2 will
   * use the same path as the system directory.
   * <p>
   * To move remote disk directories back to the master directory after a test
   * run, set <code>-DmoveRemoteDirs=true</code> in your batterytest command.
   * <p>
   * See also {@link #diskDirBaseMapFileName}.
   */
  public static Long diskDirBases;

  /**
   * (String(s))
   * Absolute name of a file containing a mapping of physical host names
   * to lists of absolute base paths to use for each data store.  Overrides
   * {@link #diskDirBases}.  Defaults to null (no map file is used).
   * <p>
   * Example:
   *   <code>
   *   hydra.DiskStorePrms-names = store1 store2;
   *   hydra.DiskStorePrms-diskDirNum = 3;
   *   hydra.DiskStorePrms-diskDirBaseMapFileName =
   *                       /home/me/bin/diskmap.txt
   *                       default;
   *   </code>
   * where <code>diskmap.txt</code> contains:
   *   <code>
   *     wheat  /export/wheat1/users/me/scratch
   *            /export/wheat2/users/me/scratch
   *     millet /export/millet1/users/me/scratch
   *            /export/millet2/users/me/scratch
   *            /export/millet3/users/me/scratch
   *   </code>
   * A region using store1 on host wheat will spread its {@link #diskDirNum}
   * disk directories round robin onto the wheat file systems (wheat1, wheat2,
   * wheat1), while a region using store1 on host millet will use the millet
   * file systems (millet1, millet2, millet3).  A process using store2 will
   * use the same path as the system directory.
   */
  public static Long diskDirBaseMapFileName;

  /**
   * (int(s))
   * Number of disk directories to use for each data store.  Defaults to 1.
   * <p>
   * The directory names are autogenerated at runtime, numbered consecutively,
   * using the base path(s) determined by {@link #diskDirBases} and {@link
   * #diskDirBaseMapFileName}.  If there is more than one base path available,
   * the disk directories are assigned to them in a round robin fashion.
   */
  public static Long diskDirNum;

  /**
   * (Comma-separated int(s))
   * Sizes of disk directories to use for each data store, in megabytes.
   * If more than {@link #diskDirNum} sizes are given, the extra sizes are
   * ignored.
   */
  public static Long diskDirSizes;

  /**
   * (long(s))
   * Max oplog size for each disk store, in megabytes.
   */
  public static Long maxOplogSize;

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
