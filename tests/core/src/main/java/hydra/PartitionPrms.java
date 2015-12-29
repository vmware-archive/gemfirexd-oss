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
 * A class used to store keys for region configuration settings related to
 * partitioning.  The settings are used to create instances of {@link
 * PartitionDescription}, and can be referenced from a region configuration
 * via {@link RegionPrms#partitionName} if the configuration explicitly sets
 * the {@link RegionPrms#dataPolicy} to partition the region.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values, fields, and subfields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class PartitionPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the partition descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * Region name to be colocated with for each partition.  Can be specified as
   * {@link #NONE} (default).  Note that the product requires {@link
   * #partitionResolver} to also be set.
   */
  public static Long colocatedWith;

  /**
   * (String(s))
   * Name of logical fixed partition configuration for each region, as found
   * in {@link FixedPartitionPrms#names}.  Can be specified as {@link #NONE}
   * (default).
   */
  public static Long fixedPartitionName;

  /**
   * (int(s))
   * Local max memory for each partition, in megabytes.
   * <p>
   * The product default is computed at runtime.
   */
  public static Long localMaxMemory;

  /**
   * (Comma-separated String(s))
   * Class names of partition listeners for each partition description.
   * Can be specified as {@link #NONE} (default).
   *
   * Example: To use ClassA and ClassB for the first region, no listeners for
   * the second region, and ClassC for the third region, specify:
   *     <code>ClassA ClassB, none, ClassC</code>
   * <p>
   * Note that a new instance of each class is created each time a given
   * partition description is used to create a region.
   */
  public static Long partitionListeners;

  /**
   * (String(s))
   * Class name of partition resolver for each partition.  Can be specified as
   * {@link #NONE} (default).  See also {@link #colocatedWith}.
   * <p>
   * Hydra creates a singleton instance for each partition description.
   */
  public static Long partitionResolver;

  /**
   * (long(s))
   * Delay in milliseconds that existing members will wait before satisfying
   * redundancy after another member crashes.
   */
  public static Long recoveryDelay;

  /**
   * (int(s))
   * Redundant copies for each partition.
   */
  public static Long redundantCopies;

//  /**
//   * (int(s))
//   * Retry timeout for each partition, in milliseconds.
//   */
//  public static Long retryTimeout;

  /**
   * (long(s))
   * Delay in milliseconds that a new member will wait before trying to satisfy
   * redundancy of data hosted on other members.
   */
  public static Long startupRecoveryDelay;

  /**
   * (int(s))
   * Total num buckets for each partition.
   */
  public static Long totalNumBuckets;

  /**
   * (long(s))
   * Total max memory for each partition, in megabytes.
   */
  public static Long totalMaxMemory;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(PartitionPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("partitionprms", "info");
    dumpKeys();
  }
}
