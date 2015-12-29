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
 * fixed partitioning.  The settings are used to create instances of {@link
 * Fixed PartitionDescription}, and can be referenced from a partition
 * configuration via {@link PartitionPrms#fixedPartitionName}.
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
 * Values, fields, and subfields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class FixedPartitionPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the fixed partition descriptions.  Each name must be
   * unique.  Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (int(s))
   * Number of datastores that will create regions using each fixed partition
   * description.  This is a required parameter.
   */
  public static Long datastores;

  /**
   * (Comma-separated String pair(s))
   * Algorithm used to map primaries and secondaries to nodes for fixed
   * partitions.  An algorithm consists of a classname followed by a method
   * name.
   * <p>
   * The method must have signature <code>public static
   * List<FixedPartitionAttributes></code> and take three arguments: a
   * <code>String</code> with the region name, the logical <code>
   * FixedPartitionDescription</code>, and an <code>int</code> giving the
   * number of redundant copies.  It must return the list of fixed partition
   * attributes for the calling hydra client JVM.
   * <p>
   * Algorithms can use instances of a hydra blackboard such as {@link
   * FixedPartitionBlackboard} to coordinate the mapping across
   * multiple hydra client JVMs.  It is up to the user to ensure that all
   * fixed partitions are legally assigned, including redundant copies.
   * <p>
   * If an algorithm intends to support multiple regions being created from the
   * same logical region description, it can use the region name argument to
   * distinguish invocations of the algorithm on behalf of different regions.
   * If an algorithm intends to support creating the same region in multiple
   * WAN sites, it can use the logical distributed system name obtained through
   * {@link DistributedSystemHelper} to distinguish invocations of the
   * algorithm on behalf of different WAN sites.
   * <p>
   * Defaults to {@link FixedPartitionHelper#assignRoundRobin}, which assigns
   * primaries and secondaries in a round robin fashion, while ensuring that
   * no node contains multiple <code>FixedPartitionAttributes</code> for the
   * same partition.
   */
  public static Long mappingAlgorithm;

  /**
   * (Comma-separated String(s))
   * Names of fixed partitions for each fixed partition description.  This
   * is a required parameter.
   *
   * EXAMPLE: To configure a logical partition configuration named "quarters"
   * with fixed partitions representing 4 yearly quarters with the default of
   * 1 bucket per quarter, and another one for "halves" with 5 and 10 buckets
   * each, with each configuration mapping partitions to 2 datastores, specify:
   *
   *     hydra.FixedPartitionPrms-names            = quarters     halves;
   *     hydra.FixedPartitionPrms-partitionNames   = Q1 Q2 Q3 Q4, H1 H2;
   *     hydra.FixedPartitionPrms-partitionBuckets = default,     5  10;
   *     hydra.FixedPartitionPrms-datastores       = 2;
   */
  public static Long partitionNames;

  /**
   * (Comma-separated String(s))
   * Number of buckets in the fixed partitions for each fixed partition
   * description.  Defaults to 1 bucket per partition.
   *
   * EXAMPLE: To configure a logical fixed partition configuration "quarters"
   * with partitions representing 4 yearly quarters with 10 buckets for Q1,
   * 12 buckets for Q2, and 42 buckets for Q3 and Q4, specify:
   *
   *     hydra.PartitionPrms-names                 = datastore accessor;
   *     hydra.PartitionPrms-fixedPartitionNames   = quarters  none;
   *
   *     hydra.FixedPartitionPrms-names            = quarters;
   *     hydra.FixedPartitionPrms-partitionNames   = Q1 Q2 Q3 Q4;
   *     hydra.FixedPartitionPrms-partitionBuckets = 10 12 42;
   */
  public static Long partitionBuckets;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(FixedPartitionPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("fixedpartitionprms", "info");
    dumpKeys();
  }
}
