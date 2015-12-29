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
 * A class used to store keys for fabric server configuration settings.
 * The settings are used to create instances of {@link FabricServerDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class FabricServerPrms extends BasePrms {

  static {
    setValues(FabricServerPrms.class);
  }

  public static final String DEFAULT_DISTRIBUTED_SYSTEM_NAME = "ds";

  /**
   * (String(s))
   * Logical names of the fabric server descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;
  
  /**
   * (int(s))
   * The "ack-severe-alert-threshold" property, in seconds.
   */
  public static Long ackSevereAlertThreshold;

  /**
   * (int(s))
   * The "ack-wait-threshold" property, in seconds.
   */
  public static Long ackWaitThreshold;

  /**
   * (int(s))
   * The "archive-disk-space-limit" property, in megabytes.
   */
  public static Long archiveDiskSpaceLimit;

  /**
   * (int(s))
   * The "archive-file-size-limit" property, in megabytes.
   */
  public static Long archiveFileSizeLimit;

  /**
   * (int(s))
   * The "async-distribution-timeout" property, in milliseconds.
   */
  public static Long asyncDistributionTimeout;

  /**
   * (int(s))
   * The "async-max-queue-size" property, in megabytes.
   */
  public static Long asyncMaxQueueSize;

  /**
   * (int(s))
   * The "async-queue-timeout" property, in milliseconds.
   */
  public static Long asyncQueueTimeout;

  /**
   * (boolean(s))
   * Whether automatic reconnect after a forced disconnect is disabled.
   */
  public static Long disableAutoReconnect;

  /**
   * (Comma-separated Lists of String(s))
   * Names of logical hydra client configurations, as found in {@link
   * hydra.ClientPrms#names}.  No client name can be listed more than once.
   * Can be specified as {@link #NONE} (default).
   * <p>
   * This parameter is used to wire each logical fabric server description
   * to specific hydra clients.  For example, it is used in the topology
   * include files in $JTESTS/hydraconfig/gemfirexd for p2p, hct, and wan,
   * which is then used by certain methods in {@link FabricServerHelper}
   * to complete the wiring.
   */
  public static Long clientNames;

  /**
   * (boolean(s))
   * The "conserve-sockets" property.
   */
  public static Long conserveSockets;

  /**
   * (boolean(s))
   * The "disable-tcp" property.
   */
  public static Long disableTcp;

  /**
   * (String(s))
   * Logical name of the distributed system associated with each of the
   * {@link names}.  Defaults to {@link #DEFAULT_DISTRIBUTED_SYSTEM_NAME}.
   * <p>
   * To create a loner distributed system, use {@link hydra.gemfirexd.LonerPrms}.
   * Loners can be used to connect thin clients to a distributed system
   * for statistics collection.
   */
  public static Long distributedSystem;

  /**
   * (boolean(s))
   * The "enable-network-partition-detection" property.
   */
  public static Long enableNetworkPartitionDetection;

  /**
   * (boolean(s))
   * The "enable-stats" property.  Defaults to false.
   * This turns on GemFireXD statement-level statistics globally
   * using a system property.  To turn them on per-connection, use the
   * connection property enable-stats in the client (note: peers only).
   */
  public static Long enableStatsGlobally;

  /**
   * (boolean(s))
   * The "enable-time-statistics" property.  Defaults to true, which overrides
   * the product default.  This turns on GemFire-level time statistics.
   */
  public static Long enableTimeStatistics;

  /**
   * (boolean(s))
   * The "enable-timestats" property.  Defaults to false.
   * This turns on GemFireXD statement-level time statistics globally
   * using a system property.  To turn them on per-connection, use the
   * connection property enable-timestats in the client (note: peers only).
   */
  public static Long enableTimeStatsGlobally;

  /**
   * (Boolean(s))
   * The enforce-unique-host property.
   */
  public static Long enforceUniqueHost;

  /**
   * (String(s))
   * Name of logical fabric security configuration, as found in {@link
   * FabricSecurityPrms#names}.  Can be specified as {@link #NONE} (default).
   */
  public static Long fabricSecurityName;

  /**
   * (boolean(s))
   * The "host-data" property.  Note that the product overrides this setting
   * for stand-alone locators, which never host data.
   */
  public static Long hostData;

  /**
   * (boolean(s))
   * The "lock-memory" property. Locks both heap and off-heap memory.
   * <p>
   * When set true, required jna-3.5.1.jar in the product library to be added to
   * the classpath.
   * <p>
   * The heap memory locked is the amount the JVM is started with. Set -Xms the
   * same as -Xmx to lock all heap.
   */
  public static Long lockMemory;

  /**
   * (int(s))
   * The "log-disk-space-limit" property, in megabytes.
   */
  public static Long logDiskSpaceLimit;

  /**
   * (int(s))
   * The "log-file-size-limit" property, in megabytes.
   */
  public static Long logFileSizeLimit;

  /**
   * (String(s))
   * The "log-level" property.
   */
  public static Long logLevel;

  /**
   * (int(s))
   * The "max-num-reconnect-tries" property.
   */
  public static Long maxNumReconnectTries;

  /**
   * (int(s))
   * The "max-wait-time-reconnect" property, in milliseconds.
   */
  public static Long maxWaitTimeForReconnect;

  /**
   * (String(s))
   * The "mcast-address" property.  Defaults to a random address chosen
   * by hydra and based on the IP protocol such that all descriptions in
   * the same distributed system use the same address.
   */
  public static Long mcastAddress;

  /**
   * (boolean(s))
   * Whether multicast distribution is enabled.  Defaults to false.
   * <p>
   * All logical fabric server descriptions in the same {@link
   * #distributedSystem} must use the same value for this parameter.
   * If set true, hydra autogenerates a multicast port if one is not
   * specified in {@link #mcastPort}.
   */
  public static Long mcastDistributionEnabled;

  /**
   * (int(s))
   * The byte allowance portion of the "mcast-flow-control"
   * property, which is used by both multicast and UDP.
   * Used with {@link #mcastFlowControlRechargeBlockMs} and
   * {@link #mcastFlowControlRechargeThreshold}.
   */
  public static Long mcastFlowControlByteAllowance;

  /**
   * (int(s))
   * The recharge block milliseconds portion of the "mcast-flow-control"
   * property, which is used by both multicast and UDP.
   * Used with {@link #mcastFlowControlByteAllowance} and
   * {@link #mcastFlowControlRechargeThreshold}.
   */
  public static Long mcastFlowControlRechargeBlockMs;

  /**
   * (float(s))
   * The recharge threshold portion of the "mcast-flow-control"
   * property, which is used by both multicast and UDP.
   * Used with {@link #mcastFlowControlByteAllowance} and
   * {@link #mcastFlowControlRechargeBlockMs}.
   */
  public static Long mcastFlowControlRechargeThreshold;

  /**
   * (int(s))
   * The "mcast-port" property.  Defaults to a random available port chosen
   * by hydra such that all descriptions in the same distributed system use
   * the same port.  It is set to "0" when {@link #mcastDistributionEnabled}
   * is false.
   */
  public static Long mcastPort;

  /**
   * (int(s))
   * The "mcast-recv-buffer-size" property, in bytes.
   */
  public static Long mcastRecvBufferSize;

  /**
   * (int(s))
   * The "mcast-send-buffer-size" property, in bytes.
   */
  public static Long mcastSendBufferSize;

  /**
   * (int(s))
   * The "mcast-ttl" property.  Defaults to "0", which overrides the product
   * default.
   */
  public static Long mcastTtl;

  /**
   * (Pairs of hyphen-separated int(s))
   * The "membership-port-range" property.
   * <p>
   * For example:
   *    hydra.gemfirexd.FabricServerPrms-membershipPortRange = 60000-61000;
   */
  public static Long membershipPortRange;

  /**
   * (int(s))
   * The "member-timeout" property, in milliseconds.
   */
  public static Long memberTimeout;

  /**
   * (String(s))
   * The total size of off-heap memory in the form <n>[g|m]. 
   * <n> is the size. [g|m] indicates whether the size should be interpreted as 
   * gigabytes or megabytes.
   */
  public static Long offHeapMemorySize;

  /**
   * (boolean(s))
   * The "persist-dd" property.  Defaults to true.
   * <p>
   * If true, hydra will automatically set the "sys-disk-dir" property to a
   * directory named for the logical VM ID, such as vm_3_client2_disk, with
   * the same path as the system disk directory, which defaults to the same
   * path as the system directory.
   */
  public static Long persistDD;
  public static boolean persistDD() {
    Long key = persistDD;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean(s))
   * Whether to make indexes persistent.  Defaults to true.
   */
  public static Long persistIndexes;

  /**
   * (boolean(s))
   * Whether the test intends to use persistence for queues.  Defaults to false.
   * <p>
   * If true, hydra will automatically set the "sys-disk-dir" property to a
   * directory named for the logical VM ID, such as vm_3_client2_disk, with
   * the same path as the system disk directory, which defaults to the same
   * path as the system directory.
   */
  public static Long persistQueues;

  /**
   * (boolean(s))
   * Whether the test intends to use persistence for tables.  Defaults to false.
   * <p>
   * If true, hydra will automatically set the "sys-disk-dir" property to a
   * directory named for the logical VM ID, such as vm_3_client2_disk, with
   * the same path as the system disk directory, which defaults to the same
   * path as the system directory.
   */
  public static Long persistTables;

  /**
   * (boolean(s))
   * Whether the server should initiate rebalance on startup. Defaults to false.
   */
  public static Long rebalance;

  /**
   * (String(s))
   * The redundancy-zone property. Defaults to null.
   */
  public static Long redundancyZone;
  
  /**
   * (Comma-separated list of String(s))
   * Remote distributed system names to use when connecting as a locator with
   * {@link FabricServerHelper} in a WAN configuration.  Defaults to {@link
   * #NONE}.  This is used to set the <code>remote-locators</code> property to
   * the locators that have been created for each remote distributed system
   * with {@link FabricServerHelper#createLocator}.
   * <p>
   * Suitable test configuration functions to use with the WAN topology include
   * files in $JTESTS/hydraconfig/gemfirexd are:
   * <pre>
   * <code>
   *   // for a ring-connected locator topology
   *   fcn "hydra.TestConfigFcns.generateNamesRepeatedlyShift
   *        (\"ds_\", ${wanSites}, ${locatorHostsPerSite}, false, true)" ncf,
   *   none;
   *
   *   // for a fully-connected locator topology
   *   fcn "hydra.TestConfigFcns.generateNameListsRepeatedlyShift
   *        (\"ds_\", ${wanSites}, ${locatorHostsPerSite})" ncf,
   *   none;
   * </code>
   * </pre>
   */
  public static Long remoteDistributedSystems;

  /**
   * (boolean(s))
   * Whether to save the sys-disk-dirs in place regardless of whether
   * <code>moveRemoteDirs</code> is set true in BatteryTest by omitting them
   * from the <code>movedirs.sh</code> script. This also circumvents <code>
   * hydra.Prms-removeDiskFilesAfterTest</code>. Defaults to false.
   * <p>
   * This is typically used with {@link #sysDiskDirBaseMapFileName} to
   * generate disk files that can be used in another test.
   */
  public static Long saveSysDiskDir;

  /**
   * (Comma-separated Lists of String(s))
   * The "server-groups" property.  Each logical fabric server description
   * can belong to multiple server groups.  Can be specified as {@link #NONE}.
   */
  public static Long serverGroups;

  /**
   * (int(s))
   * The "socket-buffer-size" property, in bytes.
   */
  public static Long socketBufferSize;

  /**
   * (int(s))
   * The "socket-lease-time" property, in milliseconds.
   */
  public static Long socketLeaseTime;

  /**
   * (int(s))
   * The "statistic-sample-rate" property, in milliseconds.
   */
  public static Long statisticSampleRate;

  /**
   * (boolean(s))
   * The "statistic-sampling-enabled" property.  Defaults to true, which
   * overrides the product default.
   */
  public static Long statisticSamplingEnabled;

  /**
   * (String(s))
   * Absolute name of a file containing a mapping of physical host names
   * to lists of absolute base paths to use for each sys-disk-dir. Defaults
   * to null (no map file is used), which uses the same path as the system
   * directory.
   * <p>
   * Example:
   *   <code>
   *   hydra.gemfirexd.FabricServerPrms-names = locatorConfig serverConfig;
   *   hydra.gemfirexd.FabricServerPrms-sysDiskDirBaseMapFileName =
   *                                  none $PWD/../diskmap.txt;
   *   </code>
   * where <code>diskmap.txt</code> contains:
   *   <code>
   *     millet /export/millet1/users/$USER/scratchdisk
   *            /export/millet2/users/$USER/scratchdisk
   *            /export/millet3/users/$USER/scratchdisk
   *     wheat  /export/wheat1/users/$USER/scratchdisk
   *            /export/wheat2/users/$USER/scratchdisk
   *   </code>
   * <p>
   * In the example, fabric servers using locatorConfig will not configure
   * disk directory bases (they will default to the system directory), while
   * fabric servers using serverConfig and running on millet or wheat will
   * use one of the configured base directories. These are assigned round
   * robin by their logical hydra client VMID.
   * <p>
   * For example, if vm_3_server* and vm_4_server* use serverConfig and
   * both run on host millet, the first server will use the path for millet1
   * and the second will use the path for millet2.
   * <p>
   * So, for example, if the test requires that each fabric server on a given
   * host use a different disk, the map file must at least as many paths (to
   * different disks) for that host as there will be servers running on that
   * host. In addition, the logical hydra client VMIDs need to be consecutive
   * to ensure each maps to a unique base path.
   */
  public static Long sysDiskDirBaseMapFileName;

  /**
   * (boolean(s))
   * Whether to expect the sys-disk-dirs to already exist. Defaults to false.
   * <p>
   * This is typically used with {@link #sysDiskDirBaseMapFileName} to use
   * disk files generated elsewhere.
   */
  public static Long useExistingSysDiskDir;

  /**
   * (boolean(s))
   * Whether to omit the test result directory name from the sys-disk-dir
   * path. Defaults to false.
   * <p>
   * This is typically used with {@link #sysDiskDirBaseMapFileName},
   * {@link #saveSysDiskDirs} and/or {@link #useExistingSysDiskDirs} to
   * share disk files between tests.
   */
  public static Long useGenericSysDiskDir;

  /**
   * (boolean(s))
   * The "table-default-partitioned" property. Defaults to true.
   */
  public static Long tableDefaultPartitioned;

  /**
   * (int(s))
   * The "tcp-port" property.  Defaults to an available port selected by
   * the operating system.
   */
  public static Long tcpPort;

  /**
   * (int(s))
   * The "udp-fragment-size" property, in bytes.
   */
  public static Long udpFragmentSize;

  /**
   * (int(s))
   * The "udp-recv-buffer-size" property, in bytes.
   */
  public static Long udpRecvBufferSize;

  /**
   * (int(s))
   * The "udp-send-buffer-size" property, in bytes.
   */
  public static Long udpSendBufferSize;
}
