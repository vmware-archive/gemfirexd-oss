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

import java.util.*;
import com.gemstone.gemfire.cache.Cache;

/**
 *
 * A class used to store keys for GemFire configuration settings.  The settings
 * are used to create instances of {@link hydra.GemFireDescription}, which are
 * used to configure and manage both gemfire systems and distributed systems.
 * <p>
 * The number of instances is gated by {@link #names}.  The remaining parameters
 * have the indicated default values.  If fewer non-default values than names
 * are given for these parameters, the remaining instances will use the last
 * value in the list.  See $JTESTS/hydra/hydra.txt for more details.
 *
 */
public class GemFirePrms extends BasePrms {

    public static final String GEMFIRE_NAME_PROPERTY = "gemfireName";
    public static final String DISTRIBUTED_SYSTEM_NAME_PROPERTY = "distributedSystemName";
    public static final String DEFAULT_DISTRIBUTED_SYSTEM_NAME = "ds";
    /** Name for loner distributed systems. */
    public static final String LONER = "loner";
    public static final String NONE = "none";

    /**
     *  (String(s))
     *  Logical names of the GemFire descriptions.  Each name must be unique.
     *  Defaults to null.  See the include files in $JTESTS/hydraconfig for
     *  common configurations.
     */
    public static Long names;

    /**
     *  (String(s))
     *  Used to group nodes into distributed systems.  Defaults to {@link
     *  #DEFAULT_DISTRIBUTED_SYSTEM_NAME}.
     *  <p>
     *  To create a loner distributed system, use {@link #LONER}, which is
     *  reserved for this purpose (and is case-insensitive).
     */
    public static Long distributedSystem;

    /**
     * (Comma-separated list of String(s))
     * Remote distributed system names to use when connecting as a locator
     * with {@link DistributedSystemHelper} in a WAN configuration.  Defaults
     * to {@link #NONE}.  This is used to set the <code>remote-locators</code>
     * property to the locators that have been created for each remote
     * distributed system with {@link DistributedSystemHelper#createLocator}.
     * <p>
     * Suitable test configuration functions to use with the WAN topology
     * include files in $JTESTS/hydraconfig are:
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
     *  (String)
     *  Gives the extra VM arguments passed to all master-managed locators.
     *  Ignored if {@link Prms#manageLocatorAgents} is false.
     *  Defaults to null.
     */
    public static Long extraLocatorVMArgs;
    protected static String getExtraLocatorVMArgs(int i) {
      Long key = extraLocatorVMArgs;
      String args = null;
      Vector val = tab().vecAtWild( extraLocatorVMArgs, i, null );
      if ( val != null ) {
        args = "";
        for ( Iterator it = val.iterator(); it.hasNext(); ) {
          try {
            String arg = (String) it.next();
            if ( arg.equals( "=" ) ) {
              throw new HydraConfigException( "Malformed value in " + nameForKey(key) + ", use quotes to include '='" );
            } else {
              args += " " + arg;
            }
          } catch( ClassCastException e ) {
        	  e.printStackTrace();
            throw new HydraConfigException( "Malformed value in " + nameForKey(key) + ", use single string or list of values" );
          }
        }
      }
      // Use IPv6 addresses if asked
      if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
        if (args == null) args = "";
        args += " -D" + Prms.USE_IPV6_PROPERTY + "=true";
      }
      return args;
    }

    /**
     *  (String(s))
     *  Extra classpath for master-managed locators, if used.
     *  Ignored if {@link Prms#manageLocatorAgents} is false.
     *  Default is the corresponding
     *  entry in {@link hydra.HostPrms#testDirs} for the target host O/S, and
     *  lib/gemfire.jar from the corresponding entry in
     *  {@link hydra.HostPrms#gemfireHomes}, if it is defined.  The extra
     *  classpaths, if any, are prepended to the defaults.  All occurrences of
     *  $JTESTS and $GEMFIRE in a path are expanded relative to -DJTESTS and
     *  -Dgemfire.home on the target host.
     *  Gives the extra VM arguments passed to all master-managed locators.
     */
    public static Long extraLocatorClassPath;

    /**
     *  (String(s))
     *  Logical names of the host descriptions to use when creating gemfire
     *  systems.  Defaults to null.
     *
     *  @see HostPrms#names
     *  @see HostDescription
     */
    public static Long hostNames;

    /**
     *  (boolean(s))
     *  Tells hydra whether to stop system resources at the end of the test.
     *  Defaults to true.  The resources will not be stopped if master detects a
     *  hung client or system process.
     */
    public static Long stopSystemsAfterTest;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_ACK_WAIT_THRESHOLD
     */
    public static Long ackWaitThreshold;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_LOG_LEVEL
     */
    public static Long logLevel;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_LOG_FILE_SIZE_LIMIT
     */
    public static Long logFileSizeLimit;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_LOG_DISK_SPACE_LIMIT
     */
    public static Long logDiskSpaceLimit;

    /**
     *  Whether to gather statistics.  Defaults to true.
     */
    public static Long statisticSamplingEnabled;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_STATISTIC_SAMPLE_RATE
     */
    public static Long statisticSampleRate;

    /**
     * @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_ENABLE_TIME_STATISTICS
     */
    public static Long enableTimeStatistics;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_ARCHIVE_FILE_SIZE_LIMIT
     */
    public static Long archiveFileSizeLimit;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_ARCHIVE_DISK_SPACE_LIMIT
     */
    public static Long archiveDiskSpaceLimit;

     /**
     *  Whether to enable multicast for the distributed system. Defaults to false.
     */
    public static Long enableMcast;

    /**
     *  Whether to use Locator(s) for the distributed system. Defaults to true.
     */
    public static Long useLocator;

    /**
     *  Whether to turn on peer locator in locator(s). Defaults to true.
     *  Master-managed locators are always peer locators and ignore this
     *  parameter.
     */
    public static Long isPeerLocator;

    /**
     *  Whether to turn on server locator in locator(s). Defaults to true.
     */
    public static Long isServerLocator;

     /**
      * Whether to allow multicast when IPv6 is used. Defaults to false.
      * <p>
      * IMPORTANT: This works around issues in our network that cause
      * disruptive spikes.  To test multicast with IPv6, first get approval
      * from I.S. and run a short-lived test with their oversight.
      */
    public static Long allowMcastWithIPv6;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_ADDRESS
     */
    public static Long mcastAddress;
    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_PORT
     */

    public static Long mcastPort;

    /**
     * See <code>mcast-ttl</code> in {@link
     * com.gemstone.gemfire.distributed.DistributedSystem}.  Defaults to 0.
     */
    public static Long mcastTtl;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_SEND_BUFFER_SIZE
     */
    public static Long mcastSendBufferSize;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_RECV_BUFFER_SIZE
     */
    public static Long mcastRecvBufferSize;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_FLOW_CONTROL
     */
    public static Long mcastFlowControlByteAllowance;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_FLOW_CONTROL
     */
    public static Long mcastFlowControlRechargeThreshold;
    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MCAST_FLOW_CONTROL
     */
    public static Long mcastFlowControlRechargeBlockMs;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_UDP_FRAGMENT_SIZE
     */
    public static Long udpFragmentSize;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_UDP_SEND_BUFFER__SIZE
     */
    public static Long udpSendBufferSize;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_UDP_RECV_BUFFER__SIZE
     */
    public static Long udpRecvBufferSize;

    /**
     * @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_TCP_PORT
     */
    public static Long tcpPort;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_DISABLE_TCP
     */
    public static Long disableTcp;

    /**
     * @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MEMBERSHIP_PORT_RANGE
     *
     * For example:
     *
     *   hydra.GemFirePrms-membershipPortRange = 60000-61000;
     */
    public static Long membershipPortRange;

    /**
     *  @see com.gemstone.gemfire.distributed.internal.DistributionConfig#DEFAULT_MEMBER_TIMEOUT
     */
    public static Long memberTimeout;

  /**
   *  (boolean)
   *  Should gemfire conserveSockets?  Defaults to "true", the minimal number
   *  of sockets will be used when connecting to the distributed system.
   *  This conserves resources usage but can cause performance to suffer.
   *  If "false", then every application thread that sends
   *  will own its own sockets and have exclusive access to them.  The
   *  length of time a thread can have exclusive access to a socket can
   *  be configured with "socketLeaseTime".
   */
  public static Long conserveSockets;

  /** 
   *  (Long)
   *  The number of ms a thread can keep exclusive access to a socket
   *  that it is not actively using.  Once a thread loses its lease to
   *  a socket it will need to re-acquire a socket the next time it
   *  sends a message.  A value of zero causes the socket leases to
   *  never expire.  Default = 15000.  Allowed values 0 ... 600000.
   */
  public static Long socketLeaseTime;

  /**
   *  (Long)
   *  The size of each socket buffer, in bytes.  Smaller buffers conserve
   *  memory.  Larger buffers can improve performance; in particular if
   *  large messages are being sent.  Default = 32768.  Allowed values = 
   *  128 ... 20000000.
   */
  public static Long socketBufferSize;

  // -------------- Asynchronous Messaging Properties ------------------

  /**
   *  (int)
   *  The async-distribution-timeout: the time in ms to wait on socketWrite
   *  before queuing begins.
   *  Default value = 0 (no queuing).  Allowed values 0 ... 60000
   */
  public static Long asyncDistributionTimeout;

  /**
   *  (int)
   *  The async-queue-timeout: the time in ms to queue before declaring 
   *  a subscriber (slow) and triggering a High Priority disconnect to
   *  that slow subscriber.
   *  Default = 60000.  Allowed values = 0 ... 60000.
   */
   public static Long asyncQueueTimeout;

  /**
   *  (int)
   *  The async-max-queue-size: value (in MB?) of the maximum queue
   *  size supported.  Once this max size is reached, the slow receiver
   *  will be sent a High Priority disconnect and the queue will be cleared
   *  Default = 8.  Allowed values = 0 ... 1024.
   */
   public static Long asyncMaxQueueSize;

  /**
   *  (String(s))
   *  Roles to use when creating gemfiresystems.  Defaults to null.
   */
  public static Long roles;
  
  public static Long maxWaitTimeForReconnect;
    
  public static Long maxNumReconnectTries;

  /** 
   *  (boolean)
   *  Whether to configure as a durableClient. Defaults to false. 
   *  <p>
   *  If true, the durable-client-id will be set to the logical VM name and
   *  {@link #durableClientTimeout} will be set.  If false, values for
   *  {@link #durableClientTimeout} will be ignored.  
   *  <p>
   *  If enabled, clients must invoke {@link com.gemstone.gemfire.cache.Cache#readyForEvents} for the 
   *  client to receive any updates from the server.
   */
  public static Long enableDurableClient;

  /** 
   *  (int)
   * The number of seconds a disconnected durable client's queue is kept alive (and updates accumulated) by
   * the server.  Ignored if {@link #enableDurableClient} is false.  Default = 300.
   */
  public static Long durableClientTimeout;

  /**
   * (String(s))
   * Whether to conflate events for client queues.  Intended for use by bridge
   * clients (edges).  Valid values are "server" (use the server setting),
   * "true" (conflate), "false" (do not conflate).
   */
  public static Long conflateEvents;

  /**
   * (String(s))
   * Name of logical security configuration, as found in {@link
   * SecurityPrms#names}.  Can be specified as {@link #NONE} (default).
   * <p>
   * See also {@link #sslName}.
   */
  public static Long securityName;

//------------------------------------------------------------------------------
// Partition redundancy
//------------------------------------------------------------------------------

  /**
   * (Boolean(s))
   * Whether to enforce unique hosts for this description.  Defaults to false.
   */
  public static Long enforceUniqueHost;

  /**
   * (String(s))
   * The redundancy zone for this description.  Defaults to null.
   */
  public static Long redundancyZone;

//------------------------------------------------------------------------------
// Split brain
//------------------------------------------------------------------------------

  /**
   * (int(s))
   * The ack severe alert threshold, in seconds, for this description.
   */
  public static Long ackSevereAlertThreshold;

  /**
   * (boolean(s))
   * Whether to enable network partition detection for this description.
   */
  public static Long enableNetworkPartitionDetection;
  
  /**
   * (boolean(s))
   * Whether automatic reconnect after a forced disconnect is disabled.
   */
  public static Long disableAutoReconnect;

//------------------------------------------------------------------------------

  /**
   * (String(s))
   * Name of logical SSL configuration, as found in {@link SSLPrms#names}.
   * Can be specified as {@link #NONE} (default).
   * <p>
   * See also {@link #securityName}.
   */
  public static Long sslName;

//------------------------------------------------------------------------------
// jmx
//------------------------------------------------------------------------------

  /**
   * (boolean(s))
   * Willingness to be a JMX manager. If this is false, all other JMX manager
   * parameters are disregarded by the product. The product defaults to true
   * for locators and false otherwise.
   */
  public static Long jmxManager;

  /**
   * (String(s))
   * Path to JMX manager access file.
   */
  public static Long jmxManagerAccessFile;

  /**
   * (boolean(s))
   * Whether to configure an autogenerated port for the JMX manager http
   * server to use so that the GemFire Pulse application can connect to the
   * manager on the port. Defaults to false, which sets the port to 0 and
   * disables the http server. Otherwise generates an endpoint in the {@link
   * JMXManagerBlackboard}. See also {@link #jmxManagerPort}.
   */
  public static Long jmxManagerHttpPort;

  /**
   * (String(s))
   * Path to JMX manager password file.
   */
  public static Long jmxManagerPasswordFile;

  /**
   * (boolean(s))
   * Whether to configure an autogenerated port for the JMX manager to use
   * so that remote tools can connect to the manager on the port. Defaults to
   * false, which sets the port to 0. Otherwise generates an endpoint in the
   * {@link JMXManagerBlackboard}. See also {@link #jmxManagerHttpPort}.
   */
  public static Long jmxManagerPort;

  /**
   * (boolean(s))
   * Whether the JMX manager should use SSL. When true, requires specifying
   * {@link #jmxManagerPort} and {@link #sslName}. Defaults to false.
   */
  public static Long jmxManagerSSL;

  /**
   * (boolean(s))
   * Whether to start the JMX manager on cache creation. Defaults to false.
   */
  public static Long jmxManagerStart;

//------------------------------------------------------------------------------
// Delta propagation
//------------------------------------------------------------------------------

  /**
   * (boolean(s))
   * Whether to enable delta propagation.
   */
  public static Long deltaPropagation;
  
//------------------------------------------------------------------------------
//off heap memory
//------------------------------------------------------------------------------

  /**
   * (String(s))
   * The total size of off-heap memory in the form <n>[g|m]. 
   * <n> is the size. [g|m] indicates whether the size should be interpreted as 
   * gigabytes or megabytes.
   */
  public static Long offHeapMemorySize;

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

    static {
        setValues( GemFirePrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "gemfireprms", "info" );
        dumpKeys();
    }
}
