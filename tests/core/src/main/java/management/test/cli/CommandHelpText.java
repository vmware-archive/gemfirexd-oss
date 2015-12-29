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
package management.test.cli;

import java.util.HashMap;
import java.util.Map;

/**
 * The myTest class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class CommandHelpText {

  protected static String alterDiskStoreCmdHelpText =
    "NAME\n"
    + "      alter disk-store\n"
    + "  IS AVAILABLE\n"
    + "      true\n"
    + "  SYNOPSIS\n"
    + "      Alter some options for a region or remove a region in an offline disk store.\n"
    + "  SYNTAX\n"
    + "      alter disk-store --name=value --region=value --disk-dirs=value(,value)* [--lru-algorthm=value]\n"
    + "      [--lru-action=value] [--lru-limit=value] [--concurrency-level=value] [--initial-capacity=value]\n"
    + "      [--load-factor=value] [--enable-statistics=value] [--remove(=value)?]\n"
    + "  PARAMETERS\n"
    + "      name\n"
    + "          Name of the disk store whose contents will be altered.\n"
    + "          Required: true\n"
    + "      region\n"
    + "          Name/Path of the region in the disk store to alter.\n"
    + "          Required: true\n"
    + "      disk-dirs\n"
    + "          Directories where data for the disk store was previously written.\n"
    + "          Required: true\n"
    + "      lru-algorthm\n"
    + "          Least recently used eviction algorithm.  Valid values are: none, lru-entry-count,\n"
    + "          lru-heap-percentage and lru-memory-size.\n"
    + "          Required: false\n"
    + "      lru-action\n"
    + "          Action to take when evicting entries from the region. Valid values are: none,\n"
    + "          overflow-to-disk and local-destroy.\n"
    + "          Required: false\n"
    + "      lru-limit\n"
    + "          Number of entries allowed in the region before eviction will occur.\n"
    + "          Required: false\n"
    + "      concurrency-level\n"
    + "          An estimate of the maximum number of application threads that will concurrently access a\n"
    + "          region entry at one time. This attribute does not apply to partitioned regions.\n"
    + "          Required: false\n"
    + "      initial-capacity\n"
    + "          Together with --load-factor, sets the parameters on the underlying\n"
    + "          java.util.ConcurrentHashMap used for storing region entries.\n"
    + "          Required: false\n"
    + "      load-factor\n"
    + "          Together with --initial-capacity, sets the parameters on the underlying\n"
    + "          java.util.ConcurrentHashMap used for storing region entries. This must be a floating point\n"
    + "          number between 0 and 1, inclusive.\n"
    + "          Required: false\n"
    + "      enable-statistics\n"
    + "          Whether to enable statistics. Valid values are: true and false.\n"
    + "          Required: false\n"
    + "      remove\n"
    + "          Whether to remove the region from the disk store.\n"
    + "          Required: false\n"
    + "          Default (if the parameter is specified without value): true\n"
    + "          Default (if the parameter is not specified): false";
  
  protected final static String alterRegionCmdHelpText =
      "NAME\n"
      + "    alter region\n"
      + "IS AVAILABLE\n"
      + "    true\n"
      +"SYNOPSIS\n"
      +"    Alter a region with the given path and configuration.\n"
      +"SYNTAX\n"
      +"    alter region --name=value [--group=value(,value)*] [--entry-idle-time-expiration(=value)?]\n"
      +"    [--entry-idle-time-expiration-action=value] [--entry-time-to-live-expiration(=value)?]\n"
      +"    [--entry-time-to-live-expiration-action=value] [--region-idle-time-expiration(=value)?]\n"
      +"    [--region-idle-time-expiration-action=value] [--region-time-to-live-expiration(=value)?]\n"
      +"    [--region-time-to-live-expiration-action=value] [--cache-listener=value(,value)*]\n"
      +"    [--cache-loader=value] [--cache-writer=value] [--async-event-queue-id=value(,value)*]\n"
      +"    [--gateway-sender-id=value(,value)*] [--enable-cloning(=value)?] [--eviction-max(=value)?]\n"
      +"PARAMETERS\n"
      +"    name\n"
      +"        Name/Path of the region to be altered.\n"
      +"        Required: true\n"
      +"    group\n"
      +"        Group(s) of members on which the region will be altered.\n"
      +"        Required: false\n"
      +"    entry-idle-time-expiration\n"
      +"        How long the region’s entries can remain in the cache without being accessed. The default\n"
      +"        is no expiration of this type.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): -1\n"
      +"    entry-idle-time-expiration-action\n"
      +"        Action to be taken on an entry that has exceeded the idle expiration.\n"
      +"        Required: false\n"
      +"    entry-time-to-live-expiration\n"
      +"        How long the region’s entries can remain in the cache without being accessed or updated.\n"
      +"        The default is no expiration of this type.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): -1\n"
      +"    entry-time-to-live-expiration-action\n"
      +"        Action to be taken on an entry that has exceeded the TTL expiration.\n"
      +"        Required: false\n"
      +"    region-idle-time-expiration\n"
      +"        How long the region can remain in the cache without being accessed. The default is no\n"
      +"        expiration of this type.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): -1\n"
      +"    region-idle-time-expiration-action\n"
      +"        Action to be taken on a region that has exceeded the idle expiration.\n"
      +"        Required: false\n"
      +"    region-time-to-live-expiration\n"
      +"        How long the region can remain in the cache without being accessed or updated. The default\n"
      +"        is no expiration of this type.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): -1\n"
      +"    region-time-to-live-expiration-action\n"
      +"        Action to be taken on a region that has exceeded the TTL expiration.\n"
      +"        Required: false\n"
      +"    cache-listener\n"
      +"        Fully qualified class name of a plug-in to be instantiated for receiving after-event\n"
      +"        notification of changes to the region and its entries. Any number of cache listeners can be\n"
      +"        configured.\n"
      +"        Required: false\n"
      +"    cache-loader\n"
      +"        Fully qualified class name of a plug-in to be instantiated for receiving notification of\n"
      +"        cache misses in the region. At most, one cache loader can be defined in each member for the\n"
      +"        region. For distributed regions, a cache loader may be invoked remotely from other members\n"
      +"        that have the region defined.\n"
      +"        Required: false\n"
      +"    cache-writer\n"
      +"        Fully qualified class name of a plug-in to be instantiated for receiving before-event\n"
      +"        notification of changes to the region and its entries. The plug-in may cancel the event. At\n"
      +"        most, one cache writer can be defined in each member for the region.\n"
      +"        Required: false\n"
      +"    async-event-queue-id\n"
      +"        IDs of the Async Event Queues that will be used for write-behind operations.\n"
      +"        Required: false\n"
      +"    gateway-sender-id\n"
      +"        IDs of the Gateway Senders to which data will be routed.\n"
      +"        Required: false\n"
      +"    enable-cloning\n"
      +"        Determines how fromDelta applies deltas to the local cache for delta propagation. When\n"
      +"        true, the updates are applied to a clone of the value and then the clone is saved to the\n"
      +"        cache. When false, the value is modified in place in the cache.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): false\n"
      +"    eviction-max\n"
      +"        Maximum value for the Eviction Attributes which the Eviction Algorithm uses to determine\n"
      +"        when to perform its Eviction Action. The unit of the maximum value is determined by the\n"
      +"        Eviction Algorithm.\n"
      +"        Required: false\n"
      +"        Default (if the parameter is specified without value): 0";

  protected static String alterRuntimeCmdHelpText =
    "NAME\n"
    + "      alter runtime\n"
    + "  IS AVAILABLE\n"
    + "      true\n"
    + "  SYNOPSIS\n"
    + "      Alter a subset of member or members configuration properties while running.\n"
    + "  SYNTAX\n"
    + "      alter runtime [--member=value] [--group=value] [--archive-disk-space-limit=value]\n"
    + "      [--archive-file-size-limit=value] [--log-disk-space-limit=value] [--log-file-size-limit=value]\n"
    + "      [--log-level=value] [--statistic-archive-file=value] [--statistic-sample-rate=value]\n"
    + "      [--enable-statistics=value]\n"
    + "  PARAMETERS\n"
    + "      member\n"
    + "          Name/Id of the member in whose configuration will be altered.\n"
    + "          Required: false\n"
    + "      group\n"
    + "          Name/Id of the member in whose configuration will be altered.\n"
    + "          Required: false\n"
    + "      archive-disk-space-limit\n"
    + "          Archive disk space limit. Valid values are (in megabytes): 0 - 1000000.\n"
    + "          Required: false\n"
    + "      archive-file-size-limit\n"
    + "          Archive file size limit. Valid values are (in megabytes): 0 - 1000000.\n"
    + "          Required: false\n"
    + "      log-disk-space-limit\n"
    + "          Log disk space limit. Valid values are (in megabytes): 0 - 1000000.\n"
    + "          Required: false\n"
    + "      log-file-size-limit\n"
    + "          Log file size limit. Valid values are (in megabytes): 0 - 1000000.\n"
    + "          Required: false\n"
    + "      log-level\n"
    + "          Log level. Valid values are: none, error, info, config , fine, finer and finest.\n"
    + "          Required: false\n"
    + "      statistic-archive-file\n"
    + "          File to which the statistics will be written.\n"
    + "          Required: false\n"
    + "      statistic-sample-rate\n"
    + "          Statistic sampling rate. Valid values are (in milliseconds): 100 - 60000.\n"
    + "          Required: false\n"
    + "      enable-statistics\n"
    + "          Whether statistic sampling should be enabled. Valid values are: true and false.\n"
    + "          Required: false";

  protected final static String closeDurableCqCmdHelpText = 
     "NAME\n"
     +"    close durable-cq\n"
     +"IS AVAILABLE\n"
     +"    true\n"
     +"SYNOPSIS\n"
     +"    Closes the durable cq registered by the durable client and drain events held for the durable cq\n"
     +"    from the subscription queue.\n"
     +"SYNTAX\n"
     +"    close durable-cq --durable-client-id=value --durable-cq-name=value [--member=value]\n"
     +"    [--group=value]\n"
     +"PARAMETERS\n"
     +"    durable-client-id\n"
     +"        The id of the durable client\n"
     +"        Required: true\n"
     +"    durable-cq-name\n"
     +"        Name of the cq to be closed.\n"
     +"        Required: true\n"
     +"    member\n"
     +"        Name/Id of the member for which the durable client is registered and the cq to be closed.\n"
     +"        Required: false\n"
     +"    group\n"
     +"        Group of members for which the durable client is registered and the cq to be closed.\n"
     +"        Required: false";


  protected final static String closeDurableClientCmdHelpText = 
      "NAME\n"
      +"    close durable-client\n"
      +"IS AVAILABLE\n"
      +"    true\n"
      +"SYNOPSIS\n"
      +"    Attempts to close the durable client, the client must be disconnected.\n"
      +"SYNTAX\n"
      +"    close durable-client --durable-client-id=value [--member=value] [--group=value]\n"
      +"PARAMETERS\n"
      +"    durable-client-id\n"
      +"        The id used to identify the durable client.\n"
      +"        Required: true\n"
      +"    member\n"
      +"        Name/Id of the member for which the durable client is to be closed.\n"
      +"        Required: false\n"
      +"    group\n"
      +"        Group of members for which the subscription queue events are to be counted.\n"
      +"        Required: false";

  protected final static String createAsyncEventQueueCmdHelpText = 
        "NAME\n"
        +"    create async-event-queue\n"
        +"IS AVAILABLE\n"
        +"    true\n"
        +"SYNOPSIS\n"
        +"    Create Async Event Queue.\n"
        +"SYNTAX\n"
        +"    create async-event-queue --id=value --listener=value [--group=value] [--batch-size=value]\n"
        +"    [--persistent(=value)?] [--disk-store=value] [--max-queue-memory=value]\n"
        +"    [--listener-param=value(,value)*]\n"
        +"PARAMETERS\n"
        +"    id\n"
        +"        ID of the queue to be created.\n"
        +"        Required: true\n"
        +"    group\n"
        +"        Group(s) of members on which queue will be created. If no group is specified the queue will\n"
        +"        be created on all members.\n"
        +"        Required: false\n"
        +"    batch-size\n"
        +"        Maximum number of messages that a batch can contain.\n"
        +"        Required: false\n"
        +"        Default (if the parameter is not specified): 100\n"
        +"    persistent\n"
        +"        Whether events should be persisted to a disk store.\n"
        +"        Required: false\n"
        +"        Default (if the parameter is specified without value): true\n"
        +"        Default (if the parameter is not specified): false\n"
        +"    disk-store\n"
        +"        Disk store to be used by this queue.\n"
        +"        Required: false\n"
        +"    max-queue-memory\n"
        +"        Maximum amount of memory, in megabytes, that the queue can consume before overflowing to\n"
        +"        disk.\n"
        +"        Required: false\n"
        +"        Default (if the parameter is not specified): 100\n"
        +"    listener\n"
        +"        Fully qualified class name of the AsyncEventListener for this queue.\n"
        +"        Required: true\n"
        +"    listener-param\n"
        +"        Parameter name for the AsyncEventListener.  Optionally, parameter names may be followed by\n"
        +"        # and a value for the parameter.  Example: --listener-param=loadAll\n"
        +"        --listener-param=maxRead#1024\n"
        +"        Required: false";

  protected final static String createDiskStoreCmdHelpText = 
      "NAME\n"
     +"    create disk-store\n"
     +"IS AVAILABLE\n"
     +"    true\n"
     +"SYNOPSIS\n"
     +"    Create a disk store.\n"
     +"SYNTAX\n"
     +"    create disk-store --name=value --dir=value(,value)* [--allow-force-compaction(=value)?]\n"
     +"    [--auto-compact(=value)?] [--compaction-threshold=value] [--max-oplog-size=value]\n"
     +"    [--queue-size=value] [--time-interval=value] [--write-buffer-size=value]\n"
     +"    [--group=value(,value)*]\n"
     +"PARAMETERS\n"
     +"    name\n"
     +"        Name of the disk store to be created.\n"
     +"        Required: true\n"
     +"    allow-force-compaction\n"
     +"        Whether to allow manual compaction through the API or command-line tools.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is specified without value): true\n"
     +"        Default (if the parameter is not specified): false\n"
     +"    auto-compact\n"
     +"        Whether to automatically compact a file when it reaches the compaction-threshold.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is specified without value): true\n"
     +"        Default (if the parameter is not specified): true\n"
     +"    compaction-threshold\n"
     +"        Percentage of garbage allowed in the file before it is eligible for compaction.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is not specified): 50\n"
     +"    max-oplog-size\n"
     +"        The largest size, in megabytes, to allow an operation log to become before automatically\n"
     +"        rolling to a new file.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is not specified): 1024\n"
     +"    queue-size\n"
     +"        For asynchronous queueing. The maximum number of operations to allow into the write queue\n"
     +"        before automatically flushing the queue. The default of 0 indicates no limit.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is not specified): 0\n"
     +"    time-interval\n"
     +"        For asynchronous queueing. The number of milliseconds that can elapse before data is\n"
     +"        flushed to disk. Reaching this limit or the queue-size limit causes the queue to flush.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is not specified): 1000\n"
     +"    write-buffer-size\n"
     +"        Size of the buffer used to write to disk.\n"
     +"        Required: false\n"
     +"        Default (if the parameter is not specified): 32768\n"
     +"    dir\n"
     +"        Directories where the disk store files will be written.  Optionally, directory names may be\n"
     +"        followed by # and the maximum number of megabytes that the disk store can use in the\n"
     +"        directory.  Example: --dir=/data/ds1 --dir=/data/ds2#5000\n"
     +"        Required: true\n"
     +"    group\n"
     +"        Group(s) of members on which the disk store will be created. If no group is specified the\n"
     +"        disk store will be created on all members.\n"
     +"        Required: false";

  protected static String createIndexCmdHelpText =
    "NAME\n"
    + "    create index\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Create an index that can be used when executing queries.\n"
    + "SYNTAX\n"
    + "    create index --name=value --expression=value --region=value [--member=value] [--type=value]\n"
    + "    [--group=value]\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Name of the index to create.\n"
    + "        Required: true\n"
    + "    expression\n"
    + "        Field of the region values that are referenced by the index.\n"
    + "        Required: true\n"
    + "    region\n"
    + "        Name/Path of the region which corresponds to the \"from\" clause in a query.\n"
    + "        Required: true\n"
    + "    member\n"
    + "        Name/Id of the member in which the index will be created.\n"
    + "        Required: false\n"
    + "    type\n"
    + "        Type of the index. Valid values are: range, key and hash.\n"
    + "        Required: false\n"
    + "        Default (if the parameter is not specified): range\n"
    + "    group\n"
    + "        Group of members in which the index will be created.\n"
    + "        Required: false";

  protected static String describeConfigCmdHelpText =
    "NAME\n"
    + "    describe config\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display configuration details of a member or members.\n"
    + "SYNTAX\n"
    + "    describe config --member=value [--hide-defaults(=value)?]\n"
    + "PARAMETERS\n"
    + "    member\n"
    + "        Name/Id of the member whose configuration will be described.\n"
    + "        Required: true\n"
    + "    hide-defaults\n"
    + "        Whether to hide configuration information for properties with the default value.\n"
    + "        Required: false\n"
    + "        Default (if the parameter is specified without value): true\n"
    + "        Default (if the parameter is not specified): true";

  protected static String describeConnectionCmdHelpText =
    "NAME\n"
    + "    describe connection\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display information about the current connection.\n"
    + "SYNTAX\n"
    + "    describe connection";

  protected static String describeDiskStoreCmdHelpText =
    "NAME\n"
    + "    describe disk-store\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display information about a member's disk store.\n"
    + "SYNTAX\n"
    + "    describe disk-store --member=value --name=value\n"
    + "PARAMETERS\n"
    + "    member\n"
    + "        Name/Id of the member with the disk store to be described.\n"
    + "        Required: true\n"
    + "    name\n"
    + "        Name of the disk store to be described.\n"
    + "        Required: true";

  protected static String describeMemberCmdHelpText =
    "NAME\n"
    + "    describe member\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display information about a member, including name, id, groups, regions, etc.\n"
    + "SYNTAX\n"
    + "    describe member --name=value\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Display information about a member, including name, id, groups, regions, etc.\n"
    + "        Required: true";
  

  protected static String describeRegionCmdHelpText =
    "NAME\n"
    + "    describe region\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display the attributes and key information of a region.\n"
    + "SYNTAX\n"
    + "    describe region --name=value\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Name/Path of the region to be described.\n"
    + "        Required: true";
  
  protected static String destroyDiskStore =
    "NAME\n"
    + "    destroy disk-store\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Destroy a disk store, including deleting all files on disk used by the disk store. Data for closed regions previously using the disk store will be lost.\n"
    + "SYNTAX\n"
    + "    destroy disk-store --name=value [--group=value(,value)*]\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Name of the disk store that will be destroyed.\n"
    + "        Required: true\n"
    + "    group\n"
    + "        Group(s) of members on which the disk store will be destroyed. If no group is specified the disk store will be destroyed on all members.\n"
    + "        Required: false";
  
  protected static String destroyIndexCmdHelpText =
    "NAME\n"
    + "    destroy index\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Destroy/Remove the specified index.\n"
    + "SYNTAX\n"
    + "    destroy index [--name=value] [--region=value] [--member=value] [--group=value]\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Name of the index to remove.\n"
    + "        Required: false\n"
    + "    region\n"
    + "        Name/Path of the region from which the index will be removed.\n"
    + "        Required: false\n"
    + "    member\n"
    + "        Name/Id of the member from which the index will be removed.\n"
    + "        Required: false\n"
    + "    group\n"
    + "        Group of members from which the index will be removed.\n"
    + "        Required: false";

  protected static String destroyRegionCmdHelpText =
    "NAME\n"
    + "    destroy region\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Destroy/Remove a region.\n"
    + "SYNTAX\n"
    + "    destroy region --name=value\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Name/Path of the region to be removed.\n"
    + "        Required: true";

  protected static String encryptPasswordCmdHelpText =
    "NAME\n"
    + "    encrypt password\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Encrypt a password for use in data source configuration.\n"
    + "SYNTAX\n"
    + "    encrypt password --password=value\n"
    + "PARAMETERS\n"
    + "    password\n"
    + "        Password to be encrypted.\n"
    + "        Required: true\n";

  protected static String exportConfigCmdHelpText =
    "NAME\n"
    + "    export config\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Export configuration properties for a member or members.\n"
    + "SYNTAX\n"
    + "    export config [--member=value(,value)*] [--group=value(,value)*] [--dir=value]\n"
    + "PARAMETERS\n"
    + "    member\n"
    + "        Name/Id of the member(s) whose configuration will be exported.\n"
    + "        Required: false\n"
    + "    group\n"
    + "        Group(s) of members whose configuration will be exported.\n"
    + "        Required: false\n"
    + "    dir\n"
    + "        Directory to which the exported configuration files will be written.\n"
    + "        Required: false";

  protected static String exportStacksTraceCmdHelpText =
    "NAME\n"
    + "    export stack-traces\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Export the stack trace for a member or members.\n"
    + "SYNTAX\n"
    + "    export stack-traces --file=value [--member=value] [--group=value]\n"
    + "PARAMETERS\n"
    + "    member\n"
    + "        Export the stack trace for a member or members.\n"
    + "        Required: false\n"
    + "    group\n"
    + "        group\n"
    + "        Required: false\n"
    + "    file\n"
    + "        Name of the file to which the stack traces will be written.\n"
    + "        Required: true";

  protected static String getCmdHelpText =
    "NAME\n"
    + "    get\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display an entry in a region. If using a region whose key and value classes have been set, then\n"
    + "    specifying --key-class and --value-class is unnecessary.\n"
    + "SYNTAX\n"
    + "    get --key=value --region=value [--key-class=value] [--value-class=value]\n"
    + "PARAMETERS\n"
    + "    key\n"
    + "        String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and\n"
    + "        \"('id': 'l34s')\".\n"
    + "        Required: true\n"
    + "    region\n"
    + "        Region from which to get the entry.\n"
    + "        Required: true\n"
    + "    key-class\n"
    + "        Fully qualified class name of the key's type. The default is the key constraint for the\n"
    + "        current region or String.\n"
    + "        Required: false\n"
    + "    value-class\n"
    + "        Fully qualified class name of the value's type. The default is the value constraint for the\n"
    + "        current region or String.\n"
    + "        Required: false";

  protected final static String listAsyncEventQueueCmdHelpText = 
      "NAME\n"
      +"    list async-event-queues\n"
      +"IS AVAILABLE\n"
      +"    true\n"
      +"SYNOPSIS\n"
      +"    Display the Async Event Queues for all members.\n"
      +"SYNTAX\n"
      +"    list async-event-queues";

  protected static String listDiskStoresCmdHelpText =
    "NAME\n"
    + "    list disk-stores\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display disk stores for all members.\n"
    + "SYNTAX\n"
    + "    list disk-stores";
  
protected final static String listDurableCqsCmdHelpText = 
    "NAME\n"
    +"    list durable-cqs\n"
    +"IS AVAILABLE\n"
    +"    true\n"
    +"SYNOPSIS\n"
    +"    List durable client cqs associated with the specified durable client id.\n"
    +"SYNTAX\n"
    +"    list durable-cqs --durable-client-id=value [--member=value] [--group=value]\n"
    +"PARAMETERS\n"
    +"    durable-client-id\n"
    +"        The id used to identify the durable client\n"
    +"        Required: true\n"
    +"    member\n"
    +"        Name/Id of the member for which the durable client is registered and durable cqs will be\n"
    +"        displayed.\n"
    +"        Required: false\n"
    +"    group\n"
    +"        Group of members for which the durable client is registered and durable cqs will be\n"
    +"        displayed.\n"
    +"        Required: false";

  protected static String listFunctionsCmdHelpText =
    "NAME\n"
    + "    list functions\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display a list of registered functions. The default is to display functions for all members.\n"
    + "SYNTAX\n"
    + "    list functions [--matches=value] [--group=value(,value)*] [--member=value(,value)*]\n"
    + "PARAMETERS\n"
    + "    matches\n"
    + "        Pattern that the function ID must match in order to be included. Uses Java pattern matching\n"
    + "        rules, not UNIX. For example, to match any character any number of times use \".*\" instead\n"
    + "        of \"*\".\n"
    + "        Required: false\n"
    + "    group\n"
    + "        Group(s) of members for which functions will be displayed.\n"
    + "        Required: false\n"
    + "    member\n"
    + "        Name/Id of the member(s) for which functions will be displayed.\n"
    + "        Required: false";

  protected static String listIndexesCmdHelpText =
    "NAME\n"
    + "    list indexes\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display the list of indexes created for all members.\n"
    + "SYNTAX\n"
    + "    list indexes [--with-stats(=value)?]\n"
    + "PARAMETERS\n"
    + "    with-stats\n"
    + "        Whether statistics should also be displayed.\n"
    + "        Required: false\n"
    + "        Default (if the parameter is specified without value): true\n"
    + "        Default (if the parameter is not specified): false";

  protected static String listMembersCmdHelpText =
    "NAME\n"
    + "    list members\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display all or a subset of members.\n"
    + "SYNTAX\n"
    + "    list members [--group=value]\n"
    + "PARAMETERS\n"
    + "    group\n"
    + "        Group name for which members will be displayed.\n"
    + "        Required: false";

  protected static String listRegionsCmdHelpText =
    "NAME\n"
    + "    list regions\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display regions of a member or members.\n"
    + "SYNTAX\n"
    + "    list regions [--group=value] [--member=value]\n"
    + "PARAMETERS\n"
    + "    group\n"
    + "        Group of members for which regions will be displayed.\n"
    + "        Required: false\n"
    + "    member\n"
    + "        Name/Id of the member for which regions will be displayed.\n"
    + "        Required: false";

  protected static String locateEntryCmdHelpText =
    "NAME\n"
    + "    locate entry\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Identifies the location, including host, member and region, of entries that have the specified\n"
    + "    key.\n"
    + "SYNTAX\n"
    + "    locate entry --key=value --region=value [--key-class=value] [--value-class=value]\n"
    + "    [--recursive=value]\n"
    + "PARAMETERS\n"
    + "    key\n"
    + "        String or JSON text from which to create a key.  Examples include: \"James\", \"100L\" and\n"
    + "        \"('id': 'l34s')\".\n"
    + "        Required: true\n"
    + "    region\n"
    + "        Region in which to locate values.\n"
    + "        Required: true\n"
    + "    key-class\n"
    + "        Fully qualified class name of the key's type. The default is java.lang.String.\n"
    + "        Required: false\n"
    + "    value-class\n"
    + "        Fully qualified class name of the value's type. The default is java.lang.String.\n"
    + "        Required: false\n"
    + "    recursive\n"
    + "        Whether to traverse regions and subregions recursively.\n"
    + "        Required: false\n"
    + "        Default (if the parameter is not specified): false";

  protected static String putCmdHelpText =
    "NAME\n"
    + "    put\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Add/Update an entry in a region. If using a region whose key and value classes have been set,\n"
    + "    then specifying --key-class and --value-class is unnecessary.\n"
    + "SYNTAX\n"
    + "    put --key=value --value=value --region=value [--key-class=value] [--value-class=value]\n"
    + "    [--skip-if-exists=value]\n"
    + "PARAMETERS\n"
    + "    key\n"
    + "        String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and\n"
    + "        \"('id': 'l34s')\".\n"
    + "        Required: true\n"
    + "    value\n"
    + "        String or JSON text from which to create the value.  Examples include: \"manager\", \"100L\"\n"
    + "        and \"('value': 'widget')\".\n"
    + "        Required: true\n"
    + "    region\n"
    + "        Region into which the entry will be put.\n"
    + "        Required: true\n"
    + "    key-class\n"
    + "        Fully qualified class name of the key's type. The default is java.lang.String.\n"
    + "        Required: false\n"
    + "    value-class\n"
    + "        Fully qualified class name of the value's type. The default is java.lang.String.\n"
    + "        Required: false\n"
    + "    skip-if-exists\n"
    + "        Skip the put operation when an entry with the same key already exists. The default is to\n"
    + "        overwrite the entry (false).\n"
    + "        Required: false\n"
    + "        Default (if the parameter is not specified): false";

  protected static String removeCmdHelpText =
    "NAME\n"
    + "    remove\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Remove an entry from a region. If using a region whose key class has been set, then specifying\n"
    + "    --key-class is unnecessary.\n"
    + "SYNTAX\n"
    + "    remove --region=value [--key=value] [--all(=value)?] [--key-class=value]\n"
    + "PARAMETERS\n"
    + "    key\n"
    + "        String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and\n"
    + "        \"('id': 'l34s')\".\n"
    + "        Required: false\n"
    + "    region\n"
    + "        Region from which to remove the entry.\n"
    + "        Required: true\n"
    + "    all\n"
    + "        Clears the region by removing all entries. Partitioned region does not support remove-all\n"
    + "        Required: false\n"
    + "        Default (if the parameter is specified without value): true\n"
    + "        Default (if the parameter is not specified): false\n"
    + "    key-class\n"
    + "        Fully qualified class name of the key's type. The default is the key constraint for the\n"
    + "        current region or String.\n"
    + "        Required: false";

  protected static String showDeadLocksCmdHelpText =
    "NAME\n"
    + "    show dead-locks\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display any deadlocks in the GemFire distributed system.\n"
    + "SYNTAX\n"
    + "    show dead-locks --file=value\n"
    + "PARAMETERS\n"
    + "    file\n"
    + "        Name of the file to which dependencies between members will be written.\n"
    + "        Required: true";

  protected static String showMetricsCmdHelpText =
    "NAME\n"
    + "    show metrics\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display or export metrics for the entire distributed system, a member or a region.\n"
    + "SYNTAX\n"
    + "    show metrics [--member=value] [--region=value] [--file=value] [--port=value]\n"
    + "    [--categories=value(,value)*]\n"
    + "PARAMETERS\n"
    + "    member\n"
    + "        Name/Id of the member whose metrics will be displayed/exported.\n"
    + "        Required: false\n"
    + "    region\n"
    + "        Name/Path of the region whose metrics will be displayed/exported.\n"
    + "        Required: false\n"
    + "    file\n"
    + "        Name of the file to which metrics will be written.\n"
    + "        Required: false\n"
    + "    port\n"
    + "        Port number of the Cache Server whose metrics are to be displayed/exported. This can only\n"
    + "        be used along with the --member parameter.\n"
    + "        Required: false\n"
    + "    categories\n"
    + "        Categories available based upon the parameters specified are:\n"
    + "        - no parameters specified: cluster, cache, diskstore, query\n"
    + "        - region specified: cluster, region, partition, diskstore, callback, eviction\n"
    + "        - member specified: member, jvm, region, serialization, communication, function,\n"
    + "        transaction, diskstore, lock, eviction, distribution\n"
    + "        - member and region specified: region, partition, diskstore, callback, eviction\n"
    + "        Required: false";

  protected final static String showSubscriptionQueueSizeCmdHelpText = 
     "NAME\n"
     +"    show subscription-queue-size\n"
     +"IS AVAILABLE\n"
     +"    true\n"
     +"SYNOPSIS\n"
     +"    Shows the number of events in the subscription queue.  If a cq name is provided, counts the number of events\n"
     +"    in the subscription queue for the specified cq.\n"
     +"SYNTAX\n"
     +"    show subscription-queue-size --durable-client-Id=value [--durable-cq-name=value] [--member=value]\n"
     +"    [--group=value]\n"
     +"PARAMETERS\n"
     +"    durable-client-Id\n"
     +"        The id used to identify the durable client.\n"
     +"        Required: true\n"
     +"    durable-cq-name\n"
     +"        The name that identifies the cq.\n"
     +"        Required: false\n"
     +"    member\n"
     +"        Name/Id of the member for which the subscription events are to be counted.\n"
     +"        Required: false\n"
     +"    group\n"
     +"        Group of members for which the subscription queue events are to be counted.\n"
     +"        Required: false";
  
  protected static String statusLocatorCmdHelpText =
    "NAME\n"
    + "    status locator\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display the status of a Locator. Possible statuses are: started, online, offline or not\n"
    + "    responding.\n"
    + "SYNTAX\n"
    + "    status locator [--name=value] [--host=value] [--port=value] [--pid=value] [--dir=value]\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Member name or ID of the Locator in the GemFire cluster.\n"
    + "        Required: false\n"
    + "    host\n"
    + "        Hostname or IP address on which the Locator is running.\n"
    + "        Required: false\n"
    + "    port\n"
    + "        Port on which the Locator is listening. The default is 10334.\n"
    + "        Required: false\n"
    + "    pid\n"
    + "        Process ID (PID) of the running Locator.\n"
    + "        Required: false\n"
    + "    dir\n"
    + "        Working directory in which the Locator is running. The default is the current directory.\n"
    + "        Required: false";

  protected static String statusServerCmdHelpText =
    "NAME\n"
    + "    status server\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display the status of a GemFire Cache Server.\n"
    + "SYNTAX\n"
    + "    status server [--name=value] [--pid=value] [--dir=value]\n"
    + "PARAMETERS\n"
    + "    name\n"
    + "        Member name or ID of the Cache Server in the GemFire cluster.\n"
    + "        Required: false\n"
    + "    pid\n"
    + "        Process ID (PID) of the running GemFire Cache Server.\n"
    + "        Required: false\n"
    + "    dir\n"
    + "        Working directory in which the Cache Server is running. The default is the current\n"
    + "        directory.\n"
    + "        Required: false";

  protected static String versionCmdHelpText =
    "NAME\n"
    + "    version\n"
    + "IS AVAILABLE\n"
    + "    true\n"
    + "SYNOPSIS\n"
    + "    Display product version information.\n"
    + "SYNTAX\n"
    + "    version [--full(=value)?]\n"
    + "PARAMETERS\n"
    + "    full\n"
    + "        Whether to show the full version information.\n"
    + "        Required: false\n"
    + "        Default (if the parameter is specified without value): true\n"
    + "        Default (if the parameter is not specified): false";
  

  protected static final String[] expectedCommandsArr = new String[] {
    "alter disk-store", "alter region", "alter runtime", "backup disk-store", "close durable-client", "close durable-cq", "compact disk-store", "compact offline-disk-store", "connect",
    "create async-event-queue", "create disk-store", "create gateway-receiver", "create gateway-sender", "create index", "create region",
    "debug", "deploy", "describe config", "describe connection", "describe disk-store", "describe member", "describe offline-disk-store", "describe region",
    "destroy disk-store", "destroy function", "destroy index", "destroy region", "disconnect",
    "echo", "encrypt password", "execute function", "exit", "export config", "export data", "export logs", "export stack-traces", "gc", "get", "help", "hint", "history", "import data",
    "list async-event-queues", "list deployed", "list disk-stores", "list durable-cqs", "list functions", "list gateways", "list indexes", "list members", "list regions",
    "locate entry", "netstat", "pause gateway-sender", "put", "rebalance", "remove", "resume gateway-sender", "revoke missing-disk-store", "run",
    "query", "set variable", "sh", "show dead-locks", "show log", "show metrics", "show missing-disk-stores", "show subscription-queue-size", "shutdown", "sleep", "start data-browser", "start gateway-receiver", "start gateway-sender", "start pulse",
    "start jconsole", "start jvisualvm", "start locator", "start server", "start vsd", "status gateway-receiver", "status gateway-sender", "status locator", "status server", "stop gateway-receiver",
    "stop gateway-sender", "stop locator", "stop server", "undeploy", "upgrade offline-disk-store", "validate offline-disk-store", "version" };

  protected static Map<String, Boolean> BuildExpectedConnectedHelpAvailabilities() {
    Map<String, Boolean> expectedAvailabilities = new HashMap();

    for (String commandName: CommandHelpText.expectedCommandsArr) {
      expectedAvailabilities.put(commandName,  true);
    }
    return expectedAvailabilities;
  }

  protected static Map<String, Boolean> BuildExpectedDisconnectedHelpAvailabilities() {
    Map<String, Boolean> expectedAvailabilities = new HashMap();

    for (String commandName: CommandHelpText.expectedCommandsArr) {
      expectedAvailabilities.put(commandName,  true);
    }

    expectedAvailabilities.put("alter region", false);
    expectedAvailabilities.put("alter runtime", false);
    expectedAvailabilities.put("backup disk-store", false);
    expectedAvailabilities.put("close durable-client", false);
    expectedAvailabilities.put("close durable-cq", false);
    expectedAvailabilities.put("compact disk-store", false);
    expectedAvailabilities.put("create async-event-queue", false);
    expectedAvailabilities.put("create disk-store", false);
    expectedAvailabilities.put("create gateway-receiver", false);
    expectedAvailabilities.put("create gateway-sender", false);
    expectedAvailabilities.put("create index", false);
    expectedAvailabilities.put("create region", false);
    expectedAvailabilities.put("deploy", false);
    expectedAvailabilities.put("describe config", false);
    expectedAvailabilities.put("describe disk-store", false);
    expectedAvailabilities.put("describe member", false);
    expectedAvailabilities.put("describe region", false);
    expectedAvailabilities.put("destroy disk-store", false);
    expectedAvailabilities.put("destroy function", false);
    expectedAvailabilities.put("destroy index", false);
    expectedAvailabilities.put("destroy region", false);
    expectedAvailabilities.put("execute function", false);
    expectedAvailabilities.put("export config", false);
    expectedAvailabilities.put("export data", false);
    expectedAvailabilities.put("export logs", false);
    expectedAvailabilities.put("export stack-traces", false);
    expectedAvailabilities.put("gc", false);
    expectedAvailabilities.put("get", false);
    expectedAvailabilities.put("import data", false);
    expectedAvailabilities.put("list async-event-queues", false);
    expectedAvailabilities.put("list deployed", false);
    expectedAvailabilities.put("list disk-stores", false);
    expectedAvailabilities.put("list durable-cqs", false);
    expectedAvailabilities.put("list functions", false);
    expectedAvailabilities.put("list gateways", false);
    expectedAvailabilities.put("list indexes", false);
    expectedAvailabilities.put("list members", false);
    expectedAvailabilities.put("list regions", false);
    expectedAvailabilities.put("locate entry", false);
    expectedAvailabilities.put("netstat", false);
    expectedAvailabilities.put("pause gateway-sender", false);
    expectedAvailabilities.put("put", false);
    expectedAvailabilities.put("query", false);
    expectedAvailabilities.put("rebalance", false);
    expectedAvailabilities.put("remove", false);
    expectedAvailabilities.put("resume gateway-sender", false);
    expectedAvailabilities.put("revoke missing-disk-store", false);
    expectedAvailabilities.put("show dead-locks", false);
    expectedAvailabilities.put("show log", false);
    expectedAvailabilities.put("show metrics", false);
    expectedAvailabilities.put("show missing-disk-stores", false);
    expectedAvailabilities.put("show subscription-queue-size", false);
    expectedAvailabilities.put("shutdown", false);
    expectedAvailabilities.put("start gateway-receiver", false);
    expectedAvailabilities.put("start gateway-sender", false);
    expectedAvailabilities.put("status gateway-receiver", false);
    expectedAvailabilities.put("status gateway-sender", false);
    expectedAvailabilities.put("stop gateway-receiver", false);
    expectedAvailabilities.put("stop gateway-sender", false);
    expectedAvailabilities.put("undeploy", false);

    return expectedAvailabilities;
  }

  protected static Map<String, String> BuildExpectedHelpDescriptions() {
    Map<String, String> expectedDescriptions = new HashMap();

    expectedDescriptions.put("alter disk-store", "Alter some options for a region or remove a region in an offline disk store.");
    expectedDescriptions.put("alter region", "Alter a region with the given path and configuration.");
    expectedDescriptions.put("alter runtime", "Alter a subset of member or members configuration properties while running.");
    expectedDescriptions.put("backup disk-store", "Perform a backup on all members with persistent data. The target directory must exist on all members, but can be either local or shared. This command can safely be executed on active members and is strongly recommended over copying files via operating system commands.");
    expectedDescriptions.put("close durable-client", "Attempts to close the durable client, the client must be disconnected.");
    expectedDescriptions.put("close durable-cq", "Closes the durable cq registered by the durable client and drain events held for the durable cq from the subscription queue.");
    expectedDescriptions.put("compact disk-store", "Compact a disk store on all members with that disk store. This command uses the compaction threshold that each member has configured for its disk stores. The disk store must have \"allow-force-compaction\" set to true.");
    expectedDescriptions.put("compact offline-disk-store", "Compact an offline disk store. If the disk store is large, additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.");
    expectedDescriptions.put("connect", "Connect to a jmx-manager either directly or via a Locator. If connecting via a Locator, and a jmx-manager doesn't already exist, the Locator will start one.");
    expectedDescriptions.put("create async-event-queue", "Create Async Event Queue.");
    expectedDescriptions.put("create disk-store", "Create a disk store.");
    expectedDescriptions.put("create gateway-receiver", "Create the Gateway Receiver on a member or members.");
    expectedDescriptions.put("create gateway-sender", "Create the Gateway Sender on a member or members.");
    expectedDescriptions.put("create index", "Create an index that can be used when executing queries.");
    expectedDescriptions.put("create region", "Create a region with the given path and configuration. Specifying a --key-constraint and --value-constraint makes object type information available during querying and indexing.");
    expectedDescriptions.put("debug", "Enable/Disable debugging output in GFSH.");
    expectedDescriptions.put("deploy", "Deploy JARs to a member or members.  Only one of either --jar or --dir may be specified.");
    expectedDescriptions.put("describe config", "Display configuration details of a member or members.");
    expectedDescriptions.put("describe connection", "Display information about the current connection.");
    expectedDescriptions.put("describe disk-store", "Display information about a member's disk store.");
    expectedDescriptions.put("describe member", "Display information about a member, including name, id, groups, regions, etc.");
    expectedDescriptions.put("describe offline-disk-store", "Display information about an offline disk store.");
    expectedDescriptions.put("describe region", "Display the attributes and key information of a region.");
    expectedDescriptions.put("destroy disk-store", "Destroy a disk store, including deleting all files on disk used by the disk store. Data for closed regions previously using the disk store will be lost.");
    expectedDescriptions.put("destroy function", "Destroy/Unregister a function. The default is for the function to be unregistered from all members.");
    expectedDescriptions.put("destroy index", "Destroy/Remove the specified index.");
    expectedDescriptions.put("destroy region", "Destroy/Remove a region.");
    expectedDescriptions.put("disconnect", "Close the current connection, if one is open.");
    expectedDescriptions.put("echo", "Echo the given text which may include system and user variables.");
    expectedDescriptions.put("encrypt password", "Encrypt a password for use in data source configuration.");
    expectedDescriptions.put("execute function", "Execute the function with the specified ID. By default will execute on all members.");
    expectedDescriptions.put("exit", "Exit GFSH and return control back to the calling process.");
    expectedDescriptions.put("export config", "Export configuration properties for a member or members.");
    expectedDescriptions.put("export data", "Export user data from a region to a file.");
    expectedDescriptions.put("export logs", "Export the log files for a member or members.");
    expectedDescriptions.put("export stack-traces", "Export the stack trace for a member or members.");
    expectedDescriptions.put("gc", "Force GC (Garbage Collection) on a member or members. The default is for garbage collection to occur on all caching members.");
    expectedDescriptions.put("get", "Display an entry in a region. If using a region whose key and value classes have been set, then specifying --key-class and --value-class is unnecessary.");
    expectedDescriptions.put("help", "Display syntax and usage information for all commands or list all available commands if <command> isn't specified.");
    expectedDescriptions.put("hint", "Provide hints for a topic or list all available topics if \"topic\" isn't specified.");
    expectedDescriptions.put("history", "Display or export previously executed GFSH commands.");
    expectedDescriptions.put("import data", "Import user data from a file to a region.");
    expectedDescriptions.put("list async-event-queues", "Display the Async Event Queues for all members.");
    expectedDescriptions.put("list deployed", "Display a list of JARs that were deployed to members using the \"deploy\" command.");
    expectedDescriptions.put("list disk-stores", "Display disk stores for all members.");
    expectedDescriptions.put("list durable-cqs", "List durable client cqs associated with the specified durable client id.");
    expectedDescriptions.put("list functions", "Display a list of registered functions. The default is to display functions for all members.");
    expectedDescriptions.put("list gateways", "Display the Gateway Senders and Receivers for a member or members.");
    expectedDescriptions.put("list indexes", "Display the list of indexes created for all members.");
    expectedDescriptions.put("list members", "Display all or a subset of members.");
    expectedDescriptions.put("list regions", "Display regions of a member or members.");
    expectedDescriptions.put("locate entry", "Identifies the location, including host, member and region, of entries that have the specified key.");
    expectedDescriptions.put("netstat", "Report network information and statistics via the \"netstat\" operating system command.");
    expectedDescriptions.put("pause gateway-sender", "Pause the Gateway Sender on a member or members.");
    expectedDescriptions.put("put", "Add/Update an entry in a region. If using a region whose key and value classes have been set, then specifying --key-class and --value-class is unnecessary.");
    expectedDescriptions.put("query", "Run the specified OQL query as a single quoted string and display the results in one or more pages. Limit will default to the value stored in the \"APP_FETCH_SIZE\" variable. Page size will default to the value stored in the \"APP_COLLECTION_LIMIT\" variable.");
    expectedDescriptions.put("rebalance", "Rebalance partitioned regions. The default is for all partitioned regions to be rebalanced.");
    expectedDescriptions.put("remove", "Remove an entry from a region. If using a region whose key class has been set, then specifying --key-class is unnecessary.");
    expectedDescriptions.put("resume gateway-sender", "Resume the Gateway Sender on a member or members.");
    expectedDescriptions.put("revoke missing-disk-store", "Instructs the member(s) of a distributed system to stop waiting for a disk store to be available. Only revoke a disk store if its files are lost as it will no longer be recoverable once revoking is initiated. Use the \"show missing-disk-store\" command to get descriptions of missing disk stores.");
    expectedDescriptions.put("run", "Execute a set of GFSH commands. Commands that normally prompt for additional input will instead use default values.");
    expectedDescriptions.put("set variable", "Set GFSH variables that can be used by commands. The \"echo\" command can be used to see a variable's value.");
    expectedDescriptions.put("sh", "Allows execution of operating system (OS) commands. Use '&' to return to gfsh prompt immediately. NOTE: Commands which pass output to another shell command are not currently supported.");
    expectedDescriptions.put("show dead-locks", "Display any deadlocks in the GemFire distributed system.");
    expectedDescriptions.put("show log", "Display the log for a member.");
    expectedDescriptions.put("show metrics", "Display or export metrics for the entire distributed system, a member or a region.");
    expectedDescriptions.put("show missing-disk-stores", "Display a summary of the disk stores that are currently missing from a distributed system.");
    expectedDescriptions.put("show subscription-queue-size", "Shows the number of events in the subscription queue.  If a cq name is provided, counts the number of events in the subscription queue for the specified cq.");
    expectedDescriptions.put("shutdown", "Stop all members hosting a cache. This command does not shut down any running locators. To shut down a locator, use the \"stop locator\" command.");
    expectedDescriptions.put("sleep", "Delay for a specified amount of time in seconds - floating point values are allowed.");
    expectedDescriptions.put("start data-browser", "Start Data Browser in a separate process.");
    expectedDescriptions.put("start gateway-receiver", "Start the Gateway Receiver on a member or members.");
    expectedDescriptions.put("start gateway-sender", "Start the Gateway Sender on a member or members.");
    expectedDescriptions.put("start jconsole", "Start the JDK's JConsole tool in a separate process. JConsole will be launched, but connecting to GemFire must be done manually.");
    expectedDescriptions.put("start jvisualvm", "Start the JDK's Java VisualVM (jvisualvm) tool in a separate process. Java VisualVM will be launched, but connecting to GemFire must be done manually.");
    expectedDescriptions.put("start locator", "Start a Locator.");
    expectedDescriptions.put("start pulse", "Open a new window in the default Web browser with the URL for the Pulse application.");
    expectedDescriptions.put("start server", "Start a GemFire Cache Server.");
    expectedDescriptions.put("start vsd", "Start VSD in a separate process.");
    expectedDescriptions.put("status gateway-receiver", "Display the status of a Gateway Receiver.");
    expectedDescriptions.put("status gateway-sender", "Display the status of a Gateway Sender.");
    expectedDescriptions.put("status locator", "Display the status of a Locator. Possible statuses are: started, online, offline or not responding.");
    expectedDescriptions.put("status server", "Display the status of a GemFire Cache Server.");
    expectedDescriptions.put("stop gateway-receiver", "Stop the Gateway Receiver on a member or members.");
    expectedDescriptions.put("stop gateway-sender", "Stop the Gateway Sender on a member or members.");
    expectedDescriptions.put("stop locator", "Stop a Locator.");
    expectedDescriptions.put("stop server", "Stop a GemFire Cache Server.");
    expectedDescriptions.put("undeploy", "Undeploy JARs from a member or members.");
    expectedDescriptions.put("upgrade offline-disk-store", "Upgrade an offline disk store. If the disk store is large, additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.");
    expectedDescriptions.put("validate offline-disk-store", "Scan the contents of a disk store to verify that it has no errors.");
    expectedDescriptions.put("version", "Display product version information.");
    return expectedDescriptions;
  }
}
