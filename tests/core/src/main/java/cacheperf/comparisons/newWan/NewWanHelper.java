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
package cacheperf.comparisons.newWan;

import hydra.GatewaySenderDescription;
import hydra.GatewaySenderHelper;
import hydra.GatewaySenderPrms;
import hydra.Log;
import hydra.TestConfig;

import java.util.Vector;

import com.gemstone.gemfire.cache.wan.GatewaySender;

/**
 * 
 * @author rdiyewar
 * @since 7.0
 */
public class NewWanHelper {
  static int wanSites           = new Integer(TestConfig.getInstance().getSystemProperty("wanSites")).intValue();
  static int dataHostsPerSite   = new Integer(TestConfig.getInstance().getSystemProperty("dataHostsPerSite")).intValue();
  static int dataVMsPerHost     = new Integer(TestConfig.getInstance().getSystemProperty("dataVMsPerHost")).intValue();    
  static int dataThreadsPerVM   = new Integer(TestConfig.getInstance().getSystemProperty("dataThreadsPerVM")).intValue();
 
  static String groupB          = TestConfig.getInstance().getSystemProperty("B"); //either accessor or clients
  static int bHostsPerSite      = new Integer(TestConfig.getInstance().getSystemProperty(groupB + "HostsPerSite")).intValue();
  static int bVMsPerHost        = new Integer(TestConfig.getInstance().getSystemProperty(groupB + "VMsPerHost")).intValue();
  static int bThreadsPerVM      = new Integer(TestConfig.getInstance().getSystemProperty(groupB + "ThreadsPerVM")).intValue();
  
  static int objectSize         = new Integer(TestConfig.getInstance().getSystemProperty("objectSize")).intValue();
  
  static long maxQueuesCapacity = NewWanHelper.getMaxQueuesCapacity();
  static int perEntryOverhead   = 600;   //600 byte is theoretical per entry overhead
  static int inputPutsPerSecPerThread = NewWanPerfPrms.getInputPutsPerSec() / (wanSites * bHostsPerSite * bVMsPerHost * bThreadsPerVM);
  
  static {
    StringBuffer str = new StringBuffer().append("NewWanHelper: wanSites=").append(wanSites)
        .append(" dataHostsPerSite=").append(dataHostsPerSite)
        .append(" dataVMsPerHost=").append(dataVMsPerHost)
        .append(" dataThreadsPerVM=").append(dataThreadsPerVM)
        .append(" bHostsPerSite=").append(bHostsPerSite)
        .append(" bVMsPerHost=").append(bVMsPerHost)
        .append(" bThreadsPerVM=").append(bThreadsPerVM)
        .append(" objectSize=").append(objectSize)
        .append(" maxQueuesCapacity=").append(maxQueuesCapacity)
        .append(" inputPutsPerSecPerThread=").append(inputPutsPerSecPerThread);
    Log.getLogWriter().info(str.toString()); 
  }
  
  /**
   * Get cumulative maximum queue capacity across all queues
   * @param isSerial true for serial sender, false for parallel sender.
   * @return max queue capacity
   */
  private static long getMaxQueuesCapacity() {
    Vector senderNames = TestConfig.tab().vecAt(GatewaySenderPrms.names, null);
    GatewaySenderDescription gsd = GatewaySenderHelper.getGatewaySenderDescription((String)senderNames.get(0));

    boolean isSerial = !gsd.getParallel();
    int numOfActiveQueues = (isSerial) ? wanSites : (wanSites * dataHostsPerSite * dataVMsPerHost);
    long perQueueMaxMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY * 1000000;
    int objSize = objectSize + perEntryOverhead; //600 is the entry overhead
    
    // perQueueMaxMemory = maxEntries * (perEntrySize + perEntryOverhead) / numOfActiveQueues
    long maxEntries = perQueueMaxMemory * numOfActiveQueues / objSize; 
    return maxEntries;
  }
  
  /**
   * Get max entries per user thread per iteration.
   * @param perEntrySize dataSize in bytes per entry
   * @param totalUserThreads Total user threads doing operations
   * @param percFactor Percentage factor (if 80 is set, then 80% of entriesPerThread will be return)
   * @return max entries per thread.
   */
  public static long getEntriesPerThread(int perEntrySize, int totalUserThreads, int numOfQueues, int percFactor){
    //use default queue memory, whenever tuning requires, change the default value.
    long queueMaxMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY * 1000000;
    
    // queueMaxMemory = queueMaxEntries * numOfQueues * (perEntrySize + perEntryOverhead)    
    long queueMaxEntries = queueMaxMemory * numOfQueues / (perEntrySize + perEntryOverhead);
    
    //maxEntriesPerUser = queueMaxEntries / totalUserThreads
    long maxEntriesPerUser = queueMaxEntries / totalUserThreads;
    return maxEntriesPerUser * percFactor / 100;    
  }
}
