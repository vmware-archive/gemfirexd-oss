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

import hydra.GatewaySenderHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.blackboard.SharedMap;

import java.util.Iterator;
import java.util.Set;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;

public class NewWanPSTClient extends CachePerfClient {
  
  private static HydraThreadLocal queueMonitorStarted = new HydraThreadLocal();
  
  /**
   * TASK to put objects with new wan and wait for the sender queues to drain.
   */
  public static void putDataGWSenderTask() {
    NewWanPSTClient c = new NewWanPSTClient();
    c.initialize(PUTS);
    c.putDataGWSender();
  }
  private void putDataGWSender() {
    if (this.useTransactions) {
      this.begin();
    }
    int gatewayQueueEntries = CachePerfPrms.getGatewayQueueEntries();
    boolean batchDone = false;
    do {
      // delay getting key until inside put loop
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      for (int i = 0; i < gatewayQueueEntries; i++) {
        int key = getNextKey();
        put(key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
      }
//      waitForGWSenderQueuesToDrain();
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void waitForGWSenderQueuesToDrain() {
    long start = this.statistics.startGatewayQueueDrain();    
    Set senders = GatewaySenderHelper.getGatewaySenders();
    if(senders != null){ // datastore node having senders 
      while (senders != null) {
        MasterController.sleepForMs(1);
        int size = 0;
        int totalBatchSize = 0;
        for (Iterator i = senders.iterator(); i.hasNext();) {        
          GatewaySender sender = (GatewaySender)i.next();
          totalBatchSize += sender.getBatchSize();
          Set<RegionQueue> rqs = ((AbstractGatewaySender)sender).getQueues();
          for (RegionQueue rq: rqs){
            size += rq.size();
          }
        }
        if (size <= totalBatchSize) {
          break;
        }
      }
    }
    else { // accessor node.
      final SharedMap sharedMap = NewWanPerfBlackboard.getInstance().getSharedMap();
      while (true) {
        MasterController.sleepForMs(100);
        long size = 0;
        Set keySet = sharedMap.getMap().keySet();
        Iterator kIt = keySet.iterator();
        while (kIt.hasNext()) {
          String k = (String)kIt.next();
          if (k.startsWith("EVENT_QUEUE")) {
            Long value = (Long)sharedMap.get(k);
            size += value.longValue();
          }
        }
        if (size <= 500) {
          break;
        }
      }
    }

    this.statistics.endGatewayQueueDrain(start, 1, this.isMainWorkload,
                                                   this.histogram);
  }
  
  /**
   * TASK start sender queue monitoring
   */
  public synchronized static void startQueueMonitorTask(){
    NewWanPSTClient c = new NewWanPSTClient();
    c.initialize();
    if (queueMonitorStarted.get() == null 
        || ((Boolean)queueMonitorStarted.get()).equals(new Boolean(false))){
      c.startQueueMonitor();  
      queueMonitorStarted.set(new Boolean(true));
    }    
  }
  /**
   * Starts a thread to monitor queues on the cache. Should only be invoked from GatewaySender 
   */
  protected void startQueueMonitor() {
    final String key = "EVENT_QUEUE_SIZE: for vm_" + RemoteTestModule.getMyVmid();
    Log.getLogWriter().info("Started event queue monitor with key: " + key);
    final SharedMap bb = NewWanPerfBlackboard.getInstance().getSharedMap();
    Thread queueMonitor = new Thread(new Runnable() {
      public void run() {
        while (true) {
          Set senders = GatewaySenderHelper.getGatewaySenders();
          long size = 0;
          if (senders != null) {
            for (Iterator i = senders.iterator(); i.hasNext();) {
              GatewaySender sender = (GatewaySender)i.next();
              Set<RegionQueue> rqs = ((AbstractGatewaySender)sender).getQueues();
              for (RegionQueue rq : rqs) {
                size += rq.size();
              }
            }
          }
          try {
            bb.put(key, new Long(size));
          }
          catch (Exception e) {
            e.printStackTrace();
          }
          MasterController.sleepForMs(5);
        }
      }

      protected void finalize() throws Throwable {
        Object o = bb.remove(key);
        super.finalize();
      }
    }, "Sender Queue Monitor");
    queueMonitor.setDaemon(true);
    queueMonitor.start();
  }
}
