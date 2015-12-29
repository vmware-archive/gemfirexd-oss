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
package newWan.orderPolicy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import newWan.WANTestPrms;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;

import hydra.ConfigPrms;
import hydra.GatewayReceiverHelper;
import hydra.GatewaySenderHelper;
import hydra.Log;
import hydra.blackboard.SharedMap;
import orderPolicy.OrderPolicyTest;
import util.TestException;
import util.TestHelper;
import wan.WANBlackboard;

public class NewWanOrderPolicyTest extends OrderPolicyTest {

  /**
   * Creates GatewaySender ids based on the
   * {@link ConfigPrms#gatewaySenderConfig}.
   */
  public synchronized static void createGatewaySenderIdsTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.createGatewaySenderIds(senderConfig);
  }
  
  /**
   * Initialise new wan components
   */
  public synchronized static void HydraTask_initWANComponentsTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.startGatewaySenders(senderConfig);

    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
  }

  /**
   * Check that no Listener Exceptions were posted to the BB
   */
  public static void checkForEventErrors() {
    // process ALL events
//    waitForQueuesToDrain();

    // check to see if we've had any skips in value sequence in listeners
    // (posted to BB)
    TestHelper.checkForEventError(WANBlackboard.getInstance());
  }

  /**
   * Waits {@link newWan.WANTestPrms#secToWaitForQueue} for queues to drain.
   * Suitable for use as a helper method or hydra task.
   */
  public static void waitForQueuesToDrain() {    
    // Lets check to make sure the queues are empty
    long startTime = System.currentTimeMillis();
    long maxWait = WANTestPrms.getSecToWaitForQueue();
    long entriesLeft = 0;
    Map queueEntryMap = new HashMap();
    
    while ((System.currentTimeMillis() - startTime) < (maxWait * 1000)) {
      boolean pass = true;
      entriesLeft = 0;
      Set<GatewaySender> gwSender = GatewaySenderHelper.getGatewaySenders();
      for (GatewaySender gs : gwSender) {
        int queuesize = 0;
        Set<RegionQueue> qrs = ((AbstractGatewaySender)gs).getQueues();        
        for (RegionQueue rq : qrs) {
          queuesize += rq.size();
        }
        entriesLeft += queuesize;
        queueEntryMap.put(gs.getId(), new Integer(queuesize));
        if (queuesize > 0) {
          Log.getLogWriter().warning("Still waiting for queue to drain. SerialGatewaySender "
                  + gs + " has " + queuesize + " entries in it.");
          pass = false;
        }

      }

      if (pass) {
        entriesLeft = 0;
        break;
      }
      else {
        try {
          Thread.sleep(1000);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }      
    }
    if (entriesLeft > 0) {
      throw new TestException(
          "Timed out waiting for queue to drain. Waited for " + maxWait
              + " sec, total entries left in all queues are " + entriesLeft
              + ". queueEntryMap=" + queueEntryMap);
    }
    else {
      Log.getLogWriter().info("SENDER QUEUES ARE DRAINED");
    }
  }
}
