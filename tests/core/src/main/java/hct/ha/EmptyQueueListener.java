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
package hct.ha;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import hydra.blackboard.SharedCounters;
import hydra.Log;
import hydra.MasterController;

/**
 * This class is a <code>CacheListener</code> implementation attached to the
 * cache-clients for test validations. The callback methods validate the order
 * of the data coming via the events.This listener is used for failover test
 * cases
 * 
 * @author Dinesh Patel
 * @author Mitul Bid
 * 
 */
public class EmptyQueueListener extends CacheListenerAdapter
{
  
  public static boolean stableStateAchieved = false;

  /**
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event)
  {
    verify("afterCreate");
  }

  /**
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event)
  {
    verify("afterUpdate");
  }

  /**
    */
  public void afterInvalidate(EntryEvent event)
  {
    verify("afterInvalidate");
  }

  /**
   * the entry event received in callback
   */
  public void afterDestroy(EntryEvent event)
  {
    verify("afterDestroy");
  }

  
  /**
   * Waits for signal that feeding cycle is complete, sleeps, signals to self
   * and other VMs that a stable state has been achieved.
   */
  public static void setStableStateAchievedAndSleep(){
    // wait (poll) for feed signal to be incremented
    SharedCounters counters = HAClientQueueBB.getBB().getSharedCounters();
    Log.getLogWriter().info("Waiting for feed signal...");
    while (true) {
      MasterController.sleepForMs(3000);
      if (counters.read(HAClientQueueBB.feedSignal) > 0) {
        Log.getLogWriter().info("Got feed signal...");
        break;
      }
    }
    try{
      Thread.sleep(180000);
    }
    catch(Exception ignore){
    }
    stableStateAchieved = true;
    Log.getLogWriter().info("Setting stable signal...");
    counters.increment(HAClientQueueBB.stableSignal); // signal stability
  }
  
  public void verify(String operation)
  {
    if (stableStateAchieved) {
      String reason = "Received an " + operation
          + " operation when it was not expected";
      long exceptionNumber = HAClientQueueBB.getBB().getSharedCounters().incrementAndRead(
          HAClientQueueBB.NUM_EXCEPTION);
      HAClientQueueBB.getBB().getSharedMap().put(new Long(exceptionNumber),
          reason);
    }

  }
 
}
