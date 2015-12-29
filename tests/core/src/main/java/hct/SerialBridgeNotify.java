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
package hct; 

import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.cache.*;

/**
 * A version of the <code>BridgeNotify</code> that performs operations
 * serially.  It also validates the state of the cache after each
 * operation.  Note that this test must be configured to use servers
 * whose scope is <code>DISTRIBUTED_ACK</code> or <code>GLOBAL</code>.
 *
 * @see #checkEventCounters(boolean)
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.1
 */
public class SerialBridgeNotify extends BridgeNotify {

/* hydra task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (bridgeClient == null) {
      bridgeClient = new SerialBridgeNotify();
      bridgeClient.initialize();

      // Clear out any unwanted AfterRegionCreate counts
      EventCountersBB.getBB().zeroAllCounters();
   }
}

public synchronized static void HydraTask_endTask() {
   bridgeClient = new SerialBridgeNotify();
   bridgeClient.initialize();
   ((SerialBridgeNotify)bridgeClient).checkEventCounters(false);
}

/* override methods */
/* ======================================================================== */
protected int getNumVMsWithListeners() {
   BridgeNotifyBB.getBB().printSharedCounters();
   return (int)BridgeNotifyBB.getBB().getSharedCounters().read(BridgeNotifyBB.NUM_LISTENERS);
}

protected void addObject(Region aRegion, boolean aBoolean) {
   super.addObject(aRegion, aBoolean);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
   super.invalidateObject(aRegion, isLocalInvalidate);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
   super.destroyObject(aRegion, isLocalDestroy);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void updateObject(Region aRegion) {
   super.updateObject(aRegion);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void putIfAbsent(Region aRegion, boolean aBoolean) {
   super.putIfAbsent(aRegion, aBoolean);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void remove(Region aRegion) {
   super.remove(aRegion);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void replace(Region aRegion) {
   super.replace(aRegion);
   // We can't check for events w/transactions until we've committed
   if (!useTransactions) {
      checkEventCounters();
   }
}

/** Check all counters from EventCountersBB. For ALL interest subscribers, this 
 *  verifies that all events were distributed to all VMs (full distribution) and
 *  that no region events occurred.
 *
 *  When ALL interest subscribers are used, ensures that a minimum number of 
 *  events are delivered (we may additionally update caches and deliver events
 *  for clients who got data values through netLoad).
 *
 *  @see com.gemstone.gemfire.cache.InterestPolicy.ALL
 */
protected void checkEventCounters() {

   Log.getLogWriter().info("checkEventCounters will validate that counters are exact");
   checkEventCounters(true);
}

/** Check event counters. If numCloseIsExact is true, then the number of
 *  close events must be an exact match, otherwise allow numClose events
 *  to be the minimum of the expected numClose counter. This is useful in
 *  tests where the timing of shutting down the VMs/C clients may or may
 *  not cause a close event.
 *
 *  @param eventCountExact - True if the event counters must exactly
 *         match the expected value, false if the event
 *         counters must be no less than the expected counter.
 */
protected void checkEventCounters(boolean eventCountExact) {

   SharedCounters counters = BridgeNotifyBB.getBB().getSharedCounters();
   long numCreate = counters.read(BridgeNotifyBB.NUM_CREATE);
   long numUpdate = counters.read(BridgeNotifyBB.NUM_UPDATE);
   long numPutAllCreate = counters.read(BridgeNotifyBB.NUM_PUTALL_CREATE);
   long numPutAllUpdate = counters.read(BridgeNotifyBB.NUM_PUTALL_UPDATE);
   long numDestroy = counters.read(BridgeNotifyBB.NUM_DESTROY);
   long numInval = counters.read(BridgeNotifyBB.NUM_INVALIDATE);
   long numRegionCreate = counters.read(BridgeNotifyBB.NUM_REGION_CREATE);
   long numRegionDestroy = counters.read(BridgeNotifyBB.NUM_REGION_DESTROY);
   long numRegionInval = counters.read(BridgeNotifyBB.NUM_REGION_INVALIDATE);
   long numLocalDestroy = counters.read(BridgeNotifyBB.NUM_LOCAL_DESTROY);
   long numLocalInval = counters.read(BridgeNotifyBB.NUM_LOCAL_INVALIDATE);
   long numLocalRegionDestroy = counters.read(BridgeNotifyBB.NUM_LOCAL_REGION_DESTROY);
   long numLocalRegionInval = counters.read(BridgeNotifyBB.NUM_LOCAL_REGION_INVALIDATE);
   long numClose = counters.read(BridgeNotifyBB.NUM_CLOSE);

   int numVmsWithList = getNumVMsWithListeners();
   Log.getLogWriter().info("num VMs with listener installed: " + numVmsWithList);

   // todo@lhughes - other flags seem random at this point, use notExp for now and expand as product matures
   // todo@lhughes - set notifyBySubscription based on BridgePrms ... so we can strictly look for invalidate events
   boolean receiveValuesAsInvalidates = ((Boolean)BridgeNotifyBB.getBB().getSharedMap().get(BridgeNotifyBB.RECEIVE_VALUES_AS_INVALIDATES)).booleanValue();

   ArrayList al = new ArrayList();
   Log.getLogWriter().info("BridgeNotifyBB.RECEIVE_VALUES_AS_INVALIDATES is set to " + receiveValuesAsInvalidates);
   if (!receiveValuesAsInvalidates) {    // expect events with data
      al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (numCreate * numVmsWithList)+(numPutAllCreate * numVmsWithList), eventCountExact)); 
//      al.add(new ExpCounterValue("numAfterPutAllCreateEvents_isNotExp", (numPutAllCreate * numVmsWithList), eventCountExact));
      al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList), eventCountExact)); 
      al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numInval * numVmsWithList), eventCountExact)); 
      al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", (numUpdate * numVmsWithList)+(numPutAllUpdate * numVmsWithList), eventCountExact)); 
//      al.add(new ExpCounterValue("numAfterPutAllUpdateEvents_isNotExp", (numPutAllUpdate * numVmsWithList), eventCountExact));
      al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0, eventCountExact));
      al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotExp", 0, eventCountExact));
      al.add(new ExpCounterValue("numClose", numClose, eventCountExact));
   } else {
      al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numUpdate * (numVmsWithList-1) + numCreate * (numVmsWithList-1) + numPutAllUpdate * (numVmsWithList-1) + numPutAllCreate * (numVmsWithList-1)), eventCountExact));
      al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList), eventCountExact));
   }

   EventCountersBB.getBB().checkEventCounters(al);
}

}
