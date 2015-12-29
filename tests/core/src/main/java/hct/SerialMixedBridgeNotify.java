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
public class SerialMixedBridgeNotify extends BridgeNotify {

/* hydra task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (bridgeClient == null) {
      bridgeClient = new SerialMixedBridgeNotify();
      bridgeClient.initialize();

      // Clear out any unwanted AfterRegionCreate counts
      EventCountersBB.getBB().zeroAllCounters();
   }
}

public synchronized static void HydraTask_endTask() {
   bridgeClient = new SerialMixedBridgeNotify();
   bridgeClient.initialize();
   ((SerialMixedBridgeNotify)bridgeClient).checkEventCounters(false);
}

protected void addObject(Region aRegion, boolean aBoolean) {
   super.addObject(aRegion, aBoolean);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void addObjectViaPutAll(Region aRegion, boolean aBoolean) {
  super.addObjectViaPutAll(aRegion, aBoolean);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
   super.invalidateObject(aRegion, isLocalInvalidate);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
   super.destroyObject(aRegion, isLocalDestroy);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void destroyAndCreateObject(Region aRegion, boolean isLocalDestroy) {
   super.destroyAndCreateObject(aRegion, isLocalDestroy);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void updateObject(Region aRegion) {
   super.updateObject(aRegion);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void updateObjectViaPutAll(Region aRegion) {
  super.updateObjectViaPutAll(aRegion);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

// ConcurrentMap API support
protected void putIfAbsent(Region aRegion, boolean aBoolean) {
   super.putIfAbsent(aRegion, aBoolean);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void remove(Region aRegion) {
   super.remove(aRegion);
   // We cannot check counters until commit time with tx
   if (!useTransactions) {
      checkEventCounters();
   }
}

protected void replace(Region aRegion) {
   super.replace(aRegion);
   // We cannot check counters until commit time with tx
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

   /* the ALL_KEYS client will always get events (including creates on new keys)
    * the ALL_KEYS and one of the KeyList (odd/even) clients will always get all events 
    * on the original keySet
    * we'll get 3 events per operation if we happen to select the singleKey client's key
    * This is written to the BB by the VM performing the operation (it knows how many
    * clients should get the update)
    */
   // factor this out to separate lines to track down an NPE issue
   // 2006-08-29 jpenney
   BridgeNotifyBB bnbb = BridgeNotifyBB.getBB();
   SharedMap sm = bnbb.getSharedMap();
   Object nl = sm.get(BridgeNotifyBB.numListeners);
   int numVmsWithList = ((Integer)nl).intValue();
   Log.getLogWriter().info("num VMs with listener installed: " + numVmsWithList);

   // This mixedBridgeNotify test always uses notifyBySubscription/registerInterest
   // Note:  to some degree we have to know if this key was in the original keyList
   ArrayList al = new ArrayList();
   al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (numCreate * numVmsWithList + numPutAllCreate * numVmsWithList), eventCountExact)); 
   al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList), eventCountExact)); 
   al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numInval * numVmsWithList), eventCountExact)); 
   al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", (numUpdate * numVmsWithList + numPutAllUpdate * numVmsWithList), eventCountExact)); 
   al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0, eventCountExact));
   al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotExp", 0, eventCountExact));
   al.add(new ExpCounterValue("numClose", numClose, eventCountExact));

   EventCountersBB.getBB().checkEventCounters(al);

   // This only works on a serial basis, since each operation performed has a different
   // number of listeners with registered interest, therefore, we must clean the 
   // BridgeNotifyBB operation counters each time
   counters.zero( BridgeNotifyBB.NUM_CREATE);
   counters.zero( BridgeNotifyBB.NUM_UPDATE);
   counters.zero( BridgeNotifyBB.NUM_PUTALL_CREATE);
   counters.zero( BridgeNotifyBB.NUM_PUTALL_UPDATE);
   counters.zero( BridgeNotifyBB.NUM_DESTROY);
   counters.zero( BridgeNotifyBB.NUM_INVALIDATE);

   // clear EventCountersBB as well (for actual event counters recv'd)
   EventCountersBB.getBB().zeroAllCounters();
}

}
