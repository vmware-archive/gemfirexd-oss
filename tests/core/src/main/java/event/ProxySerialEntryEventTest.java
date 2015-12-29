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
package event; 

import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.cache.*;

/**
 * A version of the <code>EventTask</code> that performs operations
 * serially.  It also validates the state of the cache after each
 * operation.  Note that this test must be configured to use regions
 * whose scope is <code>DISTRIBUTED_ACK</code> or <code>GLOBAL</code>.
 *
 * @see #checkEventCounters(boolean)
 *
 * @author Lynn Gallinat & lhughes
 * @since 5.0
 */
public class ProxySerialEntryEventTest extends ProxyEventTest {

/* hydra task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ProxySerialEntryEventTest();
      eventTest.initialize();

      // Clear out any unwanted AfterRegionCreate counts
      EventCountersBB.getBB().zeroAllCounters();
      OperationCountersBB.getBB().zeroAllCounters();
   }
}

public synchronized static void HydraTask_closeTask() {
   ((ProxySerialEntryEventTest)eventTest).checkEventCounters(false);
}

/* override methods */
/* ======================================================================== */
protected int getNumVMsWithListeners() {
   // if we don't have an AllEvents dataPolicy, only the local listener is notified
   RegionAttributes ra = CacheUtil.getRegion(eventTest.regionName).getAttributes();
   DataPolicy dataPolicy = ra.getDataPolicy();
   InterestPolicy interestPolicy = ra.getSubscriptionAttributes().getInterestPolicy();
   boolean allEvents = dataPolicy.withReplication() || interestPolicy.isAll();
   return (allEvents) ? TestHelper.getNumVMs()-1 : 1;
}

protected void addObject(Region aRegion, boolean aBoolean) {
   super.addObject(aRegion, aBoolean);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
   super.invalidateObject(aRegion, isLocalInvalidate);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
   super.destroyObject(aRegion, isLocalDestroy);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

protected void updateObject(Region aRegion) {
   super.updateObject(aRegion);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

/** Check all counters from EventCountersBB. This verifies that 
 *  all events were distributed to all VMs (full distribution) and
 *  that no region or local events occurred.
 */
protected void checkEventCounters() {
   checkEventCounters(true);
}

/** Check event counters. If numCloseIsExact is true, then the number of
 *  close events must be an exact match, otherwise allow numClose events
 *  to be the minimum of the expected numClose counter. This is useful in
 *  tests where the timing of shutting down the VMs/C clients may or may
 *  not cause a close event.
 *
 *  @param numCloseIsExact True if the numClose event counters must exactly
 *         match the expected numClose value, false if the numClose event
 *         counters must be no less than the expected numClose counter.
 */
protected void checkEventCounters(boolean numCloseIsExact) {
   SharedCounters counters = EventBB.getBB().getSharedCounters();
   long numCreate = counters.read(EventBB.NUM_CREATE);
   long numUpdate = counters.read(EventBB.NUM_UPDATE);
   long numDestroy = counters.read(EventBB.NUM_DESTROY);
   long numInval = counters.read(EventBB.NUM_INVALIDATE);
   long numRegionCreate = counters.read(EventBB.NUM_REGION_CREATE);
   long numRegionDestroy = counters.read(EventBB.NUM_REGION_DESTROY);
   long numRegionInval = counters.read(EventBB.NUM_REGION_INVALIDATE);
   long numLocalDestroy = counters.read(EventBB.NUM_LOCAL_DESTROY);
   long numLocalInval = counters.read(EventBB.NUM_LOCAL_INVALIDATE);
   long numLocalRegionDestroy = counters.read(EventBB.NUM_LOCAL_REGION_DESTROY);
   long numLocalRegionInval = counters.read(EventBB.NUM_LOCAL_REGION_INVALIDATE);
   long numClose = counters.read(EventBB.NUM_CLOSE);

   /* Note that with Proxy's, updates are reflected as creates (since there is
    * no local storage (therefore, combine the numCreate and numUpdate counters)
    */
   numCreate = numCreate + numUpdate;
   numUpdate = 0;

   int numVmsWithList = getNumVMsWithListeners();
   Log.getLogWriter().info("num VMs InterestALL with listener installed: " + numVmsWithList);

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numAfterCreateEvents_isDist", (numCreate * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (numCreate * numVmsWithList)));

           al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", (numCreate * (numVmsWithList - 1))));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", numCreate));

           al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", ((numCreate * numVmsWithList) + (numUpdate * (numVmsWithList-1))))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", ((numCreate * numVmsWithList) + (numUpdate * (numVmsWithList-1))))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", ((numCreate * numVmsWithList) + (numUpdate * (numVmsWithList-1))))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", ((numCreate * numVmsWithList) + (numUpdate * (numVmsWithList-1))))); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", (numDestroy * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", (numDestroy * (numVmsWithList - 1))));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", numDestroy));

           al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", (numDestroy * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", (numDestroy * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", (numDestroy * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", (numDestroy * numVmsWithList))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", (numInval * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", (numInval * (numVmsWithList - 1))));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", numInval));

           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", (numInval * numVmsWithList))); 

        // afterUpdate counters
        // Note:  numUpdates has been initialized to 0 (above)
        // as there is no local storage and ALL updates are creates.
           al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", 0));

           al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotRemote", 0));

           al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", 0));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", 0));

        // afterRegionDestroy counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0));

        // afterRegionInvalidate counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", (numRegionInval * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", numLocalRegionInval)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", ((numRegionInval * numVmsWithList) + numLocalRegionInval)));

           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", (numRegionInval * (numVmsWithList - 1))));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", (numRegionInval + numLocalRegionInval)));

        // afterRegionCreate counters
        // Listeners won't count regionCreates during test initialization or endtask
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotRemote", 0));

        // Region.close()
           al.add(new ExpCounterValue("numClose", numClose, numCloseIsExact));

   EventCountersBB.getBB().checkEventCounters(al);

   // Same EventBB counters can be used to determine validity of OperationCounters
   OperationCountersBB.getBB().checkEventCounters(al);
}

}
