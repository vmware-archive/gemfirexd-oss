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
 * @author Lynn Gallinat
 * @since 3.0
 */
public class SerialEntryEventTest extends EventTest {

/* hydra task methods */
/* ======================================================================== */
public static void createLocatorTask() {
  DistributedSystemHelper.createLocator();
}

public static void startLocatorAndAdminDSTask() {
  DistributedSystemHelper.startLocatorAndAdminDS();
}

public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new SerialEntryEventTest();
      eventTest.initialize();

      // Clear out any unwanted AfterRegionCreate counts
      EventCountersBB.getBB().zeroAllCounters();
      OperationCountersBB.getBB().zeroAllCounters();
   }
}

public synchronized static void HydraTask_endTask() {
   eventTest = new SerialEntryEventTest();
   eventTest.initialize();
   ((SerialEntryEventTest)eventTest).checkEventCounters(false);
}

/* override methods */
/* ======================================================================== */
protected int getNumVMsWithListeners() {
   // if we don't have an AllEvents dataPolicy, only the local listener is notified
   RegionAttributes ra = CacheUtil.getRegion(regionName).getAttributes();
   DataPolicy dataPolicy = ra.getDataPolicy();
   InterestPolicy interestPolicy = ra.getSubscriptionAttributes().getInterestPolicy();
   boolean allEvents = dataPolicy.withReplication() || interestPolicy.isAll();
   if (allEvents) {
     // assumes exactly one locator whether master-managed or client-managed
     int numVMs = TestHelper.getNumVMs();
     boolean masterManagedLocator = TestConfig.tab().booleanAt(Prms.manageLocatorAgents);
     return masterManagedLocator ? numVMs : numVMs - 1;
   } else {
     return 1;
   }
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

// ConcurrentMap APIs
protected void putIfAbsent(Region aRegion, boolean aBoolean) {
   super.putIfAbsent(aRegion, aBoolean);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

protected void replace(Region aRegion) {
   super.replace(aRegion);
   // We can't check for events w/ transactions until we've committed
   if (!useTransactions) {
     checkEventCounters();
   }
}

protected void remove(Region aRegion) {
   super.remove(aRegion);
   // We can't check for events w/ transactions until we've committed
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
   boolean eventCountExact = false;

   RegionAttributes ra = CacheUtil.getRegion(regionName).getAttributes();
   DataPolicy dataPolicy = ra.getDataPolicy();
   InterestPolicy interestPolicy = ra.getSubscriptionAttributes().getInterestPolicy();
   boolean allEvents = dataPolicy.withReplication() || interestPolicy.isAll();
   if (allEvents) {
      eventCountExact = true;
   }
   String dpString = (eventCountExact) ? "exact" : "an established minimum";
   Log.getLogWriter().info("checkEventCounters will validate that counters are " + dpString + " based on dataPolicy " + dataPolicy + " and interestPolicy " + interestPolicy);

   checkEventCounters(eventCountExact);
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

   int numVmsWithList = getNumVMsWithListeners();
   Log.getLogWriter().info("num VMs InterestALL with listener installed: " + numVmsWithList);

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numAfterCreateEvents_isDist", (numCreate * numVmsWithList), eventCountExact));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", (numCreate * numVmsWithList), eventCountExact)); 

           al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", (numCreate * (numVmsWithList - 1)), eventCountExact));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", numCreate, eventCountExact));

           al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", (numCreate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", (numCreate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", (numCreate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", (numCreate * numVmsWithList), eventCountExact)); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", (numDestroy * numVmsWithList), eventCountExact));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", (numDestroy * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", (numDestroy * (numVmsWithList - 1)), eventCountExact));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", numDestroy, eventCountExact));

           al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", (numDestroy * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", (numDestroy * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", (numDestroy * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", (numDestroy * numVmsWithList), eventCountExact)); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", (numInval * numVmsWithList), eventCountExact));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (numInval * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", (numInval * (numVmsWithList - 1)), eventCountExact));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", numInval, eventCountExact));

           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", (numInval * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", (numInval * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", (numInval * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", (numInval * numVmsWithList), eventCountExact)); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", (numUpdate * numVmsWithList), eventCountExact));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", (numUpdate * numVmsWithList), eventCountExact)); 

           al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", (numUpdate * (numVmsWithList - 1)), eventCountExact));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotRemote", numUpdate, eventCountExact));

           al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", (numUpdate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", (numUpdate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", (numUpdate * numVmsWithList), eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", (numUpdate * numVmsWithList), eventCountExact)); 

        // afterRegionDestroy counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0, eventCountExact));

        // afterRegionInvalidate counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", (numRegionInval * numVmsWithList), eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", numLocalRegionInval, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", ((numRegionInval * numVmsWithList) + numLocalRegionInval), eventCountExact));

           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", (numRegionInval * (numVmsWithList - 1)), eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", (numRegionInval + numLocalRegionInval), eventCountExact));

        // afterRegionCreate counters
        // Listeners won't count regionCreates during test initialization or endtask
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isDist", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotExp", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isRemote", 0, eventCountExact));
           al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotRemote", 0, eventCountExact));

        // Region.close()
           al.add(new ExpCounterValue("numClose", numClose, eventCountExact));

   EventCountersBB.getBB().checkEventCounters(al);

   // Same EventBB counters can be used to determine validity of OperationCounters
   OperationCountersBB.getBB().checkEventCounters(al);

   // Validate cacheWriter invocations (one per operation, local VM preferred)
   al = new ArrayList();
        // beforeCreate counters
           al.add(new ExpCounterValue("numBeforeCreateEvents_isDist", numCreate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotExp", numCreate, eventCountExact)); 

           al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", "numBeforeCreateEvents_isNotRemote", numCreate));

           al.add(new ExpCounterValue("numBeforeCreateEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLoad", numCreate, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLocalLoad", numCreate, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetLoad", numCreate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetSearch", numCreate, eventCountExact));

        // beforeDestroy counters
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isDist", numDestroy, eventCountExact));
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotExp", numDestroy, eventCountExact));
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", "numBeforeDestroyEvents_isNotRemote", numDestroy));

           al.add(new ExpCounterValue("numBeforeDestroyEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLoad", numDestroy, eventCountExact));
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLocalLoad", numDestroy, eventCountExact));
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetLoad", numDestroy, eventCountExact));
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetSearch", numDestroy, eventCountExact));

        // beforeUpdate counters
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isDist", numUpdate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotExp", numUpdate, eventCountExact));

           al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", "numBeforeUpdateEvents_isNotRemote", numUpdate));

           al.add(new ExpCounterValue("numBeforeUpdateEvents_isLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLoad", numUpdate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isLocalLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLocalLoad", numUpdate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetLoad", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetLoad", numUpdate, eventCountExact));
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetSearch", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetSearch", numUpdate, eventCountExact));

        // beforeRegionDestroy counters
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isDist", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotExp", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isRemote", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotRemote", 0, eventCountExact));

        // beforeRegionClear counters
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isDist", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isNotDist", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isExp", 0, eventCountExact)); 
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isNotExp", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isRemote", 0, eventCountExact));
           al.add(new ExpCounterValue("numBeforeRegionClearEvents_isNotRemote", 0, eventCountExact));

   WriterCountersBB.getBB().checkEventCounters(al);
}

}
