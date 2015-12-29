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
import event.*;
import hydra.blackboard.*;
import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.*;

/**
 * A version of the <code>EventTask</code> that performs operations
 * serially.  It also validates the state of the cache after each
 * operation.  Note that this test must be configured to use regions
 * whose scope is <code>DISTRIBUTED_ACK</code> or <code>GLOBAL</code>.
 *
 * @see #checkEventCounters(boolean)
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 */
public class SerialWBCLEventTest extends WBCLEventTest {

/* hydra task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new SerialWBCLEventTest();
      ((SerialWBCLEventTest)eventTest).initialize();
   }
}

public synchronized static void HydraTask_endTask() {
   eventTest = new SerialWBCLEventTest();
   ((SerialWBCLEventTest)eventTest).initialize();
   ((SerialWBCLEventTest)eventTest).checkEventCounters();
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

/** Check all counters from WBCLEventBB.   This verifies that the single WBCL in this
 *  DistributedSystem processed all the events for the test region.
 */
protected void checkEventCounters() {
   SharedCounters sc = EventBB.getBB().getSharedCounters();
   // get the number of operations executed thus far from the EventBB 
   long numCreates = sc.read(EventBB.NUM_CREATE);
   long numUpdates = sc.read(EventBB.NUM_UPDATE);
   long numInvalidates = sc.read(EventBB.NUM_INVALIDATE);
   long numDestroys = sc.read(EventBB.NUM_DESTROY);

   // check that we have the correct number of events thus far
   // afterCreates
   TestHelper.waitForCounter(WBCLEventBB.getBB(),        // the BB for this counter
                             "NUM_CREATES",  // String version of counter waiting on
                             WBCLEventBB.NUM_CREATES,    // Counter index (we're waiting on)
                             numCreates,                   // value we're waiting for
                             true,                         // exact
                             30000,                        // waitLimitMS
                             3000);                        // sleepTimeMS
   Log.getLogWriter().info("Successful validation of " + numCreates + " create operations");

   // afterUpdates
   TestHelper.waitForCounter(WBCLEventBB.getBB(),        // the BB for this counter
                             "NUM_UPDATES",  // String version of counter waiting on
                             WBCLEventBB.NUM_UPDATES,    // Counter index (we're waiting on)
                             numUpdates,                   // value we're waiting for
                             true,                         // exact
                             30000,                        // waitLimitMS
                             3000);                        // sleepTimeMS
   Log.getLogWriter().info("Successful validation of " + numUpdates + " update operations");

   // afterInvalidates are not processed by the WBCL (GatewayEventListener)
/*
   TestHelper.waitForCounter(WBCLEventBB.getBB(),        // the BB for this counter
                             "NUM_INVALIDATES",  // String version of counter waiting on
                             WBCLEventBB.NUM_INVALIDATES,    // Counter index (we're waiting on)
                             numInvalidates,               // value we're waiting for
                             true,                         // exact
                             30000,                        // waitLimitMS
                             3000);                        // sleepTimeMS
   Log.getLogWriter().info("Successful validation of " + numInvalidates + " invalidate operations");
*/

   // afterDestroys
   TestHelper.waitForCounter(WBCLEventBB.getBB(),        // the BB for this counter
                             "NUM_DESTROYS",  // String version of counter waiting on
                             WBCLEventBB.NUM_DESTROYS,   // Counter index (we're waiting on)
                             numDestroys,                  // value we're waiting for
                             true,                         // exact
                             30000,                        // waitLimitMS
                             3000);                        // sleepTimeMS
   Log.getLogWriter().info("Successful validation of " + numDestroys + " destroy operations");

}

}
