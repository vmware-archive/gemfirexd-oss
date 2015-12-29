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

//import java.math.*;
import java.util.*;
//import java.lang.reflect.*;
import util.*;
//import hydra.*;
import hydra.blackboard.*;
//import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.NanoTimer;

public class SerialRegionEventTest extends EventTest {

boolean inEndTask = false;

/* init methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new SerialRegionEventTest();
      eventTest.initialize();

      // Clear out any unwanted AfterRegionCreate counts
      EventCountersBB.getBB().zeroAllCounters();
      OperationCountersBB.getBB().zeroAllCounters();
   }
}

/* override methods */
/* ======================================================================== */
public static void HydraTask_endTask() throws Throwable {
//  eventTest = new SerialRegionEventTest();
  ((SerialRegionEventTest)eventTest).inEndTask = true;
  eventTest.checkEventCounters();
}

protected int getNumVMsWithListeners() {
   return 1;
}

protected void addRegion() {
   super.addRegion();
   checkEventCounters();
}

protected int invalidateRegion(boolean isLocalInvalidate) {
   int numInval = super.invalidateRegion(isLocalInvalidate);
   checkEventCounters();
   return numInval;
}

protected int destroyRegion(boolean isLocalDestroy) {
   int numDestroy = super.destroyRegion(isLocalDestroy);
   checkEventCounters();
   return numDestroy;
}

/** Check all counters from EventCountersBB. This verifies that 
 *  events were invoked as expected and that no entry or local 
 *  events occurred.
 */
protected void checkEventCounters() {
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
   long totalRegDestroys = numLocalRegionDestroy + numRegionDestroy;
   long totalRegInval = numLocalRegionInval + numRegionInval;
   long numClose = counters.read(EventBB.NUM_CLOSE);

   // Entry counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   ArrayList al = new ArrayList();
      // afterCreate counters
         al.add(new ExpCounterValue("numAfterCreateEvents_isDist", numCreate));
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", numCreate)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", 
                                    "numAfterCreateEvents_isNotRemote", numCreate));
         al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", numCreate)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", numCreate)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", numCreate)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0)); 
         al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", numCreate)); 

      // afterDestroy counters
         al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", numDestroy));
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", numDestroy)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", 
                                    "numAfterDestroyEvents_isNotRemote", numDestroy));
         al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", numDestroy)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", numDestroy)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", numDestroy)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0)); 
         al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", numDestroy)); 

      // afterInvalidate counters
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", numInval));
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", numInval)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", 
                                    "numAfterInvalidateEvents_isNotRemote", numInval));
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", numInval)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", numInval)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", numInval)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0)); 
         al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", numInval)); 

      // afterUpdate counters
         al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", numUpdate));
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", numUpdate)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", 
                                   "numAfterUpdateEvents_isNotRemote", numUpdate));
         al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", numUpdate)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", numUpdate)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", numUpdate)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0)); 
         al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", numUpdate)); 

      // afterRegionDestroy counters
         al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", numRegionDestroy));
         al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", numLocalRegionDestroy)); 
         al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", (numRegionDestroy + numLocalRegionDestroy)));
         al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 
                                    "numAfterRegionDestroyEvents_isNotRemote", (numRegionDestroy + numLocalRegionDestroy)));

      // afterRegionInvalidate counters
         al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isDist", numRegionInval));
         al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotDist", numLocalRegionInval)); 
         al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotExp", (numRegionInval + numLocalRegionInval)));
         al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isRemote", 
                                    "numAfterRegionInvalidateEvents_isNotRemote", numRegionInval + numLocalRegionInval));

      // afterRegionCreate counters
      // allow 1 per VM for initial setup of region
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isDist", 0));
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotDist", numRegionCreate)); 
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isExp", 0)); 
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotExp", numRegionCreate));
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isRemote", 0));
         al.add(new ExpCounterValue("numAfterRegionCreateEvents_isNotRemote", numRegionCreate));

      if (inEndTask) { // numClose should be a min value
         long[] counterValue = new long[2];
         counterValue[0] = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numClose);
         counterValue[1] = OperationCountersBB.getBB().getSharedCounters().read(OperationCountersBB.numClose);
         if (numClose > counterValue[0] || numClose > counterValue[1])
            throw new TestException("Expected EventCountersBB.numClose " + counterValue + 
                  " to be > " + numClose);
      } else {
         // numClose counters
            al.add(new ExpCounterValue("numClose", numClose));
      }

   EventCountersBB.getBB().checkEventCounters(al);

   OperationCountersBB.getBB().checkEventCounters(al);

}

}
