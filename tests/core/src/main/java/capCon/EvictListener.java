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
package capCon; 

import java.util.*;
import util.*;
import hydra.Log;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

public class EvictListener extends util.AbstractListener implements CacheListener {

private hydra.blackboard.SharedCounters counters = CapConBB.getBB().getSharedCounters();

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   incrementAfterCreateCounters(event, EventCountersBB.getBB());
   try {
      doAfterCreate(event);
   } 
   catch (VirtualMachineError err) {
     SystemFailure.initiateFailure(err);
     // If this ever returns, rethrow the error.  We're poisoned
     // now, so don't let this thread continue.
     throw err;
   }
   catch (Throwable aThrowable) {
     // Whenever you catch Error or Throwable, you must also
     // catch VirtualMachineError (see above).  However, there is
     // _still_ a possibility that you are dealing with a cascading
     // error condition, so you also need to check to see if the JVM
     // is still usable:
     SystemFailure.checkFailure();
      String errStr = aThrowable.toString() + TestHelper.getStackTrace(aThrowable);
      CapConBB.getBB().getSharedMap().put(TestHelper.EVENT_ERROR_KEY, errStr);
   }
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   incrementAfterDestroyCounters(event, EventCountersBB.getBB());
   Log.getLogWriter().info("Beginning afterDestroy event: " + evictedCountersToString());
   try {
      doAfterDestroy(event);
   } 
   catch (VirtualMachineError err) {
     SystemFailure.initiateFailure(err);
     // If this ever returns, rethrow the error.  We're poisoned
     // now, so don't let this thread continue.
     throw err;
   }
   catch (Throwable aThrowable) {
     // Whenever you catch Error or Throwable, you must also
     // catch VirtualMachineError (see above).  However, there is
     // _still_ a possibility that you are dealing with a cascading
     // error condition, so you also need to check to see if the JVM
     // is still usable:
     SystemFailure.checkFailure();
      String errStr = aThrowable.toString() + TestHelper.getStackTrace(aThrowable);
      CapConBB.getBB().getSharedMap().put(TestHelper.EVENT_ERROR_KEY, errStr);
   } finally {
      Log.getLogWriter().info("Done in EvictListener.afterDestroy, " + evictedCountersToString());
   }
}

public void afterInvalidate(EntryEvent event) {
//   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event, EventCountersBB.getBB());
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   incrementAfterUpdateCounters(event, EventCountersBB.getBB());
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   incrementAfterRegionDestroyCounters(event, EventCountersBB.getBB());
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   incrementAfterRegionInvalidateCounters(event, EventCountersBB.getBB());
}
public void afterRegionClear( RegionEvent event) {
   logCall("afterRegionClear", event);
}
//Nand Kishor : Added afterRegionCreate callback
public void afterRegionCreate(RegionEvent event) {
   logCall("afterRegionCreate", event);
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void close() {
   logCall("close", null);
   incrementCloseCounter(EventCountersBB.getBB());
}

private void doAfterCreate(EntryEvent event) throws CacheException {
   Region aRegion = event.getRegion(); 
   int numKeys = aRegion.keys().size();
   Log.getLogWriter().info("In EvictListener.doAfterCreate: region " + TestHelper.regionToString(aRegion, false) + " num keys is " + numKeys);
}

private synchronized void doAfterDestroy(EntryEvent event) {
   Region aRegion = event.getRegion(); 
   int numKeys = aRegion.keys().size();
   Log.getLogWriter().info("In EvictListener.doAfterDestroy: region " + TestHelper.regionToString(aRegion, false) + " num keys is " + numKeys);
   long highestEvictedNameID = getHighestEvictedNameID();
   long highestSeqEvictedNameID = counters.read(CapConBB.HIGHEST_SEQ_NAME_ID);
   Object key = event.getKey();
   long currNameID = NameFactory.getCounterForName(key);
   if (currNameID <= highestSeqEvictedNameID) {
      String aStr = "Destroying an object that was previously evicted: " + key + " " +
            evictedCountersToString();
      Log.getLogWriter().info(aStr);
      throw new TestException(aStr);
   } 

   // see if this object is too far away from the least recently used
   long diff = Math.abs(highestEvictedNameID - currNameID);
   counters.setIfLarger(CapConBB.MAX_LRU_DIFF, diff);
   Log.getLogWriter().info("highestEvictedNameID is " + highestEvictedNameID + ", currNameID is " + currNameID + ", diff is " + diff);
   if (diff > CapConTest.testInstance.LRUAllowance) {
      String errStr = "Evicted object is outside of LRUAllowance " + 
             CapConTest.testInstance.LRUAllowance + 
             ", difference between this object and the highest evicted object: " + diff + 
             " " + evictedCountersToString();
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr); 
   }

   if (currNameID == highestSeqEvictedNameID + 1) { 
      // evicted name was next in line to seq counter
      Log.getLogWriter().info("currNameID " + currNameID + " is next in line to seq counter");
      highestSeqEvictedNameID = counters.incrementAndRead(CapConBB.HIGHEST_SEQ_NAME_ID);
      Log.getLogWriter().info("highestSeqEvictedNameID is now  " + highestSeqEvictedNameID);

      // now look through the out-of-order set for sequential name IDs
      hydra.blackboard.SharedMap aMap = CapConBB.getBB().getSharedMap();
      Set aSet = (Set)aMap.get(CapConBB.OUT_OF_ORDER_SET);
      aSet.remove(key);
      Iterator it = aSet.iterator();
      List removeList = new ArrayList();
      while (it.hasNext()) {
          Object name = it.next();
          long nameID = NameFactory.getCounterForName(name);
          if (nameID == highestSeqEvictedNameID + 1) {
             highestSeqEvictedNameID = counters.incrementAndRead(CapConBB.HIGHEST_SEQ_NAME_ID);
             Log.getLogWriter().info("Found another sequential name in the out-of-order set " + name + 
                 " highestSeqEvictedNameID is now  " + highestSeqEvictedNameID);
             removeList.add(name);
             it = aSet.iterator();
          }
      }

      // remove those names that are now in order
      for (int i = 0; i < removeList.size(); i++) {
         Object name = removeList.get(i);
         Log.getLogWriter().info("Removing " + name + " from the out-of-order set");
         aSet.remove(name);
      }    
      aMap.put(CapConBB.OUT_OF_ORDER_SET, aSet); 
   } else { // currNameID is > highestSeqEvictedNameID+1; object destroyed is out of LRU order
      Log.getLogWriter().info("Adding " + key + " to the out-of-order set");
      hydra.blackboard.SharedMap aMap = CapConBB.getBB().getSharedMap();
      Set aSet = (Set)aMap.get(CapConBB.OUT_OF_ORDER_SET);
      aSet.add(key);
      aMap.put(CapConBB.OUT_OF_ORDER_SET, aSet);
      if (aSet.size() > CapConTest.testInstance.LRUAllowance) {
         throw new TestException("Number of elements that are out of order " + aSet.size() + 
                   " is more than the LRUAllowance of " + CapConTest.testInstance.LRUAllowance);
      }
   }
}

private String evictedCountersToString() {
   StringBuffer aStr = new StringBuffer();
   long highestSeqNameID = counters.read(CapConBB.HIGHEST_SEQ_NAME_ID);
   long highestNameID = getHighestEvictedNameID();
   aStr.append("Highest sequentially evicted nameID: " + highestSeqNameID + "\n");
   aStr.append("Highest evicted nameID: " + highestNameID + "\n");
   hydra.blackboard.SharedMap aMap = CapConBB.getBB().getSharedMap();
   Set aSet = (Set)aMap.get(CapConBB.OUT_OF_ORDER_SET);
   Iterator it = aSet.iterator();
   while (it.hasNext()) {
      aStr.append("   Out of order: " + it.next() + "\n");
   }
   return aStr.toString();
}

private long getHighestEvictedNameID() {
   long highest = counters.read(CapConBB.HIGHEST_SEQ_NAME_ID);
   hydra.blackboard.SharedMap aMap = CapConBB.getBB().getSharedMap();
   Set aSet = (Set)aMap.get(CapConBB.OUT_OF_ORDER_SET);
   Iterator it = aSet.iterator();
   while (it.hasNext()) {
      long nameID = NameFactory.getCounterForName(it.next());
      highest = Math.max(highest, nameID);
   }
   return highest;
}

}
