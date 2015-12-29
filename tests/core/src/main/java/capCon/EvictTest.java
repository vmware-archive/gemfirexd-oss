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

import util.*;
import hydra.*;
//import hydra.blackboard.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;

public class EvictTest {

static boolean scopeIsLocal;

static void initEvictTest(Region workRegion) {
   Scope scope = workRegion.getAttributes().getScope();
   scopeIsLocal = scope.isLocal();
   Log.getLogWriter().info("Setting scopeIsLocal to " + scopeIsLocal + ", scope is " + scope);
}

static void serialEvictTest(CapConTest testInstance, Region workRegion) {
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   if (exeNum == 0) {
      long destroyCounter = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterDestroyEvents_isNotExp);
      if (destroyCounter != 0)
         throw new TestException("Expected no destroys to have occurred on the first serial execution, but execution number is " + exeNum + " and numAfterDestroyEvents_isNotExp is " + destroyCounter);
   }
   hydra.blackboard.SharedCounters counters = EventCountersBB.getBB().getSharedCounters();
   long numEvicted = 0;
   Set beforeKeySet;
   Set afterKeySet;

   // before the add
   long before_afterCreateEvents = counters.read(EventCountersBB.numAfterCreateEvents_isNotExp);
   long before_afterDestroyEvents = counters.read(EventCountersBB.numAfterDestroyEvents_isNotExp);
   beforeKeySet = new HashSet(workRegion.keys());
   Log.getLogWriter().info("Before add, keySet size is " + beforeKeySet.size() +
       "before_afterCreateEvents is " + before_afterCreateEvents +
       ", before_afterDestroyEvents is " + before_afterDestroyEvents +
       ", scopeIsLocal is " + scopeIsLocal);

   // do the add
   Object[] nameAddedArr = null;
   if (testInstance.useTransactions) { // add in a transaction
      TxHelper.begin();
      int numToAddInTx = TestConfig.tab().intAt(CapConPrms.numToAddInTx);
      Log.getLogWriter().info("Adding " + numToAddInTx+ " entries to region during transaction");
      nameAddedArr = new Object[numToAddInTx];
      for (int i = 0; i < numToAddInTx; i++) {
         Object nameAdded = (testInstance.addEntry())[0];
         nameAddedArr[i] = nameAdded;
      }
      beforeKeySet = new HashSet(workRegion.keys());
      Log.getLogWriter().info("Before commit, keySet size is " + beforeKeySet.size());
      TxHelper.commitExpectSuccess();
      afterKeySet = new HashSet(workRegion.keys());
      if (afterKeySet.size() <= beforeKeySet.size())
         numEvicted = beforeKeySet.size() - afterKeySet.size(); 
   } else {
      nameAddedArr = new Object[1];
      nameAddedArr[0] = (testInstance.addEntry())[0];
      afterKeySet = new HashSet(workRegion.keys());
      if (afterKeySet.size() <= beforeKeySet.size())
         numEvicted = beforeKeySet.size() - afterKeySet.size() + 1; 
   }

   // after the add
   Log.getLogWriter().info("After add, keySet size is " + afterKeySet.size() +
                           ", numEvicted is " + numEvicted);

   // wait for afterCreate events; each VM will count an event if scope is distributed
   long expectedAfterCreateEvents = 0;
   if (scopeIsLocal) {
      expectedAfterCreateEvents = before_afterCreateEvents + nameAddedArr.length;  // only this VM will count an event
      Log.getLogWriter().info("scopeIsLocal is " + scopeIsLocal + ", incrementing expectedAfterCreateEvents by one, now is " + expectedAfterCreateEvents);
   } else {
      expectedAfterCreateEvents = before_afterCreateEvents + 
         (testInstance.numVMs * nameAddedArr.length);  // each VM will count an event
      Log.getLogWriter().info("scopeIsLocal is " + scopeIsLocal + ", incrementing expectedAfterCreateEvents by "
          + testInstance.numVMs + ", now is " + expectedAfterCreateEvents);
   }
   TestHelper.waitForCounter(EventCountersBB.getBB(),
                             "EventCountersBB.numAfterCreateEvents_isNotExp",
                             EventCountersBB.numAfterCreateEvents_isNotExp,
                             expectedAfterCreateEvents,
                             true,
                             60000);

   // wait for afterDestroy events; destroys are cause by LRU eviction and are local destroys
   long expectedAfterDestroyEvents = before_afterDestroyEvents + numEvicted;
   TestHelper.waitForCounter(EventCountersBB.getBB(),
                             "EventCountersBB.numAfterDestroyEvents_isNotExp",
                             EventCountersBB.numAfterDestroyEvents_isNotExp,
                             expectedAfterDestroyEvents,
                             true,
                             60000);

   // check the evictions
   for (int i = 0; i < nameAddedArr.length; i++) {
      checkEvictions(workRegion, beforeKeySet, afterKeySet, nameAddedArr[i], testInstance);
   }

   if (testInstance instanceof LRUEvict) {
      ((LRUEvict)testInstance).verifyNumKeys();
   } else if (testInstance instanceof MemLRUEvict) {
      MemLRUEvict aTest = (MemLRUEvict)testInstance;
      aTest.verifyMemCapacity(aTest.memLRUParams.getTotalAllowableBytes());
   } else
      throw new TestException("Unknown testInstance " + testInstance);
   CapConTest.checkForEventError();
}

static void checkEvictions(Region workRegion, Set beforeKeySet, Set afterKeySet, Object nameAdded, CapConTest testInstance) {
   // is the new name in the afterKeySet?
   boolean newNameIsPresent = afterKeySet.contains(nameAdded);
   if (!newNameIsPresent)
      throw new TestException("After adding " + nameAdded + ", it is not present in " + TestHelper.regionToString(workRegion, false));
   Log.getLogWriter().info(nameAdded + " is present in the afterKeySet");

   // remove the new name from the after name set
   Set afterTmpSet = new HashSet(afterKeySet);
   afterTmpSet.remove(nameAdded);
   if (afterTmpSet.contains(nameAdded))
      throw new TestException("After removing " + nameAdded + " it is still in " + afterTmpSet);

   // see if any unexpected new objects are in the afterNameSet
   boolean containsAll = beforeKeySet.containsAll(afterTmpSet);
   if (!containsAll) {
      StringBuffer aStr = new StringBuffer();
      aStr.append("Before key set: ");
      int count = 0;
      for (Iterator it = beforeKeySet.iterator(); it.hasNext(); count++) {
         aStr.append(it.next() + " ");
         if ((count % 10) == 0)
            aStr.append("\n   ");
      }
      aStr.append("After key set:\n");
      count = 0;
      for (Iterator it = afterTmpSet.iterator(); it.hasNext(); count++) {
         aStr.append(it.next() + " ");
         if ((count % 10) == 0)
            aStr.append("\n   ");
      }
      throw new TestException("Expected beforeKeySet to be a superset of afterTmpSet: \n" + aStr);
   }

   // find the evicted objects and log them
   Set evictedSet = new HashSet(beforeKeySet);
   evictedSet.removeAll(afterKeySet);
   Log.getLogWriter().info("evictedSet is " + evictedSet);
}

}
