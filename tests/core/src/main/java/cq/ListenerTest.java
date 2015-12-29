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
package cq;

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/**
 * A Hydra test that concurrently performs a number of entry
 * operations while adding and removing CQ listeners.  
 * Verifies CQListeners are invoked when expected and in the correct order.
 *
 * @see ListenerPrms
 * @see ListenerBB
 *
 * @author lhughes
 * @since 5.2
 */
public class ListenerTest extends CQEventTest {

static protected final int ADD_LISTENER = 1;
static protected final int REMOVE_LISTENER = 2;
static protected final int INIT_LISTENERS = 3;

// In the serial version of the tests, the thread executing the listener
// and entry operations sets targetVm to true.  The listeners only count
// ListenerBB.NUM_LISTENER_INVOCATIONS in the targetVM.  
static public boolean targetVm = false;

// ========================================================================
// initialization methods
// ========================================================================

/**
 * Creates and {@linkplain #initialize initializes} the singleton
 * instance of <code>ListenerTest</code> in this VM.
 */
public synchronized static void HydraTask_initialize() throws CqClosedException, RegionNotFoundException {
   if (testInstance == null) {
      testInstance = new ListenerTest();
      testInstance.isBridgeClient = true;
      testInstance.initializeRegion("clientRegion");
   
      // If configured, registerInterest
      boolean registerInterest = TestConfig.tab().booleanAt(CQUtilPrms.registerInterest, false);
      if (registerInterest) { 
         testInstance.aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      }
   }
   testInstance.initializeCQ();
   testInstance.initializePrms();

   // reset CQListener using CqAttributeMutator
   QueryService qService = CacheHelper.getCache().getQueryService();
   CqQuery cq = qService.getCq( CQUtil.getCqName() );
   CqAttributesMutator mutator = cq.getCqAttributesMutator();
   mutator.initCqListeners( null );
}

// ========================================================================
// additional methods (not in EventTest) 
// ========================================================================

/** 
 *  HydraTask method to perform listener operations (add, remove, set, init),
 *  stores the expected list of listeners to be invoked in the BB and then
 *  performs a random entry operation.  Listeners add their name to the 
 *  InvokedListener List in the BB when invoked.  In serialExecution mode,
 *  the two listener lists are then compared to ensure that the listeners
 *  were invoked in the expected order.
 */
public synchronized static void HydraTask_exerciseListeners() {
   ((ListenerTest)testInstance).exerciseListeners();
}

/**
 *  see HydraTask_exerciseListeners
 */
protected void exerciseListeners() {
   long startTime = System.currentTimeMillis();

   do {
      TestHelper.checkForEventError(ListenerBB.getBB());

      CqQuery cq = CQUtil.getCQ();

      int listenerOp = getListenerOp();
      switch (listenerOp) {
         case ADD_LISTENER:
            addListener(cq);
            break;
         case REMOVE_LISTENER:
            removeListener(cq);
            break;
         case INIT_LISTENERS:
            initListener(cq);
            break;
         default:
            throw new TestException("Unknown Listener Operation " + listenerOp);
      }

      // For concurrent tests, a separate doEntryOperations task will run
      // simultaneously w/exerciseListeners
      if (this.isSerialExecution) {
         writeExpectedListenerListToBB(cq);
         clearInvokedListenerList(cq);
         ListenerBB.getBB().getSharedCounters().zero(ListenerBB.NUM_LISTENER_INVOCATIONS);
         // Let the listeners know that we want to count the events in this VM
         targetVm = true;

         // Perform an entry operation
         doEntryOperations(aRegion);

         // wait for distribution/invocation of CQListeners
         TestHelper.waitForCounter( ListenerBB.getBB(), "NUM_LISTENER_INVOCATIONS", ListenerBB.NUM_LISTENER_INVOCATIONS, getNumCqListeners(), true, 60000);

         // verify that the expected listeners were invoked (in order)
         compareListenerLists(cq);

         // reset the target flag
         targetVm = false;
      }

   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

protected int getNumCqListeners() {
   int numListeners = 0;
   CqQuery[] cqs = CacheHelper.getCache().getQueryService().getCqs();
   Log.getLogWriter().fine("getNumCqListeners: cqs = " + cqs);
   for (int i = 0; i < cqs.length; i++) {
      CqListener[] list = cqs[i].getCqAttributes().getCqListeners(); 
      if (list.length > 0) {
         Log.getLogWriter().fine("cqs[" + i + "] (" + cqs[i].getName() + ") has " + list.length + " listeners " + list);
         numListeners += list.length;
      }
   }
   Log.getLogWriter().fine("getNumCqListeners returns " + numListeners);
   return numListeners;
}

/**
 *  Return a random operation to perform on the cacheListenerList.
 */
protected int getListenerOp() {
   int op = 0;

   String operation = TestConfig.tab().stringAt(ListenerPrms.listenerOperations);
   if (operation.equals("add")) 
      op = ADD_LISTENER;
   else if (operation.equals("remove"))
      op = REMOVE_LISTENER;
   else if (operation.equals("init"))
      op = INIT_LISTENERS;
   else 
      throw new TestException("Unknown listenerOperation " + operation);

   return op;
}

/**
 *  Returns a new instance of MultiListener
 */
protected CqListener getNewListener() {
   MultiListener newListener = new MultiListener( NameFactory.getNextListenerName() );
   Log.getLogWriter().info("getNewListener() returns listener " + newListener.getName());
   return newListener;
}

/**
 *  Adds a listener to the existing listener list 
 *
 */
protected void addListener(CqQuery cq) {
   CqAttributesMutator mutator = cq.getCqAttributesMutator();
  
   // Add a random listener from the list of availableListeners
   CqListener newListener = getNewListener(); 
   mutator.addCqListener( newListener );
   Log.getLogWriter().info("After adding listener " + ((MultiListener)newListener).getName() + " new list = " + getCqListenerNames(cq));
}

/**
 *  Removes a listener from the existing listener list 
 */
protected void removeListener(CqQuery cq) {
   // From existing listeners for this cq, pick one to remove
   CqAttributes attrs = cq.getCqAttributes();
   CqListener[] assignedListeners = attrs.getCqListeners();
  
   if (assignedListeners.length == 0) {
      Log.getLogWriter().info("removeListeners invoked, but no assigned listeners to remove.  Returning.");
      return;
   }

   int randInt = TestConfig.tab().getRandGen().nextInt(0, assignedListeners.length-1);

   Log.getLogWriter().info("Removing listener " + ((MultiListener)assignedListeners[randInt]).getName() + " from list of assignedCqListeners " + getCqListenerNames(cq));

   CqAttributesMutator mutator = cq.getCqAttributesMutator();
   mutator.removeCqListener( assignedListeners[randInt] );
   Log.getLogWriter().info("After removing listener " + ((MultiListener)assignedListeners[randInt]).getName() + " listener list = " + getCqListenerNames(cq));
}

/**
 *  Initializes the listener list with ListenerPrms.maxListeners entries
 */
protected void initListener(CqQuery cq) {

   int maxListeners = TestConfig.tab().intAt(ListenerPrms.maxListeners, 10);
   StringBuffer aStr = new StringBuffer();

   // initialize with a minimum of 3 listeners
   int randInt = TestConfig.tab().getRandGen().nextInt(3, maxListeners-1);
   CqListener[] newListenerList = new CqListener[randInt];
   for (int i=0; i < randInt; i++) {
      newListenerList[i] = getNewListener();
      aStr.append( ((MultiListener)newListenerList[i]).getName() + ":" );
   }

   Log.getLogWriter().info("Initializing cqListeners with " + aStr);
   
   cq.getCqAttributesMutator().initCqListeners( newListenerList );
   Log.getLogWriter().info("After initCqListeners, list = " + getCqListenerNames(cq));
}

/**
 *  Writes the listener lists for the given cq out to the BB
 *
 *  @see ListenerBB.ExpectedListeners
 */
protected void writeExpectedListenerListToBB(CqQuery cq) {
   String expected = getCqListenerNames(cq);
   String key = ListenerBB.ExpectedListeners + "_" + cq.getName();
   ListenerBB.getBB().getSharedMap().put(key, expected);
}

/**
 *  Compares the Expected and Invoked Listener Lists in ListenerBB
 *  for the given CQ
 *
 *  @see ListenerBB.ExpectedListeners
 *  @see ListenerBB.InvokedListeners
 */
protected void compareListenerLists(CqQuery cq) {

   // ExpectedList
   String key = ListenerBB.ExpectedListeners + "_" + cq.getName();
   String expected = (String)ListenerBB.getBB().getSharedMap().get(key);
   Log.getLogWriter().info("Expected listener list (" + key + ") = " + expected);
   // Actual list of listeners invoked
   key = ListenerBB.InvokedListeners +  "_" + cq.getName();
   String invoked = (String)ListenerBB.getBB().getSharedMap().get(key);
   Log.getLogWriter().info("Invoked listener list (" + key + ") = " + invoked);

   // It's possible that we didn't do any actual operations (if there were
   // no keys to invalidate, etc.
/* todo@lhughes -- will we ever not expect a CQ event in this test?
   if (invoked.equals(""))
     return;
*/

   if (!expected.equals(invoked)) {
     StringBuffer aStr = new StringBuffer();
     aStr.append("Listeners may not have been invoked in order expected.\n");
     aStr.append("ExpectedList = " + expected + "\n");
     aStr.append("InvokedList = " + invoked + "\n");
     throw new TestException(aStr.toString());
   }
}

/**
 *  Utility method to get a string containing the names of the listeners
 *  (separated by ":") for the given region.
 */
protected String getCqListenerNames(CqQuery cq) {
   StringBuffer aStr = new StringBuffer();

   CqListener[] list = cq.getCqAttributes().getCqListeners();
   if (list == null) {
     return "";
   }

   for (int i=0; i < list.length; i++) {
     aStr.append( ((MultiListener)list[i]).getName() + ":");
   }
   return aStr.toString();
}

/**
 *  Clears the InvokedListener list in the BB (for the given CQ)
 */
protected void clearInvokedListenerList(CqQuery cq) {
   String key = ListenerBB.InvokedListeners + "_" + cq.getName();
   ListenerBB.getBB().getSharedMap().put(key, ""); 
}

}


