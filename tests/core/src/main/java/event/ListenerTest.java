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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

/**
 * A Hydra test that concurrently performs a number of cache-related
 * (both entry and region) operations while adding and removing listeners.  
 * Verifies listeners are invoked when expected and in the correct order.
 *
 * @see ListenerPrms
 * @see ListenerBB
 *
 * @author lhughes
 * @since 5.0
 */
public class ListenerTest extends EventTest {

static protected final int ADD_LISTENER = 1;
static protected final int REMOVE_LISTENER = 2;
static protected final int INIT_LISTENERS = 3;
static protected final int SET_LISTENER = 4;

// ========================================================================
// initialization methods
// ========================================================================

/**
 * Creates and {@linkplain #initialize initializes} the singleton
 * instance of <code>ListenerTest</code> in this VM.
 */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ListenerTest();
      eventTest.initialize();
   }
}

// ========================================================================
// override methods in EventTest
// ========================================================================
/**
 *  returns the number of VMs with listeners
 *
 *  In the listener test, we simply assume there is at least one listener
 *  for any region.  This method is invoked when we invalidate the region
 *  to ensure that at least some minimum number of listeners are invoked.
 *  It is not used to determine the expected counts for various events.
 */
protected int getNumVMsWithListeners() {
   return 1;
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
   //Region rootRegion = CacheUtil.getCache().getRegion(REGION_NAME);
   ((ListenerTest)eventTest).exerciseListeners();
}

/**
 *  see HydraTask_exerciseListeners
 */
protected void exerciseListeners() {
   long startTime = System.currentTimeMillis();

   if (isSerialExecution) {
      logExecutionNumber();
   }

   do {

      // randomly select the region
      Region aRegion = getRandomRegion(true);
      if (aRegion == null) {
        Log.getLogWriter().info("no regions available, returning");
        return;
      }

      Log.getLogWriter().info("Invoked exerciseListeners for region " + aRegion.getName());

      int listenerOp = getListenerOp();
      switch (listenerOp) {
         case ADD_LISTENER:
            addListener(aRegion);
            break;
         case REMOVE_LISTENER:
            removeListener(aRegion);
            break;
         case INIT_LISTENERS:
            initListener(aRegion);
            break;
         case SET_LISTENER:
            setListener(aRegion);
            break;
         default:
            throw new TestException("Unknown Listener Operation " + listenerOp);
      }

      if (isCarefulValidation) {
         writeExpectedListenerListToBB(aRegion);
         clearInvokedListenerList(aRegion.getName());
      }

      doOperation(aRegion);

      if (isCarefulValidation) {
        compareListenerLists(aRegion.getName());
      }

   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** 
 *  Perform a random region or entry operation
 *
 *  @param aRegion - targeted region
 */
protected void doOperation(Region aRegion) {

   // randomly select entry vs. region operation (90% entry operations)
   int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
                                                                                       
   if (randInt < 90) {
      doEntryOperation(aRegion);
   } else {
      doRegionOperation(aRegion);
   }
}

/**
 *  Perform a random entry operation 
 *
 *  @param aRegion - targeted region
 */
protected void doEntryOperation(Region aRegion) {

   // execute a random operation to invoke callbacks
   int whichOp = getOperation(EventPrms.entryOperations, isMirrored);
   switch (whichOp) {
      case ADD_OPERATION:
         addObject(aRegion, true);
         break;
      case INVALIDATE_OPERATION:
         invalidateObject(aRegion, false);
         break;
      case DESTROY_OPERATION:
         destroyObject(aRegion, false);
         break;
      case UPDATE_OPERATION:
         updateObject(aRegion);
         break;
      case READ_OPERATION:
         readObject(aRegion);
         break;
      case LOCAL_INVALIDATE_OPERATION:
         invalidateObject(aRegion, true);
         break;
      case LOCAL_DESTROY_OPERATION:
         destroyObject(aRegion, true);
         break;
      default: {
         throw new TestException("Unknown operation " + whichOp);
      }
   }
}

/**
 *  Perform a random region operation 
 *
 *  @param aRegion - targeted region
 */
protected void doRegionOperation(Region aRegion) {

   long numRegions = getNumNonRootRegions();
   int whichOp = getOperation(EventPrms.regionOperations, false);
   if (numRegions == 0)  // no regions other than the roots; add another
      whichOp = ADD_OPERATION;
   else if (numRegions >= maxRegions)
      whichOp = DESTROY_OPERATION;

   // Don't allow destructive region operations on root regions
   if (aRegion.getParentRegion() == null) {
      whichOp = ADD_OPERATION;
   }
   
   switch (whichOp) {
      case ADD_OPERATION:
         addRegion(aRegion);
         break;
      case DESTROY_OPERATION:
         destroyRegion(false, aRegion);
         break;
      case INVALIDATE_OPERATION:
         invalidateRegion(false, aRegion);
         break;
      case LOCAL_DESTROY_OPERATION:
         destroyRegion(true, aRegion);
         break;
      case LOCAL_INVALIDATE_OPERATION:
         invalidateRegion(true, aRegion);
         break;
      case REGION_CLOSE_OPERATION:
         closeRegion(aRegion);
         break;
      case CLEAR_OPERATION:
         clearRegion(aRegion);
         break;
      default: {
         throw new TestException("Unknown operation " + whichOp);
      }
   }
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
   else if (operation.equals("set"))
      op = SET_LISTENER;
   else 
      throw new TestException("Unknown listenerOperation " + operation);

   return op;
}

/**
 *  Returns a new instance of CacheListener for the type given by EventPrms.listeners
 */
protected CacheListener getNewListener() {
   MultiListener newListener = new MultiListener( NameFactory.getNextListenerName() );
   Log.getLogWriter().info("getNewListener() returns listener " + newListener.getName());
   return newListener;
}

/**
 *  Adds a listener to the existing listener list in the indicated Region 
 *
 *  @param aRegion - the targeted region for the listener operation 
 */
protected void addListener(Region aRegion) {
   AttributesMutator mutator = aRegion.getAttributesMutator();
  
   // Add a random listener from the list of availableListeners
   CacheListener newListener = getNewListener(); 
   Log.getLogWriter().info("adding listener " + ((MultiListener)newListener).getName() + " to existing list of " + getCacheListenerNames(aRegion));

   mutator.addCacheListener( newListener );
   Log.getLogWriter().info("After adding listener " + ((MultiListener)newListener).getName() + " new list = " + getCacheListenerNames(aRegion));
}

/**
 *  Removes a listener from the existing listener list in the indicated Region 
 *
 *  @param aRegion - the targeted region for the listener operation 
 */
protected void removeListener(Region aRegion) {
   // From existing listeners for this region, pick one to remove
   RegionAttributes ratts = aRegion.getAttributes();
   CacheListener[] assignedListeners = ratts.getCacheListeners();
  
   if (assignedListeners.length == 0) {
      Log.getLogWriter().info("removeListeners invoked, but no assigned listeners to remove.  Returning.");
      return;
   }

   int randInt = TestConfig.tab().getRandGen().nextInt(0, assignedListeners.length-1);

   Log.getLogWriter().info("Removing listener " + ((MultiListener)assignedListeners[randInt]).getName() + " from list of assignedCacheListeners " + getCacheListenerNames(aRegion));

   AttributesMutator mutator = aRegion.getAttributesMutator();
   mutator.removeCacheListener( assignedListeners[randInt] );
   Log.getLogWriter().info("After removing listener " + ((MultiListener)assignedListeners[randInt]).getName() + " listener list = " + getCacheListenerNames(aRegion));
}

/**
 *  Initializes the listener list with ListenerPrms.maxListeners entries
 *
 *  @param aRegion - the targeted region for the listener operation 
 */
protected void initListener(Region aRegion) {

   int maxListeners = TestConfig.tab().intAt(ListenerPrms.maxListeners, 10);
   StringBuffer aStr = new StringBuffer();

   // initialize with a minimum of 3 listeners
   int randInt = TestConfig.tab().getRandGen().nextInt(3, maxListeners-1);
   CacheListener[] newListenerList = new CacheListener[randInt];
   for (int i=0; i < randInt; i++) {
      newListenerList[i] = getNewListener();
      aStr.append( ((MultiListener)newListenerList[i]).getName() + ":" );
   }

   Log.getLogWriter().info("Initializing cacheListeners with " + aStr);
   aRegion.getAttributesMutator().initCacheListeners( newListenerList );
   Log.getLogWriter().info("After initCacheListeners, list = " + getCacheListenerNames(aRegion));
}

/**
 *  Initializes the listener list with a single listener (set vs. init)
 *
 *  @param aRegion - the targeted region for the listener operation 
 */
protected void setListener(Region aRegion) {
   AttributesMutator mutator = aRegion.getAttributesMutator();

   Log.getLogWriter().info("Clearing existing listenerList of " + getCacheListenerNames(aRegion));

   // Clear out existing listeners (to avoid IllegalStateException if > 1)
   mutator.initCacheListeners( null );
   Log.getLogWriter().info("After init (with null array) list = " + getCacheListenerNames(aRegion));

   CacheListener newListener = getNewListener();
   Log.getLogWriter().info("setListener calling setCacheListener with " + ((MultiListener)newListener).getName());

   // Add a random listener from the list of availableListeners
   mutator.setCacheListener( newListener );
   Log.getLogWriter().info("After setCacheListener cacheListenerList = " + getCacheListenerNames(aRegion));
}

/**
 *  Writes the listener list for the indicated region out to the BB
 *
 *  @param aRegion - the targeted region
 *
 *  @see ListenerBB.ExpectedListeners
 */
protected void writeExpectedListenerListToBB(Region aRegion) {
   String expected = getCacheListenerNames(aRegion);

   String clientName = System.getProperty( "clientName" );
   String key = ListenerBB.ExpectedListeners + clientName + "_" + aRegion.getName();
   ListenerBB.getBB().getSharedMap().put(key, expected);
}

/**
 *  Compares the Expected and Invoked Listener Lists in ListenerBB
 *
 *  @param regionName - name of targeted region
 *
 *  @see ListenerBB.ExpectedListeners
 *  @see ListenerBB.InvokedListeners
 */
protected void compareListenerLists(String regionName) {
   String clientName = System.getProperty( "clientName" );
   String key = ListenerBB.ExpectedListeners + clientName + "_" + regionName;
   String expected = (String)ListenerBB.getBB().getSharedMap().get(key);
   Log.getLogWriter().info("Expected listener list (" + key + ") = " + expected);
   key = ListenerBB.InvokedListeners + clientName + "_" + regionName;
   String invoked = (String)ListenerBB.getBB().getSharedMap().get(key);
   Log.getLogWriter().info("Invoked listener list (" + key + ") = " + invoked);

   // It's possible that we didn't do any actual operations (if there were
   // no keys to invalidate, etc.
   if (invoked.equals(""))
     return;

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
protected String getCacheListenerNames(Region aRegion) {
   StringBuffer aStr = new StringBuffer();

   CacheListener[] list = aRegion.getAttributes().getCacheListeners();

   for (int i=0; i < list.length; i++) {
     aStr.append( ((MultiListener)list[i]).getName() + ":");
   }
   return aStr.toString();
}

/**
 *  Clears the InvokedListener list in the BB.
 *
 *  @param regionName - name of targeted region (for cacheListener & cache Operations)
 */
protected void clearInvokedListenerList(String regionName) {
   String clientName = System.getProperty( "clientName" );
   String key = ListenerBB.InvokedListeners + clientName + "_" + regionName;
   ListenerBB.getBB().getSharedMap().put(key, ""); 
}

}


