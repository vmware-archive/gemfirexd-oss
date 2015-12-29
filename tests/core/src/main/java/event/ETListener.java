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
//import hydra.blackboard.SharedCounters;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;

/** Event Test Listener. 
 *  Does validation of callback objects and that the event is invoked in the
 *  VM where it was created.
 *
 * @see EventBB#isSerialExecution
 * @see EventCountersBB
 *
 * @author Lynn Gallinat
 * @since 3.0
 */
public class ETListener extends util.AbstractListener implements CacheListener, Declarable {

protected boolean isCarefulValidation;
private boolean useTransactions;

/** noArg constructor which sets isCarefulValidation based on the tests
 *  setting of serialExecution
 */
public ETListener() {
   TestConfig config = TestConfig.getInstance();
   ConfigHashtable tab = config.getParameters();
   this.isCarefulValidation = tab.booleanAt(Prms.serialExecution);
   this.useTransactions = tab.booleanAt(EventPrms.useTransactions, false);
}

/** Create a new listener and specify whether the test is doing careful validation.
 *
 *  @param isCarefulValidation true if the test is doing careful validate (serial test)
 *         false otherwise.
 */
public ETListener(boolean isCarefulValidation) {
   this.isCarefulValidation = isCarefulValidation;
}

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);

   // Don't increment counters for SEARCH_CREATE.  We haven't bumped the
   // expected counters for these (because we simply did a get, which used
   // netsearch to get (and create) a value for us.  In addition, afterCreate
   // is only called locally for SEARCH_CREATE (it doesn't get distributed
   // to other VMs).
   if (!event.getOperation().equals(Operation.SEARCH_CREATE)) {
      incrementAfterCreateCounters(event, EventCountersBB.getBB());
   }
   checkVM();

   String key = (String)event.getKey();
   if (!key.equals("invalidateMe")) {
      checkCallback(event, EventTest.createCallbackPrefix);
   }
   validateProxyEventBehavior(event);
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   incrementAfterDestroyCounters(event, EventCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.destroyCallbackPrefix);
   validateProxyEventBehavior(event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event, EventCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.invalidateCallbackPrefix);
   validateProxyEventBehavior(event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   incrementAfterUpdateCounters(event, EventCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.updateCallbackPrefix);
   validateProxyEventBehavior(event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   // Don't increment the counters for the regionDestroy that comes with 
   // each CLOSE.  Don't check the callback (its null in this case).
   if (!event.getOperation().isClose()) {
      incrementAfterRegionDestroyCounters(event, EventCountersBB.getBB());
      checkCallback(event, EventTest.regionDestroyCallbackPrefix);
   }
   checkVM();
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   incrementAfterRegionInvalidateCounters(event, EventCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.regionInvalidateCallbackPrefix);
}
public void afterRegionClear(RegionEvent event) {
  logCall("afterRegionClear", event);

  }

public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);
  incrementAfterRegionCreateCounters(event, EventCountersBB.getBB());
  checkVM();
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
  incrementAfterRegionLiveCounters(event, EventCountersBB.getBB());
  checkVM();
}

public void close() {
   logCall("close", null);
   EventCountersBB.getBB().getSharedCounters().increment(EventCountersBB.numClose);
   checkVM();
}

/** Utility method to validate that EntryEvents.getOldValue() returns 
 *  NOT_AVAILALBLE from Proxy (Cacheless) regions (for remote events) and null from
 *  locally generated EntryEvents.
 *
 *  @param event EntryEvent
 *  @throws TestException if validation fails.
 */
protected void validateProxyEventBehavior(EntryEvent event) {

   DataPolicy dataPolicy = null;
   try {
      dataPolicy = event.getRegion().getAttributes().getDataPolicy();
   } catch (RegionDestroyedException e) {
      if (isCarefulValidation) {
         throwException("Listener caught RegionDestroyedException while processing event " + event);
      } 
      else { // region destroyed expected in concurrent tests 
         return;
      }
   } 
   catch (CancelException e) {
      // allow & return (can happen if shutdown occurs while listener processing)
      return;
   }

   if (dataPolicy.isEmpty()) {
     if (event.isOriginRemote()) {
       if (!event.isOldValueAvailable()) {
         // getOldValue will return null when isOldValueAvailable returns false
         if (event.getOldValue() != null) {
            throwException("Listener detected remote Event with getOldValue = " + event.getOldValue() + " with DataPolicy " + dataPolicy + ".  Expected getOldValue null when event.isOldValueAvailable() is false");
         }
       }
     } else {  // local event ==> Proxy DataPolicies should return null for getOldValue
       // transactional updates are likely to have an oldValue (since tx state
       // is maintained, even for proxy regions).
       if (!useTransactions) {
         if (event.getOldValue()!=null) {
           throwException("Listener detected local Event with getOldValue = " + event.getOldValue() + " with DataPolicy " + dataPolicy + " expected getOldValue = null");
         }
       }
     }
   }
}

/** Check that this method is running in the same VM where the listener
 *  was created. Log an error if any problems are detected.
 *
 */
protected void checkVM() {
   int myPID = ProcessMgr.getProcessId();
   if (whereIWasRegistered != myPID) {
      String errStr = "Expected event to be invoked in VM " + whereIWasRegistered +
         ", but it was invoked in " + myPID + ": " + toString() + "; see system.log for call stack";
      hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
   }
}
 
/** Check that the callback object is expected. Log an error if any problems are detected.
 *
 *  @param event The event object.
 *  @param expectedCallbackPrefix - The expected prefix on the callback object (String).
 */
protected void checkCallback(CacheEvent event, String expectedCallbackPrefix) {

   // In transactional tests, the CacheListeners in Remote VMs do not get the 
   // callbackArg (it is null).  This is as designed.
   //if (useTransactions && event.isOriginRemote()) {
   // todo@lhughes -- all remote listeners or just when part of tx?
   if (event.isOriginRemote()) {
     return;
   }

   // Any 'gets' which result in a LOAD will have a null callback, ignore
   if (event.getOperation().equals(Operation.SEARCH_CREATE)) {
     return;
   }

   String callbackObj = (String)event.getCallbackArgument();

   if (isCarefulValidation) {
      if (callbackObj == null) {
         String errStr = "Callback object is " + TestHelper.toString(callbackObj);
         hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
         throw new TestException(errStr);
      }
      if (!callbackObj.startsWith(expectedCallbackPrefix)) {
         String errStr = "Expected " + expectedCallbackPrefix + ", but callback object is " + 
                TestHelper.toString(callbackObj);
         hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
         Log.getLogWriter().info(errStr);
         throw new TestException(errStr);
      }
   }

   if (callbackObj != null) {
      // verify memberID in callbackObj matches what's held in event
      String memberIdString = "memberId=";
      int index = callbackObj.indexOf(memberIdString, 0);
      // Some tests (query) don't assign memberId to callback, so allow for this
      if (index > 0) {
         index += memberIdString.length();
         memberIdString = callbackObj.substring(index);
         Log.getLogWriter().info("memberId from callbackObj = <" + memberIdString + "> memberId from event = <" + event.getDistributedMember() + ">");
         if (!memberIdString.equals(event.getDistributedMember().toString())) {
            String errStr = "Expected <" + event.getDistributedMember() + ">, but callback object contains <" + memberIdString + ">";
   
            hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
            aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
            Log.getLogWriter().info(errStr);
            throw new TestException(errStr);
         }
      }

      // verify isRemote flag
      int myPID = ProcessMgr.getProcessId();
      // An easy workaround to the problem where some pid strings might be equal except
      // for a final digit, as in "pid 501" and "pid 5012". Also, some tests append a space
      // plus more text to the callback string while others end the callback string after
      // the pid. So, appending a space to the callback string here and checking for the 
      // desired pid plus a space should avoid this problem, even if it's not elegant.
      String tmpCallbackObj = callbackObj + " ";
      boolean eventProducedInThisVM = tmpCallbackObj.indexOf("pid " + myPID + " ") >= 0;
      boolean isRemote = event.isOriginRemote();
      if (isRemote == eventProducedInThisVM) { // error with isRemote
         String errStr = "Unexpected event.isOriginRemote() = " + isRemote + ", myPID = " + myPID +
                ", callbackObj showing origination VM = " + callbackObj;
         hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr);
         Log.getLogWriter().info(errStr);
         throw new TestException(errStr);
      }
   }
}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to EventBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
