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
import diskReg.DiskRegUtil;
//import hydra.blackboard.SharedCounters;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.Token;

/** Event Test CacheWriter.
 *  Does validation of callback objects and that the event is invoked in the
 *  VM where it was created.
 *
 * @see EventBB#isSerialExecution
 * @see WriterCountersBB
 *
 * @author Lynn Gallinat and lhughes
 * @since 5.0
 */
public class ETWriter extends util.AbstractWriter implements CacheWriter, Declarable {

private boolean isCarefulValidation;
private boolean useTransactions;
                                                                                      
/** noArg constructor which sets isCarefulValidation based on the tests
 *  setting of serialExecution
 */
public ETWriter() {
   TestConfig config = TestConfig.getInstance();
   ConfigHashtable tab = config.getParameters();
   this.isCarefulValidation = tab.booleanAt(Prms.serialExecution);
   this.useTransactions = tab.booleanAt(EventPrms.useTransactions, false);
}

/** Create a new writer and specify whether the test is doing careful validation.
 *
 *  @param isCarefulValidation true if the test is doing careful validate (serial test)
 *         false otherwise.
 */
public ETWriter(boolean isCarefulValidation) {
   this.isCarefulValidation = isCarefulValidation;
}

//==============================================================================
// implementation CacheWriter methods
public void beforeCreate(EntryEvent event) {
   logCall("beforeCreate", event);
   incrementBeforeCreateCounters(event, WriterCountersBB.getBB());
   checkVM();

   String sKey = (String)event.getKey();
   if (!sKey.equals("invalidateMe")) {
      checkCallback(event, EventTest.createCallbackPrefix);
   }

   compareValueInVM("beforeCreate", event);
}

public void beforeDestroy(EntryEvent event) {
   logCall("beforeDestroy", event);
   incrementBeforeDestroyCounters(event, WriterCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.destroyCallbackPrefix);

   compareValueInVM("beforeDestroy", event);
}

public void beforeUpdate(EntryEvent event) {
   logCall("beforeUpdate", event);
   incrementBeforeUpdateCounters(event, WriterCountersBB.getBB());
   checkVM();
   checkCallback(event, EventTest.updateCallbackPrefix);

   compareValueInVM("beforeUpdate", event);
}

public void beforeRegionDestroy(RegionEvent event) {
   logCall("beforeRegionDestroy", event);
   incrementBeforeRegionDestroyCounters(event, WriterCountersBB.getBB());
   checkCallback(event, EventTest.regionDestroyCallbackPrefix);
   checkVM();

   // Don't check for region access when regionDestroy is part of CACHE_CLOSE
   // or if we're executing region operations concurrently
   if (!event.getOperation().equals(Operation.CACHE_CLOSE) && (isCarefulValidation == true)) {
      final Region region = event.getRegion();

      try {
        region.size();  // Execute a region operation to ensure still available
      } catch (Exception e) {
        throwException("ETWriter: beforeRegionDestroy() caught " + e + " during beforeRegionDestroy");
      }
   }
}

public void beforeRegionClear(RegionEvent event) {
   logCall("beforeRegionClear", event);

   // do not do validation if we're executing region operations concurrently
   if (isCarefulValidation == false) {
      return;
   }

   final Region region = event.getRegion();

   try {
      region.size();  // Execute a region operation to ensure still available
   } catch (Exception e) {
      throwException("ETWriter: beforeRegionClear() caught " + e + " during beforeRegionClear");
   }
}

public void close() {
   logCall("close", null);
}


/** For the given EntryEvent, compare the event.oldValue() against the actual
 *  value in the cache.  Throw an exception if an unexpected value is encountered.
 */
private void compareValueInVM(String callbackMethod, EntryEvent event) {

   // cannot validate event values with concurrent execution
   if (isCarefulValidation == false) {
      return;
   }
  
   Object key = event.getKey();
   Object expectedValue = event.getOldValue();
   Region region = event.getRegion();

   Object actualValue = DiskRegUtil.getValueInVM(region, key);

   // We can't actually do these checks if we're in a transaction (the oldValue won't
   // be in our tx state (which is the view we'll have at the time) and oldValue (fromVM)
   // will be null (as designed).
   if (useTransactions) {
      return;
   }

   String sActualValue = (actualValue == null)?"null":TestHelper.toString(actualValue);
   String sExpectedValue = (expectedValue == null)?"null":TestHelper.toString(expectedValue);
   Log.getLogWriter().info("ETWriter: " + callbackMethod + "(): comparing expectedValue " + sExpectedValue + " against actualValue " + sActualValue + " for key = " + key);

   String myException = null;
   if (expectedValue == null) {
     if (!event.isOldValueAvailable()) {
       if (actualValue == null) { // good this is expected so why log?
         Log.getLogWriter().info("ETWriter: " + callbackMethod + "() expected " + sExpectedValue + " but getValueInVM() returned null");
       } else {
         myException = "ETWriter: " + callbackMethod + "() since oldValue was unavailable for key " + key + " expected actualValue to be null but instead found value of " + sActualValue;
       }
     } else if ((actualValue != null) && !(Token.isInvalid(actualValue))) {
         myException = "ETWriter: " + callbackMethod + "() expects null or INVALID value, but found " + sActualValue;
     } 
   } else if ((expectedValue instanceof BaseValueHolder) && (actualValue instanceof BaseValueHolder)) {
      if (!((BaseValueHolder)actualValue).equals((BaseValueHolder)expectedValue)) {   
        myException = "ETWriter: " + callbackMethod + "() expects to find oldValue " + sExpectedValue + " for key " + key + " but instead found value of " + sActualValue;
      }
   } else if (actualValue == null) {
     myException = "ETWriter: " + callbackMethod + "() expects " + sExpectedValue + " but received an actualValue of null";
   } else if (!actualValue.equals(expectedValue)) {
        myException = "ETWriter: " + callbackMethod + "() expects to find oldValue " + sExpectedValue + " for key " + key + " but instead found value of " + sActualValue;
   }

   // throwException to EventBB if any issues found
   if (myException != null) { 
      throwException(myException);
   }
}

/** Check that this method is running in the same VM where the writer
 *  was created. Log an error if any problems are detected.
 *  If writers are installed on all regions, the product should prefer
 *  a local writer (so it should be in the same VM).
 */
protected void checkVM() {
   int myPID = ProcessMgr.getProcessId();
   if (whereIWasRegistered != myPID) {
      String errStr = "Expected cacheWriter to be invoked in VM " + whereIWasRegistered +
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

/* todo@lhughes -- do remote writers get callback (or just local writers)?
   if (event.isOriginRemote()) {
     return;
   }
*/

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
      boolean eventProducedInThisVM = callbackObj.indexOf("pid " + myPID + " ") >= 0;
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
