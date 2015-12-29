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
package hct; 

import util.*;
import event.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

/** Event Test Operations Listener.  Maintains counters based on CacheEvents passed
 *  to callback methods.  Validates the callbackArgument (including memberId) and 
 *  verifies the event.getOperation() data vs. the event.is() methods.
 *
 * @see EventBB#isSerialExecution
 * @see OperationCountersBB
 *
 * @author lhughes
 * @since 5.0
 */
public class OperationListener extends util.AbstractListener implements CacheListener, Declarable {

private boolean isCarefulValidation;

/** noArg constructor which sets isCarefulValidation based on the tests
 *  setting of serialExecution
 */
public OperationListener() {
   TestConfig config = TestConfig.getInstance();
   ConfigHashtable tab = config.getParameters();
   this.isCarefulValidation = tab.booleanAt(Prms.serialExecution);
}

/** Create a new listener and specify whether the test is doing careful validation.
 *
 *  @param isCarefulValidation true if the test is doing careful validate (serial test)
 *         false otherwise.
 */
public OperationListener(boolean isCarefulValidation) {
   this.isCarefulValidation = isCarefulValidation;
}

//==============================================================================
// implementation of CacheListener methods
//==============================================================================

/** 
 *  Handles the event of new key being added to region: increments counters, 
 *  performs validation.
 *
 *  @param event the EntryEvent
 *  @throws TestException if validation fails
 */
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);

   if (!event.getOperation().isCreate()) 
     throwException("afterCreate invoked, but Operation.isCreate() returns false");

   // Don't count creates (local) based on doing a netsearch to satisfy a get
   if (!event.getOperation().equals(Operation.SEARCH_CREATE)) {
      incrementAfterCreateCounters(event.getOperation(), OperationCountersBB.getBB());
   }
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event and entry being destroyed: increments counters, performs validation.
 *  performs validation.
 *
 *  @param event the EntryEvent
 *  @throws TestException if validation fails
 */
public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);

   if (!event.getOperation().isDestroy()) 
     throwException("afterDestroy invoked, but Operation.isDestroy() returns false");

   incrementAfterDestroyCounters(event.getOperation(), OperationCountersBB.getBB());
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event of an entry's value being invalidated: increments counters,
 *  performs validation.
 *
 *  @param event the EntryEvent
 *  @throws TestException if validation fails
 */
public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);

   if (!event.getOperation().isInvalidate()) 
     throwException("afterInvalidate invoked, but Operation.isInvalidate() returns false");

   incrementAfterInvalidateCounters(event.getOperation(), OperationCountersBB.getBB());
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event of new key being modified in a region: increments counters, 
 *  performs validation.
 *
 *  @param event the EntryEvent
 *  @throws TestException if validation fails
 */
public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);

   if (!event.getOperation().isUpdate()) 
     throwException("afterUpdate invoked, but Operation.isUpdate() returns false");

   incrementAfterUpdateCounters(event.getOperation(), OperationCountersBB.getBB());
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event of a region being destroyed: increments counters, 
 *  performs validation.
 *
 *  @param event the RegionEvent
 *  @throws TestException if validation fails
 */
public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);

   if (!event.getOperation().isRegionDestroy()) 
     throwException("afterRegionDestroy invoked, but Operation.isRegionDestroy() returns false");

   // Don't count the destroyRegions associated wtih cache.close()
   // these can be Operation.REGION_CLOSE, Operation.CACHE_CLOSE
   if (!event.getOperation().isClose()) {
      incrementAfterRegionDestroyCounters(event.getOperation(), OperationCountersBB.getBB());
   }
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event of region being invalidated: increments counters, 
 *  performs validation.
 *
 *  @param event the RegionEvent
 *  @throws TestException if validation fails
 */
public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);

   if (!event.getOperation().isRegionInvalidate()) 
     throwException("afterRegionInvalidate invoked, but Operation.isRegionInvalidate() returns false");

   incrementAfterRegionInvalidateCounters(event.getOperation(), OperationCountersBB.getBB());
   verifyOperationMethods(event);
   checkVM();
}

/** 
 *  Handles the event of region being cleared: increments counters, 
 *  performs validation.
 *
 *  @param event the RegionEvent
 *  @throws TestException if validation fails
 */
public void afterRegionClear(RegionEvent event) {
  logCall("afterRegionClear", event);

  }

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

/** 
 *  Handles the event of a region being created: increments counters, 
 *  performs validation.
 *
 *  @param event the RegionEvent
 *  @throws TestException if validation fails
 */
public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);

  if (!(event.getOperation() == Operation.REGION_CREATE)) 
    throwException("afterRegionCreate invoked, but Operation.isRegionCreate() returns false");

  incrementAfterRegionCreateCounters(event.getOperation(), OperationCountersBB.getBB());
  verifyOperationMethods(event);
  checkVM();
}

/** 
 *  Called when the region containing this callback is destroyed, when the cache
 *  is closed, or when a callback is removed from a region using an 
 *  <code>AttributesMutator</code>.  Increments counters and performs validation.
 *
 *  @throws TestException if validation fails
 */
public void close() {
   logCall("close", null);

   OperationCountersBB.getBB().getSharedCounters().increment(OperationCountersBB.numClose);
   checkVM();
}

/** 
 *  Verify that (entry) event & operation methods are in sync with one another.
 *  performs validation.
 *
 *  @param event the EntryEvent
 *  @throws TestException if validation fails
 */
protected void verifyOperationMethods(EntryEvent event) {
  Operation op = event.getOperation();

  if (!op.isEntry()) 
    throwException("Operation.isEntry() for EntryEvent returns false");

  if (event.isDistributed() != op.isDistributed()) 
    throwException("event.isDistributed(" + event.isDistributed() + ") != op.isDistributed(" + op.isDistributed() + ")");

  if (event.isExpiration() != op.isExpiration()) 
    throwException("event.isExpiration(" + event.isExpiration() + ") != op.isExpiration(" + op.isExpiration() + ")");

  if (event.isLoad() != op.isLoad()) 
    throwException("event.isLoad(" + event.isLoad() + ") != op.isLoad(" + op.isLoad() + ")");

  if (event.isLocalLoad() != op.isLocalLoad()) 
    throwException("event.isLocalLoad(" + event.isLocalLoad() + ") != op.isLocalLoad(" + op.isLocalLoad() + ")");

  if (event.isNetLoad() != op.isNetLoad()) 
    throwException("event.isNetLoad(" + event.isNetLoad() + ") != op.isNetLoad(" + op.isNetLoad() + ")");

  if (event.isNetSearch() != op.isNetSearch()) 
    throwException("event.isNetSearch(" + event.isNetSearch() + ") != op.isNetSearch(" + op.isNetSearch() + ")");
}

/** Check that methods on (region) event & operation agree with one another
 *
 *  @param event the RegionEvent
 *  @throws TestException if validation fails
 */
protected void verifyOperationMethods(RegionEvent event) {
  Operation op = event.getOperation();

  if (!op.isRegion()) {
    throwException("Operation.isRegion() for RegionEvent returns false");
  } 

  if ((op.isRegionDestroy() || (op.isRegionInvalidate()))) {
    if (event.isDistributed() != op.isDistributed()) 
      throwException("event.isDistributed(" + event.isDistributed() + ") != op.isDistributed(" + op.isDistributed() + ")");

    if (event.isExpiration() != op.isExpiration()) 
      throwException("event.isExpiration(" + event.isExpiration() + ") != op.isExpiration(" + op.isExpiration() + ")");
  }
}

/** Utility method to write the exception to the EventCountersBB where it can be used to 
 *  throw an Exception back to hydra's Master Controller.  Also logs the error
 *  in the client log.
 *  
 *  @param errStr the error string to include in the Exception
 *  @throws TestException if validation fails
 *  @see TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = EventCountersBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());      
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

/** Check that this method is running in the same VM where the listener
 *  was created. Log an error if any problems are detected.
 *
 *  @throws TestException if validation fails
 */
protected void checkVM() {
   int myPID = ProcessMgr.getProcessId();
   if (whereIWasRegistered != myPID) {
      String errStr = "Expected event to be invoked in VM " + whereIWasRegistered +
         ", but it was invoked in " + myPID + ": " + toString() + "; see system.log for call stack";
      hydra.blackboard.SharedMap aMap = EventCountersBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
   }
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
