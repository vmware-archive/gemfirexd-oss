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
package mirror;

//import diskReg.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.cache.*;
import java.lang.reflect.*;
import java.util.*;

public class EndTaskValidator extends Validator {

protected CacheListener listener = null;
protected long numAfterCreateEventsForListener = 0; 
protected long numAfterCreateEventsForOtherListeners = 0;
protected long creationKeySetSize = 0;; 
protected long endKeySetSize = 0;;  
protected boolean validatePositiveNameCounter = true; 
protected boolean validateNegativeNameCounter = true;

/**
 *  Set listener. Sets the event listener to install for this VM.
 */
public void setListener(CacheListener listener) {
   this.listener = listener;
}

/**
 *  Set numAfterCreateEventsForOtherListeners. Sets the expected number of add events which should
 *      occur for listeners other than the listener set in this instance of Validator. 
 *      The number of events will be tested after iterating through the region with 
 *      validateRegionContents.
 */
public void setNumAfterCreateEventsForOtherListeners(int numAfterCreateEventsForOtherListeners) {
   this.numAfterCreateEventsForOtherListeners = numAfterCreateEventsForOtherListeners;
}

/**
 *  Set numAfterCreateEventsForListener. Sets the expected number of add events which should
 *      occur for listener. The number of events will be tested after iterating
 *      through the region with validateRegionContents.
 */
public void setNumAfterCreateEventsForListener(int numAfterCreateEventsForListener) {
   this.numAfterCreateEventsForListener = numAfterCreateEventsForListener;
}

/**
 *  Set creationKeySetSize. Sets the expected size of the key set after creating the region.
 */
public void setCreationKeySetSize(int creationKeySetSize) {
   this.creationKeySetSize = creationKeySetSize;
}

/**
 *  Set endKeySetSize. Sets the expected size of the key set after creating the region.
 */
public void setEndKeySetSize(int endKeySetSize) {
   this.endKeySetSize = endKeySetSize;
}

/**
 *  Set validatePositiveCounter. True if the validator should validate that the positive
 *      name counter is > 0, false otherwise.
 */
public void setValidatePositiveNameCounter(boolean validatePositiveNameCounter) {
   this.validatePositiveNameCounter = validatePositiveNameCounter;
}

/**
 *  Set validateNegativeCounter. True if the validator should validate that the negative
 *      name counter is > 0, false otherwise.
 */
public void setValidateNegativeNameCounter(boolean validateNegativeNameCounter) {
   this.validateNegativeNameCounter = validateNegativeNameCounter;
}

/**
 *  Do the end task validation according to the set parameters.
 */
public String endTaskValidate() {
   // print blackboards
   NameBB.getBB().printSharedCounters();
   MirrorBB.getMirrorBB().printSharedCounters();
   blackboard.printSharedCounters();

   StringBuffer errStr = new StringBuffer();
   errStr.append(validateKeySetSize("After creating region in end task: ", creationKeySetSize));
// lynn
//   errStr.append(validateEndTaskEvents(listener, 0, numAfterCreateEventsForOtherListeners));
   errStr.append(validateNameCounters());
   try {
      validateRegionContents(CacheUtil.getCache().getRegion(MirrorTest.REGION_NAME));
   } catch (TestException e) {
      errStr.append(e.getMessage());
   }

   // now sleep to let events come through
   try {
      long sleepTime = 30000;
      Log.getLogWriter().info("Sleeping for " + sleepTime + " millis to allow events to occur");
      Thread.sleep(sleepTime);
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }

   errStr.append(validateKeySetSize("After validating region in end task: ", endKeySetSize));
// lynn
//   errStr.append(validateEndTaskEvents(listener, numAfterCreateEventsForListener, numAfterCreateEventsForOtherListeners));
   NameBB.getBB().printSharedCounters();
   MirrorBB.getMirrorBB().printSharedCounters();
   blackboard.printSharedCounters();
   return errStr.toString();
}

///**
// *  Validate end task events. 
// *
// *  @param expectedAddEvents - The expected value for all add events for all end task listeners.
// *         All other events (destroy, invalidate) are expected to be 0 for end task listeners.
// *         If this argument is < 0, then no events get checked.
// *
// *  @return A String containing any validation error, or empty if none.
// */
//private String validateEndTaskEvents(long expectedAddEvents) {
//   Log.getLogWriter().info("Validating end task events, expecting add events for all end task listeners to be " +
//       expectedAddEvents);
//   return validateEndTaskEvents(expectedAddEvents, expectedAddEvents, expectedAddEvents, 
//                                expectedAddEvents, expectedAddEvents);
//}

/** 
 *  Given a blackboard, a counter from that blackboard and an expected value
 *  for the counter, check that the counter has the given expected value.
 *
 *  @param bb The Blackboard containing the counterName.
 *  @param counterName The name of the counter.
 *  @param expectedValue The expected value of the counter. If this is < 0, then
 *         the counter is not checked.
 *
 *  @return A String containing validation error, or an empty String if no errors.
 */
static String checkCounter(Blackboard bb, String counterName, long expectedValue) {
   try {
      if (expectedValue < 0) // don't check this counter
         return "";
      Class BBClass = bb.getClass();
      Field aField = BBClass.getField(counterName);
      Object value = aField.get(null);
      if (value.getClass() != Integer.class)
         throw new TestException("Expected value of " + counterName + " from " +
               BBClass.getName() + " to be an Integer, but it is " + value);
      int whichCounter = ((Integer)value).intValue();
      SharedCounters sharedCounters = bb.getSharedCounters();
      long counterValue = sharedCounters.read(whichCounter);
      String aStr = "Validating " + counterName + " in " +
         BBClass.getName() + ", expected value " + expectedValue + 
          ", actual value " + counterValue;
      Log.getLogWriter().info(aStr);
      if (counterValue != expectedValue) {
         return aStr;
      }
      return ""; 
   } catch (NoSuchFieldException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

///**
// *  Validate end task events. 
// *
// *  @param listener - The end task listener whose expected value for AfterCreate events is expectedAfterCreateEvents.
// *  @param expectedAfterCreateEvents - The expected value for AfterCreate events for listener. If < 0 then
// *         this event is not checked.
// *  @param expectedOtherAfterCreateEvents - The expected value for AfterCreate events of end task listeners 
// *         OTHER THAN listener. If < 0 then this event is not checked.
// *
// *  @return A String containing any validation error, or empty if none.
// */
//private static String validateEndTaskEvents(CacheListener listener, 
//                                            long expectedAfterCreateEvents,
//                                            long expectedOtherAfterCreateEvents) {
//   Log.getLogWriter().info("Validating end task events, expecting AfterCreate events for " + listener + 
//       " to be " + expectedAfterCreateEvents + ", expecting all other AfterCreate events to be " + expectedOtherAfterCreateEvents);
//
//   long expectedAfterCreateEvent1 = expectedOtherAfterCreateEvents;
//   long expectedAfterCreateEvent2 = expectedOtherAfterCreateEvents;
//   long expectedAfterCreateEvent3 = expectedOtherAfterCreateEvents;
//   long expectedAfterCreateEvent4 = expectedOtherAfterCreateEvents;
//   long expectedAfterCreateEvent5 = expectedOtherAfterCreateEvents;
//   if (listener instanceof EndTaskListener1)
//      expectedAfterCreateEvent1 = expectedAfterCreateEvents;
//   if (listener instanceof EndTaskListener2)
//      expectedAfterCreateEvent2 = expectedAfterCreateEvents;
//   if (listener instanceof EndTaskListener3)
//      expectedAfterCreateEvent3 = expectedAfterCreateEvents;
//   if (listener instanceof EndTaskListener4)
//      expectedAfterCreateEvent4 = expectedAfterCreateEvents;
//   if (listener instanceof EndTaskListener5)
//      expectedAfterCreateEvent5 = expectedAfterCreateEvents;
//   return validateEndTaskEvents(expectedAfterCreateEvent1, expectedAfterCreateEvent2, expectedAfterCreateEvent3, 
//                                expectedAfterCreateEvent4, expectedAfterCreateEvent5);
//}

///**
// *  Validate end task events. 
// *
// *  @param expectedAfterCreateEvents_1 - The expected value for add events for EndTaskListener1.
// *         If < 0 then this event is not checked.
// *  @param expectedAfterCreateEvents_2 - The expected value for add events for EndTaskListener2.
// *         If < 0 then this event is not checked.
// *  @param expectedAddEventsOEL3 - The expected value for add events for EndTaskListener3.
// *         If < 0 then this event is not checked.
// *  @param expectedAddEventsOEL4 - The expected value for add events for EndTaskListener4.
// *         If < 0 then this event is not checked.
// *  @param expectedAddEventsOEL5 - The expected value for add events for EndTaskListener5.
// *         If < 0 then this event is not checked.
// *  All other events (destroy, invalidate) are expected to be 0.
// *
// *  @return A String containing any validation error, or empty if none.
// */
//private static String validateEndTaskEvents(long expectedAfterCreateEvents_1,
//                                            long expectedAfterCreateEvents_2,
//                                            long expectedAfterCreateEvents_3,
//                                            long expectedAfterCreateEvents_4,
//                                            long expectedAfterCreateEvents_5) {
//   Log.getLogWriter().info("Validating end task events with expectedAfterCreateEvents_1 " + 
//       expectedAfterCreateEvents_1 +
//       ", expectedAfterCreateEvents_2 "  + expectedAfterCreateEvents_2 + 
//       ", expectedAfterCreateEvents_3 "  + expectedAfterCreateEvents_3 +
//       ", expectedAfterCreateEvents_4 "  + expectedAfterCreateEvents_4 + 
//       ", expectedAfterCreateEvents_5 "  + expectedAfterCreateEvents_5);
//   StringBuffer errStr = new StringBuffer();
//   errStr.append(checkCountersInBB(EndTaskEventCounters1BB.getBB(), expectedAfterCreateEvents_1, false));
//   errStr.append(checkCountersInBB(EndTaskEventCounters2BB.getBB(), expectedAfterCreateEvents_2, false));
//   errStr.append(checkCountersInBB(EndTaskEventCounters3BB.getBB(), expectedAfterCreateEvents_3, false));
//   errStr.append(checkCountersInBB(EndTaskEventCounters4BB.getBB(), expectedAfterCreateEvents_4, false));
//   errStr.append(checkCountersInBB(EndTaskEventCounters5BB.getBB(), expectedAfterCreateEvents_5, false));
//
//   if (errStr.length() > 0)
//      Log.getLogWriter().info("Detected end task errors: " + errStr);
//   return errStr.toString();
//}

String validateNameCounters() {
   Log.getLogWriter().info("Validating name counters...");
   StringBuffer errStr = new StringBuffer();

   long positiveCounter = NameFactory.getPositiveNameCounter();
   long negativeCounter = NameFactory.getNegativeNameCounter();
   if ((positiveCounter == 0) && (negativeCounter == 0)) {
      errStr.append("Error in test, both positive counter and negative counters are <= 0");
   }

   // verify the counters; if it is zero the test didn't do any work
   if (validatePositiveNameCounter) {
      Log.getLogWriter().info("positiveNameCounter is " + positiveCounter);
   } else {
      Log.getLogWriter().info("Not validating positiveNameCounter because validatePositiveNameCounter is " +
          validatePositiveNameCounter);
   }

   if (validateNegativeNameCounter) {
      Log.getLogWriter().info("negativeNameCounter is " + negativeCounter);
   } else {
      Log.getLogWriter().info("Not validating negativeNameCounter because validateNegativeNameCounter is " +
          validateNegativeNameCounter);
   }
   return errStr.toString();
}

static String validateKeySetSize(String logStr, long expectedKeySetSize) {
   Log.getLogWriter().info("Validating name set size, expecting size to be " + expectedKeySetSize);
   StringBuffer errStr = new StringBuffer();
   Region aRegion = CacheUtil.createCache().getRegion(MirrorTest.REGION_NAME);
   Set keySet = aRegion.keys();
   int size = keySet.size();
   Log.getLogWriter().info(logStr + "Name set size is " + size);
   if (size != expectedKeySetSize) {
      errStr.append(logStr + "Expected key set size to be " + expectedKeySetSize + ", but it was " + size);
      
      Log.getLogWriter().info(errStr.toString());
   }
   return errStr.toString();
}

static String validateMirrorNoneEvents() {
   Log.getLogWriter().info("Validating mirror none events...");
   long numNames = NameFactory.getTotalNameCounter();
   long numPositiveNames = NameFactory.getPositiveNameCounter();
   long numNegativeNames = Math.abs(NameFactory.getNegativeNameCounter());
   StringBuffer errStr = new StringBuffer();
   errStr.append(checkCountersInBB(EventCounters1BB.getBB(), numPositiveNames, false));
   errStr.append(checkCountersInBB(EventCounters2BB.getBB(), numNegativeNames, false));
   errStr.append(checkCountersInBB(EventCounters3BB.getBB(), 0, false));
   errStr.append(checkCountersInBB(EventCounters4BB.getBB(), 0, false));
   errStr.append(checkCountersInBB(EventCounters5BB.getBB(), 0, false));
   return errStr.toString();
}

static String validateMirrorKeysEvents() {
   Log.getLogWriter().info("Validating mirror keys events...");
   long numPositiveNames = NameFactory.getPositiveNameCounter();
   long numNegativeNames = Math.abs(NameFactory.getNegativeNameCounter());
   long numNames = NameFactory.getTotalNameCounter();
   StringBuffer errStr = new StringBuffer();
   errStr.append(checkCountersInBB(EventCounters1BB.getBB(), numPositiveNames, false));
   errStr.append(checkCountersInBB(EventCounters2BB.getBB(), numNegativeNames, false));
   errStr.append(checkCountersInBB(EventCounters3BB.getBB(), numNames, true));
   errStr.append(checkCountersInBB(EventCounters4BB.getBB(), numNames, true));
   errStr.append(checkCountersInBB(EventCounters5BB.getBB(), numNames, true));
   return errStr.toString();
}

static String validateMirrorKeysValuesEvents() {
   Log.getLogWriter().info("Validating mirror keys/values events...");
   long numPositiveNames = NameFactory.getPositiveNameCounter();
   long numNegativeNames = Math.abs(NameFactory.getNegativeNameCounter());
   long numNames = NameFactory.getTotalNameCounter();
   StringBuffer errStr = new StringBuffer();
   errStr.append(checkCountersInBB(EventCounters1BB.getBB(), numPositiveNames, false));
   errStr.append(checkCountersInBB(EventCounters2BB.getBB(), numNegativeNames, false));
   errStr.append(checkCountersInBB(EventCounters3BB.getBB(), numNames, true));
   errStr.append(checkCountersInBB(EventCounters4BB.getBB(), numNames, true));
   errStr.append(checkCountersInBB(EventCounters5BB.getBB(), numNames, true));
   return errStr.toString();
}

static protected String checkCountersInBB(Blackboard bb, long expectedAfterCreateCount, boolean isOriginRemote) {
   StringBuffer errStr = new StringBuffer();

   // afterCreate counters
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isDist", expectedAfterCreateCount));
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isNotExp", expectedAfterCreateCount));
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isRemote", (isOriginRemote ? expectedAfterCreateCount : 0)));
   errStr.append(checkCounter(
      bb, "numAfterCreateEvents_isNotRemote", (isOriginRemote ? 0 : expectedAfterCreateCount)));

   // afterDestroy counters
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isNotExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isRemote", 0));
   errStr.append(checkCounter(
      bb, "numAfterDestroyEvents_isNotRemote", 0));

   // afterInvalidate counters
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isNotExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isRemote", 0));
   errStr.append(checkCounter(
      bb, "numAfterInvalidateEvents_isNotRemote", 0));

   // afterUpdate counters
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isNotExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isRemote", 0));
   errStr.append(checkCounter(
      bb, "numAfterUpdateEvents_isNotRemote", 0));

   // afterRegionDestroy counters
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isNotExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isRemote", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionDestroyEvents_isNotRemote", 0));

   // afterRegionInvalidate counters
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isNotDist", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isNotExp", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isRemote", 0));
   errStr.append(checkCounter(
      bb, "numAfterRegionInvalidateEvents_isNotRemote", 0));

   return errStr.toString();
}

}
