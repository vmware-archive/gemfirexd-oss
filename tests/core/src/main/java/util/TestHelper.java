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
package util; 

import hydra.*;
import hydra.blackboard.*;
import java.lang.reflect.*;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.size.WellKnownClassSizer;
import com.gemstone.gemfire.pdx.PdxSerializable;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

import diskRecovery.RecoveryTestVersionHelper;

import java.io.*;
import java.util.*;

import perffmwk.*;
import rollingupgrade.RollingUpgradePrms;

public class TestHelper {

public static final int MILLIS = 0;
public static final int NANOS = 1;
public static final int MICROS = 2;
public static final int SECONDS = 3;

public static final long SEC_NANO_FACTOR    = 1000000000;
public static final long MILLI_NANO_FACTOR  = 1000000;
public static final long MILLI_MICRO_FACTOR = 1000;
public static final long MICRO_NANO_FACTOR  = 1000;
public static final long SEC_MILLI_FACTOR   = 1000;

// Key for use in any blackboard sharedMap. This is typically used
// for errors that are detected in event listener methods; errors
// thrown from event listener methods are not caught by the client,
// thus hydra does not know about them. Tests can implement listeners 
// that put error strings into a blackboard shared map, then the client can 
// call TestHelper.checkForEventError periodically to check for errors.
public static String EVENT_ERROR_KEY = "EventErrorMessage";

//======================================================================== 
// String methods
static int recursionCount = 0;

/** Return a String representing anObj. Use hydra params for number of elements 
 *  and number of levels. 
 */
public static String toString(Object anObj) {
   recursionCount = 0;
   int printElementsLimit = TestConfig.tab().intAt(TestHelperPrms.printElementsLimit, 0);
   int printObjectDepth = TestConfig.tab().intAt(TestHelperPrms.printObjectDepth, 0);
   StringBuffer aStr = new StringBuffer();
   toString(anObj, "", printElementsLimit, 0, printObjectDepth, aStr);
   return aStr.toString();
}

/** Return a String representing anObj using the given number of elements 
 *  and number of levels. 
 *
 *  @param numElements The maximum number of elements in anObj to print.
 *  @param depth The maximum depth to print for elements of anObj.
 */
public static String toString(Object anObj, int numElements, int depth) {
   recursionCount = 0;
   StringBuffer aStr = new StringBuffer();
   toString(anObj, "", numElements, 0, depth, aStr);
   return aStr.toString();
}

private static void toString(Object anObj, String prefix, int numElements, int currentDepth, int maxDepth, StringBuffer result) {
   /* Return a string describing the object anObj. */
   recursionCount++;
   if (recursionCount >= 1000) {
      result.append("...");
      return;
   }
   if (currentDepth > maxDepth)
      return;
   String tab = "";
   for (int i = 1; i <= currentDepth; i++) tab = tab + "  ";
   if (result.length() > 0)
      result.append("\n");
   result.append(tab);
   result.append(prefix);
   if (anObj == null) {
      result.append("null");
      return;
   }
   Class aClass = anObj.getClass();
   result.append("<");
   if (anObj instanceof Map) {
      Map aMap = (Map)anObj;
      int size = aMap.size();
      result.append(basicsToString(anObj, new Integer(size)));
      if (numElements > 0) {
         Iterator it = aMap.keySet().iterator();
         int count = 0;
         while (it.hasNext() && (count++ < numElements)) {
            Object key = it.next();
            toString(key, "Key: ", numElements, currentDepth + 1, maxDepth, result);
            toString(aMap.get(key), "Value: ", numElements, currentDepth + 1, maxDepth, result);
         }
         addCutoffWarning((count != size) && (count >= numElements), tab + "  ", result, numElements);
      }
   } else if (anObj instanceof List) {
      List aList = (List)anObj;
      int size = aList.size();
      result.append(basicsToString(anObj, new Integer(size)));
      if (numElements > 0) {
         int count = 0;
         while ((count < size) && (count < numElements)) {
            toString(aList.get(count), count + ": ", numElements, currentDepth + 1, maxDepth, result);
            count++;
         }
         addCutoffWarning((count != size) && (count >= numElements), tab + "  ", result, numElements);
      }
   } else if (anObj instanceof Set) {
      Set aSet = (Set)anObj;
      int size = aSet.size();
      result.append(basicsToString(anObj, new Integer(size)));
      if (numElements > 0) {
         Iterator it = aSet.iterator();
         int count = 0;
         while (it.hasNext() && (count++ < numElements)) {
            toString(it.next(), "", numElements, currentDepth + 1, maxDepth, result);
         }
         addCutoffWarning((count != size) && (count >= numElements), tab + "  ", result, numElements);
      }
   } else if (aClass == String.class) {
      int stringLimit = 10;
      try {
        stringLimit = TestConfig.tab().intAt(TestHelperPrms.printStringLimit, 100);
      } catch (HydraRuntimeException e) { // might be calling in system manager VM for fine logging
         // do nothing; default to 10
      }
      String aStr = (String)anObj;
      result.append(basicsToString(anObj, new Integer(aStr.length())));
      result.append(" ");
      if (aStr.length() > stringLimit) {
         result.append("\"");
         for (int i = 0; i < stringLimit; i++) {
            result.append(aStr.charAt(i));
         }
         result.append("...\"");
      } else 
         result.append("\"").append(aStr).append("\"");
   } else if (aClass == StringBuffer.class) {
      int stringLimit = 50;
      StringBuffer aStr = (StringBuffer)anObj;
      result.append(basicsToString(anObj, new Integer(aStr.length()))).append(" ");
      if (aStr.length() > stringLimit) {
         result.append("\"");
         for (int i = 0; i < stringLimit; i++)
            result.append(aStr.charAt(i));
         result.append("...\"");
      } else 
         result.append("\"").append(aStr).append("\"");
   } else if (anObj instanceof Number) {
      result.append(basicsToString(anObj, null));
      result.append(", value ").append(anObj);
   } else if (aClass.isArray()) {
      result.append(basicsToString(anObj, new Integer(Array.getLength(anObj))));
      Class componentType = aClass.getComponentType();
      result.append(", componentType ").append( componentType.getName());
      if (numElements > 0) {
         int size = Array.getLength(anObj);
         int count = 0;
         int i = 0;
         while ((i < size) && (count++ < numElements)) {
            toString(Array.get(anObj, i), "", numElements, currentDepth + 1, maxDepth, result);
            i++;
         }
         addCutoffWarning((count != size) && (count >= numElements), tab + "  ", result, numElements);
      }
   } else if (aClass == Boolean.class) {
      result.append(basicsToString(anObj, null));
      result.append(", value ").append(anObj);
   } else if (aClass == Character.class) {
      result.append(basicsToString(anObj, null));
      result.append(" value (as int) " + (int)(((Character)anObj).charValue()));
   } else if (aClass == Date.class) {
      result.append(basicsToString(anObj, null));
      Date aDate = (Date)anObj;
      result.append(", getTime() = ").append(  aDate.getTime());
      result.append( ", (").append(  aDate ).append( ")");
   } else if (anObj instanceof ValueHolderIF) {
      ValueHolderIF valueHolder = (ValueHolderIF)anObj;
      //result.append(basicsToString(anObj, null));
      result.append(valueHolder.toString());
   } else if (anObj instanceof QueryObject) {
      result.append(basicsToString(anObj, null));
      result.append(" " + anObj.toString());
   } else if (anObj instanceof PdxSerializable){
      //result.append(basicsToString(anObj, null));
      result.append(anObj.toString());
   } else {
     result.append(basicsToString(anObj, null));
   }
   result.append(">");
}

private static void addCutoffWarning(boolean abool, String tab, StringBuffer aStr, int printElementsLimit) {
   if (abool) {
      aStr.append("\n");
      aStr.append(tab);
      aStr.append("....(not showing remaining elements due to printElementsLimit " + printElementsLimit);
   }
}

private static String basicsToString(Object anObj, Integer size) {
   /* Return a string showing the class name, size and objectID of anObj */
   if (anObj == null)
      return "null";
   StringBuffer result = new StringBuffer();
   result.append(anObj.getClass().getName());
   if (size != null)
      result.append(" Size ").append(size);
   return result.toString();
}

/** Get a stack trace of the current stack and return it as a String */
public static String getStackTrace() {
  return getStackTrace( "Exception to get stack trace" );
}

/** Get a stack trace of the current stack and return it as a String */
public static String getStackTrace( String reason ) {
   try {
      throw new Exception(reason);
   } catch (Exception e) {
      return getStackTrace(e);
   }
}

/**
 * Get the first Throwable in the cause chain of aThrowable that is
 * an instanceof type.
 *
 * @param aThrowable the head of the cause chain
 * @param type the Class of the cause to look for
 * @return the Throwable the matches, or null if not found
 */
public static Throwable findCause(Throwable aThrowable, Class type) {
  if (aThrowable == null) {
    throw new IllegalArgumentException("aThrowable cannot be null");
  }
  while (true) {
    if (aThrowable == null || type.isInstance(aThrowable)) {
      break;
    }
    aThrowable = aThrowable.getCause();
  };
  return aThrowable;
}

/** Get a stack trace for the given Throwable and return it as a String.
 *  
 *  @param aThrowable The exception to get the stack trace for.
 *
 *  @return The call stack for aThrowable as a string.
 */
public static String getStackTrace(Throwable aThrowable) {
   StringWriter sw = new StringWriter();
   aThrowable.printStackTrace(new PrintWriter(sw, true));
   return sw.toString();
}

/** Format the given double as a string.
 *  
 *  @param adouble The double to format.
 *  @param placesBeforeDecimal The number of places to have before the decimal point. Pads with
 *         spaces if necessary. If this is not large enough for adouble, more than placesBeforeDecimal
 *         will be used to accomodate the double.
 *  @param placesAfterDecimal The number of places to have after the decimal point. Pads with
 *         zeroes if necessary. If this is not large enough for adouble, more than placesAfterDecimal
 *         will be used to accomodate the double.
 *
 *  @return The formatted double as a String.
 */
public static String format(double adouble, int placesBeforeDecimal, int placesAfterDecimal) {
   String aStr = "" + adouble;
   int index = aStr.indexOf(".");
   if (!((index < 0) || (index >= placesBeforeDecimal))) {
      int numToAdd = placesBeforeDecimal - index;
      for (int i = 1; i <= numToAdd; i++) {
         aStr = "0" + aStr;
      }
   }
   index = aStr.indexOf(".");
   int currPlacesAfterDecimal = aStr.length() - index - 1;
   if (currPlacesAfterDecimal >= placesAfterDecimal)
      return aStr;
   int numToAdd = placesAfterDecimal - currPlacesAfterDecimal;
   for (int i = 1; i <= numToAdd; i++) {
      aStr = aStr + "0";
   }
   return aStr;
}

/** Format a long and return it as a String.
 *  
 *  @param along The long to format.
 *  @param places The number of places for anInt. Pads with zeroes if necessary.
 *
 *  @return The formatted long as a String.
 */
public static String format(long along, int places) {
   String intStr = "" + along;
   while (intStr.length() < places)
      intStr = "0" + intStr;
   return intStr;
}

public static String millisToString(long millis) {
   long sec = (millis / 1000);
//   long remMillis = millis % 1000;
   long min = sec / 60;
   sec = sec % 60;
   String aStr = millis + " ms (" + min + ":" + format(sec, 2) + ":" + format(millis, 3) + " (min:sec:ms))";
   return aStr;
}

public static String nanosToString(long nanos) {
   if (nanos < 0)
      return "" + nanos;
   long millis = (nanos % SEC_NANO_FACTOR);
   long sec = (nanos / SEC_NANO_FACTOR);
   long min = sec / 60;
   sec = sec % 60;
   String aStr = min + ":" + format(sec, 2) + ":" + format((millis/MILLI_NANO_FACTOR), 3) + "." +
                 (millis%MILLI_NANO_FACTOR) + " (min:sec:ms)";
   long checkNanos = stringToNanos(aStr);
   if (checkNanos != nanos)
      throw new TestException("Original nanos: " + nanos + ", as string " + aStr + ", back to nanos "
 + checkNanos);
   return aStr;
}

/** Given a String which was returned from TestHelper.nanosToString, return
 *  the nanos as a long.
 */
public static long stringToNanos(String aStr) {
   try {
      int index1 = aStr.indexOf(":");
      String minStr = aStr.substring(0, index1);
      int index2 = aStr.indexOf(":", ++index1);
      String secStr = aStr.substring(index1, index2);
      index1 = index2+1;
      index2 = aStr.indexOf(".", index1);
      String millisStr = aStr.substring(index1, index2);
      index1 = index2+1;
      index2 = aStr.indexOf(" ", index1);
      if (index2 < 0) index2 = aStr.length();
      String nanosStr = aStr.substring(index1, index2);
      long min = (new Long(minStr)).longValue();
      long sec = (new Long(secStr)).longValue();
      long millis = (new Long(millisStr)).longValue();
      long nanos = (new Long(nanosStr)).longValue();
      nanos = (min * 60 * SEC_NANO_FACTOR) + (sec * SEC_NANO_FACTOR) + (millis * MILLI_NANO_FACTOR) + nanos;
      return nanos;
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e) + "\n" + aStr);
   }
}

public static String unitToString(int timeUnit) {
   if (timeUnit == MILLIS)
      return "millis";
   else if (timeUnit == NANOS)
      return "nanos";
   else if (timeUnit == MICROS)
      return "micros";
   else if (timeUnit == SECONDS)
      return "seconds";
   else
      throw new TestException("Unknown time unit " + timeUnit);
}

//========================================================================
// Percentage methods
public static int getPercentageOf(int percentage, int anInt) {
   /* Given an int and a percentage (a number between 0 and 100), return
      an int which is the percentage of anInt */
   if (percentage == 0)
      return 0;
   if (percentage == 100)
      return anInt;
   if (percentage < 0)
      throw new TestException("Percentage = " + percentage + " is negative");
   int returnInt = (int)(anInt * (percentage / 100.0));
   return returnInt;
}

public static boolean borderCase(Long paramKey) {
   int borderCasePercentage = TestConfig.tab().intAt(paramKey);
   boolean borderCase = TestConfig.tab().getRandGen().nextInt(1, 100) <= borderCasePercentage;
   return borderCase;
}

//========================================================================
// Other methods
/** Return a random key in aMap. This should only be called for relatively small
 *  maps, as it is expensive.
 */
public static Object getRandomKeyInMap(Map aMap) {
   if (aMap.size() == 0)
      throw new TestException("Map is empty");
   Iterator it = aMap.keySet().iterator();
   int randInt = TestConfig.tab().getRandGen().nextInt(1, aMap.size());
   int count = 0;
   Object key = null;
   while (count < randInt) {
      key = it.next();
      count++;
   }
   return key;
}

/** Return a random element in aSet. This should only be called for relatively small
 *  sets, as it is expensive.
 */
public static Object getRandomElementInSet(Set aSet) {
   if (aSet.size() == 0)
      throw new TestException("Set is empty");
   TreeSet tSet = new TreeSet(new GenericComparator());
   tSet.addAll(aSet); // sort the set so that tests can run the same with the same random seed
   Iterator it = tSet.iterator();
   int randInt = TestConfig.tab().getRandGen().nextInt(1, tSet.size());
   Object element = null;
   for (int i = 0; i < randInt; i++) {
      element = it.next();
      boolean contains = aSet.contains(element);
      if (!contains)
         throw new TestException(TestHelper.toString(aSet, aSet.size(), 1) + 
            " contains " + TestHelper.toString(element) + " but calling contains returns false");
   }
   return element;
}

public static ExpirationAction stringToExpirationAction(String eaStr) {
   if (eaStr.equalsIgnoreCase("DESTROY"))
      return ExpirationAction.DESTROY;
   if (eaStr.equalsIgnoreCase("INVALIDATE"))
      return ExpirationAction.INVALIDATE;
   if (eaStr.equalsIgnoreCase("LOCAL_DESTROY"))
      return ExpirationAction.LOCAL_DESTROY;
   if (eaStr.equalsIgnoreCase("LOCAL_INVALIDATE"))
      return ExpirationAction.LOCAL_INVALIDATE;
   throw new TestException("Unknown expiration action " + eaStr);
}

public static void sleepForever() {
   try {
      Log.getLogWriter().info("Calling Thread.currentThread().sleep(" + Long.MAX_VALUE + ")...");
      Thread.sleep(Long.MAX_VALUE);
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Wait for the sum of two hydra blackboard counters to have a particular value.
 *
 * @param BB The hydra blackboard containing the counters to use.
 * @param counterName1 The name of the blackboard counter in BB.
 * @param counterName2 The name of the blackboard counter in BB.
 * @param requiredCounterSum The required counter sum.
 * @param exact If true, then the counter sum must have exactly the value requiredCounterSum.
 *        If false, then the counters' sum must be >= requiredCounterSum.
 * @param waitLimitMS If the counter sum does not have the required value by this many milliseconds
 *        then throw an error.
 *
 * @throws TestException if the counter sum does not have the required value in waitLimitMS millis.
 */
public static void waitForCounterSum(hydra.blackboard.Blackboard BB, 
                                     String counterName1, 
                                     String counterName2, 
                                     long requiredCounterSum,
                                     boolean exact,
                                     long waitLimitMS) {
   SharedCounters counters = BB.getSharedCounters();
   long startTime = System.currentTimeMillis();
   long lastLogTime = startTime;
   long logIntervalMillis = 5000; // 5 seconds
   int whichCounter1 = BB.getSharedCounter(counterName1);
   int whichCounter2 = BB.getSharedCounter(counterName2);
   long currCounter1 = counters.read(whichCounter1);
   long currCounter2 = counters.read(whichCounter2);
   long currSum = currCounter1 + currCounter2;
   Log.getLogWriter().info("Waiting for " + BB.getClass().getName() + "." + 
       counterName1 + " (current value: " + counters.read(whichCounter1) + ") and " +
       BB.getClass().getName() + "." + counterName2 + " (current value: " + counters.read(whichCounter2) +
       ") to have sum " + requiredCounterSum +
       "; exact=" + exact + "; current counter sum is " + currSum);
   boolean done = exact ? (currSum == requiredCounterSum) :
                          (currSum >= requiredCounterSum);
   while (!done) {
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMillis) {
         lastLogTime = System.currentTimeMillis();
         currCounter1 = counters.read(whichCounter1);
         currCounter2 = counters.read(whichCounter2);
         currSum = currCounter1 + currCounter2;
         Log.getLogWriter().info("Waiting for " + BB.getClass().getName() + "." + 
             counterName1 + " (current value: " + counters.read(whichCounter1) + ") and " +
             BB.getClass().getName() + "." + counterName2 + " (current value: " + counters.read(whichCounter2) +
             ") to have sum " + requiredCounterSum +
             "; exact=" + exact + "; current counter sum is " + currSum);
      }
      if ((waitLimitMS >= 0) && (System.currentTimeMillis() - startTime >= waitLimitMS)) {
         BB.printSharedCounters();
         throw new TestException("After waiting for " + (System.currentTimeMillis() - startTime) + " millis, " +
             BB.getClass().getName() + "." + 
             counterName1 + " (current value: " + currCounter1 + ") and " +
             BB.getClass().getName() + "." + counterName2 + " (current value: " + currCounter2 +
             ") have sum " + currSum + ", but expected it to be " + requiredCounterSum);
      }
      try {
         Thread.sleep(100);
      } catch (InterruptedException e) {
         throw new TestException(e.toString());
      }
      currCounter1 = counters.read(whichCounter1);
      currCounter2 = counters.read(whichCounter2);
      currSum = currCounter1 + currCounter2;
      done = exact ? (currSum == requiredCounterSum) :
                     (currSum >= requiredCounterSum);
   }
   Log.getLogWriter().info("Finished waiting for " + BB.getClass().getName() + "." + 
       counterName1 + " (current value: " + counters.read(whichCounter1) + ") and " +
       BB.getClass().getName() + "." + counterName2 + " (current value: " + counters.read(whichCounter2) +
       ") to have sum " + requiredCounterSum +
       "; exact=" + exact + "; current counter sum is " + currSum);
}

/** Wait for a hydra blackboard counter to have a particular value.
 *
 * @param BB The hydra blackboard containing the counters to use.
 * @param counterName The name of the blackboard counter, used to log information about it.
 * @param whichCounter The hydra index of the counter.
 * @param requiredCounterValue The required counter value.
 * @param exact If true, then the counter value must have exactly the value requiredCounterValue.
 *        If false, then the counter's value must be >= requiredCounterValue.
 * @param waitLimitMS If the counter does not have the required value by this many milliseconds
 *        then throw an error.
 * @param sleepMS The number of millis to sleep between each read of the counter.
 *
 * @throws TestException if the counter does not have the required value in waitLimitMS millis.
 */
public static void waitForCounter(hydra.blackboard.Blackboard BB, 
                                  String counterName, 
                                  int whichCounter, 
                                  long requiredCounterValue,
                                  boolean exact,
                                  long waitLimitMS,
                                  long sleepMS) {
   SharedCounters counters = BB.getSharedCounters();
   long startTime = System.currentTimeMillis();
   long lastLogTime = startTime;
   long logIntervalMillis = 5000; // 5 seconds
   Log.getLogWriter().info("Waiting for " + BB.getClass().getName() + "." + 
       counterName + " to have value " + requiredCounterValue +
       "; exact=" + exact + "; current counter value is " + counters.read(whichCounter));
   boolean done = exact ? (counters.read(whichCounter) == requiredCounterValue) :
                          (counters.read(whichCounter) >= requiredCounterValue);
   while (!done) {
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMillis) {
         lastLogTime = System.currentTimeMillis();
         Log.getLogWriter().info("Waiting for " + counterName + " to reach " + requiredCounterValue +
             "; current counter value is " + counters.read(whichCounter));
      }
      if ((waitLimitMS >= 0) && (System.currentTimeMillis() - startTime >= waitLimitMS)) {
         BB.printSharedCounters();
         throw new TestException("After waiting for " + (System.currentTimeMillis() - startTime) + " millis, " +
             BB.getClass().getName() + "." + counterName + " has value of " + 
             counters.read(whichCounter) + ", but expected it to become " +
             requiredCounterValue);
      }
      try {
         Thread.sleep(sleepMS);
      } catch (InterruptedException e) {
         throw new TestException(e.toString());
      }
      done = exact ? (counters.read(whichCounter) == requiredCounterValue) :
                     (counters.read(whichCounter) >= requiredCounterValue);
   }
   Log.getLogWriter().info("Finished waiting for " + counterName + " to have value " + requiredCounterValue +
       "; exact=" + exact + "; current counter value is " + counters.read(whichCounter));
}

/** Wait for a hydra blackboard counter to have a particular value.
 *
 * @param BB The hydra blackboard containing the counters to use.
 * @param counterName The name of the blackboard counter, used to log information about it.
 * @param whichCounter The hydra index of the counter.
 * @param requiredCounterValue The required counter value.
 * @param exact If true, then the counter value must have exactly the value requiredCounterValue.
 *        If false, then the counter's value must be >= requiredCounterValue.
 * @param waitLimitMS If the counter does not have the required value by this many milliseconds
 *        then throw an error.
 *
 * @throws TestException if the counter does not have the required value in waitLimitMS millis.
 */
public static void waitForCounter(hydra.blackboard.Blackboard BB, 
                                  String counterName, 
                                  int whichCounter, 
                                  long requiredCounterValue,
                                  boolean exact,
                                  long waitLimitMS) {
   waitForCounter(BB, counterName, whichCounter, requiredCounterValue, exact, waitLimitMS, 100);
}

/** Using the given region and key, wait for containsKey to become the same value
 *  as expected. Throw an error if it does not have the expected value
 *  by timeoutMS milliseconds.
 *
 *  @param aRegion The region to use for containsKey.
 *  @param key The key in aRegion to call containsKey on.
 *  @param expected The expected value of containsKey.
 *  @param timeoutMS The number of milliseconds to wait.
 *  @param logProgress If true, then log the progress of the wait, otherwise wait silently.
 *
 *  @return A string containing a log of the progress of the wait.
 *
 *  @throws TestException if containsKey does not become expectedContainsKey in timeoutMS millis.
 */
public static String waitForContainsKey(Region aRegion, 
                                        Object key, 
                                        boolean expected, 
                                        long timeoutMS,
                                        boolean logProgress) {
   StringBuffer logStr = new StringBuffer();
   String aStr = "Waiting for " + TestHelper.regionToString(aRegion, false) + ".containsKey(" + key + 
       ") to become " + expected;
   logStr.append(new Date() + ": " + aStr + "\n");
   if (logProgress) Log.getLogWriter().info(aStr);
   logStr.append(aStr);
   long startTime = System.currentTimeMillis();
   long lastLogTime = startTime;
   long logIntervalMS = 15000;
   while (true) {
      boolean containsKey = aRegion.containsKey(key);
      if (containsKey == expected) {
         aStr = TestHelper.regionToString(aRegion, false) + ".containsKey(" + key + 
            ") has expected value " + containsKey;
         logStr.append(new Date() + ": " + aStr + "\n");
         if (logProgress) Log.getLogWriter().info(aStr);
         return logStr.toString();
      }
      long currentWaitTime = System.currentTimeMillis() - startTime;
      if (currentWaitTime >= timeoutMS) {
         throw new TestException("After waiting " + currentWaitTime + " millis, " + 
            TestHelper.regionToString(aRegion, false) + ".containsKey(" + key + ") is " + containsKey + 
            "; expected it to become " + expected + "\nLog of wait progress: " + logStr);
      }
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMS) {
         aStr = "Waiting for " + TestHelper.regionToString(aRegion, false) + ".containsKey(" + key + 
             ") to become " + expected + "...";
         logStr.append(new Date() + ": " + aStr + "\n");
         if (logProgress) Log.getLogWriter().info(aStr);
         lastLogTime = System.currentTimeMillis();
      }
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException ie) {
        throw new TestException("Interrupted after waiting " + currentWaitTime + " millis for " +
            TestHelper.regionToString(aRegion, false) + ".containsKey(" + key +
            ") to become " + expected);
      }
   }
}

/** Using the given region and key, wait for containsValueForKey to become the same value
 *  as expected. Throw an error if it does not have the expected value by timeoutMS milliseconds.
 *
 *  @param aRegion The region to use for containsValueForKey.
 *  @param key The key in aRegion to call containsValueForKey on.
 *  @param expected The expected value of containsValueForKey.
 *  @param timeoutMS The number of milliseconds to wait.
 *  @param logProgress If true, then log the progress of the wait, otherwise wait silently.
 *
 *  @return A string containing a log of the progress of the wait.
 *
 *  @throws TestException if containsValueForKey does not become expected in timeoutMS millis.
 */
public static String waitForContainsValueForKey(Region aRegion, 
                                                Object key, 
                                                boolean expected, 
                                                long timeoutMS,
                                                boolean logProgress) {
   StringBuffer logStr = new StringBuffer();
   String aStr = "Waiting for " + TestHelper.regionToString(aRegion, false) + ".containsValueForKey(" + key + 
       ") to become " + expected;
   logStr.append(new Date() + aStr + "\n");
   if (logProgress) Log.getLogWriter().info(aStr);
   long startTime = System.currentTimeMillis();
   long lastLogTime = startTime;
   long logIntervalMS = 15000;
   while (true) {
      boolean containsValueForKey = aRegion.containsValueForKey(key);
      if (containsValueForKey == expected) {
         aStr = TestHelper.regionToString(aRegion, false) + ".containsValueForKey(" + key + 
            ") has expected value " + containsValueForKey;
         logStr.append(new Date() + ": " + aStr + "\n");
         if (logProgress) Log.getLogWriter().info(aStr);
         return logStr.toString();
      }
      long currentWaitTime = System.currentTimeMillis() - startTime;
      if (currentWaitTime >= timeoutMS) {
         throw new TestException("After waiting " + currentWaitTime + " millis, " + 
            TestHelper.regionToString(aRegion, false) + ".containsValueForKey(" + key + ") is " + containsValueForKey + 
            "; expected it to become " + expected + "\nLog of wait progress: " + logStr);
      }
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMS) {
         aStr = "Waiting for " + TestHelper.regionToString(aRegion, false) + ".containsValueForKey(" + key + 
             ") to become " + expected + "...";
         logStr.append(new Date() + ": " + aStr + "\n");
         if (logProgress) Log.getLogWriter().info(aStr);
         lastLogTime = System.currentTimeMillis();
      }
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException ie) {
        throw new TestException("Interrupted after waiting " + currentWaitTime + " millis for " +
            TestHelper.regionToString(aRegion, false) + ".containsValueForKey(" + key +
            ") to become " + expected);
      }
   }
}

/** Using the given region and key, wait for the object's value to become 
 *  expectedObjectValue.  Throw an error if it does not have the expected value
 *  by timeoutMS milliseconds.
 *
 *  @param aRegion The region to use.
 *  @param key The key in aRegion.
 *  @param timeoutMS The number of milliseconds to wait.
 *
 *  @throws TestException if the object does not have the expected valuein timeoutMS millis.
 */
public static void waitForObjectValue(Region aRegion, 
                                      Object key, 
                                      Object expectedObjectValue, 
                                      long timeoutMS) {
   Log.getLogWriter().info("Waiting for " + TestHelper.regionToString(aRegion, false) + ".get(" + key + 
       ") to have a value of " + TestHelper.toString(expectedObjectValue));
   long startTime = System.currentTimeMillis();
   long lastLogTime = startTime;
   long logIntervalMS = 15000;
   while (true) {
      Object value = CacheUtil.get(aRegion, key);
      if (value == null) {
         if (expectedObjectValue == null) {
            Log.getLogWriter().info(TestHelper.regionToString(aRegion, false) + ".get(" + key + 
               ") has expected value " + value);
            return;
         }
      } else if (value.equals(expectedObjectValue)) {
         Log.getLogWriter().info(TestHelper.regionToString(aRegion, false) + ".get(" + key + 
            ") has expected value " + value);
         return;
      }
      long currentWaitTime = System.currentTimeMillis() - startTime;
      if (currentWaitTime >= timeoutMS) {
         throw new TestException("After waiting " + currentWaitTime + " millis, " + 
            TestHelper.regionToString(aRegion, false) + ".get(" + key + ") is " + TestHelper.toString(value) + 
            "; expected it to be " + TestHelper.toString(expectedObjectValue));
      }
      if (System.currentTimeMillis() - lastLogTime >= logIntervalMS) {
         Log.getLogWriter().info("Waiting for " + TestHelper.regionToString(aRegion, false) + ".get(" + key + 
             ") to have a value of " + TestHelper.toString(expectedObjectValue));
         lastLogTime = System.currentTimeMillis();
      }
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException ie) {
        throw new TestException("Interrupted after waiting " + currentWaitTime + " millis for " +
            TestHelper.regionToString(aRegion, false) + ".get(" + key +
            ") to have a value of " + TestHelper.toString(expectedObjectValue));
      }
   }
}

/** Returns the name of the gemfire system for this VM */
public static String getGemFireName() {
   String clientName = System.getProperty( "clientName" );
   ClientDescription cd = TestConfig.getInstance().getClientDescription( clientName );  
   GemFireDescription gfd = cd.getGemFireDescription();
   return gfd.getName();
}

//======================================================================== 
// Region methods

/** Given a Region, return a string logging its name and attributes (if
 *  specified by the arguments)
 *
 *  @param aRegion Return a string describing this region.
 *  @param showAttributes True if the string should include aRegion's attributes,
 *         false otherwise.
 *  
 *  @return A String representing aRegion.
 */
public static String regionToString(Region aRegion, boolean showAttributes) {
   StringBuffer aStr = new StringBuffer();
   aStr.append(aRegion.getFullPath());
   try {
      if (aRegion.getAttributes().getPartitionAttributes() != null)
         aStr.append(" (PartitionedRegion)");
      if (showAttributes)
         return aStr.toString() + " with attributes " + TestHelper.regionAttributesToString(aRegion.getAttributes());
      else
         return aStr.toString();
   } catch (RegionDestroyedException e) {
      boolean destroyed = aRegion.isDestroyed();
      if (destroyed)
         return aRegion.getFullPath() + ": attributes for region not available because region was destroyed";
      else
         throw new TestException("Got " + e + " but region is destroyed is " + destroyed);
   } 
   catch (CacheRuntimeException e) {
      ExceptionVersionHelper.checkCancelException(e);
      return aRegion.getFullPath() + ": attributes for region not available because cache was closed";
   }
}

/** Given a Region, return a string logging its name and attributes (if
 *  specified by the arguments) and all its subregions.
 *
 *  @param aRegion Return a string describing this region and all its subregions.
 *  @param showAttributes True if the string should include the regions' attributes,
 *         false otherwise.
 *  
 *  @return A String representing aRegion and all its subregions.
 */
public static String regionsToString(Region aRegion, boolean showAttributes) {
   StringBuffer aStr = new StringBuffer();
   aStr.append(regionToString(aRegion, showAttributes) + " with subregions ");
   Set regionSet = null;
   try {
      regionSet = aRegion.subregions(true);
   } catch (RegionDestroyedException e) {
      boolean isDestroyed = aRegion.isDestroyed();
      if (aRegion.isDestroyed()) {
         return "<" + aRegion.getFullPath() + " destroyed>";
      } else {
         throw new TestException("Getting subregions of region [" + aStr + "...] got " + e.toString() + ", but isDestroyed is " + isDestroyed);
      } 
   }
   Iterator it = regionSet.iterator();
   if (!it.hasNext()) {
      aStr.append("NONE");
      return aStr.toString();
   }
   aStr.append("\n");

   TreeSet aSet = new TreeSet();
   while (it.hasNext()) {
      aSet.add("   " + regionToString((Region)it.next(), showAttributes) + "\n");
   }
   int count = 0;
   it = aSet.iterator();
   while (it.hasNext()) {
      aStr.append(it.next());
      count++;
   }
   aStr.append("Total: " + count + " regions");
   return aStr.toString();   
}

/** Given RegionAttributes, return a string logging its configuration.
 *
 *  @param attr Return a string describing this region.
 *  
 *  @return A String representing aRegion.
 */
public static String regionAttributesToString(RegionAttributes attr) {
   StringBuffer aStr = new StringBuffer();
   aStr.append(attr + "\n");
   aStr.append("   cacheListeners: \n");
   CacheListener[] listeners = attr.getCacheListeners();
   for (int i=0; i < listeners.length; i++) {
     aStr.append("      " +  listeners[i] + "\n");
   }
   aStr.append("   cacheLoader: " + attr.getCacheLoader() + "\n");
   aStr.append("   cacheWriter: " + attr.getCacheWriter() + "\n");
   aStr.append("   enableOffHeapMemory: " + attr.getEnableOffHeapMemory() + "\n");
   aStr.append("   entryIdleTimeout: " + attr.getEntryIdleTimeout() + "\n");
   aStr.append("   entryTimeToLive: " + attr.getEntryTimeToLive() + "\n");
   aStr.append("   evictionAttributes: " + attr.getEvictionAttributes() + "\n");
   aStr.append("   initialCapacity: " + attr.getInitialCapacity() + "\n");
   aStr.append("   keyConstraint: " + attr.getKeyConstraint() + "\n");
   aStr.append("   loadFactor: " + attr.getLoadFactor() + "\n");
   aStr.append("   enableOffHeapMemory: " + attr.getEnableOffHeapMemory() + "\n");
   aStr.append("   dataPolicy: " + attr.getDataPolicy() + "\n");
   String concurrencyChecksEnabled = getConcurrencyChecksEnabled(attr);
   if (concurrencyChecksEnabled != null) {    // support for this attribute starts in GemFire 7.0
      aStr.append(concurrencyChecksEnabled);
   }
   SubscriptionAttributes subAttr = attr.getSubscriptionAttributes();
   if (subAttr == null)
      aStr.append("   SubscriptionAttributes (interestPolicy): " + subAttr + "\n");
   else 
      aStr.append("   interestPolicy: " + subAttr.getInterestPolicy() + "\n");
   aStr.append("   regionIdleTimeout: " + attr.getRegionIdleTimeout() + "\n");
   aStr.append("   regionTimeToLive: " + attr.getRegionTimeToLive() + "\n");
   aStr.append("   scope: " + attr.getScope() + "\n");
   aStr.append("   enableAsyncConflation: " + attr.getEnableAsyncConflation() + "\n");
   aStr.append("   enableSubscriptionConflation: " + attr.getEnableSubscriptionConflation() + "\n");
   aStr.append("   enableWAN: " + attr.getEnableWAN() + "\n");
   aStr.append("   statisticsEnabled: " + attr.getStatisticsEnabled() + "\n");
   PartitionAttributes parAttr = attr.getPartitionAttributes();
   if (parAttr == null) {
      aStr.append("   PartitionAttributes: " + parAttr + "\n");
   } else {
      aStr.append("   PartitionAttributes:\n");
      // aStr.append("      cacheLoader: " + parAttr.getCacheLoader() + "\n"); 
      // aStr.append("      cacheWriter: " + parAttr.getCacheWriter() + "\n"); 
      // aStr.append("      entryIdleTimeout: " + parAttr.getEntryIdleTimeout() + "\n");
      // aStr.append("      entryTimeToLive: " + parAttr.getEntryTimeToLive() + "\n");
      aStr.append("      redundantCopies: " + parAttr.getRedundantCopies() + "\n");
      aStr.append("      localProperties: " + parAttr.getLocalProperties() + "\n");
      aStr.append("      globalProperties: " + parAttr.getGlobalProperties() + "\n");
      aStr.append("      partitonResolver: " + parAttr.getPartitionResolver()+ "\n");
      aStr.append("      colocatedWith: " + parAttr.getColocatedWith() + "\n");
      aStr.append("      recoveryDelay: " + parAttr.getRecoveryDelay() + "\n");
      aStr.append("      startupRecoveryDelay: " + parAttr.getStartupRecoveryDelay() + "\n");
   }
   aStr.append("   multicastEnabled: " + attr.getMulticastEnabled() + "\n");
   aStr.append("   persistBackup: " + attr.getPersistBackup() + "\n");
   if (attr.getDiskWriteAttributes() != null) {
      aStr.append("   DiskWriteAttributes.isSynchronous: " + attr.getDiskWriteAttributes().isSynchronous() + "\n");
      aStr.append("   DiskWriteAttributes.timeInterval: " + attr.getDiskWriteAttributes().getTimeInterval() + "\n");
      aStr.append("   DiskWriteAttributes.bytesThreshold: " + attr.getDiskWriteAttributes().getBytesThreshold() + "\n");
      aStr.append("   diskFiles: ");
      File[] fileArr = attr.getDiskDirs();
      for (int i = 0; i < fileArr.length; i++) {
         aStr.append(fileArr[i].getAbsolutePath() + " ");
      }
      aStr.append("\n");
   }
  
   MembershipAttributes ratts = attr.getMembershipAttributes();
   if (ratts != null) {
      aStr.append("   membershipAttributes: " + ratts + "\n");
   }
   return aStr.toString();
}

/** Display string for concurrencyChecksEnabled.  
 *  Support for this attribute was introduced in GemFire 7.0 (previous versions will not support)
 *
 *  @return A String to display the concurrencyChecksEnabled setting (see regionAttributesToString)
 */
public static String getConcurrencyChecksEnabled(RegionAttributes attrs) {
   return TestHelperVersionHelper.getConcurrencyChecksEnabled(attrs);
}

/** Given a lossAction, return the corresponding LossAction
 * 
 *  @param lossAction The loss action as a string
 *
 *  @return A LossAction
 */
public static LossAction getLossAction(String lossAction) {
   if (lossAction.equalsIgnoreCase("fullAccess")) {
      return LossAction.FULL_ACCESS;
   } else if (lossAction.equalsIgnoreCase("limitedAccess")) {
      return LossAction.LIMITED_ACCESS;
   } else if (lossAction.equalsIgnoreCase("noAccess")) {
      return LossAction.NO_ACCESS;
   } else if (lossAction.equalsIgnoreCase("reconnect")) {
     return LossAction.RECONNECT;
   } else {
      throw new TestException("Unknown lossAction " + lossAction);
   }
}

/** Given a resumptionAction, return the corresponding ResumptionAction
 *
 *  @param resumptionAction The resumptionAction as a string
 *
 *  @return A ResumptionAction
 */
public static ResumptionAction getResumptionAction(String resumptionAction) {
   if (resumptionAction.equalsIgnoreCase("none")) {
     return ResumptionAction.NONE;
   } else if (resumptionAction.equalsIgnoreCase("reinitialize")) {
     return ResumptionAction.REINITIALIZE;
   } else {
      throw new TestException("Unknown reliabilityAction " + resumptionAction);
   }
}

/** Given an action, return the corresponding ExpirationAction.
 *
 *  @param action The action as a String.
 *
 *  @return An ExpirationAction  
 */
public static ExpirationAction getExpirationAction(String action) {
   if (action.equalsIgnoreCase("destroy"))
      return ExpirationAction.DESTROY;
   else if ((action.equalsIgnoreCase("local_destroy")) || (action.equalsIgnoreCase("localDestroy")))
      return ExpirationAction.LOCAL_DESTROY;
   else if (action.equalsIgnoreCase("invalidate"))
      return ExpirationAction.INVALIDATE;
   else if ((action.equalsIgnoreCase("local_invalidate")) || (action.equalsIgnoreCase("localInvalidate")))
      return ExpirationAction.LOCAL_INVALIDATE;
   else
      throw new TestException("Unknown action " + action);
}

/** Given a result policy as a string, return the corresponding InterestResultPolicy.
 *
 *  @param policy The result policy as a String.
 *
 *  @return An InterestResultPolicy.
 */
public static InterestResultPolicy getResultPolicy(String policy) {
   if (policy.equalsIgnoreCase("keys"))
      return InterestResultPolicy.KEYS;
   else if ((policy.equalsIgnoreCase("keys_values")) || (policy.equalsIgnoreCase("keysValues")))
      return InterestResultPolicy.KEYS_VALUES;
   else if (policy.equalsIgnoreCase("none"))
      return InterestResultPolicy.NONE;
   else
      throw new TestException("Unknown result policy " + policy);
}

/** Given a mirroring attribute, return the corresponding MirrorType.
 *
 *  @param attribute The mirroring attribute.
 *
 *  @return A MirrorType 
 */
public static MirrorType getMirrorType(String attribute) {
   if (attribute.equalsIgnoreCase("keys"))
      return MirrorType.KEYS;
   else if ((attribute.equalsIgnoreCase("keys_values")) || (attribute.equalsIgnoreCase("keysValues")))
      return MirrorType.KEYS_VALUES;
   else if (attribute.equalsIgnoreCase("none"))
      return MirrorType.NONE;
   else
      throw new TestException("Unknown mirroring attribute " + attribute);
}

/** Given a dataPolicy attribute, return the corresponding DataPolicy.
 *
 *  @param attribute The dataPolicy attribute.
 *
 *  @return A DataPolicy
 */
public static DataPolicy getDataPolicy(String attribute) {
   if (attribute.equalsIgnoreCase("normal"))
      return DataPolicy.NORMAL;
   else if (attribute.equalsIgnoreCase("preloaded"))
      return DataPolicy.PRELOADED;
   else if (attribute.equalsIgnoreCase("empty")) 
      return DataPolicy.EMPTY;
   else if (attribute.equalsIgnoreCase("replicate")) 
      return DataPolicy.REPLICATE;
   else if ((attribute.equalsIgnoreCase("persistent_replicate")) || 
            (attribute.equalsIgnoreCase("persistentReplicate")))
      return DataPolicy.PERSISTENT_REPLICATE;
   else if ((attribute.equalsIgnoreCase("persistentPartition")))
     return DataPolicy.PERSISTENT_PARTITION;
   else
      throw new TestException("Unknown dataPolicy attribute " + attribute);
}
/** Given a interestPolicy attribute, return the corresponding InterestPolicy.
 *
 *  @param attribute The interestPolicy attribute.
 *
 *  @return A InterestPolicy
 */
public static InterestPolicy getInterestPolicy(String attribute) {
   if (attribute.equalsIgnoreCase("all"))
      return InterestPolicy.ALL;
   else if ((attribute.equalsIgnoreCase("cache_content")) || 
            (attribute.equalsIgnoreCase("cacheContent")))
      return InterestPolicy.CACHE_CONTENT;
   else
      throw new TestException("Unknown interestPolicy attribute " + attribute);
}
/** Given a interestResultPolicy attribute, return the corresponding
 *  InterestResultPolicy.
 *
 *  @param attribute The interestResultPolicy attribute.
 *
 *  @return A InterestResultPolicy
 */
public static InterestResultPolicy getInterestResultPolicy(String attribute) {
   if (attribute.equalsIgnoreCase("none"))
      return InterestResultPolicy.NONE;
   else if (attribute.equalsIgnoreCase("keys"))
      return InterestResultPolicy.KEYS;
   else if ((attribute.equalsIgnoreCase("keys_values")) || 
            (attribute.equalsIgnoreCase("keysValues")))
      return InterestResultPolicy.KEYS_VALUES;
   else
      throw new TestException("Unknown interestResultPolicy attribute " + attribute);
}

/** Given a scope attribute, return the corresponding Scope.
 *
 *  @param attribute The scope attribute.
 *
 *  @return A Scope 
 */
public static Scope getScope(String attribute) {
   if ((attribute.equalsIgnoreCase("distributed_ack")) || 
       (attribute.equalsIgnoreCase("distributedAck")) || 
       (attribute.equalsIgnoreCase("ack")))
      return Scope.DISTRIBUTED_ACK;
   else if ((attribute.equalsIgnoreCase("distributed_no_ack")) ||
            (attribute.equalsIgnoreCase("distributedNoAck")) ||
            (attribute.equalsIgnoreCase("noAck")))
      return Scope.DISTRIBUTED_NO_ACK;
   else if (attribute.equalsIgnoreCase("local"))
      return Scope.LOCAL;
   else if (attribute.equalsIgnoreCase("global"))
      return Scope.GLOBAL;
   else
      throw new TestException("Unknown scope attribute \"" + attribute + "\"");
}

/** Given a region, get the number of keys in the region and all subregions of the region.
 *
 *  @param aRegion Get the number of keys in this region and all subregions of this region.
 *
 *  @return int The number of keys in this region and all subregions of this region.
 */
public static int getTotalNumKeys(Region aRegion) {
   int numKeys = 0;
   Set regionSet = aRegion.subregions(true);
   Iterator it = regionSet.iterator();
   while (it.hasNext()) {
      numKeys += (((Region)it.next()).keys().size());
   }
   return numKeys;
}

/** Given a region, return a set containing all keys in the region and all subregions of the region.
 *
 *  @param aRegion Get the keys in this region and all subregions of this region.
 *
 *  @return Set The keys in this region and all subregions of this region.
 */
public static Set getAllKeys(Region aRegion) {
   Set keySet = new HashSet();
   Set regionSet = aRegion.subregions(true);
   Iterator it = regionSet.iterator();
   while (it.hasNext()) {
      Region thisRegion = (Region)it.next();
      keySet.addAll(thisRegion.keys());
   }
   return keySet;
}

/** Using TestHelper.EVENT_ERROR_KEY, look in the sharedMap of the given 
 *  blackboard for any error strings and throw an error if found.
 *
 *  This is typically used with event listeners. Errors thrown from an event 
 *  listener do not throw back to the client.  Tests can implement listeners 
 *  that put error strings into a blackboard shared map, then the client can 
 *  call this method periodically to check for errors.
 *
 *  @param bb The blackboard to check.
 *
 *  @throws TestException if any error string was found in the bb using
 *          key TestHelper.EVENT_ERROR_KEY
 */
public static void checkForEventError(Blackboard bb) {
   hydra.blackboard.SharedMap aMap = bb.getSharedMap();   
   Object anObj = aMap.get(TestHelper.EVENT_ERROR_KEY);
   if (anObj != null) {
      throw new TestException(anObj.toString());
   } 
}

/** Return the number of LRU evictions that have occurred.
 *
 *  @return The number of LRU evictions, obtained from stats.
 *
 */
public static double getNumLRUEvictions() {
   String spec = "* " // search all archives
                 + "LRUStatistics "
                 + "* " // match all instances
                 + "lruEvictions "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double totalEvictions = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
//      Log.getLogWriter().info(stat.toString());
   }
   return totalEvictions;
}

/** Return the number of MemLRU evictions that have occurred.
 *
 *  @return The number of MemLRU evictions, obtained from stats.
 *
 */
public static double getNumMemLRUEvictions() {
   String spec = "*bridge* " // search all BridgeServer archives
                 + "MemLRUStatistics "
                 + "* " // match all instances
                 + "lruEvictions "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double totalEvictions = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
   }
   return totalEvictions;
}

/** Return the number of HeapLRU evictions that have occurred.
 *
 *  @return The number of HeapLRU evictions, obtained from stats.
 *
 */
public static double getNumHeapLRUEvictions() {
   String spec = "* " // search all archives
                 + "HeapLRUStatistics "
                 + "* " // match all instances
                 + "lruEvictions "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double totalEvictions = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      totalEvictions += stat.getMax();
   }
   return totalEvictions;
}

/** Return the number of loadsCompleted
 *
 *  @return The number of loadsCompleted from servers, obtained from product cachePerfStats.
 *
 */
public static double getNumLoadsCompleted() {
   String spec = "*bridge* " // search all BridgeServer archives
                 + "CachePerfStats "
                 + "cachePerfStats " // match all cache instances
                 + "loadsCompleted "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double loadsCompleted = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      loadsCompleted += stat.getMax();
   }
   return loadsCompleted;
}

/** Return the number of CQs (CqServiceStats.numCQsCreated)
 *
 *  @return The total number of CQs (CqServiceStats.numCQsCreated) for all client VMs
 *
 */
public static double getNumCQsCreated() {
   String spec = "*edge* " // search all archives
                 + "CqServiceStats "
                 + "* " // match all instances
                 + "numCQsCreated "
                 + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                 + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                 + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   List aList = PerfStatMgr.getInstance().readStatistics(spec);
   if (aList == null) {
      Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
      return 0.0;
   }
   double numCqs = 0;
   for (int i = 0; i < aList.size(); i++) {
      PerfStatValue stat = (PerfStatValue)aList.get(i);
      numCqs += stat.getMax();
   }
   return numCqs;
}

/** Return the number of VMs in this test
 *
 */
public static int getNumVMs() {
   int numVMs = 0;
   Vector gemFireNamesVec = TestConfig.tab().vecAt(GemFirePrms.names);
   Vector numVMsVec = TestConfig.tab().vecAt(ClientPrms.vmQuantities);
   if (gemFireNamesVec.size() == numVMsVec.size()) {
      for (int i = 0; i < numVMsVec.size(); i++) {
         numVMs = numVMs + (new Integer(((String)numVMsVec.elementAt(i)))).intValue();
      }
   } else {
      numVMs = new Integer((String)(numVMsVec.elementAt(0))).intValue() * gemFireNamesVec.size();
   }
   return numVMs;
}

/** Return the number of threads in this test
 *
 */
public static int getNumThreads() {
   int numThreads = 0;
   int numLocatorThreads = 0;
   Vector gemFireNamesVec = TestConfig.tab().vecAt(GemFirePrms.names);
   Vector numVMsVec = TestConfig.tab().vecAt(ClientPrms.vmQuantities);
   Vector vmThreadsVec = TestConfig.tab().vecAt(ClientPrms.vmThreads);
   if ((gemFireNamesVec.size() > 1) ||
       (numVMsVec.size() > 1) ||
       (vmThreadsVec.size() > 1)) {
      int gemFireSize = gemFireNamesVec.size();
      int numVMsSize = numVMsVec.size();
      int vmThreadsSize = vmThreadsVec.size();
      int maxSize = Math.max(Math.max(gemFireSize, numVMsSize), vmThreadsSize);
      for (int i = 0; i < maxSize; i++) {
         int numVMsForThisGF = 0;
         int numThreadsPerVM = 0;
         if (i < numVMsSize)
            numVMsForThisGF = new Integer((String)(numVMsVec.elementAt(i))).intValue();
         else
            numVMsForThisGF = new Integer((String)(numVMsVec.elementAt(numVMsSize-1))).intValue();
         if (i < vmThreadsSize)
            numThreadsPerVM = new Integer((String)(vmThreadsVec.elementAt(i))).intValue();
         else
            numThreadsPerVM = new Integer((String)(vmThreadsVec.elementAt(numVMsSize-1))).intValue();
         numThreads += (numVMsForThisGF * numThreadsPerVM);
         if (TestConfig.tab().booleanAt(RollingUpgradePrms.excludeLocatorThreadsFromTotalThreads, false)) {
           String gfName = (String)gemFireNamesVec.elementAt(i);
           if (gfName.startsWith("locator")) {
             numThreads -= (numVMsForThisGF * numThreadsPerVM);
           }
         }
      }
   } else {
      numThreads = (new Integer((String)(numVMsVec.elementAt(0)))).intValue() * 
                   (new Integer((String)vmThreadsVec.elementAt(0))).intValue() *
                   gemFireNamesVec.size();
   }
   
   return numThreads;
}

/** Get the size in bytes of anObj.
 * 
 *  @param anObj Determine the size in bytes of this object.
 *
 *  @return The number of bytes of anObj.
 */
public static int getSizeInBytes(Object anObj) {
   try {

      // serialize
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(anObj);
      byte[] byteArray = baos.toByteArray();
      return byteArray.length;
   } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Returns the current stat value of DiskStoreStatistics(diskStoreName).recoveriesInProgress
 *  This is done by getting the stat directly without going to the archive 
 *  file, thus the stat is local to this cache and cannot be combined across
 *  archives, but it is faster than going to the archive.
 *
 *  @return - the current value of DiskStoreStatistics(diskStoreName).recoveriesInProgress
 */
public static int getStat_getRecoveriesInProgress(String diskStoreName) {

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
       statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the stat to be created
    Statistics[] diskStoreStats = statFactory.findStatisticsByTextId(diskStoreName);
    while (diskStoreStats.length == 0) {
       diskStoreStats = statFactory.findStatisticsByTextId(diskStoreName);
    }
    int diskRecoveryInProgress = diskStoreStats[0].getInt("recoveriesInProgress");
    return diskRecoveryInProgress;
}

/** Returns the current stat value of CachePerfStats.getIntialImagesInProgress.
 *  This is done by getting the stat directly without going to the archive 
 *  file, thus the stat is local to this cache and cannot be combined across
 *  archives, but it is faster than going to the archive.
 *
 *  @return - the current value of CachePerfStats.getIntialImagesInProgress
 */
public static int getStat_getInitialImagesInProgress(String regionName) {
    String regionStatName = "RegionStats-" + regionName;

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
       statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the regionStats to be created
    Statistics[] regionStats = statFactory.findStatisticsByTextId(regionStatName);
    while (regionStats.length == 0) {
       regionStats = statFactory.findStatisticsByTextId(regionStatName);
    }
    int giisInProgress = regionStats[0].getInt("getInitialImagesInProgress");
    return giisInProgress;
}
   
/** Returns the current stat value of CachePerfStats.getIntialImagesCompleted.
 *  This is done by getting the stat directly without going to the archive 
 *  file, thus the stat is local to this cache and cannot be combined across
 *  archives, but it is faster than going to the archive.
 *
 *  @return - the current value of CachePerfStats.getIntialImagesCompleted
 */
public static int getStat_getInitialImagesCompleted(String regionName) {
    String regionStatName = "RegionStats-" + regionName;

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
       statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the regionStats to be created
    Statistics[] regionStats = statFactory.findStatisticsByTextId(regionStatName);
    while (regionStats.length == 0) {
       regionStats = statFactory.findStatisticsByTextId(regionStatName);
    }
    int giisCompleted = regionStats[0].getInt("getInitialImagesCompleted");
    return giisCompleted;
}

  /** Returns the current stat value of CachePerfStats.deltaGetIntialImagesCompleted.
   *  This is done by getting the stat directly without going to the archive
   *  file, thus the stat is local to this cache and cannot be combined across
   *  archives, but it is faster than going to the archive.
   *
   *  @return - the current value of CachePerfStats.deltaGetIntialImagesCompleted
   */
  public static int getStat_deltaGetInitialImagesCompleted(String regionName) {
    String regionStatName = "RegionStats-" + regionName;

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
      statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the regionStats to be created
    Statistics[] regionStats = statFactory.findStatisticsByTextId(regionStatName);
    while (regionStats.length == 0) {
      regionStats = statFactory.findStatisticsByTextId(regionStatName);
    }
    int deltaGiisCompleted = regionStats[0].getInt("deltaGetInitialImagesCompleted");
    Log.getLogWriter().info("getStat_deltaGetInitialImagesCompleted() returns " + deltaGiisCompleted);
    return deltaGiisCompleted;
  }

 /** Returns the current stat value of CachePerfStats.getIntialImageTime.
 *  This is done by getting the stat directly without going to the archive 
 *  file, thus the stat is local to this cache and cannot be combined across
 *  archives, but it is faster than going to the archive.
 *
 *  @return - the current value of CachePerfStats.getIntialImageTime
 */
public static long getStat_getInitialImageTime(String regionName) {
    String regionStatName = "RegionStats-" + regionName;

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
       statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the regionStats to be created
    Statistics[] regionStats = statFactory.findStatisticsByTextId(regionStatName);
    while (regionStats.length == 0) {
       regionStats = statFactory.findStatisticsByTextId(regionStatName);
    }
    long giiTime = regionStats[0].getLong("getInitialImageTime");
    return giiTime;
}

/** Create an instance of the given class name. It is assumed that the class has a 
 *  no arg constructor.
 *
 *  @param className The class to create an instance of.
 */
public static Object createInstance(String className) {
   try {
      return createInstance(Class.forName(className));
   } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Create an instance of the given class. It is assumed that the class has a 
 *  no arg constructor.
 *
 *  @param aClass The class to create an instance of.
 */
public static Object createInstance(Class aClass) {
   try {
      Object anObj = aClass.newInstance();
      return anObj;
   } catch (java.lang.InstantiationException anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   } catch (java.lang.IllegalAccessException anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   } catch (Exception anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   }
}

/** Calculate the size of a String (often used by the tests for a key)
 *  for use by tests that track bytes for eviction. This is in parallel
 *  with how the product determines the size of a String.
 * @param aStr The String to calculate
 * @return The number of bytes the product allocates for aStr.
 */
public static int calculateStringSize(String aStr) {
  return WellKnownClassSizer.sizeof(aStr);
}
/** Given a Throwable, return the last cause or the given Throwable if no
 *  caused by's. 
 * @param e The Throwable for which to find the last caused by.
 * @return The last caused by for e, or e if no caused by's. 
 */
public static Throwable getLastCausedBy(Throwable e) {
  Throwable cause = e;
  while (cause != null) {
    Throwable nextCause = cause.getCause();
    if (nextCause == null) {
      // cause is the last non-null cause
      return cause;
    }
    cause = nextCause;
  }
  return cause;
}

/** Log all regions in the cache
 * 
 */
public static void logRegionHierarchy() {
  Log.getLogWriter().info(regionHierarchyToString());
}

/** Return a string representation of the region hierarchy for this test. 
 * 
 */
public static String regionHierarchyToString() {
  Cache theCache = CacheHelper.getCache();
  if (theCache == null) {
    return "Unable to log regions, this member does not have a cache";
  }
  Set<Region<?,?>> roots = theCache.rootRegions();
  StringBuffer aStr = new StringBuffer();
  int totalNumRegions = 0;
  for (Region aRegion: roots) {
    aStr.append(getRegionStr(aRegion));
    totalNumRegions++;
    Object[] tmp = subregionsToString(aRegion, 1);
    aStr.append(tmp[0]);
    totalNumRegions += (Integer)(tmp[1]);
  }
  return "Region hierarchy with " + totalNumRegions + " regions\n" + aStr.toString();
}

/** Return a String describing important attributes of the given region.
 * 
 * @param aRegion The region to describe.
 */
private static String getRegionStr(Region aRegion) {
  StringBuffer aStr = new StringBuffer();
  aStr.append(aRegion.getFullPath() + "  (size " + aRegion.size() + " dataPolicy " +
      aRegion.getAttributes().getDataPolicy() + " eviction " + aRegion.getAttributes().getEvictionAttributes());
  PartitionAttributes prAttr = aRegion.getAttributes().getPartitionAttributes();
  if (prAttr != null) {
    aStr.append(" localMaxMemory=" + aRegion.getAttributes().getPartitionAttributes().getLocalMaxMemory());
    aStr.append(" redundantCopies=" + aRegion.getAttributes().getPartitionAttributes().getRedundantCopies());
    if (prAttr.getColocatedWith() != null) {
      aStr.append(" colocatedWith=" + prAttr.getColocatedWith());
    }
  }
  RecoveryTestVersionHelper.logDiskStore(aRegion, aStr);
  aStr.append(")\n");
  return aStr.toString();
}

/** Given a region and the current level in the region hierarchy, return a String
 *   representation of the region's subregions. 
 *   
 * @param region The parent whose children are return as a String.
 * @param level The current hiearachy level.
 * @return [0] A String representation of region's subregions.
 *                 [1] The number of subregions in [0]
 */
private static Object[] subregionsToString(Region region, int level) {
  Set<Region> subregionSet = region.subregions(false);
  StringBuffer aStr = new StringBuffer();
  int numRegions = subregionSet.size();
  for (Region reg: subregionSet) {
    for (int i = 1; i <= level; i++) {
      aStr.append("  ");
    }
    aStr.append(getRegionStr(reg));
    Object[] tmp = subregionsToString(reg, level+1);
    aStr.append(tmp[0]);
    numRegions += (Integer)(tmp[1]);
  }
  return new Object[] {aStr.toString(), numRegions};
}

}
