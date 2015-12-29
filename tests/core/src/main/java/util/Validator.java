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

//import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import java.util.*;
//import com.gemstone.gemfire.internal.NanoTimer;

public class Validator {

// constants used to specify how to validate
public static final long VALIDATE_ALL = Long.MAX_VALUE;
public static final int POSITIVE = 0;
public static final int NEGATIVE = 1;
public static final int BOTH = 2;
public static final int NONE = 3;
public static final int IF_HAS_VALUE = 4;
public static final int ALWAYS = 5;
public static final int NEVER = 7;
public static final int SOMETIMES = 8;
public static final int DONT_CHECK = 9;

// instance fields
protected int containsKeyCategory = -1; // can be POSITIVE, NEGATIVE, BOTH or NONE
protected int containsValueForKeyCategory = -1; // can be POSITIVE, NEGATIVE, BOTH, NONE
protected int doGetStrategy = -1; // can be IF_HAS_VALUE or ALWAYS
protected int valueShouldBeNull = -1;
protected boolean stableKeys = true;
protected boolean waitForContainsKey = true;
protected boolean waitForContainsValueForKey = true;
protected boolean throwOnFirstError = true;
protected CacheBB blackboard = null;
protected long maxNumberToValidate = VALIDATE_ALL;
protected long timeLimitMS = 0;

private Set keySet = null;
private int numKeysPresent = 0;
private int numValuesPresent = 0;
private hydra.blackboard.SharedCounters sharedCounters = null;
private StringBuffer errStr = null;

public Validator() {
   timeLimitMS = (TestConfig.tab().intAt(hydra.Prms.maxResultWaitSec) * 1000) - 60000;
}

/**
 *  Set containsKeyCategory. Specifies the nature of the keys in the region 
 *         which should return true for containsKey(...).
 */
public void setContainsKeyCategory(int containsKeyCategory) {
   boolean valid = (containsKeyCategory == POSITIVE) || (containsKeyCategory == NEGATIVE) ||
                   (containsKeyCategory == BOTH) || (containsKeyCategory == NONE) ||
                   (containsKeyCategory == DONT_CHECK);
   if (!valid) throw new TestException("Invalid containsKeyCategory " + containsKeyCategory);
   this.containsKeyCategory = containsKeyCategory;
}

/**
 *  Set containsValueForKeyCategory. Specifies the nature of the objects in the region which
 *         should return true for containsValueForKey(...).
 */
public void setContainsValueForKeyCategory(int containsValueForKeyCategory) {
   boolean valid = (containsValueForKeyCategory == POSITIVE) || (containsValueForKeyCategory == NEGATIVE) ||
                   (containsValueForKeyCategory == BOTH) || (containsValueForKeyCategory == NONE) ||
                   (containsValueForKeyCategory == DONT_CHECK);
   if (!valid) throw new TestException("Invalid containsValueForKeyCategory " + containsValueForKeyCategory);
   this.containsValueForKeyCategory = containsValueForKeyCategory;
}

/**
 *  Set doGetStrategy. Specifies the stategy used to verify the object
 *         associated with a key.
 */
public void setDoGetStrategy(int doGetStrategy) {
   boolean valid = (doGetStrategy == IF_HAS_VALUE) || (doGetStrategy == ALWAYS);
   if (!valid) throw new TestException("Invalid doGetStrategy " + doGetStrategy);
   this.doGetStrategy = doGetStrategy;
}

/**
 *  Set valueShouldBeNull. Specifies if values should always, never or sometimes be null.
 */
public void setValueShouldBeNull(int valueShouldBeNull) {
   boolean valid = (valueShouldBeNull == ALWAYS) || (valueShouldBeNull == NEVER) || (valueShouldBeNull == SOMETIMES);
   if (!valid) throw new TestException("Invalid valueShouldBeNull " + valueShouldBeNull);
   this.valueShouldBeNull = valueShouldBeNull;
}

/**
 *  Set stableKeys. True if the keys can change during execution of
 *         the validator (ie concurrent execution test), false otherwise
 */
public void setStableKeys(boolean stableKeys) {
   this.stableKeys = stableKeys;
}

/**
 *  Set waitForContainsKey. True if the validator should wait for containsKey to become
 *         true when it is expected to be true but is currently false.
 */
public void setWaitForContainsKey(boolean waitForContainsKey) {
   this.waitForContainsKey = waitForContainsKey;
}

/**
 *  Set waitForContainsValueForKey. True if the validator should wait for containsValueForKey to become
 *         true when it is expected to be true but is currently false.
 */
public void setWaitForContainsValueForKey(boolean waitForContainsValueForKey) {
   this.waitForContainsValueForKey = waitForContainsValueForKey;
}

/**
 *  Set throwOnFirstError. True if the validator should throw an error on the first
 *         error encountered, false if it should find all errors before it throws an error.
 */
public void setThrowOnFirstError(boolean throwOnFirstError) {
   this.throwOnFirstError = throwOnFirstError;
}

/**
 *  Set blackboard. This is the hydra blackboard to use for gathering limited performance
 *         information and error information for ownership timeouts, etc.
 */
public void setBlackboard(CacheBB bb) {
   this.blackboard = bb;
   this.sharedCounters = bb.getSharedCounters();
}

/**
 *  Set maxNumberToValidate. This indicates many entries in the region to validate.
 */
public void setMaxNumberToValidate(long maxNumberToValidate) {
   this.maxNumberToValidate = maxNumberToValidate;
}

/**
 *  Set timeLimitMS. This indicates how long to validate before returning.
 */
public void setTimeLimitMS(long timeLimitMS) {
   this.timeLimitMS = timeLimitMS;
}

/** Given a region and using {@link util.NameFactory}'s names and counters, iterate over 
 *  the expected names in the region, testing each according to this Validator
 *  instance. Throws an error if any problems are detected.
 *  
 *  @param aRegion The region containing the names generated by NameFactory.
 *
 *  @return Object[0] The number of keys encountered
 *          Object[1] The number of values 
 * 
 *  @throws TestException Throws an error if any of the validation fails.
 */
public Object[] validateRegionContents(Region aRegion) {
   Log.getLogWriter().info("In validateRegionContents with Validator: " + this);
   numKeysPresent = 0;
   numValuesPresent = 0;
   keySet = aRegion.keys();
   int beforeKeySetSize = keySet.size();
   long positiveCounter = NameFactory.getPositiveNameCounter();
   long negativeCounter = NameFactory.getNegativeNameCounter();
   errStr = new StringBuffer();
   logMessage("validateRegionContents: positiveCounter = " + positiveCounter +
       ", negativeCounter = " + negativeCounter);
   long valCount = 0;
   boolean endedEarly = false;
   long startTime = System.currentTimeMillis();
   for (long i = negativeCounter; i <= positiveCounter; i++) {
      if (i == 0) continue; // no name uses 0
      String name = NameFactory.getObjectNameForCounter(i);
      validateKey(aRegion, name);
      valCount++;
      if (valCount >= maxNumberToValidate) {
         logMessage("Ending validation because maxNumberToValidate " + maxNumberToValidate + 
                    " has been reached.");        
         endedEarly = true;
         break;
      }
      if (System.currentTimeMillis() - startTime >= timeLimitMS) {
         logMessage("Ending validation because time limit has been reached: " + timeLimitMS + " millis");
         endedEarly = true;
         break;
      }
   }
   keySet = aRegion.keys();
   int afterKeySetSize = keySet.size();
   logMessage("Key set size at beginning of validation: " + beforeKeySetSize + 
              ", after validation: " + afterKeySetSize);
   if (!endedEarly) {
      if (stableKeys) {
         if (numKeysPresent != afterKeySetSize) {
            logError("In validateRegionContents, expected key set size to be " + numKeysPresent + 
                     " but it was " + afterKeySetSize+ "\n" + getKeySetDiscrepancies(aRegion, keySet));
         }
      } else {
         if (numKeysPresent > afterKeySetSize) {
            logError("In validateRegionContents, expected key set size to be >= " + numKeysPresent + 
                     " but it was " + afterKeySetSize + "\n");
         }
      }
   }
   if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
   } else
      Log.getLogWriter().info("No errors detected by Validator");
   return new Object[] {new Integer(numKeysPresent), new Integer(numValuesPresent)};
}

void validateKey(Region aRegion, String key) {
   long waitTime = TestConfig.tab().longAt(CachePrms.keyValueWaitTime) * TestHelper.SEC_MILLI_FACTOR;
   hydra.blackboard.SharedCounters counters = blackboard.getSharedCounters();
   long nameCounter = NameFactory.getCounterForName(key);
   boolean containsKey = aRegion.containsKey(key); 
   boolean containsValue = aRegion.containsValueForKey(key); 
   logMessage("validateKey: containsKey(" + key + ")=" + containsKey + ", containsValueforKey(" + 
              key + ")=" + containsValue);
   
   if (containsKeyCategory == DONT_CHECK) {
      logMessage("Not checking containsKey because containsKeyCategory = " + containsKeyCategoryToString());
   } else { // wait for containsKey to become true
      boolean expectedContainsKey = (containsKeyCategory == BOTH) ||
                                    ((nameCounter > 0) && (containsKeyCategory == POSITIVE)) ||
                                    ((nameCounter < 0) && (containsKeyCategory == NEGATIVE));
      logMessage("validateKey: For key " + key + ", expectedContainsKey = " + expectedContainsKey);
      if (waitForContainsKey) {
         try {
            String logStr = TestHelper.waitForContainsKey(aRegion, key, expectedContainsKey, waitTime, false);
            logMessage(logStr);
         } catch (TestException e) {
            logError(e + TestHelper.getStackTrace(e));
         }
      } else if (expectedContainsKey) {
         if (!containsKey) {
            String logStr = "Expected containsKey for key " + key + " to be true, but it is " + containsKey;
            logError(logStr);
         }
      }
   }

   containsKey = aRegion.containsKey(key); 
   containsValue = aRegion.containsValueForKey(key); 
   // wait for containsValueForKey to become true if containsValueForKey is expected to be true
   if (containsValueForKeyCategory == DONT_CHECK) {
      logMessage("Not checking containsValueForKey because containsValueForKeyCategory = " + containsValueForKeyCategoryToString());
   } else {
      boolean expectedContainsValueForKey = (containsKey) && 
                                  ((containsValueForKeyCategory == BOTH) ||
                                  ((nameCounter > 0) && (containsValueForKeyCategory == POSITIVE)) ||
                                  ((nameCounter < 0) && (containsValueForKeyCategory == NEGATIVE))); 
      if (waitForContainsValueForKey) {
         logMessage("validateKey: For key " + key + ", expectedContainsValueForKey = " + 
            expectedContainsValueForKey + "; containsKey = " + containsKey + 
            ", containsValueForKeyCategory = " + containsValueForKeyCategory + 
            ", nameCounter = " + nameCounter);
         try {
            String logStr = TestHelper.waitForContainsValueForKey(aRegion, key, expectedContainsValueForKey, waitTime, false);
            logMessage(logStr);
         } catch (TestException e) {
            logError(e + TestHelper.getStackTrace(e));
         }
      }
   }

   // check that the key is in the keySet
   containsKey = aRegion.containsKey(key); 
   containsValue = aRegion.containsValueForKey(key); 
   if (containsKey) {
      boolean contains = keySet.contains(key);
      if (!contains) { 
         // try once more in case the key just wasn't here yet on the prior call to key set
         logMessage("key " + key + " not found in key set; getting key set again");
         keySet = aRegion.keys();
         contains = keySet.contains(key);
         // key must be here this time, otherwise containsKey check above would have failed
      }
      if (!contains) {
         logError("key " + key + " is not contained in key set\n"); 
      }
   }
   if (containsKey)
      numKeysPresent++; 
   if (containsValue)
      numValuesPresent++; 

   // check the value with a "get"
   if ((doGetStrategy == ALWAYS) || ((doGetStrategy == IF_HAS_VALUE) && containsValue)) {
      Object value = CacheUtil.get(aRegion, key);
      checkNameAndValue(key, value);
   }
}

/** Check a key and value from a region. The key contains the expected
 *  value (ie <someName>14 must have value 14). 
 *
 *  @param key The key for value.
 *  @param value The value for key.
 *  @throws TestException if the key/value fails validation for any reason.
 */
void checkNameAndValue(String key, Object value) {
   if (valueShouldBeNull == SOMETIMES)
      return;
   logMessage("Checking key " + key + ", value " + value);
   if (valueShouldBeNull == ALWAYS) {
      if (value == null) {
         return; // everything is OK
      } else { // value is not null
         logError("Expected value for key " + key + " to be null, but it was " + value + "\n");
         return;
      }
   }

   BaseValueHolder vh = null;
   Long aLong = null;
   if (value instanceof BaseValueHolder) {
      vh = (BaseValueHolder)value;
      long longValue = 0;
      if (vh.myValue instanceof Long) {
         aLong = (Long)vh.myValue;
         longValue = aLong.longValue();
      } else {
         logError("Expected value for key " + key + " to contain a Long, but it is " + TestHelper.toString(value) + "\n");
         return;
      }
   } else {
      logError("Expected value for key " + key + " to be a ValueHolder, but it is " + TestHelper.toString(value) + "\n");
   }

   Long nameCounter = new Long(NameFactory.getCounterForName(key));
   if (!nameCounter.equals(aLong)) {
      logError("key " + key + " and value " + TestHelper.toString(value) + " are not correct\n");
      return;
   }
}

private String containsKeyCategoryToString() {
   if (containsKeyCategory == NEGATIVE) return "NEGATIVE";
   if (containsKeyCategory == POSITIVE) return "POSITIVE";
   if (containsKeyCategory == BOTH) return "BOTH";
   if (containsKeyCategory == NONE) return "NONE";
   if (containsKeyCategory == DONT_CHECK) return "DONT_CHECK";
   throw new TestException("unknown containsKeyCategory " + containsKeyCategory);
}

private String containsValueForKeyCategoryToString() {
   if (containsValueForKeyCategory == NEGATIVE) return "NEGATIVE";
   if (containsValueForKeyCategory == POSITIVE) return "POSITIVE";
   if (containsValueForKeyCategory == BOTH) return "BOTH";
   if (containsValueForKeyCategory == NONE) return "NONE";
   if (containsValueForKeyCategory == DONT_CHECK) return "DONT_CHECK";
   throw new TestException("unknown containsValueForKeyCategory " + containsValueForKeyCategory);
}

private String doGetStrategyToString() {
   if (doGetStrategy == IF_HAS_VALUE) return "IF_HAS_VALUE";
   if (doGetStrategy == ALWAYS) return "ALWAYS";
   throw new TestException("unknown doGetStrategy " + doGetStrategy);
}

private String valueShouldBeNullToString() {
   if (valueShouldBeNull == ALWAYS) return "ALWAYS";
   if (valueShouldBeNull == NEVER) return "NEVER";
   if (valueShouldBeNull == SOMETIMES) return "SOMETIMES";
   throw new TestException("unknown valueShouldBeNull " + valueShouldBeNull);
}

public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("containsKeyCategory: " + containsKeyCategoryToString() + "\n");
   aStr.append("containsValueForKeyCategory: " + containsValueForKeyCategoryToString() + "\n");
   aStr.append("doGetStrategy: " + doGetStrategyToString() + "\n");
   aStr.append("valueShouldBeNull: " + valueShouldBeNullToString() + "\n");
   aStr.append("stableKeys: " + stableKeys + "\n");
   aStr.append("waitForContainsKey: " + waitForContainsKey + "\n");
   aStr.append("waitForContainsValueForKey: " + waitForContainsValueForKey + "\n");
   aStr.append("throwOnFirstError: " + throwOnFirstError + "\n");
   aStr.append("blackboard: " + blackboard + "\n");
   aStr.append("maxNumberToValidate: " + maxNumberToValidate + "\n");
   aStr.append("timeLimitMS: " + timeLimitMS + "\n");
   return aStr.toString();
}

// lynn no longer needed?
/** 
 *  Given a region and a key in the region, do a get on the key.
 *
 *  @param aRegion The region to use for the get.
 *  @param key The key to do the get on.
 *
 *  @return [0] true if the get operation succeeded, false otherwise
 *          [1] if [0] is true this is the object returned from the get
 */
/*
protected Object[] doGet(Region aRegion, Object key) {
   logMessage("Getting object for key " + key + "...");
   long waitLimit = 300 * TestHelper.SEC_NANO_FACTOR;
   long failedCount = 0;
   long loopStartTime = NanoTimer.getTime();
   while (true) {
      try {
         long startTime = NanoTimer.getTime();
         Object value = aRegion.get(key);
         long endTime = NanoTimer.getTime();
         long getDuration = endTime - startTime;
         long loopTime = endTime - loopStartTime;
         logMessage("Object for key " + key + " is " + TestHelper.toString(value) + 
            "\n   Successful get took " + TestHelper.nanosToString(getDuration) + 
            "\n   All gets (including failed gets) took " + TestHelper.nanosToString(loopTime) + 
            "\n   Total number of get failures: " + failedCount);
         sharedCounters.setIfLarger(CacheBB.MAX_SUCCESSFUL_GET_TIME_NANOS, getDuration);
         sharedCounters.setIfLarger(CacheBB.MAX_GET_TIME_NANOS, loopTime);
         sharedCounters.setIfLarger(CacheBB.MAX_FAILED_GET_ATTEMPTS_FOR_ONE_OBJECT, failedCount);
         return new Object[] {new Boolean(true), value};
      } catch (CacheException e) {
         sharedCounters.setIfLarger(CacheBB.MAX_FAILED_GET_ATTEMPTS_FOR_ONE_OBJECT, failedCount);
         logError(TestHelper.getStackTrace(e) + "\n");
         return new Object[] {new Boolean(false), null};
      }      
      long loopTime = NanoTimer.getTime() - loopStartTime;
      if (loopTime >= waitLimit) {
         sharedCounters.setIfLarger(CacheBB.MAX_FAILED_GET_ATTEMPTS_FOR_ONE_OBJECT, failedCount);
         blackboard.printSharedCounters();
         throw new TestException("After waiting " + TestHelper.nanosToString(loopTime) + 
               ", unable to get " + key + " from region " + TestHelper.regionToString(aRegion, false) + "\n" +
               "Failed gets: " + failedCount);
      } 
   }
}
*/

private void logError(String aStr) {
   errStr.append(aStr + "\n");
   Log.getLogWriter().info(aStr);
   if (throwOnFirstError) {
      throw new TestException(errStr.toString());
   }
}

protected void logMessage(String aStr) {
      Log.getLogWriter().info(aStr);
}

private String getKeySetDiscrepancies(Region aRegion, Set keySet) {
   if (doGetStrategy != Validator.ALWAYS)
      return "";
   if (valueShouldBeNull != Validator.NEVER)
      return "";
   if (!stableKeys)
      return "";
   long posCounter = 0;
   if ((containsValueForKeyCategory == POSITIVE) || (containsValueForKeyCategory == BOTH))
      posCounter = NameFactory.getPositiveNameCounter(); 
   long negCounter = 0;
   if ((containsValueForKeyCategory == NEGATIVE) || (containsValueForKeyCategory == BOTH))
      negCounter = NameFactory.getNegativeNameCounter(); 
   return NameFactory.getKeySetDiscrepancies(aRegion, keySet, (int)posCounter, (int)negCounter);
}

}
