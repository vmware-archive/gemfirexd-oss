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

import java.util.*;

public class KeyIntervals implements java.io.Serializable {

// the possible intervals; use higher numbers to as to not confuse these
// constants with ones in the test code 
public static final int NONE                   = 1000;
public static final int INVALIDATE             = 1001;
public static final int LOCAL_INVALIDATE       = 1002;
public static final int DESTROY                = 1003;
public static final int LOCAL_DESTROY          = 1004;
public static final int UPDATE_EXISTING_KEY    = 1005;
public static final int GET                    = 1006;

// first and last intervals
private static final int FIRST_INTERVAL        = 1000;
private static final int LAST_INTERVAL         = 1006;

private static final int NO_INTERVAL = -1;

// fields to track the total number of keys and where each interval
// begins and ends; contains -1 for intervals that are not used
private int numKeys;
private int[] firstKey = new int[LAST_INTERVAL - FIRST_INTERVAL + 1];
private int[] lastKey  = new int[LAST_INTERVAL - FIRST_INTERVAL + 1];

/** Constructor to create an instance of KeyInterval using the given number
 *  of keys and the intervals in includeIntervals.
 *
 *  @param includeIntervals - the intervals to use when dividing numKeys into intervals
 *  @param numKeys - the total number of keys to use to divide among the intervals.
 */
public KeyIntervals(int[] includeIntervals, int numKeys) {
   initKeyIntervals(includeIntervals, numKeys);
}

/** Constructor to create an instance of KeyInterval using the given number 
 *  of keys and all intervals defined in this class.
 *
 *  @param numKeys - the total number of keys to use to divide among the intervals.
 */
public KeyIntervals(int numKeys) {
   int[] ops = new int[LAST_INTERVAL - FIRST_INTERVAL + 1];
   for (int i = FIRST_INTERVAL; i <= LAST_INTERVAL; i++) {
      ops[i - FIRST_INTERVAL] = i;
   }
   initKeyIntervals(ops, numKeys);
}

/** Initialize private fields do designate first and last keys for
 *  the given intervals and number of keys.
 *
 *  @param includeIntervals - the intervals to use when dividing numKeys into intervals
 *  @param numKeys - the total number of keys to use to divide among the intervals.
 */
private void initKeyIntervals(int[] includeIntervals, int numKeys) {
   Arrays.sort(includeIntervals);
   this.numKeys = numKeys;
   int numOps = includeIntervals.length;
   int numPerInterval = numKeys / numOps;
   int numLeftOver = numKeys % numOps;
   int startKey = 1;
   for (int i = 0; i < firstKey.length; i++) {
      int whichInterval = i + FIRST_INTERVAL;
      int index = Arrays.binarySearch(includeIntervals, whichInterval); 
      if (index >= 0) {
         int numInInterval = numPerInterval;
         if (numLeftOver > 0) {
            numInInterval++;
            numLeftOver--;
         }
         firstKey[i] = startKey;
         lastKey[i] = startKey + numInInterval - 1;
         startKey = lastKey[i] + 1;
      } else {
         firstKey[i] = NO_INTERVAL;
         lastKey[i] = NO_INTERVAL;
      }
   }
}

/** Given an interval, return its first key index.
 *
 *  @param whichInterval - the interval to get the first key for.
 *
 *  @return The first key index for this interval.
 */
public int getFirstKey(int whichInterval) {
   if ((whichInterval >= FIRST_INTERVAL) && (whichInterval <= LAST_INTERVAL)) {
      int key = firstKey[whichInterval - FIRST_INTERVAL];
      return key;
   } else {
      throw new TestException("Unknown interval " + whichInterval);
   }
}

/** Given an interval, return its last key index.
 *
 *  @param whichInterval - the interval to get the last key for.
 *
 *  @return The last key index for this interval.
 */
public int getLastKey(int whichInterval) {
   if ((whichInterval >= FIRST_INTERVAL) && (whichInterval <= LAST_INTERVAL)) {
      int key = lastKey[whichInterval - FIRST_INTERVAL];
      return key;
   } else {
      throw new TestException("Unknown interval " + whichInterval);
   }
}

/** Return the total number of keys for all intervals.
 */
public int getNumKeys() {
   return numKeys;
}

/** Return the total number of keys for the given interval.
 */
public int getNumKeys(int whichInterval) {
   if ((whichInterval >= FIRST_INTERVAL) && (whichInterval <= LAST_INTERVAL)) {
      int firstKey = getFirstKey(whichInterval);
      int lastKey = getLastKey(whichInterval);
      if (firstKey == NO_INTERVAL)
         return 0;
      else
         return lastKey - firstKey + 1;
   } else 
      throw new TestException("Unknown interval " + whichInterval);
}

/** Given an interval, return a String describing it
 */
public String intervalToString(int whichInterval) {
   switch (whichInterval) {
      case NONE:                return "none";
      case INVALIDATE:          return "invalidate";
      case LOCAL_INVALIDATE:    return "localInvalidate";
      case DESTROY:             return "destroy";
      case LOCAL_DESTROY:       return "localDestroy";
      case UPDATE_EXISTING_KEY: return "updateExistingKey";
      case GET:                 return "get";
      default: {
         throw new TestException("Unknown interval " + whichInterval);
      }
   }
}

/** Return a string describing the intervals in this instance
 */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   for (int i = 0; i < firstKey.length; i++) {
      aStr.append(intervalToString(FIRST_INTERVAL + i) + 
                  ", firstKey: " + firstKey[i] + ", lastKey: " + lastKey[i] + "\n");
   }
   return aStr.toString();
}

/** Determine if a key index is in range of an interval.
 *
 *  @param whichInterval The interval to consider for the range.
 *  @param keyIndex The specific key index.
 *
 *  @return True if the keyIndex is in range for the given interval,
 *           false otherwise.
 */
public boolean keyInRange(int whichInterval, long keyIndex) {
   int firstKey = getFirstKey(whichInterval);
   int lastKey = getLastKey(whichInterval);
   return (keyIndex >= firstKey) && (keyIndex <= lastKey);
}

/** Return the op for the given keyIndex, or -1 if none
 * 
 * @param keyIndex Find the op for this keyIndex
 * @return The op for keyIndex or -1 if none. 
 */
public int opForKeyIndex(long keyIndex) {
  if (keyInRange(NONE, keyIndex)) {
    return NONE;
  } else if (keyInRange(INVALIDATE, keyIndex)) {
    return INVALIDATE;
  } else if (keyInRange(LOCAL_INVALIDATE, keyIndex)) {
    return LOCAL_INVALIDATE;
  } else if (keyInRange(DESTROY, keyIndex)) {
    return DESTROY;
  } else if (keyInRange(LOCAL_DESTROY, keyIndex)) {
    return LOCAL_DESTROY;
  } else if (keyInRange(UPDATE_EXISTING_KEY, keyIndex)) {
    return UPDATE_EXISTING_KEY;
  } else if (keyInRange(GET, keyIndex)) {
    return GET;
  } else {
    return -1;
  }
}

}
