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
package tx; 

import util.*;
import hydra.*;
//import hydra.blackboard.*;
//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import java.lang.*;
//import java.util.*;

/**
 * A subclass of TxUtil that forces each VM to use a unique range of
 * keys for its random operations, thus no conflicts.
 */
public class TxUtilKeyRange extends TxUtil {

// Define the size of the key range, and its lower and upper limits
protected static final int KEY_RANGE = 50;
public HydraThreadLocal lowerKeyRange = new HydraThreadLocal();
public HydraThreadLocal upperKeyRange = new HydraThreadLocal();

/** Initialize the key ranges
 */
public void initialize() {
   long uniqueKeyCounter = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.UniqueKeyCounter);
   int lower = (int)(((uniqueKeyCounter - 1) * KEY_RANGE) + 1);
   int upper = (int)(uniqueKeyCounter * KEY_RANGE);
   lowerKeyRange.set(new Integer(lower));
   upperKeyRange.set(new Integer(upper));
   Log.getLogWriter().info("LowerKeyRange is " + lowerKeyRange.get() + 
                           ", upperKeyRange is " + upperKeyRange.get());
   super.initialize();
}

/** Get a random key from the given region that is within this instance's range.
 *  If no keys qualify in the region, return null.
 *
 *  @param aRegion - The region to get the key from.
 *  @param excludeKey - The region to get the key from.
 *
 *  @returns A key from aRegion, or null.
 */
public Object getRandomKey(Region aRegion, Object excludeKey) {
   long start = System.currentTimeMillis();
   int lower = ((Integer)(lowerKeyRange.get())).intValue();
   int upper = ((Integer)(upperKeyRange.get())).intValue();
   long randomKeyIndex = TestConfig.tab().getRandGen().nextLong(lower, upper);   
   long startKeyIndex = randomKeyIndex;
   Object key = NameFactory.getObjectNameForCounter(randomKeyIndex);
   do {
      boolean done = false;
      if ((!(key.equals(excludeKey))) && (aRegion.containsKey(key)))
         done = true;
      if (done)
         break;
      randomKeyIndex++; // go to the next key
      if (randomKeyIndex > upper)
         randomKeyIndex = lower; 
      if (randomKeyIndex == startKeyIndex) { // considered all keys
         key = null;
         break;
      }
      key = NameFactory.getObjectNameForCounter(randomKeyIndex);
   } while (true);
   long end = System.currentTimeMillis();
   Log.getLogWriter().info("Done in TxUtilKeyRange:getRandomKey, key is " + key + 
       " " + aRegion.getFullPath() + " getRandomKey took " + (end - start) + " millis");
   return key;
}

/** Creates a new key/value in the given region by creating a new
 *  key within the range and a random value.
 *  
 *  @param aRegion The region to create the new key in.
 *  @param exists Not used in this overridden method; this test wants to use
 *  unique keys even on creates, so we don't do anything different here based
 *  on the value of exists.
 *  
 *  @return An instance of Operation describing the create operation.
 */
@Override
public Operation createEntry(Region aRegion, boolean exists) {
   int lower = ((Integer)(lowerKeyRange.get())).intValue();
   int upper = ((Integer)(upperKeyRange.get())).intValue();
   long keyIndex = TestConfig.tab().getRandGen().nextInt(lower, upper);
   long startKeyIndex = keyIndex;
   Object key = NameFactory.getObjectNameForCounter(keyIndex);
   boolean containsKey = aRegion.containsKey(key);
   while (containsKey) { // looking for a key that does not exist 
      keyIndex++; // go to the next key
      if (keyIndex > upper)
         keyIndex = lower; 
      if (keyIndex == startKeyIndex) { // considered all keys
         return null;
      }
      key = NameFactory.getObjectNameForCounter(keyIndex);
      containsKey = aRegion.containsKey(key);
   }
   BaseValueHolder vh = new ValueHolder(key, randomValues, new Integer(modValInitializer++));
   try {
      Log.getLogWriter().info("createEntryKeyRange: putting key " + key + ", object " + 
        vh.toString() + " in region " + aRegion.getFullPath());
      aRegion.put(key, vh);
      Log.getLogWriter().info("createEntryKeyRange: done putting key " + key + ", object " + 
        vh.toString() + " in region " + aRegion.getFullPath());
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new Operation(aRegion.getFullPath(), key, Operation.ENTRY_CREATE, null, vh.modVal);
}

/** Called for debugging */
public void printKeyRange(Region aRegion) {
   int lower = ((Integer)(lowerKeyRange.get())).intValue();
   int upper = ((Integer)(upperKeyRange.get())).intValue();
   StringBuffer aStr = new StringBuffer();
   for (int i = lower; i <= upper; i++) {
       Object key = NameFactory.getObjectNameForCounter(i);
       aStr.append("key " + key + " containsKey: " + aRegion.containsKey(key)
         + ", containsValueForKey: " + aRegion.containsValueForKey(key) +
         ", getValueInVM: " + diskReg.DiskRegUtil.getValueInVM(aRegion, key) + "\n");
   }
   Log.getLogWriter().info(aStr.toString());
}

}
