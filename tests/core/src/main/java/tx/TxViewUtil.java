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
import hydra.blackboard.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import java.util.*;

/**
 * A class to contain methods useful for view transactions tests.
 * In this class TxUtil.suspendResume only affects getRandomKey
 * not getValueInVM or containsKey, as these methods are overridden.
 * This is required when verifying the tx state (prior to commit).
 */
public class TxViewUtil extends TxUtil {

// overridden methods
/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_createRegionForest() {
   Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());
   CacheUtil.setCache(c);  // required by TxUtil and splitBrain/distIntegrityFD
   if (TxBB.getUpdateStrategy().equalsIgnoreCase(TxPrms.USE_COPY_ON_READ))
      c.setCopyOnRead(true);
   if (txUtilInstance == null) {
      txUtilInstance = new TxViewUtil();
      txUtilInstance.initialize();
      txUtilInstance.createRegionHierarchy(); 
   }
   txUtilInstance.summarizeRegionHier();
}  

/** Call containsKey on the given region and key 
 *
 *  @param aRegion - The region to test for the key.
 *  @param key - The key to use for containsKey.
 *
 *  @returns true if the region contains an entry with key, false otherwise.
 */
public boolean containsKey(Region aRegion, Object key) {
   boolean result = aRegion.containsKey(key);
   return result;
}

/** Call getValueInVM on the given region and key.  Override TxUtil version
 *  which suspends the transaction (View tests only suspend for getRandomKey
 *  not for getValueInVM).
 *
 *  @param aRegion - The region to use for the call.
 *  @param key - The key to use for the call.
 *
 *  @returns The value in the VM of key.
 */
public Object getValueInVM(Region aRegion, Object key) {
   return getValueInVM(aRegion, key, true);
}

/* getValueInVM with option to NOT translate null -> Token.INVALID
 * We need to be able to avoid this translation when using concurrentMap ops (like remove)
 * where we pass in the existing value.  remove(key, Token.INVALID) is not correct and the 
 * remove will fail (see BUG 42737), so don't do the translation for those calls.
 *
 *  @param aRegion - The region to use for the call.
 *  @param key - The key to use for the call.
 *  @param translateNullToInvalid - if true and entry exists, but value is null return Token.INVALID.
 *               Otherwise, return the actual value of null
 *
 *  @returns The value in the VM of key.
 */
public Object getValueInVM(Region aRegion, Object key, boolean translateNullToInvalid) {
   Object value = null;
   // getEntry will not invoke the loader
   Region.Entry entry = aRegion.getEntry(key);
   if (entry != null) {
     value = entry.getValue();
     if (value == null && translateNullToInvalid) {   // contains key with null value => Token.INVALID
       value = Token.INVALID;
     }
   } 
   return value;
}

/** Get a random key from the given region, excluding the key specified
 *  by excludeKey. If no keys qualify in the region, return null.
 *
 *  @param aRegion - The region to get the key from.
 *  @param excludeKey - key to exclude 
 *
 *  @returns A key from aRegion, or null.
 */
public Object getRandomKey(Region aRegion, Object excludeKey) {
   if (aRegion == null) {
      return null;
   }
   Set aSet = null;
   try {
      aSet = new HashSet(((LocalRegion)aRegion).testHookKeys());
   } catch (RegionDestroyedException e) {
      if (isSerialExecution)
         throw e;
      return null;
   }
   Object[] keyArr = aSet.toArray();
   if (keyArr.length == 0) {
      Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + " because the region has no keys");
      return null;
   }
   int randInt = TestConfig.tab().getRandGen().nextInt(0, keyArr.length-1);
   Object key = keyArr[randInt];
   if (key.equals(excludeKey)) { // get another key
      if (keyArr.length == 1) { // there are no other keys
         return null;
      }
      randInt++; // go to the next key
      if (randInt == keyArr.length)
         randInt = 0;
      key = keyArr[randInt];
   }
   return key;
}

}
