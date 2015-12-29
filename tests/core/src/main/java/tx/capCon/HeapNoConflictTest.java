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
package tx.capCon; 

import hydra.*;
import tx.TxUtil;
import util.*;

/**
 * A class to test that capacity controllers do not cause conflicts.
 */
public class HeapNoConflictTest extends NoConflictTest {


public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      TxUtil.txUtilInstance = new TxUtil();
      TxUtil.txUtilInstance.initialize();
      testInstance = new HeapNoConflictTest();
      CacheDefinition cacheDef = CacheDefinition.createCacheDefinition(util.CacheDefPrms.cacheSpecs , "cache1");
      cacheDef.createCache();
      testInstance.initialize();
   }
}

/** Verify that the current capacity of the region does not exceed its
 *  allowable upper limit
 */
protected void verifyCapacity() {
   // capacity is hard to verify for heap controllers
}

/** Fill the region with some object to get started */
protected void fill() {
   int numToAdd = 100;
   Log.getLogWriter().info("Filling with " + numToAdd + " entries...");
   for (int i = 1; i <= numToAdd; i++) {
      TxUtil.txUtilInstance.createEntry(aRegion, false);
   }
}

}
