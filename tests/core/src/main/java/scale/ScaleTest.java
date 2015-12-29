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
package scale; 

import util.*;
import com.gemstone.gemfire.cache.*;

public class ScaleTest extends capCon.EntryEventMemLRUTest {

public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ScaleTest();
      ((ScaleTest)eventTest).initialize();
   }
}

protected void initialize() {
   super.initialize();
}

public static void HydraTask_doEntryOperations() {
   long startTime = System.currentTimeMillis();
   long taskLengthMS = 180000; // 3 minutes
   Region rootRegion = CacheUtil.getCache().getRegion(eventTest.regionName);
   do {
      ((ScaleTest)eventTest).doEntryOperations(rootRegion);
   } while (System.currentTimeMillis() - startTime < taskLengthMS);
}

protected void doEntryOperations(Region aRegion) {
   super.doEntryOperations(aRegion);
}

}
