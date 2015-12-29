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

public class ConcRegionEventTest extends EventTest {

/* task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ConcRegionEventTest();
      eventTest.initialize();
      eventTest.isCarefulValidation = false;
   }
}

public synchronized static void HydraTask_reinitialize() {
   if (eventTest == null) {
      eventTest = new ConcRegionEventTest();
      eventTest.initialize();
      eventTest.isCarefulValidation = false;
      eventTest.faultInNewRegions();
   }
}

public static void HydraTask_endTask() throws Throwable {
   CacheBB.getBB().print();
   EventBB.getBB().print();
   EventCountersBB.getBB().print();
   TestHelper.checkForEventError(EventBB.getBB());
   eventTest = new ConcRegionEventTest();
   eventTest.initialize();
}

//private void iterateAllRegions() {
//   Region rootRegion = CacheUtil.getCache().getRegion(REGION_NAME);
//   Log.getLogWriter().info("List of all regions: " + TestHelper.regionsToString(rootRegion, false));
//   Object[] tmp = iterateRegion(rootRegion, true, true);
//   int totalKeys = ((Integer)tmp[0]).intValue();
//   int totalNonNullValues = ((Integer)tmp[1]).intValue();
//   StringBuffer errStr = new StringBuffer();
//   errStr.append(tmp[2]);
//   Set regions = rootRegion.subregions(true);
//   Iterator it = regions.iterator();
//   while (it.hasNext()) {
//      Region aRegion = (Region)it.next();
//      tmp = iterateRegion(aRegion, true, true);
//      totalKeys += ((Integer)tmp[0]).intValue();
//      totalNonNullValues += ((Integer)tmp[1]).intValue();
//      errStr.append(tmp[2]);
//   }
//   if (totalKeys == 0)
//      errStr.append("Total number of keys in all regions is 0\n");
//   if (totalNonNullValues == 0)
//      errStr.append("Total number of non-null values in all regions is 0\n");
//   if (errStr.length() > 0)
//      throw new TestException(errStr.toString());
//}

/* override methods */
/* ======================================================================== */
protected void doRegionOperations() {
   Log.getLogWriter().info("Faulting in available regions...");
   faultInNewRegions();
   Log.getLogWriter().info("Done faulting in available regions.");
   super.doRegionOperations();
   writeMyRegionNames();
}

}
