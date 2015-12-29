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
package capCon; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

public abstract class RegEventCapCon extends event.EventTest {

public static void HydraTask_invalidateRegion() {
   ((RegEventCapCon)eventTest).doInvalidate();
   ((RegEventCapCon)eventTest).writeMyRegionNames();
}

private void doInvalidate() {
//   long startTime = System.currentTimeMillis();
   if (isSerialExecution)
      logExecutionNumber();
   TestHelper.checkForEventError(EventCountersBB.getBB());

   Log.getLogWriter().info("Faulting in available regions...");
   faultInNewRegions();
   Log.getLogWriter().info("Done faulting in available regions.");

   invalidateRegion(TestConfig.tab().getRandGen().nextBoolean());
}

public static void HydraTask_destroyRegions() {
   ((RegEventCapCon)eventTest).doDestroys();
   ((RegEventCapCon)eventTest).writeMyRegionNames();
}

private void doDestroys() {
//   long startTime = System.currentTimeMillis();
   if (isSerialExecution)
      logExecutionNumber();
   TestHelper.checkForEventError(EventCountersBB.getBB());

   Log.getLogWriter().info("Faulting in available regions...");
   faultInNewRegions();
   Log.getLogWriter().info("Done faulting in available regions.");

   while (getNumNonRootRegions() > maxRegions) {
      destroyRegion(TestConfig.tab().getRandGen().nextBoolean());
   }
}

public static void HydraTask_addRegionsAndObjects() {
   ((RegEventCapCon)eventTest).doAddRegionsAndObjects();
   ((RegEventCapCon)eventTest).writeMyRegionNames();
}

private void doAddRegionsAndObjects() {
   long startTime = System.currentTimeMillis();
   if (isSerialExecution)
      logExecutionNumber();
   TestHelper.checkForEventError(EventCountersBB.getBB());

   do {
      Log.getLogWriter().info("Faulting in available regions...");
      faultInNewRegions();
      Log.getLogWriter().info("Done faulting in available regions.");

      // add new regions if necessary
      long numRegions = getNumNonRootRegions();
      if (numRegions < maxRegions) {
         addRegion();
         ((RegEventCapCon)eventTest).writeMyRegionNames();
      }

      // add objects to region
      Log.getLogWriter().info("Adding objects to regions...");
      final long addTimeMS = 10000;
      long startAddTime = System.currentTimeMillis();
      do {
         Region aRegion = getRandomRegion(false);
         if (aRegion != null)
            addObject(aRegion, false);
      } while (System.currentTimeMillis() - startAddTime < addTimeMS);
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

protected Object getObjectToAdd(String name) {
   return randomValues.getRandomObjectGraph();
}

}
