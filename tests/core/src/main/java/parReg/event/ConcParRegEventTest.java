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
package parReg.event; 

import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ConcParRegEventTest extends event.ConcRegionEventTest {

/* task methods */
/* ======================================================================== */
public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ConcParRegEventTest();
      ((ConcParRegEventTest)eventTest).initialize();
      eventTest.isCarefulValidation = false;
      System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "60000");
   }
}

protected void initialize() {
   super.initialize();
   useCounters = false;
}

protected CacheListener getCacheListener() {
   return null; // no cache listeners for PR for Congo
}

protected Object[] getPartitionAttributes() {
   RegionDefinition regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, "partitionedRegionSpec");
   RegionAttributes attr = regDef.getRegionAttributes();
   return new Object[] {attr.getDataPolicy(), attr.getPartitionAttributes()};
}

}
