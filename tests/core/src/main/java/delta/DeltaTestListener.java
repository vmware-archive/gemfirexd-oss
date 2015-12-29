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
package delta; 

import com.gemstone.gemfire.cache.*;
import hydra.Log;
import diskReg.DiskRegUtil;

public class DeltaTestListener extends util.AbstractListener implements CacheListener, Declarable {

public void afterCreate(EntryEvent event) {
   // the ordering of this code is critical to avoiding test bug 42098 and 40836
   // first log the call; this will deserialize the newValue; if the value lives
   // in this vm, then the value will be deserialized in the region, but if the
   // value does not live in this vm (or this is a secondary copy and the value
   // will live in this vm but doesn't yet), it will be deserialized but only
   // for logging purposes and the value in the region remains serialized
   boolean objLivesInThisVM = DiskRegUtil.getValueInVM(event.getRegion(), event.getKey()) != null;
   logCall("afterCreate", event);
   DeltaTest.logPRMembers(event.getRegion(), event.getKey());
   Object valueInVM = DiskRegUtil.getValueInVM(event.getRegion(), event.getKey());
   if ((valueInVM instanceof DeltaObject) && (objLivesInThisVM)) { // object was in this vms when we did logCall, so
     // we know that the newValue lives as deserialized in the region; we can
     // safely save the object reference for later cloning validation
     DeltaObserver.addReference((DeltaObject)valueInVM);
   } else {
     // in afterCreate, but value in VM is null or an object other than DeltaObject
     // this can happen because of the race described in bug 40836/42098
     // or because this is a dataStore that does not host the value; cannot
     // save the reference for later cloning validation
     Log.getLogWriter().info("Current valueInVM is null, so not saving reference");
   }
   
   Log.getLogWriter().info("NUM_CREATE is now " + DeltaPropagationBB.getBB().getSharedCounters().incrementAndRead(
       DeltaPropagationBB.NUM_CREATE));
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   DeltaObserver.nullReference(event.getKey());
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   DeltaObserver.nullReference(event.getKey());
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   Log.getLogWriter().info("NUM_UPDATE is now " + DeltaPropagationBB.getBB().getSharedCounters().incrementAndRead(
       DeltaPropagationBB.NUM_UPDATE));
   Object anObj = event.getNewValue();
   if (anObj instanceof DeltaObject) { 
      DeltaObserver.addReference((DeltaObject)(anObj));
   }
}

public void close() {
   logCall("close", null);
}

public void afterRegionClear(RegionEvent event) {
   logCall("afterRegionClear", event);
}

public void afterRegionCreate(RegionEvent event) {
   logCall("afterRegionCreate", event);
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
