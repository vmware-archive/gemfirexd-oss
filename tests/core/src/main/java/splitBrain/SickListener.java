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
package splitBrain; 

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import hydra.blackboard.*;
import util.*;

import java.util.*;

/** SickListener, increments counters to determine that a VM received a
 *  forced disconnect.
 */
public class SickListener extends util.AbstractListener 
                                  implements CacheListener, Declarable {

//================================================================================ 
// event methods

public void afterCreate(EntryEvent event) { 
   String eventStr = logCall("afterCreate", event);
}  
public void afterUpdate(EntryEvent event) {
   String eventStr = logCall("afterUpdate", event);
}  
public void afterDestroy(EntryEvent event) {
   String eventStr = logCall("afterDestroy", event);
}  
public void afterInvalidate(EntryEvent event) { 
   String eventStr = logCall("afterInvalidate", event);
}  
public void afterRegionInvalidate(RegionEvent event) {
   String eventStr = logCall("afterRegionInvalidate", event);
}  
public void afterRegionClear(RegionEvent event) {
   String eventStr = logCall("afterRegionClear", event);
}  
public void afterRegionCreate(RegionEvent event) {
   String eventStr = logCall("afterRegionCreate", event);
}  
public void afterRegionLive(RegionEvent event) {
   String eventStr = logCall("afterRegionLive", event);
}  
public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}  
public void close() {
   logCall("close", null);
}  


public void afterRegionDestroy(RegionEvent event) { 
   String eventStr = logCall("afterRegionDestroy", event);
   Operation op = event.getOperation();
   if (op.equals(Operation.FORCED_DISCONNECT)) {
      Log.getLogWriter().info("Received an afterRegionDestroy event with Operation.FORCED_DISCONNECT");
      ControllerBB.incMapCounter(ControllerBB.NumForcedDiscEventsKey); // quit waiting for a forced disconnect in afterCreate
      if (!ControllerBB.isPlayDeadEnabled()) { // we were not expecting this
         String errStr = "Got an unexpected forced disconnect event invoked in vmID " + 
                          RemoteTestModule.getMyVmid() + ": " + eventStr;
         Log.getLogWriter().info(errStr);
         ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
      }
   }   
}

}
