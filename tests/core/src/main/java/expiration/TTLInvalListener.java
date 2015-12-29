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
package expiration; 

//import util.*;
//import hydra.*;
//import hydra.blackboard.SharedCounters;
import com.gemstone.gemfire.cache.*;

/** Listener to bump blackboard counters.
 */
public class TTLInvalListener extends util.AbstractListener implements CacheListener {

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   incrementAfterCreateCounters(event, TTLInvalBB.getBB());
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   incrementAfterDestroyCounters(event, TTLInvalBB.getBB());
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event, TTLInvalBB.getBB());
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   incrementAfterUpdateCounters(event, TTLInvalBB.getBB());
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   incrementAfterRegionDestroyCounters(event, TTLInvalBB.getBB());
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   incrementAfterRegionInvalidateCounters(event, TTLInvalBB.getBB());
}

public void close() {
   logCall("close", null);
   TTLInvalBB.getBB().getSharedCounters().increment(TTLInvalBB.numClose);
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

}
