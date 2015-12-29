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
public class IdleTOLocalDestroyListener extends util.AbstractListener implements CacheListener {

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   incrementAfterCreateCounters(event, IdleTOLocalDestroyBB.getBB());
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   incrementAfterDestroyCounters(event, IdleTOLocalDestroyBB.getBB());
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event, IdleTOLocalDestroyBB.getBB());
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   incrementAfterUpdateCounters(event, IdleTOLocalDestroyBB.getBB());
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   incrementAfterRegionDestroyCounters(event, IdleTOLocalDestroyBB.getBB());
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   incrementAfterRegionInvalidateCounters(event, IdleTOLocalDestroyBB.getBB());
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

public void close() {
   logCall("close", null);
   IdleTOLocalDestroyBB.getBB().getSharedCounters().increment(IdleTOLocalDestroyBB.numClose);
}

}
