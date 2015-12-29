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
package mirror; 

import hydra.blackboard.*;
//import hydra.Log;
import com.gemstone.gemfire.cache.*;

public class EventListener6 extends util.AbstractListener implements CacheListener {

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   incrementAfterCreateCounters(event);
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   incrementAfterDestroyCounters(event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   incrementAfterUpdateCounters(event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   incrementAfterRegionDestroyCounters(event);
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   incrementAfterRegionInvalidateCounters(event);
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
}

//==============================================================================
// private methods
private void incrementAfterCreateCounters(EntryEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterCreateEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterCreateEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterCreateEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterCreateEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterCreateEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterCreateEvents_isNotRemote);
}

private void incrementAfterDestroyCounters(EntryEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterDestroyEvents_isNotRemote);
}

private void incrementAfterInvalidateCounters(EntryEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterInvalidateEvents_isNotRemote);
}

private void incrementAfterUpdateCounters(EntryEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterUpdateEvents_isNotRemote);
}

private void incrementAfterRegionDestroyCounters(RegionEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterRegionDestroyEvents_isNotRemote);
}

private void incrementAfterRegionInvalidateCounters(RegionEvent event) {
   SharedCounters counters = EventCounters6BB.getBB().getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isDist);
   else
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isNotDist);
   if (event.isExpiration())
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isExp);
   else
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isNotExp);
   if (event.isOriginRemote())
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isRemote);
   else
      counters.increment(EventCounters6BB.numAfterRegionInvalidateEvents_isNotRemote);
}

}
