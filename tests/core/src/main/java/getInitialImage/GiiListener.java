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
package getInitialImage; 

import util.*;
//import hydra.*;
//import hydra.blackboard.SharedCounters;
import com.gemstone.gemfire.cache.*;

public class GiiListener extends util.AbstractListener implements CacheListener {

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterCreate, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterCreateCounters(event, EventCountersBB.getBB());
   }
}

public void afterDestroy(EntryEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterDestroy, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterDestroyCounters(event, EventCountersBB.getBB());
   }
}

public void afterInvalidate(EntryEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterInvalidate, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterInvalidateCounters(event, EventCountersBB.getBB());
   }
}

public void afterUpdate(EntryEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterUpdate, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterUpdateCounters(event, EventCountersBB.getBB());
   }
}

public void afterRegionDestroy(RegionEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterRegionDestroy, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterRegionDestroyCounters(event, EventCountersBB.getBB());
   }
}

public void afterRegionInvalidate(RegionEvent event) {
   boolean inProgress = InitImageTest.isLocalGiiInProgress(event.getRegion());
   logCall("afterRegionInvalidate, giiInProgress: " + inProgress, event);
   if (inProgress) {
      incrementAfterRegionInvalidateCounters(event, EventCountersBB.getBB());
   }
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
}

}
