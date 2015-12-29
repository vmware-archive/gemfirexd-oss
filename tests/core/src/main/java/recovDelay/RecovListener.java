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
package recovDelay; 

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;

/** Mark in the bb the earliest time when a region is created, 
 *  or when membership loss is noticed to determine an approximate begin 
 *  time for recovery delay timers. 
 *
 *  Recovery delay timers in the product are started either when
 *  a PR is created, or when a member leaves. 
 */
public class RecovListener extends util.AbstractListener implements CacheListener, Declarable, RegionMembershipListener {

public void afterRegionCreate(RegionEvent event) {
   PRObserverBB.getBB().getSharedCounters().setIfSmaller(PRObserverBB.approxTimerStartTime, 
                new Long(System.currentTimeMillis()));
   logCall("afterRegionCreate", event);
}

public synchronized void afterRemoteRegionCrash(RegionEvent event) {
   PRObserverBB.getBB().getSharedCounters().setIfSmaller(PRObserverBB.approxTimerStartTime, 
                new Long(System.currentTimeMillis()));
   logCall("afterRemoteRegionCrash", event);
}

public void afterRemoteRegionDeparture(RegionEvent event) {
   PRObserverBB.getBB().getSharedCounters().setIfSmaller(PRObserverBB.approxTimerStartTime, 
                new Long(System.currentTimeMillis()));
   logCall("afterRemoteRegionDeparture", event);
}

public void initialMembers(Region aRegion, DistributedMember[] initialMembers) {
    StringBuffer aStr = new StringBuffer();
    aStr.append("Invoked " + this.getClass().getName() + ": initialMembers with: ");
    for (int i = 0; i < initialMembers.length; i++) {
       aStr.append(initialMembers[i] + " ");
    }
    Log.getLogWriter().info(aStr.toString());
}

// silent listener methods
public void afterRemoteRegionCreate(RegionEvent event) { }
public void afterCreate(EntryEvent event) { }
public void afterUpdate(EntryEvent event) { }
public void afterInvalidate(EntryEvent event) { }
public void afterDestroy(EntryEvent event) { }
public void afterRegionDestroy(RegionEvent event) { }
public void afterRegionClear(RegionEvent event) { }
public void afterRegionInvalidate(RegionEvent event) { }
public void afterRegionLive(RegionEvent event) { }
public void init(java.util.Properties prop) { }
public void close() { }

}
