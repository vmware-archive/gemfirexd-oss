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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;

public class RegMemLogListener extends AbstractListener implements RegionMembershipListener, Declarable {

public synchronized void afterRemoteRegionCrash(RegionEvent event) {
   logCall("afterRemoteRegionCrash", event);
}

public void afterRemoteRegionCreate(RegionEvent event) {
    logCall("afterRemoteRegionCreate", event);
}

public void afterRemoteRegionDeparture(RegionEvent event) {
    logCall("afterRemoteRegionDeparture", event);
}

public void close() {
   Log.getLogWriter().info("Invoked " + this.getClass().getName() + ": close");
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
public void afterCreate(EntryEvent event) { }
public void afterUpdate(EntryEvent event) { }
public void afterInvalidate(EntryEvent event) { }
public void afterDestroy(EntryEvent event) { }
public void afterRegionClear(RegionEvent event) { }
public void afterRegionCreate(RegionEvent event) { }
public void afterRegionDestroy(RegionEvent event) { }
public void afterRegionInvalidate(RegionEvent event) { }
public void afterRegionLive(RegionEvent event) { }

public void init(java.util.Properties props) { }
}
