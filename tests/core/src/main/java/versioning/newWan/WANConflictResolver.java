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
package versioning.newWan;

import versioning.VersionBB;
import newWan.WANOperationsClientBB;
import hydra.Log;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.util.GatewayConflictHelper;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * Custom wan conflict resolver
 * @author rdiyewar
 *
 */
public class WANConflictResolver implements GatewayConflictResolver {

  LogWriter log = Log.getLogWriter();
  WANOperationsClientBB bb = WANOperationsClientBB.getBB();
  
  public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper) {
    bb.getSharedCounters().increment(WANOperationsClientBB.WanEventResolved);
    
    log.info("WANConflictResolver: existing timestamp=" + event.getOldTimestamp() + " existing value=" + event.getOldValue()
        + "\n proposed timestamp=" + event.getNewTimestamp() + " proposed value=" + event.getNewValue());
    
    // use the default timestamp and ID based resolution
    if (event.getOldTimestamp() > event.getNewTimestamp()) {
      log.info("New event is older, disallow the event " + event);
      helper.disallowEvent();
    }
    if (event.getOldTimestamp() == event.getNewTimestamp()
        && event.getOldDistributedSystemID() > event.getNewDistributedSystemID()) {
      log.info("Both event has same timestamp, but new event's ds id small. Thus dissallow the event " + event);
      helper.disallowEvent();
    }    
  }
}
