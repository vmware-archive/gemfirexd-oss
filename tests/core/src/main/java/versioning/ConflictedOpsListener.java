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
package versioning; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import java.util.*;
import java.util.concurrent.*;

/** ConflictedOpsListener
 *
 *  For each round of execution, maintains a list of CacheListener events 
 *  and the corresponding callbackArgs for this VM.  The test can obtain
 *  this map and uses it to verify that no events are fired for operations
 *  not applied due to conflict resolution.
 *
 * @author Lynn Hughes-Godfrey
 * @since 7.0
 */
public class ConflictedOpsListener extends util.SilenceListener implements CacheListener, Declarable {

 static Object lastNewValue = null;
 static int numEvents = 0;
 protected String myVmId = null;
 protected String myClientName = null;
 protected Executor serialExecutor;

/** noArg constructor
 */
public ConflictedOpsListener() {
   serialExecutor = Executors.newSingleThreadExecutor();
   myVmId = "vm_" +  RemoteTestModule.getMyVmid();
   myClientName = RemoteTestModule.getMyClientName();
}

 public Object getLastNewValue() {
  return this.lastNewValue;
 }

 public void setLastNewValue(Object v) {
   this.lastNewValue = v;
 }

public int getNumEvents() {
   return this.numEvents;
}

public void setNumEvents(int n) {
   this.numEvents = n; 
}

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
}

/** test is currently only doing updates and we want to ensure we process the 
 *  events in order.
 */
public void afterUpdate(final EntryEvent event) {

   final Object key = event.getKey();
   final Object value = event.getNewValue();
   final Region aRegion = event.getRegion();

   serialExecutor.execute(new Runnable() {
     public void run() {
       logCall("afterUpdate", event);
       if (ConflictedOpsBB.getInstance().getSharedCounters().read(ConflictedOpsBB.processEvents) > 0) {
          setLastNewValue(value);
          numEvents++;
       }
     }
   });

   if (((EntryEventImpl)event).isConcurrencyConflict()) {  // we should never fire events with this true
     throwException("ConflictedOpsListener processed CacheEvent with isConcurrencyConflict = true in " + myVmId + "_" + myClientName);
   }
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
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

/** Inner class for maintaining order of delivery of events
 */
class SerialExecutor implements Executor {
   final Queue<Runnable> tasks = new ArrayDeque<Runnable>();
   final Executor executor;
   Runnable active;

   SerialExecutor(Executor executor) {
       this.executor = executor;
   }

   public synchronized void execute(final Runnable r) {
       tasks.offer(new Runnable() {
           public void run() {
               try {
                   r.run();
               } finally {
                   scheduleNext();
               }
           }
       });
       if (active == null) {
           scheduleNext();
       }
   }
 
   protected synchronized void scheduleNext() {
       if ((active = tasks.poll()) != null) {
           executor.execute(active);
       }
   }
}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to ConflictedOpsBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = ConflictedOpsBB.getInstance().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
