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
package parReg.wbcl; 

import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

import java.util.*;
import java.util.concurrent.*;

/** WBCLEventListener (GatewayEventListener)
 *
 * @author Lynn Hughes-Godfrey
 * @since 662
 */
public class WBCLEventListener implements GatewayEventListener, Declarable {

// updated with current time as each event processed by the WBCLEventListener
public static int lastEventTime;    

/** The process ID of the VM that created this listener */
public int whereIWasRegistered;
protected Executor serialExecutor;


/** noArg constructor 
 */
public WBCLEventListener() {
   whereIWasRegistered = ProcessMgr.getProcessId();
   serialExecutor = Executors.newSingleThreadExecutor();
}

//----------------------------------------------------------------------------
// GatewayEventListener API
//----------------------------------------------------------------------------

/**
 * Counts events based on operation type
 */
public boolean processEvents(List events) {
  boolean status = false;

  Log.getLogWriter().info("processEvents received List with " + events.size() + " GatewayEvents");
  // Fail 10% of the time ... ensure that we replay these events
  if (TestConfig.tab().getRandGen().nextInt(1,100) < 90) {
    status = true;
    for (Iterator i = events.iterator(); i.hasNext();) {
      GatewayEvent event = (GatewayEvent)i.next();
      try {
        logCall("processEvents", event);
        WBCLTestBB.getBB().getSharedCounters().setIfLarger(WBCLTestBB.lastEventTime, System.currentTimeMillis());

        // use the event to update the local wbcl region
        final Region wbclRegion = CacheHelper.getCache().getRegion("wbclRegion");
        final Object key = event.getKey();
        final Object value = event.getDeserializedValue();
        final Operation op = event.getOperation();

        serialExecutor.execute(new Runnable() {
          public void run() {
            if (op.isCreate()) {
              try {
                Log.getLogWriter().info("Creating key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
                wbclRegion.create(key, value);
                Log.getLogWriter().info("Done creating key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
              } catch (EntryExistsException e) {
                 Log.getLogWriter().info("Caught " + e + ", expected with concurrent operations; continuing with test");
              }
            } else if (op.isUpdate()) {
              Log.getLogWriter().info("Putting key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
              wbclRegion.put(key, value);
              Log.getLogWriter().info("Done Putting key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
            } else if (op.isInvalidate()) {
              throwException("Unexpected INVALIDATE encounted in WBCLEventListener " + op.toString() + ", " + TestHelper.getStackTrace());
            } else if (op.isDestroy()) {
              Log.getLogWriter().info("Destroying key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
              try {
                 wbclRegion.destroy(key);
              } catch (EntryNotFoundException e) {
                 Log.getLogWriter().info("Caught " + e + ", expected with concurrent operations; continuing with test");
              }
              Log.getLogWriter().info("Done destroying key/value pair (" + key + ", " + value + ") in region named " + wbclRegion.getName());
            }
          }
        });
      } catch (Exception e) {
        status = false;
        throwException("WBCL Listener caught unexpected Exception " + e + ", " + TestHelper.getStackTrace(e));
      }
    }
  } 
  if (status) {
    Log.getLogWriter().info("WBCLEventListener processed batch of " + events.size() + " events, returning " + status);
  } else {
    Log.getLogWriter().info("WBCLEventListener DID NOT process batch of " + events.size() + " events, returning " + status);
  }
  return status;
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

public void close() {
   logCall("close", null);
}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to EventBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = event.EventBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

/** Log that a gateway event occurred.
 *
 *  @param event The event object that was passed to the event.
 */
public String logCall(String methodName, GatewayEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}


/** Return a string description of the GatewayEvent.
 *
 *  @param event The GatewayEvent object that was passed to the CqListener
 *
 *  @return A String description of the invoked GatewayEvent
 */
public String toString(String methodName, GatewayEvent event) {
   StringBuffer aStr = new StringBuffer();

   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName() + "\n");
   aStr.append("   whereIWasRegistered: " + whereIWasRegistered + "\n");

   if (event == null) {
     return aStr.toString();
   }

   Operation op = event.getOperation();
   aStr.append("   Operation: " + op.toString() + "\n");
   if (op.isCreate()) {
     aStr.append("      Operation.getCorrespondingCreateOp(): " + op.getCorrespondingCreateOp().toString() + "\n");
   } else if (op.isUpdate()) {
     aStr.append("      Operation.getCorrespondingUpdate(Op): " + op.getCorrespondingUpdateOp().toString() + "\n");
   }
   aStr.append("      Operation.guaranteesOldValue(): " + op.guaranteesOldValue() + "\n");
   aStr.append("      Operation.isEntry(): " + op.isEntry() + "\n");
   aStr.append("      Operation.isPutAll(): " + op.isPutAll() + "\n");
   aStr.append("      Operation.isDistributed(): " + op.isDistributed() + "\n");
   aStr.append("      Operation.isLoad(): " + op.isLoad() + "\n");
   aStr.append("      Operation.isLocal(): " + op.isLocal() + "\n");
   aStr.append("      Operation.isLocalLoad(): " + op.isLocalLoad() + "\n");
   aStr.append("      Operation.isNetLoad(): " + op.isNetLoad() + "\n");
   aStr.append("      Operation.isNetSearch(): " + op.isNetSearch() + "\n");
   aStr.append("      Operation.isSearchOrLoad(): " + op.isSearchOrLoad() + "\n");
   aStr.append("   event.getCallbackArgument(): " + TestHelper.toString(event.getCallbackArgument()) + "\n");
   aStr.append("   event.getRegion(): " + TestHelper.regionToString(event.getRegion(), false) + "\n");
   aStr.append("   event.getKey(): " + event.getKey() + "\n");
   aStr.append("   event.getPossibleDuplicate(): " + event.getPossibleDuplicate() + "\n");
   aStr.append("   event.getDeserializedValue(): " + event.getDeserializedValue() + "\n");
   aStr.append("   event.getSerializedValue(): " + event.getSerializedValue() + "\n");
   return aStr.toString();
}

  /** Inner class for serializing (ordering) application of updates 
   *  based on gateway events.
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

/** Return when no events have been invoked for the given number of seconds.
 *
 *  @param sleepMS The number of milliseonds to sleep between checks for
 *         silence.
 */
public static void waitForSilence(long desiredSilenceSec, long sleepMS) {
   Log.getLogWriter().info("Waiting for a period of silence for " + desiredSilenceSec + " seconds...");
   long desiredSilenceMS = desiredSilenceSec * 1000;

   long silenceStartTime = System.currentTimeMillis();
   long currentTime = System.currentTimeMillis();
   long lastEventTime = WBCLTestBB.getBB().getSharedCounters().read(WBCLTestBB.lastEventTime);

   while (currentTime - silenceStartTime < desiredSilenceMS) {
      try {
         Thread.sleep(sleepMS);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      lastEventTime = WBCLTestBB.getBB().getSharedCounters().read(WBCLTestBB.lastEventTime);
      if (lastEventTime > silenceStartTime) {
         // restart the wait
         silenceStartTime = lastEventTime;
      }
      currentTime = System.currentTimeMillis();
   }
   long duration = currentTime - silenceStartTime;
   Log.getLogWriter().info("Done waiting, clients have been silent for " + duration + " ms");
}

}
