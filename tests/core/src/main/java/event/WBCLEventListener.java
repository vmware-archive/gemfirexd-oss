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
package event; 

import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

import java.util.*;

/** WBCLEventListener (GatewayEventListener)
 *
 * @author Lynn Hughes-Godfrey
 * @since 662
 */
public class WBCLEventListener implements GatewayEventListener, Declarable {

/** The process ID of the VM that created this listener */
public int whereIWasRegistered;

/** noArg constructor 
 */
public WBCLEventListener() {
   whereIWasRegistered = ProcessMgr.getProcessId();
}

//----------------------------------------------------------------------------
// GatewayEventListener API
//----------------------------------------------------------------------------

/**
 * Counts events based on operation type
 */
public boolean processEvents(List events) {
  boolean status = false;

  // Fail 10% of the time ... ensure that we replay these events
  if (TestConfig.tab().getRandGen().nextInt(1,100) < 90) {
    for (Iterator i = events.iterator(); i.hasNext();) {
      GatewayEvent event = (GatewayEvent)i.next();
      try {
        logCall("processEvents", event);
      } catch (Exception e) {
        throwException("Caught unexpected Exception " + e + ", " + TestHelper.getStackTrace(e));
      }

      // I think these events are relative to the GatewayQueue (not the region)
      // So, we want to count all events added to the queue
      // this would only work with conflation (because otherwise, we could update or destroy events)?
      //WBCLEventBB.getBB().getSharedCounters().incrementAndRead(WBCLEventBB.NUM_EVENTS);
      
      Operation op = event.getOperation();
      try {
      if (op.isCreate()) {
        WBCLEventBB.getBB().getSharedCounters().incrementAndRead(WBCLEventBB.NUM_CREATES);
        Operation o = op.getCorrespondingCreateOp();
      } else if (op.isUpdate()) {
        WBCLEventBB.getBB().getSharedCounters().incrementAndRead(WBCLEventBB.NUM_UPDATES);
        Operation o = op.getCorrespondingUpdateOp();
      } else if (op.isInvalidate()) {
        WBCLEventBB.getBB().getSharedCounters().incrementAndRead(WBCLEventBB.NUM_INVALIDATES);
        throwException("Unexpected INVALIDATE encounted in WBCLEventListener " + op.toString() + ", " + TestHelper.getStackTrace());
      } else if (op.isDestroy()) {
        WBCLEventBB.getBB().getSharedCounters().incrementAndRead(WBCLEventBB.NUM_DESTROYS);
      }
      } catch (Exception e) {
          throwException("Caught unexpected Exception " + e + ", " + TestHelper.getStackTrace(e));
      }
    }
    status = true;
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
   aStr.append("   event.getDeserializedValue(): " + event.getDeserializedValue() + "\n");
   aStr.append("   event.getSerializedValue(): " + event.getSerializedValue() + "\n");
   return aStr.toString();
}

}
