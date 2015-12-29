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
package cq.newWan.serial; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

import cq.CQUtilBB;

import java.util.HashMap;
import java.util.Map;

/** CQ SequentialValues Listener. 
 *  Logs events processed by the CQListener, increments counters
 *  for types of baseOperations received.  In addition, it verifies that
 *  the value increments by 1 (lastValueReceived = newValue - 1) for updates.
 *  This can only be used in tests with a single publisher (for each key)
 *  where the updates contain Integer values which increment sequentially.
 *
 *  If a non-sequential update is received, the Exception is written to the 
 *  CQ Blackboard.  (Only the first failure is written to the blackboard).
 *
 *  This Exception can be retrieved via TestHelper.checkForEventError().
 *     @see util.TestHelper.checkForEventError
 *     @see util.TestHelper.EVENT_ERROR_KEY
 *
 * @author rdiyewar
 * @since 7.0
 */
public class WANCQListener extends util.AbstractListener implements CqListener {

// Maintain the latestValue on a per key basis (for this VM)
private final Map latestValues = new HashMap();

// Implement the CqListener interface
public void onEvent(CqEvent event) {

   logCQEvent("onEvent", event);
   Object key = event.getKey();
   QueryObject newValue = (QueryObject)event.getNewValue();   
   String cq = (String)event.getCq().getName();

   Operation op = event.getBaseOperation();
  
   // increment baseOperation counters
   if (op.equals(Operation.CREATE)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_CREATE", CQUtilBB.NUM_CREATE);
      latestValues.put(key, newValue);
   } else if (op.equals(Operation.DESTROY)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_DESTROY", CQUtilBB.NUM_DESTROY);
   } else if (op.equals(Operation.INVALIDATE)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_INVALIDATE", CQUtilBB.NUM_INVALIDATE);
   } else if (op.equals(Operation.UPDATE)) {
     QueryObject oldValue = (QueryObject)latestValues.get(key);
      CQUtilBB.incrementCounter("CQUtilBB.NUM_UPDATE", CQUtilBB.NUM_UPDATE);

      // update latestValues map and counter or throwing the Exception 
      // will cause additional missing updates to be reported by mistake
      latestValues.put(key, newValue);

      // report missing or late event arrival
      StringBuffer errMsg = new StringBuffer();
      long diff = newValue.aPrimitiveLong - oldValue.aPrimitiveLong;
      if (diff != 1) {
         errMsg.append("Cq Update Event did not incrementally increase for " + cq + ": last reported value for Key(" + key + ") = " + oldValue + ", newValue from CqEvent = " + newValue);
         // keep track of the number of dropped events
         if (diff > 0) {
            CQUtilBB.getBB().getSharedCounters().add(CQUtilBB.MISSING_UPDATES, diff - 1);
         } else {
            errMsg.append("Late event arrival for key (" + key + "), value = " + newValue);
            CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.LATE_UPDATES);
         }
         throwException(errMsg.toString());
      }
   } 
}

public void onError(CqEvent event) {
   logCQEvent("onError", event);
   CQUtilBB.incrementCounter("CQUtilBB.NUM_ERRORS", CQUtilBB.NUM_ERRORS);
}

public void close() {
}

/**
 * Utility method to write an Exception string to the CQ Blackboard and
 * to also Log an exception containing the same string.  Only the first
 * Exception encountered is written to the Blackboard.  All Exceptions
 * are logged (but not thrown).
 *
 * @param errStr String to log and post to CQUtilBB 
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = CQUtilBB.getBB().getSharedMap();
      // we only store the very first problem encountered (all are logged)
      if (aMap.get(TestHelper.EVENT_ERROR_KEY) == null) {
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " in " + getMyUniqueName() + " " + TestHelper.getStackTrace());
      }
      Log.getLogWriter().info("Listener encountered Exception: " + errStr + ", written to CQUtilBB");
      throw new TestException(errStr);
}

/**
 *  Uses RemoteTestModule information to produce a name to uniquely identify
 *  a client vm (vmid, clientName, host, pid) for the calling thread
 */
public static String getMyUniqueName() {
  StringBuffer buf = new StringBuffer( 50 );
  buf.append("vm_" ).append(RemoteTestModule.getMyVmid());
  buf.append( "_" ).append(RemoteTestModule.getMyClientName());
  buf.append( "_" ).append(RemoteTestModule.getMyHost());
  buf.append( "_" ).append(RemoteTestModule.getMyPid());
  return buf.toString();
}

}
