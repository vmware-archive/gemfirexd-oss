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
package cq; 

import java.util.*;

import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.pdx.PdxInstance;

/** CQ Listener to log events, increment counters, and gather all events in a Map. 
 *
 */
public class CQGatherListener extends util.AbstractListener implements CqListener {

// The globalGatherMap is a map where the keys are cq names, and the values are HashMaps
// containing the results of the cqEvents for that cq name.
private static Map globalGatherMap = Collections.synchronizedMap(new HashMap());
private static final String ErrorKey = "ErrorKey";

/** Return the map which has gathered all cq event operations since the
 *  last resetGatherMap call.
 */
public static Map getGatherMap(String cqName) {
   Map aMap = (Map)(globalGatherMap.get(cqName));
   if (aMap == null) {
      aMap = new HashMap();
      globalGatherMap.put(cqName, aMap);
   }
   return aMap;
}

/** Resets the gather map to a new and empty Map.
 */
public static void resetGatherMap(String cqName) {
   globalGatherMap.put(cqName, new HashMap());
}

/** Method to allow the test to poll for a listener error
 */
public static void checkForError() {
   String errStr = (String)(CQUtilBB.getBB().getSharedMap().get(ErrorKey));
   if (errStr != null) {
      throw new TestException("Error occurred in CQGatherListener: " + errStr);
   }
}

// implement methods from CqListener
public void onError(CqEvent aCqEvent) {
   String listenerStr = toString("onError", aCqEvent);
   Log.getLogWriter().info(listenerStr);
   CQUtilBB.getBB().getSharedMap().put(ErrorKey, listenerStr);
}

/** For a cqEvent, log it, increment counters, and reflect the operation
 *  in the gather map.
 */
public void onEvent(CqEvent event) {
   CQUtilBB.getBB().getSharedCounters().setIfLarger(CQUtilBB.lastEventTime, System.currentTimeMillis());
   String eventStr = logCQEventAsSummary("onEvent", event);
   String cqName = event.getCq().getName();
   String queryStr = event.getCq().getQueryString();
   Object newValue = event.getNewValue();
   newValue = PdxTestVersionHelper.toBaseObject(newValue);
   if ((newValue != null) &&  !(newValue instanceof QueryObject)) {
      String errStr = "Validation failure in pid " + ProcessMgr.getProcessId() +
             " CqEvent was invoked, but getNewValue() returned unexpected " + newValue + " " + 
             eventStr + ", query " + cqName + ", " + CQTest.getReadableQueryString(queryStr);
      Log.getLogWriter().info(errStr);
      CQUtilBB.getBB().getSharedMap().put(ErrorKey, errStr);
      return;
   }
   Map gatherMap = (Map)(globalGatherMap.get(cqName));
   if (gatherMap == null) {
      gatherMap = new HashMap();
   }
  
   // update the gather map based on the query operation
   boolean newValueShouldSatisfyQuery = true;
   Operation op = event.getQueryOperation();
   if (op.equals(Operation.CREATE)) {
      gatherMap.put(event.getKey(), newValue);
      newValueShouldSatisfyQuery = true;
   } else if (op.equals(Operation.DESTROY)) {
      gatherMap.remove(event.getKey());
      newValueShouldSatisfyQuery = false;
   } else if (op.equals(Operation.INVALIDATE)) {
      gatherMap.put(event.getKey(), null);
      newValueShouldSatisfyQuery = true;
   } else if (op.equals(Operation.UPDATE)) {
      gatherMap.put(event.getKey(), newValue);
      newValueShouldSatisfyQuery = true;
   } 
   globalGatherMap.put(cqName, gatherMap);

   // now verify this event
   QueryObject qo = (QueryObject)(newValue);
   boolean satisfied = CQTest.satisfiesQuery(queryStr, qo);
   if (newValueShouldSatisfyQuery) {
      if (!satisfied) {
         String errStr = "Validation failure in pid " + ProcessMgr.getProcessId() +
                " CQGatherListener, " + eventStr + ", query " + cqName + ", " +
                CQTest.getReadableQueryString(queryStr) + " is not satisfied by " + qo.toStringFull();
         Log.getLogWriter().info(errStr);
         CQUtilBB.getBB().getSharedMap().put(ErrorKey, errStr);
      }
   } else {
      if (satisfied) {
         String errStr = "Validation failure in pid " + ProcessMgr.getProcessId() +
                " CQGatherListener, " + eventStr + ", query " + cqName + ", " +
                CQTest.getReadableQueryString(queryStr) + " is satisfied by " + qo.toStringFull() +
                " but it should not be";
         Log.getLogWriter().info(errStr);
         CQUtilBB.getBB().getSharedMap().put(ErrorKey, errStr);
      }
   }
}

public void close() {
   Log.getLogWriter().info("Invoked CQGatherListener.close()");
}

/** Return when no CQ events have been invoked for the given number of seconds.
*
*  @param sleepMS The number of milliseonds to sleep between checks for
*         silence.
*/
public static void waitForSilence(long desiredSilenceSec, long sleepMS) {
  Log.getLogWriter().info("Waiting for a period of silence for " + desiredSilenceSec + " seconds...");
  long desiredSilenceMS = desiredSilenceSec * 1000;

  long silenceStartTime = System.currentTimeMillis();
  long currentTime = System.currentTimeMillis();
  long lastEventTime = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.lastEventTime);

  while (currentTime - silenceStartTime < desiredSilenceMS) {
     try {
        Thread.sleep(sleepMS); 
     } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
     lastEventTime = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.lastEventTime);
     if (lastEventTime > silenceStartTime) {
        // restart the wait
        silenceStartTime = lastEventTime;
     }
     currentTime = System.currentTimeMillis();
  } 
  long duration = currentTime - silenceStartTime;
  Log.getLogWriter().info("Done waiting, CQs have been silent for " + duration + " ms");
}

}
