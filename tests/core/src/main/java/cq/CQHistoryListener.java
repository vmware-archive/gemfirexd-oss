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
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/** CQ Listener to log events, and record the history of a query's
 *  initialResultSet plus all its events.
 *
 */
public class CQHistoryListener extends util.AbstractListener implements CqListener {

// The globalHistoryMap is a map where the keys are cq names, and the values are 
// instances of CQHistory.
private static Map globalHistoryMap = Collections.synchronizedMap(new HashMap());
private static final String ErrorKey = "ErrorKey";

/** Return the CQHistory for the given cqName.
 */
public static CQHistory getCQHistory(String cqName) {
   return (CQHistory)(globalHistoryMap.get(cqName));
}

/** Record the given CQHistory
 */
public static void recordHistory(CQHistory history) {
   Object anObj = globalHistoryMap.get(history.getCqName());
   if (anObj != null) {
      throw new TestException("Test problem; history already exists for " + history.getCqName());
   }
   globalHistoryMap.put(history.getCqName(), history);
}

/** Method to allow the test to poll for a listener error
 */
public static void checkForError() {
   String errStr = (String)(CQUtilBB.getBB().getSharedMap().get(ErrorKey));
   if (errStr != null) {
      throw new TestException("Error occurred in CQHistoryListener: " + errStr);
   }
}

// implement methods from CqListener
public void onError(CqEvent aCqEvent) {
   String listenerStr = toString("onError", aCqEvent);
   Log.getLogWriter().info(listenerStr);
   CQUtilBB.getBB().getSharedMap().put(ErrorKey, listenerStr);
}

/** For a cqEvent, log it, verify it and save the event in the history.
 */
public void onEvent(CqEvent event) {
   String eventStr = logCQEventAsSummary("onEvent", event);
   String cqName = event.getCq().getName();
   String queryStr = event.getCq().getQueryString();
   CQHistory history = (CQHistory)globalHistoryMap.get(cqName);
   history.addEvent(event);
  
   // determine if the event satisfies the query
   boolean newValueShouldSatisfyQuery = true;
   Operation op = event.getQueryOperation();
   if (op.equals(Operation.CREATE)) {
      newValueShouldSatisfyQuery = true;
   } else if (op.equals(Operation.DESTROY)) {
      newValueShouldSatisfyQuery = false;
   } else if (op.equals(Operation.INVALIDATE)) {
      newValueShouldSatisfyQuery = true;
   } else if (op.equals(Operation.UPDATE)) {
      newValueShouldSatisfyQuery = true;
   } 

   // now verify this event
   QueryObject qo = (QueryObject)(event.getNewValue());
   boolean satisfied = CQTest.satisfiesQuery(queryStr, qo);
   if (newValueShouldSatisfyQuery) {
      if (!satisfied) {
         String errStr = "Validation failure in pid " + ProcessMgr.getProcessId() +
                " CQHistoryListener, " + eventStr + ", query " + cqName + ", " +
                CQTest.getReadableQueryString(queryStr) + " is not satisfied by " + qo.toStringFull();
         Log.getLogWriter().info(errStr);
         CQUtilBB.getBB().getSharedMap().put(ErrorKey, errStr);
      }
   } else {
      if (satisfied) {
         String errStr = "Validation failure in pid " + ProcessMgr.getProcessId() +
                " CQHistoryListener, " + eventStr + ", query " + cqName + ", " +
                CQTest.getReadableQueryString(queryStr) + " is satisfied by " + qo.toStringFull() +
                " but it should not be";
         Log.getLogWriter().info(errStr);
         CQUtilBB.getBB().getSharedMap().put(ErrorKey, errStr);
      }
   }
}

public void close() {
   Log.getLogWriter().info("Invoked CQHistoryListener.close()");
}

}
