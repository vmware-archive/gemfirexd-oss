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

/** An instance of this class records an initial result set of a CQ along
 *  with its events.
 *
 */
public class CQHistory {

private String cqName = null;
private SelectResults sr = null;
private List eventList = new ArrayList();

public CQHistory(String cqNameArg) {
   cqName = cqNameArg;
}

public void addEvent(CqEvent anEvent) {
   eventList.add(anEvent);
}

public void setSelectResults(SelectResults srArg) {
   sr = srArg;
}

public List getEvents() {
   return eventList;
}

public String getCqName() {
   return cqName;
}

public SelectResults getSelectResults() {
   return sr;
}

public String toString() {
   StringBuffer aStr = new StringBuffer();
   List srList = sr.asList();
   aStr.append("CQHistory for " + cqName + ", SelectResults is size " + srList.size() + " as follows\n");
   for (int i = 0; i < srList.size(); i++) {
      aStr.append(srList.get(i)+ "\n");
   }
   aStr.append("Event history is size " + eventList.size() + " as follows\n");
   for (int i = 0; i < eventList.size(); i++) {
      CqEvent cqEvent = (CqEvent)(eventList.get(i));
      QueryObject qo = (QueryObject)(cqEvent.getNewValue());
      aStr.append(qo + ", qo.extra = " + ((qo == null) ? null : qo.extra) + "\n");
   }
   return aStr.toString();
}

/** Combine select results with the history of cq events to
 *  get the final picture of what the query results are.
 *  Note: this method only works for SelectResults that contain
 *  instances of QueryObject that contain the key as the QueryObject's
 *  extra field.
 */
public Map getCombinedResults() {
   Log.getLogWriter().info("Combining SelectResults and event history for " + this);
   // add all results from initial list to combined map with unknown keys
   Map combinedMap = new HashMap();
   List srList = sr.asList();
   Log.getLogWriter().info("Processing " + srList.size() + " elements of SelectResults:");
   for (int i = 0; i < srList.size(); i++) {
      Object value = srList.get(i);
      QueryObject qo = (QueryObject)(value);
      Object key = qo.extra;
      combinedMap.put(key, value);
      Log.getLogWriter().info("Adding to combined map from srList: " + key + " " + value);
   }

   // process the history of events
   Set keysOfCQEventsSet = new HashSet();
   for (int i = 0; i < eventList.size(); i++) {
      CqEvent event = (CqEvent)(eventList.get(i));
      Object key = event.getKey();
      Operation op = event.getQueryOperation();
      if (keysOfCQEventsSet.contains(key)) {
        // this is a knownKeys type test; we only operation on a key once
        throw new TestException("Received two cq events for " + key + ", but expected 1; second event is " + event);
      }
      keysOfCQEventsSet.add(key);
      if (op.isCreate()) {
         Log.getLogWriter().info("Adding to combined map (create event): " + event.getKey() + " " + 
             event.getNewValue() + " event is " + event);
         Object oldResult = combinedMap.put(event.getKey(), event.getNewValue());
         // bug 39636 is too hard to fix, so we now document that we allow duplicates (meaning we can
         // get a cq event for something that is already in the result set); commenting out the
         // following check for 39636 (September 15, 2010)
         //if (oldResult != null) {
         //   throw new TestException("Probably bug 39636: Processing event " + event + ", but combined map already contains key " + 
         //                           event.getKey());
         //}
      } else if (op.isDestroy() || op.isInvalidate()) {
         if (combinedMap.containsKey(event.getKey())) {
            combinedMap.remove(event.getKey());
            Log.getLogWriter().info("Removing from combined map (destroy or invalidate): " + 
                event.getKey() + " event is " + event);
         } else { // if we get here, then the key in the event does not exist in combinedMap
           // bug 39636 is too hard to fix, so we now document that we allow duplicates (meaning we can
           // get a cq event for something that is already in the result set); commenting out the
           // following check for 39636 (September 15, 2010)
           // throw new TestException("Probably bug 39636: Processing event " + event + " but did not find " + 
           //           event.getKey() + " encoded in values of combined map");
         }
      } else if (op.isUpdate()) {
        // bug 39636 is too hard to fix, so we now document that we allow duplicates (meaning we can
        // get a cq event for something that is already in the result set); commenting out the
        // following check for 39636 (September 15, 2010)
         //if (!combinedMap.containsKey(event.getKey())) {
         //   throw new TestException("Probably bug 39636: Processing event " + event + ", but key " + event.getKey() + " does not exist in combinedMap");
         //}
         combinedMap.put(event.getKey(), event.getNewValue());
         Log.getLogWriter().info("Updating combined map (update event): " + event.getKey() + " " + 
             event.getNewValue() + " event is " + event);
      } else {
         throw new TestException("Test problem: Unknown operation " + op);
      }
   }
   StringBuffer aStr = new StringBuffer();
   aStr.append("CombinedMap is size " + combinedMap.size() + "\n");
   Iterator it = combinedMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      aStr.append(key + " " + combinedMap.get(key) + "\n");
   }
   Log.getLogWriter().info(aStr.toString());
   return combinedMap;
}

}
