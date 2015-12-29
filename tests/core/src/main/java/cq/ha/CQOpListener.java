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
package cq.ha; 

import util.*;
import hydra.*;

import cq.CQUtilBB;
import hct.ha.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/** CQ Op Listener (updates counters based on CQ operation vs. BaseRegion operation)
 *  Simply increments counters for CQEvents processed by the CQListener; increments counters
 *  based on event.getQueryOperation(): create, update, destroy.
 *
 * @author lhughes
 * @since 5.1
 */
public class CQOpListener extends util.AbstractListener implements CqListener {

// Implement the CqListener interface
public void onEvent(CqEvent event) {

   logCQEvent("onEvent", event);

   Operation op = event.getQueryOperation();
  
   // increment Query Operation counters
   if (op.equals(Operation.CREATE)) {
      // We don't count the last key
      if (!event.getKey().equals(Feeder.LAST_KEY)) {
         CQUtilBB.incrementCounter("CQUtilBB.NUM_CREATE", CQUtilBB.NUM_CREATE);
      }
   } else if (op.equals(Operation.DESTROY)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_DESTROY", CQUtilBB.NUM_DESTROY);
   } else if (op.equals(Operation.UPDATE)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_UPDATE", CQUtilBB.NUM_UPDATE);
   } 
}

public void onError(CqEvent event) {
   logCQEvent("onError", event);
   CQUtilBB.incrementCounter("CQUtilBB.NUM_ERRORS", CQUtilBB.NUM_ERRORS);
}

public void close() {
}

}
