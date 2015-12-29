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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/** CQ Test Listener. 
 *  Simply logs events processed by the CQListener, increments counters
 *  for types of baseOperations recv'd
 *
 * @author lhughes
 * @since 5.1
 */
public class CQTestListener extends util.AbstractListener implements CqListener {

// Implement the CqListener interface
public void onEvent(CqEvent event) {

   logCQEvent("onEvent", event);

   Operation op = event.getBaseOperation();
  
   // increment baseOperation counters
   if (op.equals(Operation.CREATE)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_CREATE", CQUtilBB.NUM_CREATE);
   } else if (op.equals(Operation.DESTROY)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_DESTROY", CQUtilBB.NUM_DESTROY);
   } else if (op.equals(Operation.INVALIDATE)) {
      CQUtilBB.incrementCounter("CQUtilBB.NUM_INVALIDATE", CQUtilBB.NUM_INVALIDATE);
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
