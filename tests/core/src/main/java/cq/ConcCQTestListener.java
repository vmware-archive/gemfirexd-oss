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
 * @author prafulla
 * @since 5.1
 */
public class ConcCQTestListener extends util.AbstractListener implements CqListener {
  
  private static CQUtilBB cqBB = CQUtilBB.getBB();

  // Implement the CQListener interface
  public void onEvent(CqEvent event) {
  
     logCQEvent("onEvent", event);
  
     Operation op = event.getBaseOperation();
    
     // increment baseOperation counters
     if (op.equals(Operation.CREATE)) {
       CQUtilBB.incrementCounter("CQUtilBB.NUM_CREATE", CQUtilBB.NUM_CREATE);
       Object key = event.getKey();
       CQUtilBB.getBB().getSharedMap().put(key+":" + RemoteTestModule.getMyVmid(), new Integer(1));
     } else if (op.equals(Operation.DESTROY)) {
       //Log.getLogWriter().info("new value is " + event.getNewValue().toString());
       CQUtilBB.incrementCounter("CQUtilBB.NUM_DESTROY", CQUtilBB.NUM_DESTROY);
     } else if (op.equals(Operation.INVALIDATE)) {
       CQUtilBB.incrementCounter("CQUtilBB.NUM_INVALIDATE", CQUtilBB.NUM_INVALIDATE);
     } else if (op.equals(Operation.UPDATE)) {
       Log.getLogWriter().info("new value is " + event.getNewValue().toString());
       CQUtilBB.incrementCounter("CQUtilBB.NUM_UPDATE", CQUtilBB.NUM_UPDATE);
     } else if (op.equals(Operation.REGION_CLEAR)) {
       CQUtilBB.incrementCounter("CQUtilBB.NUM_CLEARREGION", CQUtilBB.NUM_CLEARREGION);
       Log.getLogWriter().info("clearRegion Ops");
     } else if (op.equals(Operation.REGION_INVALIDATE)) {
       CQUtilBB.incrementCounter("CQUtilBB.NUM_INVALIDATEREGION", CQUtilBB.NUM_INVALIDATEREGION);
       Log.getLogWriter().info("invalidateRegion Ops");
     }
       
     cqBB.getSharedMap().put("lastEventReceived", new Long(System.currentTimeMillis()));
  }
      
  public void onError(CqEvent event) {
     cqBB.getSharedMap().put("lastEventReceived", new Long(System.currentTimeMillis()));
     logCQEvent("onError", event);
     CQUtilBB.incrementCounter("CQUtilBB.NUM_ERRORS", CQUtilBB.NUM_ERRORS);
  }
  
  public void close() {
    Log.getLogWriter().info("CQListener close() is invoked.");
    CQUtilBB.getBB().getSharedCounters().increment(CQUtilBB.NUM_CLOSE);
  }

}
