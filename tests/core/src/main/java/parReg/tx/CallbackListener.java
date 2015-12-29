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
package parReg.tx; 

import util.*;
import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;

import java.util.*;

/** CallbackListener     
 *  Validates that all event callbacks  produce the same RoutingObject (to  
 *  verify customPartitioning by using a callback object that implements the 
 *  PartitionResolver interface).
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class CallbackListener extends util.AbstractListener implements CacheListener, Declarable {

// ArrayList of unique routing objects in this VM
private ArrayList roList = new ArrayList();

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   processCallback(event);
}
  

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   processCallback(event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   processCallback(event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   processCallback(event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
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

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}
 
private void processCallback(EntryEvent event) {
   Object callback = event.getCallbackArgument();
   if (callback instanceof PidRoutingObject) {
      PidRoutingObject ro = (PidRoutingObject)callback;
      if (!roList.contains(ro)) {
         roList.add(ro);
      } 
   } else {
      throw new TestException("CallbackListener expected callbackArgument of type PidRoutingObject, but found " + callback.getClass());
   }

   int numVms = TestConfig.getInstance().getTotalVMs()-1;
   Iterator it = roList.iterator();
   PidRoutingObject ro = (PidRoutingObject)it.next();
   int hashBucket = ro.hashCode() % numVms;
   while (it.hasNext()) {
      PidRoutingObject nextRo = (PidRoutingObject)it.next();
      int nextHashBucket = nextRo.hashCode() % numVms;
      if (hashBucket != nextHashBucket) {
        throw new TestException("VM " + ProcessMgr.getProcessId() + " is the dataStore for disparate routingObjects " + hashBucket + " and " + nextHashBucket);
      } else {
        Log.getLogWriter().info("Two pids resolved to same bucket: " + ro.getPid() + " and " + nextRo.getPid() + " both resolved to bucket " + hashBucket);
      }
   }
     
   if (roList.size() > 1) {
   }

}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to PrTxBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = PrTxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}


}
