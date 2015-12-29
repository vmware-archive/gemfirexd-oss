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

/** ObjectNameListener     
 *  Records the key's hashCode being received by this VM.
 *  With keys from NameFactory and InterestPolicy.CACHE_CONTENT, we know 
 *  that this VM will be processing events for one hashCode only.
 *  Once we record that hashCode, we'll know what other new keys will
 *  be created in this VM.
 *
 *  Knowledge of the required hashCode is necessary for PR_TX tests to 
 *  only use co-located data in transactions.
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class ObjectNameListener extends util.AbstractListener implements CacheListener, Declarable {

// ArrayList of unique routing objects in this VM
private ArrayList hashCodes = new ArrayList();
private ArrayList keySet = new ArrayList();   // with InterestPolicy.Cache_Content, entries processed by the
                                              // listener must be local to this VM (primary)

// getter methods
public List getHashCodes() {
   return this.hashCodes;
}

public synchronized Object[] getKeySetArray() {
   return this.keySet.toArray();
}

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   processEvent(event);
}
  

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   processEvent(event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   processEvent(event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   processEvent(event);
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
 
private synchronized void processEvent(EntryEvent event) {
   Object key = event.getKey();
   int numVms = TestConfig.getInstance().getTotalVMs()-1;
   if (key instanceof String) {
      int counter = (int)NameFactory.getCounterForName(key);
      int hash = counter % numVms;
      Log.getLogWriter().info("In " + RemoteTestModule.getMyClientName() + ": adding key " + key + " with hashCode " + hash);
      if (!hashCodes.contains(hash)) {
         hashCodes.add(hash);
      } 
   } else {
      throw new TestException("ObjectNameListener expected key of type String , but found " + key.getClass());
   }
   if (event.getOperation().isDestroy()) {
      int index = keySet.indexOf(key);
      keySet.remove(index);
      Log.getLogWriter().info("Removed " + key + " from keySet");
   } else {
      keySet.add(key);
      Log.getLogWriter().info("Added " + key + " from keySet");
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
