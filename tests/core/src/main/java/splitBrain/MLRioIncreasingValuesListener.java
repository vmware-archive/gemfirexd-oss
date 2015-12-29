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
package splitBrain; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

import java.util.HashMap;
import java.util.Map;

/** MLRio IncreasingValues Listener. 
 *  Logs create and  update Events processed by the CacheListener (if they violate a constraint or fine logging enabled).  Allows skips in sequence (useful for testing Gateway w/batchConflation true).
 *  This can only be used in tests with a single publisher (for each key)
 *  where the updates contain ValueHolders with modVal Integer values which are always increasing
 *
 *  Constraints:
 *  - skips in sequence are permitted, and counts accumulated in the MLRioBB (missedUpdates)
 *  - values must be increasing (Exception thrown if newValue less than  oldValue)
 *  - duplicates (oldValue.equals(newValue) are not permitted (Exception thrown)
 *
 *  This Exception can be retrieved via TestHelper.checkForEventError().
 *     @see util.TestHelper.checkForEventError
 *     @see util.TestHelper.EVENT_ERROR_KEY
 *
 * @author lhughes
 * @since 5.5
 */
public class MLRioIncreasingValuesListener extends util.AbstractListener implements CacheListener, Declarable {

// Implement the EntryEvent portion of this interface
// Currently only tracks create/update events, flagging skips in sequence
public void afterCreate(EntryEvent event) {
   if (Log.getLogWriter().fineEnabled()) {
      logCall("afterCreate", event);
   }
}

public void afterUpdate(EntryEvent event) {
   Object key = event.getKey();
   Integer newValue = ((BaseValueHolder)event.getNewValue()).modVal;
   Integer oldValue = ((BaseValueHolder)event.getOldValue()).modVal;

   // report missing, duplicate or late event arrival
   StringBuffer errMsg = new StringBuffer();
   int diff = newValue.intValue() - oldValue.intValue();
   // values should always be increasing (allow skip in the value seq)
   if (newValue.intValue() < oldValue.intValue()) {
      logCall("afterUpdate", event);
      errMsg.append("Value for Key(" + key + ") did not increase as expected, event.getOldValue() =  " + oldValue + ", event.getNewValue() = " + newValue);
      throwException(errMsg.toString());
   } else if (diff > 1) {   
      // count skips in sequence, okay but need to compare w/conflation counts
      logCall("afterUpdate", event);
      MLRioBB.getBB().getSharedCounters().add(MLRioBB.missedUpdates, diff-1);
   } else if (diff == 0) {   // duplicate update
      logCall("afterUpdate", event);
      long ctr = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.duplicateUpdates);
      errMsg.append("duplicate update, total number of duplicates = " + ctr + "\n");
      errMsg.append("Key = " + event.getKey() + ", oldValue(modVal) = " + oldValue.intValue() + ", newValue(modVal) = " + newValue.intValue());
      throwException(errMsg.toString());
   } else if (Log.getLogWriter().fineEnabled()) {
      logCall("afterUpdate", event);
   }
}

public void afterDestroy(EntryEvent event) {
   //logCall("afterDestroy", event);
}

public void afterInvalidate(EntryEvent event) {
   //logCall("afterInvalidate", event);
}

public void afterRegionDestroy(RegionEvent event) {
   //logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   //logCall("afterRegionInvalidate", event);
}

public void close() {
   //logCall("close", null);
}

public void afterRegionClear(RegionEvent event) {
  //logCall("afterRegionClear", event);
}

public void afterRegionCreate(RegionEvent event) {
  //logCall("afterRegionCreate", event);
}

public void afterRegionLive(RegionEvent event) {
  //logCall("afterRegionLive", event);
}

public void init(java.util.Properties prop) {
   //logCall("init(Properties)", null);
}

/**
 * Utility method to write an Exception string to the MLRio Blackboard and
 * to also Log an exception containing the same string.  Only the first
 * Exception encountered is written to the Blackboard.  All Exceptions
 * are logged (but not thrown).
 *
 * @param errStr String to log and post to MLRioBB
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = MLRioBB.getBB().getSharedMap();
      // we only store the very first problem encountered (all are logged)
      if (aMap.get(TestHelper.EVENT_ERROR_KEY) == null) {
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " in " + getMyUniqueName() + " " + TestHelper.getStackTrace());
      }
      Log.getLogWriter().info("Listener encountered Exception: " + errStr + ", written to MLRioBB");
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
