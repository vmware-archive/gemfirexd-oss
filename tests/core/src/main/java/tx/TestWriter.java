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
package tx; 

import util.*;
import hydra.*;
import diskReg.DiskRegUtil;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.distributed.*;

/** Serial Tx View Test CacheWriter 
 *  Counts events and validates callback objects and that the event is invoked in the
 *  VM where it was created.
 *
 * @see util.WriterCountersBB
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class TestWriter extends util.AbstractWriter implements CacheWriter, Declarable {

//==============================================================================
// implementation of CacheWriter methods
public void beforeCreate(EntryEvent event) {
   logCall("beforeCreate", event);
   if (processingTx()) {
      incrementBeforeCreateCounters(event, WriterCountersBB.getBB());
   }
   checkVM(event);
   checkCallback(event, TxUtil.createCallbackPrefix);
}

public void beforeDestroy(EntryEvent event) {
   logCall("beforeDestroy", event);
   if (processingTx()) {
      incrementBeforeDestroyCounters(event, WriterCountersBB.getBB());
   }
   checkVM(event);
   checkCallback(event, TxUtil.destroyCallbackPrefix);
}

public void beforeUpdate(EntryEvent event) {
   logCall("beforeUpdate", event);
   if (processingTx()) {
      incrementBeforeUpdateCounters(event, WriterCountersBB.getBB());
   }
   checkVM(event);
   checkCallback(event, TxUtil.updateCallbackPrefix);
}

public void beforeRegionDestroy(RegionEvent event) {
   logCall("beforeRegionDestroy", event);
   checkCallback(event, TxUtil.regionDestroyCallbackPrefix);
   checkVM();

   // Don't check for region access when regionDestroy is part of CACHE_CLOSE
   // or if we're executing region operations concurrently
   if (!event.getOperation().equals(Operation.CACHE_CLOSE)) {
      final Region region = event.getRegion();

      try {
        region.size();  // Execute a region operation to ensure still available
      } catch (Exception e) {
        throwException("TestWriter: beforeRegionDestroy() caught " + e + " during beforeRegionDestroy");
      }
   }
}

public void beforeRegionClear(RegionEvent event) {
   logCall("beforeRegionClear", event);

   // Ensure region still accessible
   final Region region = event.getRegion();
   try {
      region.size();  
   } catch (Exception e) {
      throwException("TestWriter: beforeRegionClear() caught " + e + " during beforeRegionClear");
   }
}

public void close() {
   logCall("close", null);
}

/** Check that this method is running in the same VM where the writer
 *  was created. Log an error if any problems are detected.
 *  If writers are installed on all regions, the product should prefer
 *  a local writer (so it should be in the same VM).
 *
 *  Updated with GemFire 6.5: For PartitionedRegions, the CacheWriter will be invoked
 *  in the primary dataStore (for the entry).
 */
protected void checkVM(EntryEvent event) {
   
   Region region = event.getRegion();
   if (PartitionRegionHelper.isPartitionedRegion(region)) {
     Object key = event.getKey();
     DistributedMember thisDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
     DistributedMember eventDM = PartitionRegionHelper.getPrimaryMemberForKey(region, key);
     if (!thisDM.equals(eventDM)) {
       throwException("TxWriter invoked in " + thisDM + " which is not the primary for entry " + region + " " + key + ".  " + eventDM + " is the primary for this entry");
     }
     return;
   }

   // distributed regions follow the original rules
   checkVM();
}

protected void checkVM() {
   
   int myPID = ProcessMgr.getProcessId();
   if (whereIWasRegistered != myPID) {
      String errStr = "Expected cacheWriter to be invoked in VM " + whereIWasRegistered +
         ", but it was invoked in " + myPID + ": " + toString() + "; see system.log for call stack";
      hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
   }
}
 
/** Check that the callback object is expected. Log an error if any problems are detected.
 *
 *  @param event The event object.
 *  @param expectedCallbackPrefix - The expected prefix on the callback object (String).
 */
protected void checkCallback(CacheEvent event, String expectedCallbackPrefix) {

   // Any 'gets' which result in a LOAD will have a null callback, ignore
   if (event.getOperation().isLoad() || event.getOperation().isPutAll()) {
     return;
   }

   String callbackObj = (String)event.getCallbackArgument();

   if (callbackObj != null) {
   if (!callbackObj.startsWith(expectedCallbackPrefix)) {
      String errStr = "Expected " + expectedCallbackPrefix + ", but callback object is " + 
             TestHelper.toString(callbackObj);
      hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
   }

   // CacheWriters for PartitionedRegions are invoked in the primary dataStore VM
   // not necessarily the VM executing the region operation.
   if (PartitionRegionHelper.isPartitionedRegion(event.getRegion())) {
     return;
   }

      // verify memberID in callbackObj matches what's held in event
      String memberIdString = "memberId=";
      int index = callbackObj.indexOf(memberIdString, 0);
      // Some tests (query) don't assign memberId to callback, so allow for this
      if (index > 0) {
        index += memberIdString.length();
        memberIdString = callbackObj.substring(index).toLowerCase();
        String distributedMemberIdString = event.getDistributedMember().toString().toLowerCase();
        Log.getLogWriter().info("memberId from callbackObj = <" + memberIdString + "> memberId from event = <" + distributedMemberIdString + ">");
        if (!memberIdString.equals(distributedMemberIdString)) {
          String errStr = "Expected <" + distributedMemberIdString + ">, but callback object contains <" + memberIdString + ">";

          hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
          aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
          Log.getLogWriter().info(errStr);
          throw new TestException(errStr);
        }

        // verify isRemote flag
      int myPID = ProcessMgr.getProcessId();
      boolean eventProducedInThisVM = callbackObj.indexOf("pid " + myPID + " ") >= 0;
      boolean isRemote = event.isOriginRemote();
      if (isRemote == eventProducedInThisVM) { // error with isRemote
         String errStr = "Unexpected event.isOriginRemote() = " + isRemote + ", myPID = " + myPID +
                ", callbackObj showing origination VM = " + callbackObj;
         hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
         aMap.put(TestHelper.EVENT_ERROR_KEY, errStr);
         Log.getLogWriter().info(errStr);
         throw new TestException(errStr);
      }
   }
  }
}

private boolean processingTx() {
  // Check to ensure we've started processing transactions ...
  // we don't care about events from region creation/population
  boolean rc = true;
  if (TxBB.getBB().getSharedCounters().read(TxBB.PROCESS_EVENTS) == 0) {
    rc = false;
  }
  return rc;
}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to TxBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
