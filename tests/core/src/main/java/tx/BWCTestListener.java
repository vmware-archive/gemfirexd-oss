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

import hydra.*;
import util.*;
import hydra.Log;
import com.gemstone.gemfire.cache.*;

/** CacheListener to handle CacheEvents during transaction tests: maintains
 *  event counters and performs validation on CacheEvents.  
 *
 *  With pre-70 clients, we cannot validate the memberId (callbackArgs) vs. event.getDistributedMember().
 * 
 *  @author lhughes
 */
public class BWCTestListener extends util.AbstractListener implements CacheListener {

//==============================================================================
// implementation CacheListener methods
// note that RegionEvents are not transactional ... they occur immediately
// on commit state.
//==============================================================================

public void afterCreate(EntryEvent event) {
  logCall("afterCreate", event);
  boolean eventsProcessed = handleEntryEvent(Operation.ENTRY_CREATE, event);
  if (eventsProcessed) {
    incrementAfterCreateCounters(event, EventCountersBB.getBB());
    checkCallback(event, TxUtil.createCallbackPrefix);
  }
}

public void afterDestroy(EntryEvent event) {
  logCall("afterDestroy", event);

  // note that we have to determine whether this is an entry-destroy
  // or an entry-localDestroy.  We need to base this decision on both
  // isDistributed & the scope.
  String opName = Operation.ENTRY_DESTROY;
  if (!event.isDistributed()) {
    RegionAttributes ratts = event.getRegion().getAttributes();
    boolean scopeLocal = (ratts.getScope() == Scope.LOCAL) ? true : false; 
    if (!scopeLocal) {
      opName = Operation.ENTRY_LOCAL_DESTROY;
    }
  }
  boolean eventsProcessed = handleEntryEvent(opName, event);
  if (eventsProcessed) {
    incrementAfterDestroyCounters(event, EventCountersBB.getBB());
    checkCallback(event, TxUtil.destroyCallbackPrefix);
  }
}

public void afterInvalidate(EntryEvent event) {
  logCall("afterInvalidate", event);

  // note that we have to determine whether this is an entry-destroy
  // or an entry-localDestroy.  We need to base this decision on both
  // isDistributed & the scope.
  String opName = Operation.ENTRY_INVAL;
  if (!event.isDistributed()) {
    RegionAttributes ratts = event.getRegion().getAttributes();
    boolean scopeLocal = (ratts.getScope() == Scope.LOCAL) ? true : false; 
    if (!scopeLocal) {
      opName = Operation.ENTRY_LOCAL_INVAL;
    }
  }
  boolean eventsProcessed = handleEntryEvent(opName, event);
  if (eventsProcessed) {
    incrementAfterInvalidateCounters(event, EventCountersBB.getBB());
    checkCallback(event, TxUtil.invalidateCallbackPrefix);
  }
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

public void afterUpdate(EntryEvent event) {
  logCall("afterUpdate", event);
  boolean eventsProcessed = handleEntryEvent(Operation.ENTRY_UPDATE, event);
  if (eventsProcessed) {
    incrementAfterUpdateCounters(event, EventCountersBB.getBB());
    checkCallback(event, TxUtil.updateCallbackPrefix);
  }
}

public void afterRegionDestroy(RegionEvent event) {
  logCall("afterRegionDestroy", event);
  checkCallback(event, TxUtil.regionDestroyCallbackPrefix);
}

public void afterRegionInvalidate(RegionEvent event) {
  logCall("afterRegionInvalidate", event);
  checkCallback(event, TxUtil.regionInvalidateCallbackPrefix);
}

public void close() {
  logCall("close", null);
}

private boolean handleEntryEvent(String opName, EntryEvent event) {
  // Check to ensure we've started processing transactions ...
  // we don't care about events from region creation/population
  if (TxBB.getBB().getSharedCounters().read(TxBB.PROCESS_EVENTS) == 0) {
    Log.getLogWriter().fine("handleEntryEvent - currently not processing events");
    return false;
  }

  // Create an Operation from the info in the EntryEvent
  Object oldValue = event.getOldValue();
  if (oldValue instanceof BaseValueHolder) {
    oldValue = ((BaseValueHolder)oldValue).modVal;
  }

  Object newValue = event.getNewValue();
  if (newValue instanceof BaseValueHolder) {
    newValue = ((BaseValueHolder)newValue).modVal;
  }
  
  Operation op = new Operation(event.getRegion().getFullPath(),
                              event.getKey(),
                              opName,
                              oldValue,
                              newValue);

  Log.getLogWriter().fine("BWCTestListener: op from event = " + op.toString());

  Long txid = new Long(TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER));
  String key = null;
  if ((txid != null) && (txid.intValue() > 0)) {
    if (TxUtil.inTxVm()) {
      key = TxBB.LocalListenerOpListPrefix + txid;
    } else  {
      key = TxBB.RemoteListenerOpListPrefix + txid;
    }

    Log.getLogWriter().info("handleEntryEvent() - adding <" + op.toString() + ">to opList with key <" + key + ">");

    // get the OpList at key ... CacheListenerOpList_<opName>_<txid>
    // save away for validation of TransactionListener
    synchronized(TxBB.getBB()) {
      OpList opList = (OpList)TxBB.getBB().getSharedMap().get(key);
      if (opList == null) {
        opList = new OpList();
      }
      opList.add(op);
      TxBB.getBB().getSharedMap().put(key, opList);
    }
    return true;
  }
  return false;
}

/** Check that the callback object is expected. Log an error if any problems are detected.
 *
 *  @param event The event object.
 *  @param expectedCallbackPrefix - The expected prefix on the callback object (String).
 *  @throws TestException if error encountered during validation
 */
protected void checkCallback(CacheEvent event, String expectedCallbackPrefix) {

   // Events like RegionDestroyed & Close won't have a callback
   if (event.getOperation().isClose()) {
     return;
   }

   // Tx tests must allow null callbackArg in EntryEvents of CacheListeners
   // in remote VMs.  This is as designed.
   //
   // TxUtil does gets() which result in a create (load) ... these will not
   // have a callbackArg.  
   //if (!TxUtil.inTxVm() || event.getOperation().isLoad()) {
   if (event.isOriginRemote() || event.getOperation().isLoad() || event.getOperation().isPutAll()) {
     return;
   }

   String callbackObj = (String)event.getCallbackArgument();

  if (callbackObj != null) {
     // verify memberID in callbackObj matches what's held in event
      String memberIdString = "memberId=";
      int index = callbackObj.indexOf(memberIdString, 0);
      index += memberIdString.length();
      memberIdString = callbackObj.substring(index);
      Log.getLogWriter().info("memberId from callbackObj = <" + memberIdString + "> memberId from event = <" + event.getDistributedMember() + ">");
     // todo@lhughes -- why is the memberId from the callbackArg different from the event.getDistributedMember()?
/*
      if (!memberIdString.equals(event.getDistributedMember().toString())) {
        String errStr = "Expected <" + event.getDistributedMember() + ">, but callback object contains <" + memberIdString + ">";
        throwException(errStr);
      }
*/

      // verify isRemote flag
      int myPID = ProcessMgr.getProcessId();
      boolean eventProducedInThisVM = callbackObj.indexOf("" + myPID + " ") >= 0;
      boolean isRemote = event.isOriginRemote();
      if (isRemote == eventProducedInThisVM) { // error with isRemote
         String errStr = "Unexpected event.isOriginRemote() = " + isRemote + ", myPID = " + myPID + ", callbackObj showing origination VM = " + callbackObj;
         throwException(errStr);
      }
   }
}

/** Utility method to log and store an error string into the EventBB and 
 *  throw a TestException containing the given string.
 *
 *  @param errStr string to log, store in EventBB and include in TestException
 *  @throws TestException containing the given string
 * 
 *  @see TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());  
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}


}
