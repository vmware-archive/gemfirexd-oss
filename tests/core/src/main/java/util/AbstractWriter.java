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
package util; 

import util.TestHelper;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import hydra.blackboard.*;

import java.util.*;

/** An abstract writer class that provides methods to subclasses for
 *  logging events, incrementing counters for invoked events, and
 *  keeping information about the VM where the writer was created.
 */
public abstract class AbstractWriter {

/** To use counter increment methods defined in this class, the blackboard
 *  that is passed to the increment method must have counters defined with
 *  these names, such as util.WriterCountersBB.java
 */
public final String CntrName_BeforeCreate_isDist         = "numBeforeCreateEvents_isDist";
public final String CntrName_BeforeCreate_isNotDist      = "numBeforeCreateEvents_isNotDist";
public final String CntrName_BeforeCreate_isExp          = "numBeforeCreateEvents_isExp";
public final String CntrName_BeforeCreate_isNotExp       = "numBeforeCreateEvents_isNotExp";
public final String CntrName_BeforeCreate_isRemote       = "numBeforeCreateEvents_isRemote";
public final String CntrName_BeforeCreate_isNotRemote    = "numBeforeCreateEvents_isNotRemote";
public final String CntrName_BeforeCreate_isLoad         = "numBeforeCreateEvents_isLoad";
public final String CntrName_BeforeCreate_isNotLoad      = "numBeforeCreateEvents_isNotLoad";
public final String CntrName_BeforeCreate_isLocalLoad    = "numBeforeCreateEvents_isLocalLoad";
public final String CntrName_BeforeCreate_isNotLocalLoad = "numBeforeCreateEvents_isNotLocalLoad";
public final String CntrName_BeforeCreate_isNetLoad      = "numBeforeCreateEvents_isNetLoad";
public final String CntrName_BeforeCreate_isNotNetLoad   = "numBeforeCreateEvents_isNotNetLoad";
public final String CntrName_BeforeCreate_isNetSearch    = "numBeforeCreateEvents_isNetSearch";
public final String CntrName_BeforeCreate_isNotNetSearch = "numBeforeCreateEvents_isNotNetSearch";

public final String CntrName_BeforeDestroy_isDist         = "numBeforeDestroyEvents_isDist";
public final String CntrName_BeforeDestroy_isNotDist      = "numBeforeDestroyEvents_isNotDist";
public final String CntrName_BeforeDestroy_isExp          = "numBeforeDestroyEvents_isExp";
public final String CntrName_BeforeDestroy_isNotExp       = "numBeforeDestroyEvents_isNotExp";
public final String CntrName_BeforeDestroy_isRemote       = "numBeforeDestroyEvents_isRemote";
public final String CntrName_BeforeDestroy_isNotRemote    = "numBeforeDestroyEvents_isNotRemote";
public final String CntrName_BeforeDestroy_isLoad         = "numBeforeDestroyEvents_isLoad";
public final String CntrName_BeforeDestroy_isNotLoad      = "numBeforeDestroyEvents_isNotLoad";
public final String CntrName_BeforeDestroy_isLocalLoad    = "numBeforeDestroyEvents_isLocalLoad";
public final String CntrName_BeforeDestroy_isNotLocalLoad = "numBeforeDestroyEvents_isNotLocalLoad";
public final String CntrName_BeforeDestroy_isNetLoad      = "numBeforeDestroyEvents_isNetLoad";
public final String CntrName_BeforeDestroy_isNotNetLoad   = "numBeforeDestroyEvents_isNotNetLoad";
public final String CntrName_BeforeDestroy_isNetSearch    = "numBeforeDestroyEvents_isNetSearch";
public final String CntrName_BeforeDestroy_isNotNetSearch = "numBeforeDestroyEvents_isNotNetSearch";

public final String CntrName_BeforeUpdate_isDist         = "numBeforeUpdateEvents_isDist";
public final String CntrName_BeforeUpdate_isNotDist      = "numBeforeUpdateEvents_isNotDist";
public final String CntrName_BeforeUpdate_isExp          = "numBeforeUpdateEvents_isExp";
public final String CntrName_BeforeUpdate_isNotExp       = "numBeforeUpdateEvents_isNotExp";
public final String CntrName_BeforeUpdate_isRemote       = "numBeforeUpdateEvents_isRemote";
public final String CntrName_BeforeUpdate_isNotRemote    = "numBeforeUpdateEvents_isNotRemote";
public final String CntrName_BeforeUpdate_isLoad         = "numBeforeUpdateEvents_isLoad";
public final String CntrName_BeforeUpdate_isNotLoad      = "numBeforeUpdateEvents_isNotLoad";
public final String CntrName_BeforeUpdate_isLocalLoad    = "numBeforeUpdateEvents_isLocalLoad";
public final String CntrName_BeforeUpdate_isNotLocalLoad = "numBeforeUpdateEvents_isNotLocalLoad";
public final String CntrName_BeforeUpdate_isNetLoad      = "numBeforeUpdateEvents_isNetLoad";
public final String CntrName_BeforeUpdate_isNotNetLoad   = "numBeforeUpdateEvents_isNotNetLoad";
public final String CntrName_BeforeUpdate_isNetSearch    = "numBeforeUpdateEvents_isNetSearch";
public final String CntrName_BeforeUpdate_isNotNetSearch = "numBeforeUpdateEvents_isNotNetSearch";

public final String CntrName_BeforeRegionDestroy_isDist      = "numBeforeRegionDestroyEvents_isDist";
public final String CntrName_BeforeRegionDestroy_isNotDist   = "numBeforeRegionDestroyEvents_isNotDist";
public final String CntrName_BeforeRegionDestroy_isExp       = "numBeforeRegionDestroyEvents_isExp";
public final String CntrName_BeforeRegionDestroy_isNotExp    = "numBeforeRegionDestroyEvents_isNotExp";
public final String CntrName_BeforeRegionDestroy_isRemote    = "numBeforeRegionDestroyEvents_isRemote";
public final String CntrName_BeforeRegionDestroy_isNotRemote = "numBeforeRegionDestroyEvents_isNotRemote";

public final String CntrName_BeforeRegionClear_isDist      = "numBeforeRegionClearEvents_isDist";
public final String CntrName_BeforeRegionClear_isNotDist   = "numBeforeRegionClearEvents_isNotDist";
public final String CntrName_BeforeRegionClear_isExp       = "numBeforeRegionClearEvents_isExp";
public final String CntrName_BeforeRegionClear_isNotExp    = "numBeforeRegionClearEvents_isNotExp";
public final String CntrName_BeforeRegionClear_isRemote    = "numBeforeRegionClearEvents_isRemote";
public final String CntrName_BeforeRegionClear_isNotRemote = "numBeforeRegionClearEvents_isNotRemote";

/** The process ID of the VM that created this writer */
public int whereIWasRegistered;

/** Constructor */
public AbstractWriter() {
   whereIWasRegistered = ProcessMgr.getProcessId();
}

/** Return a string description of the event. 
 *
 *  @param methodName The name of the CacheEvent method that was invoked.
 *  @param event The event object that was passed to the event.
 *
 *  @return A String description of the invoked event.
 */
public String toString(String methodName, CacheEvent event) {
   StringBuffer aStr = new StringBuffer();
   String clientName = RemoteTestModule.getMyClientName();
   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + clientName + "\n");
   aStr.append("   whereIWasRegistered: " + whereIWasRegistered + "\n");

   if (event == null)
      return aStr.toString();

   // beforeCreate, beforeDestroy, beforeUpdate
   if (event instanceof EntryEvent) {
      EntryEvent eEvent= (EntryEvent)event;
      aStr.append("   event.getKey(): " + eEvent.getKey() + "\n");
      aStr.append("   event.getOldValue(): " + getOldValueStr(eEvent) + "\n");
      aStr.append("   event.getNewValue(): " + getNewValueStr(eEvent) + "\n");
      aStr.append("   event.isLoad(): " + eEvent.isLoad() + "\n");
      aStr.append("   event.isLocalLoad(): " + eEvent.isLocalLoad() + "\n");
      aStr.append("   event.isNetLoad(): " + eEvent.isNetLoad() + "\n");
      aStr.append("   event.isNetSearch(): " + eEvent.isNetSearch() + "\n");
   } 

   // beforeRegionDestroy, beforeRegionClear
   if (event instanceof RegionEvent) {
      RegionEvent rEvent = (RegionEvent)event;
      aStr.append("   event.isReinitializing(): " + rEvent.isReinitializing() + "\n");
   }

   aStr.append("   event.getMemberId(): " + event.getDistributedMember().toString() + "\n");
   aStr.append("   event.getCallbackArgument(): " + TestHelper.toString(event.getCallbackArgument()) + "\n");
   aStr.append("   event.getRegion(): " + TestHelper.regionToString(event.getRegion(), false) + "\n");
   aStr.append("   event.isDistributed(): " + event.isDistributed() + "\n");
   aStr.append("   event.isExpiration(): " + event.isExpiration() + "\n");
   aStr.append("   event.isOriginRemote(): " + event.isOriginRemote() + "\n");

   Operation op = event.getOperation();
   aStr.append("   Operation: " + op.toString() + "\n");
   if (op.isEntry()) {
     aStr.append("   Operation.isLoad(): " + op.isLoad() + "\n");
     aStr.append("   Operation.isLocalLoad(): " + op.isLocalLoad() + "\n");
     aStr.append("   Operation.isNetLoad(): " + op.isNetLoad() + "\n");
     aStr.append("   Operation.isNetSearch(): " + op.isNetSearch() + "\n");
   }
   aStr.append("   Operation.isDistributed(): " + op.isDistributed() + "\n");
   aStr.append("   Operation.isExpiration(): " + op.isExpiration() + "\n");

//   aStr.append(TestHelper.getStackTrace());
   return aStr.toString();
}

/**
 * @param eEvent
 * @return
 */
public String getOldValueStr(EntryEvent eEvent) {
  return TestHelper.toString(eEvent.getOldValue());
}

/**
 * @return
 */
public String getNewValueStr(EntryEvent eEvent) {
  return TestHelper.toString(eEvent.getNewValue());
}

/** Return a string description of the TransactionEvent. 
 *
 *  @param methodName The name of the TransactionEvent method that was invoked.
 *  @param event The TransactionEvent object that was passed to the Transaction
 *               Writer.
 *
 *  @return A String description of the invoked transaction event.
 */
public String toString(String methodName, TransactionEvent event) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName() + "\n");
   aStr.append("   whereIWasRegistered: " + whereIWasRegistered + "\n");
   if (event == null)
      return aStr.toString();

   aStr.append("   event.getTransactionId(): " + event.getTransactionId() + "\n");
      
   // use newer API (not deprecated getCreateEvents, etc).
   Collection txEvents = event.getEvents();
   aStr.append("TxEvent contains " + txEvents.size() + " entry events\n");
   aStr.append(getEntryEvents(txEvents));
   return aStr.toString();
}

// Display ordered list of TX Entry Events
private String getEntryEvents(Collection txEvents) {
   StringBuffer aStr = new StringBuffer();
   for (Iterator it = txEvents.iterator(); it.hasNext();) {
     EntryEvent eEvent = (EntryEvent)it.next();
     aStr.append(toString(eEvent.getOperation().toString(), eEvent));
     aStr.append("\n");
   }
   return aStr.toString();
}

/** Log that a transaction event occurred.
 *
 *  @param methodName The name of the TransactionEvent method that was invoked.
 *                    (beforeCommit)
 *  @param event The TransactionEvent object that was passed to the event.
 */ 
public String logTxEvent(String methodName, TransactionEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}

/** Log that an event occurred.
 *
 *  @param methodName The name of the CacheEvent method that was invoked.
 *  @param event The event object that was passed to the event.
 */ 
public String logCall(String methodName, CacheEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}

/** Increment appropriate blackboard counters for a beforeCreate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementBeforeCreateCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeCreate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for a beforeDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementBeforeDestroyCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeDestroy_isNotNetSearch));
}

/** Increment appropriate blackboard counters for a beforeUpdate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementBeforeUpdateCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeUpdate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for a beforeRegionDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementBeforeRegionDestroyCounters(RegionEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionDestroy_isNotRemote));
}

/** Increment appropriate blackboard counters for a beforeRegionClear event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementBeforeRegionClearCounters(RegionEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_BeforeRegionClear_isNotRemote));
}

}
