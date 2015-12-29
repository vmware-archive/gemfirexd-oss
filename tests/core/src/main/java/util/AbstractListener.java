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
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.cache.query.*;
import hydra.blackboard.*;
//import java.lang.reflect.*;
import java.util.*;

/** An abstract listener class that provides methods to subclasses for
 *  logging events, incrementing counters for invoked events, and
 *  keeping information about the VM where the listener was created.
 */
public abstract class AbstractListener {

/** To use counter increment methods defined in this class, the blackboard
 *  that is passed to the increment method must have counters defined with
 *  these names, such as util.EventCountersBB.
 */
public final String CntrName_AfterCreate_isDist         = "numAfterCreateEvents_isDist";
public final String CntrName_AfterCreate_isNotDist      = "numAfterCreateEvents_isNotDist";
public final String CntrName_AfterCreate_isExp          = "numAfterCreateEvents_isExp";
public final String CntrName_AfterCreate_isNotExp       = "numAfterCreateEvents_isNotExp";
public final String CntrName_AfterCreate_isRemote       = "numAfterCreateEvents_isRemote";
public final String CntrName_AfterCreate_isNotRemote    = "numAfterCreateEvents_isNotRemote";
public final String CntrName_AfterCreate_isLoad         = "numAfterCreateEvents_isLoad";
public final String CntrName_AfterCreate_isNotLoad      = "numAfterCreateEvents_isNotLoad";
public final String CntrName_AfterCreate_isLocalLoad    = "numAfterCreateEvents_isLocalLoad";
public final String CntrName_AfterCreate_isNotLocalLoad = "numAfterCreateEvents_isNotLocalLoad";
public final String CntrName_AfterCreate_isNetLoad      = "numAfterCreateEvents_isNetLoad";
public final String CntrName_AfterCreate_isNotNetLoad   = "numAfterCreateEvents_isNotNetLoad";
public final String CntrName_AfterCreate_isNetSearch    = "numAfterCreateEvents_isNetSearch";
public final String CntrName_AfterCreate_isNotNetSearch = "numAfterCreateEvents_isNotNetSearch";
public final String CntrName_AfterCreate_isPutAll       = "numAfterCreateEvents_isPutAll";
public final String CntrName_AfterCreate_isNotPutAll    = "numAfterCreateEvents_isNotPutAll";

public final String CntrName_AfterDestroy_isDist         = "numAfterDestroyEvents_isDist";
public final String CntrName_AfterDestroy_isNotDist      = "numAfterDestroyEvents_isNotDist";
public final String CntrName_AfterDestroy_isExp          = "numAfterDestroyEvents_isExp";
public final String CntrName_AfterDestroy_isNotExp       = "numAfterDestroyEvents_isNotExp";
public final String CntrName_AfterDestroy_isRemote       = "numAfterDestroyEvents_isRemote";
public final String CntrName_AfterDestroy_isNotRemote    = "numAfterDestroyEvents_isNotRemote";
public final String CntrName_AfterDestroy_isLoad         = "numAfterDestroyEvents_isLoad";
public final String CntrName_AfterDestroy_isNotLoad      = "numAfterDestroyEvents_isNotLoad";
public final String CntrName_AfterDestroy_isLocalLoad    = "numAfterDestroyEvents_isLocalLoad";
public final String CntrName_AfterDestroy_isNotLocalLoad = "numAfterDestroyEvents_isNotLocalLoad";
public final String CntrName_AfterDestroy_isNetLoad      = "numAfterDestroyEvents_isNetLoad";
public final String CntrName_AfterDestroy_isNotNetLoad   = "numAfterDestroyEvents_isNotNetLoad";
public final String CntrName_AfterDestroy_isNetSearch    = "numAfterDestroyEvents_isNetSearch";
public final String CntrName_AfterDestroy_isNotNetSearch = "numAfterDestroyEvents_isNotNetSearch";

public final String CntrName_AfterInvalidate_isDist         = "numAfterInvalidateEvents_isDist";
public final String CntrName_AfterInvalidate_isNotDist      = "numAfterInvalidateEvents_isNotDist";
public final String CntrName_AfterInvalidate_isExp          = "numAfterInvalidateEvents_isExp";
public final String CntrName_AfterInvalidate_isNotExp       = "numAfterInvalidateEvents_isNotExp";
public final String CntrName_AfterInvalidate_isRemote       = "numAfterInvalidateEvents_isRemote";
public final String CntrName_AfterInvalidate_isNotRemote    = "numAfterInvalidateEvents_isNotRemote";
public final String CntrName_AfterInvalidate_isLoad         = "numAfterInvalidateEvents_isLoad";
public final String CntrName_AfterInvalidate_isNotLoad      = "numAfterInvalidateEvents_isNotLoad";
public final String CntrName_AfterInvalidate_isLocalLoad    = "numAfterInvalidateEvents_isLocalLoad";
public final String CntrName_AfterInvalidate_isNotLocalLoad = "numAfterInvalidateEvents_isNotLocalLoad";
public final String CntrName_AfterInvalidate_isNetLoad      = "numAfterInvalidateEvents_isNetLoad";
public final String CntrName_AfterInvalidate_isNotNetLoad   = "numAfterInvalidateEvents_isNotNetLoad";
public final String CntrName_AfterInvalidate_isNetSearch    = "numAfterInvalidateEvents_isNetSearch";
public final String CntrName_AfterInvalidate_isNotNetSearch = "numAfterInvalidateEvents_isNotNetSearch";

public final String CntrName_AfterUpdate_isDist         = "numAfterUpdateEvents_isDist";
public final String CntrName_AfterUpdate_isNotDist      = "numAfterUpdateEvents_isNotDist";
public final String CntrName_AfterUpdate_isExp          = "numAfterUpdateEvents_isExp";
public final String CntrName_AfterUpdate_isNotExp       = "numAfterUpdateEvents_isNotExp";
public final String CntrName_AfterUpdate_isRemote       = "numAfterUpdateEvents_isRemote";
public final String CntrName_AfterUpdate_isNotRemote    = "numAfterUpdateEvents_isNotRemote";
public final String CntrName_AfterUpdate_isLoad         = "numAfterUpdateEvents_isLoad";
public final String CntrName_AfterUpdate_isNotLoad      = "numAfterUpdateEvents_isNotLoad";
public final String CntrName_AfterUpdate_isLocalLoad    = "numAfterUpdateEvents_isLocalLoad";
public final String CntrName_AfterUpdate_isNotLocalLoad = "numAfterUpdateEvents_isNotLocalLoad";
public final String CntrName_AfterUpdate_isNetLoad      = "numAfterUpdateEvents_isNetLoad";
public final String CntrName_AfterUpdate_isNotNetLoad   = "numAfterUpdateEvents_isNotNetLoad";
public final String CntrName_AfterUpdate_isNetSearch    = "numAfterUpdateEvents_isNetSearch";
public final String CntrName_AfterUpdate_isNotNetSearch = "numAfterUpdateEvents_isNotNetSearch";
public final String CntrName_AfterUpdate_isPutAll = "numAfterUpdateEvents_isPutAll";
public final String CntrName_AfterUpdate_isNotPutAll = "numAfterUpdateEvents_isNotPutAll";

public final String CntrName_AfterRegionDestroy_isDist      = "numAfterRegionDestroyEvents_isDist";
public final String CntrName_AfterRegionDestroy_isNotDist   = "numAfterRegionDestroyEvents_isNotDist";
public final String CntrName_AfterRegionDestroy_isExp       = "numAfterRegionDestroyEvents_isExp";
public final String CntrName_AfterRegionDestroy_isNotExp    = "numAfterRegionDestroyEvents_isNotExp";
public final String CntrName_AfterRegionDestroy_isRemote    = "numAfterRegionDestroyEvents_isRemote";
public final String CntrName_AfterRegionDestroy_isNotRemote = "numAfterRegionDestroyEvents_isNotRemote";

public final String CntrName_AfterRegionInvalidate_isDist      = "numAfterRegionInvalidateEvents_isDist";
public final String CntrName_AfterRegionInvalidate_isNotDist   = "numAfterRegionInvalidateEvents_isNotDist";
public final String CntrName_AfterRegionInvalidate_isExp       = "numAfterRegionInvalidateEvents_isExp";
public final String CntrName_AfterRegionInvalidate_isNotExp    = "numAfterRegionInvalidateEvents_isNotExp";
public final String CntrName_AfterRegionInvalidate_isRemote    = "numAfterRegionInvalidateEvents_isRemote";
public final String CntrName_AfterRegionInvalidate_isNotRemote = "numAfterRegionInvalidateEvents_isNotRemote";

public final String CntrName_AfterRegionCreate_isDist      = "numAfterRegionCreateEvents_isDist";
public final String CntrName_AfterRegionCreate_isNotDist   = "numAfterRegionCreateEvents_isNotDist";
public final String CntrName_AfterRegionCreate_isExp       = "numAfterRegionCreateEvents_isExp";
public final String CntrName_AfterRegionCreate_isNotExp    = "numAfterRegionCreateEvents_isNotExp";
public final String CntrName_AfterRegionCreate_isRemote    = "numAfterRegionCreateEvents_isRemote";
public final String CntrName_AfterRegionCreate_isNotRemote = "numAfterRegionCreateEvents_isNotRemote";

public final String CntrName_AfterRegionLive_isDist      = "numAfterRegionLiveEvents_isDist";
public final String CntrName_AfterRegionLive_isNotDist   = "numAfterRegionLiveEvents_isNotDist";
public final String CntrName_AfterRegionLive_isExp       = "numAfterRegionLiveEvents_isExp";
public final String CntrName_AfterRegionLive_isNotExp    = "numAfterRegionLiveEvents_isNotExp";
public final String CntrName_AfterRegionLive_isRemote    = "numAfterRegionLiveEvents_isRemote";
public final String CntrName_AfterRegionLive_isNotRemote = "numAfterRegionLiveEvents_isNotRemote";

public final String CntrName_Close = "numClose";

/** The process ID of the VM that created this listener */
public int whereIWasRegistered;

/** Constructor */
public AbstractListener() {
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
   if (event instanceof EntryEvent) {
      aStr.append("Invoked " + this.getClass().getName() + " for key " + ((EntryEvent)event).getKey() +
                  ": " + methodName + " in " + clientName + " event=" + event + "\n");
   } else {
     aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + clientName + "\n");
   }
   aStr.append("   whereIWasRegistered: " + whereIWasRegistered + "\n");

   // For special multi-listener class, display encoded listener id 
   if (this instanceof event.MultiListener) {
      aStr.append("   MultiListener: " + ((event.MultiListener)this).getName() + "\n");
   }
   if (this instanceof event.RegionListener) {
      aStr.append("   RegionListener: " + ((event.RegionListener)this).getName() + "\n");
   }

   if (event == null)
      return aStr.toString();

   Operation op = event.getOperation();

   if (event instanceof EntryEvent) {
      EntryEvent eEvent= (EntryEvent)event;
      aStr.append("   event.getKey(): " + eEvent.getKey() + "\n");
      aStr.append("   event.getOldValue(): " + getOldValueStr(eEvent) + "\n");
      aStr.append("   event.getNewValue(): " + getNewValueStr(eEvent) + "\n");
      aStr.append("   event.isLoad(): " + eEvent.isLoad() + "\n");
      aStr.append("   event.isLocalLoad(): " + eEvent.isLocalLoad() + "\n");
      aStr.append("   event.isNetLoad(): " + eEvent.isNetLoad() + "\n");
      aStr.append("   event.isNetSearch(): " + eEvent.isNetSearch() + "\n");
      aStr.append(TestHelperVersionHelper.getConcurrencyConflict(eEvent));
   } 

   if (event instanceof RoleEvent) {
      RoleEvent rEvent = (RoleEvent)event;
      aStr.append("   event.isReinitializing(): " + rEvent.isReinitializing()  + "\n");
      Set aSet = rEvent.getRequiredRoles();
      aStr.append("   RequiredRoles: \n");
      for (Iterator iter = aSet.iterator(); iter.hasNext(); ) {
         Role role = (Role)iter.next();
         aStr.append("     Role = " + role.getName() + " isPresent = " + role.isPresent() + " Count = " + role.getCount());
      }
   aStr.append("\n");
   }

   if (event instanceof RegionEvent) {
      RegionEvent rEvent = (RegionEvent)event;
      aStr.append("   event.isReinitializing(): " + rEvent.isReinitializing() + "\n");
   }

   aStr.append("   event.getDistributedMember(): " + event.getDistributedMember().toString() + "\n");
   aStr.append("   event.getCallbackArgument(): " + TestHelper.toString(event.getCallbackArgument()) + "\n");
   aStr.append("   event.getRegion(): " + TestHelper.regionToString(event.getRegion(), false) + "\n");
   aStr.append("   event.isDistributed(): " + event.isDistributed() + "\n");
   aStr.append("   event.isExpiration(): " + event.isExpiration() + "\n");
   aStr.append("   event.isOriginRemote(): " + event.isOriginRemote() + "\n");

   aStr.append("   Operation: " + op.toString() + "\n");
   if (op.isEntry()) {
     aStr.append("   Operation.isLoad(): " + op.isLoad() + "\n");
     aStr.append("   Operation.isLocalLoad(): " + op.isLocalLoad() + "\n");
     aStr.append("   Operation.isNetLoad(): " + op.isNetLoad() + "\n");
     aStr.append("   Operation.isNetSearch(): " + op.isNetSearch() + "\n");
   }
   aStr.append("   Operation.isPutAll(): " + op.isPutAll() + "\n");
   aStr.append("   Operation.isDistributed(): " + op.isDistributed() + "\n");
   aStr.append("   Operation.isExpiration(): " + op.isExpiration());

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


/** Return a string description of the CqEvent. 
 *
 *  @param methodName The name of the CqListener method that was invoked.
 *  @param event The CqEvent object that was passed to the CqListener
 *
 *  @return A String description of the invoked CqEvent
 */
public String toString(String methodName, CqEvent event) {
   StringBuffer aStr = new StringBuffer();

   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName() + "\n");
   aStr.append("   whereIWasRegistered: " + whereIWasRegistered + "\n");

   // For special multi-listener class, display encoded listener id 
   if (this instanceof cq.MultiListener) {
      aStr.append("   MultiListener: " + ((cq.MultiListener)this).getName() + "\n");
   } 

   if (event == null)
      return aStr.toString();

   aStr.append("   event.getBaseOperation(): " + event.getBaseOperation().toString() + "\n");
   aStr.append("   event.getKey(): " + event.getKey() + "\n");
   aStr.append("   event.getNewValue(): " + TestHelper.toString(event.getNewValue()) + "\n");
   aStr.append("   event.getCq():\n" + toString(event.getCq()) + "\n");

   // todo@lhughes - don't attempt to display queryOperation if onError (NPE)
   if (event.getThrowable() != null) {
      aStr.append("    event.getThrowable(): " + event.getThrowable().toString());
   } else {
      aStr.append("   event.getQueryOperation(): " + event.getQueryOperation().toString());
   }

   return aStr.toString();

}

/** Return a string description of a CqQuery
 *
 *  @param CqQuery - The CqQuery to interpret
 *  @return A string description of the CqQuery
 */
public String toString(CqQuery cq) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("      cq.getName(): " + cq.getName() + "\n");
   aStr.append("      cq.getQueryString(): " + cq.getQueryString() + "\n");
   aStr.append("      cq.getState(): " + cq.getState().toString());
   return aStr.toString();
}

/** Return a string description of the TransactionEvent. 
 *
 *  @param methodName The name of the TransacationEvent method that was invoked.
 *  @param event The TransactionEvent object that was passed to the Transaction
 *               Listener.
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

/** Log that a transaction event occurred.
 *
 *  @param methodName The name of the TransactionEvent method that was invoked.
 *                    (commit, rollback or failedCommit)
 *  @param event The TransactionEvent object that was passed to the event.
 */ 
public String logTxEvent(String methodName, TransactionEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}

/** Log that a CqEvent occurred.
 *
 *  @param methodName The name of the CqEvent method that was invoked
 *         (onEvent, onError)
 *  @param event The CqEvent object passed to the event method
 */
public String logCQEvent(String methodName, CqEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}

/** Log that a CqEvent occurred, using a one line summary string
 *  of the event that is useful for searching logs in a failed run.
 *
 *  @param methodName The name of the CqEvent method that was invoked
 *         (onEvent, onError)
 *  @param event The CqEvent object passed to the event method
 */
public String logCQEventAsSummary(String methodName, CqEvent event) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + 
        RemoteTestModule.getMyClientName() + " key="+event.getKey()+" whereIWasRegistered: " + whereIWasRegistered+" ");
   if (event == null) {
      return aStr.toString();
   } else { // event is not null, we just want a summary string
      aStr.append("baseOperation: " + event.getBaseOperation() + ", ");
      aStr.append("queryOperation: " + event.getQueryOperation() + ", ");
      if (!DistributionManager.VERBOSE) { // [bruce] change OK'd by Lynn H
          aStr.append("key: " + event.getKey() + ", ");
      }
      aStr.append("cqName: " + event.getCq().getName() + ", ");
      aStr.append("queryString: " + event.getCq().getQueryString() + ", ");
      aStr.append("newValue: " + TestHelper.toString(event.getNewValue()) + ", ");
      Log.getLogWriter().info(aStr.toString());
   }
   return aStr.toString();
}

/** Increment appropriate blackboard counters for an afterCreate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterCreateCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterDestroyCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterInvalidate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterInvalidateCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterUpdate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterUpdateCounters(EntryEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotRemote));
   if (event.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotLoad));
   if (event.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotLocalLoad));
   if (event.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotNetLoad));
   if (event.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterRegionCreate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionCreateCounters(RegionEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isNotRemote));
}

/** Increment appropriate blackboard counters for an afterRegionLive event
*
*  @param event The event object for this event.
*  @param bb Any blackboard object containing event counters with names
*            specified by the names defined in this class.
*/
public void incrementAfterRegionLiveCounters(RegionEvent event, Blackboard bb) {
  SharedCounters counters = bb.getSharedCounters();
  if (event.isDistributed()) 
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isDist));
  else
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isNotDist));
  if (event.isExpiration())
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isExp));
  else
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isNotExp));
  if (event.isOriginRemote())
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isRemote));
  else
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isNotRemote));
}

/** Increment appropriate blackboard counters for an afterRegionDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionDestroyCounters(RegionEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isNotRemote));
}

/** Increment appropriate blackboard counters for an afterRegionInvalidate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionInvalidateCounters(RegionEvent event, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (event.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isNotDist));
   if (event.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isNotExp));
   if (event.isOriginRemote())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isRemote));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isNotRemote));
}

/** Increment appropriate blackboard counters for a close event
 *
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementCloseCounter(Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   counters.increment(bb.getSharedCounter(CntrName_Close));
}

//********************************************************************
// Support for new cache.Operation methods & counters
//********************************************************************

/** Increment appropriate blackboard counters for an afterCreate event
 *
 *  @param operation The event.getOperation() object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterCreateCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotExp));
   if (operation.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotLoad));
   if (operation.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotLocalLoad));
   if (operation.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotNetLoad));
   if (operation.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotNetSearch));
   if (operation.isPutAll())
     counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isPutAll));
   else
     counters.increment(bb.getSharedCounter(CntrName_AfterCreate_isNotPutAll));
}

/** Increment appropriate blackboard counters for an afterDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterDestroyCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotExp));
   if (operation.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotLoad));
   if (operation.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotLocalLoad));
   if (operation.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotNetLoad));
   if (operation.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterDestroy_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterInvalidate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterInvalidateCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotExp));
   if (operation.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotLoad));
   if (operation.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotLocalLoad));
   if (operation.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotNetLoad));
   if (operation.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterInvalidate_isNotNetSearch));
}

/** Increment appropriate blackboard counters for an afterUpdate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterUpdateCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotExp));
   if (operation.isLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotLoad));
   if (operation.isLocalLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isLocalLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotLocalLoad));
   if (operation.isNetLoad())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNetLoad));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotNetLoad));
   if (operation.isNetSearch())
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNetSearch));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotNetSearch));
   if (operation.isPutAll())
     counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isPutAll));
   else
     counters.increment(bb.getSharedCounter(CntrName_AfterUpdate_isNotPutAll));
}

/** Increment appropriate blackboard counters for an afterRegionCreate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionCreateCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   // All afterRegionCreate operations should be "isNotDist", "isNotExp" and "isNotRemote"
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionCreate_isNotExp));
}

/** Increment appropriate blackboard counters for an afterRegionLive event
*
*  @param event The event object for this event.
*  @param bb Any blackboard object containing event counters with names
*            specified by the names defined in this class.
*/
public void incrementAfterRegionLiveCounters(Operation operation, Blackboard bb) {
  SharedCounters counters = bb.getSharedCounters();
  // All afterRegionLive operations should be "isNotDist", "isNotExp" and "isNotRemote"
  if (operation.isDistributed()) 
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isDist));
  else
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isNotDist));
  if (operation.isExpiration())
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isExp));
  else
     counters.increment(bb.getSharedCounter(CntrName_AfterRegionLive_isNotExp));
}

/** Increment appropriate blackboard counters for an afterRegionDestroy event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionDestroyCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionDestroy_isNotExp));
}

/** Increment appropriate blackboard counters for an afterRegionInvalidate event
 *
 *  @param event The event object for this event.
 *  @param bb Any blackboard object containing event counters with names
 *            specified by the names defined in this class.
 */
public void incrementAfterRegionInvalidateCounters(Operation operation, Blackboard bb) {
   SharedCounters counters = bb.getSharedCounters();
   if (operation.isDistributed()) 
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isDist));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isNotDist));
   if (operation.isExpiration())
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isExp));
   else
      counters.increment(bb.getSharedCounter(CntrName_AfterRegionInvalidate_isNotExp));
}

}
