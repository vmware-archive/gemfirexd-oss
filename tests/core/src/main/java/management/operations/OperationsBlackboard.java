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
package management.operations;

import static management.util.HydraUtil.logInfo;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import management.operations.events.CQAndIndexOperationEvents;
import management.operations.events.EntryOperationEvents;
import management.operations.events.FunctionOperationEvents;
import management.operations.events.RegionOperationEvents;
import management.operations.events.RegionQueryOperationEvents;
import management.operations.events.TransactionOperationEvents;
import management.util.HydraUtil;

import util.RegionDefinition;

import com.gemstone.gemfire.cache.Region;

public class OperationsBlackboard extends Blackboard implements RegionOperationEvents, EntryOperationEvents, FunctionOperationEvents,
    TransactionOperationEvents, RegionQueryOperationEvents, CQAndIndexOperationEvents {

  static final String BB_NAME = "MGMT_OPS_Blackboard";
  static final String BB_TYPE = "RMI";
  public static final String EVENTLIST = "EVENT_LIST";
  public static final String FUNCTION_MAP = "FUNCTION_MAP";

  public static int SLEEP_FUNCTION_COUNT;
  public static int REGION_FUNCTION_COUNT;
  public static int FIRENFORGET_FUNCTION_COUNT;
  public static int GENERIC_FUNCTION_COUNT;

  public static int REGION_COUNT;
  public static int DLOCK_COUNT;

  private static OperationsBlackboard bbInstance = null;

  public static OperationsBlackboard getBB() {
    if (bbInstance == null) {
      synchronized (OperationsBlackboard.class) {
        if (bbInstance == null)
          bbInstance = new OperationsBlackboard(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  public OperationsBlackboard() {
  }

  public OperationsBlackboard(String name, String type) {
    super(name, type, OperationsBlackboard.class);
  }

  @Override
  public void regionAdded(Region region) {
    if (OperationPrms.recordRegionOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_ADDED;
      event.data = region.getFullPath();
      addEvent(event);
    }
    logInfo("RegionOperations: Finished with creating region : " + region.getFullPath());
  }
  
  @Override
  public void regionAdded(String regionPath) {
    if (OperationPrms.recordRegionOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_ADDED;
      event.data = regionPath;
      addEvent(event);
    }
    logInfo("RegionOperations: Finished with creating region : " + regionPath);
  }

  @Override
  public void regionDestroyed(String name, Set<String> chlildren) {
    if (OperationPrms.recordRegionOperations()) {
      List<OperationEvent> eList = new ArrayList<OperationEvent>();
      
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_DESTROYED;
      event.data = name;
      eList.add(event);
      
      if(chlildren!=null && !chlildren.isEmpty()){
        for(String child  : chlildren){
          OperationEvent childrenevent = new OperationEvent();
          event.name = EVENT_REGION_DESTROYED;
          event.data = child;
          eList.add(childrenevent);
        }
      }
      addEvents(eList);
    }
    logInfo("RegionOperations: Finished with destroying region : " + name);
  }

  @Override
  public void regionInvalidated(String name) {
    if (OperationPrms.recordRegionOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_DESTROYED;
      event.data = name;
      addEvent(event);
    }
    logInfo("RegionOperations: Finished with invalidating region : " + name);
  }

  @Override
  public void regionClosed(String name) {
    if (OperationPrms.recordRegionOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_CLOSED;
      event.data = name;
      addEvent(event);
    }
    logInfo("RegionOperations: Finished with closing region : " + name);
  }

  @Override
  public void regionCleared(String name) {
    if (OperationPrms.recordRegionOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_REGION_CLEARED;
      event.data = name;
      addEvent(event);
    }
    logInfo("RegionOperations: Finished with clearing region : " + name);
  }

  private void addEvent(OperationEvent event) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      // TODO : Adding clientVMID so that events can be uniquely located to its
      // source
      // TODO : Adding relative timestamp
      ArrayList<OperationEvent> list = (ArrayList<OperationEvent>) getSharedMap().get(EVENTLIST);
      if (list == null)
        list = new ArrayList<OperationEvent>();
      list.add(event);
      getSharedMap().put(EVENTLIST, list);
    } finally {
      lock.unlock();
    }
  }
  
  public void addEvents(List<OperationEvent> events) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();      
      ArrayList<OperationEvent> list = (ArrayList<OperationEvent>) getSharedMap().get(EVENTLIST);
      if (list == null)
        list = new ArrayList<OperationEvent>();
      list.addAll(events);
      getSharedMap().put(EVENTLIST, list);      
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void entryAdded(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_ADDED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with adding entry : " + key);
  }

  @Override
  public void entryInvalidated(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_INVALIDATED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with invalidating entry : " + key);
  }

  @Override
  public void entryDestroyed(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_DESTROYED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with destroying entry : " + key);
  }

  @Override
  public void entryUpdated(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_UPDATED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with updating entry : " + key);
  }

  @Override
  public void entryRead(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_READ;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with reading entry : " + key);
  }

  @Override
  public void entryInvalidatedLocal(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_INVALIDATEDLOCAL;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with invalidating locally entry : " + key);
  }

  @Override
  public void entryDestroyedLocal(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_DESTROYEDLOCAL;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with destroying locally entry : " + key);
  }

  @Override
  public void entryPutIfAbsent(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_PUTIFABSENT;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with puting IFAbsent entry : " + key);
  }

  @Override
  public void entryRemoved(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_REMOVED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with removing entry : " + key);
  }

  @Override
  public void entryReplaced(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_REPLACED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with replacing entry : " + key);
  }

  @Override
  public void transactionStarted() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_STARTED;
      addEvent(event);
    }
    logInfo("TransactionOperations: Started");
  }

  @Override
  public void transactionCommited() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_COMMITED;
      addEvent(event);
    }
    logInfo("TransactionOperations: Commited Successfully");
  }

  @Override
  public void transactionRolledback() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_ROLLEDBACK;
      addEvent(event);
    }
    logInfo("TransactionOperations: Rolledback Successfully");
  }

  @Override
  public void queryRegion(String regionName, int resultSize) {
    if (OperationPrms.recordQueryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_QUERY;
      event.data = new Object[] { regionName, resultSize };
      addEvent(event);
    }
    logInfo("QueryOperations: Region Query Completed");
  }

  @Override
  public void functionRegistered(String id) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_REGISTER;
      event.data = id;
      addEvent(event);
    }
    logInfo("FunctionOperations: Function registration completed");
  }

  @Override
  public void functionUnregistered(String id) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_UNREGISTER;
      event.data = id;
      addEvent(event);
    }
    logInfo("FunctionOperations: Function un-registration completed");
  }

  @Override
  public void functionExecuted(String id, Object result) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_EXEC;
      event.data = new Object[] { id, result };
      addEvent(event);
    }
    logInfo("FunctionOperations: Function execution completed");
  }

  @Override
  public void cqCreated(String name, String query, String listeners) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_CQ_CREATED;
      event.data = new Object[] { name, query };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Cq create complete");
  }

  @Override
  public void cqStopped(String name, String query) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_CQ_STOP;
      event.data = new Object[] { name, query };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Cq stop complete");
  }

  @Override
  public void indexCreated(String name, String expression, String fromClause) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_INDEX_CREATED;
      event.data = new Object[] { name, expression, fromClause };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Index create complete");
  }

  @Override
  public void indexRemoved(String name) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_INDEX_REMOVED;
      event.data = new Object[] { name };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Index create complete");
  }

  public void printEvents() {
    logInfo("Printing all events ");
    ArrayList<OperationEvent> list = (ArrayList<OperationEvent>) getSharedMap().get(EVENTLIST);
    for (OperationEvent e : list) {
      logInfo("E:" + e.name + ", data: " + HydraUtil.ObjectToString(e.data));
    }
    logInfo("Finished with Printing all events ");
  }

  public void addRegionDefinitions(Map<String, RegionDefinition> map) {
    OperationsBlackboard.getBB().getSharedMap().put("REGION_DEFS", map);
  }

  public Map<String, RegionDefinition> getRegionDefinitions() {
    return (Map<String, RegionDefinition>) OperationsBlackboard.getBB().getSharedMap().get("REGION_DEFS");
  }

  public String getNextSleepFunctionId() {
    return "SLEEP_" + getBB().getSharedCounters().incrementAndRead(SLEEP_FUNCTION_COUNT);
  }

  public int getSleepFunctionCounter() {
    return (int) getSharedCounters().read(SLEEP_FUNCTION_COUNT);
  }
  
  public String getNextFunctionId(String name) {
    return name + "_" + getBB().getSharedCounters().incrementAndRead(GENERIC_FUNCTION_COUNT);
  }

  public int getFunctionCounter() {
    return (int) getSharedCounters().read(GENERIC_FUNCTION_COUNT);
  }

  public int getNextRegionCounter() {
    return (int) getBB().getSharedCounters().incrementAndRead(REGION_COUNT);
  }

  public int getRegionCounter() {
    return (int) getSharedCounters().read(REGION_COUNT);
  }
  
  public int getNextDLockCounter() {
    return (int) getBB().getSharedCounters().incrementAndRead(DLOCK_COUNT);
  }

  public int getDLockCounter() {
    return (int) getSharedCounters().read(DLOCK_COUNT);
  }

}
