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
package management.operations.events.impl;

import static management.util.HydraUtil.logInfo;
import hydra.RemoteTestModule;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import management.operations.OperationEvent;
import management.operations.OperationsBlackboard;
import management.operations.events.RegionOperationEvents;
import management.util.HydraUtil;

/**
 * 
 * This is query-able store of events only for one hydra-client VM
 * Now supports only one index - event-name
 * 
 * You can export events from this Store to blackBoard using method exportToBlackBoard
 * 
 * @author tushark
 * 
 */
public class AbstractEvents {

  protected ReadWriteLock lock = new ReentrantReadWriteLock();
  protected Lock readLock = lock.readLock();
  protected Lock writeLock = lock.writeLock();
  protected Map<String, List<OperationEvent>> eventMap = new HashMap<String, List<OperationEvent>>();
  
  public AbstractEvents(Map<String, List<OperationEvent>> eventMap){
    this.eventMap = eventMap;
  }
  
  public AbstractEvents(){
    
  }

  public void addEvent(OperationEvent event) {
    writeLock.lock();
    try {
      // TODO : Adding relative time-stamp
      String name = event.name;
      List list = null;
      if (eventMap.containsKey(name)) {
        list = eventMap.get(name);

      } else {
        list = new ArrayList();
        eventMap.put(name, list);

        // process more indexes
        processIndexes(event);
      }
      list.add(event);
    } finally {
      writeLock.unlock();
    }
  }
  
  public void addEvents(List<OperationEvent> events) {
    writeLock.lock();
    try {
      // TODO : Adding relative time-stamp
      for(OperationEvent event : events){
        String name = event.name;
        List list = null;
        if (eventMap.containsKey(name)) {
          list = eventMap.get(name);
  
        } else {
          list = new ArrayList();
          eventMap.put(name, list);
  
          // process more indexes
          processIndexes(event);
        }
        list.add(event);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public void processIndexes(OperationEvent event) {

  }  

  public Map<String, List<OperationEvent>> getAllEvents() {
    readLock.lock();
    try {
      return eventMap;
    } finally {
      readLock.unlock();
    }
  }

  public List<OperationEvent> getEvents(String name) {
    readLock.lock();
    try {
      return eventMap.get(name);
    } finally {
      readLock.unlock();
    }
  }

  public void clearEvents() {
    writeLock.lock();
    try {
      eventMap.clear();
    } finally {
      writeLock.unlock();
    }
  }
  
  

  public void exportToBlackBoard(Blackboard bb) {
    /*-
     * In SharedMap - 
     * add Key vmId_operationEvents
     * underThat get all lists for event names
     * and update the lists
     */
    long l1 = System.currentTimeMillis();    
    HydraUtil.logInfo("Upating blackboard with events  " + eventMap.keySet() + " kinds");
    readLock.lock();
    SharedLock lock = bb.getSharedLock();
    try {
      lock.lock();
      Map<String, Map<String,List<OperationEvent>>> globalEventMap = (Map<String, Map<String, List<OperationEvent>>>) bb.getSharedMap().get(
          OperationsBlackboard.EVENTLIST);
      if(globalEventMap==null)
        globalEventMap = new HashMap<String, Map<String,List<OperationEvent>>>(); 
      int eventCount = 0;
      String thisVM = RemoteTestModule.getMyClientName();
      Map<String,List<OperationEvent>> localEvents = globalEventMap.get(thisVM);
      if(localEvents==null)
        localEvents = new HashMap<String,List<OperationEvent>>(); 
      for(String key : eventMap.keySet()){
        List list = localEvents.get(key);
        if(list==null)
          list = new ArrayList<String>();
        List localList = eventMap.get(key);
        eventCount += localList.size();
        HydraUtil.logInfo("Adding events of type " + key + " total events " + localList.size());
        list.addAll(localList);
        localEvents.put(key,list);
      }
      globalEventMap.put(thisVM, localEvents);
      bb.getSharedMap().put(OperationsBlackboard.EVENTLIST, globalEventMap);
      HydraUtil.logInfo("Total events added to blackboard " + eventCount);
      long l2 = System.currentTimeMillis();
      HydraUtil.logInfo("Total time to add events " + (l2-l1) + " ms.");
    } finally {
      lock.unlock();
      readLock.unlock();
    }
  }
  
  
  //TODO : Support importing events of multiple type
  public static Map<String,AbstractEvents> importFromBlackBoard(String type, Blackboard bb) {
    /*-
     * In SharedMap - 
     * add Key vmId_operationEvents
     * underThat get all lists for event names
     * and update the lists
     */
    long l1 = System.currentTimeMillis();    
    HydraUtil.logInfo("Importing blackboard with events  " + type + " kinds");    
    SharedLock lock = bb.getSharedLock();
    try {
      lock.lock();
      Map<String, Map<String,List<OperationEvent>>> globalEventMap = (Map<String, Map<String, List<OperationEvent>>>) bb.getSharedMap().get(
          OperationsBlackboard.EVENTLIST);
      //List<AbstractEvents> list = new ArrayList<AbstractEvents>();
      Map<String,AbstractEvents> eventMap = new HashMap<String,AbstractEvents>();
      int eventCount = 0;
      int clientCount=0;
      for(String clientName : globalEventMap.keySet()){
        Map<String,List<OperationEvent>> localEvents = globalEventMap.get(clientName);
        AbstractEvents events = getEventImplOfType(localEvents,type);
        eventMap.put(clientName, events);
        eventCount += events.getNumberOfEvents();
        HydraUtil.logInfo("Total events imported from blackboard " + events.getNumberOfEvents() + " for client " + clientName);
      }
      HydraUtil.logInfo("Total events imported from blackboard " + eventCount);
      long l2 = System.currentTimeMillis();
      HydraUtil.logInfo("Total time to import events " + (l2-l1) + " ms.");
      return eventMap;
    } finally {
      lock.unlock();      
    }
  }
  
  public static AbstractEvents importFromBlackBoard(String type, String clientName, Blackboard bb) {
    /*-
     * In SharedMap - 
     * add Key vmId_operationEvents
     * underThat get all lists for event names
     * and update the lists
     */
    long l1 = System.currentTimeMillis();    
    HydraUtil.logInfo("Importing blackboard with events  " + type + " kinds");    
    SharedLock lock = bb.getSharedLock();
    try {
      lock.lock();
      Map<String, Map<String,List<OperationEvent>>> globalEventMap = (Map<String, Map<String, List<OperationEvent>>>) bb.getSharedMap().get(
          OperationsBlackboard.EVENTLIST);
      //List<AbstractEvents> list = new ArrayList<AbstractEvents>();
      Map<String,AbstractEvents> eventMap = new HashMap<String,AbstractEvents>();
      int eventCount = 0;
      int clientCount=0;
      Map<String,List<OperationEvent>> localEvents = globalEventMap.get(clientName);
      AbstractEvents events = getEventImplOfType(localEvents,type);
      eventCount += events.getNumberOfEvents();
      HydraUtil.logInfo("Total events imported from blackboard " + events.getNumberOfEvents() + " for client " + clientName);
      HydraUtil.logInfo("Total events imported from blackboard " + eventCount);
      long l2 = System.currentTimeMillis();
      HydraUtil.logInfo("Total time to import events " + (l2-l1) + " ms.");
      return events;
    } finally {
      lock.unlock();      
    }
  }
  
  
  private static AbstractEvents getEventImplOfType(Map<String, List<OperationEvent>> localEvents, String type) {
    Map<String, List<OperationEvent>> eventMap = new HashMap<String, List<OperationEvent>>();
    List<OperationEvent> list = localEvents.get(type);
    List<OperationEvent> newList = new ArrayList<OperationEvent>();
    if(list!=null){
      //Collections.copy(newList, list);
      for(OperationEvent e : list)
        newList.add(e);
    }
      
    eventMap.put(type, newList);
    AbstractEvents events = null;    
    if(type.contains("REGION"))
      events = new RegionEvents(eventMap);
    else if(type.contains("CQ") || type.contains("INDEX"))
      events = new CQAndIndexEvents(eventMap);
    else if(type.contains("ENTRY"))
      events = new EntryEvents(eventMap);
    else if(type.contains("FUNCT"))
      events = new FunctionEvents(eventMap);
    else if(type.contains("QUERY"))
      events = new RegionQueryEvents(eventMap);
    else if(type.contains("TX"))
      events = new TransactionEvents(eventMap);    
    else if(type.contains("DLOCK"))
      events = new DLockEvents(eventMap);    
   return events;
  }

  private int getNumberOfEvents() {
    readLock.lock();
    try {
      int total = 0;
      for(List<OperationEvent> list : eventMap.values())
        total += list.size();
      return total;
    } finally {
      readLock.unlock();
    }
  }

  public void printEvents() {
    logInfo("Printing all events ");
    for(String eventType : eventMap.keySet()){
      logInfo("Printing " + eventType +  " events : ");
      List<OperationEvent> list = eventMap.get(eventType);
      for (OperationEvent e : list) {
        logInfo("E:" + e.name + ", data: " + HydraUtil.ObjectToString(e.data));
      }
    }
    logInfo("Finished with Printing all events ");
  }
  
  public static List<Object> getUniqueEventsForClient(String givenClientName, String kind){
    List<Object> objectList = new ArrayList<Object>();    
    //TODO Export this code to abstraceEvents so that all other can use it.
    AbstractEvents ae = AbstractEvents.importFromBlackBoard(kind,givenClientName, OperationsBlackboard.getBB());
    if(ae!=null){      
      List<OperationEvent> opList = ae.getEvents(kind);
      for(OperationEvent oe:opList){
        String eventName= (String)oe.data;
        if(!objectList.contains(eventName))
          objectList.add(oe.data);
      }
    }
    return objectList;
  }  
  
  public static List<Object> getUniqueEventsAcrossAll(String kind){
    List<Object> objectList = new ArrayList<Object>();    
    //TODO Export this code to abstraceEvents so that all other can use it.
    Map<String,AbstractEvents> emap = AbstractEvents.importFromBlackBoard(kind, OperationsBlackboard.getBB());
    if(emap!=null){
      for(Map.Entry<String,AbstractEvents> e : emap.entrySet()){
        AbstractEvents ae = e.getValue();        
        List<OperationEvent> opList = ae.getEvents(kind);
        for(OperationEvent oe:opList){
          String eventName= (String)oe.data;
          if(!objectList.contains(eventName))
            objectList.add(oe.data);
        }
      }
    }
    return objectList;
  }  
  

  // public void addIndex(String indexName, Object data, ....)/ : Indxing operation
  // public List<OperationEvent> getIndexedEvents(String indexName)
  // public  fromBlackBoard : serialized **ALLL** events back to this JVM to do validations
  // public  toBlackboard : call export to blackboard

}
