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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import management.operations.OperationEvent;
import management.operations.OperationPrms;
import management.operations.OperationsBlackboard;
import management.operations.events.RegionOperationEvents;
import management.util.HydraUtil;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

public class RegionEvents extends AbstractEvents implements RegionOperationEvents {

  public RegionEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);   
  }

  public RegionEvents() {    
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

  
  /**
   * This method returns all regions created and updated in blackboard so far. 
   * It does not LOOK up to REGION_DESTROYED events
   * 
   */
  public static List<String> getAllRegions(){
    List<String> regionList = new ArrayList<String>();    
    //TODO Export this code to abstraceEvents so that all other can use it.
    Map<String,AbstractEvents> emap = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_ADDED, OperationsBlackboard.getBB());
    if(emap!=null){
      for(Map.Entry<String,AbstractEvents> e : emap.entrySet()){
        String clientName = e.getKey();
        AbstractEvents ae = e.getValue();
        RegionEvents regionEvents = (RegionEvents)ae;
        List<OperationEvent> opList = regionEvents.getEvents(RegionOperationEvents.EVENT_REGION_ADDED);
        for(OperationEvent oe:opList){
          String regionName= (String)oe.data;
          if(!regionList.contains(regionName))
            regionList.add((String) oe.data);
        }
      }
    }
    return regionList;
  }
  
  
  /**
   * This method returns all regions created and updated (destroyed regions are subtracted from the set) in blackboard so far 
   */
  
  public static List<String> getCurrentRegions(){
    List<String> regionList = new ArrayList<String>();    
    //TODO Export this code to abstraceEvents so that all other can use it.
    Map<String,AbstractEvents> emap = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_ADDED, OperationsBlackboard.getBB());
    Map<String,AbstractEvents> emap2 = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_DESTROYED, OperationsBlackboard.getBB());
    if(emap!=null){
      for(Map.Entry<String,AbstractEvents> e : emap.entrySet()){
        String clientName = e.getKey();
        AbstractEvents ae = e.getValue();
        RegionEvents regionEvents = (RegionEvents)ae;
        List<OperationEvent> opList = regionEvents.getEvents(RegionOperationEvents.EVENT_REGION_ADDED);
        for(OperationEvent oe:opList){
          String regionName= (String)oe.data;
          if(!regionList.contains(regionName))
            regionList.add((String) oe.data);
        }
      }
    }
    
    HydraUtil.logInfo("Region Create List : " + regionList);
    
    if(emap2!=null){
      for(Map.Entry<String,AbstractEvents> e : emap2.entrySet()){
        String clientName = e.getKey();
        AbstractEvents ae = e.getValue();
        RegionEvents regionEvents = (RegionEvents)ae;
        List<OperationEvent> opList = regionEvents.getEvents(RegionOperationEvents.EVENT_REGION_DESTROYED);
        for(OperationEvent oe:opList){
          String regionName= (String)oe.data;
          if(regionList.contains(regionName))
            regionList.remove((String) oe.data);
        }
      }
    }
    
    HydraUtil.logInfo("Region List(After sbustracting destroyed regiond) : " + regionList);
    
    return regionList;
  }
  
  /**
   * This method returns all regions created and updated in blackboard so far for given clientName 
   * It does not LOOK up to REGION_DESTROYED events
   * 
   */
  public static List<String> getAllRegions(String givenClientName){
    List<String> regionList = new ArrayList<String>();    
    //TODO Export this code to abstraceEvents so that all other can use it.
    AbstractEvents ae = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_ADDED,givenClientName, OperationsBlackboard.getBB());
    if(ae!=null){
      RegionEvents regionEvents = (RegionEvents)ae;
      List<OperationEvent> opList = regionEvents.getEvents(RegionOperationEvents.EVENT_REGION_ADDED);
      for(OperationEvent oe:opList){
        String regionName= (String)oe.data;
        if(!regionList.contains(regionName))
          regionList.add((String) oe.data);
      }
    }
    return regionList;
  }
  
  public List<String> getLocalRegions(){
    List<String> regionList = new ArrayList<String>();    
    AbstractEvents ae = this;
    if(ae!=null){
      RegionEvents regionEvents = (RegionEvents)ae;
      List<OperationEvent> opList = regionEvents.getEvents(RegionOperationEvents.EVENT_REGION_ADDED);
      for(OperationEvent oe:opList){
        String regionName= (String)oe.data;
        if(!regionList.contains(regionName))
          regionList.add((String) oe.data);
      }
    }
    return regionList;
  }

}
