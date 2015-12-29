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
package newWan.serial.filters;

import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.EdgeHelper;
import hydra.GemFireDescription;
import hydra.GsRandom;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import newWan.WANBlackboard;
import util.MethodCoordinator;
import util.TestException;
import util.ValueHolder;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;

public class WanFilterTest {
  
  private static WanFilterTest instance;
  private static LogWriter logger = Log.getLogWriter();
  private static GsRandom rand = new GsRandom();
 
  protected static volatile MethodCoordinator eventFilterValiatorCoordinator = null;
  protected static volatile MethodCoordinator tranportFilterDataValidatorCoordinator = null;
  
  public static final String FILTER_KEY_PRIFIX = "FilterKey_" ;
  
  protected Map<String, Integer> distributedSystems = new HashMap();  
  
  public static synchronized WanFilterTest getInstance(){
    if (instance == null){
      instance = new WanFilterTest();
      instance.setDistributedSystems();
      eventFilterValiatorCoordinator = new MethodCoordinator(WanFilterTest.class.getName(), "validateDoOpsForEventFilter");
      tranportFilterDataValidatorCoordinator = new MethodCoordinator(WanFilterTest.class.getName(), "validateTranportFilterData");
    }
    return instance;
  }
  
  public static void doOpsForEventFilterTask(){
    getInstance().doOpsForEventFilter();
  }
  
  public static void validateDoOpsForEventFilterTask(){
    WanFilterTest test = getInstance();
    test.eventFilterValiatorCoordinator.executeOnce(test, new Object[0]);
  }

  public static void validateTransportFilterDataTask(){
    WanFilterTest test = getInstance();
    test.tranportFilterDataValidatorCoordinator.executeOnce(test, new Object[0]);
  }
  public static void removeEventFilterKeysTask(){
    getInstance().removeEventFilterKeys();
  }

  public void doOpsForEventFilter() {
    int keysPerTask = WanFilterTestPrms.getNumKeysPerTask();
    for (int i = 0; i < keysPerTask; i++) {
      int filterKeyCounter = getNextDsKeyCounter();
      String key = new String(FILTER_KEY_PRIFIX + filterKeyCounter);
      Object val = new Long(filterKeyCounter);

      Set rootRegions = CacheHelper.getCache().rootRegions();
      int x = rand.nextInt(100);
      if (x < 50) {
        Iterator rit = rootRegions.iterator();
        while (rit.hasNext()) {
          Region aRegion = (Region)rit.next();
          logger.info("doOpsForEventFilter: doing put on "
              + aRegion.getFullPath() + " with key " + key + " => " + val);
          Object old = aRegion.put(key, val);
          logger.info("doOpsForEventFilter: done put on "
              + aRegion.getFullPath() + " with key " + key + " => " + val
              + ", old value=" + old);
        }
      }
      else {
        Iterator rit = rootRegions.iterator();
        while (rit.hasNext()) {
          Region aRegion = (Region)rit.next();
          Map map = new HashMap();
          map.put(key, val);
          logger.info("doOpsForEventFilter: doing putAll on "
              + aRegion.getFullPath() + " with map " + map);
          aRegion.putAll(map);
          logger.info("doOpsForEventFilter: done putAll on "
              + aRegion.getFullPath() + " with map " + map);
        }
      }
    }
    
    // sleep in proportion to iterations we performed. 
    // Also this will increase the required task time.
    MasterController.sleepForMs(keysPerTask * 1000);
  }

  public void validateDoOpsForEventFilter() {      
    Set rootRegions = CacheHelper.getCache().rootRegions();
    Iterator itr = rootRegions.iterator();
    while (itr.hasNext()) {      
      Region r = (Region)itr.next();
      logger.info("validateDoOpsForEventFilter: validating region " + r.getFullPath());
                  
      StringBuilder str = new StringBuilder();
      Set extraFilterKeys = new TreeSet(r.keySet());
      int dsKeyCounter = getDsKeyCounter();
      int totalDs = distributedSystems.size();
      int dsId = DistributedSystemHelper.getDistributedSystemId().intValue();
      if(dsId == -1){// this is client vm
        dsId = EdgeHelper.toWanSite(RemoteTestModule.getMyClientName());
      }
      while (dsKeyCounter >= dsId) {
        String key = new String(FILTER_KEY_PRIFIX + dsKeyCounter);
        Object expVal = new Long(dsKeyCounter);  
        extraFilterKeys.remove(key);
        if(r.containsKey(key)){
          Object val = r.get(key);
          if(val == null || !val.equals(expVal)){
            str.append("  for key " + key + ", expected " + expVal + ", found " + val + "\n");
          }
        }else{
          str.append("  for key " + key + ", expected " + expVal + ", but key does not exists on region \n");          
        }
        dsKeyCounter = dsKeyCounter - totalDs;
      }
      
      //remove all non filter keys
      Iterator i = extraFilterKeys.iterator();
      Set temp = new HashSet();
      while(i.hasNext()){
        String k = (String)i.next();
        if(!k.contains(FILTER_KEY_PRIFIX)){
          temp.add(k);          
        }
      }
      extraFilterKeys.removeAll(temp);
      
      if(extraFilterKeys.size() > 0){
        str.append("Found " + extraFilterKeys.size() + " unexpected filter keys in region " + r.getFullPath() + " : " + extraFilterKeys + "\n");
      }
      
      if(str.length() > 0){
        throw new TestException("Error found in validating region " + r.getFullPath() + "\n" + str.toString());
      }
    }
  }

  public void validateTranportFilterData(){
    Set rootRegions = CacheHelper.getCache().rootRegions();
    Iterator itr = rootRegions.iterator();
    while (itr.hasNext()) {      
      Region r = (Region)itr.next();
      logger.info("validateTranportFilterData: validating transport filter data on region " + r.getFullPath());
      
      StringBuilder str = new StringBuilder();
      Iterator<String> keysItr = r.keySet().iterator();
      while(keysItr.hasNext()){
        String key = keysItr.next();
        Object value = r.get(key);
        if(value == null){
          str.append(" for key " + key + ", expected value but found null");
        }else if(value instanceof ValueHolder){
          ValueHolder v = (ValueHolder)value;
          Object extraObj = v.getExtraObject();
          if(extraObj instanceof MyExtraObject){
            str.append("Tranport filter are not applied correctly at receivers.  for " + key + " with value " + v 
                + ". Filters not applied are:" + ((MyExtraObject)extraObj).getTranFilterList() + "\n");
          }          
        }else{
          str.append(" Possible test issue, for key " + key + ", expected value to be instance of ValueHolder, but found instance of " + value.getClass().getName() + "\n");
        }
      }
      
      if(str.length() > 0){
        throw new TestException("In validating transport filters on region " + r.getFullPath() + " " + str.toString());
      }
      logger.info("validateTranportFilterData: done vaidating transport filter data on region " + r.getFullPath());
    }
  }
    
  public void removeEventFilterKeys(){
    Set rootRegions = CacheHelper.getCache().rootRegions();
    Iterator itr = rootRegions.iterator();
    while (itr.hasNext()) {      
      Region r = (Region)itr.next();
      logger.info("removeEventFilterKeys: removing all filter keys from region " + r.getFullPath());
      Iterator keysItr = r.keySet().iterator();
      while (keysItr.hasNext()){
        String k = (String)keysItr.next();
        if(k.contains(FILTER_KEY_PRIFIX)){
          logger.info("removeEventFilterKeys: removing filter key " + k + " from region " + r.getFullPath());
          r.remove(k);          
        }
      }      
    }    
  }
  /**
   * Return next unique key counter for current ds.
   * 
   * @return unique key counter for current ds
   */
  private int getNextDsKeyCounter() {
    WANBlackboard bb = WANBlackboard.getInstance();
    Integer dsid = DistributedSystemHelper.getDistributedSystemId();
    if(dsid.equals(new Integer(-1))){// this is client vm, get dsid of server
      dsid = new Integer(EdgeHelper.toWanSite(RemoteTestModule.getMyClientName()));
    }
    
    bb.getSharedLock().lock();
    String key = "DsKeyNum_" + dsid;
    Integer uniqueIndex = (Integer)bb.getSharedMap().get(key); // get unique key for current Ds    
    if (uniqueIndex == null) { // first time set unique key to current ds      
       uniqueIndex = dsid;      
    }
    else {      
      uniqueIndex = new Integer(uniqueIndex.intValue() + distributedSystems.size());      
    }     
    bb.getSharedMap().put(key, uniqueIndex);
    bb.getSharedLock().unlock();    
    return uniqueIndex.intValue();
  }
  
  /**
   * Return current unique key counter for current ds.
   * 
   * @return unique key counter for current ds
   */
  private int getDsKeyCounter() {
    WANBlackboard bb = WANBlackboard.getInstance();
    Integer dsid = DistributedSystemHelper.getDistributedSystemId();
    if(dsid.equals(new Integer(-1))){// this is client vm, get dsid of server
      dsid = new Integer(EdgeHelper.toWanSite(RemoteTestModule.getMyClientName()));
    }    
    String key = "DsKeyNum_" + dsid;
    Integer uniqueIndex = (Integer)bb.getSharedMap().get(key); // get unique key for current Ds    
    return (uniqueIndex == null) ? -1 : uniqueIndex.intValue();
  }
  
  private void setDistributedSystems(){
    Map<String, GemFireDescription> gdm = TestConfig.getInstance().getGemFireDescriptions();    
    Iterator itr = gdm.keySet().iterator();
    while (itr.hasNext()){
      GemFireDescription gfd = gdm.get(itr.next());
      if(!gfd.getDistributedSystem().contains("loner") && !distributedSystems.containsKey(gfd.getDistributedSystem())){
        distributedSystems.put(gfd.getDistributedSystem(), gfd.getDistributedSystemId());
      }      
    }    
  }
  
}

class MyExtraObject {
  private Object extraObject;
  private List<String> tranFilterList = new ArrayList<String>();
  
  public void setExtraObject(Object extraObject) {
    this.extraObject = extraObject;
  }
  
  public Object getExtraObject() {
    return extraObject;
  }
  
  public void addTranFilter(String filter) {
    this.tranFilterList.add(filter);
  }
  
  public boolean removeTranFilter(String filter) {
    return this.tranFilterList.remove(filter);
  }
  
  public List<String> getTranFilterList() {
    return tranFilterList;
  }
}