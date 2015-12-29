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
package management.operations.ops;

import static management.util.HydraUtil.logInfo;
import hydra.BasePrms;
import hydra.DistributedConnectionMgr;
import hydra.GsRandom;
import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.ProcessMgr;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import management.operations.OperationPrms;
import management.operations.OperationsBlackboard;
import management.operations.events.RegionOperationEvents;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.org.jgroups.protocols.PARTITIONER.PartitionerHeader;

/**
 * Performs following kind of Region Operations
 * 
 * addRegion destroyRegion invalidateRegion invalidateLocalRegion closeRegion
 * clearRegion reCreateRegion
 * 
 * Contains startTask for reading all regionSpecs and storing them in BlackBoard
 * 
 * Currently *Does not* contains operation validation.
 * 
 * TODO : Logging all actions in HydraLog and Blackboard : Done Surround or
 * region operations using try-catch block and assert unexpected exceptions
 * :Done Support capturing event to random recorder instead of Blackboard so
 * that it can be validated right-away using Expectations : Done Protection
 * against concurrent operations!!!!! : Done Add thorough concurrent test for
 * region operations with Function Specified for creating large number of
 * regions : Done
 * 
 * Generate operation depending on the underlying region so as to avoid
 * unexpected operations for eg. clear not supported by partitionRegion
 * 
 * @author tushark
 */
public class RegionOperations {

  /**
   * Adds a region using regionDefinition read from Blackboard If region Exists
   * then next option is tried.
   */

  public static final int ADD_REGION = 21;
  public static final int DESTROY_REGION = 22;
  public static final int INVALIDATE_REGION = 23;
  public static final int INVALDIATELOCAL_REGION = 24;
  public static final int CLEAR_REGION = 25;
  public static final int RE_CREATE_REGION = 26;

  public static final AtomicInteger regionTotalOp = new AtomicInteger();

  private static List<String> regionNames = null;
  private static GsRandom randGen = TestConfig.tab().getRandGen();

  private RegionOperationEvents operationRecorder = null;
  private Cache cache = null;
  private Set<Region> availableRegions = new HashSet<Region>();

  public RegionOperations(Cache cache) {
    this.operationRecorder = OperationsBlackboard.getBB();
    this.cache = cache;

    populateRegionList();
  }
  
  

  public synchronized void setOperationRecorder(RegionOperationEvents operationRecorder) {
    this.operationRecorder = operationRecorder;
  }



  public synchronized RegionOperationEvents getOperationRecorder() {
    return operationRecorder;
  }



  private static synchronized void populateRegionList() {
    if (regionNames == null) {
      regionNames = new ArrayList<String>();
      HydraVector regionList = TestConfig.tab().vecAt(OperationPrms.regionList);
      Iterator<String> regionIterator = regionList.iterator();
      while (regionIterator.hasNext()) {
        regionNames.add(regionIterator.next());
      }
    }
  }

  public RegionOperations(Cache cache, RegionOperationEvents operationRecorder) {
    this.operationRecorder = operationRecorder;
    this.cache = cache;
    populateRegionList();
  }

  public static void HydraStartTask_ReadRegionSpecs() {

    /*
     * populateRegionList();
     * 
     * Map<String,RegionDefinition> map = new
     * HashMap<String,RegionDefinition>(); for (int i = 0; i <
     * regionNames.size(); i++) { RegionDefinition regDef =
     * RegionDefinition.createRegionDefinition( RegionDefPrms.regionSpecs,
     * regionNames.get(i)); map.put(regionNames.get(i), regDef); }
     * logInfo("RegionOperations: RegionName List : " + regionNames);
     * logInfo("RegionOperations: Creating regions Map : " + map);
     * OperationsBlackboard.getBB().addRegionDefinitions(map);
     */
  }

  public Set<Region> createAllRegions() {

    logInfo("RegionOperations: Creating regions List : " + regionNames);
    for (String s : regionNames) {
      synchronized (availableRegions) {
        availableRegions.add(createRegion(getUniqueRegionName(s), s));
      }
    }
    logInfo("RegionOperations: Finished with creating regions List : " + regionNames);
    return availableRegions;
  }

  private String getUniqueRegionName(String s) {
    return s + "_" + OperationsBlackboard.getBB().getNextRegionCounter();
  }

  
  
  public Set<Region> getAvailableRegions(){
    synchronized (availableRegions) {
      return Collections.unmodifiableSet(availableRegions);
    }
  }

  public Region getRegion() {
    synchronized (availableRegions) {
      StringBuilder sb = new StringBuilder();
      for (Region r : availableRegions)
        sb.append(r.getName() + ", ");
      logInfo("List of Regions " + sb);
      if (availableRegions.size() > 0) {
        int index = randGen.nextInt(availableRegions.size() - 1);
        int i = 0;
        // Random element from set since availableRegions changed to set from
        // List
        Region r1 = null;
        for (Region r : availableRegions) {
          if (i == index) {
            return r;
          } else
            i++;
        }
        throw new TestException("No random region why? index=" + index + " i=" + i);
      } else {
        logInfo("Error no available regions !!!");
        return null;
      }
    }
  }
  
  
  public String createRegion(String regionTemplateName) {
    synchronized (availableRegions) {
      RegionDescription regionDescription = RegionHelper.getRegionDescription(regionTemplateName);      
      String regionName = getUniqueRegionName(regionDescription.getRegionName());
      Region r = createRegion((regionName), regionTemplateName);
      availableRegions.add(r);
      return r.getFullPath();
    }
  }
  
  public String createRegion(){
    HydraVector regionTemplateList = TestConfig.tab().vecAt(OperationPrms.regionList);
    String s = (String) regionTemplateList.get(TestConfig.tab().getRandGen().nextInt(regionTemplateList.size() - 1));
    return createRegion(s);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public String createSubRegion(String name){
    synchronized (availableRegions) {      
        Region parent = RegionHelper.getRegion(name);
        String subregionName = "child" + OperationsBlackboard.getBB().getNextRegionCounter();
        logInfo("RegionOperations: Creating subregion named : " + subregionName + " with parent " + parent.getFullPath());
        RegionAttributes attr= parent.getAttributes();
        Region child = parent.createSubregion(subregionName, attr);
        logInfo("RegionOperations: Created subregion named " + child.getFullPath());
        availableRegions.add(child);
        operationRecorder.regionAdded(child);
        return child.getFullPath();      
    }   
  }
  
  public Region addRegion(String templateName){
    boolean flag = false;
    HydraVector regionTemplateList = TestConfig.tab().vecAt(OperationPrms.regionList);
    for(int i=0;i<regionTemplateList.size();i++)
      if(regionTemplateList.get(i).equals(templateName))
        flag = true;
    if(!flag)
      throw new TestException("Template named " + templateName + " not present in regionTemplateList : " + regionTemplateList);
    else
      return createRegion(getUniqueRegionName(templateName), templateName);
  }
  
  public void invalidateRegion(Region region) {
    _invalidateRegion(region);
  }
  
  public void closeRegion(Region region) {
    _closeRegion(region);
  }
  
  public void clearedRegion(Region region){
    _clearedRegion(region);
  }
  
  public void destroyRegion(Region region){
    _destroyRegion(region);
  }

  @SuppressWarnings("rawtypes")
  public Region createRegion(String name, String regionTemplateName) {
    // static synchronize
    synchronized (this.getClass()) {
      Region region = RegionHelper.getRegion(name);
      if (region == null) {
        logInfo("RegionOperations: Creating region named : " + name + " with template " + regionTemplateName);
        String regionConfig = regionTemplateName;
        AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
        logInfo("RegionOperations: RegionAttrs Factory " + aFactory.toString());
        
        //Method 1 : Directly create region
        /*Region rootRegion = null;
        try {
          RegionAttributes attr = aFactory.create();
          logInfo("RegionOperations: RegionAttrs : " + attr);
          rootRegion = cache.createRegion(name, attr);
        } catch (RegionExistsException e) {
          throw new HydraInternalException("Should not happen", e);
        }
        */
        //Method 3 : Use RegionHelper 
        Region rootRegion = RegionHelper.createRegion(name, aFactory);        
        
        //Method 2 : NOT WORKING : Region rootRegion = RegionHelper.createRegion(name,regionConfig);
        
        
        logInfo("RegionOperations: Created root region " + name);
        // edge clients register interest in ALL_KEYS
        if (rootRegion.getAttributes().getPoolName() != null) {
          rootRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
          logInfo("RegionOperations: registered interest in ALL_KEYS for " + name);
        }
        region = rootRegion;
        availableRegions.add(rootRegion);
        operationRecorder.regionAdded(rootRegion);
      }
      return region;
    }
  }
  
  /*
  public static AttributesFactory getAttributesFactory(String regionConfig) {
    // look up the region configuration
    RegionDescription rd = getRegionDescription(regionConfig);

    // create the attributes factory
    AttributesFactory factory = new AttributesFactory();

    // configure the attributes factory
    HydraUtil.logInfo("RegionOperations: Configuring attributes factory for region config: " + regionConfig);
    rd.configure(rd.getRegionName(), factory, true);
    HydraUtil.logInfo("Configured attributes factory: " + factory);

    // return the result
    return factory;
  }
  
  public static RegionDescription getRegionDescription(String regionConfig) {
    if (regionConfig == null) {
      throw new IllegalArgumentException("regionConfig cannot be null");
    }
    HydraUtil.logInfo("RegionOperations: Looking up region config: " + regionConfig);
    RegionDescription rd = TestConfig.getInstance()
                                     .getRegionDescription(regionConfig);
    if (rd == null) {
      String s = regionConfig + " not found in "
               + BasePrms.nameForKey(RegionPrms.names);
      throw new HydraRuntimeException(s);
    }
    HydraUtil.logInfo("Looked up region config:\n" + rd);
    return rd;
  }*/

  private void invalidateRegion() {
    String name = null;
    Region region = getRegion();
    _invalidateRegion(region);
  }
  
  private void _invalidateRegion(Region region) {
    if (region != null) {
      String name = region.getName();
      logInfo("RegionOperations: Invalidating region named : " + name);
      String callback = callback("Invalidating region" + name + " in ");
      try {
        region.invalidateRegion(callback);
        operationRecorder.regionInvalidated(name);
      } catch (Exception e) {
        handleException("RegionOperationError : invalidateRegion", e, region);
      }
    } else {
      logInfo("No region Found continuing test list : " + availableRegions);
    }
  }  

  private void closeRegion() {
    String name = null;
    Region region = getRegion();
    _closeRegion(region);    
  }
  
  private void _closeRegion(Region region) {
    if (region != null) {
      String name = region.getName();
      String callback = callback("RegionOperations: Closing region" + name + " in ");
      logInfo(callback);
      try {
        region.close();
        removeAvailableRegion(region);
        operationRecorder.regionClosed(name);
      } catch (Exception e) {
        handleException("RegionOperationError : closeRegion", e, region);
      }
    } else {
      logInfo("No region Found continuing test list : " + availableRegions);
    }
  }  

  private void clearedRegion() {
    String name = null;
    Region region = getRegion();
    _clearedRegion(region);
  }
  
  private void _clearedRegion(Region region) {
    if (region != null) {
      String name = region.getName();
      String callback = callback("RegionOperations: Clearing region" + name + " in ");
      logInfo(callback);
      try {
        region.clear();
        operationRecorder.regionCleared(name);
      } catch (Exception e) {
        handleException("RegionOperationError : clearedRegion", e, region);
      }
    } else {
      logInfo("No region Found continuing test list : " + availableRegions);
    }
  }

  private void removeAvailableRegion(Region region) {
    synchronized (availableRegions) {
      Region toDelete = null;
      for (Region r : availableRegions) {
        if (r.getFullPath().equals(region.getFullPath()))
          toDelete = r;
      }
      if (toDelete != null)
        availableRegions.remove(toDelete);
      else {
        if (!HydraUtil.isConcurrentTest())
          throw new TestException("Can't find region in the availableRegions list ");
      }
    }
  }  

  private void destroyRegion() {
    String name = null;
    Region region = getRegion();
    _destroyRegion(region);
  }
  
  private void _destroyRegion(Region region){
    if (region != null) {
      String name = region.getName();
      String path = region.getFullPath();     
      String callback = callback("RegionOperations: Destroying region" + name + " in ");
      logInfo(callback);
      // TODO Optional dumping of region snapshot
      try {
        Region r = CacheFactory.getAnyInstance().getRegion(name);
        Set<Region> children = r.subregions(true);
        HashSet<String> childrennames = new HashSet<String>();
        if (children != null && children.size() > 0) {
          for (Region child : children) {
            childrennames.add(child.getFullPath());
          }
        }
        region.destroyRegion();
        removeAvailableRegion(region);
        operationRecorder.regionDestroyed(path, childrennames);
      } catch (Exception e) {
        handleException("RegionOperationError : destroyRegion", e, region);
      }
    } else {
      logInfo("No region Found continuing test list : " + availableRegions);
    }
  }

  private void handleException(String string, Exception e, Region r) {
    if ((HydraUtil.isConcurrentTest() && e instanceof RegionDestroyedException)) {
      logInfo(string + " RegionDestroy expected during concurrent test. Contnuing test ");
    }
    if (e instanceof RegionDestroyedException && !r.getAttributes().getScope().equals(Scope.LOCAL)) {
      // region destroy is propogated to this jvm
      logInfo(string + " Region is already destroyed probably due to remote destroy, continuing with test");
      removeAvailableRegion(r);
    } else {
      throw new TestException(string, e);
    }
  }

  public void doRegionOperations() {
    // 1. get operations maintaining the size
    String operation = TestConfig.tab().stringAt(OperationPrms.regionOperations);
    int whichOp;
    if ("add".equals(operation))
      whichOp = ADD_REGION;
    else if ("destroy".equals(operation))
      whichOp = DESTROY_REGION;
    else if ("invalidate".equals(operation))
      whichOp = INVALIDATE_REGION;
    /*
     * else if("invalidateLocal".equals(operation)) whichOp =
     * INVALDIATELOCAL_REGION;
     */
    else if ("clear".equals(operation))
      whichOp = CLEAR_REGION;
    else if ("reCreate".equals(operation))
      whichOp = RE_CREATE_REGION;
    else
      throw new TestException("Unknow region operations " + operation);

    synchronized (availableRegions) {
      int size = availableRegions.size();
      if (size < 1) {
        logInfo("RegionOperations: Number of Regions " + availableRegions.size() + " so adding a new region.");
        whichOp = ADD_REGION;
      }
    }

    switch (whichOp) {
    case ADD_REGION:
      addRegion();
      break;
    case DESTROY_REGION:
      destroyRegion();
      break;
    case INVALIDATE_REGION:
      invalidateRegion();
      break;
    case CLEAR_REGION:
      clearedRegion();
      break;
    case RE_CREATE_REGION:
      reCreateRegion();
      break;
    default:
      throw new TestException("Unknow region operations " + whichOp);
    }
    // 2.
  }

  private void reCreateRegion() {
    throw new UnsupportedOperationException();
  }
  
  private void addRegion() {
    HydraVector regionTemplateList = TestConfig.tab().vecAt(OperationPrms.regionList);
    String s = (String) regionTemplateList.get(TestConfig.tab().getRandGen().nextInt(regionTemplateList.size() - 1));
    createRegion(s);

    /*
     * Map<String,RegionDefinition> map
     * =OperationsBlackboard.getBB().getRegionDefinitions(); //1. find regions
     * not yet created or not available int listSize=0; Region[] regionArray =
     * null;
     * 
     * synchronized (availableRegions) { listSize = availableRegions.size();
     * Object[] array = availableRegions.toArray(); regionArray = new
     * Region[array.length]; for(int i=0;i<array.length;i++){ regionArray[i] =
     * (Region)array[i]; }
     * 
     * if(listSize == map.size()){ //all regions are already created logInfo(
     * "No regionDefinition avaialable so returning without adding new regions listSize="
     * + listSize + "definitions="+ map.size()); return; }else if(listSize <
     * map.size()){ Set<String> definedList = map.keySet(); List<String>
     * listOfDeletedRegions = new ArrayList<String>(); for(String s :
     * definedList){ boolean flag = true; for(Region r: regionArray){
     * if(r.getName().equals(s)){ flag = true; break; } } if(flag)
     * listOfDeletedRegions.add(s); }
     * logInfo("List of regions available for creation " +
     * listOfDeletedRegions); if(listOfDeletedRegions.size()==0){ logInfo(
     * "No regionDefinition avaialable so returning without adding new reigions"
     * ); return; }else{ int index =
     * randGen.nextInt(listOfDeletedRegions.size()-1); String s =
     * listOfDeletedRegions.get(index); try{ Region r=
     * createRegion(getUniqueRegionName(s),s); //synchronized (availableRegions)
     * { availableRegions.add(r); //} }catch(Exception e){ throw new
     * TestException("RegionOperationError : addRegion",e); } } }else{ throw new
     * TestException("Cannot have regions created="+ listSize +
     * " greater than defined value " + map.size()); } }//Made global lock see
     * if can be removed!!!
     */
  }

  private String callback(String createCallbackPrefix) {
    String memberId = null;
    if (DistributedConnectionMgr.getConnection() != null)
      memberId = DistributedConnectionMgr.getConnection().getDistributedMember().toString();
    String callback = createCallbackPrefix + " " + ProcessMgr.getProcessId() + " memberId=" + memberId;
    return callback;
  }

}
