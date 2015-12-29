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
package management.operations.ops.jmx;

import static management.util.HydraUtil.logInfo;
import hydra.RegionHelper;
import hydra.RemoteTestModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import management.jmx.JMXBlackboard;
import management.jmx.JMXPrms;
import management.operations.OperationEvent;
import management.operations.OperationsBlackboard;
import management.operations.events.RegionOperationEvents;
import management.operations.events.impl.AbstractEvents;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.JMXOperations;
import management.operations.ops.RegionOperations;
import management.test.federation.FederationBlackboard;
import management.test.jmx.JMXTest;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.FixedPartitionAttributesData;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;

/**
 * Test Class for RegionTestMBean Attributes, operations and notifications
 * @author tushark
 *
 */

@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class RegionTestMBean extends AbstractTestMBean<RegionTestMBean> {
  
  static {
    prefix = "RegionTestMBean : ";
  }

  private static boolean validationsComplete = false;
  
  public RegionTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, RegionTestMBean.class, ton, tests);
  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[RegionMBean];
  }
  
  
  public void createRegion(JMXOperations ops, ObjectName targetMbean) throws IOException{
    logInfo(prefix + " Calling createRegion");
    
    String newRegion=null;
    RegionOperations regionOps = JMXTest.getRegionOperations();
    boolean createSubRegion = HydraUtil.getRandomBoolean(); 
    if(!createSubRegion){      
      newRegion= regionOps.createRegion();
    }else{      
      Set<Region> regions = regionOps.getAvailableRegions();
      List<String> destroyedRegions = JMXBlackboard.getBB().getList(JMXBlackboard.REGION_DELETE_LIST);
      HydraUtil.logFine("RegionOperations: Searching replicatged regions in " + HydraUtil.ObjectToString(regions));
      List<Region> replicatedRegions = new ArrayList<Region>();
      for(Region replR : regions){        
        String path= null;
        if(replR.getParentRegion()!=null){
          path = replR.getParentRegion().getFullPath();
        }
        else path = replR.getFullPath();               
        if(replR instanceof DistributedRegion && !destroyedRegions.contains(path))
          replicatedRegions.add(replR);
      }
      HydraUtil.logFine("RegionOperations: Region eligible for destroy operations " + HydraUtil.ObjectToString(replicatedRegions));
      if(replicatedRegions.size()>0){
        newRegion= regionOps.createSubRegion(HydraUtil.getRandomElement(replicatedRegions).getFullPath());
      }else{
        throw new TestException("No replicated regions found. Subregions can be created only on replicated regions");
      }      
    }
    
    logInfo(prefix + " Finished with creating region named " + newRegion + " adding a notif expectation");
    addRegionCreateNotificationExp(newRegion);
    Region region = RegionHelper.getRegion(newRegion);
    if(newRegion.contains("Persistent") && !createSubRegion){
      String diskStoreName = region.getAttributes().getDiskStoreName();
      if(diskStoreName==null)
        throw new TestException("Diskstore returned null for persistent region " + newRegion);
      addDiskStoreCreatedNotificationExp(diskStoreName);
    }
    if(region==null)
      throw new TestException("Could not find newly created Region " + newRegion);
    String regionPath = region.getFullPath();
    logInfo(prefix + " Waiting for sleepReplication");
    HydraUtil.sleepForReplicationJMX();

    DistributedMember distributedMember = InternalDistributedSystem.getConnectedInstance().getDistributedMember();
    ObjectName regionName = JMXTest.getManagementService().getRegionMBeanName(distributedMember, regionPath);
    ObjectName distributedRegionName = MBeanJMXAdapter.getDistributedRegionMbeanName(regionPath);
    String url = ops.selectManagingNode();
    
    boolean runningOnManaingNode = RemoteTestModule.getMyClientName().contains("managing");
    MBeanServerConnection server = null;
    if(runningOnManaingNode){
      server = ManagementUtil.getPlatformMBeanServerDW();
      logInfo(prefix + " Checking for " + regionPath + " local platform mbeanServer");
    }
    else{
      server = ManagementUtil.connectToUrlOrGemfireProxy(url);
      logInfo(prefix + " Checking for " + regionPath + " MBeans at node " + url);
    }    
    targetMbean = regionName;
    if (!ManagementUtil.checkIfMBeanExists(server, targetMbean))
      throw new TestException("Could not find mean " + targetMbean + " on manager " + url);
    
    targetMbean = distributedRegionName;
    if (!ManagementUtil.checkIfMBeanExists(server, targetMbean))
      throw new TestException("Could not find mean " + targetMbean + " on manager " + url);
    logInfo(prefix + " Completed createRegion test successfully");
  }
  
  
  public void destroyRegion(JMXOperations ops, ObjectName targetMbean)throws IOException{
    logInfo(prefix + " Calling destroyRegion");
    RegionOperations regionOps = JMXTest.getRegionOperations();
    List<String> regions = RegionEvents.getAllRegions(RemoteTestModule.getMyClientName());
    List deletedRegions = JMXBlackboard.getBB().getList(JMXBlackboard.REGION_DELETE_LIST);
    //Set<Region> regions = regionOps.getAvailableRegions();
    logInfo(prefix + " Regions Created in this VM : " + regions);
    logInfo(prefix + " Regions Deleted in all VMs : " + deletedRegions);
    
    Region regionToDelete = null;
    for(int i=0;i<regions.size();i++){
      regionToDelete = RegionHelper.getRegion(regions.get(i));
      if(regionToDelete!=null && !deletedRegions.equals(regionToDelete))
        break;
    }
    if(regionToDelete!=null){
      String name = regionToDelete.getName();
      String regionPath = regionToDelete.getFullPath();
      regionOps.destroyRegion(regionToDelete);
      JMXBlackboard.getBB().addToList(JMXBlackboard.REGION_DELETE_LIST, regionPath);
      logInfo(prefix + " Finished with destroying region named " + name + " adding a notif expectation");
      addRegionDestroyNotificationExp(regionPath);
      logInfo(prefix + " Waiting for sleepReplication");
      HydraUtil.sleepForReplicationJMX();      
      DistributedMember distributedMember = InternalDistributedSystem.getConnectedInstance().getDistributedMember();
      ObjectName regionName = JMXTest.getManagementService().getRegionMBeanName(distributedMember, regionPath);
      ObjectName distributedRegionName = MBeanJMXAdapter.getDistributedRegionMbeanName(regionPath);
      String url = ops.selectManagingNode();
      MBeanServerConnection server = ManagementUtil.connectToUrlOrGemfireProxy(url);
      logInfo(prefix + " Checking for " + regionPath + " MBeans at node " + url);
      targetMbean = regionName;
      if (ManagementUtil.checkIfMBeanExists(server, targetMbean))
        throw new TestException("Proxy to " + targetMbean + " is stil present at " + url);
      
      targetMbean = distributedRegionName;
      if (ManagementUtil.checkIfMBeanExists(server, targetMbean))
        throw new TestException("Proxy to " + targetMbean + " is stil present at " + url);
      
    }else{
      throw new TestException("No Regions found in this VM for destoroRegion Test case");
    }
    logInfo(prefix + " Completed destroyRegion test successfully");
  }
  
  public void checkRegionStatistics(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionStatistics");
    String attributes[] = {
        "GetsRate",
        "PutsRate",
        "CreatesRate",
        "DestroyRate",
        "PutAllRate",
        "PutLocalRate",
        "PutRemoteRate",
        "PutRemoteLatency",
        "PutRemoteAvgLatency",
        "DiskReadsRate",
        "DiskWritesRate",
        "CacheWriterCallsAvgLatency",
        "CacheListenerCallsAvgLatency",
        "LruEvictionRate",
        "LruDestroyRate"        
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionStatistics " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionStatistics test successfully"); 
  }
  
  public void checkRegionRuntimeAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionRuntimeAttributes");
    String attributes[] = {
      "LastModifiedTime",
      "LastAccessedTime",
      "MissCount",
      "HitCount",
      "HitRatio",
      "EntryCount",
      "TotalEntriesOnlyOnDisk",
      "TotalDiskEntriesInVM"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionRuntimeAttributes " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionRuntimeAttributes test successfully");
  }
  
  public void checkRegionConfigAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionConfigAttributes");
    String attributes[] = {
        "Name",
        "RegionType",
        "FullPath",
        "ParentRegion"      
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionConfigAttributes " + HydraUtil.ObjectToString(attrList));
    
    
    if(!JMXPrms.useGemfireProxies()){
      
      CompositeData gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listRegionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp));
      
      gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listPartitionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listPartitionAttributes " + HydraUtil.ObjectToString(gemfireProp));
      
      gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listFixedPartitionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listFixedPartitionAttributesData " + HydraUtil.ObjectToString(gemfireProp));
      
      gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listEvictionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listEvictionAttributes " + HydraUtil.ObjectToString(gemfireProp));
      
      gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listMembershipAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listMembershipAttributes " + HydraUtil.ObjectToString(gemfireProp));
      
      
    }else{
      RegionAttributesData gemfireProp = (RegionAttributesData) callJmxOperation(url, ops,
          buildOperationArray("listRegionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp));
      
      PartitionAttributesData gemfireProp2 = (PartitionAttributesData) callJmxOperation(url, ops,
          buildOperationArray("listPartitionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp2));
      
      FixedPartitionAttributesData[] gemfireProp3 = (FixedPartitionAttributesData[]) callJmxOperation(url, ops,
          buildOperationArray("listFixedPartitionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp3));
      
      EvictionAttributesData gemfireProp4 = (EvictionAttributesData) callJmxOperation(url, ops,
          buildOperationArray("listEvictionAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp4));
      
      MembershipAttributesData gemfireProp5 = (MembershipAttributesData) callJmxOperation(url, ops,
          buildOperationArray("listMembershipAttributes", null,null, null), targetMbean);
      logInfo("checkRegionConfigAttributes:listRegionAttributes " + HydraUtil.ObjectToString(gemfireProp5));
      
      
    }
    logInfo(prefix + " Completed checkRegionConfigAttributes test successfully");
  }


  @Override
  public void doValidation(JMXOperations ops) {
    try{
      synchronized (RegionTestMBean.class) {
        if(!validationsComplete){
          validationsComplete = true;
          //Add logic to wait till updates to blackboard have been done
          int count = 0;
          if(JMXPrms.useGemfireProxies()){
            Map<String,String> map = FederationBlackboard.getBB().getManagerONs();
            count = map.size();
          }else{
            Map<String,String> map = FederationBlackboard.getBB().getMemberONs();
            count = map.size();
          }
          logInfo("Waiting for even VM to export events to blackboard and updates its counters");
          String clientName = RemoteTestModule.getMyClientName();
          TestHelper.waitForCounter(JMXBlackboard.getBB(), "TASKS_COMPLETE_COUNT", JMXBlackboard.VM_TASKS_COMPLETE_COUNT, count, true, 10*1000);
          AbstractEvents createEvents = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_ADDED, clientName, OperationsBlackboard.getBB());
          AbstractEvents destroyEvents = AbstractEvents.importFromBlackBoard(RegionOperationEvents.EVENT_REGION_DESTROYED, clientName, OperationsBlackboard.getBB());        
          List<String> regions = new ArrayList<String>();
          List<String> destroyedRegion = new ArrayList<String>();
          for(OperationEvent e : createEvents.getEvents(RegionOperationEvents.EVENT_REGION_ADDED)){
            regions.add((String)e.data);
          }
          
          for(OperationEvent e : destroyEvents.getEvents(RegionOperationEvents.EVENT_REGION_DESTROYED)){
            destroyedRegion.add((String)e.data);
          }
          
          for(Object name : JMXBlackboard.getBB().getList(JMXBlackboard.REGION_DELETE_LIST)){
            if(!destroyedRegion.contains(name))
              destroyedRegion.add((String)name);
          }
          
          logInfo("Checking to see if region " + destroyedRegion + " still exists.");
          String url = ops.selectManagingNode();
          MBeanServerConnection server = ManagementUtil.connectToUrlOrGemfireProxy(url);
          for(String name : destroyedRegion){
            DistributedMember distributedMember = InternalDistributedSystem.getConnectedInstance().getDistributedMember();
            ObjectName regionName = JMXTest.getManagementService().getRegionMBeanName(distributedMember, name);
            if(ManagementUtil.checkIfMBeanExists(server, regionName))
              throw new TestException("MBean " + regionName + " is not expected");
            regions.remove(name);
          }          
        }
      }
    }
    catch(IOException e){
      throw new TestException("Error doing validation " ,e);
    }    
  }

}
