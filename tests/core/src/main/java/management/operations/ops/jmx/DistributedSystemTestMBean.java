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
import hydra.ClientPrms;
import hydra.ClientVmInfo;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import management.Expectations;
import management.jmx.Expectation;
import management.jmx.JMXNotificationListener;
import management.jmx.JMXPrms;
import management.jmx.SimpleJMXRecorder;
import management.operations.ops.JMXOperations;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;

import com.gemstone.gemfire.management.DiskMetrics;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.JVMMetrics;
import com.gemstone.gemfire.management.NetworkMetrics;
import com.gemstone.gemfire.management.OSMetrics;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;


/**
 * 
 * @author tushark
 */
@SuppressWarnings("serial")
public class DistributedSystemTestMBean extends AbstractTestMBean<DistributedSystemTestMBean> {
  
  static {
    prefix = "DistributedSystemTestMBean : ";
  }

  
  public DistributedSystemTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, DistributedSystemTestMBean.class, ton, tests);

  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[DistributedSystemMBean];
  }
  
  public void checkDSRuntime(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "MemberCount",
        "LocatorCount",
        "SystemDiskStoreCount",
        "AlertLevel" ,
        "DistributedSystemId"
    };
    logInfo(prefix + " Calling checkDSRuntime");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDSRuntime " + HydraUtil.ObjectToString(attrList)); 
  }

  public void checkDSRuntimeMetrics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "TotalHeapSize",
        "TotalRegionEntryCount",
        "TotalRegionCount",
        "TotalMissCount",
        "TotalHitCount",
        "NumClients",
        "NumInitialImagesInProgress",
        "ActiveCQCount" 
    };
    logInfo(prefix + " Calling checkDSRuntimeMetrics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDSRuntimeMetrics " + HydraUtil.ObjectToString(attrList)); 
  }
  
  public void checkStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "QueryRequestRate"
    };
    logInfo(prefix + " Calling checkStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkStatistics " + HydraUtil.ObjectToString(attrList)); 
  }
  
  public void checkDiskStoreStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "DiskReadsRate",
        "DiskWritesRate",
        "DiskFlushAvgLatency",
        "TotalBackupInProgress"
    };
    logInfo(prefix + " Calling checkDiskStoreStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDiskStoreStatistics " + HydraUtil.ObjectToString(attrList)); 
  }
  
  
  public void checkWANStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {

    };
    logInfo(prefix + " Calling checkWANStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkWANStatistics " + HydraUtil.ObjectToString(attrList));
  }
  
  
  
  public void fetchMemberConfiguration(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchMemberConfiguration");    
    String url = ops.selectManagingNode();    
    
    String member = getMemberId(ops,url,targetMbean);    
    logInfo(prefix + " Calling fetchMemberConfiguration for member " + member);
    Object[] params= {member};
    String[] signature = {"java.lang.String"};
    if(!JMXPrms.useGemfireProxies()){
      CompositeData props = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("fetchMemberConfiguration", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchMemberConfiguration returned null for " + member);
      logInfo(prefix + " Member configuration for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }else{
      GemFireProperties props = (GemFireProperties) callJmxOperation(url, ops,
          buildOperationArray("fetchMemberConfiguration", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchMemberConfiguration returned null for " + member);
      logInfo(prefix + " Member configuration for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }
    logInfo(prefix + " Completed fetchMemberConfiguration test successfully");
  }
  
  private String getMemberId(JMXOperations ops, String url, ObjectName targetMBean) {
    String members[] = (String[]) callJmxOperation(url, ops, buildOperationArray("listMembers", null, null, null), targetMBean);    
    String member = HydraUtil.getRandomElement(members);
    if("UNDEFINED".equals(member))
      throw new TestException("Could not locate member. Return value is UNDEFINED indicates exception while calling jmx operations");
    return member;
  }


  public void fetchMemberUpTime(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchMemberUpTime");
    String url = ops.selectManagingNode();    
    String member = getMemberId(ops,url,targetMbean);  
    logInfo(prefix + " Calling fetchMemberUpTime for member " + member);
    Object[] params= {member};
    String[] signature = {"java.lang.String"};
    Long prop =  (Long)callJmxOperation(url, ops,
        buildOperationArray("fetchMemberUpTime", params, signature, null), targetMbean);    
    logInfo(prefix + " Member fetchMemberUpTime for member " + member + " is "  + prop);
    logInfo(prefix + " Completed fetchMemberUpTime test successfully");
  }
  
  public void fetchJVMMetrics(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchJVMMetrics");
    String url = ops.selectManagingNode();    
    String member = getMemberId(ops,url,targetMbean);    
    logInfo(prefix + " Calling fetchJVMMetrics for member " + member);
    Object[] params= {member};
    String[] signature = {"java.lang.String"};
    if(!JMXPrms.useGemfireProxies()){
      CompositeData props = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showJVMMetrics", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchJVMMetrics returned null for " + member);
      logInfo(prefix + " Member fetchJVMMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }else{
      JVMMetrics props = (JVMMetrics) callJmxOperation(url, ops,
          buildOperationArray("showJVMMetrics", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchJVMMetrics returned null for " + member);
      logInfo(prefix + " Member fetchJVMMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }
    logInfo(prefix + " Completed fetchJVMMetrics test successfully");
  }
  
  public void fetchOSMetrics(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchOSMetrics");
    String url = ops.selectManagingNode();    
    String member = getMemberId(ops,url,targetMbean);
    logInfo(prefix + " Calling fetchOSMetrics for member " + member);
    Object[] params= {member};
    String[] signature = {"java.lang.String"};
    if(!JMXPrms.useGemfireProxies()){
      CompositeData props = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showOSMetrics", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchOSMetrics returned null for " + member);
      logInfo(prefix + " Member fetchOSMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }else{
      OSMetrics props = (OSMetrics) callJmxOperation(url, ops,
          buildOperationArray("showOSMetrics", params, signature, null), targetMbean);
      if(props==null)
        throw new TestException("fetchOSMetrics returned null for " + member);
      logInfo(prefix + " Member fetchOSMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }
    logInfo(prefix + " Completed fetchOSMetrics test successfully");
  }
  
  public void fetchNetworkMetric(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchNetworkMetric");
    String url = ops.selectManagingNode();    
    String member = getMemberId(ops,url,targetMbean);
    logInfo(prefix + " Calling fetchNetworkMetric for member " + member);
    Object[] params= {member};
    String[] signature = {"java.lang.String"};

    if (!JMXPrms.useGemfireProxies()) {
      CompositeData props = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showNetworkMetric", params, signature, null),
          targetMbean);
      if (props == null)
        throw new TestException("showNetworkMetric returned null for " + member);
      logInfo(prefix + " Member showNetworkMetric for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    } else {
      NetworkMetrics props = (NetworkMetrics) callJmxOperation(url, ops,
          buildOperationArray("showNetworkMetric", params, signature, null),
          targetMbean);
      if (props == null)
        throw new TestException("showNetworkMetric returned null for " + member);
      logInfo(prefix + " Member showNetworkMetric for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }
    logInfo(prefix + " Completed fetchNetworkMetric test successfully");
  }

  public void fetchDiskMetrics(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling fetchDiskMetrics");
    String url = ops.selectManagingNode();
    String member = getMemberId(ops, url, targetMbean);
    logInfo(prefix + " Calling fetchDiskMetrics for member " + member);
    Object[] params = { member };
    String[] signature = { "java.lang.String" };

    if (!JMXPrms.useGemfireProxies()) {
      CompositeData props = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showDiskMetrics", params, signature, null),
          targetMbean);
      if (props == null)
        throw new TestException("showDiskMetrics returned null for " + member);
      logInfo(prefix + " Member showDiskMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    } else {
      DiskMetrics props = (DiskMetrics) callJmxOperation(url, ops,
          buildOperationArray("showDiskMetrics", params, signature, null),
          targetMbean);
      if (props == null)
        throw new TestException("showDiskMetrics returned null for " + member);
      logInfo(prefix + " Member showDiskMetrics for member " + member);
      logInfo(prefix + " " + HydraUtil.ObjectToString(props));
    }
    logInfo(prefix + " Completed fetchDiskMetrics test successfully");
  }
  

  //TODO : revokeMissingDiskStores
  public void revokeMissingDiskStores(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling revokeMissingDiskStores");
    logInfo(prefix + " Completed revokeMissingDiskStores test successfully");
  }
  
  
  //TODO : listMissingDiskStores
  public void listMissingDiskStores(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling listMissingDiskStores");
    logInfo(prefix + " Completed listMissingDiskStores test successfully");
  }
  
  
  //TODO : shutDownAllMembers
  public void shutDownAllMembers(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling shutDownAllMembers");
    //TODO How do we fire this with being graceful to Hydra 
    logInfo(prefix + " Completed shutDownAllMembers test successfully");
  }

  //TODO : backupAllMembers
  public void backupAllMembers(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling backupAllMembers");
    logInfo(prefix + " Completed backupAllMembers test successfully");
  }
  
  
  private static final Pattern memberPattern = Pattern.compile("GemFire:type=Member,member=(.*)");
  
  String getMemberIdFromON(ObjectName name){
    Matcher match = memberPattern.matcher(name.toString());
    String memberName = null;
    if(match.find()){
      memberName = match.group(1);
      return memberName;
    }
    else return null;
  }
  
  public void restartMembers(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling restartMembers");
    
    String url = ops.selectManagingNode();
    InPlaceJMXNotifValidator validator = new InPlaceJMXNotifValidator("restartMembers-Before",targetMbean,url);
    
    String list[] = (String[])callJmxOperation(url, ops, 
          buildOperationArray("listMembers", null, null, null), targetMbean);
    
    HydraUtil.logInfo("Members " + HydraUtil.ObjectToString(list));
    
    ObjectName managerMemberON = (ObjectName) ops.getAttribute(url, targetMbean, "MemberObjectName");
    String managerMemberId = getMemberIdFromON(managerMemberON);    
    HydraUtil.logFine("Excluding manager member from list " + managerMemberId);
    
    if(list!=null && list.length > 0 ){
      int members = (1 + HydraUtil.getnextRandomInt(list.length));
      HydraUtil.logInfo("Searching " + members+ " eligible member/s, excluding manager, this member and at least one locator.");
      if(members==list.length)
        members--; //1 scope for current managing
      List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
      List<ClientVmInfo> selectedVmList = new ArrayList<ClientVmInfo>();
      List<String> stopModeList = new ArrayList<String>();
      List<String> memberIdsGoingDown = new ArrayList<String>();
      for(int i=0;i<members;i++){
        String selectedMember = list[i];
        HydraUtil.logFine("Checking for " + selectedMember);
        for (ClientVmInfo cInfo : vmList) {
          String memberName = cInfo.getClientName();
          String gemfireName = getGemfireName(memberName);
          //String complaintName = MBeanJMXAdapter.makeCompliantMBeanNameProperty(selectedMember);
          String complaintName = MBeanJMXAdapter.makeCompliantRegionNameAppender(selectedMember);
          HydraUtil.logFine("clientName : " + memberName + " gemfireName = " + gemfireName + " name returned by mbean : " + selectedMember);
          
          boolean makeSureAtleastOneLocatorIsInDs = makeSureAtleastOneLocatorIsInDs(memberIdsGoingDown,memberName);
          
          if(selectedMember.contains(gemfireName) && !complaintName.equals(managerMemberId)
              && cInfo.getVmid()!= RemoteTestModule.getMyVmid()
              && makeSureAtleastOneLocatorIsInDs){
            HydraUtil.logFine("Adding " + selectedMember + " Generated Name " + gemfireName);
            memberIdsGoingDown.add(selectedMember);
            selectedVmList.add(cInfo);
            String stopMode = TestConfig.tab().stringAt(StopStartPrms.stopModes);
            stopModeList.add(stopMode);
            
            //TODO : Add memberShip Hook
            /*
            if(stopMode.contains("MEAN"))              
              validator.expectationList.add(Expectations.
                  forMBean(targetMbean).
                  expectMBeanAt(url).
                  expectNotification(
                      ResourceNotification.CACHE_MEMBER_SUSPECT, 
                      selectedMember, 
                      ResourceNotification.CACHE_MEMBER_SUSPECT_PREFIX+selectedMember, 
                      null, 
                      Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
            */  
            
            validator.expectationList.add(Expectations.
                forMBean(targetMbean).
                expectMBeanAt(url).
                expectNotification(
                    ResourceNotification.CACHE_MEMBER_DEPARTED, 
                    selectedMember, 
                    ResourceNotification.CACHE_MEMBER_DEPARTED_PREFIX+selectedMember, 
                    null, 
                    Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
            
            
          }
        }
      }     
      members = memberIdsGoingDown.size();
      if(members!=0){
        logInfo("Number of members " + list.length + " Restarting " + members + " members");
        logInfo("CliendIds not expected after shutdown : " + HydraUtil.ObjectToString(memberIdsGoingDown));
        
        ManagementUtil.stopRmiConnectors(selectedVmList);
        
        
        StopStartVMs.stopVMs(selectedVmList, stopModeList);
        HydraUtil.sleepForReplicationJMX();
        HydraUtil.sleepForReplicationJMX();
        
        String[] oldMembers = list;
        list = (String[])callJmxOperation(url, ops, 
            buildOperationArray("listMembers", null, null, null), targetMbean);
        logInfo("MemberIds after shutdown : " + HydraUtil.ObjectToString(list));
        
        validator.validateNotifications();
        
        validator = new InPlaceJMXNotifValidator("restartMembers-After",targetMbean,url);
        
        if(list!=null){
          int diff = oldMembers.length - list.length;
          List<String> diffList = new ArrayList<String>();
          for(String cId : memberIdsGoingDown){
            validator.expectationList.add(Expectations.
                forMBean(targetMbean).
                expectMBeanAt(url).
                expectNotification(
                    ResourceNotification.CACHE_MEMBER_JOINED, 
                    ManagementUtil.getMemberID(), //ideally this should be new memberId
                    ResourceNotification.CACHE_MEMBER_JOINED_PREFIX,
                    members,
                    Expectation.MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT));
            
            for(String c: list)
              if(c.equals(cId))
                diffList.add(c);
          }
          if(diff!=members)
            throw new TestException("Member list shows " + list.length + " member/s expected " + (oldMembers.length - members) + " diff : " + diffList);
          if(diffList.size()>0)
            throw new TestException("Member list shows " + list.length + " member/s expected " + (oldMembers.length - members) + " diff : " + diffList);
        }
        
        
        StopStartVMs.startVMs(selectedVmList);
        logInfo("Restarting VMS : " + HydraUtil.ObjectToString(memberIdsGoingDown));
        
        HydraUtil.sleepForReplicationJMX();
        
        validator.validateNotifications();
        
      }else{
        logInfo("No eligible memberFound");
      }
    }
    logInfo(prefix + " Completed restartMembers test successfully"); 
  }  
  
  private boolean makeSureAtleastOneLocatorIsInDs(List<String> memberIdsGoingDown, String memberName) {
    int totalLocatorsInSystem = 0;
    int locatorsAdded = 0;
    
    Vector<String> clientNames = TestConfig.tab().vecAt(ClientPrms.names);
    for(int i=0;i<clientNames.size();i++){
      String name = clientNames.get(i);
      if(name.contains("locator"))
        totalLocatorsInSystem ++;
    }
    
    for(int i=0;i<memberIdsGoingDown.size();i++){
      String name = memberIdsGoingDown.get(i);
      if(name.contains("locator"))
        locatorsAdded++;
    }
    
    if(memberName.contains("locator"))
      locatorsAdded++;

    HydraUtil.logFine("totalLocatorsInSystem = " + totalLocatorsInSystem + " locatorsAdded="+ locatorsAdded 
        + " for memberName " + memberName + " Including Member : " + (locatorsAdded < totalLocatorsInSystem));
   
    return locatorsAdded < totalLocatorsInSystem;
  }


  private String getGemfireName(String memberName) {
    Vector<String> gemfireNames = TestConfig.tab().vecAt(ClientPrms.gemfireNames);
    Vector<String> clientNames = TestConfig.tab().vecAt(ClientPrms.names);
    for(int i=0;i<clientNames.size();i++){
      String name = clientNames.get(i);
      if(name.equals(memberName))
        return gemfireNames.get(i);
    }
    return null;
  }


  @Override
  public void doValidation(JMXOperations ops) {    
    
  }

}