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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.AttributeList;
import javax.management.ObjectName;

import management.Expectations;
import management.jmx.Expectation;
import management.operations.ops.JMXOperations;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;


/**
 * 
 * @author tushark
 */
@SuppressWarnings("serial")
public class GatewaySenderTestMBean extends AbstractTestMBean<GatewaySenderTestMBean> {
  
  static {
    prefix = "GatewaySenderTestMBean : ";
  }

  
  public GatewaySenderTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, GatewaySenderTestMBean.class, ton, tests);

  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[GatewaySenderMBean];
  }
  
  
  public void checkSenderConfig(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "RemoteDSId",
        "SocketBufferSize",
        "SocketReadTimeout",
        "OverflowDiskStoreName",
        "MaximumQueueMemory",
        "BatchSize",
        "BatchTimeInterval",
        "BatchConflationEnabled",
        "PersistenceEnabled",
        "AlertThreshold",
        "GatewayEventFilters",
        "GatewayTransportFilters",
        "ManualStart",
        "OrderPolicy",
        "DiskSynchronous",
        "Parallel"
    };
    logInfo(prefix + " Calling checkSenderConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkSenderConfig " + HydraUtil.ObjectToString(attrList));   
  }
  
  
  public void checkSenderRuntime(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "Running",
        "Paused",
        "Primary",
        "DispatcherThreads"
    };
    logInfo(prefix + " Calling checkSenderRuntime");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkSenderRuntime " + HydraUtil.ObjectToString(attrList));   
  }
  
  
  public void checkSenderStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "EventsReceivedRate",
        "EventsQueuedRate",
        "EventQueueSize",
        "TotalEventsConflated",
        "AverageDistributionTimePerBatch",
        "TotalBatchesRedistributed"
    };
    logInfo(prefix + " Calling checkSenderStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkSenderStatistics " + HydraUtil.ObjectToString(attrList));   
  }
  
  public void startStopSender(JMXOperations ops, ObjectName targetMbean) throws MalformedURLException, IOException{
    logInfo(prefix + " Calling startStopSender on GW-Sender " + targetMbean);
    String url = ops.selectManagingNode();
    /*
    callJmxOperation(url, ops, buildOperationArray("stop", null, null, null),targetMbean);
    HydraUtil.sleepForReplicationJMX();
    if(!ManagementUtil.checkIfMBeanExists(ManagementUtil.connectToUrl(url), targetMbean)){
      throw new TestException("Stop operation failed to stop the gateway sender, mbean still exiists" +
          " represented by mbean " + targetMbean);    
    }*/
    boolean isRunning = (Boolean) ops.getAttribute(url,targetMbean,"Running");
    if(isRunning){
      
      InPlaceJMXNotifValidator validator = new InPlaceJMXNotifValidator("startStopSender-stop",targetMbean,url);
      callJmxOperation(url, ops, buildOperationArray("stop", null, null, null),targetMbean);
      
      validator.expectationList.add(Expectations.
          forMBean(targetMbean).
          expectMBeanAt(url).
          expectNotification(
              ResourceNotification.GATEWAY_SENDER_STOPPED, 
              getWanSenderMemberId(targetMbean), 
              ResourceNotification.GATEWAY_SENDER_STOPPED_PREFIX, 
              null, 
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));      
      
      HydraUtil.sleepForReplicationJMX();
      boolean running = (Boolean) ops.getAttribute(url,targetMbean,"Running");      
      if(running)
        throw new TestException("Stop operation failed to stop the gateway sender represented by mbean " + targetMbean);
      
      validator.validateNotifications();
      
      validator = new InPlaceJMXNotifValidator("startStopSender-start",targetMbean,url);
      
      callJmxOperation(url, ops, buildOperationArray("start", null, null, null),targetMbean);
      //addGWSenderStoppedNotificationExp();
      HydraUtil.sleepForReplicationJMX();
      
      /* It might be resume notif instead of start notif
      validator.expectationList.add(Expectations.
          forMBean(targetMbean).
          expectMBeanAt(url).
          expectNotification(
              ResourceNotification.GATEWAY_SENDER_STARTED, 
              getWanSenderMemberId(targetMbean), 
              ResourceNotification.GATEWAY_SENDER_STARTED_PREFIX, 
              null, 
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
      
      validator.validateNotifications();
      */      
      
      boolean isStarted = (Boolean) ops.getAttribute(url,targetMbean,"Running");        
      if(!isStarted)
        throw new TestException("Start operation failed to stop the gateway sender represented by mbean " + targetMbean);        
    }else{
      callJmxOperation(url, ops, buildOperationArray("start", null, null, null),targetMbean);
      HydraUtil.sleepForReplicationJMX();
      boolean isStarted = (Boolean) ops.getAttribute(url,targetMbean,"Running");
      if(!isStarted)
        throw new TestException("Start operation failed to stop the gateway sender represented by mbean " + targetMbean);
      
      /* It might be resume notif instead of start notif
      validator.expectationList.add(Expectations.
          forMBean(targetMbean).
          expectMBeanAt(url).
          expectNotification(
              ResourceNotification.GATEWAY_SENDER_STARTED, 
              getWanSenderMemberId(targetMbean), 
              ResourceNotification.GATEWAY_SENDER_STARTED_PREFIX, 
              null, 
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
      
      validator.validateNotifications();
      */
      
      //leave it running
    }
    logInfo(prefix + " startStopSender test completed successfully");
  }
  
  public void pauseResumeSender(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling pauseResumeSender on GW-Sender " + targetMbean);
    String url = ops.selectManagingNode();
    boolean isRunning = (Boolean) ops.getAttribute(url,targetMbean,"Running");
    if(isRunning){
      ObjectName dsObjectName = MBeanJMXAdapter.getDistributedSystemName();
      InPlaceJMXNotifValidator validator = new InPlaceJMXNotifValidator("pauseResumeSender-pause",dsObjectName,url);
      callJmxOperation(url, ops, buildOperationArray("pause", null, null, null),targetMbean);
      HydraUtil.sleepForReplicationJMX();
      
      validator.expectationList.add(Expectations.
          forMBean(dsObjectName).
          expectMBeanAt(url).
          expectNotification(
              ResourceNotification.GATEWAY_SENDER_PAUSED, 
              getWanSenderMemberId(targetMbean), 
              ResourceNotification.GATEWAY_SENDER_PAUSED_PREFIX, 
              null, 
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS)); 
      
      boolean isPaused = (Boolean) ops.getAttribute(url,targetMbean,"Paused");
      if(!isPaused)
        throw new TestException("Pause operation failed to pause the gateway sender represented by mbean " + targetMbean);
      validator.validateNotifications();
      
      validator = new InPlaceJMXNotifValidator("pauseResumeSender-resume",targetMbean,url);
      
      callJmxOperation(url, ops, buildOperationArray("resume", null, null, null),targetMbean);
      HydraUtil.sleepForReplicationJMX();
      
      boolean isResumed = (Boolean) ops.getAttribute(url,targetMbean,"Running");        
      if(!isResumed)
        throw new TestException("Resume operation failed to resume the gateway sender represented by mbean " + targetMbean);
      
      /*
      validator.expectationList.add(Expectations.
          forMBean(targetMbean).
          expectMBeanAt(url).
          expectNotification(
              ResourceNotification.GATEWAY_SENDER_RESUMED, 
              getWanSenderMemberId(targetMbean), 
              ResourceNotification.GATEWAY_SENDER_RESUMED_PREFIX, 
              null, 
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
              
      validator.validateNotifications();
      */      
      
    }else{
      logInfo(prefix + " Calling GW-Sender is stopeed starting it : " + targetMbean);
      callJmxOperation(url, ops, buildOperationArray("start", null, null, null),targetMbean);
      HydraUtil.sleepForReplicationJMX();
      logInfo(prefix + " Now Calling GW-Sender resume() : " + targetMbean);
      callJmxOperation(url, ops, buildOperationArray("resume", null, null, null),targetMbean);
      HydraUtil.sleepForReplicationJMX();
      boolean isResumed = (Boolean) ops.getAttribute(url,targetMbean,"Running");        
      if(!isResumed)
        throw new TestException("Start/Resume operation failed to resume the gateway sender represented by mbean " + targetMbean);      
      //leave it running
    }
    logInfo(prefix + " pauseResumeSender test completed successfully");
  }
  

  private static final Pattern memberPattern = Pattern.compile("GemFire:service=GatewaySender,gatewaySender=(.*),type=Member,member=(.*)");

  private Object getWanSenderMemberId(ObjectName targetMbean) {
    String name = targetMbean.toString();
    Matcher match = memberPattern.matcher(name.toString());
    String memberName = null;
    if(match.find()){
      memberName = match.group(2);
    }
    HydraUtil.logFine("WanSenderMember Id = " + memberName + " for ON" + name);
    return memberName;
  }


  @Override
  public void doValidation(JMXOperations ops) {    
    
  }
}