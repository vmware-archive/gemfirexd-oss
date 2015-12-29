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
import hydra.ClientVmInfo;
import hydra.TestConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import management.jmx.JMXPrms;
import management.operations.ops.JMXOperations;
import management.test.jmx.JMXTest;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;

import com.gemstone.gemfire.management.ClientHealthStatus;
import com.gemstone.gemfire.management.ServerLoadData;

/**
 * Test Class for CacheServerMBean Attributes, operations and notifications
 * @author tushark
 *
 */
public class CacheServerTestMBean extends AbstractTestMBean<CacheServerTestMBean> {
  
  
  private static final long serialVersionUID = 1L;

  static {
    prefix = "CacheServerTestMBean : ";
  }
  
  public static final String MEAN_EXIT = "MEAN_EXIT"; 
  public static final String MEAN_KILL = "MEAN_KILL";
  public static final String NICE_EXIT = "NICE_EXIT";
  public static final String NICE_KILL = "NICE_KILL";

  
  public CacheServerTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, CacheServerTestMBean.class, ton, tests);

  }
  
  public void startAndStopCacheServer(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling startAndStopCacheServer");
    String url = ops.selectManagingNode();
    boolean isRunning = (Boolean) ops.getAttribute(url, targetMbean, "Running");
    if(isRunning){      
      callJmxOperation(url, ops, buildOperationArray("stop", null, null, null), targetMbean);
      addCacheServerStoppedNotificationExp();
      HydraUtil.sleepForReplicationJMX();
      MBeanServerConnection server = ops.getMBeanConnection(url);
      try {
        boolean mbeanExists = ManagementUtil.checkIfMBeanExists(server, targetMbean);
        logInfo("Mbean Exists : " + mbeanExists);
        if(mbeanExists)
          throw new TestException("CacheServer stop operation failed. CacheServer is still running");
      } catch (IOException e) {
        throw new TestException("error trying to connect member " + url, e);
      }
    }else{
      logInfo("No CacheServer running.... that means mbean still exists...");
      throw new TestException("CacheServer MBean is present " + targetMbean + " but is not running");
    }
    logInfo(prefix + " Completed startAndStopCacheServer test successfully");
  }
  
  public void startAndStopCacheServerWtihConfigCheck(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling startAndStopCacheServerWtihConfigCheck");
    
    String attributes[] = { 
        "BindAddress",
        "Capacity",
        "DiskStoreName",
        "EvictionPolicy",
        "HostNameForClients",
        "LoadPollInterval",
        "MaxConnections",
        "MaximumMessageCount",
        "MaximumTimeBetweenPings",
        "MaxThreads",
        "MessageTimeToLive",
        "Port",
        "SocketBufferSize"
    };   
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("CacheServerConfig : " + HydraUtil.ObjectToString(attrList));    
    logInfo(prefix + " Completed startAndStopCacheServerWtihConfigCheck test successfully");
  }
  
  
  public void checkCacheServerStatistics(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkCacheServerStatistics");
    String attributes[] = {         
        "GetRequestAvgLatency",
        "GetRequestRate",
        "PutRequestAvgLatency",
        "PutRequestRate",
        "QueryRequestRate"
    };

    logInfo(prefix + " Calling checkCacheServerStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkCacheServerStatistics " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkCacheServerStatistics test successfully");
  }
  
  
  public void checkCacheServerRuntimeData(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkCacheServerRuntimeData");
    String attributes[] = { 
        "ConnectionLoad",
        "ConnectionThreads",
        "LoadPerConnection",
        "LoadPerQueue",
        "QueueLoad",
        "ThreadQueueSize",
        "TotalReceivedBytes",
        "TotalSentBytes"
    };

    logInfo(prefix + " Calling checkCacheServerRuntimeData");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkCacheServerRuntimeData " + HydraUtil.ObjectToString(attrList)); 
    
    if(!JMXPrms.useGemfireProxies()){
      CompositeData loadProbe = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("fetchLoadProbe", null, null, null), targetMbean);
      logInfo("loadProbe " + HydraUtil.ObjectToString(loadProbe));
    }else{
      ServerLoadData loadProbe = (ServerLoadData) callJmxOperation(url, ops,
          buildOperationArray("fetchLoadProbe", null, null, null), targetMbean);
      logInfo("loadProbe " + HydraUtil.ObjectToString(loadProbe));
    }
    logInfo(prefix + " Completed checkCacheServerRuntimeData test successfully");
    
  }
  
  public void checkIndexAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkIndexAttributes");
    String attributes[] = {
        "IndexCount",
        "IndexList",
        "TotalIndexMaintenanceTime"
    };
    logInfo(prefix + " Calling checkIndexAttributes");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkIndexAttributes " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkIndexAttributes test successfully");
  }
  
  public void checkCQAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkCQAttributes");
    String attributes[] = {
        "ActiveCQCount",
        "ContinuousQueryList"
    };
    logInfo(prefix + " Calling checkCQAttributes");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkCQAttributes " + HydraUtil.ObjectToString(attrList));     
    logInfo(prefix + " Completed checkCQAttributes test successfully");
  }
  
  
  public void checkClientAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkClientAttributes");
    String attributes[] = {
        "ClientConnectionCount",
        "ClientNotificationAvgLatency",
        "ClientNotificationRate",
        "CurrentClients",
        "NumClientNotificationRequests",
        "TotalConnectionsTimedOut",
        "TotalFailedConnectionAttempts",
    };
    logInfo(prefix + " Calling checkClientAttributes");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkClientAttributes " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkClientAttributes test successfully");
  }
  
  public void fetchClientStats(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchClientStats");
    String url = ops.selectManagingNode();    
    String clientId = getClientId(ops,url,targetMbean);    
    logInfo(prefix + " Calling fetchClientStats for client " + clientId);
    Object[] params= {clientId};
    String[] signature = {"java.lang.String"};
    if(!JMXPrms.useGemfireProxies()){
      CompositeData clientStats = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showClientStats", params, signature, null), targetMbean);
//      if(clientStats==null)
//        throw new TestException("fetchClientStats returned null for " + clientId);
      logInfo("ClientStats " + HydraUtil.ObjectToString(clientStats));
    }else{
      ClientHealthStatus clientStats = (ClientHealthStatus) callJmxOperation(url, ops,
          buildOperationArray("showClientStats", params, signature, null), targetMbean);
//      if(clientStats==null)
//        throw new TestException("fetchClientStats returned null for " + clientId);
      logInfo("ClientStats " + HydraUtil.ObjectToString(clientStats));
    }    
    logInfo(prefix + " Completed fetchClientStats test successfully");
  }
  
  private String getClientId(JMXOperations ops, String url, ObjectName targetMbean) {
    String clients[] = (String[]) callJmxOperation(url, ops, buildOperationArray("listClientIds", null, null, null), targetMbean);    
    String client = HydraUtil.getRandomElement(clients);
    if("UNDEFINED".equals(client))
      throw new TestException("Could not locate client. Return value is UNDEFINED indicates exception while calling jmx operations");
    return client;    
  }

  public void fetchClientIds(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling fetchClientIds");
    String url = ops.selectManagingNode();    
    String clients[] = (String[]) callJmxOperation(url, ops, buildOperationArray("listClientIds", null, null, null), targetMbean);
//    if(clients==null)
//      throw new TestException("fetchClientIds returned null ");
    logInfo("fetchClientIds " + HydraUtil.ObjectToString(clients));
    logInfo(prefix + " Completed fetchClientIds test successfully");
  }    
  
  public void closeContinuousQuery(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling closeContinuousQuery");
    String url = ops.selectManagingNode();
    String list[] = (String[]) ops.getAttribute(url, targetMbean, "ContinuousQueryList");
    String queryToClose = HydraUtil.getRandomElement(list);
    if(queryToClose!=null){
      logInfo(prefix + " Calling closeContinuousQuery with Query=" + queryToClose);
      Object[] params= {queryToClose};
      String[] signature = {"java.lang.String"};
      callJmxOperation(url, ops, buildOperationArray("closeContinuousQuery", params, signature, null), targetMbean);
    }
    logInfo(prefix + " Completed closeContinuousQuery test successfully");
  }
  
  public void closeAllContinuousQuery(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling closeAllContinuousQuery");
    String url = ops.selectManagingNode();
    Object[] params= {JMXTest.queryRegion};
    String[] signature = {"java.lang.String"};
    callJmxOperation(url, ops, buildOperationArray("closeAllContinuousQuery", params, signature, null), targetMbean);
    HydraUtil.sleepForReplicationJMX();
    long count = (Long) ops.getAttribute(url,targetMbean, "ActiveCQCount");
    String list[] = (String[]) ops.getAttribute(url, targetMbean, "ContinuousQueryList");
    logInfo("Query count after close " + count);
    logInfo("Query list after close " + HydraUtil.ObjectToString(list));
    logInfo(prefix + " Completed closeAllContinuousQuery test successfully");
  }
  
  public void stopContinuousQuery(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling stopContinuousQuery");
    String url = ops.selectManagingNode();
    String list[] = (String[]) ops.getAttribute(url, targetMbean, "ContinuousQueryList");
    String queryToStop = HydraUtil.getRandomElement(list);
    if(queryToStop!=null){
      logInfo(prefix + " Calling stopContinuousQuery with Query=" + queryToStop);
      Object[] params= {queryToStop};
      String[] signature = {"java.lang.String"};
      callJmxOperation(url, ops, buildOperationArray("stopContinuousQuery", params, signature, null), targetMbean);
    }
    logInfo(prefix + " Completed stopContinuousQuery test successfully");
  }
  
  public void executeContinuousQuery(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling executeContinuousQuery");
    String url = ops.selectManagingNode();
    String list[] = (String[]) ops.getAttribute(url, targetMbean, "ContinuousQueryList");
    String queryToExec = HydraUtil.getRandomElement(list);
    if(queryToExec!=null){
      logInfo(prefix + " Calling executeContinuousQuery with Query=" + queryToExec);
      Object[] params= {queryToExec};
      String[] signature = {"java.lang.String"};
      callJmxOperation(url, ops, buildOperationArray("executeContinuousQuery", params, signature, null), targetMbean);
    }
    logInfo(prefix + " Completed executeContinuousQuery test successfully");
  }
  
  public void removeIndex(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling removeIndex");
    String url = ops.selectManagingNode();
    String list[] = (String[]) ops.getAttribute(url, targetMbean, "IndexList");
    String indexToRemove = HydraUtil.getRandomElement(list);
    if(indexToRemove!=null){
      logInfo(prefix + " Calling removeIndex with Query=" + indexToRemove);
      Object[] params= {indexToRemove};
      String[] signature = {"java.lang.String"};
      callJmxOperation(url, ops, buildOperationArray("removeIndex", params, signature, null), targetMbean);
      HydraUtil.sleepForReplicationJMX();
      list = (String[]) ops.getAttribute(url, targetMbean, "IndexList");
//      for(String s : list)
//        if(s.equals(indexToRemove))
//          throw new TestException("removeIndex operation failed. IndexList still contains " + indexToRemove);
      logInfo("removeIndex operation failed. IndexList still contains " + indexToRemove);
    }
    logInfo(prefix + " Completed removeIndex test successfully");
  }
  
  @SuppressWarnings("unchecked")
  public void doClientHA(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling doClientHA");
    String url = ops.selectManagingNode();    
    String list[] = (String[])callJmxOperation(url, ops, 
          buildOperationArray("listClientIds", null, null, null), targetMbean);
    if(list!=null && list.length > 0 ){
      int clients = 1 + HydraUtil.getnextRandomInt(list.length);
      logInfo("Number of connected clients " + list.length + " Restarting " + clients + " clients");
      List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
      List<ClientVmInfo> selectedVmList = new ArrayList<ClientVmInfo>();
      List<String> stopModeList = new ArrayList<String>();
      List<String> clientIdsGoingDown = new ArrayList<String>();
      for(int i=0;i<clients;i++){
        //select random Client but make sure that same is not repeated
        String selectedClient = list[i]; 
        for (ClientVmInfo cInfo : vmList) {
          String clientName = cInfo.getClientName();
          String gemfireName = clientName.replace("edge", "gemfire");
          HydraUtil.logFine("clientName : " + clientName + " gemfireName = " + gemfireName + " name returned by mbean : " + selectedClient);
          if(selectedClient.contains(gemfireName)){
            clientIdsGoingDown.add(selectedClient);
            selectedVmList.add(cInfo);
            stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
          }
        }
      }
      logInfo("CliendIds not expected after shutdown : " + HydraUtil.ObjectToString(clientIdsGoingDown));
      addClientNotifExpectations(targetMbean,selectedVmList,stopModeList, clientIdsGoingDown);
      StopStartVMs.stopVMs(selectedVmList, stopModeList);
      logInfo(clients + " clients were Shutdown");
      HydraUtil.sleepForReplicationJMX();
      String[] oldClients = list;
      list = (String[])callJmxOperation(url, ops, 
          buildOperationArray("listClientIds", null, null, null), targetMbean);
      logInfo("CliendIds after shutdown : " + HydraUtil.ObjectToString(list));
      if(list!=null){
        int diff = oldClients.length - list.length;
        List<String> diffList = new ArrayList<String>();
        for(String cId : clientIdsGoingDown)
          for(String c: list)
            if(c.equals(cId))
              diffList.add(c);
        if(diff!=clients)
          throw new TestException("Client list shows " + list.length + " clients expected " + (oldClients.length - clients) + " diff : " + diffList);
        if(diffList.size()>0)
          throw new TestException("Client list shows " + list.length + " clients expected " + (oldClients.length - clients) + " diff : " + diffList);
      }      
      StopStartVMs.startVMs(selectedVmList);
      logInfo("Restart VMS : " + HydraUtil.ObjectToString(clientIdsGoingDown));
    }
    logInfo(prefix + " Completed doClientHA test successfully");
  }
  
  @SuppressWarnings("unused")
  private void addClientNotifExpectations(ObjectName targetMbean, List<ClientVmInfo> selectedVmList, List<String> stopModeList, List<String> clientIdsGoingDown) {    
    addCacheServerNotifListener(targetMbean, null);
    int i=0;
    for(ClientVmInfo info : selectedVmList){
      String mode = stopModeList.get(i);
      String clientId = clientIdsGoingDown.get(i);
      logInfo("client " + clientId + " is going down with mode " + mode);
      /*if(mode.equals(NICE_KILL) || mode.equals(NICE_EXIT)){
        addClientLeftNotificationExp(targetMbean, clientId);
      }else if (mode.equals(MEAN_KILL) || mode.equals(MEAN_EXIT)){
        addClientCrashedNotificationExp(targetMbean, clientId);
      }else{
        throw new TestException("Unknown mode of stop-start");
      }*/      
      if(mode.equals("MEAN_KILL")){
        addClientCrashedNotificationExp(targetMbean, clientId);
      }else if(mode.contains("NICE_EXIT") || mode.equals("NICE_KILL")){
        addClientLeftNotificationExp(targetMbean, clientId);
      }      
      i++;
    }   
  }

  /*
  @SuppressWarnings("rawtypes")
  private List<String> selectEdges(){
    Vector clientNames = TestConfig.tab().vecAt(hydra.ClientPrms.names, null);
    List<String> list = new ArrayList<String>();
    Iterator iterator = clientNames.iterator();
    while (iterator.hasNext()) {
      String name = (String) iterator.next();
      if (name.contains("edge"))
        list.add(name);
    }
    return list;
  }*/
  
  
  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[CacheServerMBean];
  }

  @Override
  public void doValidation(JMXOperations ops) {
    //closeAllContinuousQuery(ops,null);    
  }
  


}
