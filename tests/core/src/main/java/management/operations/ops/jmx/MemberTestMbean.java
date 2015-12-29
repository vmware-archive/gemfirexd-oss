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
import hydra.CacheHelper;
import hydra.RemoteTestModule;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import management.Expectations;
import management.jmx.Expectation;
import management.jmx.JMXPrms;
import management.operations.ops.JMXOperations;
import management.test.jmx.JMXTest;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.JVMMetrics;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.OSMetrics;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;

/**
 * Test Class for MemberMBean Attributes, operations and notifications
 * 
 *@author tushark 
 */


@SuppressWarnings("serial")
public class MemberTestMbean extends AbstractTestMBean<MemberTestMbean> {

  static {
    prefix = "MemberTestMbean : ";
  }

  public MemberTestMbean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, MemberTestMbean.class, ton, tests);

  }

  @Override
  public String getType() {
    return gemfireDefinedMBeanTypes[MemberMBean];
  }

  public void createManager(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling createManager : call createManager and then check isManager attribute");
    
    String url = JMXOperations.LOCAL_MBEAN_SERVER;
    ObjectName oldTargetMbean = targetMbean;
    targetMbean = ManagementUtil.getLocalMemberMBeanON();

    logInfo("Target mbean for test createManager has been changed from " + oldTargetMbean + " to " + targetMbean);

    boolean isManager = (Boolean) ops.getAttribute(url, targetMbean, "Manager");
    if (!isManager) {
      logInfo(prefix + " Creating manager instance on " + url);

      Object result = callJmxOperation(url, ops, buildOperationArray("createManager", null, null, null), targetMbean);
      logInfo("Result of createManager " + HydraUtil.ObjectToString(result));

      ObjectName managerMbean = ManagementUtil.getLocalManagerMBean();
      if (managerMbean == null)
        throw new TestException("Could not locate manager mbean after createManager");
      boolean startManagerResult = (Boolean) callJmxOperation(url, ops, buildOperationArray("start", null, null, null),
          managerMbean);
      logInfo("Result of startManager " + HydraUtil.ObjectToString(startManagerResult));
      if (!startManagerResult)
        throw new TestException("Could start manager on memberMBean. Returned false");
      HydraUtil.sleepForReplicationJMX();
      isManager = (Boolean) ops.getAttribute(url, targetMbean, "Manager");
      boolean isManagerRunning = (Boolean) ops.getAttribute(url, managerMbean, "Running");
      logInfo("isManager attribute after startManager : " + isManager);
      logInfo("isManagerRunning attribute after startManager : " + isManagerRunning);
      if (!isManager || !isManagerRunning)
        throw new TestException("createManager JMX OP failed to make Member " + url + " Manager");
      else {
        try {
          MBeanServerConnection server = ops.getMBeanConnection(url);
          ManagementUtil.checkIfThisMemberIsCompliantManager(server);
        } catch (IOException e) {
          throw new TestException("error trying to connect member " + url, e);
        } catch (TestException e) {
          throw new TestException("Possible reason Bug #45600 - newly created JMX Manager via Member MBean does not create proxies to managed node in timely manner",e);
        } finally {
        }
      }
    } else {
      logInfo("Member " + url + " is already manager. Skipping the testcase");
    }
  }

  public void listDiskStores(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling listDiskStores");
    String url = ops.selectManagingNode();
    Object[] params = { true };
    String[] signature = { "boolean" };
    Object result = callJmxOperation(url, ops, buildOperationArray("listDiskStores", params, signature, null),
        targetMbean);
    logInfo("Result of listDiskStores " + HydraUtil.ObjectToString(result));
  }

  public void fetchLicense(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling fetchLicense");
    String url = ops.selectManagingNode();
    Object result = callJmxOperation(url, ops, buildOperationArray("viewLicense", null, null, null), targetMbean);
    logInfo("Result of fetchLicence " + HydraUtil.ObjectToString(result));
  }

  public void fetchLog(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling fetchLog");
    String url = ops.selectManagingNode();
    Object result = callJmxOperation(url, ops, buildOperationArray("showLog", 
        new Object[] {100 } , new String[]{ "int"}, null), targetMbean);
    logInfo("Result of fetchLog " + HydraUtil.ObjectToString(result));
  }

  public void checkGemfireConfig(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling checkGemfireConfig");
    // TODO : Check against GemfirePrms
    String url = ops.selectManagingNode();
    if(!JMXPrms.useGemfireProxies()){
      CompositeData gemfireProp = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("listGemFireProperties", null,null, null), targetMbean);
      logInfo("Gemfire Properties " + HydraUtil.ObjectToString(gemfireProp));
    }else{
      GemFireProperties gemfireProp = (GemFireProperties) callJmxOperation(url, ops,
          buildOperationArray("listGemFireProperties", null,null, null), targetMbean);
      logInfo("Gemfire Properties " + HydraUtil.ObjectToString(gemfireProp));
    }
  }

  public void checkOtherConfig(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = {  
        "ClassPath", 
        "CurrentTime", 
        "Groups",
        "Host", 
        "LockLease", 
        "LockTimeout", 
        "Member", 
        "MemberUpTime", 
        "Version"    
    };
    logInfo(prefix + " Calling checkOtherConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("Other Gemfire Properties " + HydraUtil.ObjectToString(attrList));
  }

  public void checkRegionCounters(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = { 
        "PartitionRegionCount",
        "RootRegionNames", 
        "TotalBucketCount", 
        "TotalHitCount", 
        "TotalMissCount", 
        "TotalNetLoadsCompleted",
        "TotalNetSearchCompleted",
        "TotalNumberOfGrantors",
        "TotalPrimaryBucketCount", 
        "TotalRegionCount", 
        "TotalRegionEntryCount",
        "TotalLoadsCompleted"
    };
    logInfo(prefix + " Calling checkRegionCounters");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    // TODO verify against regions created in this test. Entries addded against this VM
    logInfo("Region Counters " + HydraUtil.ObjectToString(attrList));    
    
    logInfo(prefix + " Calling listRegions");    
    String[] listRegions = (String[]) callJmxOperation(url, ops,
        buildOperationArray("listRegions", null, null, null), targetMbean);   
    logInfo("listRegions " + HydraUtil.ObjectToString(listRegions));    
  }  

  public void fetchJVMMetrics(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling fetchJVMMetrics");
    String url = ops.selectManagingNode();
    if(!JMXPrms.useGemfireProxies()){
      CompositeData jvmMetrics = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showJVMMetrics", null, null, null), targetMbean);
      logInfo("JvmMetrics " + HydraUtil.ObjectToString(jvmMetrics));
    }else{
      JVMMetrics jvmMetrics = (JVMMetrics) callJmxOperation(url, ops,
          buildOperationArray("showJVMMetrics", null, null, null), targetMbean);
      logInfo("JvmMetrics " + HydraUtil.ObjectToString(jvmMetrics));
    }
    

  }

  public void fetchOSMetrics(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling fetchOSMetrics");
    String url = ops.selectManagingNode();
    if(!JMXPrms.useGemfireProxies()){
      CompositeData osMetrics = (CompositeData) callJmxOperation(url, ops,
          buildOperationArray("showOSMetrics", null, null, null), targetMbean);
      logInfo("osMetrics " + HydraUtil.ObjectToString(osMetrics));
    }else{
      OSMetrics osMetrics = (OSMetrics) callJmxOperation(url, ops,
          buildOperationArray("showOSMetrics", null, null, null), targetMbean);
      logInfo("osMetrics " + HydraUtil.ObjectToString(osMetrics));
    }
  }

  public void processCommand(JMXOperations ops, ObjectName targetMbean) {
    String commands[] = { "list member", "list region"
    // TODO Add some more commands, probably use CLI Test classes to
    };
    String command = HydraUtil.getRandomElement(commands);
    logInfo(prefix + " Calling processCommand with " + command);
    
    String url = ops.selectManagingNode();
    Object[] params= {command};
    String[] signature = {"java.lang.String"};
    String jsonResult = (String) callJmxOperation(url, ops,
    buildOperationArray("processCommand", params, signature, null), targetMbean);
    logInfo("String returned by processCommand " + jsonResult);
    /*
     No way to validated since Output returned is plain string
    CommandResult result = (CommandResult)ResultBuilder.fromJson(jsonResult);
    printCommandOutput(result);
    Status status = result.getStatus();
    if(status.equals(Status.ERROR)){
      throw new TestException("Command " + command + " failed with error ");
    }*/     
  }

  /*
  private static void printCommandOutput(CommandResult cmdResult) {
    logInfo("Command Output : ");
    StringBuilder sb = new StringBuilder();
    while (cmdResult.hasNextLine()) {
      sb.append(cmdResult.nextLine()).append(DataCommandRequest.NEW_LINE);
    }
    logInfo(sb.toString());
    logInfo("");
  }*/

  @Override
  public void doValidation(JMXOperations ops) {
    

  }

    
  public void checkRegionLatencyCounters(JMXOperations ops, ObjectName targetMbean){
    
    logInfo(prefix + " Calling checkRegionLatencyCounters");
    String attributes[] = {
        "CacheListenerCallsAvgLatency", 
        "CacheWriterCallsAvgLatency", 
        //"CreatesAvgLatency", 
        //"DestroyAvgLatency", 
        //"GeAllAvgLatency", 
        "GetsAvgLatency", 
        "LoadsAverageLatency",
        "NetLoadsAverageLatency",
        "NetSearchAverageLatency",
        "PutAllAvgLatency", 
        "PutsAvgLatency", 
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionLatencyCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionLatencyCounters test successfully");
  
  }
  
  public void checkRegionRateCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionRateCounters");
    String attributes[] = {
        "CreatesRate", 
        "DestroysRate",
       // "GetAllRate" ,
        "GetsRate" ,
        "PutAllRate", 
        "PutsRate" 
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionRateCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionRateCounters test successfully");
  }
  
  public void checkWANResources(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkWANResources");
    String url = ops.selectManagingNode();    
    logInfo(prefix + " Calling listConnectedGatewayReceivers");    
    String[] listConnectedGatewayReceivers = (String[]) callJmxOperation(url, ops,
        buildOperationArray("listConnectedGatewayReceivers", null, null, null), targetMbean);   
    logInfo("listConnectedGatewayReceivers " + HydraUtil.ObjectToString(listConnectedGatewayReceivers));  
    
    logInfo(prefix + " Calling listConnectedGatewaySenders");    
    String[] listConnectedGatewaySenders = (String[]) callJmxOperation(url, ops,
        buildOperationArray("listConnectedGatewaySenders", null, null, null), targetMbean);   
    logInfo("listConnectedGatewaySenders " + HydraUtil.ObjectToString(listConnectedGatewaySenders));
    
    logInfo(prefix + " Completed checkWANResources test successfully");
  }
  
  public void checkGemfireRateCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireRateCounters");
    String attributes[] = {
        "BytesReceivedRate", 
        "BytesSentRate"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireRateCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireRateCounters test successfully");
  }
  
  public void checkFunctionCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkFunctionCounters");
    String attributes[] = {
        "NumRunningFunctions", 
        "NumRunningFunctionsHavingResults",
        "FunctionExecutionRate"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkFunctionCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkFunctionCounters test successfully");
  }
  
  public void checkGemfireTxCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireTxCounters");
    String attributes[] = {
        "TotalTransactionsCount", 
        "TransactionCommitsAvgLatency", 
        "TransactionCommitsRate", 
        "TransactionCommittedTotalCount", 
        "TransactionRolledBackTotalCount"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireTxCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireTxCounters test successfully");
  }
  
  public void checkGemfireDiskCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireDiskCounters");
    String attributes[] = {
        "DiskFlushAvgLatency", 
        "DiskReadsRate", 
        "DiskStores", 
        "DiskWritesRate", 
        "TotalBackupInProgress", 
        //"TotalDiskQueueSize" 
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireDiskCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireDiskCounters test successfully");
  }
  
  public void checkGemfireLockCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireLockCounters");
    String attributes[] = {
        "LockRequestQueues", 
        "LockWaitsInProgress", 
        "TotalLockWaitTime", 
        "TotalNumberOfLockService"     
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireLockCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireLockCounters test successfully");
  }
  
  public void checkGemfireLruCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireLruCounters");
    String attributes[] = {
        "LruDestroyRate", 
        "LruEvictionRate"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireLruCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireLruCounters test successfully");
  }
  
  public void checkGIICounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGIICounters");
    String attributes[] = {
        "InitialImageTime", 
        "InitialImageKeysReceived", 
        "InitialImagesInProgres" 
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGIICounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGIICounters test successfully");
  }
  
  public void checkGemfireSerializationCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkGemfireSerializationCounters");
    String attributes[] = {
        "DeserializationAvgLatency",
        "DeserializationLatency",
        "DeserializationRate",
        "PDXDeserializationAvgLatency",
        "PDXDeserializationRate",
        "SerializationAvgLatency",
        "SerializationLatency",
        "SerializationRate"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkGemfireSerializationCounters " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkGemfireSerializationCounters test successfully");
  }
  
  public void checkPlatformCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkPlatformCounters");
    String attributes[] = {
        "CpuUsage", 
        "CurrentHeapSize", 
        "FileDescriptorLimit", 
        "FreeHeapSize", 
        //"JvmThreads", 
        "MaximumHeapSize", 
        "TotalBackupCompleted",
        "TotalDiskTasksWaiting",
        "TotalFileDescriptorOpen" 
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkPlatformCounters " + HydraUtil.ObjectToString(attrList));
    
    logInfo(prefix + " Calling fetchJvmThreads");    
    String[] fetchJvmThreads = (String[]) callJmxOperation(url, ops,
        buildOperationArray("fetchJvmThreads", null, null, null), targetMbean);   
    logInfo("fetchJvmThreads " + HydraUtil.ObjectToString(fetchJvmThreads));     
    
    logInfo(prefix + " Completed checkPlatformCounters test successfully");
    
  }
  
  public void cacheStopStart(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling cacheStartStop");
    
    try {
      JMXTest.cacheStopStart();
    } catch (IOException e) {
      throw new TestException("IOEXECPTION",e);
    }
    
    logInfo(prefix + " Completed cacheStartStop successfully");
  }
  
  public void checkSystemAlert(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkSystemAlert");
    
    String url = ops.selectManagingNode();
    //targetMbean = ManagementUtil.getLocalMemberMBeanON();
    targetMbean = MBeanJMXAdapter.getDistributedSystemName();
    
    int dsId = (Integer) ops.getAttribute(url, targetMbean, "DistributedSystemId");
    String source = "DistributedSystem(" + dsId +")";
    InPlaceJMXNotifValidator validator = new InPlaceJMXNotifValidator("checkSystemAlert",targetMbean,url);
    String message = "MSG" + System.nanoTime(); 
    validator.expectationList.add(Expectations.
                forMBean(targetMbean).
                expectMBeanAt(url).
                expectNotification(ResourceNotification.SYSTEM_ALERT,
                    source,
                    message,
                    null,
                    Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS));
    
    Cache cache = CacheHelper.getCache();
    LogWriter writer = cache.getLogger();
    writer.severe(message);
    
    HydraUtil.sleepForReplicationJMX();
    
    validator.validateNotifications();
    
    logInfo(prefix + " Completed checkSystemAlert test successfully");    
  }
  
  /**
   * This method does transitions from managed to managing states
   * depending on the type of the node it is destined to be.
   * After execution it will return back to original state.
   * Ideally this should repeat at least two-three times in test.
   * So set numITerations to 3-4 times of number of nodes in topology
   * 
   */

  public void doTransition(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling doTransition");
    String clientName = RemoteTestModule.getMyClientName();
    if (clientName.contains("managing")) {
      doManagerTransitions(ops, targetMbean);
    } else if (clientName.contains("managed")) {
      doManagedTransitions(ops, targetMbean);
    }
    logInfo(prefix + " Completed doTransition test successfully");
  }

  /**
   * This method does transitions managed -> managing -> managed
   * 
   */
  private void doManagedTransitions(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling doManagedTransitions");
    String url = JMXOperations.LOCAL_MBEAN_SERVER;
    
    HydraUtil.logInfo("Transitioing from Managed to Managing node..");

    ManagementService service = JMXTest.getManagementService();
    createAndCheckManager(service);    
    HydraUtil.logInfo("Transitioing from Managed to Managing node Complete.");
    
    HydraUtil.logInfo("Transitioing from Managing to Managed node ..");
    MBeanServerConnection server = ops.getMBeanConnection(url);
    stopAndCheckManager(server, service);
    HydraUtil.logInfo("Transitioing from Managing to Managed node Complete.");
    
    logInfo(prefix + " Completed doManagedTransitions test successfully");
  }

  /**
   * This method does transitions managing -> managed -> managing
   * 
   */
  private void doManagerTransitions(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling doManagerTransitions");
    String url = JMXOperations.LOCAL_MBEAN_SERVER;
    
    HydraUtil.logInfo("Transitioing from Managing to Managed node ..");
    ManagementService service = JMXTest.getManagementService();
    MBeanServerConnection server = ops.getMBeanConnection(url);
    HydraUtil.logInfo("Transitioing from Managing to Managed node Complete.");
    
    HydraUtil.logInfo("Transitioing from Managed to Managing node..");    
    stopAndCheckManager(server, service);
    createAndCheckManager(service);
    HydraUtil.logInfo("Transitioing from Managed to Managing node Complete.");
    
    logInfo(prefix + " Completed doManagerTransitions test successfully");
  }

  private void createAndCheckManager(ManagementService service) {
    if (!service.isManager())
      service.startManager();
    else
      throw new TestException("This is supposed to be managed node. Should not host manager");
    HydraUtil.sleepForReplicationJMX();
    try {
      MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
      ManagementUtil.checkIfThisMemberIsCompliantManager(server);
    } catch (IOException e) {
      throw new TestException("Error connecting manager", e);
    } catch (TestException e) {
      throw e;
    }
    //TODO Check Manager RMI Connector is up or Not?
  }

  private void stopAndCheckManager(MBeanServerConnection server, ManagementService service) {
    service.stopManager();
    if (service.isManager())
      throw new TestException("Stop manager failed. Transition from manager to managed failed..");
    HydraUtil.sleepForReplicationJMX();
    try {
      
      List<String> list = ManagementUtil.checkForManagedMbeanProxies(server);
      if(list.size()<=1)
        throw new TestException("Managed member, after transitioning still contains proxies to managed nodes " + list);
      
      list = ManagementUtil.checkForManagingMbeanProxies(server);
      if(list.size()<=1)
        throw new TestException("Managed member, after transitioning still contains proxies to managing nodes " + list);      
      
      if (ManagementUtil.checkForDistributedMBean(server))
        throw new TestException("DistributedSystemMBean is still present in platform mbean server");
      /*#45949 if (ManagementUtil.checkForManagerMBean(server))
        throw new TestException("Manager MBean is still present in platform mbean server");*/
    } catch (IOException e) {
      throw new TestException("error", e);
    } catch (TestException e) {
      throw e;
    }
    
    //TODO Check Manager RMI Connector is down or Not?    
  }
  
}
