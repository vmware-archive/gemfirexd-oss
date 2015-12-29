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
package management.test.jmx;

import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;
import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GatewayReceiverHelper;
import hydra.GatewaySenderHelper;
import hydra.GatewaySenderPrms;
import hydra.GemFireDescription;
import hydra.HydraThreadLocal;
import hydra.HydraVector;
import hydra.JMXManagerBlackboard;
import hydra.JMXManagerHelper;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;

import management.Expectations;
import management.jmx.Expectation;
import management.jmx.JMXBlackboard;
import management.jmx.JMXNotificationListener;
import management.jmx.JMXPrms;
import management.jmx.SimpleJMXRecorder;
import management.jmx.validation.JMXValidator;
import management.jmx.validation.ValidationResult;
import management.operations.OperationsBlackboard;
import management.operations.RegionKeyValueConfig;
import management.operations.SimpleRegionKeyValueConfig;
import management.operations.events.impl.AbstractEvents;
import management.operations.events.impl.CQAndIndexEvents;
import management.operations.events.impl.DLockEvents;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.CQAndIndexOperations;
import management.operations.ops.DLockOperations;
import management.operations.ops.JMXOperations;
import management.operations.ops.RegionOperations;
import management.operations.ops.jmx.AbstractTestMBean.InPlaceJMXNotifValidator;
import management.operations.ops.jmx.TestMBean;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;

public class JMXTest {
  
  private static JMXTest testInstance;
  private DistributedSystem ds = null;
  private GemFireCacheImpl cache = null;
  private ManagementService service;
  private CacheServer server = null;
  
  //Notif Infra
  private SimpleJMXRecorder jmxNotifRecorder = new SimpleJMXRecorder();
  private List<Expectation> notifExpectationList = new ArrayList<Expectation>();
  private String managingNodeForNotifs =null;  
  private Map<ObjectName,JMXNotificationListener> allNotiflisteners  = new HashMap<ObjectName,JMXNotificationListener>();  
  
  private HydraThreadLocal regionHT = new HydraThreadLocal();
  private HydraThreadLocal dlockHT = new HydraThreadLocal();
  private RegionKeyValueConfig regionkeyValueConfig = new SimpleRegionKeyValueConfig("KEY" + RemoteTestModule.getMyVmid(), 100000);
  private static String DISTRIBUTED_SUFFIX = "Distributed";
  public static String queryRegion = "TestReplicatedAck_1";
  
  public static HydraThreadLocal getRegionHT() {
    return testInstance.regionHT;
  }
  
  public static HydraThreadLocal getDLockHT() {
    return testInstance.dlockHT;
  }
  
  
  public static ManagementService getManagementService(){
    return testInstance.service;
  }
  
  //Temp fix for serial tests where regions are created on different threads and accessed in another thread.
  private static RegionOperations regionOps = null;
  public static synchronized RegionOperations getRegionOperations(){
    //regionOps = (RegionOperations) getRegionHT().get();
    if(regionOps==null){
      regionOps = new RegionOperations(testInstance.cache, new RegionEvents());
      //getRegionHT().set(regionOps);
    }
    return regionOps;
  }
  private static DLockOperations dlockOps =null;
  public static synchronized DLockOperations getDLockOperations(){    
    if(dlockOps==null){
      dlockOps = new DLockOperations(testInstance.ds,new DLockEvents());
      //getDLockHT().set(dlockOps);
    }
    return dlockOps;
  }
  
  
  private static CQAndIndexOperations cqOps = null;
  @SuppressWarnings("rawtypes")
  public static synchronized CQAndIndexOperations getCQOperations(){    
    if(cqOps==null){
      Region region = RegionHelper.getRegion(queryRegion);
      cqOps = new CQAndIndexOperations(region,testInstance.regionkeyValueConfig, new CQAndIndexEvents());
    }
    return cqOps;
  }
  
  public synchronized static void HydraInitTask_startLocator() {
    if (testInstance == null) {
      testInstance = new JMXTest();      
    }
    testInstance.startLocator();
  } 

  public synchronized static void HydraInitTask_initialize() {
    if (testInstance == null) {
      testInstance = new JMXTest();      
    }
    testInstance.initialize();
  }
  
  public synchronized static void HydraInitTask_becomeManager() {
    testInstance.becomeManager();
  }
  
  public synchronized static void HydraInitTask_setUpJMXListeners() {
    testInstance.setUpJMXListeners();
  }  

  public static void HydraInitTask_startWANSendersAndReceivers() {
    testInstance.startWANSendersAndReceivers();
  }
   
  public static synchronized void HydraInitTask_startWithBridgeServer(){
    testInstance.starBridgeServer();
  }
  
  public static synchronized void HydraInitTask_createRegionsOnEdge(){
    testInstance.createRegionsOnEdge();
  }
  
  public static synchronized void HydraInitTask_createRegionsOnBridge(){
    testInstance.createRegionsOnBridge();
  }
  
  public static synchronized void HydraInitTask_createQueryResources(){
    testInstance.createQueryResources();
  }  

  public static synchronized void HydraInitTask_createLockServiceInstances(){
    testInstance.createLockServiceInstances();
  }
  
  public static void HydraTask_jmxOperations() {
    testInstance.doJMXOps();
  }  
  
  public static void HydraCloseTask_printEvents(){
    testInstance.printEvents();
  }
  
  public static void HydraCloseTask_validateEvents(){
    testInstance.validateEvents();
  }
  
  public static void HydraCloseTask_validateNotifications(){
    testInstance.validateNotifications();
  } 

  public static void HydraCloseTask_validateWANEvents(){
    testInstance.validateWANEvents();
  }
  
  public static void addDSNotifListener(ObjectName name, String memberId) {
    try {
      MBeanServerConnection mbeanServer = ManagementUtil.connectToUrlOrGemfireProxy(testInstance.managingNodeForNotifs);
      JMXNotificationListener dsMBeanlistener = new JMXNotificationListener("JMXTest-DistributedSystemListener",testInstance.jmxNotifRecorder, name,
          memberId);
      logInfo("Added DS JMX listener for " + name + " on server " + testInstance.managingNodeForNotifs);
      mbeanServer.addNotificationListener(name, dsMBeanlistener, null, null);
      testInstance.allNotiflisteners.put(name, dsMBeanlistener);
    } catch (MalformedURLException e) {
      throw new TestException("error", e);
    } catch (IOException e) {
      throw new TestException("error", e);
    } catch (InstanceNotFoundException e) {
      throw new TestException("error", e);
    }
  }
  
  public static void addMemberNotifListener(ObjectName name, String memberId) {
    try {
      JMXNotificationListener memberMBeanlistener = new JMXNotificationListener("JMXTest-MemberListener", testInstance.jmxNotifRecorder, name,
          memberId);
      MBeanServerConnection mbeanServer = ManagementUtil.connectToUrlOrGemfireProxy(testInstance.managingNodeForNotifs);
      mbeanServer.addNotificationListener(name, memberMBeanlistener, null, null);
      testInstance.allNotiflisteners.put(name, memberMBeanlistener);
      logInfo("Added Member JMX listener for " + name + " on server " + testInstance.managingNodeForNotifs);
    } catch (MalformedURLException e) {
      throw new TestException("error", e);
    } catch (IOException e) {
      throw new TestException("error", e);
    } catch (InstanceNotFoundException e) {
      throw new TestException("error", e);
    }
  }
  
  public static void addCacheServerNotifListener(ObjectName cacheServerON, String memberId){
    try {
      MBeanServerConnection mbeanServer = ManagementUtil.connectToUrlOrGemfireProxy(testInstance.managingNodeForNotifs);      
      JMXNotificationListener cacheServerMBeanlistener = new JMXNotificationListener("JMXTest-CacheServerListener", testInstance.jmxNotifRecorder, cacheServerON);
      mbeanServer.addNotificationListener(cacheServerON, cacheServerMBeanlistener, null, null);
      testInstance.allNotiflisteners.put(cacheServerON, cacheServerMBeanlistener);
      logInfo("Added CacheServer JMX listener for " + cacheServerON + " on server " + testInstance.managingNodeForNotifs);
    } catch (MalformedURLException e) {
      throw new TestException("error", e);
    } catch (IOException e) {
      throw new TestException("error", e);
    } catch (InstanceNotFoundException e) {
      throw new TestException("error", e);
    }
  }
  
  public static boolean isNotifListenerRegistered(ObjectName name){
    return testInstance.allNotiflisteners.containsKey(name);
  }
  
  public static void cacheStopStart() throws IOException{
    final List<ObjectName> registeredMBeans = new ArrayList<ObjectName>();
    final List<ObjectName> unRegisteredMBeans = new ArrayList<ObjectName>();    
    final List<ObjectName> beforeConnectMBeans = new ArrayList<ObjectName>();
    
    ObjectName memberMBeanNameBeforeDisConnect = ManagementUtil.getLocalMemberMBeanON();
    
    for(ObjectName n : ManagementUtil.getPlatformMBeanServer().queryNames(null, null)){
      if(n.toString().contains("GemFire")){
        beforeConnectMBeans.add(n);
        logFine("Added " + n + " in gemfire mbean set");
      }
    }    
    
    NotificationListener superListener = new NotificationListener() {
      public void handleNotification(Notification notification, Object handback) {
        MBeanServerNotification mbs = (MBeanServerNotification) notification;
        if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {
          logInfo("MBean Registered [" + mbs.getMBeanName() + "]");
          if (mbs.getMBeanName().toString().contains("GemFire")) {
            registeredMBeans.add(mbs.getMBeanName());           
          }
        } else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) {
          logInfo("MBean Unregistered [" + mbs.getMBeanName() + "]");
          unRegisteredMBeans.add(mbs.getMBeanName());
        }
      }
    };
    
    try {
      MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
      MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
      filter.enableAllObjectNames();
      server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, superListener, filter, null);
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error starting locator", e);      
    } catch (IOException e) {
      throw new TestException("Error starting locator", e);
    }    
    
    testInstance.cache.getDistributedSystem().disconnect();
    testInstance.cache = null;
    testInstance.ds = null;
    testInstance.service=null;    
    
    HydraUtil.sleepForReplicationJMX();
    Set<ObjectName> afterDisConnect = ManagementUtil.getPlatformMBeanServer().queryNames(null, null);
    logFine("Checking for presence of any gemfire mbeans");
    for(ObjectName n : beforeConnectMBeans){
      if(afterDisConnect.contains(n))
        throw new TestException("Found gemfire mbean after cache disconnect  - " + n);
    }
    
    testInstance.initialize();
    
    if(RemoteTestModule.getMyClientName().contains("managing"))
      testInstance.becomeManager();
    
    ObjectName memberMBeanNameAfterDisConnect = ManagementUtil.getLocalMemberMBeanON();
    
    
    //TODO : Add some more resources like : Region and LockService, DiskStore
    
    beforeConnectMBeans.remove(memberMBeanNameBeforeDisConnect);
    unRegisteredMBeans.remove(memberMBeanNameBeforeDisConnect);
    registeredMBeans.remove(memberMBeanNameAfterDisConnect);    
    
    HydraUtil.logInfo("Gemfire MBean set(excluding memberMBean) before dis-connect : " + beforeConnectMBeans);
    HydraUtil.logInfo("Gemfire MBean set(excluding memberMBean) during dis-connect : " + unRegisteredMBeans);
    HydraUtil.logInfo("Gemfire MBean after(excluding memberMBean) re-connect : " + registeredMBeans);
    
    List<ObjectName> diffList = checkTwoLists(beforeConnectMBeans,unRegisteredMBeans);
    if(diffList.size()>0)
      throw new TestException("beforeConnectMBeans and unRegisteredMBeans mismatch Diff : " + diffList);
    
    diffList = checkTwoLists(beforeConnectMBeans,registeredMBeans);
    if(diffList.size()>0)
      throw new TestException("beforeConnectMBeans and registeredMBeans mismatch Diff : " + diffList);
    
    diffList = checkTwoLists(unRegisteredMBeans,registeredMBeans);
    if(diffList.size()>0)
      throw new TestException("unRegisteredMBeans and registeredMBeans mismatch Diff : " + diffList);
    
    try {
      ManagementUtil.getPlatformMBeanServer().removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, superListener);
    } catch (InstanceNotFoundException e) {
      HydraUtil.logError("Error removing listener", e);
    } catch (ListenerNotFoundException e) {
      HydraUtil.logError("Error removing listener", e);
    }
  }
  
  public static void addExpectationForNotification(ObjectName mbeanName, String type, String source, String message) {
    testInstance._addExpectationForNotification(mbeanName, type, source, message,
        Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE);
  }

  public static void addExpectationForNotificationPseudoMatch(ObjectName mbeanName, String type, String source,
      String message) {
    testInstance._addExpectationForNotification(mbeanName, type, source, message,
        Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS);
  }
  
  public static void validateAndPrint(JMXValidator validator) {
    List<ValidationResult> result = validator.validate();
    if (result.size() > 0) {
      logInfo("JMX Validations failed. See below details");
      StringBuilder sb = new StringBuilder();
      int i=1;
      for (ValidationResult r : result) {       
        HydraUtil.logError(r.toString());
        sb.append(i++ + "# " + r.toString()).append(HydraUtil.NEW_LINE);
      }
      throw new TestException(result.size() + " JMX validation failed \n" + sb.toString());
    } else {
      logInfo("JMX Validations suceeded");
    }
  }
  
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void validateWANEvents() {
    //temporary validation just to see if events are replicated or not
    Set<Region> regions = getRegionOperations().getAvailableRegions();
    Map<String,String> map = new HashMap();
    for(Region r : regions){
      String key = RemoteTestModule.getMyClientName() + r.getFullPath();
      String value = "VALUE";
      r.put(key, value);
      map.put(key, value);
    }    
    JMXBlackboard.getBB().saveMap("WANEVENTS_"+ RemoteTestModule.getMyClientName(), map);    
  }
  
  private void startLocator() {
    DistributedSystemHelper.createLocator();
    DistributedSystemHelper.startLocatorAndDS();
    waitForLocatorDiscovery();    
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
  private void waitForLocatorDiscovery(){
    logInfo("Waititng for locator discovery.");   
    boolean isDiscovered;    
    do{
      isDiscovered = true;
      List<Locator> locators = Locator.getLocators();
      Map gfLocMap = ((InternalLocator)locators.get(0)).getAllLocatorsInfo();
      List dsList = new ArrayList(); //non discovered ds
      for (GemFireDescription gfd : TestConfig.getInstance().getGemFireDescriptions().values()) {
        Integer ds = gfd.getDistributedSystemId();
        if(!ds.equals(-1) && !dsList.contains(ds) && !gfLocMap.containsKey(ds)){
          dsList.add(ds);
          isDiscovered = false;
        }
      }
      if(!isDiscovered){
        logInfo("Waiting for locator discovery to complete. Locators not discoverd so far from ds " + dsList 
            + ". Distribution locator map from gemfire system is " +  (new  ConcurrentHashMap(gfLocMap)).toString());
        MasterController.sleepForMs(5 * 1000); //wait for 5 seconds
      }       
    }while(!isDiscovered);
    logInfo("Locator discovery completed.");
  }
  
  @SuppressWarnings("unchecked")
  private void validateEvents() {
    recycleConnections();
    Map<String,TestMBean> mbeanMap = JMXBlackboard.getBB().getMap(JMXBlackboard.MBEAN_MAP);
    String url = FederationBlackboard.getBB().getManagingNode();
    Set<ObjectName> gemfireMBeans = ManagementUtil.getGemfireMBeans(url);
    logFine("MBeans " + gemfireMBeans + " found on url " + url);
    logFine("mbeanMap " + mbeanMap);
    JMXOperations ops = buildeTestMap(gemfireMBeans,mbeanMap,url);
    ops.doJMXValidation();    
  }

  @SuppressWarnings("unchecked")
  private void printEvents() {
    recycleConnections();   
    HydraVector printEventsList = TestConfig.tab().vecAt(JMXPrms.printEventsList);
    boolean exportRegionEvents = false;
    boolean exportDlocknEvents = false;
    for(int i=0;i<printEventsList.size();i++){
      String eventName = (String)printEventsList.get(i);
      if(eventName.contains("REGION"))
        exportRegionEvents = true;
      if(eventName.contains("DLOCK"))
        exportDlocknEvents = true;
      
      //TODO : Add other events when required
    }
    
    if(exportRegionEvents){      
      RegionEvents regionEvents = (RegionEvents) getRegionOperations().getOperationRecorder();
      logInfo("Exporting region events");
      regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    }
    
    if(exportDlocknEvents){
      DLockEvents dlockEvents = (DLockEvents) getDLockOperations().getOperationRecorder();
      logInfo("Exporting dlock events");
      dlockEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    }
    
    JMXBlackboard.getBB().getSharedCounters().increment(JMXBlackboard.VM_TASKS_COMPLETE_COUNT);
    
    String clientName = RemoteTestModule.getMyClientName();
    
    int count = 0;
    if(JMXPrms.useGemfireProxies()){
      Map<String,String> map = FederationBlackboard.getBB().getManagerONs();
      logFine("Federation Manager Map " + map);
      count = map.size();
    }else{
      Map<String,String> map = FederationBlackboard.getBB().getMemberONs();
      count = map.size();
    }
    logInfo("Waiting for even VM to export events to blackboard and updates its counters");
    TestHelper.waitForCounter(JMXBlackboard.getBB(), "TASKS_COMPLETE_COUNT", JMXBlackboard.VM_TASKS_COMPLETE_COUNT, count, true, 10*1000);    
    
    for(int i=0;i<printEventsList.size();i++){
      String eventName = (String)printEventsList.get(i);    
      Map<String,AbstractEvents> emap = AbstractEvents.importFromBlackBoard(eventName, OperationsBlackboard.getBB());
      if(emap!=null){
        for(Map.Entry<String,AbstractEvents> e : emap.entrySet()){
          clientName = e.getKey();
          AbstractEvents ae = e.getValue();
          logInfo("Events from client " + clientName);
          ae.printEvents();
        }
      }else
        logInfo("No events found of type " + eventName);
    }    
  }
  
  private synchronized void initManagerForResourceNotifs() {
    if(managingNodeForNotifs==null){
      String clientName = RemoteTestModule.getMyClientName();
      if(clientName.contains("managing"))
        managingNodeForNotifs = FederationBlackboard.getBB().getMyManagingNode();//TODO This will fail when there will be only one node
      else
        managingNodeForNotifs = FederationBlackboard.getBB().getManagingNode();
    }
  }
  
  
  
  
  private static List<ObjectName> checkTwoLists(List<ObjectName> list1, List<ObjectName> list2) {
    
    List<ObjectName> diffList = new ArrayList<ObjectName>();
    
    for(ObjectName n1 : list1){
      if(!list2.contains(n1))
        diffList.add(n1);
    }
    
    for(ObjectName n2 : list2){
      if(!list1.contains(n2))
        diffList.add(n2);
    }
    
   return diffList;
    
  }  
  
  private void setUpJMXListeners() {
    initManagerForResourceNotifs();
    String clientName = RemoteTestModule.getMyClientName();
    DistributedMember distributedMember = ManagementUtil.getMember();
    if (!clientName.contains("edge")) {
      HydraUtil.sleepForReplicationJMX();
      ObjectName name = ManagementUtil.getLocalMemberMBeanON();
      addMemberNotifListener(name,ManagementUtil.getMemberID());
      
      if(ManagementUtil.subscribeDSNotifs()){
        name = MBeanJMXAdapter.getDistributedSystemName();
        addDSNotifListener(name,ManagementUtil.getMemberID());
      }
      
      if(server!=null){
        ObjectName cacheServerON = service.getCacheServerMBeanName(server.getPort(), distributedMember);
        addCacheServerNotifListener(cacheServerON,ManagementUtil.getMemberID());
      }
    }
  }  
  
  private void _addExpectationForNotification(ObjectName mbeanName, String type, String source, String message,
      int matchMode) {
    if (allNotiflisteners.containsKey(mbeanName)) {      
      logInfo("Adding expectation for Notif Type " + type + " with message " + message + " on mbean " + mbeanName);
      notifExpectationList.add(Expectations.forMBean(mbeanName).expectMBeanAt(managingNodeForNotifs)
          .expectNotification(type, source, message, null, matchMode));      
    } else {
      throw new TestException("No notif listener setup for " + mbeanName);
    }
  }
  
  private void validateNotifications() {
    JMXValidator validator = new JMXValidator(jmxNotifRecorder, notifExpectationList);
    validateAndPrint(validator);
  }

  
  /**
   * Note here that same number and kinds of regions are created on each Bridge
   */
  @SuppressWarnings("rawtypes")
  private void createRegionsOnBridge(){    
    RegionOperations regionOps = getRegionOperations();
    HydraVector regionList = TestConfig.tab().vecAt(JMXPrms.regionListToStartWith);
    logInfo("Creating regions on gemfire bridges : ");
    int count = 1;
    for(int i=0;i<regionList.size();i++){
      String name = (String)regionList.get(i);
      if(name.contains("Bridge")){
        //String regionName = regionOps.createRegion(name);
        int index = name.indexOf("Bridge");
        String rName = "Test" + name.substring(0, index) + "_"+ count++;
        logInfo("Creating region named " + rName + " with template " + name);
        Region regionCreated = regionOps.createRegion(rName, name);
        rName = regionCreated.getFullPath();
        logInfo("Created region named " + rName + " with template " + name);        
        addExpectationForNotification(MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
            ResourceNotification.REGION_CREATED, ManagementUtil.getMemberID(), ResourceNotification.REGION_CREATED_PREFIX+rName);
        if(ManagementUtil.subscribeDSNotifs()){
          addExpectationForNotification(MBeanJMXAdapter.getDistributedSystemName(),
              ResourceNotification.REGION_CREATED, ManagementUtil.getMemberID(), ResourceNotification.REGION_CREATED_PREFIX+rName);
        }
        if(rName.contains("Persistent")){
          String diskStoreName = regionCreated.getAttributes().getDiskStoreName();
          if(diskStoreName==null)
            throw new TestException("Diskstore returned null for persistent region " + rName);
          addExpectationForNotification(MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
              ResourceNotification.DISK_STORE_CREATED, ManagementUtil.getMemberID(), ResourceNotification.DISK_STORE_CREATED_PREFIX+diskStoreName);
          if(ManagementUtil.subscribeDSNotifs()){
          addExpectationForNotification(MBeanJMXAdapter.getDistributedSystemName(),
              ResourceNotification.DISK_STORE_CREATED, ManagementUtil.getMemberID(), ResourceNotification.DISK_STORE_CREATED_PREFIX+diskStoreName);
          }
        }
      }
    }
    RegionEvents regionEvents = (RegionEvents) regionOps.getOperationRecorder();
    regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
  } 

  private void createQueryResources() {    
    CQAndIndexOperations indexOps = getCQOperations();
    QueryService service = cache.getQueryService();
    
    logInfo("Created Index # : " + indexOps.createIndex());
    logInfo("Created Index # : " + indexOps.createIndex());    
    logInfo("Created Index # : " + indexOps.createIndex());
    logInfo("Created Index # : " + indexOps.createIndex());
    logInfo("Created Index # : " + indexOps.createIndex());
    
    String clientName = RemoteTestModule.getMyClientName();
    if(clientName.contains("edge")){
      
      String query = indexOps.createCq();
      logInfo("Created CQ # : " + query);
      try {
        service.executeCqs();
      } catch (CqException e) {     
        throw new TestException("Failed to start cqs ",e);
      }    
      
      query = indexOps.createCq();
      logInfo("Created CQ # : " + query);
      try {
        service.executeCqs();
      } catch (CqException e) {     
        throw new TestException("Failed to start cqs ",e);
      }
      
      
      query = indexOps.createCq();
      logInfo("Created CQ # : " + query);
      try {
        service.executeCqs();
      } catch (CqException e) {     
        throw new TestException("Failed to start cqs ",e);
      }
      
      query = indexOps.createCq();
      logInfo("Created CQ # : " + query);
      try {
        service.executeCqs();
      } catch (CqException e) {     
        throw new TestException("Failed to start cqs ",e);
      }
      
      query = indexOps.createCq();
      logInfo("Created CQ # : " + query);
      try {
        service.executeCqs();
      } catch (CqException e) {     
        throw new TestException("Failed to start cqs ",e);
      }
    }
       
  }
 
  
  private void createRegionsOnEdge() {
    logInfo("Creating region on gemfire clients");
    RegionOperations regionOps = getRegionOperations();
    HydraVector regionList = TestConfig.tab().vecAt(JMXPrms.regionListToStartWith);
    logInfo("Creating regions on gemfire edges : ");
    int count = 1;
    for(int i=0;i<regionList.size();i++){
      String name = (String)regionList.get(i);
      if(name.contains("Edge")){
        int index = name.indexOf("Edge");        
        String rName = "Test" + name.substring(0, index) + "_"+ count++;        
        logInfo("Creating region named " + rName + " with template " + name);
        regionOps.createRegion(rName, name);
        logInfo("Created region named " + rName + " with template " + name);
      }
    }
  }
  
  private void createLockServiceInstances() {
    initManagerForResourceNotifs();
    DLockOperations lockOps = getDLockOperations();
    int locks = TestConfig.tab().intAt(JMXPrms.lockServicesToStartWith);
    logInfo("Creating lockservice instances Num : " + locks);
    for(int i=0;i<locks;i++){
      String lock = lockOps.createDLock();
      
      addExpectationForNotification(
          MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
          ResourceNotification.LOCK_SERVICE_CREATED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.LOCK_SERVICE_CREATED_PREFIX+lock);
      
      if(ManagementUtil.subscribeDSNotifs()){
        addExpectationForNotification(
            MBeanJMXAdapter.getDistributedSystemName(),
            ResourceNotification.LOCK_SERVICE_CREATED, 
            ManagementUtil.getMemberID(), 
            ResourceNotification.LOCK_SERVICE_CREATED_PREFIX+lock);
      }
      
    }
  }

  private void starBridgeServer() {    
    if(server==null){
      String bridgeConfig = TestConfig.tab().stringAt(BridgePrms.names);
      server = BridgeHelper.startBridgeServer(bridgeConfig);      
      logInfo("Started cacheServer on " + RemoteTestModule.getMyClientName());
      logInfo("Has server started " + server.isRunning());
      
      addExpectationForNotification(
          MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
          ResourceNotification.CACHE_SERVER_STARTED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.CACHE_SERVER_STARTED_PREFIX);
      
      if(ManagementUtil.subscribeDSNotifs()){
        addExpectationForNotification(
            MBeanJMXAdapter.getDistributedSystemName(),
            ResourceNotification.CACHE_SERVER_STARTED, 
            ManagementUtil.getMemberID(), 
            ResourceNotification.CACHE_SERVER_STARTED_PREFIX);
      }
    }
  }
  
  private void startWANSendersAndReceivers() {   
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    HydraVector names = TestConfig.tab().vecAt(GatewaySenderPrms.names);
    GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
    for(Object i : names){
      logInfo("Starting GatewaySender " + i);
      GatewaySenderHelper.createAndStartGatewaySenders((String)i);
    }
    
    ObjectName localMemberMBean = ManagementUtil.getLocalMemberMBeanON();
    
    
    notifExpectationList.add(Expectations.
        forMBean(localMemberMBean).
        expectMBeanAt(managingNodeForNotifs).
        expectNotification(
            ResourceNotification.GATEWAY_RECEIVER_CREATED,
            ManagementUtil.getMemberID(), 
            ResourceNotification.GATEWAY_RECEIVER_CREATED_PREFIX,
            null,
            Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE)
      );
    
    notifExpectationList.add(Expectations.
        forMBean(localMemberMBean).
        expectMBeanAt(managingNodeForNotifs).
        expectNotification(
            ResourceNotification.GATEWAY_RECEIVER_STARTED,
            ManagementUtil.getMemberID(), 
            ResourceNotification.GATEWAY_RECEIVER_STARTED_PREFIX,
            null,
            Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE)
      );
    
    //Num Notif Emmited == num of senders
    notifExpectationList.add(Expectations.
      forMBean(localMemberMBean).
      expectMBeanAt(managingNodeForNotifs).
      expectNotification(
          ResourceNotification.GATEWAY_SENDER_CREATED,
          ManagementUtil.getMemberID(), 
          ResourceNotification.GATEWAY_SENDER_CREATED_PREFIX,
          names.size(),
          Expectation.MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT)
    );
    
  }
  
  private void recycleConnections() {
    // This causes recycling of rmi connections if any managing node goes down
    // in previous iteration
    FederationBlackboard.urlsChecked = false;
    FederationBlackboard.getBB().getManagingNodes();
    FederationBlackboard.urlsChecked = true;
  }

  @SuppressWarnings("unchecked")
  private void doJMXOps() {
    
    recycleConnections();
    
    Map<String,TestMBean> mbeanMap = JMXBlackboard.getBB().getMap(JMXBlackboard.MBEAN_MAP);
    String url = FederationBlackboard.getBB().getManagingNode();
    Set<ObjectName> gemfireMBeans = ManagementUtil.getGemfireMBeans(url);
    
    logFine("MBeans " + gemfireMBeans + " found on url " + url);
    logFine("mbeanMap " + mbeanMap);
    JMXOperations ops = buildeTestMap(gemfireMBeans,mbeanMap,url);
    ops.doJMXTest();
  }

  private JMXOperations buildeTestMap(Set<ObjectName> gemfireMBeans, Map<String, TestMBean> mbeanMap, String url) {
    int mbeansVisited = gemfireMBeans.size();
    int testMbeansVisited = mbeanMap.size();
    
    JMXOperations op = new JMXOperations();
    op.addManagingUrl(url);
    
    for(ObjectName n : gemfireMBeans){
      for(String s : mbeanMap.keySet()){
        if(regexAndMemberMatch(n,s)){//template objectName matched
          TestMBean t = mbeanMap.get(s);          
          op.addMbean(n, t);
          logFine("Adding testMBean " + t.getType() + " for objectName " + n);
          testMbeansVisited--;mbeansVisited--;          
        }
      }
    }
    return op;
  }

  private boolean regexAndMemberMatch(ObjectName n, String s) {
    String name = n.toString();
    
    //TODO Need to remove when bug 45643 : Current Management Service allows more than one Federating Manager in DS
    /*
    if(!JMXPrms.useGemfireProxies()){     
      if(name.contains("managing")){
        //filterMBeansOnManagingNodes when running in pure jmx mode
        logInfo("Filtering out managing mbean " + name);
        return false;
      }
    }else{
      //Allow only mbean from current VM. Manager-to-Manager Federation is not happening
      String clientName = RemoteTestModule.getMyClientName();
      if(name.contains("managing") && !name.contains(clientName))
        return false;
    }*/
    
    
    if(s.contains("?")){
      Pattern myPattern = Pattern.compile(s);          
      Matcher match = myPattern.matcher(name);
      boolean matchFound = match.find();
      logFine("RegexMatching templateName " + s + " with string " + name + " result " + matchFound);
      if(matchFound)
        return true;
      else return false;
    }else{
      boolean bothDistributed = true;
      if(n.toString().contains(DISTRIBUTED_SUFFIX)){
        bothDistributed = n.toString().contains(DISTRIBUTED_SUFFIX) && s.contains(DISTRIBUTED_SUFFIX);
      }
      boolean matched = n.toString().contains(s) && bothDistributed; 
      logFine("StringMatching templateName " + s + " with string " + n + " result " + matched);
      return matched;
    }
  }

  private void initialize() {
    
    logInfo("Management Service created : " + service);
    JMXBlackboard.getBB().initBlackBoard();
    
    createCache();
    
    String clientName = RemoteTestModule.getMyClientName();
    checkForLocatorNotification(clientName);
    
    //only managed are members
    if(!clientName.contains("edge") && !clientName.contains("managing") && !clientName.contains("locator")){
       ManagementUtil.saveMemberMbeanInBlackboard();         
    }
    
    if (!clientName.contains("edge") && service == null) {
      service = ManagementService.getManagementService(cache);
      if(service==null)
        service = ManagementService.getExistingManagementService(cache);      
      if(!ManagementUtil.checkLocalMemberMBean())
        throw new TestException("Could not find MemberMbean in platform mbean server");
    }
    
    logInfo("Management Service created : " + service);
    
    if (!clientName.contains("edge") && service != null){
      SystemManagementService mgmtService = (SystemManagementService) service;
      mgmtService.getLocalManager().runManagementTaskAdhoc();
    }
    
    if(!clientName.contains("edge") && service==null)
      throw new TestException("Could not find management service");
    
  }
  
  private void checkForLocatorNotification(String name) {
    //Difficult to simulate because hydra does not allow managed and non-managed locators
    //in the tests.
    
    /*
    if(name.contains("locator")){    
      //TODO : For locator_managing node this might fail becase numNotif = num of locator_managing nodes
      locatorValidator.expectationList.add(Expectations.
          forMBean(locatorValidator.mbean).
          expectMBeanAt(JMXOperations.LOCAL_MBEAN_SERVER).
          expectNotification(            
              ResourceNotification.LOCATOR_STARTED, 
              null, 
              ResourceNotification.LOCATOR_STARTED_PREFIX,
              1,
              Expectation.MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT));
      locatorValidator.validateNotifications();
    }*/
  }

  private void createCache() {
    if (cache == null) {            
      String cacheConfig = TestConfig.tab().stringAt(CachePrms.names);
      if(CacheHelper.getCache()==null)
        cache = (GemFireCacheImpl) CacheHelper.createCache(cacheConfig);
      else cache = (GemFireCacheImpl)CacheHelper.getCache();
      ds = cache.getDistributedSystem();
    }
  }
  
  private void becomeManager() {
    ManagementUtil.saveMemberManagerInBlackboard();
    
    if(service==null)
      throw new TestException("Could not find management service");
    
    if(!service.isManager())
      service.startManager();
    
    HydraUtil.sleepForReplicationJMX();
     
    try {
      ManagementUtil.checkIfThisMemberIsCompliantManager(ManagementFactory.getPlatformMBeanServer());
    } catch (IOException e) {
      throw new TestException("Error connecting manager", e);
    } catch (TestException e) {
      throw e; 
    }
    
    logInfo("JMX Manager Blackboard ");
    JMXManagerBlackboard.getInstance().print();    
    
    logInfo("JMX Manager Endpoints  " + HydraUtil.ObjectToString(JMXManagerHelper.getEndpoints()));       
  } 

}

