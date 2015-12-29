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
package management.test.federation;

import static management.util.HydraUtil.logError;
import static management.util.HydraUtil.logErrorAndRaiseException;
import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;
import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.ClientVmInfo;
import hydra.HydraVector;
import hydra.JMXManagerBlackboard;
import hydra.JMXManagerHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import management.Expectations;
import management.jmx.Expectation;
import management.jmx.JMXBlackboard;
import management.jmx.JMXNotificationListener;
import management.jmx.SimpleJMXRecorder;
import management.jmx.validation.JMXValidator;
import management.jmx.validation.ValidationResult;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import splitBrain.SBUtil;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;

/**
 * 
 * <b>Start Tasks</b>
 * 
 * 
 * <b>Init Tasks</b>
 * <ul>
 * <li>HydraInitTask_initialize : Generic Init Method</li>
 * <li>HydraInitTask_becomeManager : Kicks-start federation of all other managed
 * nodes, in this VM</li>
 * <li>HydraInitTask_RegisterMBeans : Registers MBeans as specified by
 * FedPrms-mbeanInitSet</li>
 * </ul>
 * 
 * <b>Tasks</b>
 * 
 * <ul>
 * <li>HydraTask_RegisterMBean : Registers A MBean from FedPrms-mbeanSet
 * with unique ObjectName and verifies that same is proxied on Managing
 * Node</li>
 * <li>HydraTask_UnRegisterMBean : UnRegisters A MBean verifies that same is
 * not-proxied
 * on Managing Node</li>
 * <li>HydraTask_RestartManaged : Restart a random
 * managed node. Checks for notification</li>
 * <li>HydraTask_RestartManaging : Restart a random managing node.
 * After restart, all current MBeanSet is verified is proxied correctly or
 * not</li>
 * <li>HydraTask_becomeManager : Trigger Federation on this
 * node if not already federating. Check MBeanSet</li>
 * <li>HydraTask_performMbeanStateOperations : Perform JMX operation to modify
 * mbean
 * State</li>
 * 
 * <li>HydraTask_generateNotifications : This task call an operation which
 * generated notification.
 * Successful execution adds an expectation for notification to validated
 * later</li>
 * <li>HydraTask_doJMXOperations : This task call an operation on mbean and
 * saves value in expectation
 * which is later used to validate. Operation returns vmId where it runs</li>
 * 
 * </ul>
 * 
 * <b>Close Tasks</b>
 * 
 * <ul>
 * <li>HydraCloseTask_verifyMBeanSet : Verify MBeanSet currently recorded in
 * BlackBoard </li>
 * <li>HydraCloseTask_verifyMBeanState : Verify mbean state modified by
 * operations </li>
 * <li>HydraCloseTask_verifyMBeanOperationsAndNotifications</li>
 * </ul>
 * 
 * <b>Stop Tasks</b>
 * 
 * 
 * @author tushark
 * 
 */
public class FederationTest {

  private static final int ATTR_STATE_OP_INCREMENT_TOGETHER = 1;
  private static final int ATTR_STATE_OP_DECREMENT_TOGETHER = 2;
  private static final int ATTR_STATE_OP_MODIFY_DEPENDING = 3;
  private static final int ATTR_STATE_OP_MIRRORING = 4;

  private static FederationTest testInstance;

  public synchronized static void HydraInitTask_initialize() {
    if (testInstance == null) {
      testInstance = new FederationTest();
      testInstance.initialize();
    }
  }

  public static void HydraInitTask_becomeManager() {
    testInstance.becomeManager();
  }

  public static void HydraInitTask_RegisterMBeans() {
    testInstance.initMBeanSet();
  }

  public static void HydraInitTask_InitFedBB() {
    // Stores PID Into Blackboard
    testInstance.initFB();
  }

  public static void HydraTask_RegisterMBean() {
    testInstance.registerMBean();
  }

  public static void HydraTask_UnRegisterMBean() {
    testInstance.unregisterMBean();
  }

  public static void HydraTask_RestartManaged() {
    testInstance.restartRandomManagedNode();
  }

  public static void HydraTask_RestartManaging() {
    testInstance.restartRandomManagingNode();
  }

  public static void HydraTask_becomeManager() {
    testInstance.becomeManager();
  }

  public static void HydraTask_performMbeanStateOperations() {
    testInstance.performMbeanStateOperations();
  }

  public static void HydraTask_generateNotifications() {
    testInstance.generateNotifications();
  }

  public static void HydraTask_doJMXOperations() {
    testInstance.doJMXOperations();
  }

  public static void HydraTask_doSickMember() {
    testInstance.doSickMember();
  }

  public static void HydraCloseTask_verifyMBeanSet() {
    testInstance.verifyMBeanSet();
  }

  public static void HydraCloseTask_verifyMBeanState() {
    testInstance.verifyMBeanState();
  }

  public static void HydraCloseTask_verifyMBeanOperationsAndNotifications() {
    testInstance.verifyMBeanOperationsAndNotifications();
  }

  private AtomicInteger notificationSequenceCounter = new AtomicInteger();
  private long waitTime;
  private DistributedSystem ds = null;
  private GemFireCacheImpl cache = null;
  private ManagementService service;
  private Map<String, Class> klassLoaded = new HashMap<String, Class>();
  private Set<ObjectName> localMbeans = new HashSet<ObjectName>();
  private Set<ObjectName> unregisteredMbeans = new HashSet<ObjectName>();
  private Map<ObjectName, String> mbeanToOpMapping = new HashMap<ObjectName, String>();
  private Map<ObjectName, AtomicInteger> mbeanOpCountMapping = new HashMap<ObjectName, AtomicInteger>();
  private Map<ObjectName, JMXNotificationListener> notifListenerMap = new HashMap<ObjectName, JMXNotificationListener>();

  /*-
   * Below linking of object is bit confusing at first. Below diagram depicts
   * the actual linkages
   * 
   * MBean1 ----|----> JMXListener -----| 
   * MBean2 ----|----> JMXListener -----|---> EventRecorder ----> Validator <---- JMX Expectation <----- Hydra Tasks |
   * MBean3 ----|----> JMXListener -----|                           |  
   *                                                    HydraCloseTasks(validator.validate())
   */

  private List<Expectation> globalExpectationList = new ArrayList<Expectation>();

  private SimpleJMXRecorder jmxEventRecorder = new SimpleJMXRecorder();

  private void initialize() {
    waitTime = TestConfig.tab().longAt(FederationPrms.taskWaitTime);
    createCache();
    if (service == null) {
      service = ManagementService.getManagementService(cache);
    }
  }

  private void createCache() {
    if (cache == null) {
      String cacheConfig = TestConfig.tab().stringAt(CachePrms.names);
      cache = (GemFireCacheImpl) CacheHelper.createCache(cacheConfig);
    }
  }

  private void becomeManager() {
    ManagementUtil.saveMemberManagerInBlackboard();
    if(!service.isManager())
      service.startManager();    
    HydraUtil.sleepForReplicationJMX();
    logInfo("JMX Manager Blackboard ");
    JMXManagerBlackboard.getInstance().print();    
    
    logInfo("JMX Manager Endpoints  " + HydraUtil.ObjectToString(JMXManagerHelper.getEndpoints()));
    
    /*String connector = TestConfig.tab().stringAt(FederationPrms.rmiConnectorType);
    if ("custom".equals(connector)) {
      ManagementUtil.startRmiConnector();
    }*/
  }

  /**
   * Reads mbeanSet vector which list of classes which will get registered as
   * mbeans. Loads the classes, finds out the interfaces it implements Creates
   * ObjectName as combination of Template name returned by method and global
   * Counter After registering it will put the resultant objectName into
   * Blackboard
   */
  private void initMBeanSet() {
    HydraVector mbeans = TestConfig.tab().vecAt(FederationPrms.mbeanInitSet);
    Iterator iterator = mbeans.iterator();

    while (iterator.hasNext()) {
      String klass = (String) iterator.next();
      createMbeanForClass(klass);
    }

  }

  private void createMbeanForClass(String klass) {
    Class kklass = null;
    if (!klassLoaded.containsKey(klass)) {
      try {
        kklass = Class.forName(klass);
        klassLoaded.put(klass, kklass);
      } catch (Exception e) {
        logErrorAndRaiseException("Error loading class " + klass, e);
      }
    } else {
      kklass = klassLoaded.get(klass);
    }
    Class[] interfaces = kklass.getInterfaces();
    if (interfaces.length > 1) {
      HydraUtil
          .logErrorAndRaiseException("Mbean implementation has more than one interfaces dont know which one to use");
    }
    Class interfaceClass = null;

    boolean notificationEmitting = false;

    for (Class kl : interfaces) {
      if (kl.getName().contains("Custom")) {
        interfaceClass = kl;
        break;
      }
    }

    for (Class kl : interfaces) {
      if (kl.getName().contains("NotificationBroadcaster")) {
        notificationEmitting = true;
        logFine((klass + " MBean is configured for Notification emitting."));
      }
    }
    Class superclasses = kklass.getSuperclass();
    if (superclasses.getName().contains("NotificationBroadcaster")) {
      notificationEmitting = true;
      logFine((klass + " MBean is configured for Notification emitting."));
    }

    Object mbeanImpl = null;
    try {
      mbeanImpl = kklass.newInstance();
    } catch (Exception e) {
      HydraUtil.logErrorAndRaiseException("Cant create Mbean object of " + klass);
    }
    int objectNameCount = FederationBlackboard.getBB().getNextObjectNameCounter();
    String templateName = getTemplateObjectName(mbeanImpl, interfaceClass) + objectNameCount;
    // notificationEmitting = true; //harcoded all sample b
    ObjectName changedObjectName = registerMbean(mbeanImpl, interfaceClass, true, templateName, notificationEmitting);
    synchronized (localMbeans) {
      localMbeans.add(changedObjectName);
    }
    // TODO : Record it inside the Blackboard
  }

  private ObjectName registerMbean(Object mbeanImpl, Class interfaceClass, boolean federate, String objectName,
      boolean isNotificationEmitter) {
    ObjectName mbeanName = null;
    try {
      mbeanName = new ObjectName(objectName);
    } catch (MalformedObjectNameException e) {
      logErrorAndRaiseException("Failed to create objectName with " + objectName, e);
    } catch (NullPointerException e) {
      logErrorAndRaiseException("Failed to create objectName with " + objectName, e);
    }
    ObjectName changedObjectName = service.registerMBean(mbeanImpl, mbeanName);
    if (federate) {
      service.federate(changedObjectName, interfaceClass, isNotificationEmitter);
      if (isNotificationEmitter && registerForNotifications()) {
        JMXNotificationListener listner = new JMXNotificationListener(FederationTest.class.getName(), 
            jmxEventRecorder, changedObjectName);
        notifListenerMap.put(changedObjectName, listner);
        try {
          getPlatformMbeanServer().addNotificationListener(changedObjectName, listner, null, null);
        } catch (InstanceNotFoundException e) {
          logErrorAndRaiseException("Failed registed listner on " + objectName, e);
        }
      }
    }
    return changedObjectName;
  }

  private MBeanServer getPlatformMbeanServer() {
    return ManagementFactory.getPlatformMBeanServer();
  }

  private boolean registerForNotifications() {
    return TestConfig.tab().booleanAt(FederationPrms.registerForNotifications);
  }

  private String getTemplateObjectName(Object mbeanImpl, Class interfaceClass) {
    Class[] ptypes = {};
    try {
      Method m = interfaceClass.getMethod("getTemplateObjectName", ptypes);
      String str = (String) m.invoke(mbeanImpl, (java.lang.Object[])null);
      return str;
    } catch (SecurityException e) {
      logErrorAndRaiseException("Cant get template object name", e);
    } catch (NoSuchMethodException e) {
      logErrorAndRaiseException("Cant get template object name", e);
    } catch (IllegalArgumentException e) {
      logErrorAndRaiseException("Cant get template object name", e);
    } catch (IllegalAccessException e) {
      logErrorAndRaiseException("Cant get template object name", e);
    } catch (InvocationTargetException e) {
      logErrorAndRaiseException("Cant get template object name", e);
    }
    return null;
  }

  private void registerMBean() {
    String klassName = TestConfig.tab().stringAt(FederationPrms.mbeanSet);
    createMbeanForClass(klassName);

    sleepForReplication();
  }

  private void unregisterMBean() {

    synchronized (localMbeans) {
      int size = localMbeans.size();
      logInfo("Current number of mbean registered " + size);
      if (size == 1) {
        logInfo("Only one mbean left so returning without unregistering any mbean");
        return;
      }
      int r = TestConfig.tab().getRandGen().nextInt(size - 1);
      Iterator<ObjectName> iterator = localMbeans.iterator();
      ObjectName n = null;
      for (int i = 0; i <= r; i++) {
        n = iterator.next();
        if (i == r)
          break;
      }
      logInfo("Unregistering mbean with objectName " + n);
      service.unregisterMBean(n);
      logInfo("Unreigstered MBean " + n);
      clearLocalMBeanState(n);
      unregisteredMbeans.add(n);
    }

    sleepForReplication();
  }

  private void clearLocalMBeanState(ObjectName n) {
    localMbeans.remove(n);
    mbeanOpCountMapping.remove(n);
    mbeanToOpMapping.remove(n);
    notifListenerMap.remove(n);
  }

  @SuppressWarnings("rawtypes")
  private void restartRandomManagedNode() {
    Vector clientNames = TestConfig.tab().vecAt(hydra.ClientPrms.names, null);
    List<String> list = new ArrayList<String>();
    Iterator iterator = clientNames.iterator();
    while (iterator.hasNext()) {
      String name = (String) iterator.next();
      if (name.contains("managed"))
        list.add(name);
    }
    int numVMsToStop = 1 + TestConfig.tab().getRandGen().nextInt(list.size() - 1);
    List<ClientVmInfo> vmList = null;
    Set<ObjectName> oldMbeans = new HashSet<ObjectName>();
    if (numVMsToStop > 0) {
      Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "managed");
      vmList = (List) (tmpArr[0]);
      List stopModeList = (List) (tmpArr[1]);
      for (ClientVmInfo cInfo : vmList) {
        String clientName = cInfo.getClientName();
        int vmId = cInfo.getVmid();
        String pidPrefix = "MEMBERNAME_" + clientName + vmId;
        String memberId = (String) FederationBlackboard.getBB().getSharedMap().get(pidPrefix);
        oldMbeans.addAll(getMbeansFor(memberId));
      }
      logInfo("MbeanSet before restart of nodes " + vmList + " is -> " + HydraUtil.ObjectToString(oldMbeans));
      logInfo("Restarting " + numVMsToStop + " managed nodes " + " vmList " + vmList
          + " corresponding managing stop Modes " + stopModeList);
      List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);
      StopStartVMs.joinStopStart(vmList, threads);
      logInfo("Completed restart of " + numVMsToStop + " managed nodes. Checking for new mbeans");
      //sleep for 20 seconds for suspect verification, registration of new vms and new mbean propogation
      sleepForReplication();
      sleepForReplication();
      sleepForReplication();
    }

    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    Set<ObjectName> unwantedMBeans = new HashSet<ObjectName>();
    for (String managingUrl : urls) {
      logInfo("Checking MbeanSet at " + managingUrl + " for old mbeans :" + HydraUtil.ObjectToString(oldMbeans));
      MBeanServerConnection remoteMBS = null;
      try {
        remoteMBS = ManagementUtil.connectToUrl(managingUrl);
      } catch (MalformedURLException e) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } catch (IOException e) {
        if (HydraUtil.isSerialTest()) {
          logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
        } else {
          logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        }
      }
      try {
        Set<ObjectName> objectNames = remoteMBS.queryNames(null, null);
        for (ObjectName n : objectNames) {
          for (Object oldname : oldMbeans) {
            if (oldname.equals(n))
              unwantedMBeans.add(n);
          }

        }

        if (unwantedMBeans.size() > 0) {
          throw new TestException("Found " + unwantedMBeans.size() + " old mbeans " + unwantedMBeans + " after restart of managed nodes "
              + vmList );
        }
      } catch (IOException e) {
        throw new TestException("Error trying query connector at " + managingUrl, e);
      }
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  private Collection<? extends ObjectName> getMbeansFor(String memberId) {
    String managingUrl = FederationBlackboard.getBB().getManagingNode();
    MBeanServerConnection remoteMBS = null;
    List<ObjectName> list = new ArrayList<ObjectName>();
    try {
      remoteMBS = ManagementUtil.connectToUrl(managingUrl);
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
      }
    }

    try {
      Set<ObjectName> objectNames = remoteMBS.queryNames(null, null);
      for (ObjectName n : objectNames) {
        if (n.toString().contains(memberId))
          list.add(n);
      }
      return list;
    } catch (IOException e) {
      throw new TestException("Error trying query connector at " + managingUrl, e);
    }
  }

  private void restartRandomManagingNode() {
    int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
    if (numVMsToStop > 0) {
      Object[] tmpArr = StopStartVMs.getOtherVMs(numVMsToStop, "managing_1");
      List vmList = (List) (tmpArr[0]);
      List stopModeList = (List) (tmpArr[1]);
      logInfo("Restarting " + numVMsToStop + " managing nodes " + " vmList " + vmList
          + " corresponding managing stop Modes " + stopModeList);
      List threads = StopStartVMs.stopStartAsync(vmList, stopModeList);
      StopStartVMs.joinStopStart(vmList, threads);
    }
  }

  private void performMbeanStateOperations() {
    int size = localMbeans.size();
    logInfo("Current number of mbean registered " + size);
    int r = TestConfig.tab().getRandGen().nextInt(size - 1);
    Iterator<ObjectName> iterator = localMbeans.iterator();
    ObjectName n = null;
    for (int i = 0; i <= r; i++) {
      n = iterator.next();
      if (i == r)
        break;
    }

    String opName = mbeanToOpMapping.get(n);
    if (opName == null) {
      int opCode = TestConfig.tab().getRandGen().nextInt(3) + 1;
      switch (opCode) {
      case ATTR_STATE_OP_INCREMENT_TOGETHER:
        opName = "incrementCountersTogether";
        break;
      case ATTR_STATE_OP_DECREMENT_TOGETHER:
        opName = "decrementCountersTogether";
        break;
      case ATTR_STATE_OP_MODIFY_DEPENDING:
        opName = "changeBwhenAisEven";
        break;
      case ATTR_STATE_OP_MIRRORING:
        opName = "mirrorCounters";
        break;
      }
      mbeanToOpMapping.put(n, opName);
      mbeanOpCountMapping.put(n, new AtomicInteger());
    }
    logInfo("Performing attribute modification operation on mbean with objectName " + n + " Operation Name " + opName);
    doJmxOperation(n, opName, null, null, null, true);    
    sleepForReplication();
  }

  private void sleepForReplication() {
    HydraUtil.sleepForReplicationJMX();
    /*try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
    }*/
  }

  private void doJMXOperations() {
    ObjectName mbean = getRandomLocalMbean();
    String url = FederationBlackboard.getBB().getManagingNode();
    String operationName = "doJmxOp";
    Object a = doJmxOperation(mbean, operationName, url, null, null, false);
    Object b = "vmId" + RemoteTestModule.getMyVmid();
    
    logFine("Result of operation : " + operationName + " on mbean " + mbean + " is " + a);

    if (a == null) {
      logErrorAndRaiseException("Error during jmx operations returned null whereas expected value was " + b);
    }

    if (!"EXPECTED_EXCPTION".equals(a)) {
      jmxEventRecorder.addJmxOp(mbean, operationName, a);
      Expectation exp = Expectations.forMBean(mbean).expectMBeanAt(url).expectOperation(operationName, b);
      globalExpectationList.add(exp);
    }

  }

  private void generateNotifications() {
    ObjectName mbean = getRandomLocalMbean();
    String url = FederationBlackboard.getBB().getManagingNode();
    String operationName = "sendNotificationToMe";
    String arugment = "seq" + notificationSequenceCounter.incrementAndGet();
    Object[] params = { arugment };
    String[] signature = { "java.lang.String" };
    Object a = doJmxOperation(mbean, operationName, url, params, signature, false);
    // jmxEventRecorder.addJmxOp(mbean, operationName, a);
    if (!(a instanceof String && a.equals("EXPECTED_EXCPTION"))) {
      Expectation exp = Expectations
          .forMBean(mbean)
          .expectMBeanAt(url)
          .expectNotification("jmx.attribute.change", RemoteTestModule.getMyVmid(), arugment, null,
              Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE);
      globalExpectationList.add(exp);
    }
  }

  private Object doJmxOperation(ObjectName n, String opName, String managingUrl, Object[] params, String[] signature,
      boolean record) {
    if (managingUrl == null) {
      Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
      int r = TestConfig.tab().getRandGen().nextInt(urls.size() - 1);
      Iterator<String> iterator = urls.iterator();
      String murl = null;
      for (int i = 0; i <= r; i++) {
        murl = iterator.next();
        if (i == r)
          break;
      }
      managingUrl = murl;
    }

    MBeanServerConnection remoteMBS = null;
    try {
      remoteMBS = ManagementUtil.connectToUrl(managingUrl);
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        return "EXPECTED_EXCPTION";
      }
    }
    Object returnValue = null;
    try {
      logInfo("Invoking operation " + opName + " on " + n + " hosted in manging node " + managingUrl);
      returnValue = remoteMBS.invoke(n, opName, params, signature);
      if (record)
        mbeanOpCountMapping.get(n).incrementAndGet();
      logInfo("Successfully completed operation " + opName + " on " + n + " hosted in manging node " + managingUrl
          + " result " + returnValue);
      return returnValue;
    } catch (InstanceNotFoundException e) {
      if (HydraUtil.isSerialTest())
        logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + n + " on managing node "
            + managingUrl, e);
      else {
        logInfo("Continuing test expected execption " + e.getMessage() + " for mbean " + n);
        return "EXPECTED_EXCPTION";
      }
    } catch (MBeanException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + n + " on managing node "
          + managingUrl, e);
    } catch (ReflectionException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + n + " on managing node "
          + managingUrl, e);
    } catch (IOException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + n + " on managing node "
          + managingUrl, e);
    }
    return returnValue;
  }

  private void doSickMember() {
    /*
     * beSick setAttribute waitForTime getAttribute expectation
     */
    ObjectName mbean = getRandomLocalMbean();
    String managingUrl = FederationBlackboard.getBB().getManagingNode();

    SBUtil.beSick();
    sleepForReplication();

    MBeanServerConnection remoteMBS = null;
    try {
      remoteMBS = ManagementUtil.connectToUrl(managingUrl);
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
      }
    }

    long timestamp;
    try {
      remoteMBS.setAttribute(mbean, new Attribute("counter", 99));
      timestamp = System.currentTimeMillis();
    } catch (InstanceNotFoundException e) {
      if (HydraUtil.isConcurrentTest()) {
        logInfo("Expected exception in concurrent test continuing test . " + e.getMessage());
      } else
        logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);

    } catch (AttributeNotFoundException e) {
      logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);
    } catch (InvalidAttributeValueException e) {
      logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);
    } catch (MBeanException e) {
      logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);
    } catch (ReflectionException e) {
      logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isConcurrentTest()) {
        logInfo("Expected exception in concurrent test continuing test . " + e.getMessage());
      } else
        logErrorAndRaiseException("Error trying to set attribute " + managingUrl, e);
    }

    sleepForReplication();
    Expectation exp = Expectations.forMBean(mbean).expectMBeanAt(managingUrl)
        .expectAttribute("counter", ManagementConstants.UNDEFINED);
    List<Expectation> list = new ArrayList<Expectation>();
    list.add(exp);
    JMXValidator validator = new JMXValidator(JMXBlackboard.getBB(), list);
    validateAndPrint(validator);
    sleepForReplication();
    SBUtil.beHealthy();
    sleepForReplication();
  }

  private void verifyMBeanOperationsAndNotifications() {

    List<Expectation> removedList = new ArrayList<Expectation>();
    for (ObjectName n : unregisteredMbeans) {

      for (Expectation e : globalExpectationList) {
        if (e.getObjectName().equals(n)) {
          removedList.add(e);
        }
      }
    }
    globalExpectationList.removeAll(removedList);
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    for (String url : urls) {
      logInfo("Checking Global Expectation Set at  " + url);
      JMXValidator validator = new JMXValidator(jmxEventRecorder, globalExpectationList);
      validateAndPrint(validator);
    }
  }

  private void verifyMBeanSet() {
    // Verify localMbeans present on Managing Node
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    for (String url : urls) {
      logInfo("Checking MBeanSet at Managing Node Url " + url);
      List<Expectation> list = new ArrayList<Expectation>();
      logInfo("Checking mbeanSet " + localMbeans);
      for (ObjectName name : localMbeans) {
        Expectation exp = Expectations.forMBean(name).expectMBeanAt(url);
        list.add(exp);
      }

      JMXValidator validator = new JMXValidator(JMXBlackboard.getBB(), list);
      validateAndPrint(validator);

      // Verify other mbeans are properly unregistered
      MBeanServerConnection remoteMBS = null;
      try {
        remoteMBS = ManagementUtil.connectToUrl(url);
      } catch (MalformedURLException e) {
        throw new TestException("Error trying to managing node at " + url, e);
      } catch (IOException e) {
        throw new TestException("Error trying to managing node at " + url, e);
      }
      try {
        Set<ObjectName> objectNames = remoteMBS.queryNames(null, null);
        List<ObjectName> unwantedMBeans = new ArrayList<ObjectName>();
        for (ObjectName n : objectNames) {
          if (unregisteredMbeans.contains(n)) {
            unwantedMBeans.add(n);
          }
        }
        if (unwantedMBeans.size() > 0) {
          throw new TestException("Found " + unwantedMBeans.size() + " mbeans which are already unregistered "
              + unwantedMBeans);
        }
      } catch (IOException e) {
        throw new TestException("Error trying query connector at " + url, e);
      }
    }
  }

  private void verifyMBeanState() {
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    for (String url : urls) {
      logInfo("Checking MBeanSet at Managing Node Url " + url);
      List<Expectation> list = new ArrayList<Expectation>();
      logInfo("Checking mbeanSet " + localMbeans);
      for (ObjectName name : localMbeans) {
        if (mbeanToOpMapping.containsKey(name)) {
          Expectation exp = Expectations.forMBean(name).expectMBeanAt(url);
          Object[] aAndB = getAandBFor(name);
          exp.expectAttribute("A", aAndB[0]).expectAttribute("B", aAndB[1]);
          list.add(exp);
        }
      }
      JMXValidator validator = new JMXValidator(jmxEventRecorder, list);
      validateAndPrint(validator);
    }
  }

  private Integer[] getAandBFor(ObjectName name) {
    int a = 0, b = 0;
    String op = mbeanToOpMapping.get(name);
    if (op == null) {
      logErrorAndRaiseException("Did not find operation entry for " + name);
    }
    int count = mbeanOpCountMapping.get(name).get();
    logInfo("Operation " + op + " was performed on " + name + " for " + count + " times");
    if ("incrementCountersTogether".equals(op)) {
      for (int i = 0; i < count; i++) {
        a++;
        b++;
      }
    } else if ("decrementCountersTogether".equals(op)) {
      for (int i = 0; i < count; i++) {
        a--;
        b--;
      }
    } else if ("changeBwhenAisEven".equals(op)) {
      for (int i = 0; i < count; i++) {
        if ((a % 2) == 0) {
          b++;
        } else
          b--;
      }
    } else if ("mirrorCounters".equals(op)) {
      for (int i = 0; i < count; i++) {
        a++;
        b--;
      }
    }
    Integer[] array = { a, b };
    return array;
  }

  private ObjectName getRandomLocalMbean() {
    int size = localMbeans.size();
    logInfo("Current number of mbean registered " + size);
    int r = TestConfig.tab().getRandGen().nextInt(size - 1);
    Iterator<ObjectName> iterator = localMbeans.iterator();
    ObjectName n = null;
    for (int i = 0; i <= r; i++) {
      n = iterator.next();
      if (i == r)
        break;
    }
    return n;
  }
  
  private void validateAndPrint(JMXValidator validator) {
    List<ValidationResult> result = validator.validate();
    if (result.size() > 0) {
      logInfo("JMX Validations failed. See below details");
      StringBuilder sb = new StringBuilder();
      int i=1;
      for (ValidationResult r : result) {       
        HydraUtil.logError(r.toString());
        sb.append(i++ + "# " + r.toString());
      }
      throw new TestException(result.size() + " JMX validation failed \n" + sb.toString());
    } else {
      logInfo("JMX Validations suceeded");
    }
  }
  
  private void initFB() {
    String pidPrefix = "MEMBERNAME_" + RemoteTestModule.getMyClientName() + RemoteTestModule.getMyVmid();
    String formattedMemberID = MBeanJMXAdapter.getMemberNameOrId(InternalDistributedSystem
        .getConnectedInstance().getDistributedMember());
    logInfo("Storing member Name against vmId " + pidPrefix + " memberId " + formattedMemberID);
    FederationBlackboard.getBB().getSharedMap().put(pidPrefix, formattedMemberID);
  }

}
