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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;
import management.jmx.Expectation;
import management.jmx.JMXNotificationListener;
import management.jmx.SimpleJMXRecorder;
import management.jmx.validation.JMXValidator;
import management.operations.ops.JMXOperations;
import management.test.jmx.JMXTest;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import util.TestException;
import util.TestHelper;

import static management.util.HydraUtil.getRandomElement;
import static management.util.HydraUtil.logErrorAndRaiseException;
import static management.util.HydraUtil.logInfo;


/**
 * 
 * Base class for all MBean Test Classes
 * Contains method for reading Mbean test descriptors
 * Notification Expectation Methods for all gemfire resource notifications
 * 
 * @author tushark
 */

@SuppressWarnings("rawtypes")
public abstract class AbstractTestMBean<T> implements TestMBean{  
  private static final long serialVersionUID = 1L;
  
  public static final String TOKEN_TYPE = "type";
  public static final String TOKEN_CLASS = "class";
  public static final String TOKEN_ATRRIBUTES = "attributes";
  public static final String TOKEN_OPERATIONS =  "operations";
  public static final String TOKEN_ON = "templateObjectName";
  private static final String TOKEN_TEST = "tests";
  
  public static final String[] gemfireDefinedMBeanTypes = {"member", "cacheServer", "region", "lockService", 
        "diskStore", "gatewaySender", "gatewayReceiver", "locator", "manager", "distributedSystem", 
        "distributedRegion", "distributedLockService", "memberGroup" , "pulse"
  };
  
  public static final int MemberMBean = 0;
  public static final int CacheServerMBean = 1;
  public static final int RegionMBean = 2;
  public static final int LockServiceMBean = 3;
  public static final int DiskStoreMBean = 4;
  public static final int GatewaySenderMBean = 5;
  public static final int GatewayReceiverMBean = 6;
  public static final int LocatorMBean = 7;
  public static final int ManagerMBean = 8;
  public static final int DistributedSystemMBean = 9;
  public static final int DistributedRegionMBean = 10;
  public static final int DistributedLockServiceMBean = 11;
  public static final int MemberGroupMBean = 12;
  public static final int PULSEMBean = 13;
  
  
  protected String[] attributes;
  protected Object[][] ops;
  protected String templateObjectName;
  protected String[] tests;
  protected static String prefix;
  
  
  public AbstractTestMBean(List<String> attrs, List<Object[]> ops, Class klass, String templateObjectName, String[] tests) {
    this.attributes = new String[attrs.size()];
    int i=0;
    for(String s : attrs)
      this.attributes[i++] = s;
    
    i=0;
    this.ops = new Object[ops.size()][];
    for(Object[] op : ops){
      this.ops[i++] = op;
    }
    this.templateObjectName = templateObjectName;
    this.tests = tests;
    validateOps(this.ops,klass);
    validateAttr(this.attributes, klass);
  }
  
  
  private void validateAttr(String[] attributes2, Class klass) {
    //TODO Validate configured bean against the class
    
  }


  private void validateOps(Object[][] ops2, Class klass) {
  //TODO Validate configured bean against the class
    
  }


  public String[] getAttributes() {
    return attributes;
  }

  
  public Object[][] getOperations() {
    return ops;
  }


  /**
   * Reads mbean descriptor for conf file.
   * 
   * Currently only supports descriptor not Mbean class file parsing
   * 
   * In final version only mentioning type will suffice, class parsing will
   * include all things related.
   * 
   * 
   * @param mbeanDesc
   * 
   */
  public static TestMBean parseMBeanDescriptor(String mbeanDesc) {
    JSONObject json = null;

    TestMBean testmbean = null;
     try {
        json = new JSONObject(mbeanDesc);
        String mbeanClass = json.getString(TOKEN_CLASS);
        String mbeanType = json.getString(TOKEN_TYPE);
        String templateObjectName = json.getString(TOKEN_ON);
        //JSONArray attributes = json.getJSONArray(TOKEN_ATRRIBUTES);
        //JSONArray operations = json.getJSONArray(TOKEN_OPERATIONS);
        String tests[] = getStringArray(json.getJSONArray(TOKEN_TEST));
        
        Class klass =  Class.forName(mbeanClass);
        Method[] ms = klass.getDeclaredMethods();
        
        List<String> attrs = new ArrayList<String>();
        /*for(int i=0;i<attributes.length();i++){
          String attr = (String) attributes.get(i);
          attrs.add(attr);
        }*/
        
        List<Object[]> ops = new ArrayList<Object[]>();
        /*for(int i=0;i<operations.length();i++){
          String name = (String) operations.get(i); //o.getString("name");
          String returnType = null;
          Method thisMethod = null;
          for(Method m : ms)
            if(m.getName().equals(name))
              thisMethod = m;
          
          String signatureArray[] = arrayToString(thisMethod.getParameterTypes());
          returnType = thisMethod.getReturnType().getName();
          Object method[] = {name,returnType,signatureArray}; 
          ops.add(method);
        }*/        
        testmbean = getTestMbean(mbeanType,attrs,ops,templateObjectName,tests);
        return testmbean;
        
     }catch(JSONException e){
       throw new TestException(TestHelper.getStackTrace(e));
     } catch (ClassNotFoundException e) {
       throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  /*
  private static String[] arrayToString(Class<?>[] parameterTypes) {
    String s[] = new String[parameterTypes.length];
    int i=0;
    for(Class k : parameterTypes)
      s[i++] = k.getName();
    return s;
  }*/

  private static String[] getStringArray(JSONArray jsonArray) throws JSONException {
    int length = jsonArray.length();
    String array[] = new String[length];
    for(int i=0;i<length;i++)
      array[i] = (String) jsonArray.get(i);
    return array;
  }

  private static TestMBean getTestMbean(String mbeanType, List<String> attrs, List<Object[]> ops, String ton, String[] tests) {    
    boolean found = false;
    int ordinal = 0;
    for(int i=0;i<gemfireDefinedMBeanTypes.length;i++)
      if(mbeanType.equals(gemfireDefinedMBeanTypes[i])){
        found = true;ordinal = i;break;
      }
    if(!found)
      throw new TestException("Unknown mbean type in mbean descriptor " + mbeanType);
    
    TestMBean testMBean = null;
    
    switch(ordinal){
      case MemberMBean :
        testMBean = new MemberTestMbean(attrs,ops,ton,tests);break;
      case CacheServerMBean :
        testMBean = new CacheServerTestMBean(attrs,ops,ton,tests);break;
      case RegionMBean :
        testMBean = new RegionTestMBean(attrs,ops,ton,tests);break;
      case LockServiceMBean :
        testMBean = new LockServiceTestMBean(attrs,ops,ton,tests);break;
      case DiskStoreMBean :
        testMBean = new DiskStoreTestMBean(attrs,ops,ton,tests);break;
      case GatewaySenderMBean :
        testMBean = new GatewaySenderTestMBean(attrs,ops,ton,tests);break;
      case GatewayReceiverMBean :
        testMBean = new GatewayReceiverTestMBean(attrs,ops,ton,tests);break;
      case LocatorMBean :
        testMBean = new LocatorTestMBean(attrs,ops,ton,tests);break;      
      case DistributedSystemMBean :
        testMBean = new DistributedSystemTestMBean(attrs,ops,ton,tests);break;
      case DistributedRegionMBean :
        testMBean = new DistributedRegionTestMBean(attrs,ops,ton,tests);break;
      case DistributedLockServiceMBean :
        testMBean = new DistributedLockServiceTestMBean(attrs,ops,ton,tests);break;   
      case PULSEMBean :
        testMBean = new PulseTestMBean(attrs,ops,ton,tests);break;           
      default :  throw new TestException("Either not implemented test mbean or unknown mbean type " + mbeanType);       
    }    
    return testMBean;
  }
  
 
  public String getTemplateObjectName(){
    return templateObjectName;
  }
  
  
  protected void runMethod(Class class1, TestMBean testMbean, String test, JMXOperations ops, ObjectName name) {
    Method ms[] = class1.getDeclaredMethods();
    Method targetMethod = null;
    for(Method m : ms )
      if(m.getName().equals(test))
        targetMethod = m;
    
    if(targetMethod==null)
      throw new TestException("Test " + test + " not present in " + this.getClass().getName());
    Object args[] = {ops, name};
    try {
      targetMethod.invoke(testMbean, args);
    } catch (IllegalArgumentException e) {
      logErrorAndRaiseException("Error running test " + test,e);
    } catch (IllegalAccessException e) {
      logErrorAndRaiseException("Error running test " + test,e);
    } catch (InvocationTargetException e) {
      logErrorAndRaiseException("Error running test " + test,e);
    }    
  }
  
  @Override
  public String[] getTests() {    
    return tests;
  }

  @Override
  public void executeTest(JMXOperations ops, ObjectName name) {
    String test = getRandomElement(tests);
    logInfo(prefix + "Selected Testcase " + test);
    logInfo("Running method of class " + this.getClass()  + " on " + this + " with test " + test + " argument : " + ops);
    runMethod(this.getClass(),this,test,ops,name);
  }
  
  protected Object callJmxOperation(String url, JMXOperations ops,Object operation[], ObjectName targetMbean){    
    String opName = (String) operation[0];
    Object[] params = (Object[]) operation[1];
    String[] signature = (String[]) operation[2];
    Object result = ops.doJMXOp(url,targetMbean,opName,params,signature);
    return result;
  }
  
  protected Object[] buildOperationArray(String opName, Object[] params, String[] signature, Object returnValue){    
    Object operation[] = new Object[]{opName,params,signature,returnValue};
    return operation;
  }
  
  public void addRegionCreateNotificationExp(String newRegion){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.REGION_CREATED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.REGION_CREATED_PREFIX+newRegion);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.REGION_CREATED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.REGION_CREATED_PREFIX+newRegion);
    }
  }
  
  public void addRegionDestroyNotificationExp(String region){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.REGION_CLOSED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.REGION_CLOSED_PREFIX+region);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.REGION_CLOSED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.REGION_CLOSED_PREFIX+region);
    }
  }
  
  public void addDiskStoreCreatedNotificationExp(String diskStoreName){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.DISK_STORE_CREATED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.DISK_STORE_CREATED_PREFIX+diskStoreName);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.DISK_STORE_CREATED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.DISK_STORE_CREATED_PREFIX+diskStoreName);
    }
  }
  
  public void addDiskStoreDestroyedNotificationExp(String diskStoreName){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.DISK_STORE_CLOSED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.DISK_STORE_CLOSED_PREFIX+diskStoreName);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.DISK_STORE_CLOSED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.DISK_STORE_CLOSED_PREFIX+diskStoreName);
    }
  }
  
  public void addLockServiceCreateNotificationExp(String lockServiceName){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.LOCK_SERVICE_CREATED , 
        ManagementUtil.getMemberID(), 
        ResourceNotification.LOCK_SERVICE_CREATED_PREFIX+lockServiceName);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.LOCK_SERVICE_CREATED , 
          ManagementUtil.getMemberID(), 
          ResourceNotification.LOCK_SERVICE_CREATED_PREFIX+lockServiceName);
    }
  }  
  
  public void addLockServiceDestroyNotificationExp(String lockServiceName){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.LOCK_SERVICE_CLOSED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.LOCK_SERVICE_CLOSED_PREFIX+lockServiceName);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.LOCK_SERVICE_CLOSED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.LOCK_SERVICE_CLOSED_PREFIX+lockServiceName);
    }
  }
  
  public void addMemberDepartedNotificationExp(String memberId){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getDistributedSystemName(),
        ResourceNotification.CACHE_MEMBER_DEPARTED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.CACHE_MEMBER_DEPARTED_PREFIX+memberId);
  }
  
  public void addMemberSuspectNotificationExp(String memberId){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getDistributedSystemName(),
        ResourceNotification.CACHE_MEMBER_SUSPECT, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.CACHE_MEMBER_SUSPECT_PREFIX+memberId);
  }
  
  public void addMemberJoinedNotificationExp(String memberId){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getDistributedSystemName(),
        ResourceNotification.CACHE_MEMBER_JOINED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.CACHE_MEMBER_JOINED_PREFIX+memberId);
  }  
  
  public void addClientJoinedNotificationExp(ObjectName cacheServerMBean){
    JMXTest.addExpectationForNotificationPseudoMatch(
        cacheServerMBean,
        ResourceNotification.CLIENT_JOINED, 
        cacheServerMBean.toString(), 
        ResourceNotification.CLIENT_JOINED_PREFIX);
        //Peuso Matching
  }
  
  public void addClientLeftNotificationExp(ObjectName cacheServerMBean, String clientId){
    //JMXTest.addExpectationForNotification(
    JMXTest.addExpectationForNotificationPseudoMatch(
        cacheServerMBean,
        ResourceNotification.CLIENT_LEFT, 
        cacheServerMBean.toString(), 
        //ResourceNotification.CLIENT_LEFT_PREFIX+clientId);
        clientId);
  }
  
  public void addClientCrashedNotificationExp(ObjectName cacheServerMBean, String clientId){
    //JMXTest.addExpectationForNotification(
    JMXTest.addExpectationForNotificationPseudoMatch(
        cacheServerMBean,
        ResourceNotification.CLIENT_CRASHED, 
        cacheServerMBean.toString(), 
        //ResourceNotification.CLIENT_CRASHED_PREFIX+clientId);
        clientId);
  }
  
  public static void addGWReceiverCreatedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_RECEIVER_CREATED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_RECEIVER_CREATED_PREFIX);
  }
  
  public static void addGWReceiverStartedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_RECEIVER_STARTED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_RECEIVER_STARTED_PREFIX);
  }
  
  public static void addGWReceiverStoppedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_RECEIVER_STOPPED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_RECEIVER_STOPPED_PREFIX);
  }
  
  public static void addGWSenderCreatedNotificationExp(int count/*String senderId*/){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_SENDER_CREATED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_SENDER_CREATED_PREFIX
        /*+senderId*/);
  }
  
  public static void addGWSenderStoppedNotificationExp(int count/*String senderId*/){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_SENDER_STOPPED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_SENDER_STOPPED_PREFIX
        /*+senderId*/);
  }
  
  public static void addGWSenderPausedNotificationExp(/*String senderId*/){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.GATEWAY_SENDER_PAUSED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.GATEWAY_SENDER_PAUSED_PREFIX
        /*+region*/);
  }
  
  public static void addAsyncEventQueueCreatedNotificationExp(/*-String region*/){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.ASYNC_EVENT_QUEUE_CREATED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.ASYNC_EVENT_QUEUE_CREATED_PREFIX
        /*-+region*/);
  }

  public void addLocaterCreatedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.LOCATOR_STARTED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.LOCATOR_STARTED_PREFIX);
  }
  
  public void addSystemAlertNotificationExp(String message){
    JMXTest.addExpectationForNotificationPseudoMatch(
        MBeanJMXAdapter.getDistributedSystemName(),
        ResourceNotification.SYSTEM_ALERT, 
        ManagementUtil.getMemberID(), 
        message);
  }
  
  public void addCacheServerStartedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.CACHE_SERVER_STARTED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.CACHE_SERVER_STARTED_PREFIX);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.CACHE_SERVER_STARTED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.CACHE_SERVER_STARTED_PREFIX);
    }
  }
  
  public void addCacheServerStoppedNotificationExp(){
    JMXTest.addExpectationForNotification(
        MBeanJMXAdapter.getMemberMBeanName(ManagementUtil.getMemberID()),
        ResourceNotification.CACHE_SERVER_STOPPED, 
        ManagementUtil.getMemberID(), 
        ResourceNotification.CACHE_SERVER_STOPPED_PREFIX);
    
    if(ManagementUtil.subscribeDSNotifs()){
      JMXTest.addExpectationForNotification(
          MBeanJMXAdapter.getDistributedSystemName(),
          ResourceNotification.CACHE_SERVER_STOPPED, 
          ManagementUtil.getMemberID(), 
          ResourceNotification.CACHE_SERVER_STOPPED_PREFIX);
    }
  }  
  
  public static void addDSNotifListener(ObjectName name, String memberId) {
    if(!JMXTest.isNotifListenerRegistered(name)){
      JMXTest.addDSNotifListener(name, memberId);
    }
  }

  public static void addMemberNotifListener(ObjectName name, String memberId) {
    if(!JMXTest.isNotifListenerRegistered(name)){
      JMXTest.addMemberNotifListener(name, memberId);
    }
  }

  public static void addCacheServerNotifListener(ObjectName cacheServerON, String memberId) {
    if(!JMXTest.isNotifListenerRegistered(cacheServerON)){
      JMXTest.addCacheServerNotifListener(cacheServerON, memberId);
    }
  }
  
  public static class InPlaceJMXNotifValidator{
    public SimpleJMXRecorder recorder = new SimpleJMXRecorder();
    public JMXNotificationListener listener = null;
    public List<Expectation> expectationList = new ArrayList<Expectation>();
    MBeanServerConnection server = null;
    public ObjectName mbean;
    public InPlaceJMXNotifValidator(String notifListenerPrefix, ObjectName targetMbean, String url){      
      JMXNotificationListener listener = new JMXNotificationListener(notifListenerPrefix,recorder,targetMbean);          
      try {
        if(!JMXOperations.LOCAL_MBEAN_SERVER.equals(url))
          server = ManagementUtil.connectToUrlOrGemfireProxy(url);
        else
          server = ManagementUtil.getPlatformMBeanServerDW();
        HydraUtil.logFine("Adding InPlaceJMXNotifValidator listener for mbean " + targetMbean  + " named " + notifListenerPrefix);
        server.addNotificationListener(targetMbean, listener, null, null);
        mbean = targetMbean;
      } catch (MalformedURLException e) {
        throw new TestException("error", e);
      } catch (IOException e) {
        throw new TestException("error", e);
      } catch (InstanceNotFoundException e) {
        throw new TestException("error", e);
      }   
      this.listener = listener;
    }
    
    public void validateNotifications() {
      try {
        server.removeNotificationListener(mbean, listener);
      } catch (InstanceNotFoundException e) {
        HydraUtil.logError("Error while removing listener ", e);
      } catch (ListenerNotFoundException e) {
        HydraUtil.logError("Error while removing listener ", e);
      } catch (IOException e) {
        HydraUtil.logError("Error while removing listener ", e);
      }catch (Exception e) {
        HydraUtil.logError("Error while removing listener ", e);
      }
      JMXValidator validator = new JMXValidator(recorder, expectationList);
      JMXTest.validateAndPrint(validator);
    }
  }
  
  
}
