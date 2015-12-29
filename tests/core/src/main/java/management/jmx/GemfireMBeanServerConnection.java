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
package management.jmx;

import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;

import management.util.HydraUtil;
import management.util.ManagementUtil;

import util.TestException;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.DiskStoreMXBean;
import com.gemstone.gemfire.management.DistributedLockServiceMXBean;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.LockServiceMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;

public class GemfireMBeanServerConnection implements MBeanServerConnection {
  
  
  private static final Pattern distributedLockservicePattern = Pattern.compile("GemFire:service=LockService,name=(.*),type=Distributed");
  private static final Pattern distributedRegionPattern = Pattern.compile("GemFire:service=Region,name=(.*),type=Distributed");
  private static final Pattern memberPattern = Pattern.compile("GemFire:type=Member,member=(.*)");
  
  private static final Pattern regionPattern = Pattern.compile("GemFire:service=Region,name=(.*),type=(.*),member=(.*)");
  private static final Pattern diskStorePattern = Pattern.compile("GemFire:service=DiskStore,name=(.*),type=(.*),member=(.*)");
  private static final Pattern lockservicePattern = Pattern.compile("GemFire:service=LockService,name=(.*),type=(.*),member=(.*)");
  private static final Pattern gwSenderPattern = Pattern.compile("GemFire:service=GatewaySender,gatewaySender=(.*),type=(.*),member=(.*)");
  private static final Pattern gwReceiverPattern = Pattern.compile("GemFire:service=GatewayReceiver,type=(.*),member=(.*)");
  private static final Pattern cacheServerPattern = Pattern.compile("GemFire:service=CacheServer,port=(.*),type=(.*),member=(.*)");
  
  
  private SystemManagementService service = null;
  
  public GemfireMBeanServerConnection(ManagementService service){
    this.service = (SystemManagementService)service;
    /*
    if(!service.isManager()){
      throw new TestException("This is supposed to run only on manager nodes. Check your test configuration");
    }*/
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException,
      IOException {    
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");    
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException, InstanceNotFoundException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
      String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException, InstanceNotFoundException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }

  @Override
  public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException,
      IOException {    
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");    
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) throws IOException {
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    return server.queryMBeans(name, query);
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) throws IOException {    
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");    
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    return server.queryNames(name, query);    
  }

  @Override
  public boolean isRegistered(ObjectName name) throws IOException {
//    
//    if(!JMXPrms.domainAware()){
//      return jmxBackingConnection.isRegistered(name);
//    }else{
//      throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
//    }
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
    
  }

  @Override
  public Integer getMBeanCount() throws IOException {
    
//    if(!JMXPrms.domainAware()){
//      return jmxBackingConnection.getMBeanCount();
//    }else{
//      throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
//    }
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
  } 

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException,
      ReflectionException, IOException {
    
    
//    if(!JMXPrms.domainAware()){
//      return jmxBackingConnection.setAttributes(name, attributes);
//    }else{
//      throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
//    }
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
    
  } 

  @Override
  public String getDefaultDomain() throws IOException {
    
//    if(!JMXPrms.domainAware()){
//      return jmxBackingConnection.getDefaultDomain();
//    }else{
//      throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
//    }
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
  }

  @Override
  public String[] getDomains() throws IOException {
//    if(!JMXPrms.domainAware()){
//      return jmxBackingConnection.getDomains();
//    }else{
//      throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
//    }
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");  
  }

  @Override
  public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException, IOException {
    
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.addNotificationListener(name, listener, filter, handback);
    
  }

  @Override
  public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException, IOException {
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.addNotificationListener(name, listener, filter, handback);
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException,
      ListenerNotFoundException, IOException {
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.removeNotificationListener(name, listener);
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException, ListenerNotFoundException, IOException {
    
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.removeNotificationListener(name,listener,filter,handback);
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener)
      throws InstanceNotFoundException, ListenerNotFoundException, IOException {
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.removeNotificationListener(name, listener);
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException, ListenerNotFoundException, IOException {
    //throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
    MBeanServerConnection server = ManagementUtil.getPlatformMBeanServer();
    server.removeNotificationListener(name, listener,filter,handback);
  }

  @Override
  public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }

  @Override
  public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException, IOException {
    throw new UnsupportedOperationException("GemfireMBeanServerConnection does not implement this opeation ");
  }
  
  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    
    if(!service.isManager())
      throw new TestException("This node does not run Federating manager. Wrong test Configuration");
    
    if(!JMXPrms.useGemfireProxies()){
      throw new TestException("useGemfireProxies is false but still gemfireMBeanServerConnection is called ");
    }else{
      Object proxy = getProxy(name);            
      try {
        Object methodResult = execMethodOnObject(operationName, params, signature, proxy);
        return methodResult;
      } catch (SecurityException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      } catch (IllegalArgumentException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      } catch (ClassNotFoundException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      } catch (NoSuchMethodException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      } catch (IllegalAccessException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      } catch (InvocationTargetException e) {
        HydraUtil.logErrorAndRaiseException("Error running method " + operationName + " on mbean-proxy " + name, e);
      }
    }
    return null;    
  }
  
  @Override
  public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException,
      InstanceNotFoundException, ReflectionException, IOException {
    
    if(!service.isManager())
      throw new TestException("This node does not run Federating manager. Wrong test Configuration");
    
    if(!JMXPrms.useGemfireProxies()){
      throw new TestException("useGemfireProxies is false but still gemfireMBeanServerConnection is called ");
    }else{
      Object proxy = getProxy(name);
      try {
        Object returnValue = getter(attribute, proxy);
        return returnValue;
      } catch (SecurityException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (IllegalArgumentException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (NoSuchMethodException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (IllegalAccessException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (InvocationTargetException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      }
      return null;
    }
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException,
      ReflectionException, IOException {
    
    if(!service.isManager())
      throw new TestException("This node does not run Federating manager. Wrong test Configuration");
    
    if(!JMXPrms.useGemfireProxies()){
      throw new TestException("useGemfireProxies is false but still gemfireMBeanServerConnection is called ");
    }else{    
      AttributeList list = new AttributeList();
      Object proxy = getProxy(name);
      for(String attr : attributes){
        try{
          Object returnValue = getter(attr, proxy);
          list.add(new Attribute(attr,returnValue));
        } catch (SecurityException e) {
          HydraUtil.logErrorAndRaiseException("Error running getter " + attr + " on mbean-proxy " + name, e);
        } catch (IllegalArgumentException e) {
          HydraUtil.logErrorAndRaiseException("Error running getter " + attr + " on mbean-proxy " + name, e);
        } catch (NoSuchMethodException e) {
          HydraUtil.logErrorAndRaiseException("Error running getter " + attr + " on mbean-proxy " + name, e);
        } catch (IllegalAccessException e) {
          HydraUtil.logErrorAndRaiseException("Error running getter " + attr + " on mbean-proxy " + name, e);
        } catch (InvocationTargetException e) {
          HydraUtil.logErrorAndRaiseException("Error running getter " + attr + " on mbean-proxy " + name, e);
        }
     }
     return list;
    }
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException,
      AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException, IOException {
    
    if(!service.isManager())
      throw new TestException("This node does not run Federating manager. Wrong test Configuration");
    
    if(!JMXPrms.useGemfireProxies()){
      throw new TestException("useGemfireProxies is false but still gemfireMBeanServerConnection is called ");
    }else{
      Object proxy = getProxy(name);
      try {
        setter(attribute.getName(), proxy, attribute.getValue());
      } catch (SecurityException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (IllegalArgumentException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (NoSuchMethodException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (IllegalAccessException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      } catch (InvocationTargetException e) {
        HydraUtil.logErrorAndRaiseException("Error running getter " + attribute + " on mbean-proxy " + name, e);
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Object getProxy(ObjectName name) {    
    Class interfaceClass = getInerfaceClassFromObjectName(name);
    HydraUtil.logFine("Trying to locate proxy for objectName " + name + " with interfaceClass  " + interfaceClass);
    
    Object proxy = null;
    
    if(interfaceClass==null)
      throw new TestException("Unknown objectname " + name.toString());
    String thisMemberId = getMemberID();
    if (interfaceClass.equals(MemberMXBean.class)) {
      Matcher match = memberPattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(1);        
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local member mbean ");
          proxy = service.getMemberMXBean();
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    } else if (interfaceClass.equals(RegionMXBean.class)) {
      Matcher match = regionPattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(3);
        String regionName = match.group(1);        
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local region mbean for region " + regionName);
          proxy = service.getLocalRegionMBean(regionName);
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    } else if (interfaceClass.equals(DiskStoreMXBean.class)) {
      Matcher match = diskStorePattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(3);
        String diskStore = match.group(1);
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local diskStore mbean for " + diskStore);
          proxy = service.getLocalDiskStoreMBean(diskStore);
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    }else if (interfaceClass.equals(LockServiceMXBean.class)) {
      Matcher match = lockservicePattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(3);
        String lock = match.group(1);
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local lockService mbean for " + lock);
          proxy = service.getLocalLockServiceMBean(lock);
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    }else if (interfaceClass.equals(GatewaySenderMXBean.class)) {
      Matcher match = gwSenderPattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(3);
        String sender = match.group(1);
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local gwsender mbean " + sender);
          proxy = service.getLocalGatewaySenderMXBean(sender);
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    }else if (interfaceClass.equals(GatewayReceiverMXBean.class)) {
      Matcher match = gwReceiverPattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(2);
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local gwreceiver mbean ");
          proxy = service.getLocalGatewayReceiverMXBean();
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    }else if (interfaceClass.equals(CacheServerMXBean.class)) {
      Matcher match = cacheServerPattern.matcher(name.toString());
      String memberName = null;
      if (match.find()) {
        memberName = match.group(3);
        String port = match.group(1);
        int serverPort = Integer.parseInt(port);
        HydraUtil.logFine("memberName from ON = <" + memberName + "> Local MemberId from API = " + thisMemberId);
        if (thisMemberId.equals(memberName)) {
          HydraUtil.logFine("Returning local cacheserver mbean");
          proxy = service.getLocalCacheServerMXBean(serverPort);
        }
      } else
        throw new TestException("Could not locate memberName from objectName : " + name.toString());
    }
    else if(interfaceClass.equals(DistributedLockServiceMXBean.class)){
      logFine("Trying to locate DistributedLockService named " + name);
      Matcher match = distributedLockservicePattern.matcher(name.toString());
      String lockServiceName = null;
      if(match.find()){
        lockServiceName = match.group(1);
        proxy = service.getDistributedLockServiceMXBean(lockServiceName);
        logFine("Located DistributedLockService named " + lockServiceName + " is " + proxy);        
      }else throw new TestException("Could not locate lockServiceName from objectName : " + name.toString());
    }    
    else if(interfaceClass.equals(DistributedSystemMXBean.class)){
      proxy = service.getDistributedSystemMXBean();
    }
    else if(interfaceClass.equals(DistributedRegionMXBean.class)){
      Matcher match = distributedRegionPattern.matcher(name.toString());
      String regionPath = null;
      if(match.find()){
        regionPath = match.group(1);
        proxy = service.getDistributedRegionMXBean(regionPath);
      }else throw new TestException("Could not locate regionPath from objectName : " + name.toString());      
    }
    
    if(proxy==null)
      proxy = service.getMBeanProxy(name, interfaceClass);
    if(proxy==null){
      Set<ObjectName> allMbeans;
      try {
        allMbeans = this.queryNames(null, null);
        Set<ObjectName> fileteredON = new HashSet<ObjectName>();
        for(ObjectName n : allMbeans){
          if(n.toString().contains("GemFire")){
            fileteredON.add(n);          
          }
        }      
        logInfo("Gemfire mbeans " + fileteredON);
        if(fileteredON.contains(name))
          throw new TestException("MBeanServer contains MBean for " + name + " but Mgmt Service does not return"
              + " proxy for it");
        throw new TestException("Proxy for " + name + " could not be located");
      } catch (IOException e) {
        throw new TestException("Error querying mbeans",e);
      }      
    }
    return proxy;
  }

  private String getMemberID() {
    String formattedMemberID = MBeanJMXAdapter.getMemberNameOrId(InternalDistributedSystem
        .getConnectedInstance().getDistributedMember());
    return formattedMemberID;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object execMethodOnObject(String operationName, Object[] params, String[] signature, Object target) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException{    
    Class klass = target.getClass();
    Class[] parameterTypes = null;
    if(signature!=null){
      parameterTypes = new Class[signature.length];
      int i=0;
      for(String p : signature){
        parameterTypes[i++] = getPrimitiveClassOrOrigClass(p);
      }
    }
    Method method = klass.getMethod(operationName, parameterTypes);
    return method.invoke(target, params);    
  }
  
  private static Class getPrimitiveClassOrOrigClass(String p) throws ClassNotFoundException {
    if("int".equals(p))
      return int.class;
    else if("byte".equals(p))
      return byte.class;
    else if("short".equals(p))
      return short.class;
    else if("long".equals(p))
      return long.class;
    else if("float".equals(p))
      return float.class;
    else if("double".equals(p))
      return double.class;
    else if("boolean".equals(p))
      return boolean.class;
    else return Class.forName(p);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void setter(String attributeName, Object target, Object attributeValue) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException{
    Class klass = target.getClass();
    Class[] parameterTypes = new Class[1];
    parameterTypes[0] = attributeValue.getClass();
    Object[] params = {attributeValue};
    String operationName = "set" + attributeName;
    Method method = null;
    try{
      method  = klass.getMethod(operationName, parameterTypes);
    }catch(NoSuchMethodException e){
      parameterTypes[0] = getPrimitveClassType(parameterTypes[0]);
      method = klass.getMethod(operationName, parameterTypes);
    }    
    method.invoke(target, params);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Class getPrimitveClassType(Class klass) {    
    if(klass.isAssignableFrom(Byte.class))
      return byte.class;    
    if(klass.isAssignableFrom(Short.class))
      return short.class;
    if(klass.isAssignableFrom(Integer.class))
    return int.class;
    if(klass.isAssignableFrom(Long.class))
    return long.class;
    if(klass.isAssignableFrom(Float.class))
    return float.class;
    if(klass.isAssignableFrom(Double.class))
    return double.class;
    if(klass.isAssignableFrom(Boolean.class))
    return boolean.class;
    
    return null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object getter(String attributeName, Object target) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException{
    Class klass = target.getClass();
    Class[] parameterTypes = null;    
    Object[] params = null;
    String operationName = "get" + attributeName;
    Method method =null;
    try{
      method = klass.getMethod(operationName, parameterTypes);
    }catch(NoSuchMethodException ne){
      HydraUtil.logInfo("Cound not find getter " + operationName + " now trying for boolean getter is"+attributeName);
      operationName = "is" + attributeName;
      method = klass.getMethod(operationName, parameterTypes);
      HydraUtil.logInfo("Getter - " + operationName + " is" + method);
    }
    try{
      Object result = method.invoke(target,params);
      HydraUtil.logInfo("Getter - " + operationName + " result " + result);
      return result;
    }catch(Exception e){
      HydraUtil.logErrorAndRaiseException("Error for getter " + attributeName, e);
    }
    return null;
  }
  
  
  private Class getInerfaceClassFromObjectName(ObjectName name) {
    String str = name.toString();
    if(str.startsWith("GemFire:service=CacheServer")){
      return CacheServerMXBean.class;     
    }
    
    if(str.startsWith("GemFire:service=DiskStore")){
      return DiskStoreMXBean.class;
    }
    
    if(str.startsWith("GemFire:service=LockService")){
      if(str.contains("Distributed"))
        return DistributedLockServiceMXBean.class;
      return LockServiceMXBean.class;
    }
    
    if(str.startsWith("GemFire:service=Region")){
      if(str.contains("Distributed"))
        return DistributedRegionMXBean.class;
      return RegionMXBean.class;
    }    
    
    if(str.startsWith("GemFire:service=System,type=Distributed")){
      return DistributedSystemMXBean.class;
    }
    
    if(str.startsWith("GemFire:service=GatewayReceiver")){
      return GatewayReceiverMXBean.class;
    }
    
    if(str.startsWith("GemFire:service=GatewaySender")){
      return GatewaySenderMXBean.class;
    }
    
    if(str.startsWith("GemFire:type=Member")){
      return MemberMXBean.class;
    }    
    return null;
  }

  
}