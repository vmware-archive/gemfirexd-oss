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

import static management.util.HydraUtil.getRandomElement;
import static management.util.HydraUtil.logErrorAndRaiseException;
import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;
import hydra.HydraVector;
import hydra.TestConfig;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import management.jmx.JMXBlackboard;
import management.jmx.JMXEventRecorder;
import management.jmx.JMXOperation;
import management.jmx.JMXPrms;
import management.operations.ops.jmx.AbstractTestMBean;
import management.operations.ops.jmx.TestMBean;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

public class JMXOperations {
  
  public static final String LOCAL_MBEAN_SERVER= "<local>";

  public static final int JMX_OP_ATTR_ACCESS = 1;
  public static final int JMX_OP_OPERATION = 2;

  public static final String JMX_ATTR_ACESS = "accessAttribute";
  public static final String JMX_OPERATION_EXEC = "executeOperation";

  protected static String opPrefix = "JMXOperations: ";
  
  private Map<ObjectName,TestMBean> mbeanList = new HashMap<ObjectName,TestMBean>();
  private List<String> managingUrls = new ArrayList<String>();
  private Random randomGen = new Random();
  private JMXEventRecorder eventRecorder = null;
  
  public JMXOperations(){
    this.eventRecorder = JMXBlackboard.getBB();
  }
  
  public void setEventRecorder(JMXEventRecorder eventRecorder) {
    this.eventRecorder = eventRecorder;
  }

  //private Map<ObjectName,TestMBean> mbeanMap = new HashMap<ObjectName,TestMBean>();
  
  
  public static void HydraStartTask_ReadMbeanDescriptors(){
    
    HydraVector mbeanSpecList = TestConfig.tab().vecAt(JMXPrms.mbeanSpec);
    Iterator<String> iterator = mbeanSpecList.iterator();

    HashMap<String, TestMBean> mbeanMap = new HashMap<String, TestMBean>();
    while (iterator.hasNext()) {
      String mbeanSpec = iterator.next();
      mbeanSpec = mbeanSpec.replaceAll("'", "\"");
      logInfo("Mbean Descriptor : " + mbeanSpec);
      TestMBean testMbean = AbstractTestMBean.parseMBeanDescriptor(mbeanSpec);
      mbeanMap.put(testMbean.getTemplateObjectName(), testMbean);
    }

    logInfo("Mbean map is " + mbeanMap);

    JMXBlackboard.getBB().saveMap(JMXBlackboard.MBEAN_MAP, mbeanMap);
    
  }
  
  public void doJMXTest(){
    String url = selectManagingNode();
    logFine("Selecting mbean from  " + mbeanList);
    ObjectName targetMbean = selectMbean();
    logFine("Selected mbean for operation " + targetMbean);
    TestMBean testMBean = mbeanList.get(targetMbean);
    logFine("Test MBean instance for mbean "  + targetMbean + " is  " + testMBean);
    if(testMBean!=null)
      testMBean.executeTest(this,targetMbean);
    else throw new TestException("Could not find testmbean for " + targetMbean);
  }

  /*-
  public Object[] doJmxOperation() {

    String url = selectManagingNode();

    ObjectName targetMbean = selectMbean();

    int op = selectOp();
    
    Object result = null;

    switch (op) {

    case JMX_OP_ATTR_ACCESS:
      break;

    case JMX_OP_OPERATION:
      Object operation[] = selectJmxOperation(targetMbean);
      String opName = (String) operation[0];
      Object[] params = (Object[]) operation[1];
      String[] signature = (String[]) operation[2];
      result = doJMXOp(url,targetMbean,opName,params,signature);
      break;
    }
    return new Object[]{url, targetMbean,op,result};
  }

  private Object[] selectJmxOperation(ObjectName targetMbean) {
    TestMBean test = mbeanList.get(targetMbean);
    Object[][] ops= test.getOperations();
    Object op[] = getRandomElement(ops);
    String name = (String) op[0];
    return test.getOperation(targetMbean,name);
    //return op;
  }*/
  
  public MBeanServerConnection getMBeanConnection(String managingUrl){
    MBeanServerConnection remoteMBS = null;
    try {
      if(!LOCAL_MBEAN_SERVER.equals(managingUrl))
        remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(managingUrl);
      else
        remoteMBS = ManagementUtil.getPlatformMBeanServerDW();
      return remoteMBS;
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to connect managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to connect managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        eventRecorder.addEvent(new JMXOperation(null, null, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
        return null;
      }
    }
    return remoteMBS;
  }
  
  
 public void setAttribute(String managingUrl, ObjectName targetMbean, String attrName, Object value) {    
   MBeanServerConnection remoteMBS = null;
   try {
     if(!LOCAL_MBEAN_SERVER.equals(managingUrl))
       remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(managingUrl);
     else
       remoteMBS = ManagementUtil.getPlatformMBeanServerDW();
   } catch (MalformedURLException e) {
     logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
   } catch (IOException e) {
     if (HydraUtil.isSerialTest()) {
       logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
     } else {
       logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
       eventRecorder.addEvent(new JMXOperation(targetMbean, "det"+attrName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";   
     }
   }
   
   try {
     logInfo("Setting attribute " + attrName + " on " + targetMbean + " hosted in manging node " + managingUrl);
     Attribute attr = new Attribute(attrName, value);     
     remoteMBS.setAttribute(targetMbean, attr);      
     logInfo("Successfully set attribute " + attrName + " on " + targetMbean + " hosted in manging node " + managingUrl);      
   } catch (AttributeNotFoundException e) {
     logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
         + managingUrl, e);
   }catch (InstanceNotFoundException e) {
     if (HydraUtil.isSerialTest())
       logErrorAndRaiseException("Error executing operation " + attrName + " for mbean " + targetMbean + " on managing node "
           + managingUrl, e);
     else {
       logInfo("Continuing test expected execption " + e.getMessage() + " for mbean " + targetMbean);
       eventRecorder.addEvent(new JMXOperation(targetMbean, attrName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
     }
   } catch (MBeanException e) {
     logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
         + managingUrl, e);
   } catch (ReflectionException e) {
     logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
         + managingUrl, e);
   } catch (IOException e) {
     logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
         + managingUrl, e);
   } catch (InvalidAttributeValueException e) {
     logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
         + managingUrl, e);
  }
   
  }
  
  public Object getAttribute(String managingUrl, ObjectName targetMbean, String attrName){
    MBeanServerConnection remoteMBS = null;
    try {
      if(!LOCAL_MBEAN_SERVER.equals(managingUrl))
        remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(managingUrl);
      else
        remoteMBS = ManagementUtil.getPlatformMBeanServerDW();
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        eventRecorder.addEvent(new JMXOperation(targetMbean, "get"+attrName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
        return null;
      }
    }
    Object returnValue = null;
    try {
      logInfo("Accessing attribute " + attrName + " on " + targetMbean + " hosted in manging node " + managingUrl);
      returnValue = remoteMBS.getAttribute(targetMbean, attrName);      
      logInfo("Successfully accessed attribute " + attrName + " on " + targetMbean + " hosted in manging node " + managingUrl
          + " result of type : " + (returnValue==null? "<NULL>" : returnValue.getClass()));      
      return returnValue;
    } catch (AttributeNotFoundException e) {
      logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    }catch (InstanceNotFoundException e) {
      if (HydraUtil.isSerialTest())
        logErrorAndRaiseException("Error executing operation " + attrName + " for mbean " + targetMbean + " on managing node "
            + managingUrl, e);
      else {
        logInfo("Continuing test expected execption " + e.getMessage() + " for mbean " + targetMbean);
        eventRecorder.addEvent(new JMXOperation(targetMbean, attrName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
      }
    } catch (MBeanException e) {
      logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    } catch (ReflectionException e) {
      logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    } catch (IOException e) {
      logErrorAndRaiseException("Error accessing attribute " + attrName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    }
    return null;
  }
  
  
  public Object getAttributes(String managingUrl, ObjectName targetMbean, String attrs[]){
    MBeanServerConnection remoteMBS = null;
    try {
      if(!LOCAL_MBEAN_SERVER.equals(managingUrl))
        remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(managingUrl);
      else
        remoteMBS = ManagementUtil.getPlatformMBeanServerDW();
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        eventRecorder.addEvent(new JMXOperation(targetMbean, "get"+HydraUtil.ObjectToString(attrs), "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
        return null;
      }
    }
    Object returnValue = null;
    try {
      logInfo("Accessing attribute " + HydraUtil.ObjectToString(attrs) + " on " + targetMbean + " hosted in manging node " + managingUrl);
      returnValue = remoteMBS.getAttributes(targetMbean,attrs);   
      logInfo("Successfully accessed attribute " + HydraUtil.ObjectToString(attrs) + " on " + targetMbean + " hosted in manging node " + managingUrl
          + " result of type : " + returnValue.getClass());
      if(returnValue!=null){
        AttributeList list = (AttributeList) returnValue;
        logInfo(" Return list size " + list.size()  +" input attributes " + attrs.length);
        if(list.size()!= attrs.length){
          List<String> attributeDiff = new ArrayList<String>();
          for(String attr : attrs){
            boolean flag = false;
            for(Object a : list){              
              Attribute returnAttribute = (Attribute)a;
              if(returnAttribute.getName().equals(attr))
                flag = true;
            }
            if(!flag)
              attributeDiff.add(attr);
          }
          HydraUtil.logError("Return list " + HydraUtil.ObjectToString(list));
          throw new TestException(" #Bug 45474 Return attribute list does not match with input attributelist Differing attributes - " + attributeDiff);
        }
      }
      return returnValue;
    } catch (InstanceNotFoundException e) {
      if (HydraUtil.isSerialTest())
        logErrorAndRaiseException("Error executing operation " + HydraUtil.ObjectToString(attrs) + " for mbean " + targetMbean + " on managing node "
            + managingUrl, e);
      else {
        logInfo("Continuing test expected execption " + e.getMessage() + " for mbean " + targetMbean);
        eventRecorder.addEvent(new JMXOperation(targetMbean, HydraUtil.ObjectToString(attrs), "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
      }
    } catch (ReflectionException e) {
      logErrorAndRaiseException("Error accessing attribute " + HydraUtil.ObjectToString(attrs) + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    } catch (IOException e) {
      logErrorAndRaiseException("Error accessing attribute " + HydraUtil.ObjectToString(attrs) + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    }
    return null;
  }
  
  public Object doJMXOp(String managingUrl, ObjectName targetMbean, String opName, Object[] params,  String[] signature) {
    MBeanServerConnection remoteMBS = null;
    try {
      if(!LOCAL_MBEAN_SERVER.equals(managingUrl))
        remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(managingUrl);
      else
        remoteMBS = ManagementUtil.getPlatformMBeanServerDW();
    } catch (MalformedURLException e) {
      logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
    } catch (IOException e) {
      if (HydraUtil.isSerialTest()) {
        logErrorAndRaiseException("Error trying to managing node at " + managingUrl, e);
      } else {
        logInfo("Error " + e.getMessage() + " expected during concurrent test. Continuing test.");
        eventRecorder.addEvent(new JMXOperation(targetMbean, opName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
        return null;
      }
    }
    Object returnValue = null;
    try {
      logInfo("Invoking operation " + opName + " on " + targetMbean + " hosted in manging node " + managingUrl);
      returnValue = remoteMBS.invoke(targetMbean, opName, params, signature);
      if(returnValue!=null){
      logInfo("Successfully completed operation " + opName + " on " + targetMbean + " hosted in manging node " + managingUrl
          + " result of type : " + returnValue.getClass());
      }
      else{
        logInfo("Successfully completed operation " + opName + " on " + targetMbean + " hosted in manging node " + managingUrl
            + " result null");
      }
      addEvent(returnValue,opName,targetMbean);     
      return returnValue;
    } catch (InstanceNotFoundException e) {
      if (HydraUtil.isSerialTest())
        logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + targetMbean + " on managing node "
            + managingUrl, e);
      else {
        logInfo("Continuing test expected execption " + e.getMessage() + " for mbean " + targetMbean);
        eventRecorder.addEvent(new JMXOperation(targetMbean, opName, "EXPECTED_EXCPTION", e, null));//return "EXPECTED_EXCPTION";
      }
    } catch (MBeanException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    } catch (ReflectionException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    } catch (IOException e) {
      logErrorAndRaiseException("Error executing operation " + opName + " for mbean " + targetMbean + " on managing node "
          + managingUrl, e);
    }
    return null;
  }
  
   private void addEvent(Object returnValue, String opName, ObjectName targetMbean){
     if(returnValue instanceof Serializable)
       eventRecorder.addEvent(new JMXOperation(targetMbean, opName, returnValue, null, null));
     else if(returnValue!=null){ 
       eventRecorder.addEvent(new JMXOperation(targetMbean, opName, returnValue.toString(), null, null)); //Temporary fix
     }
   }

  private int selectOp() {
    return 2;
  }
  
  public JMXOperations addMbean(ObjectName name, TestMBean test){
    mbeanList.put(name,test);
    return this;
  }
  
  public JMXOperations addManagingUrl(String url){
    managingUrls.add(url);
    return this;
  }

  private ObjectName selectMbean() {    
    if(mbeanList.size()==1){
      ObjectName mbeanOn = null;
      for(ObjectName on: mbeanList.keySet())
        mbeanOn = on;
      return mbeanOn;
    }
    else
    return getRandomElement(mbeanList.keySet());
  }

  public String selectManagingNode() {
    if(managingUrls.size()==0){
      Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
      for(String str : urls){
        managingUrls.add(str);
      }
    }
    
    if(managingUrls.size()==1)
      return managingUrls.get(0);
    
    return getRandomElement(managingUrls);
  }

  public void doJMXValidation() {
    //TODO How to make sure that valdiationHappens in all urls
    for(String url : managingUrls){
      for(ObjectName targetMbean : mbeanList.keySet()){
        logFine("Performing validations on " + targetMbean + " on url " + url);
        TestMBean testMBean = mbeanList.get(targetMbean);
        logFine("Test MBean instance for mbean "  + targetMbean + " is  " + testMBean);
        if(testMBean!=null)
          testMBean.doValidation(this);
        else throw new TestException("Could not find testmbean for " + targetMbean);
      }
    }
    
  } 
  
  /*public String selectManagedNodes() {
    if(managingUrls.size()==0){
      Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
      for(String str : urls){
        managingUrls.add(str);
      }
    }
    
    if(managingUrls.size()==1)
      return managingUrls.get(0);
    
    return getRandomElement(managingUrls);
  }*/
  
  
  
}
