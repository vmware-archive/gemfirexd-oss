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

import java.util.List;

import javax.management.AttributeList;
import javax.management.ObjectName;

import management.operations.ops.JMXOperations;
import management.util.HydraUtil;


/**
 * 
 * @author tushark
 */
@SuppressWarnings("serial")
public class LocatorTestMBean extends AbstractTestMBean<LocatorTestMBean> {
  
  static {
    prefix = "LocatorTestMBean : ";
  }

  
  public LocatorTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, LocatorTestMBean.class, ton, tests);

  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[LocatorMBean];
  }
  
  
  public void checkLocatorConfig(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "Port",
        "BindAddress",
        "HostnameForClients",
        "PeerLocator",
        "ServerLocator"
    };
    logInfo(prefix + " Calling checkLocatorConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkLocatorConfig " + HydraUtil.ObjectToString(attrList));    
    logInfo(prefix + " Completed checkLocatorCounters test successfully");
  }
  
  public void checkLocatorCounters(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkLocatorCounters");
    String url = ops.selectManagingNode();
    String log = (String) callJmxOperation(url, ops, buildOperationArray("viewLog", 
        null , null, null), targetMbean);
    
    logInfo("locator Log : " + HydraUtil.ObjectToString(log));
    
    String[] listPotentialManagers = (String[]) callJmxOperation(url, ops, 
        buildOperationArray("listPotentialManagers", null , null, null), targetMbean);
    
    logInfo("listPotentialManagers : " + HydraUtil.ObjectToString(listPotentialManagers));
    
    //TODO ADD listManagers and listPotentialManagers validation
    
    String[] listManagers = (String[]) callJmxOperation(url, ops, 
        buildOperationArray("listManagers",null , null, null), targetMbean);
    
    logInfo("listManagers : " + HydraUtil.ObjectToString(listManagers));
    
    logInfo(prefix + " Completed checkLocatorCounters test successfully");
    
  }

  @Override
  public void doValidation(JMXOperations ops) {
    
    
  }

}
