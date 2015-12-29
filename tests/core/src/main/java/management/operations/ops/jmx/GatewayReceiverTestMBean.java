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
public class GatewayReceiverTestMBean extends AbstractTestMBean<GatewayReceiverTestMBean> {
  
  static {
    prefix = "GatewayReceiverTestMBean : ";
  }

  
  public GatewayReceiverTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, GatewayReceiverTestMBean.class, ton, tests);

  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[GatewayReceiverMBean];
  }

  public void checkReceiverConfig(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "Port",
        "SocketBufferSize",
        "BindAddress",
        "MaximumTimeBetweenPings",
        "StartPort",
        "EndPort",
        "GatewayTransportFilters"
    };
    logInfo(prefix + " Calling checkReceiverConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkReceiverConfig " + HydraUtil.ObjectToString(attrList));   
  }
  
    
  public void checkReceiverStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "EventsReceivedRate",
        "ClientConnectionCount",
        "ConnectionLoad",
        "ConnectionThreads",
        "DuplicateBatchesReceived",
        "LoadPerConnection",
        "LoadPerQueue",
        "NumGateways",
        "OutoforderBatchesReceived",
        "QueueLoad",
        "ThreadQueueSize",
        "TotalConnectionsTimedOut",
        "TotalFailedConnectionAttempts",
        "TotalReceivedBytes",
        "TotalSentBytes",
        "CreateRequestsRate",
        "DestroyRequestsRate",
        "GetRequestAvgLatency",
        "GetRequestRate",
        "PutRequestRate",
        "UpdateRequestsRate"

    };
    logInfo(prefix + " Calling checkReceiverStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkReceiverStatistics " + HydraUtil.ObjectToString(attrList));   
  }  

  
  @Override
  public void doValidation(JMXOperations ops) {    
    
  }
}

