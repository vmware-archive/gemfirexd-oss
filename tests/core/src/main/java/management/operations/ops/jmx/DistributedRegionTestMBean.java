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
public class DistributedRegionTestMBean extends AbstractTestMBean<DistributedRegionTestMBean> {
  static {
    prefix = "DistributedRegionTestMBean : ";
  }

  
  public DistributedRegionTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, DistributedRegionTestMBean.class, ton, tests);
  }


  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[DistributedRegionMBean];
  }
  
  
  public void checkRegionStatistics(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionStatistics");
    String attributes[] = {
        "GetsRate",
        "PutsRate",
        "CreatesRate",
        "DestroyRate",
        "PutAllRate",
        "PutLocalRate",
        "PutRemoteRate",
        "PutRemoteLatency",
        "PutRemoteAvgLatency",
        "DiskReadsRate",
        "DiskWritesRate",
        "CacheWriterCallsAvgLatency",
        "CacheListenerCallsAvgLatency",
        "LruEvictionRate",
        "LruDestroyRate"        
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionStatistics " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionStatistics test successfully"); 
  }
  
  public void checkRegionRuntimeAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionRuntimeAttributes");
    String attributes[] = {
      "MemberCount",
      "Members",
      "LastModifiedTime",
      "LastAccessedTime",
      "MissCount",
      "HitCount",
      "HitRatio",
      "SystemRegionEntryCount",
      "TotalEntriesOnlyOnDisk",
      "TotalDiskEntriesInVM"
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionRuntimeAttributes " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionRuntimeAttributes test successfully");
  }
  
  public void checkRegionConfigAttributes(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling checkRegionConfigAttributes");
    String attributes[] = {
        "Name",
        "RegionType",
        "FullPath",
        "ParentRegion",
    };    
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkRegionConfigAttributes " + HydraUtil.ObjectToString(attrList));
    logInfo(prefix + " Completed checkRegionConfigAttributes test successfully");
  }


  @Override
  public void doValidation(JMXOperations ops) {
    // TODO Auto-generated method stub
    
  }

  
}
