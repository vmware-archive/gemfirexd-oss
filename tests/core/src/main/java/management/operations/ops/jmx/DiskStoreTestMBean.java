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
 * Test Class for DiskStoreMBean Attributes, operations and notifications
 * @author tushark
 *
 */

@SuppressWarnings("serial")
public class DiskStoreTestMBean extends AbstractTestMBean<DiskStoreTestMBean> {
  
  static {
    prefix = "DiskStoreTestMBean : ";
  }
  
  public DiskStoreTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, DiskStoreTestMBean.class, ton, tests);

  }

  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[DiskStoreMBean];
  }
  
  
  public void checkDiskConfig(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "Name",
        "CompactionThreshold",
        "MaxOpLogSize",
        "TimeInterval",
        "WriteBufferSize",
        "DiskDirectories"
    };
    logInfo(prefix + " Calling checkDiskConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDiskConfig " + HydraUtil.ObjectToString(attrList));   
  }

  public void checkDiskCounters(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "QueueSize",
        "TotalBytesOnDisk",
        "TotalQueueSize",
        "TotalBackupInProgress",
        "TotalRecoveriesInProgress"
    };
    logInfo(prefix + " Calling checkDiskCounters");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDiskCounters " + HydraUtil.ObjectToString(attrList)); 
  }

  public void checkDiskStatistics(JMXOperations ops, ObjectName targetMbean){
    String attributes[] = {
        "DiskReadsRate",
        "DiskWritesRate",
        "DiskReadsAvgLatency",
        "DiskWritesAvgLatency",
        "FlushTimeAvgLatency"
    };
    logInfo(prefix + " Calling checkDiskStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDiskStatistics " + HydraUtil.ObjectToString(attrList)); 
  }


  public void forceRoll(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling forceRoll");
    String url = ops.selectManagingNode();
    callJmxOperation(url, ops, buildOperationArray("forceRoll", null, null, null), targetMbean);
    logInfo(prefix + " Completed forceRoll test successfully");
  }


  public void forceCompaction(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling forceCompaction");
    String url = ops.selectManagingNode();
    callJmxOperation(url, ops, buildOperationArray("forceCompaction", null, null, null), targetMbean);
    logInfo(prefix + " Completed forceCompaction test successfully");
  }


  public void flush(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling flush");
    String url = ops.selectManagingNode();
    callJmxOperation(url, ops, buildOperationArray("flush", null, null, null), targetMbean);
    logInfo(prefix + " Completed flush test successfully");
  }


  @Override
  public void doValidation(JMXOperations ops) {
    logInfo(prefix + " Calling doValidation");
    logInfo(prefix + " Completed doValidation successfully");
  }

}
