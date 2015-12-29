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
import java.util.Map;

import javax.management.AttributeList;
import javax.management.ObjectName;

import management.operations.ops.JMXOperations;
import management.util.HydraUtil;

/**
 * Test Class for DistributedLockServiceMBean Attributes, operations and notifications
 * @author tushark
 *
 */
@SuppressWarnings("serial")
public class DistributedLockServiceTestMBean extends AbstractTestMBean<DistributedLockServiceTestMBean> {

  static {
    prefix = "DistributedLockServiceTestMBean : ";
  }

  public DistributedLockServiceTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, DistributedLockServiceTestMBean.class, ton, tests);

  }

  @Override
  public String getType() {
    return gemfireDefinedMBeanTypes[DistributedLockServiceMBean];
  }

  public void checkDLockConfig(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = { "Name",
    // "Distributed"
    };
    logInfo(prefix + " Calling checkDLockConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDLockConfig " + HydraUtil.ObjectToString(attrList));
  }

  public void checkDLockCounters(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = { "MemberCount", "MemberNames",
    };
    logInfo(prefix + " Calling checkDLockCounters");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDLockCounters " + HydraUtil.ObjectToString(attrList));
  }

  @SuppressWarnings("unchecked")
  public void listThreadsHoldingLock(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling listThreadsHoldingLock");
    String url = ops.selectManagingNode();
    Map<String, String> result = (Map<String, String>) callJmxOperation(url, ops,
        buildOperationArray("listThreadsHoldingLock", null, null, null), targetMbean);
    logInfo(prefix + " Result of listThreadsHoldingLock : " + HydraUtil.ObjectToString(result));
    logInfo(prefix + " Completed listThreadsHoldingLock test successfully");
  }

  public void listHeldLocks(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling listHeldLocks");
    String url = ops.selectManagingNode();
    String[] result = (String[]) callJmxOperation(url, ops, buildOperationArray("listHeldLocks", null, null, null),
        targetMbean);
    logInfo(prefix + " Result of listHeldLocks : " + HydraUtil.ObjectToString(result));
    logInfo(prefix + " Completed listHeldLocks test successfully");
  }

  @Override
  public void doValidation(JMXOperations ops) {
    // TODO Auto-generated method stub

  }

}