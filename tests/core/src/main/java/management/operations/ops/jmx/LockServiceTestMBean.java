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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import management.operations.ops.JMXOperations;
import management.test.jmx.JMXTest;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;


/**
 * 
 * @author tushark
 */
@SuppressWarnings("serial")
public class LockServiceTestMBean extends AbstractTestMBean<LockServiceTestMBean> {

  static {
    prefix = "LockServiceTestMBean : ";
  }

  public LockServiceTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, LockServiceTestMBean.class, ton, tests);
  }

  @Override
  public String getType() {
    return gemfireDefinedMBeanTypes[LockServiceMBean];
  }

  public void createAndDestroyDLock(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling createAndDestroyDLock");
    logInfo(prefix + " Calling createDLock");
    String localUrl = JMXOperations.LOCAL_MBEAN_SERVER;
    ObjectName oldTargetMbean = targetMbean;
    targetMbean = ManagementUtil.getLocalMemberMBeanON();
    logInfo("Target mbean for test createDLock has been changed from " + oldTargetMbean + " to " + targetMbean);
    String lockName = JMXTest.getDLockOperations().createDLock();
    addLockServiceCreateNotificationExp(lockName);
    ObjectName newLockMBean = MBeanJMXAdapter.getLockServiceMBeanName(ManagementUtil.getMemberID(), lockName);
    ObjectName newDistributedLockMBean = MBeanJMXAdapter.getDistributedLockServiceName(lockName);
    HydraUtil.sleepForReplicationJMX();
    try {
      MBeanServerConnection server = ops.getMBeanConnection(localUrl);
      if (!ManagementUtil.checkIfMBeanExists(server, newLockMBean))
        throw new TestException("Could not locate lockService mbean in local mbean server for DLock " + lockName);

      server = ops.getMBeanConnection(ops.selectManagingNode());
      if (!ManagementUtil.checkIfMBeanExists(server, newLockMBean))
        throw new TestException("Could not locate lockService mbean on managing node for DLock " + lockName);

      if (!ManagementUtil.checkIfMBeanExists(server, newDistributedLockMBean))
        throw new TestException("Could not locate lockService mbean on managing node for DLock " + lockName);

    } catch (IOException e) {
      HydraUtil.logErrorAndRaiseException("Error during checking mbean presence", e);
    }
    logInfo(prefix + " Completed createDLock test successfully");
    logInfo(prefix + " Calling destroyDLock");
    logInfo(prefix + " Selected DLock : " + lockName);
    ObjectName destroyedLockMBean = MBeanJMXAdapter.getLockServiceMBeanName(ManagementUtil.getMemberID(), lockName);
    ObjectName destroyedDistributedLockMBean = MBeanJMXAdapter.getDistributedLockServiceName(lockName);
    JMXTest.getDLockOperations().destroyLock(lockName);
    addLockServiceDestroyNotificationExp(lockName);
    HydraUtil.sleepForReplicationJMX();
    try {
      MBeanServerConnection server = ops.getMBeanConnection(localUrl);
      if (ManagementUtil.checkIfMBeanExists(server, destroyedLockMBean))
        throw new TestException("After destroy still lockService mbean is present in local mbean server for DLock "
            + lockName);
      server = ops.getMBeanConnection(ops.selectManagingNode());
      if (ManagementUtil.checkIfMBeanExists(server, destroyedLockMBean))
        throw new TestException("After destroy still lockService mbean is present in  managing node for DLock "
            + lockName);
      if (ManagementUtil.checkIfMBeanExists(server, destroyedDistributedLockMBean))
        throw new TestException("After destroy still lockService mbean is present in  managing node for DLock "
            + lockName);
    } catch (IOException e) {
      HydraUtil.logErrorAndRaiseException("Error during checking mbean presence", e);
    }
    logInfo(prefix + " Completed destroyDLock test successfully");
    logInfo(prefix + " Completed createAndDestroyDLock test successfully");
  }

  public void checkDLockConfig(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = { "Name",
    };
    logInfo(prefix + " Calling checkDLockConfig");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDLockConfig " + HydraUtil.ObjectToString(attrList));
  }

  public void checkDLockCounters(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = { 
        "MemberCount", 
        //TODO by Tushar to convert GrantorMember to an operation
        "MemberNames",
    };
    logInfo(prefix + " Calling checkDLockCounters");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDLockCounters " + HydraUtil.ObjectToString(attrList));
  }

  /*
  public void checkDLockStatistics(JMXOperations ops, ObjectName targetMbean) {
    String attributes[] = {
    /*
     * #$% "GrantWaitAvgLatency",
     * "LockRequestRate"
     * /
    };
    logInfo(prefix + " Calling checkDLockStatistics");
    String url = ops.selectManagingNode();
    AttributeList attrList = (AttributeList) ops.getAttributes(url, targetMbean, attributes);
    logInfo("checkDLockStatistics " + HydraUtil.ObjectToString(attrList));
  }*/

  public void becomeLockGrantor(JMXOperations ops, ObjectName targetMbean) {
    logInfo(prefix + " Calling becomeLockGrantor");
    String url = ops.selectManagingNode();
    callJmxOperation(url, ops, buildOperationArray("becomeLockGrantor", null, null, null), targetMbean);
    HydraUtil.sleepForReplicationJMX();
    boolean isLockGrantor = (Boolean) ops.getAttribute(url, targetMbean, "LockGrantor");
    logInfo(prefix + " Result of becomeLockGrantor : " + isLockGrantor);
    logInfo(prefix + " Completed becomeLockGrantor test successfully");
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

  }

}
