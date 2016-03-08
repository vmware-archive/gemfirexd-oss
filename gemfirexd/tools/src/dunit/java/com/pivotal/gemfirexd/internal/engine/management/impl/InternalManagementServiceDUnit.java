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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.io.Serializable;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.junit.Test;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.impl.TableMBeanDataUpdater.MemoryAnalyticsHolder;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;

import io.snappydata.test.dunit.Host;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

@SuppressWarnings("serial")
public class InternalManagementServiceDUnit extends GfxdManagementTestBase {

  private static final String GROUP1_NAME = InternalManagementServiceDUnit.class.getSimpleName() + "_Group1";
  private static final String GROUP2_NAME = InternalManagementServiceDUnit.class.getSimpleName() + "_Group2";
  
  static final String CREATE_TABLE_SQL = "CREATE TABLE APP.BOOKS (" +
                                         "ID VARCHAR(10) NOT NULL, " +
                                         "NAME VARCHAR(25), " +
                                         "TAG VARCHAR(25), " +
                                         "SIZE BIGINT, " +
                                         "LOCATION VARCHAR(25)" +
                                         ", CONSTRAINT BOOK_PK PRIMARY KEY (ID) " +
                                         ")";
  
  static final String DROP_TABLE_SQL   = "DROP TABLE APP.BOOKS;";

  public InternalManagementServiceDUnit(String name) {
    super(name);
  }

  @Test
  public void testGfxdMemberMBeanCreation() throws Exception {
    startServerVMs(1, 0, GROUP1_NAME+","+GROUP2_NAME);
    VM vm = Host.getHost(0).getVM(0);
    String memberNameOrId = (String) vm.invoke(InternalManagementServiceDUnit.class, "verifyMemberMBeanCreation");

    stopVMNum(-1);
    vm.invoke(InternalManagementServiceDUnit.class, "verifyMemberMBeanCleanUp", new Object[] {memberNameOrId});
  }

  static String verifyMemberMBeanCreation() {
    MBeanServer mBeanServer = MBeanJMXAdapter.getMBeanServer();
    String memberNameOrId = MBeanJMXAdapter.getMemberNameOrId(Misc.getDistributedSystem().getDistributedMember());

    ObjectName memberObjectNamePattern = getMemberObjectNamePattern(memberNameOrId);
//    System.out.println("memberObjectNamePattern :: "+memberObjectNamePattern);

    Set<ObjectInstance> queryMBeans = mBeanServer.queryMBeans(memberObjectNamePattern, null);
    assertTrue("Gfxd Member MBean didn't get created.", !queryMBeans.isEmpty());

    return memberNameOrId;
  }

  static void verifyMemberMBeanCleanUp(String memberNameOrId) {
    MBeanServer mBeanServer = MBeanJMXAdapter.getMBeanServer();

    Set<ObjectInstance> queryMBeans = mBeanServer.queryMBeans(getMemberObjectNamePattern(memberNameOrId), null);
    assertTrue("Gfxd Member MBean didn't get unregistered.", queryMBeans.isEmpty());

    queryMBeans = mBeanServer.queryMBeans(getObjectName(ManagementConstants.OBJECTNAME__PREFIX_GFXD + "*"), null);
    assertTrue("All Gfxd MBeans didn't get unregistered.", queryMBeans.isEmpty());
  }
  
  @Test
  public void testMemoryAnalyticsQueryInterval() throws Exception {
    boolean serverStarted = false;
    boolean tableCreated = false;
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty("gemfirexd.tableAnalyticsUpdateIntervalSeconds",
            "30");
      }
    });
    try {
      startServerVMs(1, 0, GROUP1_NAME+","+GROUP2_NAME);
      serverStarted = true;
      
      final VM vm = Host.getHost(0).getVM(0);
      
      long lastUpdateTime = (Long) vm.invoke(InternalManagementServiceDUnit.class, "getMemoryAnalyticsQueryTime");
      assertFalse("TableMBeanBridge updater is not initialized yet.", -2L == lastUpdateTime);
      assertEquals("Without any updatable TableMBeanBridge lastUpdateTime should be -1.", ManagementConstants.NOT_AVAILABLE_LONG, lastUpdateTime);
      
      int updateRate = (Integer) vm.invoke(InternalManagementServiceDUnit.class, "getUpdateSchedulerRate");
      assertFalse("UpdateScheduler is not initalized.", -2 == updateRate);
      
      serverSQLExecute(1, CREATE_TABLE_SQL);
      tableCreated = true;

      waitForCriterion(new WaitCriterion() {
        public String description() {
          return "Waiting for TableMBeanBridge lastUpdateTime to get updated";
        }

        public boolean done() {
          long returnedLastUpdateTime = (Long) vm.invoke(InternalManagementServiceDUnit.class,"getMemoryAnalyticsQueryTime");
          boolean done = (ManagementConstants.NOT_AVAILABLE_LONG != returnedLastUpdateTime);
          return done;
        }

      }, updateRate * 4, 1000, true);
      
      lastUpdateTime = (Long) vm.invoke(InternalManagementServiceDUnit.class, "getMemoryAnalyticsQueryTime");
      assertFalse("TableMBeanBridge lastUpdateTime should not be -1.", ManagementConstants.NOT_AVAILABLE_LONG == lastUpdateTime);
      
      long latestUpdateTime = lastUpdateTime;
      do {
        sleep(updateRate);
        latestUpdateTime = (Long) vm.invoke(InternalManagementServiceDUnit.class, "getMemoryAnalyticsQueryTime");
      } while(latestUpdateTime == lastUpdateTime);

      long timeBetweenUpdates = latestUpdateTime - lastUpdateTime;
      getLogWriter().info("MemoryAnalyticsQueryInterval:: latestUpdateTime="+latestUpdateTime+",lastUpdateTime="+lastUpdateTime+". Diff(millis): "+timeBetweenUpdates);

      assertTrue("MemoryAnalytics Query is running more frequent than expected.", (timeBetweenUpdates >= MemoryAnalyticsHolder.DEFAULT_MEMANA_QUERY_INTERVAL_SEC));
      
    } finally {
      try {
        if (tableCreated) {
          serverSQLExecute(1, DROP_TABLE_SQL);
        } 
      } finally {
        if (serverStarted) {
          stopVMNum(-1);
        }
        invokeInEveryVM(new SerializableRunnable() {
          @Override
          public void run() {
            System
                .clearProperty("gemfirexd.tableAnalyticsUpdateIntervalSeconds");
          }
        });
      }
    }
  }

  void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }
  
  static long getMemoryAnalyticsQueryTime() {
    GemFireStore bootedInstance = GemFireStore.getBootedInstance();
    assertNotNull("GemFireStore is booted instance is null.", bootedInstance);
    
    InternalManagementService internalMS = InternalManagementService.getInstance(bootedInstance);
    return internalMS.getLastMemoryAnalyticsQueryTime();
  }
  
  static int getUpdateSchedulerRate() {
    GemFireStore bootedInstance = GemFireStore.getBootedInstance();
    assertNotNull("GemFireStore is booted instance is null.", bootedInstance);
    
    InternalManagementService internalMS = InternalManagementService.getInstance(bootedInstance);
    return internalMS.getUpdateSchedulerRate();
  }


  public void testUnregisterByPattern() throws Exception {
    startServerVMs(1, 0, GROUP1_NAME+","+GROUP2_NAME);

    VM vm = Host.getHost(0).getVM(0);

    ObjectName emp1 = (ObjectName) vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV01", "Employee01_01"});
    @SuppressWarnings("unused")
    ObjectName emp2 = (ObjectName) vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV01", "Employee01_02"});
    @SuppressWarnings("unused")
    ObjectName emp3 = (ObjectName) vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV02", "Employee02_03"});
    @SuppressWarnings("unused")
    ObjectName emp4 = (ObjectName) vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV03", "Employee03_04"});

    // failure for re-registration
    Exception expectedException = null;
    try {
      vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV01", "Employee01_01"});
    } catch (Exception e) {
      expectedException = e;
      getLogWriter().info(e);
    }
    assertNotNull("Expected an exception on re-registration of employee MBean.", expectedException);

    Exception unexpectedException = null;
    try {
      vm.invoke(InternalManagementServiceDUnit.class, "removeEmployee", new Object[] {emp1});
      vm.invoke(InternalManagementServiceDUnit.class, "addEmployee", new Object[] {"DIV01", "Employee01_01"});
    } catch (Exception e) {
      unexpectedException = e;
      getLogWriter().info(e);
    }
    assertNull("Unexpected exception on re-registration of employee MBean.", unexpectedException);

    vm.invoke(InternalManagementServiceDUnit.class, "removeAllEmployees");
    stopVMNum(-1);
  }

  static ObjectName addEmployee(String uid, String employeeId) {
    InternalManagementService ims = InternalManagementService.getInstance(
        GemFireStore.getBootedInstance());
    assertNotNull("InternalManagementService not found.", ims);

    EmployeeMBean employeeMBean = new EmployeeMBean(uid, employeeId);
    ObjectName registeredMBean = ims.registerMBean(employeeMBean,
        getEmployeeObjectName(uid, employeeId));

    assertNotNull("registeredMBean is null.", registeredMBean);

    return registeredMBean;
  }

  static void removeEmployee(ObjectName employeeMBeanName) {
    InternalManagementService ims = InternalManagementService.getInstance(
        GemFireStore.getBootedInstance());
    EmployeeMBean employeeMBean = ims.getMBeanInstance(employeeMBeanName,
        EmployeeMBean.class);
    assertNotNull("InternalManagementService not found.", ims);
    ims.unregisterMBean(getEmployeeObjectName(employeeMBean.getDivision(),
        employeeMBean.getEmployeeId()));
  }

  static void removeAllEmployees() {
    InternalManagementService ims = InternalManagementService.getInstance(GemFireStore.getBootedInstance());
    assertNotNull("InternalManagementService not found.", ims);

    ObjectName employeesPattern = getEmployeeObjectName("*", "*");

    Set<ObjectName> unregisteredMBeans = ims.unregisterMBeanByPattern(employeesPattern);
    assertTrue("No MBeans unregstered", !unregisteredMBeans.isEmpty());
    getGlobalLogger().info("Unregstered MBeans " + unregisteredMBeans);
  }

  static ObjectName getEmployeeObjectName(String uid, String employeeId) {
    return MBeanJMXAdapter.getObjectName(ManagementConstants
        .OBJECTNAME__PREFIX_GFXD + "type=Employee,uid=" + uid + ",eid=" + employeeId);
  }

  public interface EmployeeMXBean {
    String getDivision();
    String getEmployeeId();
    double calculateBonus(Double performanceIndex);
    void paySalary(boolean withBouns, double bonusPercent);
    void updateSalary(Double salary);
  }

  public static class EmployeeMBean implements EmployeeMXBean, Serializable {
    private String employeeId;
    private String division;
    private double salary;

    EmployeeMBean(String division, String employeeId) {
      this.division = division;
      this.employeeId = employeeId;
      this.salary = 1000;
    }

    /**
     * @return the division
     */
    @Override
    public String getDivision() {
      return division;
    }

    @Override
    public String getEmployeeId() {
      return employeeId;
    }

    @Override
    public double calculateBonus(Double performanceIndex) {
      return this.salary * performanceIndex/100;
    }

    @Override
    public void paySalary(boolean withBouns, double bonusPercent) {
      if (withBouns) {
        System.out.println("Paying " + salary +" with bonus "+ calculateBonus(bonusPercent));
      } else {
        System.out.println("Paying " + salary);
      }
    }

    @Override
    public void updateSalary(Double salary) {
      this.salary = salary.doubleValue();
    }
  }
}
