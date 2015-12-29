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
package com.gemstone.gemfire.admin.statalerts;

import java.util.Properties;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.admin.StatAlertsManager;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * tesing functionality of stats alert manager
 * 
 * @author mjha
 * 
 */
public class StatAlertManagerDUnitTest extends AdminDUnitTestCase {
  public static final int REFRESH_INTERVAL = 10;

  public static final int MEM_VM = 0;

  protected static Cache cache = null;

  /**
   * The constructor.
   * 
   * @param name
   */
  public StatAlertManagerDUnitTest(String name) {
    super(name);
  }

  public boolean isJMX() {
    return true;
  }

  /**
   * Testing the singleton behavior of stat alert manager
   * 
   * @throws Exception
   */
  public void testStatAlertManagerSingletonBehavion() throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(MEM_VM);
    final String testName = this.getName();

    vm1.invoke(new SerializableRunnable() {
      public void run() throws CacheException {
        Properties props = getDistributedSystemProperties();
        props.setProperty(DistributionConfig.NAME_NAME, testName);
        getSystem(props);

        try {
          Thread.sleep(2 * REFRESH_INTERVAL);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }

        DistributionManager dm = (DistributionManager)system.getDistributionManager();
        StatAlertsManager firstInstance = StatAlertsManager.getInstance(dm);
        StatAlertsManager secondInstance = StatAlertsManager.getInstance(dm);

        assertEquals("Stat alert manager should have only one instance",
            firstInstance, secondInstance);
      }
    });
  }

  /**
   * This method verifies refresh time interval of stat alert manager
   * 
   */
  public void testRefreshTimeInterval() throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(MEM_VM);

    final String testName = this.getName();

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties props = getDistributedSystemProperties();
        props.setProperty(DistributionConfig.NAME_NAME, testName);
        getSystem(props);
      }
    });
    pause(2 * 1000);

    AdminDistributedSystemJmxImpl adminDS = (AdminDistributedSystemJmxImpl)this.agent
        .getDistributedSystem();

    assertNotNull(" instance of AdminDistributedSystemJmxImpl cannot be null ",
        adminDS);

    adminDS.setRefreshIntervalForStatAlerts(REFRESH_INTERVAL);

    Thread.sleep(2 * REFRESH_INTERVAL);

    vm1.invoke(new CacheSerializableRunnable("Create cache") {

      public void run2() throws CacheException {
        DistributionManager dm = (DistributionManager)system.getDistributionManager();
        long actualInterval = StatAlertsManager.getInstance(dm)
            .getRefreshTimeInterval();

        assertEquals(
            "refresh time interval of stat alert manager should be same as refresh time interval set by AdminDistributedSystemJmxImpl ",
            REFRESH_INTERVAL*1000, actualInterval);
      }
    });
  }
}
