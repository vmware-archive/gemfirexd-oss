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

package com.pivotal.gemfirexd.internal.engine.management;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.management.impl.AggregateTableMBean;
import com.pivotal.gemfirexd.internal.engine.management.impl.InternalManagementService;
import com.pivotal.gemfirexd.internal.engine.management.impl.ManagementUtils;
import com.pivotal.gemfirexd.internal.engine.management.impl.GfxdManagementTestBase;

import io.snappydata.test.dunit.VM;

/**
 * @author ajayp
 *
 */

public class AggregateTableMBeanDUnit extends GfxdManagementTestBase {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /** The <code>MBeanServer</code> for this application */
  public static MBeanServer mbeanServer = ManagementFactory
      .getPlatformMBeanServer();

  public AggregateTableMBeanDUnit(String name) {
    super(name);
  }

 
  public void testAggregateMemberstats() throws Exception {
    try {

      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");

      startServerVMs(1, 0, null, serverInfo);

      serverInfo.setProperty("jmx-manager", "true");
      serverInfo.setProperty("jmx-manager-start", "true");
      serverInfo.setProperty("jmx-manager-port", "0");// No need to start an
                                                      // Agent for this test

      startServerVMs(1, 0, null, serverInfo);
      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");
      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      checkAggregateTableMBean(conn);
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }

  private void checkAggregateTableMBean(Connection conn) throws Exception {
    final VM serverVM = this.serverVMs.get(1); // Server Started as a manager
    final Statement stmt = conn.createStatement();
    final String createSchemaOrder = "create schema trade";
    stmt.execute(createSchemaOrder);
    
    stmt.execute("create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100), tid int, " + "primary key (cid))");
    
    PreparedStatement psInsertCust = conn.prepareStatement("insert into " + "trade.customers values (?,?,?,?,?)");

    java.sql.Date since = new java.sql.Date(System.currentTimeMillis());
    // Insert 0-10.
    for (int i = 0; i < 10; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX" + i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX" + i);
      psInsertCust.setInt(5, i);
      psInsertCust.executeUpdate();
    }    
    
    serverVM.invoke(this.getClass(), "getAggregateTableMBean");
    
  }

  public static void getAggregateTableMBean() {
    final InternalManagementService service = InternalManagementService.getInstance(Misc.getMemStore());  
    final ObjectName distrObjectName = ManagementUtils.getAggrgateTableMBeanName("TRADE.CUSTOMERS");
    
    logInfo("AggregateTableMXBean distrObjectName=="+distrObjectName);

    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for the getAggregateTableMBean to get reflected at managing node";
      }

      public boolean done() {
        AggregateTableMXBean bean = service.getMBeanInstance(distrObjectName,
            AggregateTableMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, ManagementConstants.REFRESH_TIME * 4, 500, true);
    

    logInfo("AggregateTableMXBean after wait criteria ");

    AggregateTableMBean aggregateTableMBean = (AggregateTableMBean) service
        .getMBeanInstance(distrObjectName, AggregateTableMXBean.class);

    if (aggregateTableMBean != null) {
      logInfo("AggregateTableMXBean aggregateTableMBean.getEntrySize()=="+aggregateTableMBean.getEntrySize());
      logInfo("AggregateTableMXBean aggregateTableMBean.getNumberOfRows()=="+aggregateTableMBean.getNumberOfRows());
      assertEquals(true, aggregateTableMBean.getEntrySize() >= 0 ? true : false);
      assertEquals(true, aggregateTableMBean.getNumberOfRows() >= 0 ? true : false);
    } else {
      logInfo("AggregateTableMXBean is null");
    }
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

}
