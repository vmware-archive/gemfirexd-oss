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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.management.impl.AggregateStatementMBean;
import com.pivotal.gemfirexd.internal.engine.management.impl.InternalManagementService;
import com.pivotal.gemfirexd.internal.engine.management.impl.ManagementUtils;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.stats.StatementStatsDUnit.StatementStatsObserver;

import io.snappydata.test.dunit.VM;

/**
 * @author rishim
 *
 */
@SuppressWarnings("serial")
public class AggregateStatementStatsDUnit extends DistributedSQLTestBase {

  /** The <code>MBeanServer</code> for this application */
  public static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();



  public AggregateStatementStatsDUnit(String name) {
    super(name);
  }

  /**
   * test for all basic statement stats gathered during dml execution.
   *
   * @see com.pivotal.gemfirexd.internal.impl.sql.StatementStats
   * @throws Exception
   *           on failure.
   */
  public void testStatementStats() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");

      startServerVMs(1, 0, null, serverInfo);

      serverInfo.setProperty("jmx-manager", "true");
      serverInfo.setProperty("jmx-manager-start", "true");
      serverInfo.setProperty("jmx-manager-port", "0");// No need to start an Agent for this test

      startServerVMs(1, 0, null, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, null, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      checkAggregateMBean(conn, true, 1);
      conn.close();

      stopVMNums(1,-1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      System.clearProperty(GfxdConstants.GFXD_ENABLE_STATS);
    }
  }



  private static StatementStatsObserver ob1;

  private static StatementStatsObserver ob2;

  private void checkAggregateMBean(Connection conn, final boolean enableStats, final int numTimesSampled)
      throws Exception {

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

    final PreparedStatement psSelectCust = conn.prepareStatement("select * "
        + "from trade.customers where cust_name = ?");

    for (int i = 0; i < 10; i++) {
      psSelectCust.setString(1, "XXXX" + i);
      ResultSet rs = psSelectCust.executeQuery();
      assertTrue("Should return one row", rs.next());
      assertFalse("Should not return more than one row", rs.next());
      rs.close();
    }

    String stmtId = ((EmbedStatement) psSelectCust).getStatementStats().getStatsId();

    System.out.println("Statement Id = " + stmtId);

    printStatementMBeans();

    serverVM.invoke(this.getClass(), "isAggregateStatementMBeanCreated", new Object[] { stmtId });

    psSelectCust.close();
    psInsertCust.close();
    stmt.close();

  }

  public static void isAggregateStatementMBeanCreated(String stmtId) {

    final InternalManagementService service = InternalManagementService.getInstance(Misc.getMemStore());
    final ObjectName statementObjectName = ManagementUtils.getAggregateStatementMBeanName(stmtId);

    printStatementMBeans();

    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for the statement aggregator to get reflected at managing node";
      }

      public boolean done() {
        AggregateStatementMXBean bean = service.getMBeanInstance(statementObjectName, AggregateStatementMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, ManagementConstants.REFRESH_TIME * 4, 500, true);

    AggregateStatementMBean aggStatementMBean = (AggregateStatementMBean)service.getMBeanInstance(statementObjectName,
        AggregateStatementMXBean.class);

    printValues(AggregateStatementMXBean.class, aggStatementMBean);

  }



  private static void printValues(Class<?> mbeanInterface, Object mbeanObject) {
    final Method[] methodArray = mbeanInterface.getMethods();
    Object[] args = null;
    for (Method m : methodArray) {
      String name = m.getName();

      String attrName = "";
      if (name.startsWith("get")) {
        attrName = name.substring(3);

      } else if (name.startsWith("is") && m.getReturnType() == boolean.class) {
        attrName = name.substring(2);
      }

      try {
        Object val = m.invoke(mbeanObject, args);
        System.out.println(attrName +" = "+val);
      } catch (IllegalArgumentException e) {
        fail("printValues failed" + e);
      } catch (IllegalAccessException e) {
        fail("printValues failed" + e);
      } catch (InvocationTargetException e) {
        fail("printValues failed" + e);
      }
    }
  }

  public static void printStatementMBeans() {
    try {
      Set<ObjectInstance> objNames = mbeanServer.queryMBeans(new ObjectName("GemFireXD:service=Statement,*"), null);
      System.out.println(objNames);
    } catch (MalformedObjectNameException e) {
      fail("printStatementMBeans failed" + e);
    } catch (NullPointerException e) {
      fail("printStatementMBeans failed" + e);
    }
  }

  @Override
  public void tearDown2() throws Exception {
    ob1 = null;
    ob2 = null;
    super.tearDown2();
  }

}
