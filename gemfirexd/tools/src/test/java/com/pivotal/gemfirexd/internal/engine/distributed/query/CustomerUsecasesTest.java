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
package com.pivotal.gemfirexd.internal.engine.distributed.query;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class CustomerUsecasesTest extends JdbcTestBase {

  // global switch to move between wit stats and no stats.
  private static final boolean analyzeMode = false;

  public CustomerUsecasesTest(String name) {
    super(name);

    if (analyzeMode) {
      System.setProperty("gemfirexd.language.logQueryPlan", "true");
      System.setProperty("gemfirexd.optimizer.trace", "true");
    }
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(CustomerUsecasesTest.class));
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public void testBet365_Top1_Ordered_Subquery_NetClient() throws Exception {
    // boot up the DB first
    setupConnection();
    Connection conn = TestUtil.startNetserverAndGetLocalNetConnection();
    executeFetch_1_OrderBy_Subquery(conn);
    TestUtil.stopNetServer();
  }

  public void testBet365_Top1_Ordered_Subquery() throws Exception {
    TestUtil.setupConnection();
    Connection conn = TestUtil.jdbcConn;

    executeFetch_1_OrderBy_Subquery(conn);

    TestUtil.shutDown();
  }

  private void executeFetch_1_OrderBy_Subquery(Connection conn) {
    try {
      conn.createStatement().execute("drop table Account");
      conn.createStatement().execute("drop table AccountHolder");
      conn.createStatement().execute("drop table OuterTable");
    } catch (Exception ignore) {
    }

    try {
      conn.createStatement().execute(
          "create table Account (acid int primary key, "
              + "acname varchar(100), actype int)");
      // redundancy 1
      conn.createStatement().execute(
          "create table AccountHolder (ahid int primary key, "
              + "ahname varchar(100), ahacid int, ahotid int, ahtype int)");
      conn
          .createStatement()
          .execute(
              "create index acc_type on AccountHolder(ahotid asc, ahacid asc, ahtype desc)");
      // conn.createStatement().execute(
      // "create index acc_type on AccountHolder(ahtype desc)");

      conn.createStatement().execute(
          "create table OuterTable (otid int primary key, "
              + "oname varchar(100), otaotid int)");

      conn.createStatement().execute(
          "create table AnotherOuterTable (aotid int primary key, "
              + "aoname varchar(100), blank int)");

      PreparedStatement ps = conn
          .prepareStatement("insert into Account values(?,?,?)");
      for (int i = 1; i <= 100; i++) {
        ps.setInt(1, i);
        ps.setString(2, "naAcc " + i);
        ps.setInt(3, i * 4);
        ps.addBatch();

        if (i % 100 == 0) {
          ps.executeBatch();
          ps.clearBatch();
        }
      }

      HashMap<Integer, Integer> checker = new HashMap<Integer, Integer>(100);
      ps = conn.prepareStatement("insert into AccountHolder values(?,?,?,?,?)");
      for (int i = 1; i <= 100; i++) {
        ps.setInt(1, i);
        ps.setString(2, "na -- " + i);
        ps.setInt(3, i);
        ps.setInt(4, i);
        ps.setInt(5, 1);
        ps.execute();
        int t = PartitionedRegion.rand.nextInt(100) + 2;
        checker.put(Integer.valueOf(i), Integer.valueOf(t));
        for (; t > 1; t--) {
          ps.setInt(1, i * 1000 + t);
          ps.setString(2, "na ++ " + i);
          ps.setInt(3, i);
          ps.setInt(4, i);
          ps.setInt(5, t);
          ps.execute();
        }

      }

      ps = conn.prepareStatement("insert into OuterTable values(?,?,?)");
      for (int i = 1; i <= 100; i++) {
        ps.setInt(1, i);
        ps.setString(2, "naOut " + i);
        ps.setInt(3, i * 4);
        ps.addBatch();

        if (i % 100 == 0) {
          ps.executeBatch();
          ps.clearBatch();
        }
      }

      ps = conn.prepareStatement("insert into AnotherOuterTable values(?,?,?)");
      for (int i = 1; i <= 100; i++) {
        ps.setInt(1, i * 4);
        ps.setString(2, "naAnOut " + i);
        ps.setInt(3, i * 3930);
        ps.addBatch();

        if (i % 100 == 0) {
          ps.executeBatch();
          ps.clearBatch();
        }
      }

      ResultSet cnt = conn.createStatement().executeQuery(
          "select count(1) from AccountHolder");
      cnt.next();
      getLogger().info("AccountHolder have = " + cnt.getInt(1));
      cnt.close();

      if (analyzeMode) {
        SanityManager.DEBUG_SET("DumpOptimizedTree");

        getLogger().info("about to enable runtime stats");
        conn.createStatement().execute(
            "CALL SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
        conn.createStatement().execute(
            "CALL SYSCS_UTIL.SET_STATISTICS_TIMING(1)");
      }
      {

        final String queryStr = "select O.otid, (select ah.ahtype from AccountHolder AS ah --GEMFIREXD-PROPERTIES index=acc_type \r "
            + " join Account ac on ah.ahacid = ac.acid where ah.ahotid = O.otid fetch first row only) "
            + "  from OuterTable O join AnotherOuterTable aO on O.otaotid = aO.aotid "
            + " where O.otid in (?,?,?,?,?) ";
        getLogger().info("about to execute query " + queryStr);

        PreparedStatement qu = conn.prepareStatement(queryStr);

        qu.setInt(1, 10);
        qu.setInt(2, 99);
        qu.setInt(3, 34);
        qu.setInt(4, 45);
        qu.setInt(5, 7);

        long beginTime = System.currentTimeMillis();
        ResultSet rs = qu.executeQuery();

        while (rs.next()) {
          int num1 = rs.getInt(1);
          // Object ob = rs.getObject(2);
          // getLogger().info(
          // "got second col " + ob + " " + ob.getClass().getName());
          int num2 = rs.getInt(2);

          assertEquals("Bet365 select top 1 subquery failure : ", num2, checker
              .get(Integer.valueOf(num1)).intValue());
        }
        long endTime = System.currentTimeMillis();
        getLogger().info(
            "closing resultset time took (milliseconds) = "
                + (endTime - beginTime));
        rs.close();
      }

      {
        CallableStatement cs = conn
            .prepareCall("CALL SYSIBM.SQLTABLES('', '', '', '', 'GETCATALOGS=1')");
        ResultSet ts = cs.executeQuery();
        ResultSetMetaData rsm = ts.getMetaData();

        int c = rsm.getColumnCount();
        int j = 0;
        getLogger().info("Trying to browse " + c);
        while (ts.next()) {
          for (int i = 1; i <= c; i++) {
            Object ob = ts.getObject(i);
            getLogger().info("Col " + i + " = " + ob);
          }
          getLogger().info("Row Completed " + (++j));
        }

        ts.close();
      }

    } catch (Exception ex) {
      getLogger().error("Exception " + ex);
      getLogger().error(SanityManager.getStackTrace(ex));
    }

  }

}
