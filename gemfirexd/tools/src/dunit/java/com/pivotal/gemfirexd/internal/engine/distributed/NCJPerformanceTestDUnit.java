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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Test the Non Collocated Join Performance.
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJPerformanceTestDUnit extends DistributedSQLTestBase {
  
  public NCJPerformanceTestDUnit(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }

  /* Note:
   * Make this 'true' while debugging 
   */
  private static boolean debugLog = false;

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    if (NCJPerformanceTestDUnit.debugLog) {
      // misnomer - increase logging
      return super.reduceLogging();
    }
    return "config";
  }

  public void testDummy() {
    // Do Nothing
  }

  public void _testNCJperf1_3_100_100_100() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_globalIndex_1(100, 100, 100, conn, "1_3_100_100_100");
  }

  public void _testNCJperf1_2_666_666_100() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_globalIndex_1(666, 666, 100, conn, "1_2_666_666_100");
  }

  public void _testNCJperf1_3_1000_1000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_globalIndex_1(1000, 1000, 100, conn, "1_3_1000_1000_100");
  }

  public void _testNCJperf1_4_1333_1333_100() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_globalIndex_1(1333, 1333, 100, conn, "1_4_1333_1333_100");
  }

  public void _testNCJperf1_3_5000_5000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_globalIndex_1(5000, 5000, 100, conn, "1_3_5000_5000_100");
  }

  public void _testNCJperf1_3_100_100_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(100, 100, 100, conn, "1_3_100_100_100_tc");
  }

  public void _testNCJperf1_2_666_666_100_tc() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(666, 666, 100, conn, "1_2_666_666_100_tc");
  }

  public void _testNCJperf1_3_1000_1000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(1000, 1000, 100, conn, "1_3_1000_1000_100_tc");
  }

  public void _testNCJperf1_4_1333_1333_100_tc() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(1333, 1333, 100, conn, "1_4_1333_1333_100_tc");
  }

  public void _testNCJperf1_5_1666_1666_100_tc() throws Exception {
    Properties cprops = new Properties();
    cprops.setProperty("host-data", "true");
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1", cprops);

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(1666, 1666, 100, conn, "1_5_1666_1666_100_tc");
  }

  public void _testNCJperf1_3_5000_5000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_globalIndex_1(5000, 5000, 100, conn, "1_3_5000_5000_100_tc");
  }

  public void _testNCJperf2_3_100_100_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_pk_1(100, 100, 100, conn, "2_3_100_100_100");
  }

  public void _testNCJperf2_2_666_666_100() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_pk_1(666, 666, 100, conn, "2_2_666_666_100");
  }

  public void _testNCJperf2_3_1000_1000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_pk_1(1000, 1000, 100, conn, "2_3_1000_1000_100");
  }

  public void _testNCJperf2_4_1333_1333_100() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_pk_1(1333, 1333, 100, conn, "2_4_1333_1333_100");
  }

  public void _testNCJperf2_3_5000_5000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_pk_1(5000, 5000, 100, conn, "2_3_5000_5000_100");
  }

  public void _testNCJperf2_3_100_100_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(100, 100, 100, conn, "2_3_100_100_100_tc");
  }

  public void _testNCJperf2_2_666_666_100_tc() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(666, 666, 100, conn, "2_2_666_666_100_tc");
  }

  public void _testNCJperf2_3_1000_1000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(1000, 1000, 100, conn, "2_3_1000_1000_100_tc");
  }

  public void _testNCJperf2_4_1333_1333_100_tc() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(1333, 1333, 100, conn, "2_4_1333_1333_100_tc");
  }

  public void _testNCJperf2_5_1666_1666_100_tc() throws Exception {
    Properties cprops = new Properties();
    cprops.setProperty("host-data", "true");
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1", cprops);

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(1666, 1666, 100, conn, "2_5_1666_1666_100_tc");
  }

  public void _testNCJperf2_3_5000_5000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_pk_1(5000, 5000, 100, conn, "2_3_5000_5000_100_tc");
  }

  public void _testSUBQperf1_3_100_100_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_subq_1(100, 100, 100, conn, "1_3_100_100_100");
  }

  public void _testSUBQperf1_2_666_666_100() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_subq_1(666, 666, 100, conn, "1_2_666_666_100");
  }

  public void _testSUBQperf1_3_1000_1000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_subq_1(1000, 1000, 100, conn, "1_3_1000_1000_100");
  }

  public void _testSUBQperf1_4_1333_1333_100() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_subq_1(1333, 1333, 100, conn, "1_4_1333_1333_100");
  }

  public void _testSUBQperf1_3_5000_5000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_subq_1(5000, 5000, 100, conn, "1_3_5000_5000_100");
  }

  public void _testSUBQperf1_3_100_100_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(100, 100, 100, conn, "1_3_100_100_100_tc");
  }

  public void _testSUBQperf1_2_666_666_100_tc() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(666, 666, 100, conn, "1_2_666_666_100_tc");
  }

  public void _testSUBQperf1_3_1000_1000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(1000, 1000, 100, conn, "1_3_1000_1000_100_tc");
  }

  public void _testSUBQperf1_4_1333_1333_100_tc() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(1333, 1333, 100, conn, "1_4_1333_1333_100_tc");
  }

  public void _testSUBQperf1_5_1666_1666_100_tc() throws Exception {
    Properties cprops = new Properties();
    cprops.setProperty("host-data", "true");
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1", cprops);

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(1666, 1666, 100, conn, "1_5_1666_1666_100_tc");
  }

  // hangs
  public void _testSUBQperf1_3_5000_5000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_subq_1(5000, 5000, 100, conn, "1_3_5000_5000_100_tc");
  }

  public void _testCOLperf1_3_100_100_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_col_1(100, 100, 100, conn, "1_3_100_100_100");
  }

  public void _testCOLperf1_2_666_666_100() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_col_1(666, 666, 100, conn, "1_2_666_666_100");
  }

  public void _testCOLperf1_3_1000_1000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_col_1(1000, 1000, 100, conn, "1_3_1000_1000_100");
  }

  public void _testCOLperf1_4_1333_1333_100() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_col_1(1333, 1333, 100, conn, "1_4_1333_1333_100");
  }

  public void _testCOLperf1_3_5000_5000_100() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();
    djPerfTest_col_1(5000, 5000, 100, conn, "1_3_5000_5000_100");
  }

  public void _testCOLperf1_3_100_100_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(100, 100, 100, conn, "1_3_100_100_100_tc");
  }

  public void _testCOLperf1_2_666_666_100_tc() throws Exception {
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(666, 666, 100, conn, "1_2_666_666_100_tc");
  }

  public void _testCOLperf1_3_1000_1000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(1000, 1000, 100, conn, "1_3_1000_1000_100_tc");
  }

  public void _testCOLperf1_4_1333_1333_100_tc() throws Exception {
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(1333, 1333, 100, conn, "1_4_1333_1333_100_tc");
  }

  public void _testCOLperf1_5_1666_1666_100_tc() throws Exception {
    Properties cprops = new Properties();
    cprops.setProperty("host-data", "true");
    startServerVMs(4, 0, "SG1");
    startClientVMs(1, 0, "SG1", cprops);

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(1666, 1666, 100, conn, "1_5_1666_1666_100_tc");
  }

  public void _testCOLperf1_3_5000_5000_100_tc() throws Exception {
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, "SG1");

    int clientPort = startNetworkServer(1, null, null);
    Connection conn = TestUtil.getNetConnection(clientPort, null, null);
    djPerfTest_col_1(5000, 5000, 100, conn, "1_3_5000_5000_100_tc");
  }

  /*
   *  Ineffective qualifier
   *  right - pk/index based
   */
  private void djPerfTest_pk_1(int insertTimes, int resultCount, int runTimes,
      Connection conn, String methodName) throws Exception {

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    for (int i = 1; i <= insertTimes; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  tglobalindex values(" + i + ",'" + s
          + "'," + 2 * i + ")");
      clientSQLExecute(1, "Insert into  tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    // warm-up
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + "on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      for (int i = 1; i <= runTimes; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      for (int i = 1; i <= runTimes; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestNCJperf djPerfTest_pk_1 " + methodName + ": "
              + "for executing " + query + " with " + insertTimes
              + " inserted rows, output rows=" + resultCount + " took "
              + runTimes + " times, with time taken was " + diff + " ns");
    }
  }

  /*
   *  Ineffective qualifier
   *  right - table scan
   */
  private void djPerfTest_globalIndex_1(int insertTimes, int resultCount,
      int runTimes, Connection conn, String methodName) throws Exception {

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    for (int i = 1; i <= insertTimes; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  tglobalindex values(" + i + ",'" + s
          + "'," + 2 * i + ")");
      clientSQLExecute(1, "Insert into  tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    // warm-up
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + "on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      for (int i = 1; i <= 1000; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      for (int i = 1; i <= runTimes; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestNCJperf djPerfTest_globalIndex_1 " + methodName + ": "
              + "for executing " + query + " with " + insertTimes
              + " inserted rows, output rows=" + resultCount + " took "
              + runTimes + " times, with time taken was " + diff + " ns");
    }
  }

  /*
   * SubQuery
   */
  private void djPerfTest_subq_1(int insertTimes, int resultCount,
      int runTimes, Connection conn, String methodName) throws Exception {

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 1; i <= insertTimes; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  tglobalindex values(" + i + ",'" + s
          + "'," + 2 * i + ")");
      clientSQLExecute(1, "Insert into  tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    // warm up
    {
      String query = "Select B.ID, B.SID, B.VID from "
          + "tdriver B where B.ID in (Select A.ID from tglobalindex A) and B.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      for (int i = 1; i <= 1000; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
    }

    {
      String query = "Select B.ID, B.SID, B.VID from "
          + "tdriver B where B.ID in (Select A.ID from tglobalindex A) and B.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      for (int i = 1; i <= runTimes; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestNCJperf djPerfTest_subq_1 " + methodName + ": "
              + "for executing " + query + " with " + insertTimes
              + " inserted rows, output rows=" + resultCount + " took "
              + runTimes + " times, with time taken was " + diff + " ns");
    }
  }

  /*
   * Collocated Join
   *  Ineffective qualifier
   *  right - pk/index based
   */
  private void djPerfTest_col_1(int insertTimes, int resultCount, int runTimes,
      Connection conn, String methodName) throws Exception {
    clientSQLExecute(1, "create table tglobalindex ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    clientSQLExecute(
        1,
        "create table tdriver ( id int not null, "
            + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tglobalindex)");

    for (int i = 1; i <= insertTimes; i++) {
      String s = "n" + i;
      clientSQLExecute(1, "Insert into  tglobalindex values(" + i + ",'" + s
          + "'," + 2 * i + ")");
      clientSQLExecute(1, "Insert into  tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    // warm up
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + "on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      for (int i = 1; i <= 1000; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + "on A.ID = B.ID where A.SID > ?";

      PreparedStatement s1 = conn.prepareStatement(query);
      long starttime = System.nanoTime();
      for (int i = 1; i <= runTimes; i++) {
        s1.setInt(1, 0);
        ResultSet rs = s1.executeQuery();
        for (int j = 1; j <= resultCount; j++) {
          assertTrue(rs.next());
        }
        assertFalse(rs.next());
      }
      long endtime = System.nanoTime();
      long diff = endtime - starttime;
      getLogWriter().info(
          "logTestNCJperf djPerfTest_col_1 " + methodName + ": "
              + "for executing " + query + " with " + insertTimes
              + " inserted rows, output rows=" + resultCount + " took "
              + runTimes + " times, with time taken was " + diff + " ns");
    }
  }
}
