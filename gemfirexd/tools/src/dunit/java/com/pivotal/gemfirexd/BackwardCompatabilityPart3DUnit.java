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

package com.pivotal.gemfirexd;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import io.snappydata.test.util.TestException;
import org.junit.Assert;

/**
 * Split out from BackwardCompatabilityDUnit to avoid timeouts due to long
 * running times of the tests.
 */
@SuppressWarnings("serial")
public class BackwardCompatabilityPart3DUnit extends
    BackwardCompatabilityTestBase {

  public BackwardCompatabilityPart3DUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  public void testDummy() {
  }

  /*
   * The reason this test fires DDLs by connecting to different servers
   * in each run of previous and current servers is to try create conditions
   * where UUIDs generated for DDLs can possibly clash, and to verify against it.
   */
  public void Bug51465testBugs_50076_50136() throws Exception {

    String[] versions = { "1.1.1", "1.1.2" };
    String[] versionDirs = { "sqlfire/releases/SQLFire1.1.1-all",
        "sqlfire/releases/SQLFire1.1.2-all" };

    for (int i = 0; i < versions.length; i++) {
      String version = versions[i];
      String versionDir = versionDirs[i];

      getLogWriter().info("Testing with SQLFire-" + version);

      String currentDir = getSysDirName();
      String locatorDir = currentDir + "/locatorDir";
      String serverOneDir = currentDir + "/serverOneDir";
      String serverTwoDir = currentDir + "/serverTwoDir";
      String serverThreeDir = currentDir + "/serverThreeDir";
      String serverFourDir = currentDir + "/serverFourDir";
      new File(locatorDir).mkdirs();
      new File(serverOneDir).mkdirs();
      new File(serverTwoDir).mkdirs();
      new File(serverThreeDir).mkdirs();
      new File(serverFourDir).mkdirs();

      try {
        product = PRODUCT_SQLFIRE;
        getLogWriter().info(
            "Starting locator version: " + version + " with working dir: "
                + locatorDir);
        ProcessStart versionedLocator = startVersionedLocator(version,
            versionDir, locatorDir, false);
        int baseVersionLocatorPort = versionedLocator.port;

        getLogWriter().info("Starting earlier version servers");

        ProcessStart versionedServer1 = startVersionedServer(version,
            versionDir, serverOneDir, -1, baseVersionLocatorPort, false);
        int clientPort1 = versionedServer1.port;
        ProcessStart versionedServer2 = startVersionedServer(version,
            versionDir, serverTwoDir, -1, baseVersionLocatorPort, false);
        ProcessStart versionedServer3 = startVersionedServer(version,
            versionDir, serverThreeDir, -1, baseVersionLocatorPort, false);
        ProcessStart versionedServer4 = startVersionedServer(version,
            versionDir, serverFourDir, -1, baseVersionLocatorPort, false);
        waitForProcesses(versionedLocator, versionedServer1, versionedServer2,
            versionedServer3, versionedServer4);

        // Add data with the client for that version
        doWithVersionedClient(PRODUCT_SQLFIRE, version, versionDir,
            clientPort1, "createDataForClient");

      } finally {
        getLogWriter().info("Stopping earlier version servers");
        stopVersionedServer(version, versionDir, serverOneDir);
        stopVersionedServer(version, versionDir, serverTwoDir);
        stopVersionedServer(version, versionDir, serverThreeDir);
        stopVersionedServer(version, versionDir, serverFourDir);

        getLogWriter().info("Stopping earlier version locator");
        stopVersionedLocator(version, versionDir, locatorDir);
      }

      getLogWriter().info(
          "VM WORKING DIRS:" + serverOneDir + "," + serverTwoDir + ","
              + serverThreeDir + "," + serverFourDir + "," + locatorDir);

      String[] storeDirs = new String[] { locatorDir, serverOneDir,
          serverTwoDir, serverThreeDir, serverFourDir };
      String[] customDSDirs = new String[] { serverOneDir, serverTwoDir,
          serverThreeDir, serverFourDir };
      String[] ddDirs = new String[] { locatorDir + "/datadictionary",
          serverOneDir + "/datadictionary", serverTwoDir + "/datadictionary",
          serverThreeDir + "/datadictionary", serverFourDir + "/datadictionary" };

      getLogWriter().info("Upgrading disk stores");
      runUpgradeDiskStore("SQLF-DEFAULT-DISKSTORE", storeDirs);
      runUpgradeDiskStore("CUSTOMDS", customDSDirs);
      runUpgradeDiskStore("SQLF-DD-DISKSTORE", ddDirs);

      try {
        getLogWriter().info("Starting current version locator");
        product = PRODUCT_GEMFIREXD;
        ProcessStart currentLocator = startCurrentVersionLocator(locatorDir);
        int currentVersionLocatorPort = currentLocator.port;

        ProcessStart currentServer1 = startCurrentVersionServer(serverOneDir,
            currentVersionLocatorPort);
        ProcessStart currentServer2 = startCurrentVersionServer(serverTwoDir,
            currentVersionLocatorPort);
        int clientPort = currentServer2.port;
        ProcessStart currentServer3 = startCurrentVersionServer(serverThreeDir,
            currentVersionLocatorPort);
        int anotherClientPort = currentServer3.port;
        ProcessStart currentServer4 = startCurrentVersionServer(serverFourDir,
            currentVersionLocatorPort);
        waitForProcesses(currentLocator, currentServer1, currentServer2,
            currentServer3, currentServer4);

        Properties props = new Properties();
        props.put("load-balance", "false");
        Connection connection = this.getNetConnection("localhost", clientPort,
            props);
        Connection anotherConnection = this.getNetConnection("localhost",
            anotherClientPort, props);

        Statement gfxdSt = connection.createStatement();
        Statement anotherGfxdSt = anotherConnection.createStatement();

        getLogWriter().info("Creating tables");
        // Fire more than 4 DDLs to reproduce the record interval problem
        gfxdSt.execute("create table t1_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");
        gfxdSt.execute("create table t2_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) colocate with (t1_temp) "
            + "redundancy 1 persistent 'customds'");
        gfxdSt.execute("create table t3_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");
        anotherGfxdSt.execute("create table t4_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");
        anotherGfxdSt.execute("create table t5_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");

        try {
          // Test for #50136
          getLogWriter().info("Verifying bug #50136");
          gfxdSt.execute("create index idx1_t1_temp on t1_temp(id2, id1)");
          gfxdSt.execute("create index idx2_t1_temp on t1_temp(id1, ts)");
        } catch (Exception e) {
          getLogWriter().info("EXCEPTION=" + e);
          Assert.fail("Exception while creating index:" + e);
        }

        getLogWriter().info("Inserting data");

        PreparedStatement gfxdPs = connection
            .prepareStatement("insert into t1_temp values(?,?,?,?)");
        for (int j = 0; j < 5; j++) {
          int id = j * 100;
          gfxdPs.setString(1, String.valueOf(id));
          gfxdPs.setInt(2, id);
          gfxdPs.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
          gfxdPs.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
          gfxdPs.execute();
        }
        gfxdPs.close();
        PreparedStatement gfxdPs2 = connection
            .prepareStatement("insert into t2_temp values(?,?,?,?)");
        for (int k = 0; k < 5; k++) {
          int id = k * 100;
          gfxdPs2.setString(1, String.valueOf(id));
          gfxdPs2.setInt(2, id);
          gfxdPs2.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
          gfxdPs2.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
          gfxdPs2.execute();
        }
        gfxdPs2.close();

        try {
          // Test for #50016
          getLogWriter().info("Verifying bug #50076 - table qualified");
          gfxdSt.execute("select count(*) from app.t1 outertable "
              + "where outertable.id1 in (select id1 from t1_temp)");
          ResultSet rs = gfxdSt.getResultSet();
          rs.next();
          int count = rs.getInt(1);
          getLogWriter().info("COUNT=" + count);
          assertEquals(5, count);

          getLogWriter().info("Firing the query with IN clause");
          gfxdSt.execute("select count(*) from t1 outertable "
              + "where outertable.id1 in (select id1 from t1_temp)");
          rs = gfxdSt.getResultSet();
          rs.next();
          count = rs.getInt(1);
          getLogWriter().info("COUNT=" + count);
          assertEquals(5, count);
          // Negative test
          gfxdSt.execute("select count(*) from t1 outertable "
              + "where outertable.id1 NOT in (select id1 from t1_temp)");
          rs = gfxdSt.getResultSet();
          rs.next();
          count = rs.getInt(1);
          getLogWriter().info("COUNT=" + count);
          assertEquals(0, count);
        } catch (SQLException e) {
          getLogWriter().info("EXCEPTION=" + e);
          Assert.fail("Exception during query:" + e);
        }
        gfxdSt.close();
        anotherGfxdSt.close();
        connection.close();
        anotherConnection.close();
        getLogWriter().info("Restarting current version servers and locator");
        // Restart GemFireXD and verify new DDLs run alright.
        stopCurrentVersionServer(serverOneDir);
        stopCurrentVersionServer(serverTwoDir);
        stopCurrentVersionServer(serverThreeDir);
        stopCurrentVersionServer(serverFourDir);
        stopCurrentVersionLocator(locatorDir);

        ProcessStart currentLocator_1 = startCurrentVersionLocator(locatorDir);
        int newLocatorPort = currentLocator_1.port;

        ProcessStart currentServer1_1 = startCurrentVersionServer(serverOneDir,
            newLocatorPort);
        int newPort = currentServer1_1.port;
        ProcessStart currentServer2_1 = startCurrentVersionServer(serverTwoDir,
            newLocatorPort);
        int anotherNewPort = currentServer2_1.port;
        ProcessStart currentServer3_1 = startCurrentVersionServer(
            serverThreeDir, newLocatorPort);
        ProcessStart currentServer4_1 = startCurrentVersionServer(
            serverFourDir, newLocatorPort);
        waitForProcesses(currentLocator_1, currentServer1_1, currentServer2_1,
            currentServer3_1, currentServer4_1);

        Connection newConn = this.getNetConnection("localhost", newPort, props);
        Connection anotherNewConn = this.getNetConnection("localhost",
            anotherNewPort, props);

        Statement newSt = newConn.createStatement();
        Statement anotherNewSt = anotherNewConn.createStatement();

        getLogWriter().info("Creating some more tables");
        newSt.execute("create table t6_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");
        newSt.execute("create table t7_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");

        anotherNewSt.execute("create table t8_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");
        anotherNewSt.execute("create table t9_temp(id1 varchar(40) not null, "
            + "id2 integer not null, ts timestamp not null, "
            + "other_ts timestamp, primary key(id1, id2, ts)) "
            + "partition by column(id1, id2, ts) "
            + "redundancy 1 persistent 'customds'");

        try {
          // Test for #50136
          getLogWriter().info("Verifying bug #50136 again");
          newSt.execute("create index idx1_t6_temp on t6_temp(id2, id1)");
          newSt.execute("create index idx2_t6_temp on t6_temp(id1, ts)");
        } catch (Exception e) {
          getLogWriter().info("EXCEPTION=" + e);
          Assert.fail("Exception while creating index:" + e);
        }

        try {
          getLogWriter().info("Verifying bug #50076 again");
          // First check the query without adding any data
          newSt.execute("select count(*) from t1 outertable "
              + "where outertable.id1 in (select id1 from t7_temp)");
          ResultSet rs = newSt.getResultSet();
          rs.next();
          int count = rs.getInt(1);
          getLogWriter().info("COUNT=" + count);
          assertEquals(0, count);

        } catch (Exception e) {
          getLogWriter().info("EXCEPTION:" + e);
          Assert.fail();
        }

        getLogWriter().info("Adding more data");
        // Add data and verify query again.
        PreparedStatement newPs = newConn
            .prepareStatement("insert into t7_temp values(?,?,?,?)");
        for (int k = 0; k < 5; k++) {
          int id = k * 100;
          newPs.setString(1, String.valueOf(id));
          newPs.setInt(2, id);
          newPs.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
          newPs.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
          newPs.execute();
        }
        newPs.close();

        getLogWriter().info("Verifying bug #50016 yet again");
        try {
          newSt.execute("select count(*) from t1 outertable "
              + "where outertable.id1 in (select id1 from t7_temp)");
          ResultSet rs = newSt.getResultSet();
          rs.next();
          int count = rs.getInt(1);
          getLogWriter().info("COUNT=" + count);
          assertEquals(5, count);
        } catch (Exception e) {
          getLogWriter().info("EXCEPTION:" + e);
          Assert.fail();
        }
        newSt.close();
        newConn.close();
      } catch (Exception e) {
        getLogWriter().info("EXCEPTION:" + e);
      } finally {
        getLogWriter().info("Stopping current version servers and locator");
        stopCurrentVersionServer(serverOneDir);
        stopCurrentVersionServer(serverTwoDir);
        stopCurrentVersionServer(serverThreeDir);
        stopCurrentVersionServer(serverFourDir);
        stopCurrentVersionLocator(locatorDir);
        product = null;
      }
      cleanUpCustomStoreFiles(storeDirs);
    }
  }

  public void DISABLED_testBug48761() throws Exception {
    // Create a locator working dir.
    String locatorPath = getSysDirName() + "/locator48761";
    getLogWriter()
        .info("Creating locator dir for base version: " + locatorPath);
    File rollingVersionLocatorDir = new File(locatorPath);
    assertTrue(rollingVersionLocatorDir.mkdir());

    // String currentBuildDir = System.getProperty("JTESTS") + "/../..";
    String currentBuildDir = System.getProperty("JTESTS") + "/../..";
    getLogWriter().info("currentBuildDir=" + currentBuildDir);

    ProcessStart versionedLocator = startVersionedLocator("", currentBuildDir,
        locatorPath, false);
    int locatorPort = versionedLocator.port;

    ProcessStart versionedServer1 = startVersionedServerSG("", currentBuildDir,
        vm1WorkingDir, 0, locatorPort, false, "MYGROUP");
    int clientPort1 = versionedServer1.port;

    ProcessStart versionedServer2 = startVersionedServerSG("", currentBuildDir,
        vm2WorkingDir, 0, locatorPort, false, "MYGROUP");
    waitForProcesses(versionedLocator, versionedServer1, versionedServer2);

    Properties props1 = new Properties();
    props1.setProperty("disable-cancel", "true");
    Connection conn = TestUtil.getNetConnection("localhost", clientPort1, null,
        props1);

    Statement st = conn.createStatement();

    st.execute("create schema apples");

    st.execute("set schema apples");

    String jarFile = TestUtil.getResourcesDir() + "/lib/callback.jar";
    st.execute("call sqlj.install_jar('" + jarFile
        + "', 'apples.callbackjar', 0)");

    jarFile = TestUtil.getResourcesDir() + "/lib/myAsyncJar.jar";
    st.execute("call sqlj.install_jar('" + jarFile
        + "', 'apples.myasyncjar', 0)");

    st.execute("CREATE TABLE command_table "
        + "(ID INT generated always as identity NOT NULL,  "
        + "EXECUTE_ON_GEMFIREXD VARCHAR(1) default 'N',  "
        + "EXECUTE_ON_GREENPLUM VARCHAR(1) default 'Y', "
        + "command_type varchar(10),  " + "COMMAND VARCHAR(800) not null ) "
        + "SERVER GROUPS (MYGROUP) partition by (id) redundancy 1");

    st.execute("call sys.ADD_LISTENER ('CommandTableEventCallBackListenerImpl', "
        + "'apples', "
        + "'command_table', "
        + "'gemfirexd.callbacktests.GfxdTestListener', "
        + "'', "
        + "'MYGROUP')");

    st.execute("create table pipe_lookup (table_name varchar(30) primary key, "
        + "pipe_name varchar(30), external_table varchar(30) ) "
        + "SERVER GROUPS (MYGROUP)");

    st.execute("CREATE ASYNCEVENTLISTENER GENERICASYNCEVENTLISTENER ("
        + "LISTENERCLASS 'jarSource.AsyncListener' "
        + "INITPARAMS '' BATCHSIZE 1000 BATCHTIMEINTERVAL 2000 "
        + "ENABLEPERSISTENCE false "
        + "MAXQUEUEMEMORY 100 ) SERVER GROUPS (MYGROUP)");

    st.execute("call SYS.START_ASYNC_EVENT_LISTENER ('GENERICASYNCEVENTLISTENER')");

    // shut-down-all

    String utilLauncher = getUtilLauncher(currentBuildDir);

    String[] startOps = new String[] { utilLauncher, "shut-down-all",
        "-locators=localhost[" + locatorPort + "]" };

    Process serverProc = new ProcessBuilder(startOps).start();
    int exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " during shut-down-all.");
    }

    // waiting for servers to shut down
    Thread.sleep(5000);

    // stop locator
    getLogWriter().info("Stopping locator from " + currentBuildDir);

    startOps = new String[] { utilLauncher, "locator", "stop",
        "-dir=" + locatorPath };

    serverProc = new ProcessBuilder(startOps).start();
    exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while stopping locator.");
    }

    // start locator and servers
    versionedLocator = startVersionedLocator("", currentBuildDir, locatorPath,
        false);
    locatorPort = versionedLocator.port;

    versionedServer1 = startVersionedServerSG("", currentBuildDir,
        vm1WorkingDir, 0, locatorPort, false, "MYGROUP");
    clientPort1 = versionedServer1.port;

    versionedServer2 = startVersionedServerSG("", currentBuildDir,
        vm2WorkingDir, 0, locatorPort, false, "MYGROUP");
    waitForProcesses(versionedLocator, versionedServer1, versionedServer2);

    // shut-down-all

    startOps = new String[] { utilLauncher, "shut-down-all",
        "-locators=localhost[" + locatorPort + "]" };

    serverProc = new ProcessBuilder(startOps).start();
    exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " during shut-down-all.");
    }

    // wait for servers to shut down
    Thread.sleep(5000);

    // stop locator
    getLogWriter().info("Stopping locator from " + currentBuildDir);

    startOps = new String[] { utilLauncher, "locator", "stop",
        "-dir=" + locatorPath };

    serverProc = new ProcessBuilder(startOps).start();
    exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while stopping locator.");
    }
  }
}
