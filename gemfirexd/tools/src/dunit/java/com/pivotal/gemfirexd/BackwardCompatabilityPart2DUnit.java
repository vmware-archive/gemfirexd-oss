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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert;

import com.gemstone.gemfire.internal.shared.NativeCalls;

/**
 * Split out from BackwardCompatabilityDUnit to avoid timeouts due to long
 * running times of the tests.
 */
@SuppressWarnings("serial")
public class BackwardCompatabilityPart2DUnit extends
    BackwardCompatabilityTestBase {

  public BackwardCompatabilityPart2DUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  private final String myjar = TestUtil.getResourcesDir()
      + "/lib/myjar.jar";

  public void testDummy() {
  }

  public void DISABLED_testBug50141() throws Exception {

    String[] versions = {
        "1.1.1", 
        "1.1.2"
        };
    String[] versionDirs = {
        "sqlfire/releases/SQLFire1.1.1-all", 
        "sqlfire/releases/SQLFire1.1.2-all"
        };
    
    for(int i = 0; i < versions.length; i++) {
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
        doWithVersionedClient(PRODUCT_SQLFIRE, version, versionDir, clientPort1, "createDataForClient");        
      } catch (Exception e) {
        getLogWriter().info(e);
        Assert.fail("Exception:"+e);
      }
      finally {
        getLogWriter().info("Stopping earlier version servers");
        stopVersionedServer(version, versionDir, serverOneDir);
        stopVersionedServer(version, versionDir, serverTwoDir);
        stopVersionedServer(version, versionDir, serverThreeDir);
        stopVersionedServer(version, versionDir, serverFourDir);
        getLogWriter().info("Stopping earlier version locator");    
        stopVersionedLocator(version, versionDir, locatorDir);
      }
      
      getLogWriter().info(
          "VM WORKING DIRS:" + serverOneDir + "," + serverTwoDir + "," + serverThreeDir + "," + serverFourDir + "," + locatorDir);

      String[] storeDirs = new String[] { locatorDir, serverOneDir,
          serverTwoDir, serverThreeDir, serverFourDir };
      String[] customDSDirs = new String[]{serverOneDir, serverTwoDir, serverThreeDir, serverFourDir };
      String[] ddDirs = new String[] { locatorDir + "/datadictionary",
          serverOneDir + "/datadictionary", 
          serverTwoDir + "/datadictionary",
          serverThreeDir + "/datadictionary",
          serverFourDir + "/datadictionary"
          };
      
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
        int clientPort = currentServer1.port;
        ProcessStart currentServer2 = startCurrentVersionServer(serverTwoDir,
            currentVersionLocatorPort);
        ProcessStart currentServer3 = startCurrentVersionServer(serverThreeDir,
            currentVersionLocatorPort);
        ProcessStart currentServer4 = startCurrentVersionServer(serverFourDir,
            currentVersionLocatorPort);
        waitForProcesses(currentLocator, currentServer1, currentServer2,
            currentServer3, currentServer4);

        Properties props = new Properties();
        props.put("load-balance", "false");
        Connection connection = this.getNetConnection("localhost", clientPort,
            new Properties());

        Statement gfxdSt = connection.createStatement();

        getLogWriter().info("Creating tables");
        // Fire more than 4 DDLs to reproduce the record interval problem
        gfxdSt
            .execute("create table t1_temp(id1 varchar(40) not null, id2 integer not null, ts timestamp not null, other_ts timestamp, primary key(id1, id2, ts))"
                + " partition by column(id1, id2, ts) redundancy 1 persistent 'customds'");
        gfxdSt
            .execute("create table t2_temp(id1 varchar(40) not null, id2 integer not null, ts timestamp not null, other_ts timestamp, primary key(id1, id2, ts))"
                + " partition by column(id1, id2, ts) colocate with (t1_temp) redundancy 1 persistent 'customds'");
        gfxdSt
            .execute("create table t3_temp(id1 varchar(40) not null, id2 integer not null, ts timestamp not null, other_ts timestamp, primary key(id1, id2, ts))"
                + " partition by column(id1, id2, ts) redundancy 1 persistent 'customds'");

        // Verify the table t1_temp is created
        ResultSet metadataRs = connection.getMetaData().getTables(null, "APP",
            "T1_TEMP", null);
        boolean foundTable = false;
        while (metadataRs.next()) {
          foundTable = metadataRs.getString(3).equalsIgnoreCase("T1_TEMP")
              && metadataRs.getString(2).equalsIgnoreCase("APP");
          assertTrue(foundTable);
        }
        metadataRs.close();

        // Try to drop the collocated table
        try {
          getLogWriter().info("Testing for Bug #50141");
          getLogWriter().info("Attempting to drop table t2_temp");
          gfxdSt.execute("drop table t2_temp");
          getLogWriter().info("Attempting to drop table t1_temp");
          gfxdSt.execute("drop table t1_temp");
        } catch (Exception e) {
          getLogWriter().info(e);
          throw e;
        }

        getLogWriter().info("Verifying that the tables are deleted");

        metadataRs = connection.getMetaData().getTables(null, "APP", "T1_TEMP",
            null);
        foundTable = false;
        while (metadataRs.next()) {
          foundTable = metadataRs.getString(3).equalsIgnoreCase("T1_TEMP")
              && metadataRs.getString(2).equalsIgnoreCase("APP");
          assertFalse(foundTable);
        }
        metadataRs = connection.getMetaData().getTables(null, "APP", "T2_TEMP",
            null);
        foundTable = false;
        while (metadataRs.next()) {
          foundTable = metadataRs.getString(3).equalsIgnoreCase("T2_TEMP")
              && metadataRs.getString(2).equalsIgnoreCase("APP");
          assertFalse(foundTable);
        }
        gfxdSt.close();

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

  // Rolling upgrade: test that makes sure that a DAP can be called
  // on a cluster that has previous GemFireXD and and current version servers
  public void DISABLED_testBug50233() throws Exception {
    final boolean isWindows = NativeCalls.getInstance().getOSType().isWindows();
    String[] prevVersions = new String[] {
        "1.0",
        "1.3.1"
    };
    String[] prevVersionDirs = new String[] {
        // GFXD 1.0.X was not released for Windows
        isWindows ? null : "gemfireXD/releases/GemFireXD1.0.0-all",
        isWindows ? "gemfireXD/releases/GemFireXD1.3.1-all/Windows_NT"
            : "gemfireXD/releases/GemFireXD1.3.1-all/Linux"
    };
    String[] products = new String[] {
        PRODUCT_GEMFIREXD,
        PRODUCT_GEMFIREXD
    };

    String currentDir = getSysDirName();
    String locatorDir = currentDir + "/locatorDir";
    String serverOneDir = currentDir + "/serverOneDir";
    String serverTwoDir = currentDir + "/serverTwoDir";
    String serverThreeDir = currentDir + "/serverThreeDir";
    String serverFourDir = currentDir + "/serverFourDir";

    for (int v = 0; v < prevVersions.length; v++) {

    String prevVersion = prevVersions[v];
    String prevVersionDir = prevVersionDirs[v];
    product = products[v];

    if (prevVersionDir == null) {
      continue;
    }

    new File(locatorDir).mkdirs();
    new File(serverOneDir).mkdirs();
    new File(serverTwoDir).mkdirs();
    new File(serverThreeDir).mkdirs();
    new File(serverFourDir).mkdirs();
    String[] dirs = new String[] { locatorDir, serverOneDir, serverTwoDir,
        serverThreeDir, serverFourDir };

    try {
      ProcessStart versionedLocator = startVersionedLocator(prevVersion,
          prevVersionDir, locatorDir, false);
      int baseVersionLocatorPort = versionedLocator.port;
      ProcessStart versionedServer1 = startVersionedServer(prevVersion,
          prevVersionDir, serverOneDir, -1, baseVersionLocatorPort, false);
      ProcessStart versionedServer2 = startVersionedServer(prevVersion,
          prevVersionDir, serverTwoDir, -1, baseVersionLocatorPort, false);
      waitForProcesses(versionedLocator, versionedServer1, versionedServer2);
      // Add the next two servers with current version
      ProcessStart currentServer1 = startCurrentVersionServer(serverThreeDir,
          baseVersionLocatorPort);
      int clientPort1 = currentServer1.port;
      ProcessStart currentServer2 = startCurrentVersionServer(serverFourDir,
          baseVersionLocatorPort);
      waitForProcesses(currentServer1, currentServer2);

      Properties props = new Properties();
      props.setProperty("disable-cancel", "true");
      props.put("load-balance", "false");
      Connection conn = TestUtil.getNetConnection("localhost", clientPort1,
          null, props);
      Statement stmt = conn.createStatement();

      stmt.execute("call sqlj.install_jar('" + myjar + "', 'app.sample2', 0)");

      String ddl = "create table EMP.PARTITIONTESTTABLE (ID int NOT NULL,"
          + " SECONDID int not null, THIRDID varchar(10) not null,"
          + " PRIMARY KEY (SECONDID, THIRDID)) PARTITION BY COLUMN (ID)";

      stmt.execute(ddl);

      PreparedStatement ps = conn.prepareStatement("INSERT INTO "
          + "EMP.PARTITIONTESTTABLE VALUES (?, ?, ?)");
      for (int i = 1; i < 100; i++) {
        ps.setInt(1, i);
        ps.setInt(2, i);
        ps.setString(3, "" + i);
        ps.execute();
      }

      stmt.execute("CREATE PROCEDURE MergeSort () "
          + "LANGUAGE JAVA PARAMETER STYLE JAVA "
          + "READS SQL DATA DYNAMIC RESULT SETS 1 "
          + "EXTERNAL NAME 'myexamples.MergeSortProcedure.mergeSort' ");

      stmt.execute("CREATE ALIAS MergeSortProcessor "
          + "FOR 'myexamples.MergeSortProcessor'");

      String sql = "CALL MergeSort() "
          + "WITH RESULT PROCESSOR MergeSortProcessor "
          + "ON TABLE EMP.PARTITIONTESTTABLE";

      CallableStatement cs = conn.prepareCall(sql);
      cs.execute();

      cs.getResultSet();
      cs.close();
    } catch (Exception e) {
      getLogWriter().info(e);
      Assert.fail("EXCEPTION=" + e);
    } finally {
      stopVersionedServer(prevVersion, prevVersionDir, serverOneDir);
      stopVersionedServer(prevVersion, prevVersionDir, serverTwoDir);
      stopCurrentVersionServer(serverThreeDir);
      stopCurrentVersionServer(serverFourDir);
      stopVersionedLocator(prevVersion, prevVersionDir, locatorDir);
      cleanUp(dirs);
      product = null;
    }

    }
  }
}
