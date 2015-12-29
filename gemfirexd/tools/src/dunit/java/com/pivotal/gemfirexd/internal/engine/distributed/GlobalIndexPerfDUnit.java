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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

/**
 * @author kneeraj
 * 
 */
@SuppressWarnings("serial")
public class GlobalIndexPerfDUnit extends DistributedSQLTestBase {

  public GlobalIndexPerfDUnit(String name) {
    super(name);
  }

  private static long timeTookWithCaching;

  private static long timeTookWithoutCaching;

  // The below dummy test is just that this does not fail
  // The actual perf test written below it can be enabled whenever required.
  public void testDummy() throws Exception {
    
  }
  
  public void _testSelectsPerfNoCaching() throws Exception {
    startVMs(1, 3);
    runSelectsToGetTest(false);
  }

  public void _testSelectsPerfCaching() throws Exception {
    startVMs(1, 3);
    try {
      invokeInEveryVM(GlobalIndexPerfDUnit.class, "setCachingInAllVMs",
          new Object[] { Boolean.TRUE });
      runSelectsToGetTest(true);
      getLogWriter().info("Time taken to run 10K selects without caching = " + timeTookWithoutCaching);
      getLogWriter().info("Time taken to run 10K selects with caching = " + timeTookWithCaching);
    } finally {
      invokeInEveryVM(GlobalIndexPerfDUnit.class, "setCachingInAllVMs",
          new Object[] { Boolean.FALSE });
    }
  }

  private void runSelectsToGetTest(boolean caching) throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
        + " partition by column(address) redundancy 1");
    insertTenKRows(conn);
    // warmup
    runTenKSelects(conn, false, false);
    // warmup
    runTenKSelects(conn, false, false);
    // take stats
    runTenKSelects(conn, true, caching);
  }

  private void runTenKSelects(Connection conn, boolean recordTime,
      boolean caching) throws SQLException {
    PreparedStatement ps = conn
        .prepareStatement("select * from TESTTABLE where ID = ?");
    long startTime = 0;
    long endTime = 0;
    if (recordTime) {
      startTime = System.currentTimeMillis();
    }
    for (int i = 0; i < 10000; i++) {
      ps.setInt(1, i);
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());
      assertFalse(rs.next());
    }
    if (recordTime) {
      endTime = System.currentTimeMillis();
      long diff = endTime - startTime;
      if (caching) {
        getLogWriter().info("setting time took with caching with val="+diff);
        timeTookWithCaching = diff;
      }
      else {
        getLogWriter().info("setting time took without caching with val="+diff);
        timeTookWithoutCaching = diff;
      }
    }
  }

  private void insertTenKRows(Connection conn) throws SQLException {
    PreparedStatement ps = conn
        .prepareStatement("insert into TESTTABLE values (?, ?, ?)");
    for (int i = 0; i < 10000; ++i) {
      ps.setInt(1, i);
      ps.setString(2, "descp" + i);
      ps.setString(3, "addr" + i);
      ps.executeUpdate();
    }
  }

  static void setCachingInAllVMs(boolean set) {
    if (set) {
      System.setProperty("gemfirexd.cacheGlobalIndex", "true");
    }
    else {
      System.clearProperty("gemfirexd.cacheGlobalIndex");
    }
  }
  
  @Override
  protected String reduceLogging() {
    return "config";
  }
}
