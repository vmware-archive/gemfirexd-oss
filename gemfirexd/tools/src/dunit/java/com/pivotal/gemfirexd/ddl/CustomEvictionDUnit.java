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
package com.pivotal.gemfirexd.ddl;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

public class CustomEvictionDUnit extends DistributedSQLTestBase {

  private static final long serialVersionUID = 1L;

  public CustomEvictionDUnit(String name) {
    super(name);
  }

  public void testHDFSEviction() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    final File homeDirFile = new File(".", "myhdfs");
    final String homeDir = homeDirFile.getAbsolutePath();

    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    ResultSet rs;
    boolean success = false;
    try {
      checkDirExistence(homeDir);
      st.execute("create hdfsstore myhdfs namenode 'localhost' homedir '" +
          homeDir + "'");
      st.execute("create table app.m1 (col1 int primary key , col2 int, " +
          "col3 int, constraint uq unique (col2, col3)) REDUNDANCY 1 " +
          "hdfsstore (myhdfs) eviction by criteria (col2 > 5) EVICT INCOMING");

      // check impact on GfxdIndexManager
      // st.execute("create index app.idx1 on app.m1(col2)");
      st.execute("insert into app.m1 values (1, 2, 3)");
      st.execute("insert into app.m1 values (11, 22, 33)");

      // update
      st.execute("update app.m1 set col2=33 where col1=11");
      // insert
      st.execute("insert into app.m1 values (66, 77, 88)");

      // verify
      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n where col2 = 22 ");
      rs = st.getResultSet();
      assertFalse(rs.next());

      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n where col2 = 33 ");
      rs = st.getResultSet();
      assertTrue(rs.next());
      assertEquals(33, rs.getInt(2));
      assertEquals(33, rs.getInt(3));
      assertFalse(rs.next());

      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n where col2 = 77 ");
      rs = st.getResultSet();
      assertTrue(rs.next());
      assertEquals(77, rs.getInt(2));
      assertEquals(88, rs.getInt(3));
      assertFalse(rs.next());

      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n");
      rs = st.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(3, count);

      // make sure data is written to HDFS
      String qname = HDFSStoreFactoryImpl.getEventQueueName("/APP/M1");
      st.execute("CALL SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('" + qname + "', 1, 0)");

      // shutdown and restart
      stopAllVMs();
      restartVMNums(-1, -2);
      restartVMNums(1);

      conn = TestUtil.getConnection();
      st = conn.createStatement();

      // verify
      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n where col1 = 11");
      rs = st.getResultSet();
      assertTrue(rs.next());
      assertEquals(33, rs.getInt(2));
      assertEquals(33, rs.getInt(3));
      assertFalse(rs.next());

      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n where col1 = 66");
      rs = st.getResultSet();
      assertTrue(rs.next());
      assertEquals(77, rs.getInt(2));
      assertEquals(88, rs.getInt(3));
      assertFalse(rs.next());

      st.execute("select * from app.m1 -- GEMFIREXD-PROPERTIES " +
          "queryHDFS=true \n");
      rs = st.getResultSet();
      count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(3, count);
      success = true;
    } finally {
      if (!success) {
        conn = TestUtil.getConnection();
        st = conn.createStatement();
      }
      st.execute("drop table if exists app.m1");
      st.execute("drop hdfsstore if exists myhdfs");
      delete(homeDirFile);
    }
  }

  public void testHDFSEvictionGlobalIndex() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    final File homeDirFile = new File(".", "myhdfs");
    final String homeDir = homeDirFile.getAbsolutePath();

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    ResultSet rs;
    boolean success = false;

    try {
      checkDirExistence(homeDir);
      stmt.execute("create hdfsstore myhdfs namenode 'localhost' homedir '" +
          homeDir + "'");
      stmt.execute("create table trade.portfolio (cid int not null, " +
          "sid int not null, qty int not null, availQty int not null, " +
          "subTotal decimal(30,20), tid int, constraint portf_pk " +
          "primary key (cid, sid), constraint qty_ck check (qty>=0), " +
          "constraint avail_ch check (availQty>=0 and availQty<=qty)) " +
          "persistent hdfsstore (myhdfs) EVICTION BY CRITERIA (qty > 500) " +
          "EVICT INCOMING");

      stmt.executeUpdate("insert into trade.portfolio values(3, 120, 1592, " +
          "1592, 14264.32000000000000000000 ,18)");
      try {
        stmt.executeUpdate("insert into trade.portfolio  values(3, 120, " +
            "1374, 1374, 12311.04000000000000000000, 18)");
        fail("failed");
      } catch (Exception c) {
        c.printStackTrace();
      }

      stmt.execute("delete from trade.portfolio where cid = 3 and " +
          "sid = 120 and tid = 18");
      rs = stmt.executeQuery("select * from trade.portfolio -- " +
          "GEMFIREXD-PROPERTIES queryHDFS=true ");
      assertFalse(rs.next());
      stmt.executeUpdate("insert into trade.portfolio values(3, 120, 1374, " +
          "1374, 12311.04000000000000000000, 8)");
      success = true;
    } finally {
      if (!success) {
        conn = TestUtil.getConnection();
        stmt = conn.createStatement();
      }
      stmt.execute("drop table if exists trade.portfolio");
      stmt.execute("drop hdfsstore if exists myhdfs");
      delete(homeDirFile);
    }
  }

  /**
   * 1. do insert which is evicted
   * 2. do update which brings it up in operational data
   * 3. bring down the primary node
   * 4. on secondary make the query on operational data and the row should be returned.
   */
  public void testHDFSEvictionHA() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);

    final File homeDirFile = new File(".", "myhdfs");
    final String homeDir = homeDirFile.getAbsolutePath();

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    boolean success = false;
    try {
      checkDirExistence(homeDir);
      stmt.execute("create hdfsstore myhdfs namenode 'localhost' homedir '" +
          homeDir + "'");
      stmt.execute("create table trade.portfolio (cid int primary key, " +
          "sid int not null) BUCKETS 1 redundancy 2 persistent " +
          "hdfsstore (myhdfs) EVICTION BY CRITERIA (sid > 500) EVICT INCOMING");

      stmt.executeUpdate("insert into trade.portfolio  values(1, 600)");
      stmt.executeUpdate("update trade.portfolio  set sid=100 where cid=1");
      success = true;
    } finally {
      if (!success) {
        conn = TestUtil.getConnection();
        stmt = conn.createStatement();
      }
      stmt.execute("drop table if exists trade.portfolio");
      stmt.execute("drop hdfsstore if exists myhdfs");
      delete(homeDirFile);
    }
  }

  /**
   * 1. table with global index
   * 2. do insert which is evicted
   * 3. do update which brings it up in operational data
   * 4. bring down the primary node
   * 5. on secondary make the query on operational data and the row should be returned.
   * 6.
   */
  public void testHDFSEvictionHAGlobalIndex() {
    // TODO
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  // Assume no other thread creates the directory at the same time
  private void checkDirExistence(String path) {
    File dir = new File(path);
    if (dir.exists()) {
      delete(dir);
    }
  }
}
