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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdCacheLoader;
import com.pivotal.gemfirexd.jdbc.GfxdCallbacksTest;

@SuppressWarnings("serial")
public class GfxdLoaderDUnit extends DistributedSQLTestBase {

  public GfxdLoaderDUnit(String name) {
    super(name);
  }

  public void testLoaderWhenPkIsNotPartition() throws Exception {
    // Start one client and one server
    startVMs(1, 3);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    // Check for PARTITION BY RANGE
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))"
        + " PARTITION By Column(SECONDID)");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdTestRowLoader",
        null);
    sqlExecuteVerify(new int[] { 1 }, null,
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get1");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get2");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get3");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get");

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    CacheLoader ldr = rattr.getCacheLoader();
    GfxdCacheLoader gfxdldr = (GfxdCacheLoader)ldr;
    assertNull(gfxdldr);
    assertEquals("Number of entries expected to be 3", regtwo.size(), 3);
  }
  
  public void testLoaderWhenPkIsNotPartition_multipleConcGets() throws Exception {
    // Start one client and one server
    startVMs(2, 2);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    // Check for PARTITION BY RANGE
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))"
        + " PARTITION By Column(SECONDID)");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdTestRowLoader",
        null);
    doConcurrently();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    assertEquals("Number of entries expected to be 3", 3, regtwo.size());
  }

  public void testLoaderWhenPkIsPartition() throws Exception {
    // Start one client and one server
    startVMs(1, 3);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = Misc.getGemFireCache();

    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdTestRowLoader",
        null);
    sqlExecuteVerify(new int[] { 1 }, null,
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get1");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get2");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get3");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "loader_get");

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    CacheLoader ldr = rattr.getCacheLoader();
    GfxdCacheLoader gfxdldr = (GfxdCacheLoader)ldr;
    assertNull(gfxdldr);
    assertEquals("Number of entries expected to be 3", regtwo.size(), 3);
  }

  public void testLoaderWhenPkIsPartition_multipleConcGets() throws Exception {
    // Start one client and one server
    startVMs(2, 2);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    // Check for PARTITION BY RANGE
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))"
        + " PARTITION By Column(ID)");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdTestRowLoader",
        null);
    doConcurrently();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    CacheLoader ldr = rattr.getCacheLoader();
    GfxdCacheLoader gfxdldr = (GfxdCacheLoader)ldr;
    assertNull(gfxdldr);
    assertEquals("Number of entries expected to be 3", 3, regtwo.size());
  }

  private void doConcurrently() {
    Runnable task1 = new Runnable() {
      public void run() {
        try {
          sqlExecuteVerify(null, new int[] {1, 2},
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get1");
          sqlExecuteVerify(new int[] { 1 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get2");
          sqlExecuteVerify(new int[] { 1 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get3");

          sqlExecuteVerify(null, new int[] {1, 2},
              "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get");
        } catch (Exception ex) {
          fail("failed while doing concurrent verification", ex);
        }
      }
    };
    Runnable task2 = new Runnable() {
      public void run() {
        try {
          sqlExecuteVerify(new int[] { 2 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get1");
          sqlExecuteVerify(new int[] { 2 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get2");
          sqlExecuteVerify(new int[] { 2 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get3");

          sqlExecuteVerify(new int[] { 2 }, null,
              "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
                  + "/lib/checkCreateTable.xml", "loader_get");
        } catch (Exception ex) {
          fail("failed while doing concurrent verification", ex);
        }
      }
    };

    Thread td1 = new Thread(task1);
    Thread td2 = new Thread(task2);
    td1.start();
    td2.start();
    try {
      td1.join(10000);
      td2.join(10000);
    } catch (InterruptedException ex) {
      fail("failed while waiting to join");
    }
  }

  public void testTransactionalBehaviourOfCacheLoaderOnReplicate_Bug42914()
      throws Exception {
    runTransactionalBehaviourOfCacheLoader_Bug42914(true);
  }

  public void testTransactionalBehaviourOfCacheLoaderOnPartitionedTable_Bug42914()
      throws Exception {
    runTransactionalBehaviourOfCacheLoader_Bug42914(false);
  }

  private void runTransactionalBehaviourOfCacheLoader_Bug42914(
      final boolean isReplicated) throws Exception {
    // Start one client and some servers
    startVMs(1, 3);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Controller VM
    final String suffix = isReplicated ? "replicate" : "redundancy 1";
    String createTable = "create table EMP.TESTTABLE (ID int primary key, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024), ID1 int) "
        + suffix;
    clientSQLExecute(1, createTable);
    GfxdCallbacksTest.addLoader("EMP", "TESTTABLE",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$GfxdTestRowLoader",
        "");
    // Test insert propagation by inserting in a data store node of DS.DS0
    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(false);
    String query = "select * from emp.testtable where ID = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    Statement stmt = conn.createStatement();
    ResultSet rs;

    for (int i = 0; i < 20; i++) {
      ps.setInt(1, i);
      rs = ps.executeQuery();
      rs.next();
      assertEquals(rs.getInt(1), i);
      assertFalse(rs.next());
    }
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select count(*) from emp.testtable", null, "0");
    // rollback and check no data
    conn.rollback();
    rs = stmt.executeQuery("select * from emp.testtable");
    assertFalse(rs.next());
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select count(*) from emp.testtable", null, "0");

    // now populate again and check successful loads
    for (int i = 0; i < 20; i++) {
      ps.setInt(1, i);
      rs = ps.executeQuery();
      rs.next();
      assertEquals(rs.getInt(1), i);
      assertFalse(rs.next());
    }
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select count(*) from emp.testtable", null, "0");
    rs = stmt.executeQuery("select count(*) from emp.testtable");
    assertTrue(rs.next());
    assertEquals(20, rs.getInt(1));
    assertFalse(rs.next());
    conn.commit();
    conn.close();

    // now check commit of loaded data
    conn = TestUtil.getConnection();
    stmt = conn.createStatement();
    ps = conn.prepareStatement(query);

    rs = stmt.executeQuery("select id from emp.testtable");
    TreeSet<Integer> ids = new TreeSet<Integer>();
    for (int i = 0; i < 20; i++) {
      assertTrue("failed next for i=" + i, rs.next());
      ids.add(Integer.valueOf(rs.getInt(1)));
    }
    assertFalse(rs.next());
    assertEquals(20, ids.size());
    assertEquals(0, ids.first().intValue());
    assertEquals(19, ids.last().intValue());

    for (int i = 0; i < 20; i++) {
      ps.setInt(1, i);
      rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
      assertFalse(rs.next());
    }
    rs = stmt.executeQuery("select count(*) from emp.testtable");
    assertTrue(rs.next());
    assertEquals(20, rs.getInt(1));
    assertFalse(rs.next());
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select count(*) from emp.testtable", null, "20");
  }
}
