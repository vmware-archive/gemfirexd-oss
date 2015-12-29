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
package com.pivotal.gemfirexd.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeRegionKey;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMBucketRowLocationThinDiskLRURegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMBucketRowLocationThinDiskRegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMBucketRowLocationThinDiskRegionEntryOffHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMBucketRowLocationThinRegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMLocalRowLocationThinDiskLRURegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMLocalRowLocationThinDiskRegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMLocalRowLocationThinDiskRegionEntryOffHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VMLocalRowLocationThinRegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VersionedBucketRowLocationThinDiskLRURegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VersionedBucketRowLocationThinDiskRegionEntryHeap;
import com.pivotal.gemfirexd.internal.engine.store.entry.VersionedBucketRowLocationThinDiskRegionEntryOffHeap;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * 
 * @author ashahid
 * 
 */
public class TableDiskPersistenceTest extends JdbcTestBase
{

  public TableDiskPersistenceTest(String name) {
    super(name);
  }

  public static void main(String[] args)
  {
    TestRunner.run(new TestSuite(TableDiskPersistenceTest.class));
  }

  /**
   * test the index containing items with the same key value.
   * 
   * @throws SQLException
   */

  public void testSingleColumnPKTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();

    try {

      s
          .execute("create table t1 (c1 int primary key, c2 int, c3 varchar(20) ) "
              +  getSuffix() );

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      // s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c2.add(15);
      c2.add(25);
      c3.add("XXXX");
      c3.add("YYYY");

      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();
      c1.add(10);
      c1.add(20);
      c2.add(15);
      c2.add(25);
      c3.add("XXXX");
      c3.add("YYYY");
      rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());

    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    finally {
      try {
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
      }
      conn.close();

    }

  }

  public void testMultiColumnPKTable() throws SQLException
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), PRIMARY KEY (c1, c2)) "
              +  getSuffix() );

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");

      String path = Misc.getRegionPath(schema, "T1", null);
      LocalRegion rgn = (LocalRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertNull(krc.getKeyBytes());
        assertNotNull(krc.getValueBytes());
        assertNotNull(krc.getTableInfo());
      }

      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      // wait for background tasks to finish
      for (DiskStoreImpl dsi : Misc.getGemFireCache().listDiskStores()) {
        dsi.waitForBackgroundTasks();
      }
      // Once the disk region has initialized, the CompactCompositeRegionKey
      // should have key bytes as null, as table info is set by then
      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertNull(krc.getKeyBytes());
        assertNotNull(krc.getValueBytes());
        assertNotNull(krc.getTableInfo());
      }

      s = conn.createStatement();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      rs = s.executeQuery("select c1, c2, c3 from t1 ");
      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }
      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertNull(krc.getKeyBytes());
        assertNotNull(krc.getValueBytes());
        assertNotNull(krc.getTableInfo());
      }
      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      s.executeUpdate("update t1 set c3 = 'AAAA' where c1 = 30 ");
      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertNull(krc.getKeyBytes());
        assertNotNull(krc.getValueBytes());
        assertNotNull(krc.getTableInfo());
      }

      shutDown();
      // loadDriver();
      conn = getConnection();
      // wait for background tasks to finish
      for (DiskStoreImpl dsi : Misc.getGemFireCache().listDiskStores()) {
        dsi.waitForBackgroundTasks();
      }
      // Once the disk region has initialized, the CompactCompositeRegionKey
      // should have key bytes as null, as table info is set by then

      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertNull(krc.getKeyBytes());
        assertNotNull(krc.getValueBytes());
        assertNotNull(krc.getTableInfo());
      }

      s = conn.createStatement();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("AAAA");
      rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }
      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());

    }
    finally {
      try {
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
      }
      conn.close();
    }
  }

  // Case 1: PR Persistent
  // Case 2 PR Persistent redundant
  // case 3 PR persistent overflow
  // case 4 PR persistent overflow redundant

  // case 5 PR overflow only
  // case 6 PR overflow redundant

  // case 7 PR default
  // case 8 : PR redundant

  // Case 1
  public void testRegionEntryTypeForPersistentNonRedundantPRTable()
      throws Exception
  {
    final Class<?> expectedEntryClass = getExpectedEntryClass();
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))  " +  getSuffix() +" synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");

      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();
      boolean enteredLoop = false;
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(expectedEntryClass, re.getClass());
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(expectedEntryClass, re.getClass());
        enteredLoop = true;
      }

      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }

  }

  private Class<?> getExpectedEntryClass() {
    Class<?> expectedEntryClass;
    boolean persistent = false;
    if(getSuffix().toLowerCase().contains("persistent")){
      persistent = true;
    }
    
    if(getSuffix().trim().toLowerCase().indexOf("offheap") == -1) {
      if(persistent) {
        // Persistent partitioned tables should have versioned region entries
        expectedEntryClass = VersionedBucketRowLocationThinDiskRegionEntryHeap.class;
      } else {
        expectedEntryClass = VMBucketRowLocationThinDiskRegionEntryHeap.class;
      }
    }else {
      if(persistent) {
        // Persistent partitioned tables should have versioned region entries
        expectedEntryClass = VersionedBucketRowLocationThinDiskRegionEntryOffHeap.class;
      } else {
        expectedEntryClass = VMBucketRowLocationThinDiskRegionEntryOffHeap.class;
      }
    }
    return expectedEntryClass;
  }

  // case 2
  public void testRegionEntryTypeForPersistentRedundantPRTable()
      throws Exception
  {
    Class<?> expectedEntryClass = getExpectedEntryClass();
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2))  "
              +  getSuffix() +" synchronous redundancy 1 ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");

      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      boolean enteredLoop = false;
      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), expectedEntryClass);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;

      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), expectedEntryClass);
        enteredLoop = true;
      }

      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }

  }

  // case 3
  public void testRegionEntryTypeForPersistentOverflowPRTable()
      throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2))  eviction by lrucount 1 evictaction overflow"
              +  getSuffix() + " synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        // Persistent partitioned tables should have versioned region entries
        assertEquals(re.getClass(), VersionedBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VersionedBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 4
  public void testRegionEntryTypeForPersistentOverflowRedundantPRTable()
      throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2)) redundancy 1 eviction by lrucount 1 evictaction overflow"
              +  getSuffix() +" synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VersionedBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VersionedBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 5
  public void testRegionEntryTypeForOverflowPRTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2))  eviction by lrucount 1 evictaction overflow"
              + "  synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());

    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 6
  public void testRegionEntryTypeForOverflowRedundantPRTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2))  eviction by lrucount 1 evictaction overflow"
              + "  synchronous redundancy 1");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMBucketRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());

    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 7
  public void testRegionEntryTypeForDefaultPRTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2)) ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(VMBucketRowLocationThinRegionEntryHeap.class, re.getClass());
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();
      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 8
  public void testRegionEntryTypeForPRRedundantTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))  redundancy 1  ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      PartitionedRegion rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMBucketRowLocationThinRegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (PartitionedRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());

    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // Case 9: Replicate Persistent Overflow
  // Case 10 Replicate persistent
  // case 11 replicate overflow
  // case 12 replicate only

  // case 9
  public void testRegionEntryTypeForPersistentOverflowReplicateTable()
      throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2))  eviction by lrucount 1 evictaction overflow"
              + " replicate  " +getSuffix() + " synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      LocalRegion rgn = (LocalRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMLocalRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMLocalRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 10
  public void testRegionEntryTypeForPersistentReplicateTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    
    Class<?> expectedEntryClass = null;
    if(getSuffix().trim().toLowerCase().indexOf("offheap") == -1) {
      expectedEntryClass = VMLocalRowLocationThinDiskRegionEntryHeap.class;
    }else {
      expectedEntryClass = VMLocalRowLocationThinDiskRegionEntryOffHeap.class;
    }
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))  " + " replicate  " +getSuffix() +" synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      LocalRegion rgn = (LocalRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), expectedEntryClass);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), expectedEntryClass);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 11
  public void testRegionEntryTypeForOverflowReplicateTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s
          .execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
              + "PRIMARY KEY (c1, c2)) replicate  eviction by lrucount 1 evictaction overflow"
              + " synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      LocalRegion rgn = (LocalRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMLocalRowLocationThinDiskLRURegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  // case 12
  public void testRegionEntryTypeForReplicateTable() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    String schema = TestUtil.getCurrentDefaultSchemaName();
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))   replicate  ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      boolean enteredLoop = false;
      String path = Misc.getRegionPath(schema, "T1", null);
      LocalRegion rgn = (LocalRegion)Misc.getRegion(path, true, false);
      Iterator<CompactCompositeRegionKey> itr = rgn.keys().iterator();

      while (itr.hasNext()) {
        CompactCompositeRegionKey krc = itr.next();
        assertTrue(krc.getKeyBytes() == null || krc.getValueBytes() == null);
        assertTrue(krc.getKeyBytes() != null || krc.getValueBytes() != null);
        assertNotNull(krc.getTableInfo());
      }
      Iterator<?> entryItr = rgn.getDataView().getLocalEntriesIterator(
          (InternalRegionFunctionContext)null, true, false, true, rgn);
      while (entryItr.hasNext()) {
        RowLocation re = (RowLocation)entryItr.next();
        assertEquals(re.getClass(), VMLocalRowLocationThinRegionEntryHeap.class);
        enteredLoop = true;
      }
      assertTrue(enteredLoop);
      enteredLoop = false;
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      conn.close();
      shutDown();
      // loadDriver();
      conn = getConnection();
      s = conn.createStatement();

      rgn = (LocalRegion)Misc.getRegion(path, true, false);
      itr = rgn.keys().iterator();
      assertFalse(itr.hasNext());
    }
    finally {
      try {
        s = conn.createStatement();
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
        ignore.printStackTrace();
      }
      conn.close();
    }
  }

  public void testPersistentTableDropBehaviour() throws Exception
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    TestUtil.getCurrentDefaultSchemaName();
    try {
      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))  "+  getSuffix() +" synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      Set c1 = new HashSet();
      Set c2 = new HashSet();
      Set c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      ResultSet rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
      s.execute("drop table t1");
      this.waitTillAllClear();
      conn.close();
      shutDown();
      conn = getConnection();
      s = conn.createStatement();
      TestUtil.getCurrentDefaultSchemaName();

      s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))   "+ getSuffix() +" synchronous ");

      s.execute("insert into t1 (c1, c2, c3) values (10, 15, 'YYYY')");
      s.execute("insert into t1 (c1, c2, c3) values (20, 25, 'XXXX')");
      s.execute("insert into t1 (c1, c2, c3) values (30, 35, 'ZZZZ')");
      c1 = new HashSet();
      c2 = new HashSet();
      c3 = new HashSet();
      c1.add(10);
      c1.add(20);
      c1.add(30);
      c2.add(15);
      c2.add(25);
      c2.add(35);
      c3.add("XXXX");
      c3.add("YYYY");
      c3.add("ZZZZ");
      rs = s.executeQuery("select c1, c2, c3 from t1 ");

      while (rs.next()) {
        assertTrue(c1.remove(rs.getInt(1)));
        assertTrue(c2.remove(rs.getInt(2)));
        assertTrue(c3.remove(rs.getString(3)));
      }

      assertFalse(rs.next());
      assertTrue(c1.isEmpty() && c2.isEmpty() && c3.isEmpty());
    }
    finally {
      try {
        s.execute("drop table t1");
        this.waitTillAllClear();
      }
      catch (SQLException ignore) {
      }
      conn.close();
    }

  }
  
  public void testSystableOffHeapAttribute() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    TestUtil.getCurrentDefaultSchemaName();    
    s.execute("create table t1 (c1 int , c2 int, c3 varchar(20), "
          + "PRIMARY KEY (c1, c2))  "+  getSuffix() +" synchronous ");
    ResultSet rs = s.executeQuery("select * from sys.systables where " +
    		"TABLENAME = 'T1'");
    assertTrue(rs.next());
    System.out.println(rs.getString("DISKATTRS"));
    if(this.getSuffix().toLowerCase().indexOf("offheap") == -1) {
      assertFalse(rs.getBoolean("OFFHEAPENABLED"));
    }else {
      assertTrue(rs.getBoolean("OFFHEAPENABLED"));
    }
  }
  
  public String getSuffix() {
    return " persistent ";
  }

  public void waitTillAllClear() {}
}
