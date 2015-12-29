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

public class GfxdJDBCRowLoaderDUnit extends DistributedSQLTestBase {

  public GfxdJDBCRowLoaderDUnit(String name) {
    super(name);
  }

  public void testJDBCLoaderWhenPkIsNotPartition() throws Exception {
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
    
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "insert into emp.testtable_one values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable_one values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable_one values(3,4,5)");    
        
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdJDBCRowLoader",
        "|url=jdbc|query-string=SELECT * FROM EMP.TESTTABLE_ONE WHERE ID=?");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_1");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_2");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_3");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get");

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    CacheLoader ldr = rattr.getCacheLoader();
    GfxdCacheLoader gfxdldr = (GfxdCacheLoader)ldr;
    assertNull(gfxdldr);
    assertEquals("Number of entries expected to be 3", regtwo.size(), 3);
  }
  
  private void doJDBCRowLoaderConcurrently() {
    Runnable task1 = new Runnable() {
      public void run() {
        try {
          sqlExecuteVerify(null, new int[] {1, 2},
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get_1");
          sqlExecuteVerify(new int[] { 1 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get_2");
          sqlExecuteVerify(new int[] { 1 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get_3");

          sqlExecuteVerify(null, new int[] {1, 2},
              "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get");
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
                  + "/lib/checkEventCallback.xml", "loader_get_1");
          sqlExecuteVerify(new int[] { 2 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get_2");
          sqlExecuteVerify(new int[] { 2 }, null,
              "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get_3");

          sqlExecuteVerify(new int[] { 2 }, null,
              "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
                  + "/lib/checkEventCallback.xml", "loader_get");
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
  
  public void testJDBCLoaderWhenPkIsNotPartition_multipleConcGets()
      throws Exception {
    // Start one client and one server
    startVMs(2, 2);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "insert into emp.testtable_one values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable_one values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable_one values(3,4,5)");
    
    // Check for PARTITION BY RANGE
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))" + " PARTITION By Column(SECONDID)");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdJDBCRowLoader", "|url=jdbc|max-connections=10|query-string=SELECT * FROM EMP.TESTTABLE_ONE WHERE ID=?");
    
    doJDBCRowLoaderConcurrently();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    assertEquals("Number of entries expected to be 3", 3, regtwo.size());
  }
  
  public void testJDBCRowLoaderWhenPkIsPartition() throws Exception {
    // Start one client and one server
    startVMs(1, 3);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Get the cache;
    Cache cache = Misc.getGemFireCache();

    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "insert into emp.testtable_one values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable_one values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable_one values(3,4,5)");
    
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdJDBCRowLoader",
        "|url=jdbc|query-string=SELECT * FROM EMP.TESTTABLE_ONE WHERE ID=?");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 1", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_1");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 2", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_2");
    sqlExecuteVerify(null, new int[] { 1, 2, 3 },
        "select THIRDID from EMP.PARTITIONTESTTABLE where ID = 3", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get_3");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from emp.partitiontesttable", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "loader_get");

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

    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "insert into emp.testtable_one values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable_one values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable_one values(3,4,5)");
    
    // Check for PARTITION BY RANGE
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))"
        + " PARTITION By Column(ID)");
    GfxdCallbacksTest.addLoader("EMP", "PARTITIONTESTTABLE",
        "com.pivotal.gemfirexd.ddl.GfxdJDBCRowLoader",
        "|url=jdbc|max-connections=10|query-string=SELECT * FROM EMP.TESTTABLE_ONE WHERE ID=?");
    
    doJDBCRowLoaderConcurrently();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    CacheLoader ldr = rattr.getCacheLoader();
    GfxdCacheLoader gfxdldr = (GfxdCacheLoader)ldr;
    assertNull(gfxdldr);
    assertEquals("Number of entries expected to be 3", 3, regtwo.size());
  }
  
}
