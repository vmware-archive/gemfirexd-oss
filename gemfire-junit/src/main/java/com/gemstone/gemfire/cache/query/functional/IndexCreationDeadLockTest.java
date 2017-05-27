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

package com.gemstone.gemfire.cache.query.functional;

/**
 *
 * @author prafulla
 * @author Asif
 */
import java.io.File;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

import com.gemstone.gemfire.cache.query.data.*;
import com.gemstone.gemfire.cache.query.*;

import io.snappydata.test.dunit.DistributedTestBase;
import junit.framework.*;

public class IndexCreationDeadLockTest extends TestCase
{
  protected boolean testFailed = false;

  protected String cause = "";

  boolean exceptionInCreatingIndex = false;

  /** Creates a new instance of IndexCreationDeadLockTest */
  public IndexCreationDeadLockTest(String testName) {
    super(testName);
  }

  Region region;

  final String indexName = "queryTest";

  protected void setUp() throws java.lang.Exception
  {
    CacheUtils.startCache();
    this.testFailed = false;
    this.cause = "";
    exceptionInCreatingIndex = false;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);

    factory.setIndexMaintenanceSynchronous(true);
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);

  }

  protected void tearDown() throws java.lang.Exception
  {
	try{
		this.region.localDestroyRegion();
	}catch(RegionDestroyedException  rde) {
		//Ignore
	}

    CacheUtils.closeCache();
  }

  public static Test suite()
  {
    TestSuite suite = new TestSuite(IndexCreationDeadLockTest.class);
    return suite;
  }

  /**
   * Tests Index creation and maintenance deadlock scenario for in memory region
   * @author ashahid
   *
   */
  public void testIndexCreationDeadLock() throws Exception
  {

    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }

  /**
   * Tests  Index creation and maintenance deadlock scenario for Persistent only disk region
   * @author ashahid
   */
  public void testIndexCreationDeadLockForDiskOnlyRegion()
  {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setIndexMaintenanceSynchronous(true);
    File dir = new File("test");
    dir.mkdir();
    DiskStoreFactory dsf = region.getCache().createDiskStoreFactory();
    DiskStore ds1 = dsf.setDiskDirs(new File[] {dir}).create("ds1");
    factory.setDiskStoreName("ds1");
    dir.deleteOnExit();
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }


  /**
   * Tests  Index creation and maintenance deadlock scenario for a region with stats enabled
   *
   * @author Asif
   */
  public void testIndexCreationDeadLockForStatsEnabledRegion()
  {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setStatisticsEnabled(true);
    factory.setIndexMaintenanceSynchronous(true);
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }

  /**
   * Tests inability to create index on a region which overflows to disk   *
   * @author Asif
   */
  public void testIndexCreationDeadLockForOverflowToDiskRegion()
  {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        1, EvictionAction.OVERFLOW_TO_DISK));
    factory.setIndexMaintenanceSynchronous(true);
    File dir = new File("test");
    dir.mkdir();
    DiskStoreFactory dsf = region.getCache().createDiskStoreFactory();
    DiskStore ds1 = dsf.setDiskDirs(new File[] {dir}).create("ds1");
    factory.setDiskStoreName("ds1");
    dir.deleteOnExit();
    region = CacheUtils.createRegion("portfolios", factory.create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertTrue(
        "Index creation succeeded . For diskRegion this shoudl not have happened",
        this.exceptionInCreatingIndex);
  }

  private void simulateDeadlockScenario() {

    Thread th = new IndexCreationDeadLockTest.PutThread("put thread");
    th.start();
    DistributedTestBase.join(th, 60 * 1000, null);
  }


  public static void main(java.lang.String[] args)
  {
    junit.textui.TestRunner.run(suite());
  }

  /**
   * following thread will perform the operations of data population and index creation.
   */
  public class HelperThread extends Thread
  {

    public HelperThread(String thName) {
      super(thName);

      System.out
          .println("--------------------- Thread started ------------------------- "
              + thName);
    }

    public void run()
    {
      try {

        System.out
            .println("--------------------- Creating Indices -------------------------");
        QueryService qs;
        qs = CacheUtils.getQueryService();
        qs.createIndex("status", IndexType.FUNCTIONAL, "pf.status",
            "/portfolios pf, pf.positions.values posit");

        qs.createIndex("secId", IndexType.FUNCTIONAL, "posit.secId",
            "/portfolios pf, pf.positions.values posit");

        System.out
            .println("--------------------- Index Creation Done-------------------------");
      }
      catch (Exception e) {
        exceptionInCreatingIndex = true;
      }
    }
  }

  /**
   * thread to put the entries in region
   */
  public class PutThread extends Thread
  {

    public PutThread(String thName) {
      super(thName);
      System.out
          .println("--------------------- Thread started ------------------------- "
              + thName);

    }

    public void run()
    {
      try {
        System.out
            .println("--------------------- Populating Data -------------------------");
        for (int i = 0; i < 10; i++) {
          region.put(String.valueOf(i), new Portfolio(i));
          Portfolio value = (Portfolio)region.get(String.valueOf(i));
          System.out.println("value for key " + i + " is: " + value);
          System.out.println("region.size(): - " + region.size());
        }
        System.out
            .println("--------------------- Data Populatio done -------------------------");

        System.out
            .println("---------------------Destroying & repopulating the data -------------------------");
        AttributesMutator mutator = IndexCreationDeadLockTest.this.region
            .getAttributesMutator();
        mutator.setCacheWriter(new BeforeUpdateCallBack());
        System.out.println("region.size(): - " + region.size());
        for (int i = 0; i < 10; i++) {
          region.destroy(String.valueOf(i));
          region.put(String.valueOf(i), new Portfolio(i + 20));
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        IndexCreationDeadLockTest.this.testFailed = true;
        IndexCreationDeadLockTest.this.cause = "Test failed because of exception="
            + e;
      }
    }
  }

  /**
   *  make the update to wait for a while before updatation to simulate the deadlock condiction
   */
  public class BeforeUpdateCallBack extends CacheWriterAdapter
  {
    int cnt = 0;

    public void beforeCreate(EntryEvent event) throws CacheWriterException
    {
      cnt++;
      if (cnt == 10) {
        System.out
            .println("--------------------- starting IndexCreation Thread-------------------------");
        Thread indxCreationThread = new HelperThread("index creator thread");
        indxCreationThread.start();
        try {
          DistributedTestBase.join(indxCreationThread, 30 * 1000, null);
        }
        catch (Exception e) {
          e.printStackTrace();
          IndexCreationDeadLockTest.this.testFailed = true;
          IndexCreationDeadLockTest.this.cause = "Test failed because of exception="
              + e;
          Assert.fail(e.toString());
        }
      }
    }
  }
}
