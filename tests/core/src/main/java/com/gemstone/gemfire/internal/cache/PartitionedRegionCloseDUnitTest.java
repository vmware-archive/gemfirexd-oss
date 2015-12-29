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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PartitionRegionConfig;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

import dunit.Host;
import dunit.VM;
import mapregion.*;

/**
 * @author tapshank, Created on Jan 19, 2006
 *  
 */
public class PartitionedRegionCloseDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  //////constructor //////////
  public PartitionedRegionCloseDUnitTest(String name) {
    super(name);
  }//end of constructor

  public static final String PR_PREFIX = "PR";

  final static int MAX_REGIONS = 1;

  final int totalNumBuckets = 5;

  final int numPut = 200;

  //////////test methods ////////////////
  public void testClose() throws Throwable
  {

    Host host = Host.getHost(0);

    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    VM datastore3 = host.getVM(3);

    // Create PR
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          try {
            cache.createRegion(PR_PREFIX + i,
                createRegionAttributesForPR(1, 20));
          }
          catch (RegionExistsException ree) {
            fail("Unexpected Region Exists Exception is thrown");
          }
        }
      }
    };

    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttributesForPR(1, 0));
        }
      }
    };

    // Create PR
    accessor.invoke(createAccessor);
    datastore1.invoke(createPRs);
    datastore2.invoke(createPRs);
    datastore3.invoke(createPRs);

    accessor.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Integer key;
          String value;
          for (int k = 0; k < numPut; k++) {
            key = new Integer(k);
            value = j + PR_PREFIX + k;
            pr.put(key, value);
          }
          getLogWriter().info(
              "VM0 Done put successfully for PR = " + PR_PREFIX + j);
        }
      }
    });

    // Close all the PRs for a given VM
    CacheSerializableRunnable closePRs = new CacheSerializableRunnable(
        "closePRs") {
      public void run2()
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + PR_PREFIX + i);
          MapBB.getBB().getSharedMap().put(PR_PREFIX + i, pr.getNode());
          pr.close();
        }
      }
    };
    // Close all the PRs on vm2
    datastore1.invoke(closePRs);
    datastore2.invoke(new CacheSerializableRunnable("validateCloseAPI") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Node targetNode = (Node)MapBB.getBB().getSharedMap()
              .get(pr.getName());
          
          checkBucketMetadataCleanup(pr, targetNode.getMemberId());
          Region root = PartitionedRegionHelper.getPRRoot(cache);

//          Region allPartitionedRegions = PartitionedRegionHelper
//              .getPRConfigRegion(root, cache);
          PartitionRegionConfig prConfig = (PartitionRegionConfig)root
              .get(pr.getRegionIdentifier());
          getLogWriter().info("prConfig = " + prConfig);
          if (prConfig.containsNode(targetNode)) {
            fail("Close clean up did not remove node from allPartitionedRegions");
          }
          getLogWriter().info(
              "datastore2 validated closing of PR = " + PR_PREFIX + j
                  + " on datastore 1");
        }
      }

    });

    accessor.invoke(new CacheSerializableRunnable("doGetOperations") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          String value;
          for (int k = 0; k < numPut; k++) {
            value = j + PR_PREFIX + k;
            assertEquals(value, pr.get(new Integer(k)));
          }
          getLogWriter().info(
              "VM0 Done put successfully for PR = " + PR_PREFIX + j);
        }
      }
    });
    datastore1.invoke(createPRs);
  }

  //////////test methods ////////////////
  public void testLocalDestroyRegion() throws Throwable
  {

    Host host = Host.getHost(0);

    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    VM datastore3 = host.getVM(3);

    // Create PR
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttributesForPR(1, 20));
        }
      }
    };

    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttributesForPR(1, 0));
        }
      }
    };

    // Create PR
    accessor.invoke(createAccessor);
    datastore1.invoke(createPRs);
    datastore2.invoke(createPRs);
    datastore3.invoke(createPRs);

    accessor.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Integer key;
          String value;
          for (int k = 0; k < numPut; k++) {
            key = new Integer(k);
            value = j + PR_PREFIX + k;
            pr.put(key, value);
          }
          getLogWriter().info(
              "VM0 Done put successfully for PR = " + PR_PREFIX + j);
        }
      }
    });

    // Here we would close all the PRs of the data store.
    CacheSerializableRunnable locallyDestroyPRs = new CacheSerializableRunnable(
        "localDestroyRegion") {
      public void run2()
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + PR_PREFIX + i);
          MapBB.getBB().getSharedMap().put(PR_PREFIX + i, pr.getNode());
          pr.localDestroyRegion();
        }
      }
    };
    // Close all the PRs on vm2
    datastore1.invoke(locallyDestroyPRs);
    datastore2.invoke(new CacheSerializableRunnable("validateCloseAPI") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          PartitionedRegion pr = (PartitionedRegion)cache
              .getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Node targetNode = (Node)MapBB.getBB().getSharedMap()
              .get(pr.getName());

          checkBucketMetadataCleanup(pr, targetNode.getMemberId());

          Region root = PartitionedRegionHelper.getPRRoot(cache);

//          Region allPartitionedRegions = PartitionedRegionHelper
//              .getPRConfigRegion(root, cache);
          PartitionRegionConfig prConfig = (PartitionRegionConfig)root
              .get(pr.getRegionIdentifier());
          getLogWriter().info("prConfig = " + prConfig);
          if (prConfig.containsNode(targetNode)) {
            fail("Close clean up did not remove node from allPartitionedRegions");
          }
          getLogWriter().info(
              "datastore2 validated closing of PR = " + PR_PREFIX + j
                  + " on datastore 1");
        }
      }
    });

    accessor.invoke(new CacheSerializableRunnable("doGetOperations") {
      public void run2()
      {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Integer key;
          String value;
          Object getVal = null;
          for (int k = 0; k < numPut; k++) {
            key = new Integer(k);
            value = j + PR_PREFIX + k;
            getVal = pr.get(key, value);
            assertEquals(getVal, value);
          }
          getLogWriter().info(
              "VM0 Done put successfully for PR = " + PR_PREFIX + j);
        }
      }
    });
    datastore1.invoke(createPRs);
  }  
  
  /**
   * This private methods sets the passed attributes and returns RegionAttribute
   * object, which is used in create region
   * @param redundancy
   * @param localMaxMem
   * 
   * @return
   */
  protected RegionAttributes createRegionAttributesForPR(int redundancy,
      int localMaxMem) {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(totalNumBuckets)
        .create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }
  /**
   * This test case checks that a closed PR (accessor/datastore) is recreatble.
   * @throws Throwable
   */
    public void testCloseRecreate() throws Throwable
    {

    Host host = Host.getHost(0);

    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    VM datastore3 = host.getVM(3);

    // Create PR
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          try {
            cache.createRegion(PR_PREFIX + i,
                createRegionAttributesForPR(1, 20));
          }
          catch (RegionExistsException ree) {
            fail("Unexpected Region Exists Exception is thrown");
          }
          catch (PartitionedRegionException pre) {
            fail("Unexpected Region Exists Exception is thrown");
          }
        }
      }
    };

    CacheSerializableRunnable createAccessor = new CacheSerializableRunnable(
        "createAccessor") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttributesForPR(1, 0));
        }
      }
    };

    // Create PR
    accessor.invoke(createAccessor);
    datastore1.invoke(createPRs);
    datastore2.invoke(createPRs);
    datastore3.invoke(createPRs);

    accessor.invoke(new CacheSerializableRunnable("doPutOperations") {
      public void run2()
      {
        Cache cache = getCache();

        for (int j = 0; j < MAX_REGIONS; j++) {
          PartitionedRegion pr = (PartitionedRegion) cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
          assertNotNull(pr);
          Integer key;
          String value;
          for (int k = 0; k < numPut; k++) {
            key = new Integer(k);
            value = j + PR_PREFIX + k;
            pr.put(key, value);
            final int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
            assertEquals(2, pr.getRegionAdvisor().getBucketOwners(bucketId).size());
          }
          getLogWriter().info(
              "VM0 Done put successfully for PR = " + PR_PREFIX + j);
        }
      }
    });

    // Here we would close all the PRs of the data store.
    CacheSerializableRunnable closePRs = new CacheSerializableRunnable(
        "closePRs") {
      public void run2()
      {
        Cache cache = getCache();
        ExpectedException ee = null;
        try {
          ee = addExpectedException(RegionDestroyedException.class.getName());
          for (int i = 0; i < MAX_REGIONS; i++) {
            PartitionedRegion pr = (PartitionedRegion)cache
                .getRegion(Region.SEPARATOR + PR_PREFIX + i);
            MapBB.getBB().getSharedMap().put(PR_PREFIX + i, pr.getNode());
            pr.close();
          }
        }
        finally {
          if (ee!=null) ee.remove();
        }

      }
    };
    // Close all the PRs on all VMs
    ExpectedException ee = null;
    try {
      ee = addExpectedException(RegionDestroyedException.class.getName());
      datastore1.invoke(closePRs);
      datastore2.invoke(closePRs);
      datastore3.invoke(closePRs);
      accessor.invoke(closePRs);
    }
    finally {
      if (ee!=null) ee.remove();
    }

    // Create PRs on all the nodes
    accessor.invoke(createAccessor);
    datastore1.invoke(createPRs);
    datastore2.invoke(createPRs);
    datastore3.invoke(createPRs);
  }
   
    static protected void checkBucketMetadataCleanup(final PartitionedRegion pr,
      final InternalDistributedMember memberId)
    {
      Iterator itr = pr.getRegionAdvisor().getBucketSet().iterator();
      try {
        while (itr.hasNext()) {
          final Integer bucketId = (Integer)itr.next();
          assertTrue("CLeanup did not remove member from bucket meta-data, member="
              + memberId + " from bucket="
              + pr.bucketStringForLogs(bucketId.intValue()), !pr.getRegionAdvisor()
              .getBucketOwners(bucketId.intValue()).contains(memberId));
        }
      } catch (NoSuchElementException done) {
      }
    }
}
