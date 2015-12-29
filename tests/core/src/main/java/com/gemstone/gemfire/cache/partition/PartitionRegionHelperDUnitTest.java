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
package com.gemstone.gemfire.cache.partition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.FixedPartitioningTestBase;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import com.gemstone.gemfire.internal.cache.partitioned.fixed.FixedPartitioningTestBase.Months_Accessor;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

public class PartitionRegionHelperDUnitTest extends CacheTestCase {

  public PartitionRegionHelperDUnitTest(String name) {
    super(name);
  }

  public void testAssignBucketsToPartitions() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    vm2.invoke(createPrRegion);

    SerializableRunnable assignBuckets = new SerializableRunnable("assign partitions") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    };

    AsyncInvocation future1 = vm0.invokeAsync(assignBuckets);
    AsyncInvocation future2 = vm1.invokeAsync(assignBuckets);
    AsyncInvocation future3 = vm2.invokeAsync(assignBuckets);
    future1.join(60*1000);
    future2.join(60*1000);
    future3.join(60*1000);
    if(future1.exceptionOccurred()) throw future1.getException();
    if(future2.exceptionOccurred()) throw future2.getException();
    if(future3.exceptionOccurred()) throw future3.getException();

    SerializableRunnable checkAssignment= new SerializableRunnable("check assignment") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, info.getCreatedBucketCount());
        assertEquals(0, info.getLowRedundancyBucketCount());
        for(PartitionMemberInfo member: info.getPartitionMemberInfo()) {
          assertEquals(8, member.getBucketCount());
          //TODO unfortunately, with redundancy, we can end up
          // not balancing primaries in favor of balancing buckets. The problem
          //is that We could create too many redundant copies on a node, which means
          //when we get to the last bucket, which should be primary on that node, we
          //don't even put a copy of the bucket on that node
          // See bug #40470
//          assertEquals(4, member.getPrimaryCount());
        }
      }
    };

    vm0.invoke(checkAssignment);
  }

  
  public void testAssignBucketsToPartitions_FPR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    SerializableRunnable createPrRegion1 = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition("Q1", true, 3);
        FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition("Q2", false, 3);
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm0.invoke(createPrRegion1);
    SerializableRunnable createPrRegion2 = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition("Q2", true, 3);
        FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition("Q3", false, 3);
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm1.invoke(createPrRegion2);
    SerializableRunnable createPrRegion3 = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        FixedPartitionAttributes fpa1 = FixedPartitionAttributes
        .createFixedPartition("Q3", true, 3);
        FixedPartitionAttributes fpa2 = FixedPartitionAttributes
        .createFixedPartition("Q1", false, 3);
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm2.invoke(createPrRegion3);

    SerializableRunnable assignBuckets = new SerializableRunnable("assign partitions") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    };

    AsyncInvocation future1 = vm0.invokeAsync(assignBuckets);
    AsyncInvocation future2 = vm1.invokeAsync(assignBuckets);
    AsyncInvocation future3 = vm2.invokeAsync(assignBuckets);
    future1.join();
    future2.join();
    future3.join();
    if(future1.exceptionOccurred()) throw future1.getException();
    if(future2.exceptionOccurred()) throw future2.getException();
    if(future3.exceptionOccurred()) throw future3.getException();

    SerializableRunnable checkAssignment= new SerializableRunnable("check assignment") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(9, info.getCreatedBucketCount());
        assertEquals(0, info.getLowRedundancyBucketCount());
        for(PartitionMemberInfo member: info.getPartitionMemberInfo()) {
          assertEquals(6, member.getBucketCount());
        }
      }
    };
    vm0.invoke(checkAssignment);
    
    
    SerializableRunnable createPrRegion4 = new SerializableRunnable(
        "createRegion") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setLocalMaxMemory(0);
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        Region region = cache.createRegion("region1", attr.create());
        for (Months_Accessor month : Months_Accessor.values()) {
          String dateString = 10 + "-" + month + "-" + "2009";
          String DATE_FORMAT = "dd-MMM-yyyy";
          SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US);
          Date date = null;
          try {
            date = sdf.parse(dateString);
          }
          catch (ParseException e) {
            FixedPartitioningTestBase.fail("Exception Occured while parseing date", e);
          }
          String value = month.toString() + 10;
          region.put(date, value);
        }
      }
    };
    vm3.invoke(createPrRegion4);
    
    SerializableRunnable checkMembers = new SerializableRunnable(
        "createRegion") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for (Months_Accessor month : Months_Accessor.values()) {
          String dateString = 10 + "-" + month + "-" + "2009";
          String DATE_FORMAT = "dd-MMM-yyyy";
          SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US);
          Date date = null;
          try {
            date = sdf.parse(dateString);
          }
          catch (ParseException e) {
            FixedPartitioningTestBase.fail("Exception Occured while parseing date", e);
          }
          DistributedMember key1Pri = PartitionRegionHelper.getPrimaryMemberForKey(region, date);
          assertNotNull(key1Pri);
          Set<DistributedMember> buk0AllMems = PartitionRegionHelper.getAllMembersForKey(region, date);
          assertEquals(2, buk0AllMems.size());
          Set<DistributedMember> buk0RedundantMems = PartitionRegionHelper.getRedundantMembersForKey(region, date);
          assertEquals(1, buk0RedundantMems.size());
        }
      }
    };
    vm3.invoke(checkMembers);
    
  }
  
  public void testMembersForKey() throws Exception {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM ds1 = host.getVM(1);
    VM ds2 = host.getVM(2);
    VM ds3 = host.getVM(3);
    final String prName = getUniqueName();
    final int tb = 11;
    final int rc = 1;

    accessor.invoke(new SerializableRunnable("createAccessor") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
        .setLocalMaxMemory(0)
        .setRedundantCopies(rc)
        .setTotalNumBuckets(tb)
        .create());
        cache.createRegion(prName, attr.create());
      }
    });

    HashMap<DistributedMember, VM> d2v  = new HashMap<DistributedMember, VM>();
    SerializableCallable createPrRegion = new SerializableCallable("createDataStore") {
      public Object call() throws Exception {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
        .setRedundantCopies(rc)
        .setTotalNumBuckets(tb)
        .create());
        cache.createRegion(prName, attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    DistributedMember dm = (DistributedMember)ds1.invoke(createPrRegion);
    d2v.put(dm, ds1);
    dm = (DistributedMember)ds2.invoke(createPrRegion);
    d2v.put(dm, ds2);
    dm = (DistributedMember)ds3.invoke(createPrRegion);
    d2v.put(dm, ds3);

    final Integer buk0Key1 = new Integer(0);
    final Integer buk0Key2 = new Integer(buk0Key1.intValue() + tb);
    final Integer buk1Key1 = new Integer(1);

    accessor.invoke(new CacheSerializableRunnable("nonPRcheck") {
      @SuppressWarnings("unchecked")
      @Override
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        {
          attr.setScope(Scope.LOCAL);
          Region lr = getCache().createRegion(prName + "lr", attr.create());
          try {
            // no-pr check
            nonPRMemberForKey(lr, buk0Key1);
          } finally {
            lr.destroyRegion();
          }
        }

        {
          attr = new AttributesFactory();
          attr.setScope(Scope.DISTRIBUTED_ACK);
          Region dr = getCache().createRegion(prName + "dr", attr.create());
          try {
            // no-pr check
            nonPRMemberForKey(dr, buk0Key1);
          } finally {
            dr.destroyRegion();
          }
        }
      }
      private void nonPRMemberForKey(Region lr, final Object key) {
        try {
          PartitionRegionHelper.getPrimaryMemberForKey(lr, key);
          fail();
        } catch (IllegalArgumentException expected) {}
        try {
          PartitionRegionHelper.getAllMembersForKey(lr, key);
          fail();
        } catch (IllegalArgumentException expected) {}
        try {
          PartitionRegionHelper.getRedundantMembersForKey(lr, key);
          fail();
        } catch (IllegalArgumentException expected) {}
      }
    });

    Object[] noKeyThenKeyStuff = (Object[]) accessor.invoke(new SerializableCallable("noKeyThenKey") {
      public Object call() throws Exception {
        Region<Integer, String> r = getCache().getRegion(prName);

        // NPE check
        try {
          PartitionRegionHelper.getPrimaryMemberForKey(r, null);
          fail();
        } catch (IllegalStateException expected) {}
        try {
          PartitionRegionHelper.getAllMembersForKey(r, null);
          fail();
        } catch (IllegalStateException expected) {}
        try {
          PartitionRegionHelper.getRedundantMembersForKey(r, null);
          fail();
        } catch (IllegalStateException expected) {}

        // buk0
        assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key1));
        assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk0Key1).isEmpty());
        assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key1).isEmpty());
        // buk1
        assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk1Key1));
        assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk1Key1).isEmpty());
        assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk1Key1).isEmpty());

        r.put(buk0Key1, "zero");
        //  buk0, key1
        DistributedMember key1Pri = PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key1);
        assertNotNull(key1Pri);
        Set<DistributedMember> buk0AllMems = PartitionRegionHelper.getAllMembersForKey(r, buk0Key1);
        assertEquals(rc+1, buk0AllMems.size());
        Set<DistributedMember> buk0RedundantMems = PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key1);
        assertEquals(rc, buk0RedundantMems.size());
        DistributedMember me = r.getCache().getDistributedSystem().getDistributedMember();
        try {
          buk0AllMems.add(me);
          fail();
        } catch (UnsupportedOperationException expected) {}
        try {
          buk0AllMems.remove(me);
          fail();
        } catch (UnsupportedOperationException expected) {}
        try {
          buk0RedundantMems.add(me);
          fail();
        } catch (UnsupportedOperationException expected) {}
        try {
          buk0RedundantMems.remove(me);
          fail();
        } catch (UnsupportedOperationException expected) {}
        assertTrue(buk0AllMems.containsAll(buk0RedundantMems));
        assertTrue(buk0AllMems.contains(key1Pri));
        assertTrue(!buk0RedundantMems.contains(key1Pri));

        // buk0, key2
        DistributedMember key2Pri = PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key2);
        assertNotNull(key2Pri);
        buk0AllMems = PartitionRegionHelper.getAllMembersForKey(r, buk0Key2);
        assertEquals(rc+1, buk0AllMems.size());
        buk0RedundantMems = PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key2);
        assertEquals(rc, buk0RedundantMems.size());
        assertTrue(buk0AllMems.containsAll(buk0RedundantMems));
        assertTrue(buk0AllMems.contains(key2Pri));
        assertTrue(!buk0RedundantMems.contains(key2Pri));

        // buk1
        assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk1Key1));
        assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk1Key1).isEmpty());
        assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk1Key1).isEmpty());
        return new Object[] {key1Pri, buk0AllMems, buk0RedundantMems};
      }
    });
    final DistributedMember buk0Key1Pri = (DistributedMember) noKeyThenKeyStuff[0];
    final Set<DistributedMember> buk0AllMems = (Set<DistributedMember>)noKeyThenKeyStuff[1];
    final Set<DistributedMember> buk0Redundants = (Set<DistributedMember>)noKeyThenKeyStuff[2];

    VM buk0Key1PriVM = d2v.get(buk0Key1Pri);
    buk0Key1PriVM.invoke(new CacheSerializableRunnable("assertPrimaryness") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(prName);
        Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null));
        try {
          BucketRegion buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
          assertNotNull(buk0);
          assertTrue(buk0.getBucketAdvisor().isPrimary());
        }
        catch (ForceReattemptException e) {
          getLogWriter().severe(e);
          fail();
        }
      }
    });
    CacheSerializableRunnable assertHasBucket = new CacheSerializableRunnable("assertHasBucketAndKey") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(prName);
        Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null));
        try {
          BucketRegion buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
          assertNotNull(buk0);
          Entry k1e = buk0.getEntry(buk0Key1);
          assertNotNull(k1e);
        }
        catch (ForceReattemptException e) {
          getLogWriter().severe(e);
          fail();
        }
      }
    };
    for (DistributedMember bom: buk0AllMems) {
      VM v = d2v.get(bom);
      getLogWriter().info("Visiting bucket owner member " + bom + " for key " + buk0Key1);
      v.invoke(assertHasBucket);
    }

    CacheSerializableRunnable assertRed = new CacheSerializableRunnable("assertRedundant") {
      @Override
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(prName);
        Integer bucketId = new Integer(PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null));
        try {
          BucketRegion buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
          assertNotNull(buk0);
          assertFalse(buk0.getBucketAdvisor().isPrimary());
        }
        catch (ForceReattemptException e) {
          getLogWriter().severe(e);
          fail();
        }
      }
    };
    for (DistributedMember redm: buk0Redundants) {
      VM v = d2v.get(redm);
      getLogWriter().info("Visiting redundant member " + redm + " for key " + buk0Key1);
      v.invoke(assertRed);
    }
  }
  
  
}
