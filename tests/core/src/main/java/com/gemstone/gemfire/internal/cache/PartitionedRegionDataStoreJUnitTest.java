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

import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

/**
 * This test checks functionality of the PartitionedRegionDatastore on a sinle
 * node.
 * 
 * Created on Dec 23, 2005
 * 
 * @author rreja, modified by Girish
 *  
 */
public class PartitionedRegionDataStoreJUnitTest extends TestCase
{
  static DistributedSystem sys;

  static Cache cache;

  byte obj[] = new byte[10240];

  String regionName = "DataStoreRegion";

  public PartitionedRegionDataStoreJUnitTest(String str) throws CacheException {
    super(str);
    if (cache == null) {
      Properties dsProps = new Properties();
      dsProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");

      //  Connect to a DS and create a Cache.
      sys = DistributedSystem.connect(dsProps);
      cache = CacheFactory.create(sys);
    }
  }

  public void testRemoveBrokenNode() throws Exception
  {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    PartitionAttributes pa = paf.setRedundantCopies(0)
      .setLocalMaxMemory(0).create();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    RegionAttributes ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion)cache.createRegion("PR2", ra);
    paf.setLocalProperties(null)
        .create();
    /* PartitionedRegionDataStore prDS = */ new PartitionedRegionDataStore(pr);
   /* PartitionedRegionHelper.removeGlobalMetadataForFailedNode(PartitionedRegion.node,
        prDS.partitionedRegion.getRegionIdentifier(), prDS.partitionedRegion.cache);*/
  }

  public void testLocalPut() throws Exception
  {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    Properties globalProps = new Properties();
    globalProps.put(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY,
        "100");

    PartitionAttributes pa = paf.setRedundantCopies(0)
      .setLocalMaxMemory(100).create();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    RegionAttributes ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion)cache.createRegion("PR3", ra);

    String key = "User";
    String value = "1";

    pr.put(key, value);
    assertEquals(pr.get(key), value);

  }

  /**
   * This method checks whether the canAccomodateMoreBytesSafely returns false
   * after reaching the localMax memory.
   *  
   */
  public void testCanAccommodateMoreBytesSafely() throws Exception
  {
    PureLogWriter logger = (PureLogWriter)cache.getLogger();
    int oldLevel = logger.getLevel();
    logger.setLevel(LogWriterImpl.INFO_LEVEL);
    try {
 
    int key = 0;
    final int numMBytes = 5;

    final PartitionedRegion regionAck = (PartitionedRegion)
      new RegionFactory()
      .setPartitionAttributes(
          new PartitionAttributesFactory()
          .setRedundantCopies(0)
          .setLocalMaxMemory(numMBytes)
          .create())
      .create(this.regionName);

    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));

    int numk = numMBytes * 1024;
    int num = numk * 1024;
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num+1));
    final int OVERHEAD = CachedDeserializableFactory.getByteSize(new byte[0]);
    for (key = 0; key < numk; key++) {
      regionAck.put(new Integer(key), new byte[1024-OVERHEAD]);
    }
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.invalidate(new Integer(--key));
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(new Integer(key), new byte[1024-OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.destroy(new Integer(key));
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(new Integer(key), new byte[1023-OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(2));

    for (key = 0; key < numk; key++) {
      regionAck.destroy(new Integer(key));
    }
    assertEquals(0, regionAck.size());
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num+1));

    for (key = 0; key < numk; key++) {
      regionAck.put(new Integer(key), "foo");
    }

    }
    finally {
      logger.setLevel(oldLevel);
    }
  }
}
