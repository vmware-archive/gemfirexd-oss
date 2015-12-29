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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionCriteria;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.lru.HeapLRUCapacityController;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.VM;

@SuppressWarnings({ "rawtypes", "unchecked", "unused", "serial", "deprecation" })
public class PartitionedRegionCustomEvictionDUnitTest extends CacheTestCase {
  public PartitionedRegionCustomEvictionDUnitTest(final String name) {
    super(name);
  }

  public void testCustomEvictionOnPR() {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final String uniqName = getTestName();
    final int redundantCopies = 1;
    final int interval = 5000;
    final String name = uniqName + "-PR";
    final long start = System.currentTimeMillis() + 1000;
    final EvictionCriteria criteria = new MyEvictionCriteria();

    final CacheSerializableRunnable create = new CacheSerializableRunnable(
        "Partitioned Region with Custom Eviction") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setEnableOffHeapMemory(false);
        factory.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(redundantCopies).create());
        factory.setDiskSynchronous(true);
        factory.setCustomEvictionAttributes(criteria, start, interval);
        final PartitionedRegion pr = (PartitionedRegion)getCache().createRegionFactory(factory.create()).create(name); 
        assertNotNull(pr);
      }
    };
    vm2.invoke(create);
    vm3.invoke(create);
    final CacheSerializableRunnable doPuts = new CacheSerializableRunnable(
        "Partitioned Region put operation") {
      public void run2() {
        PartitionedRegion pr = (PartitionedRegion)getRootRegion(name);
        for(int i=0; i< 100; i++)
          pr.put(i, i);
        
        for(int i=100; i< 200; i++)
          pr.put(i, i);
      }
    };
    vm2.invoke(doPuts);

    final CacheSerializableRunnable verify = new CacheSerializableRunnable(
        "Partitioned Region put operation") {
      public void run2() {
        PartitionedRegion pr = (PartitionedRegion)getRootRegion(name);
        for(int i=0 ; i < 200; i++) {
          if (i % 2 == 0)
            assertNull(pr.get(i));
          else
            assertNotNull(pr.get(i));
        }
      }
    };

    pause(40000);
    vm2.invoke(verify);
  }

  class MyEvictionCriteria implements EvictionCriteria, Serializable {

    @Override
    public Iterator getKeysToBeEvicted(long currentMillis, Region region) {
      return new MyItr(region);
    }

    @Override
    public boolean doEvict(EntryEvent event) {
      System.out.println("Being called for " + event.getKey() + " returning "
          + ((Integer)event.getKey() % 2 == 0));
      return ((Integer)event.getKey() % 2 == 0);
    }

    @Override
    public boolean isEquivalent(EvictionCriteria other) {
      return true;
    }
  }

  class MyItr implements Iterator {
    final PartitionedRegion region;
    Iterator itr;
    public MyItr(Region r) {
      this.region = (PartitionedRegion) r;
      this.itr = PartitionRegionHelper.getLocalData(this.region).keySet().iterator();
    }

    @Override
    public boolean hasNext() {
      return itr.hasNext();
    }

    @Override
    public Object next() {
      int key = (Integer)itr.next();
      int routingObject = key;
      return new HashMap.SimpleEntry(key, routingObject);
    }

    @Override
    public void remove() {
    }

  }
}
