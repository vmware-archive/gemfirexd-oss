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
package com.gemstone.gemfire.internal.offheap;

import java.util.Arrays;
import java.util.Properties;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;
import junit.framework.TestCase;

/**
 * Basic test of regions that use off heap storage.
 * Subclasses exist for the different types of offheap store.
 * 
 * @author darrel
 *
 */
public abstract class OffHeapRegionBase extends TestCase {
  private RegionMapClearDetector rmcd = null;
  public abstract void configureOffHeapStorage();
  public abstract void unconfigureOffHeapStorage();
  public abstract int perObjectOverhead();

  private GemFireCacheImpl createCache() {
    configureOffHeapStorage();
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    props.setProperty("off-heap-memory-size", getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    unconfigureOffHeapStorage();
    return result;
  }
  private void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
    // TODO cleanup default disk store files
    
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    rmcd = new RegionMapClearDetector();
    CacheObserverHolder.setInstance(rmcd);
  }

  @Override
  public void tearDown() throws Exception {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(null);
    super.tearDown();
  }
  
  protected abstract String getOffHeapMemorySize();
  
  public void testSizeAllocation() {
    // prevent cache from closing in reaction to ooom
    System.setProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY, "true");
    GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      MemoryChunk mc1 = ma.allocate(64, null);
      assertEquals(64+perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize-(64+perObjectOverhead()), ma.getFreeMemory());
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // do an allocation larger than the slab size
      try {
        ma.allocate(1024*1024*10, null);
        fail("Expected an out of heap exception");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      assertEquals(0, ma.getUsedMemory());
      assertFalse(gfc.isClosed());
    } finally {
      System.clearProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY);
      closeCache(gfc);
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  public void keep_testOutOfOffHeapMemoryErrorClosesCache() {
    // this test is redundant but may be useful
    final GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      MemoryChunk mc1 = ma.allocate(64, null);
      assertEquals(64+perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize-(64+perObjectOverhead()), ma.getFreeMemory());
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // do an allocation larger than the slab size
      try {
        ma.allocate(1024*1024*10, null);
        fail("Expected an out of heap exception");
      } catch (OutOfOffHeapMemoryException expected) {
        // passed
      }
      assertEquals(0, ma.getUsedMemory());
      
      final WaitCriterion waitForDisconnect = new WaitCriterion() {
        public boolean done() {
          return gfc.isClosed();
        }
        public String description() {
          return "Waiting for disconnect to complete";
        }
      };
      DistributedTestBase.waitForCriterion(waitForDisconnect, 10 * 1000, 100, true);

      assertTrue(gfc.isClosed());
    } finally {
      closeCache(gfc);
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  public void testByteArrayAllocation() {
    GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      byte[] data = new byte[] {1,2,3,4,5,6,7,8};
      MemoryChunk mc1 = (MemoryChunk)ma.allocateAndInitialize(data, false, false, null);
      assertEquals(data.length+perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize-(data.length+perObjectOverhead()), ma.getFreeMemory());
      byte[] data2 = new byte[data.length];
      mc1.readBytes(0, data2);
      assertTrue(Arrays.equals(data, data2));
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // try some small byte[] that don't need to be stored off heap.
      data = new byte[] {1,2,3,4,5,6,7};
      StoredObject so1 = ma.allocateAndInitialize(data, false, false, null);
      assertEquals(0, ma.getUsedMemory());
      assertEquals(offHeapSize, ma.getFreeMemory());
      data2 = new byte[data.length];
      data2 = (byte[])so1.getDeserializedForReading();
      assertTrue(Arrays.equals(data, data2));
    } finally {
      closeCache(gfc);
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void doRegionTest(final RegionShortcut rs, final String rName) {
    GemFireCacheImpl gfc = createCache();
    boolean isPersistent = false;
    Region r = null;
    try {
      gfc.setCopyOnRead(true);
      final MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      assertEquals(0, ma.getUsedMemory());
      r = gfc.createRegionFactory(rs).setEnableOffHeapMemory(true).create(rName);
      isPersistent = r.getAttributes().getDataPolicy().withPersistence();
      assertEquals(true, r.isEmpty());
      assertEquals(0, ma.getUsedMemory());
      Object data = new Integer(123456789);
      r.put("key1", data);
      //System.out.println("After put of Integer value off heap used memory=" + ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() == 0);
      assertEquals(data, r.get("key1"));
      r.invalidate("key1");
      assertEquals(0, ma.getUsedMemory());
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() == 0);
      long usedBeforeUpdate = ma.getUsedMemory();
      r.put("key1", data);
      assertEquals(usedBeforeUpdate, ma.getUsedMemory());
      assertEquals(data, r.get("key1"));
      r.destroy("key1");
      assertEquals(0, ma.getUsedMemory());
      
      data = new Long(0x007FFFFFL);
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() == 0);
      assertEquals(data, r.get("key1"));
      data = new Long(0xFF8000000L);
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() == 0);
      assertEquals(data, r.get("key1"));
      
      
      // now lets set data to something that will be stored offheap
      data = new Long(Long.MAX_VALUE);
      r.put("key1", data);
      assertEquals(data, r.get("key1"));
      //System.out.println("After put of Integer value off heap used memory=" + ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() > 0);
      data = new Long(Long.MIN_VALUE);
      r.put("key1", data);
      assertEquals(data, r.get("key1"));
      //System.out.println("After put of Integer value off heap used memory=" + ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() > 0);
      r.invalidate("key1");
      assertEquals(0, ma.getUsedMemory());
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      usedBeforeUpdate = ma.getUsedMemory();
      r.put("key1", data);
      assertEquals(usedBeforeUpdate, ma.getUsedMemory());
      assertEquals(data, r.get("key1"));
      r.destroy("key1");
      assertEquals(0, ma.getUsedMemory());

      // confirm that byte[] do use off heap
      {
        byte[] originalBytes = new byte[1024];
        Object oldV = r.put("byteArray", originalBytes);
        long startUsedMemory = ma.getUsedMemory();
        assertEquals(null, oldV);
        byte[] readBytes = (byte[]) r.get("byteArray");
        if (originalBytes == readBytes) {
          fail("Expected different byte[] identity");
        }
        if (!Arrays.equals(readBytes, originalBytes)) {
          fail("Expected byte array contents to be equal");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        oldV = r.put("byteArray", originalBytes);
        assertEquals(null, oldV); // we default to old value being null for offheap
        assertEquals(startUsedMemory, ma.getUsedMemory());
        
        readBytes = (byte[])r.putIfAbsent("byteArray", originalBytes);
        if (originalBytes == readBytes) {
          fail("Expected different byte[] identity");
        }
        if (!Arrays.equals(readBytes, originalBytes)) {
          fail("Expected byte array contents to be equal");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        if (!r.replace("byteArray", readBytes, originalBytes)) {
          fail("Expected replace to happen");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        byte[] otherBytes = new byte[1024];
        otherBytes[1023] = 1;
        if (r.replace("byteArray", otherBytes, originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (r.replace("byteArray", "bogus string", originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (r.remove("byteArray", "bogus string")) {
          fail("Expected remove to not happen");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        
        if (!r.remove("byteArray", originalBytes)) {
          fail("Expected remove to happen");
        }
        assertEquals(0, ma.getUsedMemory());
        oldV = r.putIfAbsent("byteArray", "string value");
        assertEquals(null, oldV);
        assertEquals("string value", r.get("byteArray"));
        if (r.replace("byteArray", "string valuE", originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (!r.replace("byteArray", "string value", originalBytes)) {
          fail("Expected replace to happen");
        }
        oldV = r.destroy("byteArray"); // we default to old value being null for offheap
        assertEquals(null, oldV);
        MyCacheListener listener = new MyCacheListener();
        r.getAttributesMutator().addCacheListener(listener);
        try {
          r.put("byteArray", "string value1");
          assertEquals(null, listener.ohOldValue);
          assertEquals("string value1", listener.ohNewValue.getDeserializedForReading());
          if (!r.replace("byteArray", listener.ohNewValue, "string value2")) {
            fail("expected replace to happen");
          }
          assertEquals("string value2", listener.ohNewValue.getDeserializedForReading());
          assertEquals("string value1", listener.ohOldValue.getDeserializedForReading());
        } finally {
          r.getAttributesMutator().removeCacheListener(listener);
        }
      }
      assertTrue(ma.getUsedMemory() > 0);
      byte[] value = new byte[1024];
      /*while (value != null) */ {
        r.put("byteArray", value);
      }
      r.remove("byteArray");
      assertEquals(0, ma.getUsedMemory());

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      r.invalidateRegion();
      assertEquals(0, ma.getUsedMemory());

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      if(!isPersistent) {
        rmcd.beforeRegionClear(r)  ;
      }
      try {    	
        r.clear();
    	rmcd.waitTillAllClear();
        assertEquals(0, ma.getUsedMemory());
      } catch (UnsupportedOperationException ok) {
    	 rmcd.setNumExpectedRegionClearCalls(0); 
      }

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      if (r.getAttributes().getDataPolicy().withPersistence()) {
        r.put("key2", Integer.valueOf(1234567890));
        r.put("key3", new Long(0x007FFFFFL));
        r.put("key4", new Long(0xFF8000000L));
        assertEquals(4, r.size());
        rmcd.beforeRegionClear(r);  
        r.close();
        rmcd.waitTillAllClear();
        assertEquals(0, ma.getUsedMemory());
        // simple test of recovery
        r = gfc.createRegionFactory(rs).setEnableOffHeapMemory(true).create(rName);
        assertEquals(4, r.size());
        assertEquals(data, r.get("key1"));
        assertEquals(Integer.valueOf(1234567890), r.get("key2"));
        assertEquals(new Long(0x007FFFFFL), r.get("key3"));
        assertEquals(new Long(0xFF8000000L), r.get("key4"));
        rmcd.beforeRegionClear(r);  
        closeCache(gfc);
        rmcd.waitTillAllClear();
        assertEquals(0, ma.getUsedMemory());
        gfc = createCache();
        if (ma != gfc.getOffHeapStore()) {
          fail("identity of offHeapStore changed when cache was recreated");
        }
        r = gfc.createRegionFactory(rs).setEnableOffHeapMemory(true).create(rName);
        assertTrue(ma.getUsedMemory() > 0);
        assertEquals(4, r.size());
        assertEquals(data, r.get("key1"));
        assertEquals(Integer.valueOf(1234567890), r.get("key2"));
        assertEquals(new Long(0x007FFFFFL), r.get("key3"));
        assertEquals(new Long(0xFF8000000L), r.get("key4"));
      }
      rmcd.beforeRegionClear(r);  
      r.destroyRegion();
      rmcd.waitTillAllClear();
      assertEquals(0, ma.getUsedMemory());

    } finally {
      if (r != null && !r.isDestroyed()) {
    	rmcd.beforeRegionClear(r);  
        r.destroyRegion();
        rmcd.waitTillAllClear();
      }
      closeCache(gfc);
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
    
  }
  
  @SuppressWarnings("rawtypes")
  private static class MyCacheListener extends CacheListenerAdapter {
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    public StoredObject ohOldValue;
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    public StoredObject ohNewValue;
    
    /**
     * This method retains both ohOldValue and ohNewValue
     */
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    private void setEventData(EntryEvent e) {
      close();
      EntryEventImpl event = (EntryEventImpl) e;
      this.ohOldValue = event.getOffHeapOldValue();
      this.ohNewValue = event.getOffHeapNewValue();
    }
    
    @Override
    public void afterCreate(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterDestroy(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterInvalidate(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterUpdate(EntryEvent e) {
      setEventData(e);
    }
    
    @Released(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    @Override
    public void close() {
      if (this.ohOldValue instanceof Chunk) {
        ((Chunk)this.ohOldValue).release();
      }
      if (this.ohNewValue instanceof Chunk) {
        ((Chunk)this.ohNewValue).release();
      }
    }
  }
  
  public void testPR() {
    doRegionTest(RegionShortcut.PARTITION, "pr1");
  }
  public void testReplicate() {
    doRegionTest(RegionShortcut.REPLICATE, "rep1");
  }
  public void testLocal() {
    doRegionTest(RegionShortcut.LOCAL, "local1");
  }
  public void DISABLED_BUG_51918_testLocalPersistent() {
    doRegionTest(RegionShortcut.LOCAL_PERSISTENT, "localPersist1");
  }
  public void testPRPersistent() {
    doRegionTest(RegionShortcut.PARTITION_PERSISTENT, "prPersist1");
  }
  
  public static class RegionMapClearDetector extends CacheObserverAdapter {
    private int numExpectedRegionClearCalls = 0;
    private int currentCallCount = 0;

    private void setNumExpectedRegionClearCalls(int numExpected) {
      this.numExpectedRegionClearCalls = numExpected;
      this.currentCallCount = 0;
    }

    public void waitTillAllClear() {
      try {
        synchronized (this) {
          if (this.currentCallCount < numExpectedRegionClearCalls) {
            this.wait();
          }
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new GemFireException(ie) {
        };
      }
    }

    @Override
    public void afterRegionCustomEntryConcurrentHashMapClear() {
      synchronized (this) {
        ++this.currentCallCount;
        if (this.currentCallCount == this.numExpectedRegionClearCalls) {
          this.notifyAll();
        }
      }
    }

    public void beforeRegionClear(Region rgn) {
      if (rgn.getAttributes().getEnableOffHeapMemory()) {
        if (rgn.getAttributes().getDataPolicy().isPartition()) {
          PartitionedRegion pr = (PartitionedRegion) rgn;
          int numBucketsHosted = pr.getRegionAdvisor().getAllBucketAdvisors()
              .size();
          this.setNumExpectedRegionClearCalls(numBucketsHosted);
        } else {
          this.setNumExpectedRegionClearCalls(1);
        }
      }
    }
  }

}
