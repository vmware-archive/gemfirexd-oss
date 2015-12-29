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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

@SuppressWarnings("synthetic-access")
public class SingleWriteSingleReadRegionQueueDUnitTest extends CacheTestCase {
  
  private static SingleWriteSingleReadRegionQueue remoteQueue = null;
  
  public SingleWriteSingleReadRegionQueueDUnitTest(String name) {
    super(name);
  }
  
  private static final long serialVersionUID = 7024693896871107318L;

  /**
   * Test to see if we can put and take elements in order from
   * the queue.
   */
  public void testPutTake() {
    SingleWriteSingleReadRegionQueue queue = initQueue();
    
    assertNull(queue.take());
    queue.put("A");
    queue.put("B");
    queue.put("C");
    assertEquals(3, queue.size());
    assertEquals("A", queue.take());
    assertEquals("B", queue.take());
    assertEquals("C", queue.take());
    assertEquals(null, queue.take());
    queue.put("D");
    assertEquals("D", queue.take());
    assertEquals(null, queue.take());
  }

  /**
   * Test to see if we can put and peek, and remove elements in order
   * from the queue.
   */
  public void testPutPeekRemove() {
    SingleWriteSingleReadRegionQueue queue = initQueue();
    
    assertNull(queue.peek());
    queue.put("A");
    queue.put("B");
    queue.put("C");
    assertEquals("A", queue.peek());
    assertEquals("A", queue.peek());
    queue.remove();
    
    //should be a noop
    queue.remove();
    
    assertEquals("B", queue.peek());
    queue.remove();
    assertEquals("C", queue.peek());
    queue.remove();
    assertEquals(null, queue.peek());
    queue.remove();
    assertEquals(null, queue.peek());
    queue.put("D");
    assertEquals("D", queue.peek());
    queue.remove();
    assertEquals(null, queue.peek());
  }
  
  /**
   * Test to see if we can peek and remove
   * batches at a time
   */
  public void testPeekBatch() {
    SingleWriteSingleReadRegionQueue queue = initQueue();
    
    queue.put("A");
    queue.put("B");
    queue.put("C");
    assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), queue.peek(20));
    assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), queue.peek(20));
    queue.remove();
    queue.remove();
    queue.remove();
    
//  should be a noop
    queue.remove();
    
    assertEquals(Collections.EMPTY_LIST, queue.peek(20));

    queue.put("D");
    
    //should be a noop
    queue.remove();
    
    queue.put("E");
    queue.put("F");
    assertEquals(Arrays.asList(new String[] {"D", "E", "F"}), queue.peek(20));
    queue.remove(20);
    assertNull(queue.peek());
  }
  
  /**
   * Test to of the blocking beviour of the batch peek.
   */
  public void testPeekBatchBlocking() {
    final SingleWriteSingleReadRegionQueue queue = initQueue();
    
    queue.put("A");
    queue.put("B");
    queue.put("C");
    long start = System.currentTimeMillis();
    assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), queue.peek(20, 1000));
    assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), queue.peek(20, 1000));
    assertTrue("Should have blocked for 2 seconds waiting for new data", System.currentTimeMillis() - start > 1500);
    queue.remove();
    queue.remove();
    queue.remove();

    start = System.currentTimeMillis();
    assertEquals(Collections.EMPTY_LIST, queue.peek(20, 1000));
    assertTrue("Should have blocked for 1 seconds waiting for new data", System.currentTimeMillis() - start > 500);
    
    Thread putter = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(5000);
          queue.put("D");
          Thread.sleep(100);
          queue.put("E");
          Thread.sleep(100);
          queue.put("F");
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    putter.start();
    
    start = System.currentTimeMillis();
    assertEquals(Arrays.asList(new String[] {"D", "E", "F"}), queue.peek(3, 30 * 1000));
    assertTrue("Should have blocked for less than 15 seconds waiting for new data", System.currentTimeMillis() - start < 15000);
  }
  
  /**
   * Tests that the queue conflates entries properly
   */
  public void testConflation() {
    final SingleWriteSingleReadRegionQueue queue = initQueue(true);
    queue.put(new MyConflatable("A", "A"));
    queue.put(new MyConflatable("B", "B"));
    queue.put(new MyConflatable("C", "C"));
    queue.put(new MyConflatable("A", "D"));
    queue.put(new MyConflatable("B", "E"));
    queue.put(new MyConflatable("A", "F"));
    
    assertEquals(3, queue.size());
    assertEquals(new MyConflatable("C", "C"), queue.take());
    assertEquals(new MyConflatable("B", "E"), queue.take());
    assertEquals(new MyConflatable("A", "F"), queue.take());
    assertEquals(null, queue.take());
    queue.put(new MyConflatable("A", "G"));
    queue.put(new MyConflatable("A", "H"));
    assertEquals(new MyConflatable("A", "H"), queue.take());
    assertEquals(null, queue.take());
  }
  
  /**
   * Test that we can put entries in one queue,
   * failover to another VM, and get the entries
   * out correctly.
   */
  public void testFailover() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        remoteQueue = initQueue();
      }
    });
    SingleWriteSingleReadRegionQueue queue = initQueue();
    
    queue.put("A");
    queue.put("B");
    queue.put("C");
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        assertEquals(3, remoteQueue.size());
        assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), remoteQueue.peek(20));
        assertEquals(Arrays.asList(new String[] {"A", "B", "C"}), remoteQueue.peek(20));
        remoteQueue.remove();
        remoteQueue.remove();
        remoteQueue.remove();
        
//  should be a noop
        remoteQueue.remove();
        
        assertEquals(Collections.EMPTY_LIST, remoteQueue.peek(20));
        
        remoteQueue.put("D");
        
        //should be a noop
        remoteQueue.remove();
        
        remoteQueue.put("E");
        remoteQueue.put("F");
        assertEquals(Arrays.asList(new String[] {"D", "E", "F"}), remoteQueue.peek(20));
        remoteQueue.remove(20);
        assertNull(remoteQueue.peek());
      }
      
    });
  }
  
  /**
   * Test that we can put entries in one queue,
   * failover to another VM, and get the entries
   * out correctly, with conflation enabled.
   */
  public void testFailoverWithConflation() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        remoteQueue = initQueue(true);
      }
    });
    
    SingleWriteSingleReadRegionQueue queue = initQueue(true);
    queue.put(new MyConflatable("A", "A"));
    queue.put(new MyConflatable("B", "B"));
    queue.put(new MyConflatable("C", "C"));
    queue.put(new MyConflatable("A", "D"));
    queue.put(new MyConflatable("B", "E"));
    queue.put(new MyConflatable("A", "F"));
    queue.destroy();
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        assertEquals(new MyConflatable("C", "C"), remoteQueue.take());
        assertEquals(new MyConflatable("B", "E"), remoteQueue.take());
        assertEquals(new MyConflatable("A", "F"), remoteQueue.peek());
        remoteQueue.put(new MyConflatable("A", "G"));
        remoteQueue.put(new MyConflatable("A", "H"));
        
        //A,F is in the queue because we don't conflate new events with events
        //that were in the queue at the time of failover.
        //If we fix that issue, this test will fail.
        assertEquals(new MyConflatable("A", "F"), remoteQueue.take());
        
        assertEquals(new MyConflatable("A", "H"), remoteQueue.take());
        assertEquals(null, remoteQueue.take());
      }
    });
  }
  
  /**
   * Test that we can put entries in one queue,
   * failover to  a queue in another VM that was created after the
   * entries were added to the queue , and get the entries
   * out correctly, with conflation enabled.
   */
  public void testFailoverWithConflationAndGII() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    SingleWriteSingleReadRegionQueue queue = initQueue(true);
    queue.put(new MyConflatable("A", "A"));
    queue.put(new MyConflatable("B", "B"));
    queue.put(new MyConflatable("C", "C"));
    queue.put(new MyConflatable("A", "D"));
    queue.put(new MyConflatable("B", "E"));
    queue.put(new MyConflatable("A", "F"));
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        remoteQueue = initQueue(true);
      }
    });
    
    queue.destroy();
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        assertEquals(new MyConflatable("C", "C"), remoteQueue.take());
        assertEquals(new MyConflatable("B", "E"), remoteQueue.take());
        assertEquals(new MyConflatable("A", "F"), remoteQueue.peek());
        remoteQueue.put(new MyConflatable("A", "G"));
        remoteQueue.put(new MyConflatable("A", "H"));
        
        //A,F is in the queue because we don't conflate new events with events
        //that were in the queue at the time of failover.
        //If we fix that issue, this test will fail.
        assertEquals(new MyConflatable("A", "F"), remoteQueue.take());
        
        assertEquals(new MyConflatable("A", "H"), remoteQueue.take());
        assertEquals(null, remoteQueue.take());
      }
    });
  }
  
  /**
   * Test what happens when our entry
   * key wraps around Long.MAX_VALUE
   * back to 0.
   * 
   * This is more of a "white-box" test, so
   * if the internal implementation of the queue
   * changes this test may fail.
   */
  public void testBoundaryWrapping() {
    SingleWriteSingleReadRegionQueue queue = initQueue();
    //this is a hack. The head and tail keys of the queue are initialized
    //lazily. So adding an entry now means that first use of the key
    //will initialize those keys. 
    queue.getRegion().put(new Long(Long.MAX_VALUE - 2), "A");
    queue.put("B");
    queue.put("C");
    queue.put("D");
    queue.put("E");
    
    assertEquals("B", queue.getRegion().get(new Long(Long.MAX_VALUE -1)));
    assertEquals("C", queue.getRegion().get(new Long(0)));
    
    assertEquals("A", queue.take());
    assertEquals("B", queue.take());
    assertEquals("C", queue.take());
    assertEquals("D", queue.take());
    assertEquals("E", queue.take());
    assertEquals(null, queue.take());
  }
  
  public void testBoundaryFailover() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    SingleWriteSingleReadRegionQueue queue = initQueue();
    //this is a hack. The head and tail keys of the queue are initialized
    //lazily. So adding an entry now means that first use of the key
    //will initialize those keys. 
    queue.getRegion().put(new Long(Long.MAX_VALUE - 2), "A");
    queue.put("B");
    queue.put("C");
    queue.put("D");
    queue.put("E");
    
    assertEquals("B", queue.getRegion().get(new Long(Long.MAX_VALUE -1)));
    assertEquals("C", queue.getRegion().get(new Long(0)));
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        remoteQueue = initQueue(true);
      }
    });
    
    queue.destroy();
    
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        assertEquals("A", remoteQueue.take());
        assertEquals("B", remoteQueue.take());
        assertEquals("C", remoteQueue.take());
        assertEquals("D", remoteQueue.take());
        assertEquals("E", remoteQueue.take());
        assertEquals(null, remoteQueue.take());
      }
    });
  }
  
  private SingleWriteSingleReadRegionQueue initQueue() {
   return initQueue(false);
  }
  
  private SingleWriteSingleReadRegionQueue initQueue(boolean conflate) {
    Cache cache = getCache();
    GatewayQueueAttributes queueAttrs = new GatewayQueueAttributes();
    queueAttrs.setBatchConflation(true);
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File overflowDir = new File(getUniqueName() + InternalDistributedSystem.getAnyInstance().getId());
    if (!overflowDir.exists()) {
      overflowDir.mkdir();
    }
    assertTrue(overflowDir.isDirectory());
    File[] dirs1 = new File[] {overflowDir.getAbsoluteFile()};
    dsf.setDiskDirs(dirs1).create(getUniqueName());
    queueAttrs.setDiskStoreName(getUniqueName());
//    queueAttrs.setOverflowDirectory(getUniqueName() + InternalDistributedSystem.getAnyInstance().getId());
    CacheListener listener = new CacheListenerAdapter() {
      
    };
    GatewayStats stats = new GatewayStats(cache.getDistributedSystem(), "g1", "h1", null);
    SingleWriteSingleReadRegionQueue queue = new SingleWriteSingleReadRegionQueue(cache, "region", queueAttrs, listener, stats);
    return queue;
  }
  
  private static class MyConflatable implements Conflatable {
    private final Object conflationKey;
    private final Object value;
    
    public MyConflatable(Object conflationKey, Object value) {
      this.conflationKey = conflationKey;
      this.value = value;
    }

    public EventID getEventId() {
      return null;
      //throw new UnsupportedOperationException();
    }

    public Object getKeyToConflate() {
      return conflationKey;
    }

    public String getRegionToConflate() {
      return "A";
    }

    public Object getValueToConflate() {
      return value;
    }

    public void setLatestValue(Object value) {
      throw new UnsupportedOperationException();
    }

    public boolean shouldBeConflated() {
      return true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldBeMerged() {
      return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean merge(Conflatable existing) {
      throw new AssertionError("not expected to be invoked");
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((conflationKey == null) ? 0 : conflationKey.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MyConflatable other = (MyConflatable) obj;
      if (conflationKey == null) {
        if (other.conflationKey != null)
          return false;
      } else if (!conflationKey.equals(other.conflationKey))
        return false;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "MyConflatable(" + conflationKey + "," + value.toString() + ")";
    }
  }

  // now that the disk format has changed this test will no longer pass
//   public void testRead57File() throws IOException {
//     Cache cache = getCache();
//     String dirName =  getUniqueName() + InternalDistributedSystem.getAnyInstance().getId();
//     UnzipUtil.unzip(this.getClass().getResourceAsStream("57GatewayQueue.zip"), dirName);
    
//     GatewayQueueAttributes queueAttrs = new GatewayQueueAttributes();
//     queueAttrs.setBatchConflation(true);
//     queueAttrs.setEnablePersistence(true);
    
//     queueAttrs.setOverflowDirectory(dirName);
//     CacheListener listener = new CacheListenerAdapter() {
      
//     };
//     GatewayStats stats = new GatewayStats(cache.getDistributedSystem(), "g1", "h1");
//     SingleWriteSingleReadRegionQueue queue = new SingleWriteSingleReadRegionQueue(cache, "region", queueAttrs, listener, stats);
//     //The 57GatewayQueue.zip file contains three entries, B, C, D
//     assertEquals("B", queue.take());
//     assertEquals("C", queue.take());
//     assertEquals("D", queue.take());
//     assertEquals(null, queue.take());
//     queue.put("E");
//     assertEquals("E", queue.take());
//     assertEquals(null, queue.take());
    
//   }

}
