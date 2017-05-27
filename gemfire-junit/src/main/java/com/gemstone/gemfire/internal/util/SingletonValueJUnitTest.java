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
package com.gemstone.gemfire.internal.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.util.SingletonValue.SingletonBuilder;

public class SingletonValueJUnitTest extends TestCase {
  public void testGet() throws Exception {
    MockCallable call = new MockCallable(null, null, false);
    SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);
    
    assertEquals(call, cc.get());
    assertTrue(cc.hasCachedValue());
  }
  
  public void testError() throws Exception {
    MockCallable call = new MockCallable(null, null, true);
    SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);
    
    try {
      cc.get();
      fail("Expected IOException");
    } catch (IOException e) {
    }
  }
  
  public void testMultithread() throws Exception {
    int count = 100;
    
    ExecutorService exec = Executors.newFixedThreadPool(count);
    final SynchronousQueue<Object> sync = new SynchronousQueue<Object>(true);
    final CountDownLatch waiting = new CountDownLatch(count - 1);
    
    final MockCallable call = new MockCallable(sync, waiting, false);
    final SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);

    final CyclicBarrier barrier = new CyclicBarrier(count + 1);
    Collection<Future<MockCallable>> results = new ArrayList<Future<MockCallable>>();
    invoke(count, exec, cc, barrier, results);

    // wait for everyone
    barrier.await();
    waiting.await();
    
    // now release the originator
    sync.put(new Object());
    
    for (Future<MockCallable> fu : results) {
      assertEquals(call, fu.get());
    }
    
    exec.shutdownNow();
  }
  
  public void testMultithreadError() throws Exception {
    int count = 100;
    
    ExecutorService exec = Executors.newFixedThreadPool(count);
    final SynchronousQueue<Object> sync = new SynchronousQueue<Object>(true);
    final CountDownLatch waiting = new CountDownLatch(count - 1);

    final MockCallable call = new MockCallable(sync, waiting, true);
    final SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);

    final CyclicBarrier barrier = new CyclicBarrier(count + 1);
    Collection<Future<MockCallable>> results = new ArrayList<Future<MockCallable>>();
    invoke(count, exec, cc, barrier, results);
    
    // wait for everyone
    barrier.await();
    waiting.await();

    // now release the originator
    sync.put(new Object());
    
    for (Future<MockCallable> fu : results) {
      try {
        fu.get();
        fail("Expected IOException");
      } catch (Exception e) {
      }
    }
    
    exec.shutdownNow();
  }
  
  public void testLoopingMultithreadError() throws Exception {
    for (int i = 0; i < 100; i++) {
      testMultithreadError();
    }
  }
  
  public void testClearWithExpect() throws Exception {
    MockCallable call = new MockCallable(null, null, false);
    SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);
    
    assertEquals(call, cc.get());
    assertTrue(cc.hasCachedValue());
    
    boolean cleared = cc.clear(null, false);
    assertFalse(cleared);
    assertTrue(cc.hasCachedValue());
    
    
    cleared = cc.clear(call, false);
    assertTrue(cleared);
    assertFalse(cc.hasCachedValue());
  }
  
  public void testClear() throws Exception {
    doClear(true);
  }

  public void testClearNoReset() throws Exception {
    doClear(false);
  }

  private void doClear(final boolean allowReset) throws Exception {
    int count = 10;
    
    ExecutorService exec = Executors.newFixedThreadPool(count);
    final SynchronousQueue<Object> sync = new SynchronousQueue<Object>(true);
    final CountDownLatch waiting = new CountDownLatch(count - 1);

    final MockCallable call = new MockCallable(sync, waiting, false);
    final SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);

    final CyclicBarrier barrier = new CyclicBarrier(count + 1);
    Collection<Future<MockCallable>> results = new ArrayList<Future<MockCallable>>();
    invoke(count, exec, cc, barrier, results);

    // wait for everyone
    barrier.await();
    waiting.await();

    // issue a clear
    MockCallable cached = cc.clear(allowReset);
    assertNull(cached);
    assertNull(cc.getCachedValue());
    assertFalse(cc.hasCachedValue());
    
    // now release the originator
    sync.put(new Object());
    
    if (allowReset) {
      assertFalse(cc.isCleared());
      
      assertFalse(cc.hasCachedValue());
      
      // release the new originator
      sync.put(new Object());
      for (Future<MockCallable> fu : results) {
        assertEquals(call, fu.get());
      }
    } else {
      assertTrue(cc.isCleared());
      assertFalse(cc.hasCachedValue());
      for (Future<MockCallable> fu : results) {
        try {
          fu.get();
          fail("Expected IOException");
        } catch (Exception e) {
        }
      }
    }
    
    exec.shutdown();
  }
  
  public void testInterrupted() throws Exception, InterruptedException {
    int count = 100;
    
    ExecutorService exec = Executors.newFixedThreadPool(count);
    final SynchronousQueue<?> sync = new SynchronousQueue<MockCallable>(true);
    final CountDownLatch waiting = new CountDownLatch(count - 1);

    final MockCallable call = new MockCallable(sync, waiting, false);
    final SingletonValue<MockCallable> cc = new SingletonValue<MockCallable>(call);

    final CyclicBarrier barrier = new CyclicBarrier(count + 1);
    Collection<Future<MockCallable>> results = new ArrayList<Future<MockCallable>>();
    invoke(count, exec, cc, barrier, results);

    // wait for everyone
    barrier.await();
    waiting.await();
    
    // interrupt the originator
    exec.shutdownNow();
    
    for (Future<MockCallable> ft : results) {
      try {
        ft.get();
        fail("Expected IOException");
      } catch (Exception e) {
      }
    }

    assertFalse(cc.hasCachedValue());
    assertNull(cc.getCachedValue());
  }

  private void invoke(int count, ExecutorService exec,
      final SingletonValue<MockCallable> cc, final CyclicBarrier barrier,
      Collection<Future<MockCallable>> results) {
    for (int i = 0; i < count; i++) {
      results.add(exec.submit(new Callable<MockCallable>() {
        @Override
        public MockCallable call() throws Exception {
          barrier.await();
          return cc.get();
        }
      }));
    }
  }
  
  private static class MockCallable implements SingletonBuilder<MockCallable>, Closeable {
    final SynchronousQueue<?> sync;
    final CountDownLatch waiting;
    final boolean err;
    
    public MockCallable(SynchronousQueue<?> sync, CountDownLatch waiting, boolean err) {
      this.sync = sync;
      this.waiting = waiting;
      this.err = err;
    }
    
    @Override
    public MockCallable create() throws IOException {
      if (sync != null) {
        try {
          sync.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }

      if (err) {
        throw new IOException("oops");
      }
      
      return this;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void postCreate() {
    }
    
    @Override
    public void createInProgress() {
      if (waiting != null) {
        waiting.countDown();
      }
    }
  }
  
}
