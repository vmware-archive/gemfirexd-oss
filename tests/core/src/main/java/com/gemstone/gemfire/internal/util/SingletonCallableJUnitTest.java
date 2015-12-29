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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

/**
 * Test for {@link SingletonCallable}
 * 
 * @author sbawaskar
 */
public class SingletonCallableJUnitTest extends TestCase {

  public void testAllThreadsThrowException() throws Exception {
    final SingletonCallable<Boolean> callable = new SingletonCallable<Boolean>();
    // start 10 threads
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    final AtomicInteger count = new AtomicInteger();
    final AtomicInteger numExceptions = new AtomicInteger();
    final CountDownLatch waitForAllThreads = new CountDownLatch(1);
    
    for (int i=0; i<numThreads; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            waitForAllThreads.await();
            callable.runSerially(new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                count.incrementAndGet();
                Thread.sleep(500);
                throw new IOException("SWAP");
              }
            });
          } catch (Exception e) {
            // expected
            numExceptions.incrementAndGet();
          }
        }
      });
      threads[i] = t;
      t.start();
    }
    waitForAllThreads.countDown();
    
    for (int i=0; i<numThreads; i++) {
      threads[i].join();
    }
    assertEquals(1, count.get());
    assertEquals(numThreads, numExceptions.get());
  }

  public void testAllThreadsInvokeCall() throws Exception {
    final SingletonCallable<Integer> callable = new SingletonCallable<Integer>();
    // start 10 threads
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    final AtomicInteger count = new AtomicInteger();
    final CountDownLatch waitForAllThreads = new CountDownLatch(1);
    
    for (int i=0; i<numThreads; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            waitForAllThreads.await();
            callable.runSerially(new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                Thread.sleep(500);
                return count.incrementAndGet();
              }
            });
          } catch (Exception e) {
            // expected
          }
        }
      });
      threads[i] = t;
      t.start();
    }
    waitForAllThreads.countDown();
    
    for (int i=0; i<numThreads; i++) {
      threads[i].join();
    }
    assertEquals(numThreads, count.get());
  }

  static class SingletonCallableNoException<T> extends SingletonCallable<T> {
    @Override
    public boolean ignoreException(Exception e) {
      return true;
    }
  }

  public void testExOverride() throws Exception {
    final SingletonCallableNoException<Boolean> callable = new SingletonCallableNoException<Boolean>();
    // start 10 threads
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    final AtomicInteger count = new AtomicInteger();
    final AtomicInteger numExceptions = new AtomicInteger();
    final CountDownLatch waitForAllThreads = new CountDownLatch(1);
    
    for (int i=0; i<numThreads; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            waitForAllThreads.await();
            callable.runSerially(new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                count.incrementAndGet();
                Thread.sleep(500);
                throw new IOException("SWAP");
              }
            });
          } catch (Exception e) {
            // expected
            numExceptions.incrementAndGet();
          }
        }
      });
      threads[i] = t;
      t.start();
    }
    waitForAllThreads.countDown();
    
    for (int i=0; i<numThreads; i++) {
      threads[i].join();
    }
    assertEquals(10, count.get()); // since the exception is ignored callable will be called 10 times
    assertEquals(numThreads, numExceptions.get());

  }
}
