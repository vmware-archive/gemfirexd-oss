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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.util.AbortableTaskService.AbortableTask;

public class AbortableTaskServiceJUnitTest extends TestCase {
  private AbortableTaskService tasks;
  
  public void testFinish() throws InterruptedException {
    DelayedTask dt = new DelayedTask();
    tasks.execute(dt);
    
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        tasks.waitForCompletion();
      }
    });
    
    
    dt.delay.countDown();
    t.join();
    
    assertEquals(false, dt.wasAborted.get());
  }
  
  public void testAbort() throws InterruptedException {
    DelayedTask dt = new DelayedTask();
    tasks.execute(dt);
    
    tasks.abortAll();
    dt.delay.countDown();
    
    tasks.waitForCompletion();
    assertEquals(true, dt.wasAborted.get());
  }
  
  public void testAbortBeforeRun() throws InterruptedException {
    DelayedTask dt = new DelayedTask();
    DelayedTask dt2 = new DelayedTask();
    
    tasks.execute(dt);
    tasks.execute(dt2);
    
    tasks.abortAll();
    dt.delay.countDown();
    
    tasks.waitForCompletion();
    assertEquals(true, dt.wasAborted.get());
    assertEquals(true, dt2.wasAborted.get());
  }
  
  public void setUp() {
    tasks = new AbortableTaskService(Executors.newSingleThreadExecutor());
  }
  
  private static class DelayedTask implements AbortableTask {
    private final CountDownLatch delay = new CountDownLatch(1);
    private final AtomicBoolean wasAborted = new AtomicBoolean(false);
    private final AtomicBoolean wasRun = new AtomicBoolean(false);

    @Override
    public void runOrAbort(AtomicBoolean aborted) {
      try {
        delay.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      wasRun.set(true);
      wasAborted.set(aborted.get());
    }

    @Override
    public void abortBeforeRun() {
      wasAborted.set(true);
    }
  }
}
