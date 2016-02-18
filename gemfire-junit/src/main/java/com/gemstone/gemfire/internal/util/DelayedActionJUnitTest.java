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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

public class DelayedActionJUnitTest extends TestCase {
  public void testDelay() throws InterruptedException {
    final AtomicBoolean hit = new AtomicBoolean(false);
    final CountDownLatch complete = new CountDownLatch(1);
    
    Runnable r = new Runnable() {
      @Override
      public void run() {
        hit.set(true);
        complete.countDown();
      }
    };
    
    DelayedAction delay = new DelayedAction(r);
    
    ExecutorService exec = Executors.newSingleThreadExecutor();
    exec.execute(delay);
    
    delay.waitForArrival();
    assertFalse(hit.get());
    
    delay.allowToProceed();
    complete.await();
    assertTrue(hit.get());
  }
}
