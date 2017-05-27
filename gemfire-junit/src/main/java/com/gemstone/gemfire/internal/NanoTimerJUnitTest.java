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
package com.gemstone.gemfire.internal;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;
import junit.framework.TestCase;

/**
 * Unit tests for NanoTimer. This is in addition to NanoTimerTest which is
 * also a JUnit test case for NanoTimer.
 *
 * @author Kirk Lund
 * @since 7.0
 * @see NanoTimerTest
 */
public class NanoTimerJUnitTest extends TestCase {

  public NanoTimerJUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testGetTimeIsPositive() {
    long lastTime = 0;
    for (int i = 0; i < 1000; i++) {
      final long time = NanoTimer.getTime();
      assertTrue(time >= 0);
      assertTrue(time >= lastTime);
      lastTime = time;
    }
  }
  
  public void testGetTimeIncreases() {
    final long startNanos = NanoTimer.getTime();
    final long startMillis = System.currentTimeMillis();

    waitMillis(10);

    final long endMillis = System.currentTimeMillis();
    final long endNanos = NanoTimer.getTime();
    
    long elapsedMillis = endMillis - startMillis;
    long elapsedNanos = endNanos - startNanos;
    
    assertTrue(elapsedMillis > 10);
    assertTrue(endNanos > NanoTimer.NANOS_PER_MILLISECOND * 10);
    assertTrue(elapsedNanos * NanoTimer.NANOS_PER_MILLISECOND >= elapsedMillis);
  }

  public void testInitialTimes() {
    final long nanoTime = NanoTimer.getTime();
    final NanoTimer timer = new NanoTimer();

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    assertTrue(timer.getLastResetTime() >= nanoTime);
    assertTrue(timer.getConstructionTime() >= nanoTime);
    assertTrue(NanoTimer.getTime() >= nanoTime);

    final long nanosOne = NanoTimer.getTime();
    
    waitMillis(10);
    
    assertTrue(timer.getTimeSinceConstruction() > NanoTimer.NANOS_PER_MILLISECOND * 10);
    assertTrue(timer.getTimeSinceConstruction() <= NanoTimer.getTime());
    
    final long nanosTwo = NanoTimer.getTime();
    
    assertTrue(timer.getTimeSinceConstruction() >= nanosTwo - nanosOne);
  }
  
  public void testReset() {
    final NanoTimer timer = new NanoTimer();
    final long nanosOne = NanoTimer.getTime();
    
    waitMillis(10);

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    
    final long nanosTwo = NanoTimer.getTime();
    final long resetOne = timer.reset();
    
    assertTrue(resetOne >= nanosTwo - nanosOne);
    assertFalse(timer.getConstructionTime() == timer.getLastResetTime());
    
    final long nanosThree = NanoTimer.getTime();

    waitMillis(10);
    
    assertTrue(timer.getLastResetTime() >= nanosTwo);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getLastResetTime() <= nanosThree);
    assertTrue(timer.getTimeSinceReset() < NanoTimer.getTime());
    assertTrue(timer.getTimeSinceReset() <= NanoTimer.getTime() - timer.getLastResetTime());
        
    final long nanosFour = NanoTimer.getTime();
    final long resetTwo = timer.reset();
    
    assertTrue(resetTwo >= nanosFour - nanosThree);
    
    waitMillis(10);

    assertTrue(timer.getLastResetTime() >= nanosFour);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getLastResetTime() <= NanoTimer.getTime());
    assertTrue(timer.getTimeSinceReset() <= NanoTimer.getTime() - timer.getLastResetTime());
  }
  
  /**
   * Waits until for the specified milliseconds to pass as measured by
   * {@link java.lang.System#currentTimeMillis()}.
   */
  private void waitMillis(final long millis) {
    final long millisOne = System.currentTimeMillis();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return System.currentTimeMillis() - millisOne > millis;
      }
      public String description() {
        return "System.currentTimeMillis() - millisOne > millis";
      }
    };
    DistributedTestBase.waitForCriterion(wc, 1000, millis, true);
  }
}
