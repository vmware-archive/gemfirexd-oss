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

import junit.framework.*;

/**
 * Unit tests for NanoTimer
 */
public class NanoTimerTest extends TestCase {

    public NanoTimerTest(String name) {
        super(name);
    }
    ///////////////////////  Test Methods  ///////////////////////

    private void _testGetTime(int waitTimeSeconds) {
	final int nanosPerMilli = 1000000;
	long start = NanoTimer.getTime();
	long startMillis = System.currentTimeMillis();
	try {
	    Thread.sleep(waitTimeSeconds * 1000);
	} catch (InterruptedException e) {
	  fail("interrupted");
	}
	long end = NanoTimer.getTime();
	long endMillis = System.currentTimeMillis();
	long elapsed = (end - start);
	long elapsedMillis = endMillis - startMillis;
	assertApproximate("expected approximately " + waitTimeSeconds + " seconds", nanosPerMilli*30,
			  elapsedMillis * nanosPerMilli, elapsed);
    }
    public void testGetTime() {
      _testGetTime(2);
    }

    private long calculateSlop() {
	// calculate how much time this vm takes to do some basic stuff.
	long startTime = NanoTimer.getTime();
	new NanoTimer();
	assertApproximate("should never fail", 0, 0, 0);
	long result = NanoTimer.getTime() - startTime;
	return result * 3; // triple to be on the safe side
    }
    
    public void testReset() {
	final long slop = calculateSlop();
	NanoTimer nt = new NanoTimer();
	long createTime = NanoTimer.getTime();
	assertApproximate("create time", slop, 0, nt.getTimeSinceConstruction());
	assertApproximate("construction vs. reset", slop, nt.getTimeSinceConstruction(), nt.getTimeSinceReset());
	assertApproximate("time since reset time same as construct", slop, NanoTimer.getTime() - createTime, nt.getTimeSinceReset());
	assertApproximate("reset time same as construct", slop, NanoTimer.getTime() - createTime, nt.reset());
	long resetTime = NanoTimer.getTime();
	assertApproximate("reset time updated", slop, NanoTimer.getTime() - resetTime, nt.getTimeSinceReset());
    }
    
    /**
     * Checks to see if actual is within range nanos of expected.
     */
    private static void assertApproximate(String message, long range,
					  long expected, long actual) {
	if ((actual < (expected - range)) || (actual > (expected + range))) {
	    fail(message + " expected to be in the range ["
                 + (expected - range) + ".." + (expected + range)
                 + "] but was:<" + actual + ">");
	}
    }
}
