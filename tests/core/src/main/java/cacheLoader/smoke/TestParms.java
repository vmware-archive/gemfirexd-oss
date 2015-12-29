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
package cacheLoader.smoke;

import hydra.*; 
import hydra.blackboard.*;
import util.*;
import perffmwk.PerfStatMgr;
//import com.gemstone.gemfire.LogWriter;
//import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import java.text.DecimalFormat;

public class TestParms extends BasePrms {

    //---------------------------------------------------------------------
    // Default Values

    public static final String DEFAULT_EXPIRATION_ACTION = "INVALIDATE";
    /* Valid expiration action values:
     *   DESTROY
     *   INVALIDATE
     *   LOCAL_DESTROY
     *   LOCAL_INVALIDATE
     */


    //---------------------------------------------------------------------
    // Test-specific parameters

    /** (boolean) controls whether CacheLoader is defined
     */
    public static Long defineCacheLoader; 

    /* 
     * Returns boolean value of TestParms.defineCacheLoader.
     * Defaults to false.
     */
    protected static boolean getDefineCacheLoader() {
	Long key = defineCacheLoader;
	return (tasktab().booleanAt(key, tab().booleanAt(key, false)));
    }


    /**
     *  searchTimeout.  Defaults to GemFireCache.DEFAULT_SEARCH_TIMEOUT.
     */
    public static Long searchTimeout;

    protected static int getSearchTimeout() {
	Long key = searchTimeout;
	return (tasktab().intAt(key, tab().intAt(key, GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT)));
    }

    /**
     *  lockTimeout.  Defaults to GemFireCache.DEFAULT_LOCK_TIMEOUT.
     */
    public static Long lockTimeout;

    protected static int getLockTimeout() {
	Long key = lockTimeout;
	return (tasktab().intAt(key, tab().intAt(key, GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT)));
    }

    /**
     *  Number of reads per read task.  Defaults to 1.
     */
    public static Long readIterations;

    protected static int getReadIterations() {
	Long key = readIterations;
	return (tasktab().intAt(key, tab().intAt(key, 1)));
    }

    /**
     *  Indicates whether detailed messages should be logged.  Defaults to false.
     */
    public static Long logDetails;

    protected static boolean getLogDetails() {
	Long key = logDetails;
	return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    static { 
	BasePrms.setValues(TestParms.class); 
    }


    /*
     * Returns int value of entryTTLSec.  Defaults to 0.
     */ 
    protected static int getEntryTTLSec() {
	Long key = util.CachePrms.entryTTLSec;
	return (tasktab().intAt(key, tab().intAt(key, 0)));
    }

    /* 
     * Returns String value of entryTTLAction.
     * Defaults to DEFAULT_EXPIRATION_ACTION.
     */ 
    protected static String getEntryTTLActionString() {
	Long key = util.CachePrms.entryTTLAction;
	return (tasktab().stringAt(key, tab().stringAt(key, DEFAULT_EXPIRATION_ACTION)));
    }

    /*
     * Returns int value of entryIdleTimeoutSec.  Defaults to 0.
     */ 
    protected static int getEntryIdleTimeoutSec() {
	Long key = util.CachePrms.entryIdleTimeoutSec;
	return (tasktab().intAt(key, tab().intAt(key, 0)));
    }

    /* 
     * Returns String value of entryIdleTimeoutAction.
     * Defaults to DEFAULT_EXPIRATION_ACTION.
     */ 
    protected static String getEntryIdleTimeoutActionString() {
	Long key = util.CachePrms.entryIdleTimeoutAction;
	return (tasktab().stringAt(key, tab().stringAt(key, DEFAULT_EXPIRATION_ACTION)));
    }

    /**
     * Write values of random parameters to log.
     */
    public static void logParms() {
	Log.getLogWriter().info
	    ("\n    Scope: " + CacheBB.getBB().getScopeAttribute() +
 	     "\n    MirrorRegion: " + CacheBB.getBB().getMirrorAttribute() +
	     "\n    TTL: " + TestParms.getEntryTTLSec() +
	     "\n    TTLExpirAction: " + TestParms.getEntryTTLActionString() +
	     "\n    IdleTimeout: " + TestParms.getEntryIdleTimeoutSec() +
	     "\n    IdleTimeoutExpirAction: " + TestParms.getEntryIdleTimeoutActionString() +
	     "\n    searchTimeout: " + TestParms.getSearchTimeout() +
	     "\n    lockTimeout: " + TestParms.getLockTimeout());

	    }


    /**
     * Writes final performance timings to a PerfStatMgr instance.
     */
    public static boolean writePerfResultsTask() {
	SharedCounters counters = BB.getInstance().getSharedCounters();
	PerfStatMgr report = PerfStatMgr.getInstance();

	// report average read time
	long timedReads = counters.read(BB.NUM_TIMED_READ_CALLS);
	long readTime = counters.read(BB.TIME_READ_CALLS);
	if (timedReads > 0) {
	    Log.getLogWriter().info(noteTiming(timedReads, "read calls", 0, readTime, "milliseconds"));
	}

	// report average put time
	long timedPuts = counters.read(BB.NUM_TIMED_PUT_CALLS);
	long putTime = counters.read(BB.TIME_PUT_CALLS);
	if (timedPuts > 0) {
	    Log.getLogWriter().info(noteTiming(timedPuts, "put calls", 0, putTime, "milliseconds"));
	}

	return true;
    }

    public static String noteTiming(long operations, String operationUnit,
                                    long beginTime, long endTime,
                                    String timeUnit)
    {
	long delta = endTime - beginTime;
	StringBuffer sb = new StringBuffer();
	sb.append("  Performed ");
	sb.append(operations);
	sb.append(" ");
	sb.append(operationUnit);
	sb.append(" in ");
	sb.append(delta);
	sb.append(" ");
	sb.append(timeUnit);
	sb.append("\n");

	double ratio = ((double) operations) / ((double) delta);
	sb.append("    ");
	sb.append(format.format(ratio));
	sb.append(" ");
	sb.append(operationUnit);
	sb.append(" per ");
	sb.append(timeUnit);
	sb.append("\n");

	ratio = ((double) delta) / ((double) operations);
	sb.append("    ");
	sb.append(format.format(ratio));
	sb.append(" ");
	sb.append(timeUnit);
	sb.append(" per ");
	sb.append(operationUnit);
	sb.append("\n");

	return sb.toString();
    }

    /** For formatting timing info */
    private static final DecimalFormat format = new DecimalFormat("###.###");

} 

  
