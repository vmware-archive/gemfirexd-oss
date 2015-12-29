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

import com.gemstone.gemfire.cache.*;
import java.rmi.RemoteException;

import hydra.*;
import hydra.blackboard.*;
import util.*;

/**
 *
 * Basic test of cache loader functionality.  
 * Contains tasks for preloading, reading value, and invalidating.
 * Additional task updates data used by cache loader.
 * 
 * Most client threads read values in distributed cache.  
 * Single thread periodically updates base value used by cache loader 
 * and invalidates object being read.
 *
 * @author Marilyn Daum
 *
 * @since 2.0
 */

public class Client extends Util {

    protected static HydraThreadLocal localClientInstance = new HydraThreadLocal();
    protected int tid;
    protected int threadReadTime;
    protected int threadNumTimedReads;
    protected int threadPutTime;
    protected int threadNumTimedPuts;

    //--------------------------------------------------------------------------
    // hydra tasks

    /** 
     * Initialize Client
     */
    public static void initClientTask() {
	Client clientInstance = new Client();
	localClientInstance.set(clientInstance);
	clientInstance.tid = RemoteTestModule.getCurrentThread().getThreadId();
	clientInstance.threadReadTime = 0;
	clientInstance.threadNumTimedReads = 0;
	clientInstance.threadPutTime = 0;
	clientInstance.threadNumTimedPuts = 0;
    }

    /** 
     * Create the cache and define regions and entries,
     * including setting the cache loader for CachedData.
     */
    public static synchronized void defineCacheRegions() {

	thisClient().defineCacheRegions(TestParms.getDefineCacheLoader());
    }

    /**
     * Shutdown the cache.
     */
    public static synchronized void closeCacheTask() {
	thisClient().closeCache();
    }

    /**
     * Store inital value of MasterData in the cache and preLoad CachedData.
     */
    public static synchronized void initializeDataTask() {
	thisClient().initializeData();
    }

    /**
     * Read value of CachedData.
     */
    public static void readTask() {
	for (int i = 0; i < TestParms.getReadIterations(); i++) {
	    thisClient().read();
	}
    }

    /**
     * Update value of MasterData, then invalidate CachedData
     * to force execution of CacheLoader.
     */
    public static void updateTask() {
	thisClient().update();
    }

    /** 
     * Summarize results for Client
     */
    public static void summarizeClientTask() {
	if (thisClient().threadNumTimedReads > 0)
	    Log.getLogWriter().info
		("Average time for region.get(" + CACHED_REGION_NAME + 
		 ") calls in slave " + thisClient().tid + " = " + 
		 (thisClient().threadReadTime / (thisClient().threadNumTimedReads * 1.0)) +
		 " milliseconds (" + thisClient().threadNumTimedReads + " data points)");
	if (thisClient().threadNumTimedPuts > 0)
	    Log.getLogWriter().info
		("Average time for region.put(" + MASTER_REGION_NAME + 
		 ") calls in slave " + thisClient().tid + " = " + 
		 (thisClient().threadPutTime / (thisClient().threadNumTimedPuts * 1.0)) +
		 " milliseconds (" + thisClient().threadNumTimedPuts + " data points)");
    }

    /**
     * Verify results from within a VM active during main task phase.
     */
    public static synchronized void verifyResultsTask() {
	thisClient().verifyResults(true);
    }

    /**
     * Verify final results, after all regular task VMs have terminated.
     */
    public static synchronized void verifyFinalResultsTask() {
 	thisClient().verifyResults(false);
    }

    //--------------------------------------------------------------------------


    /** 
     * Create the cache, create regions ({@link #REGION_NAME}, {@link
     * #MASTER_REGION_NAME}, and {@link #CACHED_REGION_NAME}.
     */
    private void defineCacheRegions(boolean defineCacheLoader) {
	if (CacheUtil.getCache() == null) {
	    Log.getLogWriter().info("Creating the cache");

	    Cache cache = CacheUtil.createCache();
	    cache.setLockTimeout(TestParms.getLockTimeout());
	    cache.setSearchTimeout(TestParms.getSearchTimeout());

	    // Root and Master region use config values for
	    // scope, and mirror type only
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(CacheBB.getBB().getScopeAttribute());
            factory.setRegionTimeToLive(ExpirationAttributes.DEFAULT);
            factory.setRegionIdleTimeout(ExpirationAttributes.DEFAULT);
            factory.setMirrorType(CacheBB.getBB().getMirrorAttribute());

            RegionAttributes regAttrs =
		factory.createRegionAttributes();

	    Region parent = CacheUtil.createRegion(REGION_NAME, 
						   regAttrs);

            Region master;
            try {
		master =
		    parent.createSubregion(MASTER_REGION_NAME, regAttrs);

            } catch (RegionExistsException ex) {
		String s = "Created the cache, defined " + " region " + REGION_NAME;
		Log.getLogWriter().info(s);
		s = "Assuming subregions have already been mapped into cache";
		// 		Log.getLogWriter().info(s, ex);
		Log.getLogWriter().info(s);
		return;
            } 
//            catch (CacheException ex) {
//		String s = "Unable to create " + MASTER_REGION_NAME;
//		throw new HydraRuntimeException(s, ex);
//            }
            
	    // Root and Master region use config values for
	    // scope, and mirror type, plus
	    // expiration, listener, and loader


            ExpirationAttributes ttl = new ExpirationAttributes
		(TestParms.getEntryTTLSec(),
		 TestHelper.getExpirationAction
		 (TestParms.getEntryTTLActionString()));
	    factory.setEntryTimeToLive(ttl);

            ExpirationAttributes idle = new ExpirationAttributes
		(TestParms.getEntryIdleTimeoutSec(),
		 TestHelper.getExpirationAction
		 (TestParms.getEntryIdleTimeoutActionString()));
	    factory.setEntryIdleTimeout(idle);
	    // Statistics must be enabled if any expiration attributes are set
	    factory.setStatisticsEnabled(true);

	    ObjectListener objEventListnr = new ObjectListener();
	    factory.setCacheListener(objEventListnr);
	    factory.setCacheWriter(objEventListnr);
	    if (defineCacheLoader) {
		CacheLoader cacheLoader = new Loader();
		factory.setCacheLoader(cacheLoader);
	    }


            try {
		parent.createSubregion(CACHED_REGION_NAME,
				       factory.createRegionAttributes());
            } catch (CacheException ex) {
		String s = "Unable to create " + CACHED_REGION_NAME;
		throw new HydraRuntimeException(s, ex);
            }

            String s = "Created the cache, defined region " + REGION_NAME + 
                " and defined subregions " +
		MASTER_REGION_NAME + " and " + CACHED_REGION_NAME + 
		((defineCacheLoader)? " (set cacheLoader)" : "");
	    Log.getLogWriter().info(s);
	}
    }


    /**
     * Shutdown the cache.
     */
    private void closeCache() {
	Log.getLogWriter().info("Closing the cache");
	CacheUtil.closeCache();
	Log.getLogWriter().info("Closed the cache");
    }


    /**
     * Store inital value of MasterData in the cache and preLoad CachedData.
     */
    private void initializeData() {
        Region master = getRootRegion().getSubregion(MASTER_REGION_NAME);
        Region cached = getRootRegion().getSubregion(CACHED_REGION_NAME);

	Integer initValue = new Integer(0);
	try {
	    master.put(DATA_NAME, initValue);
	    if (master.getEntry(DATA_NAME) == null) {
		String s = MASTER_REGION_NAME + "/" +
		    DATA_NAME + " not yet present";
		throw new HydraRuntimeException(s);
            }
	} catch (Exception ex) {
	    throw new HydraRuntimeException
		("Unable to initialize " + MASTER_REGION_NAME + "/"
                 + DATA_NAME + " with value " + initValue, ex);
	}

	try {
	    cached.create(DATA_NAME, null);
	    cached.get(DATA_NAME);
	    if (!cached.containsValueForKey(DATA_NAME))
		throw new HydraRuntimeException(CACHED_REGION_NAME + "/" + DATA_NAME + 
						" value not yet present");
	} catch (CacheException ex) {
	    throw new HydraRuntimeException
		("Unable to define " + CACHED_REGION_NAME + "/" +
                 DATA_NAME, ex);
	}
    }


    /**
     * Read value of CachedData.
     */
    private void read() {
	Region cached = getRootRegion().getSubregion(CACHED_REGION_NAME);
	Integer currentValue;
	long t1 = 0, t2 = 0;
	try {
	    t1 = System.currentTimeMillis();
	    currentValue = (Integer) cached.get(DATA_NAME);
	    t2 = System.currentTimeMillis();

        } catch( TimeoutException e ) {
	    Log.getLogWriter().info("Got timeout exception - dumping stacks, lock table");
	    try {
		RemoteTestModule.Master.printClientProcessStacks();
		// for linux: sleep a few seconds, then flush System.out and System.err
		MasterController.sleepForMs(2500);
		System.out.flush();
		System.err.flush();
	    } catch (RemoteException ex) {
		System.err.println("While trying to dump stacks, got: " + ex);
	    }
	    throw new HydraRuntimeException
	        ("Unable to read " + CACHED_REGION_NAME + "/" +
                 DATA_NAME, e);

	} catch (Exception ex) {
	    throw new HydraRuntimeException
		("Unable to read " + CACHED_REGION_NAME + "/" +
                 DATA_NAME, ex);
	}
	if (TestParms.getLogDetails())
	    Log.getLogWriter().info
		("Read " + Util.log(cached, DATA_NAME, currentValue));

	// increment counters
	SharedCounters counters = BB.getInstance().getSharedCounters();
	counters.increment(BB.NUM_READ_CALLS);
	if (counters.read(BB.NUM_READ_CALLS) > 100) {  // warm-up
	    counters.increment(BB.NUM_TIMED_READ_CALLS);
	    counters.add(BB.TIME_READ_CALLS, (t2 - t1));
	    thisClient().threadReadTime += (t2 - t1);
	    thisClient().threadNumTimedReads++;
	}
    }


    /**
     * Update value of MasterData, then invalidate CachedData
     * to force execution of CacheLoader.
     */
    private void update() {
	long t1 = 0, t2 = 0;
	Region master = getRootRegion().getSubregion(MASTER_REGION_NAME);
	Region cached = getRootRegion().getSubregion(CACHED_REGION_NAME);

	// update value of MasterData
	Integer currentValue;
	try {
	    currentValue = (Integer) master.get(DATA_NAME);
	} catch (Exception ex) {
	    throw new HydraRuntimeException
		("Unable to read " + MASTER_REGION_NAME + "/" +
                 DATA_NAME, ex);
	}
	if (TestParms.getLogDetails())
	    Log.getLogWriter().info
		("Read " + Util.log(master, DATA_NAME, currentValue));
	Integer newValue = new Integer(currentValue.intValue() + 1);
	try {
	    t1 = System.currentTimeMillis();
	    master.put(DATA_NAME, newValue);
	    t2 = System.currentTimeMillis();
	} catch (Exception ex) {
	    throw new HydraRuntimeException
		("Unable to update " + MASTER_REGION_NAME + "/" +
                 DATA_NAME + " with value " + newValue, ex);
	}
	if (TestParms.getLogDetails())
	    Log.getLogWriter().info
		("Updated " + Util.log(master, DATA_NAME, newValue));

	// increment counters
	SharedCounters counters = BB.getInstance().getSharedCounters();
	counters.increment(BB.NUM_INVALIDATE_UPDATE_CALLS);
	if (counters.read(BB.NUM_INVALIDATE_UPDATE_CALLS) > 100) {  // warm-up
	    counters.increment(BB.NUM_TIMED_PUT_CALLS);
	    counters.add(BB.TIME_PUT_CALLS, (t2 - t1));
	    thisClient().threadPutTime += (t2 - t1);
	    thisClient().threadNumTimedPuts++;
	}

	// invalidate CachedData
	try {
	    cached.invalidate(DATA_NAME);

	} catch (EntryNotFoundException ex) {
	    if ((TestHelper.getExpirationAction
		 (TestParms.getEntryIdleTimeoutActionString()) == 
		 ExpirationAction.DESTROY) ||
		(TestHelper.getExpirationAction
		 (TestParms.getEntryTTLActionString()) == 
		 ExpirationAction.DESTROY))
		return;    // assume entry was destroyed during expiration
	    else {
		String s = CACHED_REGION_NAME + "/" + DATA_NAME + 
		    " not found for invalidate";
		throw new HydraRuntimeException(s, ex);
            }

	} 
//  catch (CacheException ex) {
//	    throw new HydraRuntimeException
//		("Unable to invalidate " + CACHED_REGION_NAME + "/" +
//                 DATA_NAME, ex);
//	}

	if (TestParms.getLogDetails())
	    Log.getLogWriter().info
		("Invalidated " + Util.log(cached, DATA_NAME));
    }


    /**
     * Verify final results.
     */
    private void verifyResults(boolean checkDataValue) {
	Region master = getRootRegion().getSubregion(MASTER_REGION_NAME);
	Log.getLogWriter().info
	    ("Checking " + Util.log(master, DATA_NAME));

	SharedCounters counters = BB.getInstance().getSharedCounters();
	BB.getInstance().printSharedCounters();
	TestParms.logParms();
	long numUpdates = counters.read(BB.NUM_INVALIDATE_UPDATE_CALLS);
	long numInvalidateCallbacks = counters.read(BB.NUM_INVALIDATE_CALLBACKS);
	long numUnloadCallbacks = counters.read(BB.NUM_UNLOAD_CALLBACKS);
	long numLoads = counters.read(BB.NUM_LOAD_CALLS);
	int idleTime = TestParms.getEntryIdleTimeoutSec();
	int ttl = TestParms.getEntryTTLSec();
	int numVMs = TestConfig.getInstance().getTotalVMs();
	int numThreads = TestConfig.getInstance().getTotalThreads();

	// check number of explicit invalidate calls vs. callbacks to 
	// object event listener; there should be one callback per VM
	if (idleTime == 0 && ttl == 0) {
	    if ((numUpdates * numVMs) != numInvalidateCallbacks)
		Log.getLogWriter().info
		    //  		throw new HydraRuntimeException
		    ("Number of update/invalidate calls " + numUpdates + 
		     " times number of VMs " + numVMs +
		     " not equal to number of invalidation callbacks " + 
		     numInvalidateCallbacks);
	}

	// check number of updates vs. final value of MasterData
	if (checkDataValue) {
	    Integer finalValue;
	    try {
		finalValue = (Integer) master.get(DATA_NAME);
		Log.getLogWriter().info ("finalValue = " + finalValue);
	    } catch (Exception ex) {
		throw new HydraRuntimeException
		    ("Unable to read " + MASTER_REGION_NAME + "/" +
		     DATA_NAME, ex);
	    }
	    if (finalValue.intValue() != numUpdates)
		throw new HydraRuntimeException
		    ("Number of updates " + numUpdates + 
		     " does not equal final value " + 
		     Util.log(master, DATA_NAME, finalValue));
	}

	// check number of invocations of cacheLoader (including preLoad)
	// vs. number of updates
	if (idleTime == 0 && ttl == 0) {
	    boolean mismatch = false;
	    if (numLoads < numUpdates) 
		mismatch = true;
	    else {
		Scope scope = CacheBB.getBB().getScopeAttribute();
		if (scope.equals(Scope.GLOBAL)) {
		    // gets are synchronized, preventing multiple load invocations
		    if (numLoads > numUpdates + 1)
			mismatch = true;
		} 
		else {
		    // SCOPE_DISTRIBUTED_ACK or SCOPE_DISTRIBUTED_NO_ACK
		    // gets are not synchronized, so extra loads can occur
		    if (numLoads > (numUpdates + 1) * numVMs)
			mismatch = true;
		}
	    }
	    if (mismatch)
		Log.getLogWriter().info
		    // 	        throw new HydraRuntimeException
		    ("Number of update/invalidate calls " + numUpdates + 
		     " not consistent with number of cacheLoader invocations " +
		     numLoads);
	}

	// check number of expirations -- details TBD
	long numReads = counters.read(BB.NUM_READ_CALLS);
	long taskTime = TestConfig.tab().longAt(Prms.totalTaskTimeSec);
	long numReadsPerThread;
	if (numUpdates == 0)
	    numReadsPerThread = (numReads / numThreads );
	else
	    numReadsPerThread = (numReads / (numThreads - 1) );
	if (numReadsPerThread > 0)
	    Log.getLogWriter().info
		("\n   number reads per thread = " + numReadsPerThread + 
		 "\n   taskTime = " + taskTime + 
		 "\n   read task start interval = " + taskTime / numReadsPerThread);
	if (numUpdates > 0)
	    Log.getLogWriter().info
		("\n   write task start interval = " + taskTime / numUpdates);

	// display average read time
	long timedReads = counters.read(BB.NUM_TIMED_READ_CALLS);
	long readTime = counters.read(BB.TIME_READ_CALLS);
	if (timedReads > 0)
	    Log.getLogWriter().info
		("Average time for region.get(" + 
		 CACHED_REGION_NAME + ") calls = " + 
		 (readTime / (timedReads * 1.0)) + " milliseconds (" +
		 timedReads + " data points)");

	// display average put time
	long timedPuts = counters.read(BB.NUM_TIMED_PUT_CALLS);
	long putTime = counters.read(BB.TIME_PUT_CALLS);
	if (timedPuts > 0)
	    Log.getLogWriter().info
		("Average time for region.put(" + 
		 MASTER_REGION_NAME + ") calls = " + 
		 (putTime / (timedPuts * 1.0)) + " milliseconds (" +
		 timedPuts + " data points)");

	Log.getLogWriter().info
	    ("Checked " + Util.log(master, DATA_NAME));
    }

    //--------------------------------------------------------------------------
    // internal/test utility methods

    protected static Client thisClient() {
	// 	try {
	// 	    return (Client)localClientInstance.get();
	// 	} catch (NullPointerException ex) {
	// 	    return new Client();
	// 	}
 	Object lci = localClientInstance.get();
 	if (lci == null)
 	    return new Client();
 	else
 	    return (Client)lci;
    }

    /**
     * Returns the root of the {@linkplain Util#MASTER_REGION_NAME
     * master} and {@linkplain Util#CACHED_REGION_NAME cache} regions.
     */
    protected Region getRootRegion() {
	return CacheUtil.getRegion(REGION_NAME);
    }

}
