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
package capCon;

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.*;

public abstract class CapConTest implements java.io.Serializable {

protected static final String REGION_NAME = "capConRegion";

//static {
//  System.setProperty( "com.gemstone.gemfire.cache.LRUCapacityController.trace", "true" );
//}

// static instance of the test class
public static CapConTest testInstance; 

// instance fields
protected RandomValues randomValues;
protected static NanoTimer nt = new NanoTimer();
protected int LRUAllowance = -1;
protected int numVMs = -1;      // number of VMs connected to the same gemfire system as this VM
protected int numThreads = -1;  // number of threads in this VM
protected boolean useTransactions; // true if the test should use transactions

/** Return a new key object to put into the region.
 */
public abstract Object getNewKey();

/** Return a new value object to put into the region.
 */
public abstract Object getNewValue();

/** Return the object attributes to be used when putting an object in a region.
 */
public abstract CacheListener getEventListener();

/** Return a key previously used in this test.
 */
public abstract Object getPreviousKey();

// initialization methods
// ================================================================================
protected void initialize() {
   useTransactions = TestConfig.tab().booleanAt(CapConPrms.useTransactions, false);
   String clientName = System.getProperty("clientName");
   ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
   numVMs = cd.getVmQuantity();
   numThreads = cd.getVmThreads();
   randomValues = new RandomValues();
   Cache myCache = CacheUtil.createCache();
   CacheListener aListener = getEventListener();
   CacheLoader cacheLoader = new CapConLoader();
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   Log.getLogWriter().info("Using RegionDefinition " + regDef + " to create region");
   regDef.createRootRegion(myCache, REGION_NAME, aListener, cacheLoader, null); 
   CapConBB.getBB().printSharedCounters();
}

// method to do work
// ================================================================================
protected void addEntries() {
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In addEntries, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   boolean inTrans = false;
   if (useTransactions) {
      TxHelper.begin();
      inTrans = true;
   }
   GsRandom rand = TestConfig.tab().getRandGen();
   do {
      if (rand.nextBoolean())
         addEntry();
      else
         randomGet();
   } while ((timeToRunSec != 0) && (System.currentTimeMillis() - startTime < timeToRunMS));
   if (inTrans) {
      if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
         try {
            TxHelper.commit();
         } catch (ConflictException e) {
            Log.getLogWriter().info("addEntries caught ConflictException " + e + ", expected with concurrent execution tests (loads can now cause conflicts), continuing with test");
         }
      } else {
         TxHelper.rollback();
      }
   }
   Log.getLogWriter().info("In addEntries, done running for " + timeToRunSec + " seconds");
}

protected Object[] addEntry() {
   Region workRegion = getWorkRegion();
   Object key = getNewKey();
   boolean useCacheLoader = useCacheLoader();
   Object value = null;
   if (useCacheLoader) { // use a get
      String aStr = key + " in " + TestHelper.regionToString(workRegion, false);
      Log.getLogWriter().info("Getting " + aStr + " with callback object " + this);
      try {
         nt.reset();
         value = workRegion.get(key, this);
         long duration =  nt.reset();
         Log.getLogWriter().info("Done getting " + aStr + "; get took " + TestHelper.nanosToString(duration));
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } else { // use a put
      value = getNewValue();
      try {
         String aStr = key + " with value " + TestHelper.toString(value) + " in " + TestHelper.regionToString(workRegion, false);
         Log.getLogWriter().info("Putting " + aStr);
         nt.reset();
         workRegion.put(key, value);
         long duration =  nt.reset();
         Log.getLogWriter().info("Done putting " + aStr + "; put took " + TestHelper.nanosToString(duration));
      } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } 
   checkForEventError();
   return new Object[] {key, value};
}

/** Do a get in order to try to bring in entries from other distributed
 *  caches. This get may or may not actually bring in a new entry.
 */
protected void randomGet() {
   Region workRegion = getWorkRegion();
   Object key = getPreviousKey();
   String aStr = key + " in " + TestHelper.regionToString(workRegion, false);
   Log.getLogWriter().info("Getting previous key " + aStr + " with callback object " + this);
   try {
      nt.reset();
      workRegion.get(key, this);
      long duration =  nt.reset();
      Log.getLogWriter().info("Done getting " + aStr + "; get took " + TestHelper.nanosToString(duration));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

static protected void checkForEventError() {
   TestHelper.checkForEventError(CapConBB.getBB());
}

protected Region getWorkRegion() {
   Region workRegion = CacheUtil.getCache().getRegion(REGION_NAME);
   return workRegion;
}

// end task methods
//========================================================================
/** Check that the test evicted enough */
public static void HydraTask_endTask() {
   CapConBB.getBB().printSharedCounters();
   checkForEventError();
   double numEvictions = TestHelper.getNumLRUEvictions();
   Log.getLogWriter().info("Number of lru evictions during test: " + numEvictions);
}

public boolean useCacheLoader() {
   if (CapConBB.fixUseCacheLoader())
      return CapConBB.useCacheLoader();
   else   
      return TestConfig.tab().booleanAt(CapConPrms.useCacheLoader);
}
}
