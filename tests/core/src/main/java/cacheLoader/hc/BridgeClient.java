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
package cacheLoader.hc;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.cache.ClientHelper;
import java.lang.reflect.*;
import java.util.*;

import hydra.*;
import util.*;

/**
 * Hydra tasks for hierarchical cache loader tests.
 */

public class BridgeClient {

  protected static final String REGION_NAME = "Bloomberg";

  //--------------------------------------------------------------------------
  // Cache Initializers

  /** 
   * Initialize the bridge cache and region.
   */
  public static synchronized void initBridgeCacheTask() {
    Util.registerDriver();
    initBridgeCache();
  }

  /**
   * Create the edge cache and region.
   */
  public static synchronized void initEdgeCacheTask() {
    BridgeClient clientInstance = new BridgeClient();
    clientInstance.initEdgeCache();
  }

  /**
   * Shut down the cache.
   */
  public static synchronized void closeCacheTask() {
    CacheHelper.closeCache();
  }

    /**
     * Read value of CachedData.
     */
    public static boolean readTask() {
  BridgeClient clientInstance = new BridgeClient();
  clientInstance.initTestParameters();
  // used to do: localfaults.set( new Long( getMajorPageFaults() ) );
  //  clientInstance.initReportToBlackboard();
  clientInstance.read(clientInstance.trimIterations);
  long nanoTime = clientInstance.read(clientInstance.workIterations);
  clientInstance.read(clientInstance.trimIterations);

  //  clientInstance.reportToBlackboard(nanoTime);
  long msTime = timeInMillis(nanoTime);
  Log.getLogWriter().info
      ("Completed " + clientInstance.workIterations + " timed get calls in " + msTime +
       " ms ==> average " + (msTime / (clientInstance.workIterations * 1.0)) + " ms per get");
  return true;
    }

    //--------------------------------------------------------------------------

    /**
     * Create the cache and regions for bridges, and start the server.
     */
    private static synchronized void initBridgeCache() {

      Cache cache = CacheHelper.createCache("bridge");

      // Instantiate the cache loader here and add it to the region attributes
      // manually so it can be made to initialize its connection pool.
      //
      // Do not do this in a static initializer to avoid creating a pool in the
      // hydra master at test configuration time.
      //
      // Do not create the pool lazily during the first load to avoid causing
      // tests with low timeouts to fail.
      //
      AttributesFactory factory = RegionHelper.getAttributesFactory("bridge");
      String serverLoaderClassname = BridgeParms.getServerLoaderClassname();
      CacheLoader loader = (CacheLoader)RegionDescription.getInstance(
          BridgeParms.serverLoaderClassname, serverLoaderClassname);
      try {
        Method meth = loader.getClass().getMethod("init", new Class[0]);
        meth.invoke(loader, new Object[0]);
      } catch (Exception e) {
        String s = "Failed invoking " + loader.getClass().getName() + ".init";
        throw new HydraConfigException(s, e);
      }
      factory.setCacheLoader(loader);
      Region region = RegionHelper.createRegion(REGION_NAME, factory);

      BridgeHelper.startBridgeServer("bridge");
    }

    /**
     * Create the cache and regions for edges.
     */
    private void initEdgeCache() {
      Cache cache = CacheHelper.createCache("edge");
      Region region = RegionHelper.createRegion(REGION_NAME, "edge");
    }

    // @todo: if careful validation, maybe use
    //  cacheBridgeLoader.getActiveServerList/getDeadServerList
    //  for validation
    /**
     * Read numIterations random entries.  Get time is recorded in perfstat(1).
     * Returns elapsed get time in nanoseconds.
     */
    private long read(int numIterations) {
  boolean logDetails = BridgeParms.getLogDetails();
  boolean validate = BridgeParms.getValidate();
  int numElements = TestConfig.tab().intAt(DBParms.numPreload);
  Random rand = new Random();
  Region region = RegionHelper.getRegion(REGION_NAME);
  String key = null;
  int keyValue;
  Object value;
  long startTime = -1;
  NanoTimer timer = new NanoTimer();
  for (int i = 0; i < numIterations; i++) {
      try {
    // get random key in range
    keyValue = rand.nextInt(numElements - 1) + 1;
    key = Integer.toString(keyValue);
    if (logDetails)
        Log.getLogWriter().info("Retrieving value for " + key);
    startTime = this.perfstats.startOperation();
    value = region.get(key); 
    this.perfstats.endOperation(startTime);
    if (logDetails || validate) {
        byte[] byteArrayValue = null;
        try {
                byteArrayValue = (byte[]) value;
        } catch(ClassCastException cce) {
                ClassCastException cce2 = new ClassCastException("Expected byte[] but found:"+value.getClass().getName());
                cce2.initCause(cce);
                throw cce2;
        }
        byte[] keyBytes = new byte[4];
        StringBuffer aStr = new StringBuffer();
        for (int j=0; j<4; j++) {
          keyBytes[j] = byteArrayValue[j];
          aStr.append("   keyBytes[" + j + "] = " + keyBytes[j] + "\n");
        }
        int retrievedKey = Util.bytesTOint(keyBytes);
        if (logDetails)
      Log.getLogWriter().info
          ("Retrieved value " + retrievedKey +
           ":" + value + " for " + keyValue + "\n" + aStr.toString());
        if (validate) {
      if (retrievedKey != keyValue)
          throw new TestException
        ("Validation Error: Requested value for key " +
         key + "; received " + retrievedKey + "\n" + aStr.toString());
        }
    }


      } catch (Exception ex) {
         this.perfstats.endOperation(startTime);
         String errStr = ex.toString();
         if (isSocketTimeout(errStr)) {
            Log.getLogWriter().info("Unable to read " + key + ": " + ex + "; continuing test");
         } else {
            Throwable cause = ex.getCause();
            if(cause!=null && isSocketTimeout(cause.toString())) {
              Log.getLogWriter().info("Unable to read " + key + ": " + cause + "; continuing test");
            } else {
              throw new util.TestException(TestHelper.getStackTrace(ex));
            }
         }
      }
  }
  ClientHelper.release(region);
  return (timer.reset());
    }

  private static boolean isSocketTimeout(String errStr) {
    if (errStr.indexOf("SocketTimeoutException") >= 0) {
      return true;
    }
    if (errStr.indexOf("socket timed out") >= 0) {
      return true;
    }
    if (errStr.indexOf("socket closed on server") >= 0) {
      return true;
    }
    return false;
  }

  //--------------------------------------------------------------------------
  // internal/test utility methods

    private static long timeInMillis(double nanos) {
  Double d = new Double(nanos / 1000000.0);
  return d.longValue();
    }

  //--------------------------------------------------------------------------
  // statistics initialization tasks

  /**
   *  INITTASK to register a performance statistics object.
   */
  public static void openStatisticsTask() {
    BridgeClient c = new BridgeClient();
    c.initTestParameters();
    c.openStatistics();
  }
  protected void openStatistics() {
    if ( this.perfstats == null ) {
      Log.getLogWriter().info("Initializing local performance statistics");
      this.perfstats = PerfStats.getInstance();
      localperfstats.set( this.perfstats );
      Log.getLogWriter().info("Initialized local performance statistics");
    }
  }

  /**
   *  CLOSETASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    BridgeClient c = new BridgeClient();
    c.initTestParameters();
    MasterController.sleepForMs(2000);
    c.closeStatistics();
  }
  protected void closeStatistics() {
    if ( this.perfstats != null ) {
      Log.getLogWriter().info("Closing local performance statistics");
      this.perfstats.close();
      Log.getLogWriter().info("Closed local performance statistics");

    } else {
      throw new HydraRuntimeException("Statistics are already closed");
    }
  }

  //--------------------------------------------------------------------------
  // instance state

  protected void initTestParameters() {
    this.trimIterations = BridgeParms.getTrimIterations();
    this.workIterations = BridgeParms.getWorkIterations();
    this.perfstats = (PerfStats)localperfstats.get();
  }
  protected int trimIterations;
  protected int workIterations;
  public PerfStats perfstats;
  private static HydraThreadLocal localperfstats = new HydraThreadLocal();
}
