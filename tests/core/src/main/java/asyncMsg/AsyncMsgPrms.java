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
package asyncMsg;

import hydra.*;

import com.gemstone.gemfire.cache.*;

public class AsyncMsgPrms extends BasePrms {

  // (boolean) if true, indicates that we want to randomly override
  // the enableAsyncConflation from the RegionDefinition so that 
  // we have a mix of conflating/non-conflating regions in the region
  // hierarchy.
  // Default = false.
  public static Long randomEnableAsyncConflation;

  // (boolean) if true, verifyRegionContents also verifies that 
  // regions w/regionAttrs enableAsyncConflation = false get ALL
  // updates events, while regions with enableAsyncConflation = true
  // can have fewer events than updates in the OpList
  public static Long verifyConflationBehavior;

  // (String) full classname for ConfigurableObject type to use when
  // priming the asyncMessageQueue
  public static Long objectType;

  /**
   *  (int)
   *  The time to sleep in milliseconds for each message while
   *  priming the queue for async messaging.  Default = 0.
   */
  public static Long primeQueueSleepMs;
  public static int getPrimeQueueSleepMs() {
    Long key = primeQueueSleepMs;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ":  " + val );
    }
    return val;
  }
 
  //---------------------------------------------------------------------------
  // Cache Listener
  //---------------------------------------------------------------------------                                                                                 
  /**
   *  Class name of cache listener to use.  Defaults to null.
   */
  public static Long cacheListener;
  public static CacheListener getCacheListener() {
    Long key = cacheListener;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    try {
      return (CacheListener)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement CacheListener", e );
    }
  } 

  private static Object instantiate( Long key, String classname ) {
    if ( classname == null ) {
      return null;
    }
    try {
      Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": cannot instantiate " + classname, e );
    }
  }

    /** (Vector of Strings) A list of the operations on a region OR region entries
     *  that this test is allowed to do.  Can be one or more of:
     *     entry-create - create a new key/value in a region.
     *     entry-update - update an entry in a region with a new value.
     *     entry-destroy - destroy an entry in a region.
     *     entry-localDestroy - local destroy of an entry in a region.
     *     entry-inval - invalidate an entry in a region.
     *     entry-localInval - local invalidate of an entry in a region.
     *     entry-get - get the value of an entry.
     *     
     *     region-create - create a new region.
     *     region-destroy - destroy a region.
     *     region-localDestroy - locally destroy a region.
     *     region-inval - invalidate a region.
     *     region-localInval - locally invalidate a region.
     *     
     *     cache-close - close the cache.
     */
    public static Long operations;
    
    /** (int) The number of operations to do when AsyncMsgTest.doOperations() is called.
     */
    public static Long numOps;

    /** 
     *  (int) 
     *  Number of entries to create (per region) initially
     */
    public static Long maxKeys;

    /**
     *  (int)
     *  number of root regions to generate
     */
    public static Long numRootRegions;
                                                                                
    /**
     *  (int)
     *  number of subregions to generate for each region
     */
    public static Long numSubRegions;
                                                                                
    /**
     *  (int)
     *  depth of each region tree
     */
    public static Long regionDepth;

/** (int) The size of the region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;

/** (Vector of Strings) A list of the operations on a region entry that this
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;

/** (int) The upper size of the region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;

/** (Vector of Strings) A list of the operations on a region entry that this
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;

/** (int) For workload based tests, this is the number of executions to
 *        run to terminate the test.
 */
public static Long numExecutionsToTerminate;

  static {
     BasePrms.setValues(AsyncMsgPrms.class);
  }
}
