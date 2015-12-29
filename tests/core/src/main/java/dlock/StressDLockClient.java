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

package dlock;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;

import hydra.*;
import hydra.blackboard.*;

import java.io.*;

import util.CacheUtil;
import util.NameFactory;

/**
*
* Client intended to stress the dlock mechanisms.
*
*/

public class StressDLockClient {

  private static Cache  TheCache;
  private static Region TheRegion;
  private final static String ROOT_REGION_NAME = "DLockRegion";

  /**
   * Serializable Object class
   */
  public static class SerializableObject implements Serializable {
  }

  /**
   *  createCacheTask: General task to hook the vm to the distributed cache.
   */
  public static synchronized void createCacheTask() {
    if ( TheCache == null ) {
      TheCache = CacheUtil.createCache();
      RegionAttributes ratts = getRegionAttributes();
      TheRegion = CacheUtil.createRegion( ROOT_REGION_NAME, ratts );
    }
  }
  private static RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    Scope scope = DLockPrms.getScope();
    factory.setScope( scope );
    Log.getLogWriter().info("Setting scope to " + scope);
    RegionAttributes ratts = factory.createRegionAttributes();
    return ratts;
  }
  /**
   *  closeCacheTask: General task to unhook the vm from the distributed cache.
   */
  public static synchronized void closeCacheTask() {
    if ( TheCache != null ) {
      CacheUtil.closeCache();
      TheCache = null;
      TheRegion = null;
    }
  }

  /**
   *  createDataTask TASK: create {@link DLockPrms#numLocks} objects to lock
   *  and put them in the GemFire Cache
   */
  public static void createDataTask() {
    int n = tab().intAt( DLockPrms.numLocks );
    for ( int i = 0; i < n; i++ ) {
      Object val = new SerializableObject();
      String key = NameFactory.getObjectNameForCounter(i);
      CacheUtil.put( TheRegion, key, val );
    }
  }
  /**
   *  lockByTidTask TASK: for {@link DLockPrms#iterations}, clients lock the object
   *  at index tid%{@link DLockPrms#numLocks}, sleep {@link DLockPrms#sleepMs if
   *  {@link DLockPrms#sleep} is true, then unlcok.  There is no contention if
   *  the number of clients is less than or equal to the number of locks.
   */
  public static void lockByTidTask() {
    StressDLockClient client = new StressDLockClient();
    client.lockByTidWork();
  }
  private void lockByTidWork() {

    int n = tab().intAt( DLockPrms.numLocks );
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    int index = tid % n;
    String key = NameFactory.getObjectNameForCounter(index);

    boolean sleep = tab().booleanAt( DLockPrms.sleep );

    int iterations = tab().intAt( DLockPrms.iterations );
    //long start = NanoTimer.getTime();
    for ( int i = 0; i < iterations; i++ ) {
      log().info("locking " + key);
      DLockUtil.getLock( key );
      log().info("locked " + key);
      try {
        if ( sleep ) sleep();
      } finally {
        log().info("unlocking " + key);
        DLockUtil.unlock( key );
        log().info("unlocked " + key);
      }
    }
    //long elapsed = (NanoTimer.getTime() - start)/1000000.0; // loop time, in ms

    //counters.add( DLockBlackboard.NumLocksAcquired, iterations );
    //counters.add( DLockBlackboard.TotalTimeMs, elapsed );
    //counters.setIfSmaller( DLockBlackboard.MinLoopTimeMs, elapsed );
    //counters.setIfLarger( DLockBlackboard.MaxLoopTimeMs, elapsed );
  }
  /*
  public static void validateTask() {
    //DLockBlackboard blackboard = new DLockBlackboard();
    //counters = blackboard.getSharedCounters();
    //long numLocks = counters.read( DLockBlackboard.NumLocksAcquired );
    //long lockTime = counters.read( DLockBlackboard.TotalTimeMs );
    //log().severe( "Average time per iteration (ms): " + (double)lockTime/numLocks );
    //int iterations = tab().intAt( DLockPrms.iterations );
    //long minLoopTime = counters.read( DLockBlackboard.MinLoopTimeMs );
    //log().severe( "Min loop (ms): " + minLoopTime + " avg/iteration: " + (double)minLoopTime/iterations );
    //long maxLoopTime = counters.read( DLockBlackboard.MaxLoopTimeMs );
    //log().severe( "Max loop (ms): " + maxLoopTime + " avg/iteration: " + (double)maxLoopTime/iterations );
  }
  */
  private void sleep() {
    try {
      int sleepMs = tab().intAt( DLockPrms.sleepMs );
      if ( sleepMs > 0 ) {
        Thread.sleep( sleepMs );
      }
    } catch( InterruptedException ignore ) {}
  }
  private static ConfigHashtable tab() {
    return TestConfig.tab();
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
  private SharedCounters counters;
}
