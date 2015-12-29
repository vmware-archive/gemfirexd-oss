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

package hydra.samples;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import hydra.*;
import hydra.blackboard.*;
import util.CacheUtil;

/**
*
* A sample client that does some stuff with distributed cache.
*
*/

public class DistClient {

  private static final String DATA_KEY    = "Austin";
  private final static String REGION_NAME = "Texas";

  /**
   *  createCacheTask: general task to hook a vm to the distributed cache.
   */
  public static synchronized void createCacheTask() {
    if ( CacheUtil.getCache() == null ) {
      log().info( "Initializing the cache" );
      CacheUtil.createCache();

      AttributesFactory factory = new AttributesFactory();
      factory.setScope( Scope.DISTRIBUTED_ACK ); // switch to GLOBAL when available
      factory.setRegionTimeToLive( ExpirationAttributes.DEFAULT ); // infinite
      factory.setEntryTimeToLive( ExpirationAttributes.DEFAULT ); // infinite
      //factory.setExpirationAction( irrelevant );
      //CapacityController capcon = ...;
      //factory.setCapacityController( capcon );
      //RegionEventListener revel = ...;
      //factory.setRegionEventListener( revel );


      RegionAttributes ratts = factory.createRegionAttributes();

      CacheUtil.createRegion( REGION_NAME, ratts ); 

      log().info( "Initialized the cache" );
    }
  }
  /**
   *  closeCacheTask: general task to unhook a vm from the distributed cache.
   */
  public static synchronized void closeCacheTask() {
    if ( CacheUtil.getCache() != null ) {
      log().info( "Closing the cache" );
      CacheUtil.closeCache();
      log().info( "Closed the cache" );
    }
  }
  /**
   *  initCacheTask: STARTTASK to create shared data in the distributed cache.
   *  Should be carried out by one client against one gemfire system.
   */
  public static void initCacheTask() {
    DistClient dc = new DistClient();
    dc.initdata();
  }
  /**
   *  updateTask: TASK to update data in the distributed cache.
   *  Last update wins, so best used from only one hydra client.
   */
  public static void updateTask() {
    DistClient dc = new DistClient();
    dc.update();
  }
  /**
   *  atomicUpdateTask: TASK to atomically read and update data in the
   *  distributed cache.  Uses ownership to accomplish this.
   */
  public static void atomicUpdateTask() {
    DistClient dc = new DistClient();
    dc.atomicUpdate();
  }
  /**
   *  readTask: TASK to read data in the distributed cache.
   */
  public static void readTask() {
    DistClient dc = new DistClient();
    dc.read();
  }
  /**
   *  checkDistributedCacheTask: ENDTASK to check shared data in the distributed cache.
   *  Should be carried out against all gemfire systems.
   */
  public static synchronized void checkDistributedCacheTask() {
    DistClient dc = new DistClient();
    dc.checkdata();
  }

  //////////////////////////////////////////////////////////////////////////////

  private Region getRegionOfInterest() {
    return CacheUtil.getRegion( REGION_NAME );
  }

  private void initdata() {
    log().info( "Initializing data" );
    Region region = getRegionOfInterest();
    try {
      log().info( "Initializing " + logstr( region, DATA_KEY ) );

      // put the test data in the cache
      Integer inVal = new Integer( 0 );
      region.put( DATA_KEY, inVal );
      log().info( "Initialized " + logstr( region, DATA_KEY ) );

      // verify that the same value can be regained
      Integer outVal = (Integer) region.get( DATA_KEY );
      log().info( "Got " + logstr( region, DATA_KEY, outVal ) );
      if ( ! outVal.equals( inVal ) )
        throw new SampleTestException( "Put " + inVal + " in cache, but got " + outVal );

    } catch( Exception e ) {
      throw new HydraRuntimeException( "Cache exception", e );
    }
    log().info( "Initialized data" );
  }
  private void checkdata() {
    Region region = getRegionOfInterest();
    log().info( "Checking " + logstr( region, DATA_KEY ) );

    SharedCounters counters = DistBlackboard.getInstance().getSharedCounters();
    long writes = counters.read( DistBlackboard.NumWrites );

    Integer val = (Integer) readData( region, DATA_KEY );

    if ( val.intValue() != writes )
      throw new SampleTestException( "Expected " + writes + " but got " + logstr( region, DATA_KEY, val ) );
    log().info( "Checked " + logstr( region, DATA_KEY ) );
  }

  private void update() {
    Region region = getRegionOfInterest();
    Integer oldVal = (Integer) readData( region, DATA_KEY );
    Integer newVal = new Integer( oldVal.intValue() + 1 );
    updateData( region, DATA_KEY, newVal );
  }

  private void atomicUpdate() {
    Region region = getRegionOfInterest();
    
    Lock lock = region.getDistributedLock(DATA_KEY);
    boolean locked;

    try {
      locked = lock.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    } catch( InterruptedException e ) {
      String s = "Interrupted getting ownership of " + DATA_KEY + ", "
        + region.getName();
      throw new SampleTestException( s, e );
    }

    if ( locked ) {
      try {
        update();

      } finally {
        lock.unlock();
      }

    } else {
      String s = "Timed out getting ownership of " + DATA_KEY + ", " +
        region.getName();
      throw new SampleTestException( s );
    }
  }

  private void read() {
    Region region = getRegionOfInterest(); 
    readData( region, DATA_KEY );
  }

  private Object readData( Region region, Object key ) { 
    Object val = null;
    try {
      val = region.get( key );
      log().info( "Read " + logstr( region, key, val ) );

    } catch( Exception e ) {
      throw new SampleTestException( "Problem reading " + key, e );
    }
    SharedCounters counters = DistBlackboard.getInstance().getSharedCounters();
    counters.increment( DistBlackboard.NumReads );
    return val;
  }

  private void updateData( Region region, Object key, Object val ) { 
    try {
      region.put( key, val );
      log().info( "Wrote " + logstr( region, key, val ) );

    } catch( Exception e ) {
      throw new SampleTestException( "Problem writing " + key, e );
    }

    SharedCounters counters = DistBlackboard.getInstance().getSharedCounters();
    counters.increment( DistBlackboard.NumWrites );
  }

  private String logstr( Region region, Object key ) {
    return region.getFullPath() + Region.SEPARATOR + key;
  }

  private String logstr( Region region, Object key, Object val ) {
    return logstr( region, key ) + " ==> " + val;
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
