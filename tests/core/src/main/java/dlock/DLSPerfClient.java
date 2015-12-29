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

import com.gemstone.gemfire.cache.*;

import distcache.gemfire.*;
import cacheperf.*;
import hydra.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import objects.*;
import perffmwk.*;

/**
 *
 *  Client used to measure distributed locking service performance.
 *
 */

public class DLSPerfClient extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------

  protected static final int LOCKS          = 100;
  protected static final int UNLOCKS        = 101;

  protected static final String LOCKS_NAME      = "locks";
  protected static final String UNLOCKS_NAME    = "unlocks";


  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   *  lockTask()
   */
  public static void lockTask() {
    DLSPerfClient c = new DLSPerfClient();
    c.initialize( LOCKS );
    c.lockObjects();
  }
  private void lockObjects() {
    if (log().fineEnabled()) log().fine("locking objects");
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      
      lock( key );

      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("objects locked");
  }
  private void lock( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start;
    if (log().finerEnabled()) log().finer("locking key = " + key);
    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();
    Lock entryLock = null;
    try {
      entryLock = theRegion.getDistributedLock( key );
      start = this.dlsstats.startLock();
      entryLock.lock();
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get distributed lock", e );
    }
   
    this.dlsstats.endLock( start );
    if (log().finerEnabled()) log().finer("locked key = " + key);

    if (DLSPerfPrms.unlockAfterLock()) {
      if (log().finerEnabled()) log().finer("unlocking key = " + key);
      entryLock.unlock();
      if (log().finerEnabled()) log().finer("unlocked key = " + key);
    }
  }

  /**
   *  unlockTask()
   */
  public static void unlockTask() {
    DLSPerfClient c = new DLSPerfClient();
    c.initialize( UNLOCKS );
    c.unlockObjects();
  }
  private void unlockObjects() {
    if (log().fineEnabled()) log().fine("unlocking objects");
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      
      lock( key );
      unlock( key );

      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("objects unlocked");
  }
  private void unlock( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    if (log().finerEnabled()) log().finer("unlocking key = " + key);

    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();
    Lock entryLock = null;
    try {
      entryLock = theRegion.getDistributedLock( key );
    } finally {
      long start = this.dlsstats.startUnlock();
      entryLock.unlock();
      this.dlsstats.endUnlock( start );
    }
    if (log().finerEnabled()) log().finer("unlocked key = " + key);

  }

  /**
   *  TASK to register the DLS performance statistics object.
   *  (DistributedLockService)
   */
  public static void openStatisticsTask() {
    DLSPerfClient c = new DLSPerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  private void openStatistics() {
    if ( this.dlsstats == null ) {
      log().info( "Opening per-thread DLS performance statistics" );
      this.dlsstats = DLSPerfStats.getInstance();
      log().info( "Opened per-thread DLS performance statistics" );
    }
  }

  /**
   *  TASK to unregister the DLS performance statistics object.
   *  (DistributedLockService)
   */
  public static void closeStatisticsTask() {
    DLSPerfClient c = new DLSPerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  protected void closeStatistics() {
    MasterController.sleepForMs( 2000 );
    if ( this.dlsstats != null ) {
      log().info( "Closing per-thread DLS performance statistics" );
      this.dlsstats.close();
      log().info( "Closed per-thread DLS performance statistics" );
    }
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public DLSPerfStats dlsstats;
  public HashMap objectlist;

  private static HydraThreadLocal localdlsstats = new HydraThreadLocal();
  private static HydraThreadLocal localobjectlist = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();

    this.dlsstats = getDLSStats();
    this.objectlist = getObjectList();

  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();

    setDLSStats( this.dlsstats );
    setObjectList( this.objectlist );
  }

  /**
   *  Gets the per-thread objectList wrapper instance.
   */
  protected HashMap getObjectList() {
    HashMap objectList = (HashMap) localobjectlist.get();
    return objectList;
  }
  /**
   *  Sets the per-thread objectList wrapper instance.
   */
  protected void setObjectList( HashMap objectlist ) {
    localobjectlist.set( objectlist );
  }

  /**
   *  Gets the per-thread DLSStats wrapper instance.
   *  (DistributedLockService)
   */
  protected DLSPerfStats getDLSStats() {
    DLSPerfStats dlsstats = (DLSPerfStats) localdlsstats.get();
    return dlsstats;
  }
  /**
   *  Sets the per-thread DLSStats wrapper instance.
   *  (DistributedLockService)
   */
  protected void setDLSStats( DLSPerfStats dlsstats ) {
    localdlsstats.set( dlsstats );
  }

  //----------------------------------------------------------------------------
  //  Overridden methods
  //----------------------------------------------------------------------------

  protected String nameFor( int name ) {
    switch (name) {
      case LOCKS:      return LOCKS_NAME;
      case UNLOCKS:    return UNLOCKS_NAME;
    }
    return super.nameFor(name);
  }
}
