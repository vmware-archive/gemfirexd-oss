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

package getInitialImage;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import distcache.*;
import distcache.gemfire.GemFireCachePrms;

import distcache.gemfire.GemFireCacheTestImpl;
import hydra.*;
import java.util.*;
import objects.*;
import perffmwk.*;
import util.TestException;
import util.TestHelper;

/**
 *
 *  InitImageClient based upon cacheperf.CachePerfClient
 *  Allows us to do performance testing with additions for
 *  getInitialImage processing
 *
 */

public class InitImageClient extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Override methods
  //----------------------------------------------------------------------------


  /**
   *  TASK to unhook a vm from the cache.
   */
  public static void closeCacheTask() {
    InitImageClient c = new InitImageClient();
    c.initHydraThreadLocals();
    c.closeCache();
    c.updateHydraThreadLocals();
  }
  protected void closeCache() {
    // check to see if we want to allow the getInitialImage
    // to get underway before we close the cache
    boolean waitForGetInitialImageStart = InitImagePrms.waitForGetInitialImageStart();
    if (waitForGetInitialImageStart) {
      log().info( "closeCache: checking getInitialImage status before closing cache" );
      while (InitImageBB.getBB().getSharedCounters().read( InitImageBB.getInitialImageInProgress ) == 0 ) {
        log().fine( "closeCache: waiting for getInitialImageInProgress before closing cache" );
        MasterController.sleepForMs(200);
      }
    }

    // get the superclass to handle everything else
    super.closeCache();
 
    cacheClosedSignal();
  }
  private void cacheClosedSignal() {
    InitImageBB.getBB().getSharedCounters().increment( InitImageBB.cacheClosedSignal );
  }

  /**
   *  TASK to delay threads until getInitialImageInProgress
   */
  public static void waitForGetInitialImageInProgress() {
    InitImageClient c = new InitImageClient();
    c._waitForGetInitialImageInProgress();
  }
  private void _waitForGetInitialImageInProgress() {

    log().info( "waitForGetInitialImageInProgress: checking getInitialImage status before proceeding" );
    while (InitImageBB.getBB().getSharedCounters().read( InitImageBB.getInitialImageInProgress ) == 0 ) {
      log().fine( "waitForGetInitialImageInProgress: waiting for getInitialImageInProgress before proceeding ..." );
      MasterController.sleepForMs(200);
    }
    log().info( "GetInitialImage is IN PROGRESS" );
  }

  public static void destroyDataTask() {
    InitImageClient c = new InitImageClient();
    c.initialize( DESTROYS );
    c.destroyData();
  }
  protected void destroy( int i ) {
    super.destroy( i );

    boolean validateDestroyed = InitImagePrms.validateDestroyed();

    if (validateDestroyed) {
      Object key = ObjectHelper.createName( this.keyType, i );
      Object val = this.cache.get( key );
      // We shouldn't get a value back ... it should have been destroyed
      if (val != null) {
        throw new HydraRuntimeException( "Expected get to return null for " + key + ", but got " + val );
      }
    }
  }

  //----------------------------------------------------------------------------
  //  Support for monitoring status of getInitialImage (inProgress, Completed)
  //----------------------------------------------------------------------------

  /**
   *  TASK to check for Completion of GetInitialImage
   */
  public static void monitorGetInitialImage() {
    InitImageClient c = new InitImageClient();
    c.initHydraThreadLocals();
    c._monitorGetInitialImage();
    c.updateHydraThreadLocals();
  }
  private void _monitorGetInitialImage() {
    // first step -- update BB when getInitialImageInProgress goes to 1
    log().info( "monitorGetInitialImage: checking for getInitialImageInProgress");
    while (util.TestHelper.getStat_getInitialImagesInProgress(RegionPrms.DEFAULT_REGION_NAME) == 0) {
       log().fine("monitorGetInitialImage: waiting for getInitialImage to complete ..." );
       MasterController.sleepForMs(200);
    }
    // Update in blackboard for terminatorMethods to use
    InitImageBB.getBB().getSharedCounters().increment(InitImageBB.getInitialImageInProgress);

    // second step -- update BB when getInitialImageCompleted goes to 1
    log().info( "monitorGetInitialImage: checking for getInitialImageCompleted");
    while (util.TestHelper.getStat_getInitialImagesCompleted(RegionPrms.DEFAULT_REGION_NAME) == 0) {
       log().fine("monitorGetInitialImage: waiting for getInitialImage to complete ..." );
       MasterController.sleepForMs(200);
    }
    // Update in blackboard for terminatorMethods to use
    InitImageBB.getBB().getSharedCounters().increment(InitImageBB.getInitialImageComplete);
  }

  /** ENDTASK to flush any no-ack operations to other members of the distributed system */
  public static void sendSerialMessageToAllTask() {
    InitImageClient c = new InitImageClient();
    c.initialize();
    c.sendSerialMessageToAll();
  }
  private void sendSerialMessageToAll() {
    if (cache instanceof distcache.gemfire.GemFireCacheTestImpl) {
      try {
        com.gemstone.gemfire.distributed.internal.SerialAckedMessage msg = new com.gemstone.gemfire.distributed.internal.SerialAckedMessage();
        Region r = ((distcache.gemfire.GemFireCacheTestImpl)cache).getRegion();
        boolean mcast = r.getAttributes().getMulticastEnabled();
        msg.send(DistributedSystemHelper.getMembers(), mcast);
      }
      catch (Exception e) {
        throw new RuntimeException("Unable to send serial message due to exception", e);
      }
    }
  }

  /**
   *  ENDTASK to check number of operations completed during getInitialImage
   */
  public static void verifyOpsNotBlocked() {

    String statName = InitImagePrms.opStatName();
    int minOps = InitImagePrms.getMinOps();
    // trimspec will be cacheOpens
    String spec = "* cacheperf.CachePerfStats * " + statName + " "
                + "filter=none combine=combineAcrossArchives ops=max-min trimspec=cacheOpens";

    log().info( "statspec = " + spec );
    List psvs = PerfStatMgr.getInstance().readStatistics( spec );
    if ( psvs == null ) {
      log().info( "There was no statistic for " + statName );
    } else {
      PerfStatValue psv = (PerfStatValue) psvs.get(0);
      int max = (int) psv.getMaxMinusMin();
      if ( max < minOps ) {
        throw new HydraRuntimeException( max + " "  + statName + " operations occurred during openCache, expected a minimum of " + minOps);
      } else {
        log().info( statName + ": " + max + " (minAcceptable = " + minOps + ")" );
      }
    }
  }

  public static void verifyGII() {
    InitImageClient iic = new InitImageClient();
    iic.initialize();
    Region region = ((GemFireCacheTestImpl)iic.getCache()).getRegion();
    iic.verifyGII(region);
  }
  /**
   * Verify that all regions were gii'ed as expected (full gii).
   * throws a TestException if full gii didn't happen or if a delta gii did.
   */
  private void verifyGII(Region aRegion) {
    boolean expectDeltaGII = InitImagePrms.expectDeltaGII();
    StringBuffer aStr = new StringBuffer("verifyGII invoked with expectDeltaGII = " + expectDeltaGII + ". ");

    String regionName = aRegion.getName();

    DiskRegion diskRegion = ((LocalRegion) aRegion).getDiskRegion();
    if (diskRegion != null && diskRegion.getStats().getRemoteInitializations() == 0) {
      aStr.append(regionName + " was recovered from disk (Remote Initializations = " +
                  diskRegion.getStats().getRemoteInitializations() + ").");
    } else {
      int giisCompleted = TestHelper.getStat_getInitialImagesCompleted(regionName);
      int deltaGiisCompleted = TestHelper.getStat_deltaGetInitialImagesCompleted(regionName);
      if ((expectDeltaGII && (deltaGiisCompleted < 1)) || (!expectDeltaGII && (giisCompleted < 1 || deltaGiisCompleted > 0))) {
      //if ((giisCompleted < 1) || (deltaGiisCompleted > 0)) {
        throw new TestException("Did not perform expected type of GII. expectDeltaGII = " + expectDeltaGII +
                                aRegion.getFullPath() +
                                " GIIsCompleted = " + giisCompleted +
                                " DeltaGIIsCompleted = " + deltaGiisCompleted);
      } else {
        aStr.append(regionName + " Remote Initialization (GII): GIIsCompleted = " + giisCompleted +
                    " DeltaGIIsCompleted = " + deltaGiisCompleted + ".");
      }
    }
    Log.getLogWriter().info(aStr.toString());
  }
}
