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
package com.gemstone.gemfire.internal.admin;

import java.util.Date;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.util.BridgeWriterException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class publishes the client statistics using the admin region.
 * 
 * @author Hrishi
 */
public class ClientStatsManager {

  /**
   * Last cache that was initialized
   * 
   * @guarded.By ClientStatsManager.class
   */
  static GemFireCacheImpl lastInitializedCache = null;
  
  /**
   * @guarded.By ClientStatsManager.class
   */
  private static Statistics cachePerfStats = null;
  
  /**
   * @guarded.By ClientStatsManager.class
   */
  private static Statistics vmStats = null;
  
  /**
   * This method publishes the client stats using the admin region.
   * 
   * @param pool
   *                Connection pool which may be used for admin region.
   */
  public static synchronized void publishClientStats(PoolImpl pool) {
    GemFireCacheImpl currentCache = GemFireCacheImpl.getInstance();
    if (!initializeStatistics(currentCache)) {
      return; // handles null case too
    }
    LogWriterI18n logger = currentCache.getLoggerI18n();
    if (logger.fineEnabled())
      logger.fine("Entering ClientStatsManager#publishClientStats...");

    ClientHealthStats stats = getClientHealthStats(currentCache);

    try {
      InternalDistributedSystem ds = (InternalDistributedSystem)
          currentCache.getDistributedSystem();
      ServerRegionProxy regionProxy = new ServerRegionProxy(ClientHealthMonitoringRegion.ADMIN_REGION_NAME, pool);
      
      EventID eventId = new EventID(ds);
      EntryEventImpl event = new EntryEventImpl((Object)null);
      try {
      event.setEventId(eventId);
      regionProxy.putForMetaRegion(ds.getMemberId(), stats, null, event, null, true);
      } finally {
        event.release();
      }
    }
    catch (DistributedSystemDisconnectedException e) {
      throw e;
    }
    catch (BridgeWriterException bgx) {
      pool.getCancelCriterion().checkCancelInProgress(bgx);
      currentCache.getCancelCriterion().checkCancelInProgress(bgx);
      //TODO: Need to analyze these exception scenarios.
      logger.warning(LocalizedStrings.ClientStatsManager_FAILED_TO_SEND_CLIENT_HEALTH_STATS_TO_CACHESERVER, bgx);
    }
    catch (CacheWriterException cwx) {
      pool.getCancelCriterion().checkCancelInProgress(cwx);
      currentCache.getCancelCriterion().checkCancelInProgress(cwx);
      // TODO: Need to analyze these exception scenarios.
      logger.warning(LocalizedStrings.ClientStatsManager_FAILED_TO_SEND_CLIENT_HEALTH_STATS_TO_CACHESERVER, cwx);
    }
    catch (Exception e) {
      pool.getCancelCriterion().checkCancelInProgress(e);
      currentCache.getCancelCriterion().checkCancelInProgress(e);
      logger.info(LocalizedStrings.ClientStatsManager_FAILED_TO_PUBLISH_CLIENT_STATISTICS, e);
    }

    if (logger.fineEnabled())
      logger.fine("Exiting ClientStatsManager#publishClientStats.");

  }
  
  public static void cleanupForTests() {
    lastInitializedCache = null;
  }

  /**
   * This method initializes the client statistics to be queried.
   * 
   * @return true if statistics correctly initialized
   * @guarded.By ClientStatsManager.class
   */
  private static boolean initializeStatistics(GemFireCacheImpl currentCache) {
   
    if (currentCache == null) {
      return false;
    }
    LogWriterI18n logger = currentCache.getLoggerI18n();
    InternalDistributedSystem ds = (InternalDistributedSystem)currentCache.getDistributedSystem();
    if (currentCache.isClosed()) {
      return false;
    }
    
    boolean restart = lastInitializedCache != currentCache;
    lastInitializedCache = currentCache;
    
    if(restart) {
      if(logger.infoEnabled()) {
        logger.info(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_INTIALIZING_THE_STATISTICS);
      }
      cachePerfStats = null;
      vmStats = null;
    }
      
    if(cachePerfStats == null) {
      StatisticsType type = ds.findType("CachePerfStats");
      if(type != null) {
        Statistics[] statistics = ds.findStatisticsByType(type);
        if(statistics != null && statistics.length > 0) {
          cachePerfStats = statistics[0];
        }
      }
    }
      
    if(vmStats == null) {
      StatisticsType type = ds.findType("VMStats");
      if(type != null) {
        Statistics[] statistics = ds.findStatisticsByType(type);
        if(statistics != null && statistics.length > 0) {
          vmStats = statistics[0];
        }
      }
    }
    
    // Validate that cache has changed before logging the warning, thus logging it once per cache
    if(cachePerfStats == null && restart) {
     logger.warning(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_0_ARE_NOT_AVAILABLE, "CachePerfStats");
    }
      
    // Validate that cache has changed before logging the warning, thus logging it once per cache
    if(vmStats == null && restart) {
     logger.warning(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_0_ARE_NOT_AVAILABLE, "VMStats");
    }
   

    return true;
  }

  /**
   * This method queries the client stats & prepares the client health stats
   * object to be published to the server.
   * 
   * @return the client health stats object to be published to the server.
   */
  private static ClientHealthStats getClientHealthStats(GemFireCacheImpl currentCache) {
    if (currentCache == null) {
      return null;
    }
    ClientHealthStats stats = new ClientHealthStats();
    
    {
      int gets = -1;
      int puts = -1;
      int misses = -1;
      long processCpuTime = -1;
      int threads = -1;
      int cacheListenerCalls = -1;
      int cpus = -1;
          
      if(cachePerfStats != null) {
        gets = cachePerfStats.getInt("gets");
        puts = cachePerfStats.getInt("puts");
        misses = cachePerfStats.getInt("misses");
        cacheListenerCalls = cachePerfStats.getInt("cacheListenerCallsCompleted");
      }
      
      if(vmStats != null) {
        processCpuTime = vmStats.getLong("processCpuTime");
        threads =vmStats.getInt("threads");
        cpus = vmStats.getInt("cpus");
      }
      
      stats.setNumOfGets(gets);
      stats.setNumOfPuts(puts);
      stats.setNumOfMisses(misses);
      stats.setNumOfCacheListenerCalls(cacheListenerCalls);
      stats.setProcessCpuTime(processCpuTime);
      stats.setNumOfThreads(threads);
      stats.setCpus(cpus);
    }
    
    stats.setUpdateTime(new Date());

    return stats;
  }
}
