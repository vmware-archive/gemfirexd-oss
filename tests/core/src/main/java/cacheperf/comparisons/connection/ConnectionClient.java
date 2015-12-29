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

package cacheperf.comparisons.connection;

import cacheperf.*;
import cacheperf.comparisons.connection.preload.Preload;
import cacheperf.comparisons.connection.preload.PreloadPrms;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import hydra.*;
import hydra.blackboard.SharedCounters;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Client used to measure connection performance.
 */
public class ConnectionClient extends CachePerfClient
{
  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * INITTASK to preload classes from the resource specified in {@link
   * cacheperf.comparisons.connection.preload.PreloadPrms#resourceFile}.
   */
  public static void preloadResourceTask() throws IOException {
    Preload p = new Preload(
                      PreloadPrms.getResourceFile(),
                      PreloadPrms.useTimingClassLoader(),
                      PreloadPrms.initializeClasses(),
                      PreloadPrms.getOutputFile(),
                      PreloadPrms.verbose());
    p.preloadResource();
  }

  /**
   * TASK to cycle the connection on a distributed system, once only.
   * <p>
   * Set {@link ConnectionPrms#createCacheAndRegion} true to create a cache
   * and region once connected.  Set {@link ConnectionPrms#useCacheXml} true
   * to create the cache and region using generated XML instead of APIs.
   * <p>
   * Set {@link ConnectionPrms#bounceVm} true to bounce the VM after disconnect.
   * <p>
   * Set {@link ConnectionPrms#logTimes} true to log info-level messages with
   * the duration of various operations.
   * <p>
   * Set {@link ConnectionPrms#deleteSystemFiles} true to delete the system
   * files, and directory if the VM is being bounced, for each connection cycle,
   * to prevent I-node depletion.
   * <p>
   * Sidesteps the distcache framework and goes to great pains to avoid logging
   * overhead through minimal use of helper classes.  Cannot be used with most
   * task methods in {@link cacheperf.CachePerfClient}.
   * <p>
   * Writes statistics values for {@link cacheperf.TaskSyncBlackboard#ops}
   * and {@link cacheperf.TaskSyncBlackboard#opTime} needed by a separate VM
   * running the {@link cacheperf.CachePerfClient#statArchiverTask}.  This is
   * necessary since the statarchiver for the VM executing this task does not
   * live long enough to record stats.
   */
  public static void cycleConnectionOnceTask() throws CacheException
  {
    ConnectionClient cc = new ConnectionClient();
    cc.initialize(CONNECTS);
    cc.cycleConnectionOnce();
  }

  private void cycleConnectionOnce() throws CacheException
  {
    // lazily create cache XML file, if needed
    String xml = null;
    if (ConnectionPrms.createCacheAndRegion() && ConnectionPrms.useCacheXml()) {
      xml = getCacheXmlFileName();
      if (!FileUtil.exists(xml)) {
        generateCacheXmlFile(xml);
        bounceVm(); // start over with a fresh vm
      }
    }

    // initialize distributed system properties
    String gemfireConfig =
           System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    GemFireDescription gfd = TestConfig.getInstance()
                                       .getGemFireDescription(gemfireConfig);
    Properties p = gfd.getDistributedSystemProperties();
    if (xml != null) {
      p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, xml);
    }

    // initialize counters used for cycling statistics
    SharedCounters counters =
        TaskSyncBlackboard.getInstance().getSharedCounters();

    // do single connect-disconnect cycle
    cycleDistributedSystemConnection(p, counters);

    // reset vm and system directory as instructed
    if (ConnectionPrms.bounceVm()) {
      if (ConnectionPrms.deleteSystemFiles()) {
        deleteSystemDirectory(p);
      }
      bounceVm();
    }
    else if (ConnectionPrms.deleteSystemFiles()) {
      deleteSystemFiles(p);
    }
  }

  private void cycleDistributedSystemConnection(Properties p,
               SharedCounters counters) throws CacheException
  {
    boolean acquireConnection = ConnectionPrms.acquireConnection();
    long start = NanoTimer.getTime();
    long start1 = start;

    DistributedSystem ds = DistributedSystem.connect(p);

    long start2 = NanoTimer.getTime();
    long elapsed1 = start2 - start1;

    if (ConnectionPrms.createCacheAndRegion()) {
      PoolImpl thePool = null;
      Cache theCache = null;
      Region theRegion = null;
      if (ConnectionPrms.useCacheXml()) {
        theCache = CacheFactory.create(ds);
        String poolConfig = ConfigPrms.getPoolConfig();
        if (poolConfig != null) {
          thePool = (PoolImpl)PoolManager.find(poolConfig);
          if (acquireConnection) {
            thePool.acquireConnection(); // connect
          }
        }
      }
      else {
        // pool
        String poolConfig = ConfigPrms.getPoolConfig();
        if (poolConfig != null) {
          PoolDescription pd = TestConfig.getInstance()
                                         .getPoolDescription(poolConfig);
          PoolFactory poolFactory = PoolManager.createFactory();
          pd.configure(poolFactory);
          thePool = (PoolImpl)poolFactory.create(poolConfig);
          if (acquireConnection) {
            thePool.acquireConnection(); // connect
          }
        }
        // cache
        String cacheConfig = ConfigPrms.getCacheConfig();
        CacheDescription cd = TestConfig.getInstance()
                                        .getCacheDescription(cacheConfig);
        theCache = CacheFactory.create(ds);
        cd.configure(theCache);
        // region
        String regionConfig = ConfigPrms.getRegionConfig();
        RegionDescription rd = TestConfig.getInstance()
                                         .getRegionDescription(regionConfig);
        AttributesFactory regionFactory = new AttributesFactory();
        rd.configure(regionFactory, true); // connect
        RegionAttributes ratts = regionFactory.create();
        theRegion = theCache.createRegion("RecycledRegion", ratts);
      }
      if (theRegion != null) {
        theRegion.close();
      }
      if (theCache != null) {
        theCache.close();
      }
      if (thePool != null) {
        thePool.destroy();
      }
    }

    long start3 = NanoTimer.getTime();
    long elapsed2 = start3 - start2;

    ds.disconnect();

    long end = NanoTimer.getTime();
    long elapsed3 = end - start3;
    long elapsed = end - start;

    // notify the remote statarchiver via the blackboard
    counters.increment(TaskSyncBlackboard.ops);
    counters.add(TaskSyncBlackboard.opTime, elapsed);
    counters.add(TaskSyncBlackboard.opTime1, elapsed1);
    counters.add(TaskSyncBlackboard.opTime2, elapsed2);
    counters.add(TaskSyncBlackboard.opTime3, elapsed3);

    if (ConnectionPrms.logTimes()) {
      String s = "Total: " + elapsed + " Connect: "      + elapsed1
               + " Cache/Region: " + elapsed2 + " Disconnect: "   + elapsed3;
      Log.getLogWriter().info(s);
    }
  }

  //----------------------------------------------------------------------------
  //  Support methods
  //----------------------------------------------------------------------------

  /**
   * Generates an XML file for creating a cache and region, based on the
   * configurations set using {@link hydra.ConfigPrms}.
   */
  private void generateCacheXmlFile(String fn) {
    DistributedSystemHelper.connect();
    CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(),
                                     ConfigPrms.getRegionConfig(),
                                     ConfigPrms.getBridgeConfig(),
                                     ConfigPrms.getPoolConfig(),
                                     fn);
    DistributedSystemHelper.disconnect();
  }

  /**
   * Returns the cache XML file name suited to this logical hydra client type.
   */
  private String getCacheXmlFileName() {
    return System.getProperty("user.dir") + "/"
         + System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY) + ".xml";
  }

  /**
   * Bounces this VM with automatic restart after {@link CachePerfPrms#sleepMs}.
   */
  private void bounceVm() {
    try {
      ClientVmMgr.stop("Bouncing myself",
                        ClientVmMgr.NICE_KILL, CachePerfPrms.getSleepMs());
    }
    catch (ClientVmNotFoundException e) {
      throw new HydraRuntimeException("Unable to bounce myself", e);
    }
  }

  /**
   * Deletes the system directory and its contents.
   */
  private void deleteSystemDirectory(Properties p) {
    String dir = getSystemDirectory(p);
    if (dir != null) {
      FileUtil.rmdir(dir, true);
    }
  }

  /**
   * Deletes the files in the system directory.
   */
  private void deleteSystemFiles(Properties p) {
    String dir = getSystemDirectory(p);
    if (dir != null) {
      FileUtil.deleteFilesFromDir(dir);
    }
  }

  /**
   * Returns the name of the system directory.
   */
  private String getSystemDirectory(Properties p) {
    String dir = null;
    String fn = p.getProperty(DistributionConfig.LOG_FILE_NAME);
    if (fn != null) {
      File f = new File(fn);
      dir = f.getParent().toString();
    }
    return dir;
  }
}
