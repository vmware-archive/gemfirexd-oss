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

package hct;

import hydra.CacheHelper;
import hydra.ClientPrms;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RemoteTestModule;
import hydra.TestTask;

import java.io.File;
import java.util.Random;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.internal.NanoTimer;

/**
 * UseCase1 CBB POC reproduction
 *
 * @author lises
 * @since 4.3
 */
public class UseCase1LoadMoveClient {

  public static final String STRING_PAD = "1234567890123456789012345678901234567890";

  //GemFire client consumer static variables
  public static boolean init_ = false;
  public static final int REGION_0A = 0;
  public static final int REGION_0B = 1;
  public static final String[] REGION_NAMES={"root/day0a",
                                             "root/day0b",
                                             "root/day1",
//                                               "root/day2",
//                                               "root/day3",
//                                               "root/day4",
//                                               "root/day5",
//                                               "root/day6",
                                             "root/day7"};
  public static Region[] _regions = new Region[REGION_NAMES.length];
  public static Random rand = new Random();

  private static Pool pool;

  //GemFire client consumer instance variables
  int sleepTime_ = 0;
  int groupSize_ = 0;
  int entrySize_ = 0;
  int totalKeys_ = 0;

  /**
   * Initializes the test region in the cache server VM according to
   * the "server" params in useCase1LoadMoveServer.xml.
   */
  public static void initServerCache() {

    // create subdirectories to create disk dirs in
    String bridgeName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    File aDir1 = new File(bridgeName + "_diskDir_day0a");
    aDir1.mkdir();
    File aDir2 = new File(bridgeName + "_diskDir_day0b");
    aDir2.mkdir();
    for (int i = 0; i <= 7; i++) {
      File aDir = new File(bridgeName + "_diskDir_day" + i);
      aDir.mkdir();
    }

    String cacheXmlFile = "$JTESTS/hct/" + bridgeName + ".xml";
    CacheHelper.createCacheFromXml(cacheXmlFile);

    UseCase1Client.displayRegions();
  }

  /**
   * A Hydra INIT task that initialize the test region in an edge
   * client according to the "edge" params of useCase1Client.xml
   */
  public synchronized static void initEdgeCache() {

    pool = PoolHelper.createPool("brclient");
    
    // create cache from xml
    String cacheXmlFile = "$JTESTS/hct/useCase1LoadMoveClient.xml";
    Cache cache = CacheHelper.createCacheFromXml(cacheXmlFile);

    UseCase1Client.displayRegions();
  }

  //----------------------------------------------------------------------------
  // HYDRA TASKS
  //----------------------------------------------------------------------------

    public static void invokeLoad() {
      UseCase1LoadMoveClient svc = new UseCase1LoadMoveClient();
      try {
        svc.invokeLoadWork();
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        svc.releaseClient();
        throw new HydraRuntimeException(t.getMessage(), t);
      } finally {
        svc.releaseClient();
      }
    }

    public static void invokePutGetRegionB() {
      UseCase1LoadMoveClient svc = new UseCase1LoadMoveClient();
      try {
        svc.invokePutGetRegionBWork();
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        svc.releaseClient();
        throw new HydraRuntimeException(t.getMessage(), t);
      } finally {
        svc.releaseClient();
      }
    }

    public static void invokeMoveAtoB() {
      UseCase1LoadMoveClient svc = new UseCase1LoadMoveClient();
      try {
        svc.invokeMoveAtoBWork();
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        svc.releaseClient();
        throw new HydraRuntimeException(t.getMessage(), t);
      } finally {
        svc.releaseClient();
      }
    }

    public static void invokeMoveBtoA() {
      UseCase1LoadMoveClient svc = new UseCase1LoadMoveClient();
      try {
        svc.invokeMoveBtoAWork();
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        svc.releaseClient();
        throw new HydraRuntimeException(t.getMessage(), t);
      } finally {
        svc.releaseClient();
      }
    }

  //----------------------------------------------------------------------------
  // INSTANCE STUFF
  //----------------------------------------------------------------------------

    public UseCase1LoadMoveClient() {
      groupSize_ = 50;
      sleepTime_ = 500;
      entrySize_ = 20480;
      //totalKeys_ = 23000;
      totalKeys_ = 11500;
    }

    private void releaseClient() {
      pool.releaseThreadLocalConnection();
    }

    private void invokeLoadWork() throws Exception {
      TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
      int numThreads = task.getTotalThreads();

      String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
      int tgid = RemoteTestModule.getCurrentThread().getThreadGroupId();
      int ttgid = task.getTaskThreadGroupId(tgname, tgid);
      Log.getLogWriter().info(numThreads + " threads loading " + totalKeys_ + " keys"); 

      // keys per client
      int numKeys = (int) Math.ceil(totalKeys_ /numThreads);

      int firstKey = ttgid*numKeys;
      int lastKey =  Math.min(firstKey + numKeys - 1, totalKeys_ - 1);
      Log.getLogWriter().info("Ttgid " + ttgid + " loading keys " + firstKey + "-" + lastKey);

      //do sequential puts on region A
      String key = null;
      byte[] value = null;

      for(int i=firstKey; i<=lastKey; i++) {
        key = Integer.toString(i) + STRING_PAD;
        value = new byte[entrySize_];
        for(int regionNum=0; regionNum<REGION_NAMES.length; regionNum++) {
          long startTime = NanoTimer.getTime();
          _regions[regionNum].put(key, value);
          long deltaTime = NanoTimer.getTime() - startTime;
          pool.releaseThreadLocalConnection();
        }
      }
    }

    private void invokePutGetRegionBWork() throws Exception {
      Random rng = new Random();

      //do random gets on region A and then puts on region B
      int nulls = 0;

      byte[] value = null;
      //for(int i=0; i<groupSize_; i++) {
      for(int i=0; i<10000; i++) {
        String key = Integer.toString(rng.nextInt(totalKeys_)) + STRING_PAD;
        value = new byte[entrySize_];
        long startTime = NanoTimer.getTime();
        _regions[REGION_0B].put(key, value);
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
      for(int i=0; i<10000; i++) {
        String key = Integer.toString(rng.nextInt(totalKeys_)) + STRING_PAD;
        long startTime = NanoTimer.getTime();
        if (_regions[REGION_0B].get(key) == null) {
          ++nulls;
        }
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
      if (nulls > 0) {
        System.out.println("NULL GETS: " + nulls);
      }
    }

    private void invokeMoveAtoBWork() throws Exception {
      Random rng = new Random();

      //do random gets on region A and then puts on region B
      String[] keys = new String[groupSize_];
      int nulls = 0;
      for(int i=0; i<groupSize_; i++) {
        keys[i] = Integer.toString(rng.nextInt(totalKeys_)) + STRING_PAD;
        long startTime = NanoTimer.getTime();
        if (_regions[REGION_0A].get(keys[i]) == null) {
          ++nulls;
        }
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
      if (nulls > 0) {
        System.out.println("NULL GETS: " + nulls);
      }

      if(sleepTime_ != 0) {
        Thread.sleep(sleepTime_);
      }

      byte[] value = null;
      //for(int i=0; i<groupSize_; i++) {
      for(int i=0; i<10; i++) {
        value = new byte[entrySize_];
        long startTime = NanoTimer.getTime();
        _regions[REGION_0B].put(keys[i], value);
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
    }

    private void invokeMoveBtoAWork() throws Exception {
      Random rng = new Random();

      int nulls = 0;

      //do random gets on region B and then puts on region A
      String[] keys = new String[groupSize_];
      for(int i=0; i<groupSize_; i++) {
        keys[i] = Integer.toString(rng.nextInt(totalKeys_)) + STRING_PAD;
        long startTime = NanoTimer.getTime();
        if (_regions[REGION_0B].get(keys[i]) == null) {
          ++nulls;
        }
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
      if (nulls > 0) {
        System.out.println("NULL GETS: " + nulls);
      }

      if(sleepTime_ != 0) {
        Thread.sleep(sleepTime_);
      }

      byte[] value = null;
      //for(int i=0; i<groupSize_; i++) {
      for(int i=0; i<10; i++) {
        value = new byte[entrySize_];
        long startTime = NanoTimer.getTime();
        _regions[REGION_0A].put(keys[i], value);
        long deltaTime = NanoTimer.getTime() - startTime;
        pool.releaseThreadLocalConnection();
      }
    }
}
