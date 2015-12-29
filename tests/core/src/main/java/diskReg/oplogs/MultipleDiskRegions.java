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
/*
 * Created on 5/22/07
 * since 5.1 Beta
 * 
 * Aim:- The aim of this test to validate the region features with multiple diskregions in a single vm
 * */
package diskReg.oplogs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import mapregion.MapBB;
import mapregion.MapPrms;
import objects.ObjectHelper;
import getInitialImage.InitImageBB;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.TestConfig;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.DataPolicy;

import diskRecovery.RecoveryTest;
import diskRecovery.RecoveryTestVersionHelper;

public class MultipleDiskRegions {
  static MultipleDiskRegions testInstance;

  static Cache cache;
  static Region[] regions;

  protected long minTaskGranularitySec;   //the task granularity in seconds
  protected long minTaskGranularityMS;    //the task granularity in milliseconds
  protected int lowerThreshold;           //Value of MapRegion.lowerThreshold
  protected int upperThreshold;           //Value of MapRegion.upperThreshold 
 
  
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new MultipleDiskRegions();
      testInstance.initialize();
    }
  }

  protected void initialize() {

      minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      minTaskGranularityMS = minTaskGranularitySec* TestHelper.SEC_MILLI_FACTOR;

    try {
      cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      String[] regionNames = MapPrms.getRegionNames();
      regions = new Region[regionNames.length];

      for (int i = 0; i < regionNames.length; i++) {
        RegionAttributes attr = RegionHelper
            .getRegionAttributes(ConfigPrms.getRegionConfig());
        regions[i] = RegionHelper.createRegion(regionNames[i], attr);
      }
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  public synchronized static void HydraTask_closetask() {
    if (testInstance != null) {
      testInstance.closeCache();
      testInstance = null;
    }
  }

  public void closeCache() {
    try {
      CacheHelper.closeCache();
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  public static void HydraTask_performEntryOperations() {
    testInstance.performEntryOperations();
  }

  public static void HydraTask_performRegionOperations() {
    testInstance.performRegionOperations();
  }

  static protected final int PUT = 1;
  static protected final int PUT_ALL = 2;
  static protected final int REMOVE = 3;
  static protected final int INVALIDATE = 4;
  static protected final int PUT_IF_ABSENT = 5;
  static protected final int CM_REMOVE = 6;

  protected int getEntryOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);

    if (operation.equals("put"))
      op = PUT;
    else if (operation.equals("putAll"))
      op = PUT_ALL;
    else if (operation.equals("remove"))
      op = REMOVE;
    else if (operation.equals("invalidate"))
      op = INVALIDATE;
    else if (operation.equalsIgnoreCase("putIfAbsent"))
      op = PUT_IF_ABSENT;
    else if (operation.equalsIgnoreCase("cm_remove"))
      op = CM_REMOVE;
    else
      throw new TestException("Unknown entry operation: " + operation);
    return op;
  }

  static protected final int CLEAR = 11;

  static protected final int REGION_INVALIDATE = 12;

  static protected final int REGION_DESTROY = 13;

  static protected final int FORCE_ROLLING = 14;
  
  static protected final int WRITE_TO_DISK = 15;

  protected int getRegionOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);

    if (operation.equals("clear"))
      op = CLEAR;
    else if (operation.equals("regionInvalidate"))
      op = REGION_INVALIDATE;
    else if (operation.equals("regionDestroy"))
      op = REGION_DESTROY;
    else if (operation.equals("forceRolling"))
      op = FORCE_ROLLING;
    else if (operation.equals("writeToDisk"))
      op = WRITE_TO_DISK;
    else
      throw new TestException("Unknown region operation: " + operation);

    return op;
  }

  protected void performEntryOperations() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    lowerThreshold = TestConfig.tab().intAt(MapPrms.lowerThreshold, -1);
    upperThreshold = TestConfig.tab().intAt(MapPrms.upperThreshold, Integer.MAX_VALUE);

    Log.getLogWriter().info("lowerThreshold " + lowerThreshold + ", " + "upperThreshold " + upperThreshold );

    long startTime = System.currentTimeMillis();
    do {
      int whichOp = getEntryOperation(MapPrms.entryOperationName);
      Region region = cache.getRegion(regionName);
      
      if (region == null || region.isDestroyed()) {
          recoverRegion(regionName);
          continue;
      }

 
    try {
      int size = region.size();
      
   
      if (size >= upperThreshold) {
         whichOp = getEntryOperation(MapPrms.upperThresholdOperations);
      }else if (size <= lowerThreshold) {
         whichOp = getEntryOperation(MapPrms.lowerThresholdOperations);
      }


      switch (whichOp) {
      case PUT:
        putObject(regionName);
        break;
      case PUT_ALL:
        putAllObjects(regionName);
        break;
      case REMOVE:
        removeObject(regionName);
        break;
      case INVALIDATE:
        invalidateObject(regionName);
        break;
          case PUT_IF_ABSENT:
            putIfAbsent(regionName);
            break;
          case CM_REMOVE:
            cm_remove(regionName);
            break;
      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
      }
    } catch (RegionDestroyedException rdex) {
        Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
    } while ((System.currentTimeMillis() - startTime) < minTaskGranularityMS);
  }

  protected void performRegionOperations() {
    String regionName = TestConfig.tab().stringAt(MapPrms.regionForOps);
    try {
      int whichOp = getRegionOperation(MapPrms.regionOperationName);

      switch (whichOp) {
      case CLEAR:
        clearRegion(regionName);
        break;
      case REGION_INVALIDATE:
        invalidateRegion(regionName);
        break;
      case REGION_DESTROY:
        destroyRegion(regionName);
        break;
      case FORCE_ROLLING:
        forceRollOplogs(regionName);
        break;
      case WRITE_TO_DISK:
        writeToDisk(regionName);
        break;

      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
      }
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected synchronized void recoverRegion(String regionName) {
    final List<Boolean> revoking = new ArrayList();
    revoking.add(new Boolean(true));
    Thread revokeThread = new Thread(new Runnable() {
      public void run() {
        while (revoking.get(0)) {
          RecoveryTestVersionHelper.forceRecovery(false);
          MasterController.sleepForMs(1000);
        }
      }
    });

    try {
      Region region = cache.getRegion(regionName);
      if (region == null || region.isDestroyed()) {
        // //this is dirty fix...awaiting a feedback from fabricdev here
        // on what to do?
        // // darrel said it's a bug. Logged as BUG #34269
        Log.getLogWriter().info("recovering region...");
        Thread.sleep(4 * 1000);
        RegionAttributes attr = RegionHelper
            .getRegionAttributes(ConfigPrms.getRegionConfig());
        Log.getLogWriter().info("Starting thread to revoke members...");
        revokeThread.start();
        RegionHelper.createRegion(regionName, attr);
        Log.getLogWriter().info("region recovered..." + regionName);
        revoking.set(0, new Boolean(false));
        revokeThread.join();
        Log.getLogWriter().info("Thread to revoke members terminated");
      }
    } catch (Exception ex) {
      String stack = util.TestHelper.getStackTrace(ex);
      if (stack.indexOf("RegionExistsException") != -1) {
        Log
            .getLogWriter()
            .warning(
                "RegionExistsException is caught. It may appear in concurrent environment...continuing with the test");
      } else {
        throw new TestException(TestHelper.getStackTrace(ex));
      }
    }
  }

  protected void putObject(String regionName) {
    try {
      Object key = null, val = null;
      String objectType = MapPrms.getObjectType();
      Region region = cache.getRegion(regionName);

      if (region != null) {
        int putKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
          key = ObjectHelper.createName(putKeyInt);
          val = ObjectHelper.createObject(objectType, putKeyInt);
          region.put(key, val);

        Log.getLogWriter().info( "----performed put operation on " + regionName + " with " + key);
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info( "RegionDestroyedException...may occur in concurrent execution mode. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void putIfAbsent(String regionName) {
    try {
      Object key = null, val = null;
      String objectType = MapPrms.getObjectType();
      Region region = cache.getRegion(regionName);

      if (region != null) {

        // ConcurrentMap operations are not supported for peers with EMPTY or NORMAL dataPolicy
        DataPolicy dataPolicy = region.getAttributes().getDataPolicy();
        if (dataPolicy.equals(DataPolicy.NORMAL) || dataPolicy.equals(DataPolicy.EMPTY)) {
          return;
        }

        int putKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
        key = ObjectHelper.createName(putKeyInt);
        val = ObjectHelper.createObject(objectType, putKeyInt);
        Object retVal = region.putIfAbsent(key, val);
        Log.getLogWriter().info( "----performed putIfAbsent operation on " + regionName + " with " + key + ".  putIfAbsent returned " + retVal);
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info( "RegionDestroyedException...may occur in concurrent execution mode. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void putAllObjects(String regionName) {
    // // MapBB.NUM_PUT is taken because it will give the actual number of
    // keys already put inside region.
    Object key = null, val = null;
    String objectType = MapPrms.getObjectType();
    Map m = new TreeMap();
    int numEntries = TestConfig.tab().getRandGen().nextInt(1, 25);
    int mapSize = 0;

    do {
      int putAllKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_PUT);
      key = ObjectHelper.createName(putAllKeyInt);
      val = ObjectHelper.createObject(objectType, putAllKeyInt);
      m.put(key, val);
      mapSize++;
    } while (mapSize < numEntries);

    Region region = cache.getRegion(regionName);
    if (region != null) {
      try {
        region.putAll(m);
        Log.getLogWriter().info("----performed putAll operation on " + regionName + " with map size = " + m.size());
      } catch (RegionDestroyedException rdex) {
        Log
            .getLogWriter()
            .info(
                "RegionDestroyedException...may occur in concurrent environment mode. Continuing with test.");
        recoverRegion(regionName);
      } catch (Exception ex) {
        throw new TestException(TestHelper.getStackTrace(ex));
      }
    }
  }

  protected void removeObject(String regionName) {
    try {

      Object key = null;
      Region region = cache.getRegion(regionName);

      if (region != null) {
        int removeKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
          key = ObjectHelper.createName(removeKeyInt);

          Object got = region.get(key);
          if (got == null) {
          Log.getLogWriter().info("removeObject: " + key + " does NOT exist in " + regionName);
          } else {
            region.remove(key);
          Log.getLogWriter().info("----performed remove operation on " + regionName + " with " + key);
        }
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void cm_remove(String regionName) {
    try {

      Object key = null;
      Region region = cache.getRegion(regionName);

      if (region != null) {

        // ConcurrentMap operations are not supported for peers with EMPTY or NORMAL dataPolicy
        DataPolicy dataPolicy = region.getAttributes().getDataPolicy();
        if (dataPolicy.equals(DataPolicy.NORMAL) || dataPolicy.equals(DataPolicy.EMPTY)) {
          return;
          }

        int removeKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_REMOVE);
        key = ObjectHelper.createName(removeKeyInt);

        Object got = region.get(key);
        if (got == null) {
          Log.getLogWriter().info("cm_remove: " + key + " does NOT exist in " + regionName);
        } else {
          boolean removed = region.remove(key, got);
          Log.getLogWriter().info("----performed remove operation on " + regionName + " with " + key + " previous value " + got + ".  remove returned " + removed);
      }
      }
    } catch (RegionDestroyedException rdex) {
      Log.getLogWriter().info("RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void invalidateObject(String regionName) {
    try {

      Object key = null;
      Region region = cache.getRegion(regionName);

      if (region != null) {
          int invalidateKeyInt = (int) MapBB.getBB().getSharedCounters().incrementAndRead(MapBB.NUM_INVALIDATE);
          key = ObjectHelper.createName(invalidateKeyInt);

          Object got = region.get(key);

          if (got == null) {
            Log.getLogWriter().info("invalidate: NO entry exists in " + region.getName() + " for key " + key);
          } else {
            region.invalidate(key);

            Log.getLogWriter().info("----performed invalidate operation on " + regionName + " with " + key);
          }
      }
    } catch (EntryNotFoundException enfe) {
      Log
          .getLogWriter()
          .info(
              "EntryNotFoundException ...may occur in concurrent execution. Continuing with test.");
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...can occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void clearRegion(String regionName) {
    try {
      Region region = cache.getRegion(regionName);
      if (region != null) {
        region.clear();
        Log.getLogWriter().info(
            "----performed clear operation on region" + regionName);
      }

    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void invalidateRegion(String regionName) {
    try {
      Region region = cache.getRegion(regionName);
      if (region != null) {
        region.invalidateRegion();
        Log.getLogWriter().info(
            "----performed invalidateRegion operation on region"
                + regionName);
      }
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }

  }

  protected void destroyRegion(String regionName) {
    try {
      Region region = cache.getRegion(regionName);
      if (region != null) {
        region.destroyRegion();
        Log.getLogWriter().info(
            "----performed destroyRegion operation on region"
                + regionName);
      }
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void forceRollOplogs(String regionName) {
    try {
      int NO_DISK = 1001;
      int DISK_FOR_OVRFLW = 1002;
      int DISK_FOR_PERSIST = 1003;
      int DISK_FOR_OVRFLW_PERSIST = 1004;

      boolean persistentReplicate;
      int evictionLimit;
      int regionType = 0;

      Region region = cache.getRegion(regionName);
      if (region != null) {
        RegionAttributes attr = region.getAttributes();
        persistentReplicate = attr.getDataPolicy()
            .isPersistentReplicate();
        evictionLimit = attr.getEvictionAttributes().getMaximum();

        if (persistentReplicate) {
          if (evictionLimit <= 0)
            regionType = DISK_FOR_PERSIST;
          else
            regionType = DISK_FOR_OVRFLW_PERSIST;
        } else {
          if (evictionLimit <= 0)
            regionType = NO_DISK;
          else
            regionType = DISK_FOR_OVRFLW;
        }

        if (regionType == DISK_FOR_PERSIST
            || regionType == DISK_FOR_OVRFLW_PERSIST
            || regionType == DISK_FOR_OVRFLW) {
          region.forceRolling();
          Log.getLogWriter().info("force rolled oplogs");
        }

        if (regionType == NO_DISK) {
          try {
            region.forceRolling();
            throw new TestException(
                "Should have thrown UnsupportedOperationException while performing forceRolling on non disk regions");
          } catch (UnsupportedOperationException ignore) {
            // expected
          }
        }
      }
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }

  protected void writeToDisk(String regionName) {
    try {
      int NO_DISK = 1001;
      int DISK_FOR_OVRFLW = 1002;
      int DISK_FOR_PERSIST = 1003;
      int DISK_FOR_OVRFLW_PERSIST = 1004;

      boolean persistentReplicate;
      int evictionLimit;
      int regionType = 0;

      Region region = cache.getRegion(regionName);
      if (region != null) {
        RegionAttributes attr = region.getAttributes();
        persistentReplicate = attr.getDataPolicy()
            .isPersistentReplicate();
        evictionLimit = attr.getEvictionAttributes().getMaximum();

        if (persistentReplicate) {
          if (evictionLimit <= 0)
            regionType = DISK_FOR_PERSIST;
        } else {
            regionType = DISK_FOR_OVRFLW_PERSIST;
        } 

        if (regionType == DISK_FOR_PERSIST
            || regionType == DISK_FOR_OVRFLW_PERSIST )
            {
          region.writeToDisk();
          Log.getLogWriter().info("writeToDisk performed on region: "+region.getName());
        }       
      }
    } catch (RegionDestroyedException rdex) {
      Log
          .getLogWriter()
          .info(
              "RegionDestroyedException...may occur in concurrent environment. Continuing with test.");
      recoverRegion(regionName);
    } catch (Exception ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
    }
  }
}
