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
package target;

import util.*;
import hydra.*;
import hydratest.TaskAttributes;

import cacheperf.CachePerfStats;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.ThreadInterruptedException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.PutAll;

import java.util.*;

/**
 * Thread class that carries out a putAll operation on a region 
 */
class PutAllThread extends Thread {
  private boolean isDisconnected;
  private Map dataMap;
  private Region tRegion;
   
  public PutAllThread(Region tRegion,Map dataMap) {
    isDisconnected = false;
    this.dataMap = dataMap;
    this.tRegion = tRegion;
  }
  
   
  public void run() {      
  
    try {
      
      Log.getLogWriter().info("Begin the region.putAll(map)");
      Log.getLogWriter().info("Size of the region before the putAll  : " + tRegion.size());
      tRegion = RegionHelper.getRegion(TargetTest.REGION_NAME);
      
     if (tRegion.isEmpty()) {
        Log.getLogWriter().info("The region is empty");
     }
     TargetBB.getBB().getSharedMap().put("numCreates", new Integer(0));
                
     tRegion.putAll(dataMap);  
     
      Log.getLogWriter().info("region.putAll(map) completed");
      
    } catch (DistributedSystemDisconnectedException e) {
        Log.getLogWriter().info("DISCONNECTED" + e.getMessage());       
        isDisconnected = true;      
    } catch (CacheClosedException e) {
        Log.getLogWriter().info("DISCONNECTED " + e.getMessage());
        isDisconnected = true;
    } catch (Exception e) {
        Log.getLogWriter().info("Exception : " + e + TestHelper.getStackTrace());
        
    } finally {
       Log.getLogWriter().info("Number of entries in the region : " + TargetBB.getBB().getSharedMap().get("numCreates"));
    }
  }  
  
 
  public boolean isDisconnected() {
    return isDisconnected;
  }
}


public class TargetTest {

  static public Cache cache = null;
  static public Region region = null;
  private static Map <Integer, Byte[]> dataMap = null;
 
  static final private int defaultEntriesCount = 100;
  static final private int defaultByteArraySize = 100;
  static final private int defaultDelayInBetweenPuts = 1000;
  static final private int defaultEntriesThresholdPercent = 10;
  static final public String REGION_NAME = "testRegion";
  static private int entriesThreshold = 0;
  static private int numEntries = 0;
  static private int delayInBetweenPutsMS = 0;
  
  private static TargetTest testInstance = null;
  
  private TargetTest () {    
  }
  
  /** Factory method to retrieve/lazily create the singleton instance of TargetTest
   * 
   */
  public static TargetTest getTestInstance() {
    
    if (testInstance == null) {
    
      synchronized (TargetTest.class) {
      
        if (testInstance == null) {
          testInstance = new TargetTest();
        }
      }
    }
    
    return testInstance;
  }
  
  
  /** Initializes the Bridge server 
   * 
   */  
  public static synchronized void InitBridgeServer() {
    
      CacheHelper.createCache(ConfigPrms.getCacheConfig());    
      Region serverRegion = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
      BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
      delayInBetweenPutsMS =  TestConfig.tab().intAt(TargetPrms.delayInBetweenPutsMS, defaultDelayInBetweenPuts);
      PutAllCacheWriter cacheWriter = new PutAllCacheWriter(delayInBetweenPutsMS, true);
      serverRegion.getAttributesMutator().setCacheWriter(cacheWriter);
      
  }
  
  
  /** Initializes the edge client
   *  
   */
  public static void InitEdgeClient(){
      cache  = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      Log.getLogWriter().info("initEdgeClient() : Cache created");
      Region clientRegion = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
      clientRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      int delay =  TestConfig.tab().intAt(TargetPrms.delayInBetweenPutsMS, defaultDelayInBetweenPuts);
      PutAllCacheWriter cacheWriter = new PutAllCacheWriter(delay, true);
      clientRegion.getAttributesMutator().setCacheWriter(cacheWriter);      
      TargetBB.getBB().getBB().getSharedMap().put("regionSize", new Integer(-1));
  }
  
  
  /** The main task to be executed by the edge client in the Client-Server test.
   * 
   */
  public static void TaskTargetClientServer() {
    Log.getLogWriter().info("In the TaskTargetClientServer");
    TargetTest.getTestInstance().startPutAllAndCloseCache(false);    
  }
  
  /** Loads the datamap with number of entries 
   * 
   */
  public static void loadDataMap() {
    numEntries       = TestConfig.tab().intAt(TargetPrms.numMapEntries, defaultEntriesCount);
    int byteArraySize    = TestConfig.tab().intAt(TargetPrms.byteArraySize, defaultByteArraySize);
    int thresholdPercent = TestConfig.tab().intAt(TargetPrms.entriesThresholdPercent, defaultEntriesThresholdPercent);
    
    double e = thresholdPercent*numEntries/100;
    entriesThreshold = (int) e;
    
    Log.getLogWriter().info("Number of entries           : " + numEntries);
    Log.getLogWriter().info("Size of the byte array      : " + byteArraySize);
    Log.getLogWriter().info("Threshold number of entries : " + entriesThreshold);
    
    dataMap = new HashMap<Integer, Byte[]> (numEntries);
         
    for (int i=0 ; i < numEntries; i++) {
      Byte[] largeObj = new Byte [byteArraySize];
      dataMap.put(new Integer(i), largeObj);
    }
    
    Log.getLogWriter().info("Large data map created");
  }
   
  
  /** Initializes the Target test for the peer to peer configuration
   * 
   */
  public synchronized static void InitTargetP2PTest() {
    
    if (CacheHelper.getCache() == null) {      
      Log.getLogWriter().info("Creating cache");     
      
      //Create the cache
      cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      Log.getLogWriter().info("Cache created");
      
      //Create the region
      region = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
      Log.getLogWriter().info("Region created");
      
      //Create the large map used for Region.putAll(map)
      loadDataMap();     
    } 
    
    Log.getLogWriter().info("Hydra Task initialized");
  }
  
  
  /** Performs the task for the P2P configuration
   * 
   */      
  public static void TaskTargetP2P() {    
    Log.getLogWriter().info("In the TaskTargetP2P()");
    TargetTest.getTestInstance().startPutAllAndCloseCache(true);
  }
  
  /** Starts the putAll() on a region and closes the cache after certain number of creates
   * @param isP2P  <b>true </b>for P2P test <b>false</b> for client server test.
   */
  public void startPutAllAndCloseCache(boolean isP2P) {
    Region region = RegionHelper.getRegion(REGION_NAME);
    
    loadDataMap();
    PutAllThread t = new PutAllThread(region, dataMap);
    
    
    if (isP2P) {
      delayInBetweenPutsMS =  TestConfig.tab().intAt(TargetPrms.delayInBetweenPutsMS, defaultDelayInBetweenPuts);
      PutAllCacheWriter cacheWriter = new PutAllCacheWriter(delayInBetweenPutsMS);      
      region.getAttributesMutator().setCacheWriter(cacheWriter);      
      
      t.start();
      
      while (cacheWriter.getNumCreates() < entriesThreshold) {
        try {
          Thread.sleep(50);
        } catch (Exception e) {
            Log.getLogWriter().info("Exception  : " + e.getMessage());   
        }
      }
      
    } else {
        
      t.start();      
      int numEntries = 0;      
      
      while (numEntries < entriesThreshold) {
        Integer creates = (Integer) TargetBB.getBB().getSharedMap().get("numCreates");
        numEntries = creates.intValue();
        Log.getLogWriter().info("Number of entries : " + numEntries);
          
        try {
          Thread.sleep(50);
        } catch (Exception e) {
            Log.getLogWriter().info("Exception  : " + e.getMessage());   
        }
        
      } 
    }
    long cacheCloseStartTime, cacheCloseEndTime;
    long cacheCloseDuration = 0;
    
    Log.getLogWriter().info("The cache is being closed");
    cacheCloseStartTime = System.currentTimeMillis();
    cache.close();
    cacheCloseEndTime   = System.currentTimeMillis();    
    Log.getLogWriter().info("The cache is closed");
    
    try {
      t.join();
      Thread.sleep(30000);
    } catch (InterruptedException e) {
       Log.getLogWriter().info(e.getMessage()); 
    }

    
    //Check for disconnection only when the test is for P2P 
    if (isP2P) {
      if (t.isDisconnected()) {
        Log.getLogWriter().info("The test passed as the ongoing Region.putAll(map) was terminated due to a Cache.close() and CacheClosedException was rightly thrown");
      } else {
        Log.getLogWriter().info("The test failed as the Region.putAll(map) did not terminate due to a Cache.close()" );
       throw new TestException("Unexpected behavior : The test failed as the ongoing Region.putAll(map) did not terminate due to a Cache.close()");
      }
    } else {
        cacheCloseDuration = cacheCloseEndTime - cacheCloseStartTime;
        
        delayInBetweenPutsMS =  TestConfig.tab().intAt(TargetPrms.delayInBetweenPutsMS, defaultDelayInBetweenPuts);
       
        
        int putAllTime = (numEntries - entriesThreshold)*delayInBetweenPutsMS;
        
        //This is a definite failure        
        if (cacheCloseDuration > putAllTime) {
            Log.getLogWriter().info("The Cache close took really long time to return");
            Log.getLogWriter().info("Cache close duration : " + cacheCloseDuration + " (milliseconds)");
            Log.getLogWriter().info("The duration of putAll() after Cache.close() : " + putAllTime);
            
            throw new TestException("Cache.close() does not return immediately");
        }
        
       int putsInClient = ((Integer)TargetBB.getBB().getSharedMap().get("clientNumPuts")).intValue();
       int putsInServer = ((Integer)TargetBB.getBB().getSharedMap().get("numCreates")).intValue();
       
       Log.getLogWriter().info("Number of puts in the client : " + putsInClient);
       Log.getLogWriter().info("Number of puts by the server : " + putsInServer);
        
        
     }
    }
  
  public static void PutRegionSizeOnBB() {
    Cache cache =  CacheHelper.getCache();
    
    if (cache == null) {
      Log.getLogWriter().info("Not the right VM");  
    } else {
       Region aRegion = RegionHelper.getRegion(REGION_NAME);
       int regionSize = aRegion.size();       
       Log.getLogWriter().info("PutRegionSizeonBB()- Region size : " + aRegion.size());      
       //Put the regionsize on the blackboard       
       TargetBB.getBB().getSharedMap().put("regionSize", new Integer(regionSize));
    }
  }
  
  /**  This method to be used in the future, when the present is fixed
   * 
   */
  public static void VerifyRegionSize() {
    Cache cache =  CacheHelper.getCache();
    
    if (cache == null) {
      Log.getLogWriter().info("Not the right VM");  
    } else {
      Region aRegion = RegionHelper.getRegion(REGION_NAME);
      int regionSize = aRegion.size();       
      
      int regionSizeFromBB = (Integer)TargetBB.getBB().getSharedMap().get("regionSize");
       
      if (regionSize != regionSizeFromBB) {
        new TestException("Region sizes do not match, inconsistent putAll() ");
      }
    }
  }
  
 }
  